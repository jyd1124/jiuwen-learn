# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved

"""落库显式绑定的指纹固定（link-auth 的 DB 后端）。

把 foundation 层 link-auth 的「指纹固定」接到 management 层 :class:`CertificateManager`：
对端公钥按 ``(peer_id=iss, purpose=sign)`` 落库（``status=bound``），握手时比对指纹。
相对进程内 TOFU（:class:`InMemoryPinStore`），它是**显式绑定**：持久、可按 key_version
轮换、解绑即删行——与配置下发的密钥管理同一套模型。

因 :class:`CertificateManager` 为异步 DB 操作，本模块提供**异步** :func:`verify_and_bind`，
与 foundation 的同步 :func:`verify_and_pin`（内存 TOFU）并列；集成方按链路是否有持久 DB
二选一（构造 ``CertificatePinStore`` 走落库绑定，或 ``InMemoryPinStore`` 走进程内 TOFU）。
"""

from __future__ import annotations

import logging
from typing import Optional

from openjiuwen_runtime.foundation.security import _primitives as _p
from openjiuwen_runtime.foundation.security.link_auth import (
    AuthMode,
    NonceCache,
    VerifyResult,
    verify_token,
)
from openjiuwen_runtime.foundation.security.models import KeyPurpose

from .certificate import CertificateManager

logger = logging.getLogger(__name__)


class CertificatePinStore:
    """:class:`CertificateManager` 的指纹固定适配器（异步、落库、显式绑定）。

    - ``pinned(iss)``：读对端 ``sign`` 公钥的已绑定指纹（无则 ``None``）。
    - ``remember(iss, pub_b64, fp)``：录入/更新对端 ``sign`` 公钥（``status=bound``）。
    """

    def __init__(
        self, manager: CertificateManager, *, purpose: str = KeyPurpose.SIGN.value
    ) -> None:
        self._cm = manager
        self._purpose = purpose

    async def pinned(self, iss: str) -> Optional[str]:
        peer = await self._cm.load_peer_key(iss, self._purpose)
        return peer.fingerprint if peer is not None else None

    async def remember(self, iss: str, pub_b64: str, fp: str) -> None:
        await self._cm.save_peer_key(iss, self._purpose, _p.b64d(pub_b64))


async def verify_and_bind(
    store: CertificatePinStore,
    token: str | None,
    *,
    expect_type: str | None = None,
    nonce_cache: NonceCache | None = None,
) -> VerifyResult:
    """验令牌 + 落库显式绑定（持久端首选入口，异步）。

    首次见到该身份：验签通过即落库绑定其公钥并放行；之后：比对已绑定指纹，不一致即拒。
    内存 TOFU 版见 foundation 的 :func:`verify_and_pin`。
    """
    res = verify_token(token, expect_type=expect_type, nonce_cache=nonce_cache)
    if not res.ok or res.claims is None or res.peer_fp is None:
        return res
    iss = res.claims.iss
    pinned = await store.pinned(iss)
    if pinned is not None and pinned != res.peer_fp:
        mode = res.mode
        logger.warning(
            "[link_auth] %s: fingerprint changed for iss=%s (possible impersonation), rejecting",
            mode.value, iss,
        )
        return VerifyResult(
            allowed=(mode is AuthMode.OBSERVE),
            ok=False,
            reason="fingerprint mismatch",
            mode=mode,
            peer_fp=res.peer_fp,
            claims=res.claims,
        )
    await store.remember(iss, res.claims.pub, res.peer_fp)
    return res
