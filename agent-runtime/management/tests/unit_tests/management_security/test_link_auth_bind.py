# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved

"""link-auth 落库显式绑定单元测试（CertificatePinStore + 异步 verify_and_bind，真 sqlite）。

进程内 TOFU 版的测试在 foundation 层 ``foundation/tests/unit_tests/test_link_auth.py``。
"""

from __future__ import annotations

import pytest

from openjiuwen_runtime.foundation.db.sqlite_handler import SQLiteHandler
from openjiuwen_runtime.foundation.security.link_auth import build_token, generate_keypair
from openjiuwen_runtime.management.security import (
    CertificateManager,
    CertificatePinStore,
    KeyPurpose,
    verify_and_bind,
)


@pytest.fixture(autouse=True)
def _enforce(monkeypatch):
    monkeypatch.setenv("CLAW_LINK_AUTH_MODE", "enforce")


@pytest.fixture
async def manager(tmp_path):
    handler = SQLiteHandler(str(tmp_path / "security.db"))
    await handler.init_database()
    await handler.connect()
    cm = CertificateManager(handler)
    await cm.ensure_ready()
    try:
        yield cm
    finally:
        await handler.disconnect()


def _pin_store(manager) -> CertificatePinStore:
    return CertificatePinStore(manager, purpose=KeyPurpose.SIGN.value)


async def test_first_contact_binds_and_passes(manager):
    store = _pin_store(manager)
    priv, pub = generate_keypair()
    tok = build_token(service_id="gw-1", service_type="gateway", private_b64=priv, public_b64=pub)
    res = await verify_and_bind(store, tok, expect_type="gateway")
    assert res.allowed and res.ok
    # 已落库绑定：再查指纹应等于本次对端指纹。
    assert await store.pinned("gw-1") == res.peer_fp


async def test_same_key_passes_across_reconnects(manager):
    store = _pin_store(manager)
    priv, pub = generate_keypair()
    for _ in range(3):
        tok = build_token(service_id="gw-1", service_type="gateway", private_b64=priv, public_b64=pub)
        res = await verify_and_bind(store, tok, expect_type="gateway")
        assert res.allowed and res.ok


async def test_changed_key_rejected(manager):
    store = _pin_store(manager)
    priv, pub = generate_keypair()
    tok = build_token(service_id="gw-1", service_type="gateway", private_b64=priv, public_b64=pub)
    assert (await verify_and_bind(store, tok, expect_type="gateway")).ok  # 首次绑定

    ipriv, ipub = generate_keypair()  # 冒充者：同 iss、不同密钥
    itok = build_token(service_id="gw-1", service_type="gateway", private_b64=ipriv, public_b64=ipub)
    res = await verify_and_bind(store, itok, expect_type="gateway")
    assert (not res.allowed) and res.reason == "fingerprint mismatch"


async def test_binding_persists_across_store_instances(manager):
    # 同一个 DB,换一个 store 实例(模拟进程重启后重新加载),绑定仍在 → 显式绑定的持久性。
    priv, pub = generate_keypair()
    tok = build_token(service_id="gw-1", service_type="gateway", private_b64=priv, public_b64=pub)
    res = await verify_and_bind(_pin_store(manager), tok, expect_type="gateway")
    fp = res.peer_fp
    second = _pin_store(manager)  # 同一 DB、新 store 实例
    assert await second.pinned("gw-1") == fp
