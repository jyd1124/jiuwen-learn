# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved

"""证书/密钥管理：密钥与证书的录入、保存、读取、删除（client/server 通用）。

- 本端密钥对（self）：按用途持有，私钥落库受保护、永不外发。
- 对端公钥（peer）：握手录入并保存（=确认配对），按需读取、解绑删除。

``KeyExchange`` 提供传输无关的握手编排（begin/complete），由调用方接到自己的
通道上（WebSocket / HTTP 等）；``server_key_exchange`` / ``client_key_exchange``
给出服务端、客户端两种角色的标准用法。
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from openjiuwen_runtime.foundation.db.handler import DBHandler
from openjiuwen_runtime.foundation.security import _primitives as _p
from openjiuwen_runtime.foundation.security.interfaces import ICertificateManager
from openjiuwen_runtime.foundation.security.models import (
    EncAlgorithm,
    KeyPair,
    KeyPurpose,
    PeerKey,
    SECURITY_LOCAL_KEY_TABLE_DEF,
    SECURITY_PEER_KEY_TABLE_DEF,
    SignAlgorithm,
)

_LOCAL_TABLE = SECURITY_LOCAL_KEY_TABLE_DEF.table_name
_PEER_TABLE = SECURITY_PEER_KEY_TABLE_DEF.table_name


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _norm_purpose(purpose: Any) -> str:
    return purpose.value if isinstance(purpose, KeyPurpose) else str(purpose)


def _row(row: Any) -> Optional[dict]:
    if row is None:
        return None
    if isinstance(row, dict):
        return row
    if hasattr(row, "to_dict"):
        return row.to_dict()
    return None


def _algorithm_for(purpose: str) -> str:
    if purpose == KeyPurpose.SIGN.value:
        return SignAlgorithm.ED25519.value
    if purpose == KeyPurpose.ENCRYPT.value:
        return EncAlgorithm.X25519.value
    raise ValueError(f"unknown key purpose: {purpose!r}")


def _generate_for(purpose: str) -> tuple[bytes, bytes]:
    if purpose == KeyPurpose.SIGN.value:
        return _p.ed25519_generate()
    if purpose == KeyPurpose.ENCRYPT.value:
        return _p.x25519_generate()
    raise ValueError(f"unknown key purpose: {purpose!r}")


class CertificateManager(ICertificateManager):
    """基于 foundation ``DBHandler`` 的密钥/证书管理实现。"""

    def __init__(self, db_handler: DBHandler) -> None:
        self._db = db_handler

    async def ensure_ready(self) -> None:
        """初始化两张独立表（幂等：存在则跳过，不影响既有表）。"""
        await self._db.init_table(SECURITY_LOCAL_KEY_TABLE_DEF)
        await self._db.init_table(SECURITY_PEER_KEY_TABLE_DEF)

    # ---- 本端密钥对 ----

    async def get_or_create_keypair(self, purpose: str) -> KeyPair:
        purpose = _norm_purpose(purpose)
        row = _row(await self._db.get(_LOCAL_TABLE, {"purpose": purpose}))
        if row and row.get("private_key") and row.get("public_key"):
            priv = _p.b64d(row["private_key"])
            pub = _p.b64d(row["public_key"])
            return KeyPair(
                purpose=purpose,
                algorithm=str(row.get("algorithm") or _algorithm_for(purpose)),
                private_raw=priv,
                public_raw=pub,
                fingerprint=str(row.get("fingerprint") or _p.fingerprint(pub)),
                key_version=str(row.get("key_version") or "v1"),
            )

        priv, pub = _generate_for(purpose)
        fp = _p.fingerprint(pub)
        now = _now()
        await self._db.create(
            _LOCAL_TABLE,
            {
                "purpose": purpose,
                "algorithm": _algorithm_for(purpose),
                "private_key": _p.b64e(priv),
                "public_key": _p.b64e(pub),
                "fingerprint": fp,
                "key_version": "v1",
                "created_at": now,
                "updated_at": now,
            },
        )
        return KeyPair(purpose, _algorithm_for(purpose), priv, pub, fp, "v1")

    async def export_public(self, purpose: str) -> bytes:
        return (await self.get_or_create_keypair(purpose)).public_raw

    # ---- 对端公钥：录入 / 读取 / 删除 ----

    @staticmethod
    def _record_id(peer_id: str, purpose: str) -> str:
        return f"{peer_id}::{purpose}"

    async def save_peer_key(
        self,
        peer_id: str,
        purpose: str,
        public_key: bytes,
        *,
        algorithm: str = "",
        key_version: str = "v1",
    ) -> str:
        purpose = _norm_purpose(purpose)
        fp = _p.fingerprint(public_key)
        now = _now()
        rid = self._record_id(peer_id, purpose)
        data = {
            "peer_id": peer_id,
            "purpose": purpose,
            "algorithm": algorithm or _algorithm_for(purpose),
            "public_key": _p.b64e(public_key),
            "fingerprint": fp,
            "key_version": key_version,
            "status": "bound",
            "updated_at": now,
        }
        if _row(await self._db.get(_PEER_TABLE, {"record_id": rid})) is not None:
            await self._db.update(_PEER_TABLE, {"record_id": rid}, data)
        else:
            await self._db.create(_PEER_TABLE, {"record_id": rid, "created_at": now, **data})
        return fp

    async def load_peer_key(self, peer_id: str, purpose: str) -> Optional[PeerKey]:
        purpose = _norm_purpose(purpose)
        row = _row(await self._db.get(_PEER_TABLE, {"record_id": self._record_id(peer_id, purpose)}))
        if not row or not row.get("public_key") or str(row.get("status")) != "bound":
            return None
        pub = _p.b64d(row["public_key"])
        return PeerKey(
            peer_id=str(row.get("peer_id") or peer_id),
            purpose=purpose,
            algorithm=str(row.get("algorithm") or _algorithm_for(purpose)),
            public_raw=pub,
            fingerprint=str(row.get("fingerprint") or _p.fingerprint(pub)),
            key_version=str(row.get("key_version") or "v1"),
        )

    async def delete_peer_key(self, peer_id: str, purpose: Optional[str] = None) -> None:
        if purpose is not None:
            await self._db.delete(
                _PEER_TABLE, {"record_id": self._record_id(peer_id, _norm_purpose(purpose))}
            )
        else:
            await self._db.delete(_PEER_TABLE, {"peer_id": peer_id})

    async def list_peer_keys(self, *, purpose: Optional[str] = None) -> list[PeerKey]:
        filters = {"purpose": _norm_purpose(purpose)} if purpose is not None else None
        rows = await self._db.list_records(_PEER_TABLE, filters)
        out: list[PeerKey] = []
        for raw in rows or []:
            row = _row(raw)
            if not row or not row.get("public_key"):
                continue
            pub = _p.b64d(row["public_key"])
            out.append(
                PeerKey(
                    peer_id=str(row.get("peer_id") or ""),
                    purpose=str(row.get("purpose") or ""),
                    algorithm=str(row.get("algorithm") or ""),
                    public_raw=pub,
                    fingerprint=str(row.get("fingerprint") or _p.fingerprint(pub)),
                    key_version=str(row.get("key_version") or "v1"),
                )
            )
        return out


class KeyExchange:
    """传输无关的握手密钥交换编排。

    - ``begin()``：返回本端公钥（由调用方塞进自己的握手帧发出去）。
    - ``complete(peer_id, peer_public)``：保存对端公钥（=确认配对），返回指纹。

    ``local_purpose`` 为本端导出的公钥用途，``peer_purpose`` 为录入的对端公钥用途。
    """

    def __init__(
        self,
        manager: ICertificateManager,
        *,
        local_purpose: str,
        peer_purpose: str,
    ) -> None:
        self._cm = manager
        self._local_purpose = _norm_purpose(local_purpose)
        self._peer_purpose = _norm_purpose(peer_purpose)

    async def begin(self) -> bytes:
        return await self._cm.export_public(self._local_purpose)

    async def complete(self, peer_id: str, peer_public: bytes, *, key_version: str = "v1") -> str:
        return await self._cm.save_peer_key(
            peer_id, self._peer_purpose, peer_public, key_version=key_version
        )


def server_key_exchange(manager: ICertificateManager) -> KeyExchange:
    """服务端（如 Manager）：导出本端 ``sign`` 公钥、录入各客户端的 ``encrypt`` 公钥。"""
    return KeyExchange(
        manager, local_purpose=KeyPurpose.SIGN.value, peer_purpose=KeyPurpose.ENCRYPT.value
    )


def client_key_exchange(manager: ICertificateManager) -> KeyExchange:
    """客户端（如 Gateway）：导出本端 ``encrypt`` 公钥、录入服务端的 ``sign`` 公钥。"""
    return KeyExchange(
        manager, local_purpose=KeyPurpose.ENCRYPT.value, peer_purpose=KeyPurpose.SIGN.value
    )
