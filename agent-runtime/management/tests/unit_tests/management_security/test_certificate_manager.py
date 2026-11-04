# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved

"""证书/密钥管理（录入/保存/读取/删除）+ 握手密钥交换单元测试（真 sqlite）。"""

from __future__ import annotations

import pytest

from openjiuwen_runtime.foundation.db.sqlite_handler import SQLiteHandler
from openjiuwen_runtime.management.security import (
    CertificateManager,
    KeyPurpose,
    client_key_exchange,
    server_key_exchange,
)
from openjiuwen_runtime.foundation.security._primitives import (
    ed25519_generate,
    fingerprint,
    x25519_generate,
)


@pytest.fixture
async def cm(tmp_path):
    handler = SQLiteHandler(str(tmp_path / "security.db"))
    await handler.init_database()
    await handler.connect()
    manager = CertificateManager(handler)
    await manager.ensure_ready()
    try:
        yield manager
    finally:
        await handler.disconnect()


async def test_local_keypair_get_or_create_is_idempotent(cm):
    a = await cm.get_or_create_keypair(KeyPurpose.SIGN)
    b = await cm.get_or_create_keypair(KeyPurpose.SIGN)
    assert a.public_raw == b.public_raw and a.private_raw == b.private_raw
    assert a.algorithm == "Ed25519"
    # 加密用途是另一对（X25519）
    enc = await cm.get_or_create_keypair(KeyPurpose.ENCRYPT)
    assert enc.algorithm == "X25519" and enc.public_raw != a.public_raw


async def test_export_public_matches_keypair(cm):
    kp = await cm.get_or_create_keypair(KeyPurpose.SIGN)
    assert await cm.export_public(KeyPurpose.SIGN) == kp.public_raw


async def test_peer_key_enroll_load_delete(cm):
    _, peer_pub = x25519_generate()
    fp = await cm.save_peer_key("gateway-1", KeyPurpose.ENCRYPT, peer_pub)
    assert fp == fingerprint(peer_pub)

    loaded = await cm.load_peer_key("gateway-1", KeyPurpose.ENCRYPT)
    assert loaded is not None
    assert loaded.public_raw == peer_pub and loaded.fingerprint == fp

    # 覆盖更新（再次录入新公钥）
    _, peer_pub2 = x25519_generate()
    await cm.save_peer_key("gateway-1", KeyPurpose.ENCRYPT, peer_pub2)
    assert (await cm.load_peer_key("gateway-1", KeyPurpose.ENCRYPT)).public_raw == peer_pub2

    # 删除（解绑销毁）
    await cm.delete_peer_key("gateway-1", KeyPurpose.ENCRYPT)
    assert await cm.load_peer_key("gateway-1", KeyPurpose.ENCRYPT) is None


async def test_list_and_delete_all_for_peer(cm):
    _, k1 = x25519_generate()
    _, k2 = ed25519_generate()
    await cm.save_peer_key("p1", KeyPurpose.ENCRYPT, k1)
    await cm.save_peer_key("p1", KeyPurpose.SIGN, k2)
    await cm.save_peer_key("p2", KeyPurpose.ENCRYPT, x25519_generate()[1])

    enc_peers = await cm.list_peer_keys(purpose=KeyPurpose.ENCRYPT.value)
    assert {p.peer_id for p in enc_peers} == {"p1", "p2"}

    await cm.delete_peer_key("p1")  # 删 p1 全部用途
    assert await cm.load_peer_key("p1", KeyPurpose.ENCRYPT) is None
    assert await cm.load_peer_key("p1", KeyPurpose.SIGN) is None
    assert await cm.load_peer_key("p2", KeyPurpose.ENCRYPT) is not None


async def test_server_client_key_exchange_roundtrip(cm, tmp_path):
    """模拟握手：服务端导出 sign 公钥、客户端导出 encrypt 公钥，各自录入对端。"""
    # 另起一个客户端 CertificateManager（独立库）
    h2 = SQLiteHandler(str(tmp_path / "client.db"))
    await h2.init_database()
    await h2.connect()
    client_cm = CertificateManager(h2)
    await client_cm.ensure_ready()
    try:
        server = server_key_exchange(cm)         # 本端 sign / 录入对端 encrypt
        client = client_key_exchange(client_cm)  # 本端 encrypt / 录入对端 sign

        server_sign_pub = await server.begin()   # 服务端把签名公钥发给客户端
        client_enc_pub = await client.begin()    # 客户端把加密公钥发给服务端

        fp_c = await server.complete("client-1", client_enc_pub)  # 服务端录入客户端加密公钥
        fp_s = await client.complete("server-1", server_sign_pub)  # 客户端录入服务端签名公钥

        # 双方都能读回对端公钥，指纹一致
        assert (await cm.load_peer_key("client-1", KeyPurpose.ENCRYPT)).fingerprint == fp_c
        assert (await client_cm.load_peer_key("server-1", KeyPurpose.SIGN)).fingerprint == fp_s
        assert fp_c == fingerprint(client_enc_pub)
        assert fp_s == fingerprint(server_sign_pub)
    finally:
        await h2.disconnect()
