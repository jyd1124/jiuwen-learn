# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved

"""加签验签（Ed25519）单元测试。"""

from __future__ import annotations

from openjiuwen_runtime.management.security import (
    Ed25519Signer,
    Ed25519Verifier,
    KeyPair,
    SignAlgorithm,
    make_signer,
    make_verifier,
)
from openjiuwen_runtime.foundation.security._primitives import ed25519_generate


def test_sign_verify_roundtrip():
    priv, pub = ed25519_generate()
    signer = Ed25519Signer(priv, "v1")
    assert signer.algorithm == SignAlgorithm.ED25519.value
    sig = signer.sign(b"config-bytes")
    assert Ed25519Verifier(pub).verify(b"config-bytes", sig) is True


def test_tamper_and_wrong_key_rejected():
    priv, pub = ed25519_generate()
    sig = Ed25519Signer(priv).sign(b"data")
    assert Ed25519Verifier(pub).verify(b"DATA", sig) is False        # 篡改数据
    _, other_pub = ed25519_generate()
    assert Ed25519Verifier(other_pub).verify(b"data", sig) is False  # 错误公钥


def test_factories_from_keypair_and_public():
    priv, pub = ed25519_generate()
    kp = KeyPair("sign", SignAlgorithm.ED25519.value, priv, pub, "fp", "v1")
    sig = make_signer(kp).sign(b"m")
    assert make_verifier(pub).verify(b"m", sig) is True
