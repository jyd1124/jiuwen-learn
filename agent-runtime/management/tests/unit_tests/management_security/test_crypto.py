# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved

"""加解密（AES-256-GCM + X25519 信封）单元测试。"""

from __future__ import annotations

import pytest
from cryptography.exceptions import InvalidTag

from openjiuwen_runtime.management.security import EnvelopeCryptoProvider, SealedMessage
from openjiuwen_runtime.foundation.security._primitives import x25519_generate


def test_envelope_seal_open_roundtrip():
    crypto = EnvelopeCryptoProvider()
    priv, pub = x25519_generate()
    sealed = crypto.seal(pub, b"kubeconfig-SECRET")
    assert isinstance(sealed, SealedMessage)
    # 密文里不含明文
    assert b"kubeconfig-SECRET" not in sealed.ciphertext
    assert crypto.open(priv, sealed) == b"kubeconfig-SECRET"


def test_envelope_wrong_recipient_cannot_open():
    crypto = EnvelopeCryptoProvider()
    _, pub = x25519_generate()
    sealed = crypto.seal(pub, b"data")
    wrong_priv, _ = x25519_generate()
    with pytest.raises(InvalidTag):
        crypto.open(wrong_priv, sealed)


def test_aead_roundtrip_and_tamper():
    crypto = EnvelopeCryptoProvider()
    key = crypto.generate_data_key()
    assert len(key) == 32
    ct = crypto.aead_encrypt(key, b"hello", associated_data=b"aad")
    assert crypto.aead_decrypt(key, ct, associated_data=b"aad") == b"hello"
    # 错误 AAD / 篡改密文 → 解密失败
    with pytest.raises(InvalidTag):
        crypto.aead_decrypt(key, ct, associated_data=b"other")
    with pytest.raises(InvalidTag):
        crypto.aead_decrypt(key, ct[:-1] + bytes([ct[-1] ^ 1]), associated_data=b"aad")


def test_each_seal_uses_fresh_dek():
    crypto = EnvelopeCryptoProvider()
    _, pub = x25519_generate()
    a = crypto.seal(pub, b"x")
    b = crypto.seal(pub, b"x")
    # 每次随机 DEK + 临时公钥 → 密文/封装均不同
    assert a.ciphertext != b.ciphertext and a.epk != b.epk
