# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved

"""非对称密码学原语（内部模块）。

- Ed25519：签名/验签。
- X25519 + HKDF-SHA256：信封加密的密钥协商与 KEK 派生。
- AES-256-GCM：数据加密。
"""

from __future__ import annotations

import base64
import hashlib
import os

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)
from cryptography.hazmat.primitives.asymmetric.x25519 import (
    X25519PrivateKey,
    X25519PublicKey,
)
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.hkdf import HKDF

_RAW = serialization.Encoding.Raw
_PRIV_RAW = serialization.PrivateFormat.Raw
_PUB_RAW = serialization.PublicFormat.Raw
_NOENC = serialization.NoEncryption()
_HKDF_INFO = b"openjiuwen-runtime-security-dek-v1"


def b64e(raw: bytes) -> str:
    return base64.b64encode(raw).decode("ascii")


def b64d(text: str) -> bytes:
    return base64.b64decode(text)


def fingerprint(public_raw: bytes) -> str:
    """公钥 SHA-256 指纹（hex），用于确认配对与轮换比对。"""
    return hashlib.sha256(public_raw).hexdigest()


# ---------- Ed25519 ----------

def ed25519_generate() -> tuple[bytes, bytes]:
    sk = Ed25519PrivateKey.generate()
    return (
        sk.private_bytes(_RAW, _PRIV_RAW, _NOENC),
        sk.public_key().public_bytes(_RAW, _PUB_RAW),
    )


def ed25519_sign(private_raw: bytes, data: bytes) -> bytes:
    return Ed25519PrivateKey.from_private_bytes(private_raw).sign(data)


def ed25519_verify(public_raw: bytes, data: bytes, signature: bytes) -> bool:
    try:
        Ed25519PublicKey.from_public_bytes(public_raw).verify(signature, data)
        return True
    except (InvalidSignature, ValueError):
        return False


# ---------- X25519 信封 ----------

def x25519_generate() -> tuple[bytes, bytes]:
    sk = X25519PrivateKey.generate()
    return (
        sk.private_bytes(_RAW, _PRIV_RAW, _NOENC),
        sk.public_key().public_bytes(_RAW, _PUB_RAW),
    )


def _derive_kek(shared: bytes) -> bytes:
    return HKDF(algorithm=hashes.SHA256(), length=32, salt=None, info=_HKDF_INFO).derive(shared)


def wrap_key(peer_public_raw: bytes, key: bytes) -> tuple[bytes, bytes]:
    """用对端 X25519 公钥包裹 ``key``，返回（临时公钥 epk, wrapped=nonce|ct）。"""
    esk = X25519PrivateKey.generate()
    epk = esk.public_key().public_bytes(_RAW, _PUB_RAW)
    shared = esk.exchange(X25519PublicKey.from_public_bytes(peer_public_raw))
    kek = _derive_kek(shared)
    nonce = os.urandom(12)
    return epk, nonce + AESGCM(kek).encrypt(nonce, key, None)


def unwrap_key(private_raw: bytes, epk_raw: bytes, wrapped: bytes) -> bytes:
    """用自身 X25519 私钥与临时公钥还原被包裹的 ``key``。"""
    sk = X25519PrivateKey.from_private_bytes(private_raw)
    shared = sk.exchange(X25519PublicKey.from_public_bytes(epk_raw))
    kek = _derive_kek(shared)
    nonce, ct = wrapped[:12], wrapped[12:]
    return AESGCM(kek).decrypt(nonce, ct, None)


# ---------- AES-256-GCM ----------

def aead_encrypt(key: bytes, plaintext: bytes, associated_data: bytes | None = None) -> bytes:
    nonce = os.urandom(12)
    return nonce + AESGCM(key).encrypt(nonce, plaintext, associated_data)


def aead_decrypt(key: bytes, ciphertext: bytes, associated_data: bytes | None = None) -> bytes:
    nonce, ct = ciphertext[:12], ciphertext[12:]
    return AESGCM(key).decrypt(nonce, ct, associated_data)
