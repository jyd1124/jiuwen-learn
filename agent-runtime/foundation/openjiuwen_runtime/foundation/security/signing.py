# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved

"""加签验签实现：Ed25519。"""

from __future__ import annotations

from . import _primitives as _p
from .interfaces import ISigner, IVerifier
from .models import KeyPair, SignAlgorithm

_ALG = SignAlgorithm.ED25519.value


class Ed25519Signer(ISigner):
    """持本端 Ed25519 私钥，对字节签名。"""

    def __init__(self, private_raw: bytes, key_version: str = "v1") -> None:
        self._priv = private_raw
        self.key_version = key_version

    @property
    def algorithm(self) -> str:
        return _ALG

    def sign(self, data: bytes) -> bytes:
        return _p.ed25519_sign(self._priv, data)


class Ed25519Verifier(IVerifier):
    """持对端 Ed25519 公钥，校验签名（失败统一返回 False，fail-closed）。"""

    def __init__(self, public_raw: bytes) -> None:
        self._pub = public_raw

    @property
    def algorithm(self) -> str:
        return _ALG

    def verify(self, data: bytes, signature: bytes) -> bool:
        return _p.ed25519_verify(self._pub, data, signature)


def make_signer(keypair: KeyPair) -> ISigner:
    """由本端密钥对构造签名器。"""
    return Ed25519Signer(keypair.private_raw, keypair.key_version)


def make_verifier(public_raw: bytes) -> IVerifier:
    """由对端公钥构造验签器。"""
    return Ed25519Verifier(public_raw)
