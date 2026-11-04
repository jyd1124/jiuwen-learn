# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved

"""加解密实现：AES-256-GCM（对称）+ X25519 信封（面向公钥的混合加密）。"""

from __future__ import annotations

import os
from typing import Optional

from . import _primitives as _p
from .interfaces import ICryptoProvider
from .models import DEK_ALGORITHM, SealedMessage


class EnvelopeCryptoProvider(ICryptoProvider):
    """默认加解密 Provider（无状态，密钥按调用传入）。

    - 对称：AES-256-GCM，``key`` 为 32 字节 DEK，密文自带 12 字节 Nonce + Tag。
    - 信封：随机 DEK 加密数据；DEK 用对端 X25519 公钥经 ECDH+HKDF 包裹随密文下发。
    """

    def generate_data_key(self) -> bytes:
        return os.urandom(32)

    def aead_encrypt(
        self, key: bytes, plaintext: bytes, associated_data: Optional[bytes] = None
    ) -> bytes:
        return _p.aead_encrypt(key, plaintext, associated_data)

    def aead_decrypt(
        self, key: bytes, ciphertext: bytes, associated_data: Optional[bytes] = None
    ) -> bytes:
        return _p.aead_decrypt(key, ciphertext, associated_data)

    def seal(self, recipient_public: bytes, plaintext: bytes) -> SealedMessage:
        dek = self.generate_data_key()
        ciphertext = _p.aead_encrypt(dek, plaintext)
        epk, wrapped = _p.wrap_key(recipient_public, dek)
        return SealedMessage(
            epk=epk,
            wrapped_key=wrapped,
            ciphertext=ciphertext,
            dek_algorithm=DEK_ALGORITHM,
        )

    def open(self, recipient_private: bytes, sealed: SealedMessage) -> bytes:
        dek = _p.unwrap_key(recipient_private, sealed.epk, sealed.wrapped_key)
        return _p.aead_decrypt(dek, sealed.ciphertext)
