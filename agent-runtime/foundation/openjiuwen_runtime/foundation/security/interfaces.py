# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved

"""安全模块通用接口（加解密 / 加签 / 验签 / 证书管理）。

实现可插拔：默认提供 Ed25519 + X25519/AES-256-GCM 实现，调用方仅依赖接口。
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional

from .models import KeyPair, PeerKey, SealedMessage


class ISigner(ABC):
    """加签接口：持本端私钥，对任意字节签名。"""

    @property
    @abstractmethod
    def algorithm(self) -> str:
        ...

    @abstractmethod
    def sign(self, data: bytes) -> bytes:
        ...


class IVerifier(ABC):
    """验签接口：持对端公钥，校验签名。"""

    @property
    @abstractmethod
    def algorithm(self) -> str:
        ...

    @abstractmethod
    def verify(self, data: bytes, signature: bytes) -> bool:
        ...


class ICryptoProvider(ABC):
    """加解密接口：对称 AEAD + 面向公钥的信封加密（混合加密）。

    - ``generate_data_key`` / ``aead_*``：对称 AES-256-GCM，加密大数据用。
    - ``seal`` / ``open``：信封加密——随机 DEK 加密数据、DEK 用对端公钥包裹；
      解密方用自身私钥解包 DEK 再解数据。非对称只解决密钥分发，数据仍对称加密。
    """

    @abstractmethod
    def generate_data_key(self) -> bytes:
        ...

    @abstractmethod
    def aead_encrypt(
        self, key: bytes, plaintext: bytes, associated_data: Optional[bytes] = None
    ) -> bytes:
        ...

    @abstractmethod
    def aead_decrypt(
        self, key: bytes, ciphertext: bytes, associated_data: Optional[bytes] = None
    ) -> bytes:
        ...

    @abstractmethod
    def seal(self, recipient_public: bytes, plaintext: bytes) -> SealedMessage:
        ...

    @abstractmethod
    def open(self, recipient_private: bytes, sealed: SealedMessage) -> bytes:
        ...


class ICertificateManager(ABC):
    """证书/密钥管理接口：密钥与证书的录入、保存、读取、删除（client/server 通用）。

    职责拆分：
    - **本端**（self）：按用途持有自己的密钥对，私钥落库受保护、永不外发；
      可导出公钥用于握手交换。
    - **对端**（peer）：录入并保存对端公钥（=确认配对），按需读取、解绑删除。

    服务端 / 客户端用法（见模块文档）：
    - 服务端（如 Manager）：本端用途 ``sign``（签发配置）；保存各客户端的
      ``encrypt`` 公钥（给它们加密下发）。
    - 客户端（如 Gateway）：本端用途 ``encrypt``（解密下发）；保存服务端的
      ``sign`` 公钥（验签）。
    """

    # ---- 本端密钥对 ----
    @abstractmethod
    async def get_or_create_keypair(self, purpose: str) -> KeyPair:
        ...

    @abstractmethod
    async def export_public(self, purpose: str) -> bytes:
        ...

    # ---- 对端公钥：录入 / 读取 / 删除 ----
    @abstractmethod
    async def save_peer_key(
        self,
        peer_id: str,
        purpose: str,
        public_key: bytes,
        *,
        algorithm: str = "",
        key_version: str = "v1",
    ) -> str:
        """录入并保存对端公钥，返回其指纹（用于确认配对）。"""
        ...

    @abstractmethod
    async def load_peer_key(self, peer_id: str, purpose: str) -> Optional[PeerKey]:
        ...

    @abstractmethod
    async def delete_peer_key(self, peer_id: str, purpose: Optional[str] = None) -> None:
        ...

    @abstractmethod
    async def list_peer_keys(self, *, purpose: Optional[str] = None) -> list[PeerKey]:
        ...
