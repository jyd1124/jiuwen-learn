# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved

"""OpenJiuwen Runtime 基础安全能力（foundation 层，谁都可轻量依赖）。

放在 foundation 而非 management，是为了让**进程级**组件（如独立 AgentServer）也能
零控制面依赖地使用：

- 密码学原语：Ed25519（签验）、X25519+HKDF 信封、AES-256-GCM（``_primitives``）。
- 加解密 / 加签验签：``EnvelopeCryptoProvider`` / ``Ed25519Signer`` / ``Ed25519Verifier``。
- 控制链路握手鉴权：``link_auth``（一次性令牌 + nonce 防重放 + 指纹固定，
  off/observe/enforce 三档开关）。

DB 落库的密钥/证书管理（``CertificateManager``）在 management 层
（``openjiuwen_runtime.management.security``），需要 ``DBHandler``。
"""

from .interfaces import (
    ICertificateManager,
    ICryptoProvider,
    ISigner,
    IVerifier,
)
from .models import (
    DEK_ALGORITHM,
    EncAlgorithm,
    KeyPair,
    KeyPurpose,
    PeerKey,
    SealedMessage,
    SECURITY_LOCAL_KEY_TABLE_DEF,
    SECURITY_PEER_KEY_TABLE_DEF,
    SignAlgorithm,
)
from .crypto import EnvelopeCryptoProvider
from .signing import Ed25519Signer, Ed25519Verifier, make_signer, make_verifier
from .link_auth import (
    AuthMode,
    Claims,
    InMemoryPinStore,
    IPinStore,
    LINK_TOKEN_HEADER,
    LinkAuthError,
    NonceCache,
    VerifyResult,
    build_token,
    build_token_header,
    generate_keypair,
    get_auth_mode,
    get_ttl,
    fingerprint,
    sign_token,
    verify_and_pin,
    verify_signature,
    verify_token,
)

__all__ = (
    # interfaces
    "ICryptoProvider",
    "ISigner",
    "IVerifier",
    "ICertificateManager",
    # models
    "KeyPurpose",
    "SignAlgorithm",
    "EncAlgorithm",
    "DEK_ALGORITHM",
    "KeyPair",
    "PeerKey",
    "SealedMessage",
    "SECURITY_LOCAL_KEY_TABLE_DEF",
    "SECURITY_PEER_KEY_TABLE_DEF",
    # crypto
    "EnvelopeCryptoProvider",
    # signing
    "Ed25519Signer",
    "Ed25519Verifier",
    "make_signer",
    "make_verifier",
    # link-auth handshake
    "LINK_TOKEN_HEADER",
    "AuthMode",
    "LinkAuthError",
    "Claims",
    "VerifyResult",
    "NonceCache",
    "IPinStore",
    "InMemoryPinStore",
    "get_auth_mode",
    "get_ttl",
    "generate_keypair",
    "fingerprint",
    "sign_token",
    "verify_signature",
    "verify_token",
    "verify_and_pin",
    "build_token",
    "build_token_header",
)
