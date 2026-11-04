# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved

"""OpenJiuwen Runtime Security SDK（management 层）。

通用安全原语（加解密 / 加签验签 / 链路握手鉴权）已下沉到 foundation
（``openjiuwen_runtime.foundation.security``），供进程级组件零控制面依赖地复用；
本模块保留 **落库** 的密钥/证书管理（``CertificateManager`` 需 ``DBHandler``）与其
指纹固定适配器（``CertificatePinStore`` + 异步 ``verify_and_bind``），并 **re-export**
foundation 的安全符号以保持 ``openjiuwen_runtime.management.security`` 历史导入路径兼容。

示例：
    from openjiuwen_runtime.management.security import (
        EnvelopeCryptoProvider, Ed25519Signer, Ed25519Verifier,
        CertificateManager, server_key_exchange, client_key_exchange, KeyPurpose,
    )
"""

# --- re-export foundation 安全能力（向后兼容历史导入路径）---
from openjiuwen_runtime.foundation.security import (
    DEK_ALGORITHM,
    AuthMode,
    Claims,
    Ed25519Signer,
    Ed25519Verifier,
    EncAlgorithm,
    EnvelopeCryptoProvider,
    ICertificateManager,
    ICryptoProvider,
    InMemoryPinStore,
    IPinStore,
    ISigner,
    IVerifier,
    KeyPair,
    KeyPurpose,
    LINK_TOKEN_HEADER,
    LinkAuthError,
    NonceCache,
    PeerKey,
    SealedMessage,
    SECURITY_LOCAL_KEY_TABLE_DEF,
    SECURITY_PEER_KEY_TABLE_DEF,
    SignAlgorithm,
    VerifyResult,
    build_token,
    build_token_header,
    fingerprint,
    generate_keypair,
    get_auth_mode,
    get_ttl,
    make_signer,
    make_verifier,
    sign_token,
    verify_and_pin,
    verify_signature,
    verify_token,
)

# --- 本层独有：落库密钥/证书管理 + 落库显式绑定的指纹固定 ---
from .certificate import (
    CertificateManager,
    KeyExchange,
    client_key_exchange,
    server_key_exchange,
)
from .certificate_pin_store import CertificatePinStore, verify_and_bind

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
    # link-auth handshake (foundation)
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
    # certificate / key management (management)
    "CertificateManager",
    "KeyExchange",
    "server_key_exchange",
    "client_key_exchange",
    "CertificatePinStore",
    "verify_and_bind",
)
