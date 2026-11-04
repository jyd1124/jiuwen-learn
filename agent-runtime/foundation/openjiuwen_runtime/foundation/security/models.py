# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved

"""安全模块数据模型、枚举与落库表定义。

密钥统一以 32 字节 Raw 编码在内存中传递，落库时 base64 文本化。
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from openjiuwen_runtime.foundation.db.table_def import (
    ColumnDefinition,
    IndexDefinition,
    TableDefinition,
)


class KeyPurpose(str, Enum):
    """密钥用途。"""

    SIGN = "sign"        # Ed25519 加签/验签
    ENCRYPT = "encrypt"  # X25519 信封加解密


class SignAlgorithm(str, Enum):
    ED25519 = "Ed25519"


class EncAlgorithm(str, Enum):
    X25519 = "X25519"


# 信封加密算法标识（X25519 协商 + HKDF-SHA256 派生 + AES-256-GCM 加密）。
DEK_ALGORITHM = "X25519-HKDF-SHA256+AES-256-GCM"


@dataclass(frozen=True)
class KeyPair:
    """本端密钥对（私钥永不外发）。"""

    purpose: str
    algorithm: str
    private_raw: bytes
    public_raw: bytes
    fingerprint: str
    key_version: str = "v1"


@dataclass(frozen=True)
class PeerKey:
    """对端公钥（握手录入、落库保存）。"""

    peer_id: str
    purpose: str
    algorithm: str
    public_raw: bytes
    fingerprint: str
    key_version: str = "v1"


@dataclass(frozen=True)
class SealedMessage:
    """信封加密结果：临时公钥 + 被包裹的数据密钥 + 密文。"""

    epk: bytes           # 临时 X25519 公钥
    wrapped_key: bytes   # 被 KEK 包裹的 DEK（nonce|ciphertext）
    ciphertext: bytes    # AES-256-GCM(DEK, plaintext)（nonce|ciphertext）
    dek_algorithm: str = DEK_ALGORITHM


# ===== 落库表（基于 foundation DBHandler，新增独立表，不影响既有表） =====

# 本端密钥对：一个节点每种用途一对，主键即用途。
SECURITY_LOCAL_KEY_TABLE_DEF = TableDefinition(
    table_name="security_local_key",
    columns=[
        ColumnDefinition("purpose", "string", length=32, primary_key=True, nullable=False),
        ColumnDefinition("algorithm", "string", length=32, nullable=False),
        ColumnDefinition("private_key", "string", length=512, nullable=False),
        ColumnDefinition("public_key", "string", length=256, nullable=False),
        ColumnDefinition("fingerprint", "string", length=128, nullable=False),
        ColumnDefinition("key_version", "string", length=32, nullable=False, default="v1"),
        ColumnDefinition("created_at", "datetime", nullable=False),
        ColumnDefinition("updated_at", "datetime", nullable=False),
    ],
    indexes=[],
)

# 对端公钥：按 (peer_id, purpose) 唯一，合成主键 record_id = "<peer_id>::<purpose>"。
SECURITY_PEER_KEY_TABLE_DEF = TableDefinition(
    table_name="security_peer_key",
    columns=[
        ColumnDefinition("record_id", "string", length=160, primary_key=True, nullable=False),
        ColumnDefinition("peer_id", "string", length=128, nullable=False),
        ColumnDefinition("purpose", "string", length=32, nullable=False),
        ColumnDefinition("algorithm", "string", length=32, nullable=False),
        ColumnDefinition("public_key", "string", length=256, nullable=False),
        ColumnDefinition("fingerprint", "string", length=128, nullable=False),
        ColumnDefinition("key_version", "string", length=32, nullable=False, default="v1"),
        ColumnDefinition("status", "string", length=32, nullable=False, default="bound"),
        ColumnDefinition("created_at", "datetime", nullable=False),
        ColumnDefinition("updated_at", "datetime", nullable=False),
    ],
    indexes=[
        IndexDefinition(["peer_id"], unique=False),
        IndexDefinition(["purpose"], unique=False),
    ],
)
