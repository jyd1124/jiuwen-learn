# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved

from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class ColumnDefinition:
    """列定义"""
    name: str
    data_type: str
    primary_key: bool = False
    nullable: bool = True
    default: Any = None
    unique: bool = False
    autoincrement: bool = False
    length: Optional[int] = None


@dataclass
class IndexDefinition:
    """索引定义"""
    columns: list[str]
    unique: bool = False
    name: Optional[str] = None


@dataclass
class TableDefinition:
    """表定义"""
    table_name: str
    columns: list[ColumnDefinition]
    indexes: list[IndexDefinition] = field(default_factory=list)

    def get_primary_key(self) -> Optional[str]:
        """获取主键列名"""
        for col in self.columns:
            if col.primary_key:
                return col.name
        return None

    def get_column_names(self) -> list[str]:
        """获取所有列名"""
        return [col.name for col in self.columns]
