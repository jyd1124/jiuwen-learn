# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved

"""Docker 部署模块数据模型"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Any

from pydantic import BaseModel, Field

from openjiuwen_runtime.foundation.db.table_def import TableDefinition, ColumnDefinition, IndexDefinition


@dataclass
class DockerParams:
    """Docker 专用参数"""
    image: Optional[str] = None
    container_name: Optional[str] = None
    env_vars: Optional[dict[str, str]] = None
    volumes: Optional[list[dict[str, str]]] = None
    whl_path: Optional[str] = None
    ir_path: Optional[str] = None
    package_name: Optional[str] = None
    port: Optional[int] = None


class DockerInfo(BaseModel):
    """Docker 部署信息模型"""
    id: int
    deployment_id: str
    version: str
    host: str
    port: Optional[int] = None
    url: Optional[str] = None
    container_id: Optional[str] = None
    container_name: Optional[str] = None
    image: Optional[str] = None
    whl_path: Optional[str] = None
    ir_path: Optional[str] = None
    package_name: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    data: Optional[dict[str, Any]] = None

    class Config:
        from_attributes = True


class DockerCreate(BaseModel):
    """创建 Docker 部署请求模型"""
    deployment_id: str = Field(..., description="部署ID")
    version: str = Field(..., description="版本号")
    host: str = Field(..., description="主机地址")
    port: Optional[int] = Field(None, description="端口")
    url: Optional[str] = Field(None, description="服务URL")
    container_id: Optional[str] = Field(None, description="容器ID")
    container_name: Optional[str] = Field(None, description="容器名称")
    image: Optional[str] = Field(None, description="镜像名称")
    whl_path: Optional[str] = Field(None, description="WHL包路径")
    ir_path: Optional[str] = Field(None, description="IR文件路径")
    package_name: Optional[str] = Field(None, description="包名称")
    data: Optional[dict[str, Any]] = Field(None, description="扩展数据")


DOCKER_TABLE_DEF = TableDefinition(
    table_name="docker",
    columns=[
        ColumnDefinition("id", "integer", primary_key=True, autoincrement=True),
        ColumnDefinition("deployment_id", "string", length=64, unique=True, nullable=False),
        ColumnDefinition("version", "string", length=32, nullable=False),
        ColumnDefinition("host", "string", length=255, nullable=False),
        ColumnDefinition("port", "integer", nullable=True),
        ColumnDefinition("url", "string", length=512, nullable=True),
        ColumnDefinition("container_id", "string", length=128, nullable=True),
        ColumnDefinition("container_name", "string", length=255, nullable=True),
        ColumnDefinition("image", "string", length=512, nullable=True),
        ColumnDefinition("whl_path", "string", length=512, nullable=True),
        ColumnDefinition("ir_path", "string", length=512, nullable=True),
        ColumnDefinition("package_name", "string", length=255, nullable=True),
        ColumnDefinition("created_at", "datetime", nullable=False),
        ColumnDefinition("updated_at", "datetime", nullable=False),
        ColumnDefinition("data", "json", nullable=True),
    ],
    indexes=[
        IndexDefinition(["deployment_id"], unique=True),
        IndexDefinition(["container_id"], unique=False),
        IndexDefinition(["container_name"], unique=False),
    ],
)
