# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved

from datetime import datetime
from typing import Optional, Any

from pydantic import BaseModel, Field

from .enums import DeploymentType, DeploymentStatus


DEPLOYMENT_TABLE_NAME = "deployment"


class DeploymentFields:
    """部署表字段名常量"""
    ID = "id"
    DEPLOYMENT_ID = "deployment_id"
    VERSION = "version"
    DEPLOYMENT_TYPE = "deployment_type"
    DEPLOYMENT_STATUS = "deployment_status"
    NAME = "name"
    URL = "url"
    USER_ID = "user_id"
    SPACE_ID = "space_id"
    CREATED_AT = "created_at"
    UPDATED_AT = "updated_at"
    DATA = "data"


class DeploymentCreate(BaseModel):
    """创建部署请求模型"""
    deployment_id: str = Field(..., description="部署ID")
    version: str = Field(..., description="版本号")
    deployment_type: DeploymentType = Field(..., description="部署类型")
    name: str = Field(..., description="部署名称")
    url: Optional[str] = Field(None, description="服务URL")
    user_id: Optional[str] = Field(None, description="用户ID")
    space_id: Optional[str] = Field(None, description="空间ID")
    data: Optional[dict[str, Any]] = Field(None, description="扩展数据")


class DeploymentInfo(BaseModel):
    """部署信息模型"""
    id: int
    deployment_id: str
    version: str
    deployment_type: DeploymentType
    deployment_status: DeploymentStatus
    name: str
    url: Optional[str] = None
    user_id: Optional[str] = None
    space_id: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    data: Optional[dict[str, Any]] = None

    class Config:
        from_attributes = True
