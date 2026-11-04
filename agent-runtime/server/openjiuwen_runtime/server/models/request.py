# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved

"""Server API Models"""

from pydantic import BaseModel, Field


class DeployAgentRequest(BaseModel):
    """部署 Agent 请求模型"""
    name: str = Field(..., description="部署名称（=包名，用于打包和运行）")
    deployer_type: str = Field(default="local_subprocess", description="部署器类型")
    port: int = Field(default=8090, description="服务端口")


class DeployAgentResponse(BaseModel):
    """部署 Agent 响应模型"""
    deployment_id: str
    type: str
    url: str
    status: str
    port: int


class ErrorResponse(BaseModel):
    """错误响应模型"""
    error: str
    message: str
