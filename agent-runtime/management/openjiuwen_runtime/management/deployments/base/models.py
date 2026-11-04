# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved

"""部署模块基础数据结构"""

from dataclasses import dataclass, field
from typing import Optional, Any, Generic, TypeVar
from datetime import datetime

T = TypeVar("T")


@dataclass
class CommonParams:
    """共用参数 - 所有部署模式都需要"""
    deployment_id: str
    host: Optional[str] = field(default="127.0.0.1")
    port: Optional[int] = None
    url: Optional[str] = None


@dataclass
class DeployContext(Generic[T]):
    """部署上下文参数
    
    泛型参数 T 为特定部署模式的参数类型
    """
    common: CommonParams
    params: Optional[T] = None
    data: Optional[dict[str, Any]] = field(default_factory=dict)

    @property
    def deployment_id(self) -> str:
        return self.common.deployment_id

    @property
    def host(self) -> str:
        return self.common.host

    @property
    def port(self) -> Optional[int]:
        return self.common.port

    @property
    def url(self) -> Optional[str]:
        return self.common.url


@dataclass
class DeployResult:
    """部署结果"""
    success: bool
    deployment_id: str
    message: Optional[str] = None
    url: Optional[str] = None
    pid: Optional[int] = None
    venv_path: Optional[str] = None
