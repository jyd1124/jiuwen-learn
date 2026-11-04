# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved

"""部署器抽象基类"""

from abc import ABC, abstractmethod
from typing import Any, Optional, Generic, TypeVar

from .models import DeployContext, DeployResult
from ...models.enums import DeploymentStatus

T = TypeVar("T")


class Deployer(ABC, Generic[T]):
    """部署器抽象基类
    
    泛型参数 T 为特定部署模式的参数类型
    """

    @abstractmethod
    async def deploy(self, ctx: DeployContext[T]) -> DeployResult:
        """
        执行部署
        
        Args:
            ctx: 部署上下文参数
            
        Returns:
            DeployResult: 部署结果
        """
        pass

    @abstractmethod
    async def stop(
        self, 
        deployment_id: str, 
        **kwargs: Any
    ) -> DeployResult:
        """
        停止部署
        
        Args:
            deployment_id: 部署ID
            **kwargs: 特定部署模式的额外参数
            
        Returns:
            DeployResult: 停止结果
        """
        pass

    @abstractmethod
    async def get_status(
        self, 
        deployment_id: str, 
        **kwargs: Any
    ) -> DeploymentStatus:
        """
        获取部署状态
        
        Args:
            deployment_id: 部署ID
            **kwargs: 特定部署模式的额外参数
            
        Returns:
            DeploymentStatus: 部署状态
        """
        pass
