# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved

"""部署策略抽象基类"""

from abc import ABC, abstractmethod
from typing import Any, Optional, Generic, TypeVar
from datetime import datetime
from openjiuwen_runtime.foundation.log import get_logger
from openjiuwen_runtime.foundation.db.handler import DBHandler
from openjiuwen_runtime.foundation.db.table_def import TableDefinition

from .deployer import Deployer
from .models import DeployContext, DeployResult
from ...models.enums import DeploymentStatus
from ...models.schemas import DEPLOYMENT_TABLE_NAME, DeploymentFields

T = TypeVar("T")
logger = get_logger(__name__)


class DeploymentStrategy(ABC):
    """部署策略接口"""

    @abstractmethod
    def get_table_definition(self) -> TableDefinition:
        """获取表定义"""
        pass

    @abstractmethod
    async def create_record(
            self, db_handler: DBHandler, deployment_id: str, version: str, **kwargs: Any
    ) -> Any:
        """创建部署记录"""
        pass

    @abstractmethod
    async def get_record(
            self, db_handler: DBHandler, deployment_id: str
    ) -> Optional[Any]:
        """获取部署记录"""
        pass

    @abstractmethod
    async def delete_record(self, db_handler: DBHandler, deployment_id: str) -> bool:
        """删除部署记录"""
        pass

    @abstractmethod
    async def deploy(self, deployment_id: str, db_handler: DBHandler) -> DeployResult:
        """执行部署"""
        pass

    @abstractmethod
    async def stop(self, deployment_id: str, db_handler: DBHandler) -> DeployResult:
        """停止部署"""
        pass

    @abstractmethod
    async def get_status(
            self, deployment_id: str, db_handler: DBHandler
    ) -> DeploymentStatus:
        """获取状态"""
        pass


class BaseDeploymentStrategy(DeploymentStrategy, Generic[T]):
    """部署策略基类，封装公共逻辑"""

    def __init__(self, deployer: Optional[Deployer] = None):
        self._deployer = deployer or self._create_default_deployer()
        self._table_def = self._create_table_definition()
        logger.debug("Strategy initialized: table=%s", self._table_def.table_name)

    @abstractmethod
    def _create_default_deployer(self) -> Deployer:
        """创建默认部署器"""
        pass

    @abstractmethod
    def _create_table_definition(self) -> TableDefinition:
        """创建表定义"""
        pass

    @abstractmethod
    def _get_info_class(self) -> type:
        """获取 Info 类型"""
        pass

    @abstractmethod
    def _build_deploy_context(self, record: Any, deployment: Any) -> DeployContext:
        """构造部署上下文参数"""
        pass

    @abstractmethod
    def _build_record_data(
            self, deployment_id: str, version: str, **kwargs: Any
    ) -> dict:
        """构造记录数据"""
        pass

    @abstractmethod
    def _get_stop_kwargs(self, record: Any) -> dict:
        """从记录中提取 stop 方法所需的参数"""
        pass

    @abstractmethod
    def _get_status_kwargs(self, record: Any) -> dict:
        """从记录中提取 get_status 方法所需的参数"""
        pass

    def get_table_definition(self) -> TableDefinition:
        return self._table_def

    async def create_record(
            self, db_handler: DBHandler, deployment_id: str, version: str, **kwargs: Any
    ) -> Any:
        logger.debug(
            "Creating record: deployment_id=%s, table=%s",
            deployment_id,
            self._table_def.table_name,
        )
        data = self._build_record_data(deployment_id, version, **kwargs)
        result = await db_handler.create(self._table_def.table_name, data)
        logger.debug("Record created: deployment_id=%s", deployment_id)
        return result

    async def get_record(
            self, db_handler: DBHandler, deployment_id: str
    ) -> Optional[Any]:
        logger.debug(
            "Getting record: deployment_id=%s, table=%s",
            deployment_id,
            self._table_def.table_name,
        )
        return await db_handler.get(
            self._table_def.table_name, {DeploymentFields.DEPLOYMENT_ID: deployment_id}
        )

    async def delete_record(self, db_handler: DBHandler, deployment_id: str) -> bool:
        logger.debug(
            "Deleting record: deployment_id=%s, table=%s",
            deployment_id,
            self._table_def.table_name,
        )
        result = await db_handler.delete(
            self._table_def.table_name, {DeploymentFields.DEPLOYMENT_ID: deployment_id}
        )
        if result:
            logger.debug("Record deleted: deployment_id=%s", deployment_id)
        return result

    async def deploy(self, deployment_id: str, db_handler: DBHandler) -> DeployResult:
        logger.info("Starting deploy: deployment_id=%s", deployment_id)

        record = await self.get_record(db_handler, deployment_id)
        if not record:
            logger.error("Record not found: deployment_id=%s", deployment_id)
            return DeployResult(
                success=False, deployment_id=deployment_id, message=f"Record not found: {deployment_id}"
            )

        deployment = await db_handler.get(
            DEPLOYMENT_TABLE_NAME, 
            {DeploymentFields.DEPLOYMENT_ID: deployment_id}
        )
        if not deployment:
            logger.error("Deployment not found: deployment_id=%s", deployment_id)
            return DeployResult(
                success=False, deployment_id=deployment_id, message=f"Deployment not found: {deployment_id}"
            )

        logger.debug("Updating status to PENDING: deployment_id=%s", deployment_id)
        await db_handler.update(
            DEPLOYMENT_TABLE_NAME,
            {DeploymentFields.DEPLOYMENT_ID: deployment_id},
            {DeploymentFields.DEPLOYMENT_STATUS: DeploymentStatus.PENDING.value},
        )

        ctx = self._build_deploy_context(record, deployment)
        logger.debug("Calling deployer: deployment_id=%s", deployment_id)
        result = await self._deployer.deploy(ctx)

        if result.success:
            update_data = {DeploymentFields.DEPLOYMENT_STATUS: DeploymentStatus.RUNNING.value}
            if result.url:
                update_data[DeploymentFields.URL] = result.url
            await db_handler.update(
                DEPLOYMENT_TABLE_NAME,
                {DeploymentFields.DEPLOYMENT_ID: deployment_id},
                update_data,
            )

            logger.info("Deploy completed: deployment_id=%s, url=%s", deployment_id, result.url)
        else:
            await db_handler.update(
                DEPLOYMENT_TABLE_NAME,
                {DeploymentFields.DEPLOYMENT_ID: deployment_id},
                {DeploymentFields.DEPLOYMENT_STATUS: DeploymentStatus.FAILED.value},
            )
            logger.error(
                "Deploy failed: deployment_id=%s, message=%s",
                deployment_id,
                result.message,
            )

        return result

    async def stop(self, deployment_id: str, db_handler: DBHandler) -> DeployResult:
        logger.info("Stopping deployment: deployment_id=%s", deployment_id)

        record = await self.get_record(db_handler, deployment_id)
        kwargs = {}
        if record:
            kwargs = self._get_stop_kwargs(record)

        result = await self._deployer.stop(deployment_id, **kwargs)

        if result.success:
            await db_handler.update(
                DEPLOYMENT_TABLE_NAME,
                {DeploymentFields.DEPLOYMENT_ID: deployment_id},
                {DeploymentFields.DEPLOYMENT_STATUS: DeploymentStatus.STOPPED.value},
            )
            logger.info("Deployment stopped: deployment_id=%s", deployment_id)
        else:
            logger.error(
                "Failed to stop deployment: deployment_id=%s, message=%s",
                deployment_id,
                result.message,
            )

        return result

    async def get_status(
            self, deployment_id: str, db_handler: DBHandler
    ) -> DeploymentStatus:
        logger.debug("Getting status: deployment_id=%s", deployment_id)

        record = await self.get_record(db_handler, deployment_id)
        kwargs = {}
        if record:
            kwargs = self._get_status_kwargs(record)

        status = await self._deployer.get_status(deployment_id, **kwargs)

        deployment = await db_handler.get(
            DEPLOYMENT_TABLE_NAME, 
            {DeploymentFields.DEPLOYMENT_ID: deployment_id}
        )
        if deployment and deployment.get(DeploymentFields.DEPLOYMENT_STATUS) != status.value:
            if status == DeploymentStatus.STOPPED:
                await db_handler.update(
                    DEPLOYMENT_TABLE_NAME,
                    {DeploymentFields.DEPLOYMENT_ID: deployment_id},
                    {DeploymentFields.DEPLOYMENT_STATUS: DeploymentStatus.STOPPED.value},
                )
                logger.debug("Status updated to STOPPED: deployment_id=%s", deployment_id)

        return status

    async def get_info(self, db_handler: DBHandler, deployment_id: str) -> Optional[T]:
        logger.debug("Getting info: deployment_id=%s", deployment_id)
        record = await self.get_record(db_handler, deployment_id)
        if not record:
            return None
        if hasattr(record, "to_dict"):
            return self._get_info_class().model_validate(record.to_dict())
        return self._get_info_class().model_validate(record)
