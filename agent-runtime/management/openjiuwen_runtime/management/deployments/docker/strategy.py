# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved

"""Docker 部署策略"""

from datetime import datetime
from typing import Any

from openjiuwen_runtime.foundation.log import get_logger
from openjiuwen_runtime.foundation.log.utils import mask_userdata

from ..base.models import CommonParams, DeployContext
from ..base.strategy import BaseDeploymentStrategy
from .deployer import DockerDeployer
from .models import DOCKER_TABLE_DEF, DockerInfo, DockerParams

logger = get_logger(__name__)


class DockerStrategy(BaseDeploymentStrategy[DockerInfo]):
    """Docker 部署策略"""

    def _create_default_deployer(self) -> Any:
        return DockerDeployer()

    def _create_table_definition(self):
        return DOCKER_TABLE_DEF

    def _get_info_class(self) -> type:
        return DockerInfo

    def _build_record_data(
        self, deployment_id: str, version: str, **kwargs: Any
    ) -> dict:
        now = datetime.utcnow()
        whl_path = kwargs.get("whl_path")
        package_name = kwargs.get("package_name")

        # 如果没有提供 package_name 但有 whl_path，从 whl_path 提取
        if not package_name and whl_path:
            from pathlib import Path
            whl_name = Path(whl_path).stem.split("-")[0].lower().replace("_", "-")
            package_name = whl_name

        return {
            "deployment_id": deployment_id,
            "version": version,
            "host": kwargs.get("host", "localhost"),
            "port": kwargs.get("port"),
            "url": kwargs.get("url"),
            "container_id": kwargs.get("container_id"),
            "container_name": kwargs.get("container_name"),
            "image": kwargs.get("image"),
            "whl_path": whl_path,
            "ir_path": kwargs.get("ir_path"),
            "package_name": package_name,
            "created_at": now,
            "updated_at": now,
            "data": kwargs.get("data"),
        }

    def _build_deploy_context(self, record: Any, deployment: Any) -> DeployContext[DockerParams]:
        if hasattr(record, "to_dict"):
            data = record.to_dict()
        else:
            data = record

        # 从 data 字段中提取 userdata
        record_data = data.get("data") or {}
        userdata = record_data.get("userdata") if isinstance(record_data, dict) else None

        env_vars = {}
        if userdata:
            env_vars["RUNTIME_USERDATA"] = userdata
            logger.info("Using userdata: %s", mask_userdata(userdata))

        return DeployContext(
            common=CommonParams(
                deployment_id=data.get("deployment_id"),
                host=data.get("host"),
                port=data.get("port"),
                url=data.get("url"),
            ),
            params=DockerParams(
                whl_path=data.get("whl_path"),
                ir_path=data.get("ir_path"),
                package_name=data.get("package_name"),
                container_name=data.get("container_name"),
                image=data.get("image"),
                env_vars=env_vars,
                volumes=data.get("volumes"),
                port=data.get("port"),
            ),
            data=data.get("data"),
        )

    def _get_stop_kwargs(self, record: Any) -> dict:
        if hasattr(record, "to_dict"):
            data = record.to_dict()
        else:
            data = record
        return {
            "container_name": data.get("container_name"),
        }

    def _get_status_kwargs(self, record: Any) -> dict:
        if hasattr(record, "to_dict"):
            data = record.to_dict()
        else:
            data = record
        return {
            "container_name": data.get("container_name"),
        }
