# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved

"""Subprocess 部署策略"""

from datetime import datetime
from typing import Any

from ..base.strategy import BaseDeploymentStrategy
from ..base.models import DeployContext, CommonParams
from .models import SubprocessParams, ProcessInfo, PROCESS_TABLE_DEF
from .deployer import LocalSubprocessDeployer


class SubprocessStrategy(BaseDeploymentStrategy[ProcessInfo]):
    """本地进程部署策略"""

    def _create_default_deployer(self) -> Any:
        return LocalSubprocessDeployer()

    def _create_table_definition(self):
        return PROCESS_TABLE_DEF

    def _get_info_class(self) -> type:
        return ProcessInfo

    def _build_record_data(
        self, deployment_id: str, version: str, **kwargs: Any
    ) -> dict:
        now = datetime.utcnow()
        return {
            "deployment_id": deployment_id,
            "version": version,
            "host": kwargs.get("host", "localhost"),
            "port": kwargs.get("port"),
            "url": kwargs.get("url"),
            "pid": kwargs.get("pid"),
            "whl_path": kwargs.get("whl_path"),
            "ir_path": kwargs.get("ir_path"),
            "package_name": kwargs.get("package_name"),
            "venv_path": kwargs.get("venv_path"),
            "created_at": now,
            "updated_at": now,
            "data": kwargs.get("data"),
        }

    def _build_deploy_context(self, record: Any, deployment: Any) -> DeployContext[SubprocessParams]:
        if hasattr(record, "to_dict"):
            data = record.to_dict()
        else:
            data = record
        # 从 data 字段中提取 userdata
        record_data = data.get("data") or {}
        userdata = record_data.get("userdata") if isinstance(record_data, dict) else None
        return DeployContext(
            common=CommonParams(
                deployment_id=data.get("deployment_id"),
                host=data.get("host"),
                port=data.get("port"),
                url=data.get("url"),
            ),
            params=SubprocessParams(
                whl_path=data.get("whl_path"),
                ir_path=data.get("ir_path"),
                package_name=data.get("package_name"),
                venv_path=data.get("venv_path"),
                userdata=userdata,
            ),
            data=data.get("data"),
        )

    async def deploy(self, deployment_id: str, db_handler) -> Any:
        """部署并更新 pid 和 venv_path 到子表"""
        result = await super().deploy(deployment_id, db_handler)

        # 部署成功后，更新子表记录的 pid 和 venv_path
        if result.success and (result.pid is not None or result.venv_path):
            update_data = {}
            if result.pid is not None:
                update_data["pid"] = result.pid
            if result.venv_path:
                update_data["venv_path"] = result.venv_path

            await db_handler.update(
                self._table_def.table_name,
                {"deployment_id": deployment_id},
                update_data,
            )

        return result

    def _get_stop_kwargs(self, record: Any) -> dict:
        if hasattr(record, "to_dict"):
            data = record.to_dict()
        else:
            data = record
        return {
            "pid": data.get("pid"),
            "venv_path": data.get("venv_path"),
        }

    def _get_status_kwargs(self, record: Any) -> dict:
        if hasattr(record, "to_dict"):
            data = record.to_dict()
        else:
            data = record
        return {
            "pid": data.get("pid"),
        }
