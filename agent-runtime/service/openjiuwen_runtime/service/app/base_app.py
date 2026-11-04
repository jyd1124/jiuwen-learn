# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved

"""
BaseApp - AgentApp 和 PluginApp 的基类

提供通用功能:
- FastAPI 应用管理
- 生命周期管理 (@init, @shutdown 装饰器)
- 健康检查端点 (/health)
- 运行方法 (uvicorn 服务器)
"""
import argparse
import os
import sys
from pathlib import Path
from typing import Callable, Optional, Dict, Any
from fastapi import FastAPI
import uvicorn

from openjiuwen_runtime.foundation.log import get_logger

logger = get_logger(__name__)


def _setup_runtime_env(kwargs: Dict[str, Any]) -> None:
    """
    从 kwargs 中提取运行时环境变量配置并设置环境变量。

    参数:
        kwargs: 传入的额外参数字典
    """
    userdata = kwargs.pop("userdata", None)
    if userdata is not None:
        os.environ["RUNTIME_USERDATA"] = str(userdata)
    # 如果 kwargs 中未传入，保留已有的环境变量（如由父进程注入）

    # 处理 file 参数，设置环境变量
    file_path = kwargs.pop("file", None)
    if file_path is not None:
        os.environ["RUNTIME_IR_PATH"] = str(file_path)


def _parse_cli_args() -> Dict[str, Any]:
    """
    解析命令行参数。

    返回:
        包含 host, port, userdata, irpath(可选) 的字典
    """
    parser = argparse.ArgumentParser(
        description=f"Run {os.path.basename(sys.argv[0])}"
    )
    parser.add_argument("--host", type=str, default="0.0.0.0", help="监听地址 (默认: 0.0.0.0)")
    parser.add_argument("--port", "-p", type=int, default=8090, help="监听端口 (默认: 8090)")
    parser.add_argument("--userdata", "-u", type=str, default=None, help="用户数据，设置环境变量 RUNTIME_USERDATA")
    parser.add_argument("--irpath", "-i", type=str, default=None, help="IR 配置文件路径")

    args = parser.parse_args()
    result = {"host": args.host, "port": args.port}
    if args.userdata:
        result["userdata"] = args.userdata
    if args.irpath:
        file_path = str(Path(args.irpath).resolve())
        if not Path(args.irpath).exists():
            logger.error("配置文件不存在: %s", file_path)
            parser.error(f"配置文件不存在: {file_path}")
        result["file"] = file_path

    return result


class BaseApp:
    """
    openjiuwen 应用基类

    为 AgentApp 和 PluginApp 提供通用功能:
    - FastAPI 应用管理
    - 生命周期管理 (@init, @shutdown)
    - 健康检查 (/health)
    - 运行方法 (run)
    """

    def __init__(
        self,
        app_name: str,
        app_description: str = "",
        version: str = "1.0.0",
    ):
        self.app_name = app_name
        self.app_description = app_description
        self.version = version

        # FastAPI 应用
        self.app = FastAPI(
            title=self.app_name,
            description=self.app_description,
            version=self.version,
        )

        # 生命周期钩子
        self._init_hook: Optional[Callable] = None
        self._shutdown_hook: Optional[Callable] = None

        # 注册基础路由
        self._register_base_routes()

    async def run_init_hook(self) -> bool:
        """执行通过 @init 注册的钩子（供 AppGroup 等外部容器调用）。返回是否实际执行了钩子。"""
        if self._init_hook:
            await self._init_hook()
            return True
        return False

    async def run_shutdown_hook(self) -> bool:
        """执行通过 @shutdown 注册的钩子（供 AppGroup 等外部容器调用）。返回是否实际执行了钩子。"""
        if self._shutdown_hook:
            await self._shutdown_hook()
            return True
        return False

    def init(self, func: Callable) -> Callable:
        """
        初始化钩子装饰器

        用法:
            @app.init
            async def init():
                # 初始化代码
                pass
        """
        self._init_hook = func
        return func

    def shutdown(self, func: Callable) -> Callable:
        """
        清理钩子装饰器

        用法:
            @app.shutdown
            async def shutdown():
                # 清理代码
                pass
        """
        self._shutdown_hook = func
        return func

    def _register_base_routes(self):
        """注册基础路由 (/health, 生命周期事件)"""

        @self.app.on_event("startup")
        async def startup():
            # 执行用户初始化钩子
            if self._init_hook:
                await self._init_hook()

        @self.app.on_event("shutdown")
        async def shutdown():
            # 执行用户清理钩子
            if self._shutdown_hook:
                await self._shutdown_hook()

        @self.app.get("/health")
        async def health():
            """健康检查端点"""
            return {
                "status": "healthy",
                "app": self.app_name,
                "version": self.version,
            }

    def run(self, host: str = None, port: int = None, **kwargs):
        """
        运行应用，自动从命令行解析参数。

        命令行参数:
            --host: 监听地址 (默认: 0.0.0.0)
            --port, -p: 监听端口 (默认: 8090)
            --userdata, -u: 用户数据，设置环境变量 RUNTIME_USERDATA
            --irpath, -i: IR 配置文件路径 (可选)

        参数:
            host: 绑定主机，命令行参数优先
            port: 绑定端口，命令行参数优先
            userdata: 用户数据，会设置为环境变量 RUNTIME_USERDATA
            **kwargs: 传递给 uvicorn.run() 的额外参数
        """
        # 解析命令行参数
        cli_args = _parse_cli_args()

        # 命令行参数优先，然后是传入参数，最后是默认值
        if host is None:
            host = cli_args.get("host", "0.0.0.0")
        if port is None:
            port = cli_args.get("port", 8090)
        if "userdata" in cli_args:
            kwargs["userdata"] = cli_args["userdata"]
        if "file" in cli_args:
            kwargs["file"] = cli_args["file"]

        _setup_runtime_env(kwargs)

        logger.info("Starting %s v%s on http://%s:%s", self.app_name, self.version, host, port)
        uvicorn.run(self.app, host=host, port=port, **kwargs)
