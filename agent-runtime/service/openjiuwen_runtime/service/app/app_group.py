# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved

"""
AppGroup - 多应用容器类

提供:
- 挂载多个 AgentApp/PluginApp 到同一 FastAPI 服务
- 通过 URL 路径前缀区分不同应用
- 聚合生命周期管理 (init, shutdown)
- 聚合健康检查
"""

from typing import Dict
from fastapi import FastAPI
import uvicorn

from openjiuwen_runtime.foundation.log import get_logger

from .base_app import BaseApp, _setup_runtime_env, _parse_cli_args
from .agent_app import AgentApp
from .plugin_app import PluginApp

logger = get_logger(__name__)


class AppGroup:
    """
    多应用容器，允许多个 AgentApp/PluginApp 共享一个 FastAPI 服务。

    设计原则:
    - 单一 FastAPI 服务托管多个应用
    - 通过路径前缀区分应用 (如 /customer/query)
    - 聚合生命周期钩子执行
    - 聚合健康检查

    用法:
        group = AppGroup("MyAgentGroup")

        customer_app = AgentApp("CustomerAgent")
        @customer_app.query
        async def query(msgs, request): ...

        assistant_app = AgentApp("AssistantAgent")
        @assistant_app.query
        async def query(msgs, request): ...

        group.mount("/customer", customer_app)
        group.mount("/assistant", assistant_app)
        group.run(port=8090)
    """

    def __init__(
        self,
        group_name: str,
        description: str = "",
        version: str = "1.0.0",
    ):
        self.group_name = group_name
        self.description = description
        self.version = version

        # 主 FastAPI 应用
        self.app = FastAPI(
            title=group_name,
            description=description,
            version=version,
        )

        # 已挂载的应用注册表: prefix -> BaseApp
        self._mounted_apps: Dict[str, BaseApp] = {}

        # 路由是否已注册标记
        self._routes_registered = False

    def mount(self, prefix: str, app: BaseApp) -> "AppGroup":
        """
        挂载应用到指定路径前缀。

        参数:
            prefix: URL 路径前缀 (如 "/customer", "/assistant")
            app: BaseApp 实例 (AgentApp 或 PluginApp)

        返回:
            Self，支持链式调用

        示例:
            group.mount("/customer", customer_app).mount("/assistant", assistant_app)
        """
        # 规范化前缀
        if not prefix.startswith("/"):
            prefix = f"/{prefix}"

        # 移除末尾斜杠保持一致性
        prefix = prefix.rstrip("/")

        # 检查重复前缀
        if prefix in self._mounted_apps:
            raise ValueError(f"前缀 '{prefix}' 已被挂载")

        # 存储应用引用
        self._mounted_apps[prefix] = app

        # 挂载应用的 FastAPI 实例
        self.app.mount(prefix, app.app)

        return self

    def _register_lifecycle_events(self):
        """注册聚合的启动和关闭事件"""

        @self.app.on_event("startup")
        async def startup():
            """按顺序执行所有挂载应用的 init 钩子"""
            for prefix, app in self._mounted_apps.items():
                try:
                    if await app.run_init_hook():
                        logger.info(
                            "[OK] %s 初始化完成 (挂载于 %s)",
                            app.app_name,
                            prefix,
                        )
                except Exception as e:
                    logger.error(
                        "[ERROR] %s 初始化失败: %s",
                        app.app_name,
                        e,
                        exc_info=True,
                    )
                    raise

        @self.app.on_event("shutdown")
        async def shutdown():
            """按逆序执行所有挂载应用的 shutdown 钩子"""
            for prefix, app in reversed(list(self._mounted_apps.items())):
                try:
                    if await app.run_shutdown_hook():
                        logger.info("[OK] %s 关闭完成", app.app_name)
                except Exception as e:
                    logger.warning(
                        "[WARN] %s 关闭钩子执行失败: %s",
                        app.app_name,
                        e,
                        exc_info=True,
                    )

    def _register_routes(self):
        """注册 AppGroup 特定路由"""

        @self.app.get("/health")
        async def health():
            """
            聚合健康检查，返回所有挂载应用的状态。
            """
            apps_status = {}
            overall_healthy = True

            for prefix, app in self._mounted_apps.items():
                app_info = {
                    "name": app.app_name,
                    "version": app.version,
                    "mount_path": prefix,
                }

                # 根据应用类型添加特定信息
                if isinstance(app, AgentApp):
                    app_info["type"] = "agent"
                    app_info["agent_loaded"] = app.agent is not None
                    if app.agent is None:
                        overall_healthy = False

                elif isinstance(app, PluginApp):
                    app_info["type"] = "plugin"
                    app_info["tools_count"] = app.tools_count

                else:
                    app_info["type"] = "base"

                apps_status[prefix] = app_info

            return {
                "status": "healthy" if overall_healthy else "degraded",
                "group": self.group_name,
                "version": self.version,
                "apps_count": len(self._mounted_apps),
                "apps": apps_status,
            }

        @self.app.get("/")
        async def root():
            """根端点，列出所有挂载的应用"""
            return {
                "group": self.group_name,
                "version": self.version,
                "mounted_apps": [
                    {"prefix": prefix, "name": app.app_name}
                    for prefix, app in self._mounted_apps.items()
                ],
                "endpoints": {
                    "health": "/health",
                    "docs": "/docs",
                    "mounted_apps": [
                        f"{prefix}/health" for prefix in self._mounted_apps
                    ],
                },
            }

    def run(self, host: str = None, port: int = None, **kwargs):
        """
        运行所有挂载应用，自动从命令行解析参数。

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

        # 注册路由和生命周期事件 (只执行一次)
        if not self._routes_registered:
            self._register_routes()
            self._register_lifecycle_events()
            self._routes_registered = True

        lines = [
            "",
            "=" * 60,
            f"启动 {self.group_name} v{self.version}",
            f"服务地址: http://{host}:{port}",
            "=" * 60,
            "",
            "已挂载应用:",
        ]
        for prefix, app in self._mounted_apps.items():
            lines.append(f"  {prefix:15} -> {app.app_name} ({type(app).__name__})")
        lines.extend(
            [
                "",
                "端点:",
                "  GET  /health              - 组健康检查",
                "  GET  /                    - 组信息",
            ]
        )
        for prefix in self._mounted_apps:
            app = self._mounted_apps[prefix]
            lines.append(f"  GET  {prefix}/health        - {prefix.strip('/')} 健康检查")
            if isinstance(app, AgentApp):
                lines.append(f"  POST {prefix}/query         - {prefix.strip('/')} 查询")
        lines.extend(
            [
                "",
                f"API 文档: http://{host}:{port}/docs",
                "=" * 60,
                "",
            ]
        )
        logger.info("%s", "\n".join(lines))

        uvicorn.run(self.app, host=host, port=port, **kwargs)
