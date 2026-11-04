# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved

"""
PluginApp - Plugin 应用类

提供将工具集部署为 RESTful 服务的功能:
- 工具注册表管理
- @restful.tool() 装饰器支持
- 自动生成 RESTful 端点
- /tools 端点列出所有工具
"""

import asyncio
from typing import Callable, Dict, Any, Optional
from fastapi import Request, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, create_model
from pydantic.fields import FieldInfo
from .base_app import BaseApp


class PluginApp(BaseApp):
    """
    openjiuwen Plugin 应用类

    提供简洁的 API 用于部署同类工具集为 RESTful 服务。

    设计理念:
    - 一个 PluginApp 包含同一类别的多个工具
    - 工具应具有主题相关性（如：天气工具、搜索工具、数学工具）
    - 使用 @restful.tool() 装饰器注册工具
    - 自动为每个工具生成 RESTful 端点
    - 支持工具元数据定义

    用法 - 天气工具 Plugin:
        app = PluginApp(
            app_name="WeatherTools",
            app_description="天气查询工具集"
        )

        @app.restful.tool(name="current_weather", description="查询实时天气")
        async def current_weather(city: str) -> Dict[str, Any]:
            return {"city": city, "temperature": 25, "condition": "晴"}

        @app.restful.tool(name="weather_forecast", description="查询天气预报")
        async def weather_forecast(city: str, days: int = 3) -> Dict[str, Any]:
            return {"city": city, "forecast": [...]}

        app.run(port=8091)
    """

    def __init__(
        self,
        app_name: str,
        app_description: str = "",
        version: str = "1.0.0",
    ):
        super().__init__(app_name, app_description, version)

        # 工具注册表
        self._tools: Dict[str, Dict[str, Any]] = {}

        # 创建工具装饰器（绑定到当前 app）
        from .restful import RestfulToolDecorator
        self.restful = RestfulToolDecorator(self)

        # 注册 Plugin 特定路由
        self._register_plugin_routes()

    @property
    def tools_count(self) -> int:
        """已注册工具数量（供健康检查等场景使用）。"""
        return len(self._tools)

    def register_tool(
        self,
        name: str,
        func: Callable,
        description: str = "",
        parameters: Dict[str, Any] = None,
    ):
        """
        注册工具

        Args:
            name: 工具名称
            func: 工具函数
            description: 工具描述
            parameters: 参数定义（可选）
        """
        self._tools[name] = {
            "name": name,
            "func": func,
            "description": description,
            "parameters": parameters or {},
        }

        # 为工具创建路由
        self._create_tool_route(name, func, parameters)

    def _create_tool_route(self, name: str, func: Callable, parameters: Dict[str, Any] = None):
        """
        为工具创建 RESTful 路由

        Args:
            name: 工具名称
            func: 工具函数
            parameters: 参数定义
        """
        parameters = parameters or {}

        # 动态创建 Pydantic 模型用于请求体验证
        fields = {}
        for param_name, param_info in parameters.items():
            # 根据参数类型确定字段类型
            param_type = param_info.get("type", "any")
            python_type = self._get_python_type(param_type)

            # 创建字段信息
            field_info = FieldInfo(
                default=param_info.get("default", ...),
                description=f"Parameter: {param_name}",
            )
            fields[param_name] = (python_type, field_info)

        # 创建动态模型
        if fields:
            request_model = create_model(
                f"{name.capitalize()}Request",
                **fields
            )
        else:
            # 如果没有参数，使用空模型
            class EmptyRequest(BaseModel):
                pass
            request_model = EmptyRequest

        @self.app.post(f"/tools/{name}")
        async def tool_endpoint(request: request_model):
            """工具执行端点"""
            try:
                # 将 Pydantic 模型转换为字典
                data = request.model_dump() if hasattr(request, 'model_dump') else request.dict()

                # 执行工具函数
                if asyncio.iscoroutinefunction(func):
                    result = await func(**data)
                else:
                    result = func(**data)

                return {
                    "status": "success",
                    "tool": name,
                    "result": result,
                }
            except Exception as e:
                raise HTTPException(
                    status_code=500,
                    detail=f"Tool execution failed: {str(e)}",
                ) from e

    def _get_python_type(self, param_type: str) -> type:
        """
        将字符串类型转换为 Python 类型

        Args:
            param_type: 类型字符串 (如 "int", "float", "str", "bool")

        Returns:
            Python 类型
        """
        type_mapping = {
            "int": int,
            "float": float,
            "str": str,
            "bool": bool,
            "list": list,
            "dict": dict,
            "any": Any,
        }
        return type_mapping.get(param_type.lower(), Any)

    def _register_plugin_routes(self):
        """注册 Plugin 特定路由"""

        @self.app.get("/tools")
        async def list_tools():
            """列出所有工具"""
            tools_info = []
            for name, tool in self._tools.items():
                tools_info.append({
                    "name": tool["name"],
                    "description": tool["description"],
                    "parameters": tool["parameters"],
                    "endpoint": f"/tools/{name}",
                })
            return {
                "status": "success",
                "tools": tools_info,
            }

        # 更新健康检查，添加工具数量
        @self.app.get("/health")
        async def health_with_tools():
            """健康检查（包含工具状态）"""
            return {
                "status": "healthy",
                "app": self.app_name,
                "version": self.version,
                "tools_count": len(self._tools),
            }
