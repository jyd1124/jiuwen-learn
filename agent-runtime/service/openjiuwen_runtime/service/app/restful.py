# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved

"""
@restful.tool() 装饰器实现

用于将 Python 函数注册为 RESTful 工具。

支持的功能:
- 自动提取函数签名生成参数定义
- 支持同步和异步函数
- 支持工具元数据定义
"""

import asyncio
import inspect
from typing import Dict, Any, Callable, TYPE_CHECKING

if TYPE_CHECKING:
    from .plugin_app import PluginApp


class RestfulToolDecorator:
    """
    @restful.tool() 装饰器实现

    用于将 Python 函数注册为 RESTful 工具。

    支持的功能:
    - 自动提取函数签名生成参数定义
    - 支持同步和异步函数
    - 支持工具元数据定义
    """

    def __init__(self, plugin_app: "PluginApp"):
        """
        初始化装饰器

        参数:
            plugin_app: PluginApp 实例（必需）
        """
        self.plugin_app = plugin_app

    def tool(
        self,
        name: str = None,
        description: str = "",
    ):
        """
        工具装饰器

        参数:
            name: 工具名称（默认使用函数名）
            description: 工具描述
        """

        def decorator(func: Callable) -> Callable:
            # 提取函数签名
            sig = inspect.signature(func)
            parameters = {}
            for param_name, param in sig.parameters.items():
                param_info = {"type": str(param.annotation if param.annotation != inspect.Parameter.empty else "any")}
                if param.default != inspect.Parameter.empty:
                    param_info["default"] = param.default
                parameters[param_name] = param_info

            # 确定工具名称
            tool_name = name or func.__name__

            # 直接注册到 PluginApp
            self.plugin_app.register_tool(
                name=tool_name,
                func=func,
                description=description,
                parameters=parameters,
            )

            return func

        return decorator
