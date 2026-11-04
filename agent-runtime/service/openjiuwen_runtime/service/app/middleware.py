# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved

"""
Middleware - 中间件基类和上下文

提供:
- MiddlewareContext: 中间件上下文，用于在中间件之间传递数据
- Middleware: 中间件基类，定义生命周期钩子
"""

import logging
from typing import Any, Dict, List, Optional


class MiddlewareContext:
    """
    中间件上下文

    用于在中间件之间传递上下文数据
    """

    def __init__(self):
        self.data: Dict[str, Any] = {}

    def set(self, key: str, value: Any) -> None:
        """
        设置上下文数据

        参数:
            key: 数据键名
            value: 数据值
        """
        self.data[key] = value

    def get(self, key: str, default: Any = None) -> Any:
        """
        获取上下文数据

        参数:
            key: 数据键名
            default: 默认值 (当键不存在时返回)

        返回:
            对应的数据值，如果不存在则返回默认值
        """
        return self.data.get(key, default)


class Middleware:
    """
    中间件基类

    定义四个可选钩子方法，子类可以按需实现:
    - before_query: 查询执行前调用
    - after_query: 查询成功后调用
    - on_error: 查询出错时调用
    - before_response: 响应发送前调用
    """

    async def before_query(
        self,
        messages: List[Dict[str, Any]],
        request: Any,
        context: MiddlewareContext,
    ) -> List[Dict[str, Any]]:
        """
        查询执行前调用

        参数:
            messages: 消息列表
            request: 请求对象
            context: 中间件上下文

        返回:
            修改后的消息列表 (默认返回原消息)
        """
        return messages

    async def after_query(
        self,
        messages: List[Dict[str, Any]],
        request: Any,
        context: MiddlewareContext,
    ) -> None:
        """
        查询成功后调用

        参数:
            messages: 消息列表
            request: 请求对象
            context: 中间件上下文
        """
        pass

    async def on_error(
        self,
        messages: List[Dict[str, Any]],
        request: Any,
        error: Exception,
        context: MiddlewareContext,
    ) -> None:
        """
        查询出错时调用

        参数:
            messages: 消息列表
            request: 请求对象
            error: 异常对象
            context: 中间件上下文
        """
        pass

    async def before_response(
        self,
        messages: List[Dict[str, Any]],
        request: Any,
        response: Any,
        context: MiddlewareContext,
    ) -> Any:
        """
        响应发送前调用

        参数:
            messages: 消息列表
            request: 请求对象
            response: 响应对象
            context: 中间件上下文

        返回:
            修改后的响应对象 (默认返回原响应)
        """
        return response


class LoggingMiddleware(Middleware):
    """
    日志中间件示例

    用于记录查询生命周期的各个阶段，便于调试和监控
    """

    def __init__(self, logger: Optional[logging.Logger] = None):
        """
        初始化日志中间件

        参数:
            logger: 可选的日志记录器，如果未提供则创建默认 logger
        """
        self.logger = logger or logging.getLogger(__name__)

    async def before_query(
        self,
        messages: List[Dict[str, Any]],
        request: Any,
        context: MiddlewareContext,
    ) -> List[Dict[str, Any]]:
        """
        查询执行前调用，记录查询开始信息

        参数:
            messages: 消息列表
            request: 请求对象
            context: 中间件上下文

        返回:
            原消息列表
        """
        message_count = len(messages)
        conversation_id = getattr(request, "conversation_id", "unknown")
        self.logger.info(
            f"[LoggingMiddleware] 查询开始 - conversation_id: {conversation_id}, 消息数量: {message_count}"
        )
        return messages

    async def after_query(
        self,
        messages: List[Dict[str, Any]],
        request: Any,
        context: MiddlewareContext,
    ) -> None:
        """
        查询成功后调用，记录查询完成信息

        参数:
            messages: 消息列表
            request: 请求对象
            context: 中间件上下文
        """
        conversation_id = getattr(request, "conversation_id", "unknown")
        self.logger.info(
            f"[LoggingMiddleware] 查询完成 - conversation_id: {conversation_id}"
        )

    async def on_error(
        self,
        messages: List[Dict[str, Any]],
        request: Any,
        error: Exception,
        context: MiddlewareContext,
    ) -> None:
        """
        查询出错时调用，记录错误信息

        参数:
            messages: 消息列表
            request: 请求对象
            error: 异常对象
            context: 中间件上下文
        """
        conversation_id = getattr(request, "conversation_id", "unknown")
        self.logger.error(
            f"[LoggingMiddleware] 查询错误 - conversation_id: {conversation_id}, 错误: {error}"
        )

    async def before_response(
        self,
        messages: List[Dict[str, Any]],
        request: Any,
        response: Any,
        context: MiddlewareContext,
    ) -> Any:
        """
        响应发送前调用，记录响应信息

        参数:
            messages: 消息列表
            request: 请求对象
            response: 响应对象
            context: 中间件上下文

        返回:
            原响应对象
        """
        conversation_id = getattr(request, "conversation_id", "unknown")
        response_type = type(response).__name__
        self.logger.info(
            f"[LoggingMiddleware] 响应发送 - conversation_id: {conversation_id}, 响应类型: {response_type}"
        )
        return response
