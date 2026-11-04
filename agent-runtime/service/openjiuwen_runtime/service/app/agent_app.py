# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved

"""
AgentApp - Agent 应用类

提供部署 agent-studio Agent 的功能:
- Agent 实例管理
- @query 装饰器用于处理查询
- /query 和 /reset_conversation 端点
"""

import json
import asyncio
import inspect
import contextvars
from typing import Callable, Optional
from fastapi import HTTPException, Request
from fastapi.responses import StreamingResponse

from openjiuwen_runtime.foundation.log import get_logger

from .base_app import BaseApp
from .middleware import Middleware, MiddlewareContext
from ..models.request import QueryRequest, ResetConversationRequest

logger = get_logger(__name__)


class AgentApp(BaseApp):
    """
    openjiuwen Agent 应用类

    提供简单的 API 用于将 agent-studio Agent 部署到生产环境。

    设计原则:
    - 每个 AgentApp 持有一个 Agent 实例
    - 一个 Agent 服务所有对话
    - Agent 内部通过 conversation_id 隔离会话

    用法:
        app = AgentApp("MyAgent")

        @app.init
        async def init():
            app.agent = await load_agent_from_config(...)

        @app.query
        async def query(msgs, request):
            async for msg, last in app.agent.stream(
                messages=msgs,
                conversation_id=request.conversation_id,
            ):
                yield msg, last

        app.run(port=8090)
    """

    def __init__(
        self,
        app_name: str,
        app_description: str = "",
        version: str = "1.0.0",
        agent_config_path: str = None,
    ):
        super().__init__(app_name, app_description, version)

        # 查询钩子
        self._query_hook: Optional[Callable] = None
        self._agent_detail_hook: Optional[Callable] = None

        # 中间件列表
        self._middlewares: list = []

        # Agent 配置路径
        self.agent_config_path = agent_config_path

        # Agent 实例 (由用户初始化)
        self.agent = None

        # 可选服务
        self.sandbox_service = None

        # 注册 Agent 特定路由
        self._register_agent_routes()

    def query(self, func: Callable) -> Callable:
        """
        查询钩子装饰器

        用法:
            @app.query
            async def query(msgs, request):
                # 处理查询
                async for msg, last in agent.stream(...):
                    yield msg, last
        """
        self._query_hook = func
        return func

    def agent_detail(self, func: Callable) -> Callable:
        """
        Agent 详情钩子装饰器

        用法:
            @app.agent_detail
            async def agent_detail():
                return {"agent_name": "demo"}
        """
        self._agent_detail_hook = func
        return func

    def add_middleware(self, middleware: Middleware) -> 'AgentApp':
        """
        添加中间件实例

        参数:
            middleware: 中间件实例

        返回:
            self，支持链式调用
        """
        self._middlewares.append(middleware)
        return self

    def middleware(self, middleware_class):
        """
        中间件装饰器

        用法:
            @app.middleware(MyMiddleware)
            class MyMiddleware(Middleware):
                pass

        参数:
            middleware_class: 中间件类

        返回:
            中间件实例
        """
        instance = middleware_class()
        self._middlewares.append(instance)
        return instance

    @property
    def middlewares(self) -> tuple[Middleware, ...]:
        """已注册中间件的只读快照（顺序与执行顺序一致）。"""
        return tuple(self._middlewares)

    def _register_agent_routes(self):
        """注册 Agent 特定路由"""

        @self.app.post("/query")
        async def query_endpoint(query_request: QueryRequest, request: Request):
            """
            查询端点 - 发送消息给 Agent 并获取流式响应

            参数:
                query_request: 包含 messages 和 conversation_id 的查询请求

            返回:
                Agent 响应的服务器发送事件 (SSE) 流
            """
            # 从请求中提取消息
            messages = query_request.messages

            # 执行查询钩子
            async def generate():
                if self._query_hook:
                    # 创建中间件上下文
                    context = MiddlewareContext()
                    processed_messages = messages

                    # 调用所有中间件的 before_query 方法
                    for mw in self._middlewares:
                        try:
                            processed_messages = await mw.before_query(
                                processed_messages, query_request, context
                            )
                        except Exception:
                            logger.exception(
                                "Middleware before_query failed: %s",
                                type(mw).__name__,
                            )

                    try:
                        chunk_count = 0
                        cancel_event = asyncio.Event()
                        gen = self._query_hook(processed_messages, query_request, cancel_event)

                        try:
                            async for msg, last in gen:
                                if await request.is_disconnected():
                                    logger.info(f"[generate] client disconnected at chunk #{chunk_count}, stopping")
                                    cancel_event.set()
                                    break
                                chunk_count += 1
                                processed_msg = msg
                                for mw in self._middlewares:
                                    try:
                                        processed_msg = await mw.before_response(
                                            processed_messages, query_request, processed_msg, context
                                        )
                                    except Exception:
                                        logger.exception(
                                            "Middleware before_response failed: %s",
                                            type(mw).__name__,
                                        )

                                if chunk_count <= 3:
                                    logger.info(f"[generate] yielding chunk #{chunk_count}, "
                                                f"type={type(processed_msg).__name__}")
                                yield f"data: {json.dumps(processed_msg)}\n\n"
                        finally:
                            cancel_event.set()

                        logger.info(f"[generate] stream finished, total_chunks={chunk_count}")

                        # 调用 after_query 中间件
                        for mw in self._middlewares:
                            try:
                                await mw.after_query(processed_messages, query_request, context)
                            except Exception:
                                logger.exception(
                                    "Middleware after_query failed: %s",
                                    type(mw).__name__,
                                )

                        logger.info(
                            f"Query completed - conversation_id: {query_request.conversation_id}, "
                            f"total_messages: {chunk_count}"
                        )

                    except Exception as e:
                        logger.error(
                            f"Query execution failed - conversation_id: {query_request.conversation_id}, error: {e}",
                            exc_info=True,
                        )
                        # 调用错误中间件
                        for mw in self._middlewares:
                            try:
                                await mw.on_error(processed_messages, query_request, e, context)
                            except Exception:
                                logger.exception(
                                    "Middleware on_error failed: %s",
                                    type(mw).__name__,
                                )

                        # 发送错误事件
                        error_event = {
                            "type": "error",
                            "error": str(e),
                        }
                        error_data = f"data: {json.dumps(error_event, ensure_ascii=False, default=str)}\n\n"
                        yield error_data

                    except asyncio.CancelledError:
                        logger.warning(
                            "Query stream cancelled - conversation_id: %s",
                            query_request.conversation_id,
                        )
                        raise

            return StreamingResponse(
                generate(),
                media_type="text/event-stream",
                headers={
                    "Cache-Control": "no-cache, no-transform",
                    "Connection": "keep-alive",
                    "X-Accel-Buffering": "no",
                },
            )

        @self.app.post("/reset_conversation")
        async def reset_conversation_endpoint(request: ResetConversationRequest):
            """
            重置对话端点 - 清除对话上下文

            参数:
                request: 包含 conversation_id 的重置请求

            返回:
                状态消息
            """
            # 调用 Agent 的会话清理方法
            if self.agent and hasattr(self.agent, 'clear_session'):
                await self.agent.clear_session(request.conversation_id)

            return {"status": "ok", "message": f"Conversation {request.conversation_id} reset"}

        @self.app.get("/agent_detail")
        async def agent_detail_endpoint():
            """
            Agent 详情端点 - 返回当前 Agent 的 IR 详情信息

            返回:
                Agent 详情字典
            """
            if not self._agent_detail_hook:
                raise HTTPException(status_code=501, detail="agent_detail hook not implemented")

            try:
                detail = self._agent_detail_hook()
                if inspect.isawaitable(detail):
                    detail = await detail
                if detail is None:
                    detail = {}
                return detail
            except HTTPException:
                raise
            except Exception as e:
                logger.exception("Agent detail execution failed")
                raise HTTPException(status_code=500, detail=str(e)) from e

        # 更新健康检查，包含 agent_loaded 状态
        # 获取原始 /health 路由
        for route in self.app.routes:
            if hasattr(route, 'path') and route.path == "/health":
                # 存储原始端点
                original_endpoint = route.app
                break
        else:
            original_endpoint = None

        # 定义增强的健康检查
        @self.app.get("/health")
        async def health_with_agent():
            """健康检查（包含 Agent 状态）"""
            health_data = {
                "status": "healthy",
                "app": self.app_name,
                "version": self.version,
                "agent_loaded": self.agent is not None,
            }

            return health_data
