# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved

"""
中间件功能测试

测试内容:
1. 中间件注册 (add_middleware, middleware 装饰器, 链式调用)
2. 中间件执行顺序
3. 生命周期钩子 (before_query, before_response, after_query, on_error)
4. 上下文数据传递
5. LoggingMiddleware 功能
"""

import unittest
import asyncio
import logging
from io import StringIO
from typing import Dict, Any, List

from openjiuwen_runtime.service import AgentApp, Middleware, MiddlewareContext, LoggingMiddleware


class TestMiddlewareContext(unittest.TestCase):
    """测试 MiddlewareContext 上下文数据传递"""

    def test_set_and_get(self):
        """测试 set 和 get 方法"""
        context = MiddlewareContext()
        context.set("key1", "value1")
        context.set("key2", 123)

        self.assertEqual(context.get("key1"), "value1")
        self.assertEqual(context.get("key2"), 123)

    def test_get_with_default(self):
        """测试 get 方法使用默认值"""
        context = MiddlewareContext()
        self.assertIsNone(context.get("non_existent"))
        self.assertEqual(context.get("non_existent", "default"), "default")

    def test_set_overwrite(self):
        """测试 set 方法覆盖已有值"""
        context = MiddlewareContext()
        context.set("key", "value1")
        context.set("key", "value2")
        self.assertEqual(context.get("key"), "value2")


class OrderTrackingMiddleware(Middleware):
    """用于测试执行顺序的中间件"""

    def __init__(self, name: str, call_order: List[str]):
        self.name = name
        self.call_order = call_order

    async def before_query(
            self,
            messages: List[Dict[str, Any]],
            request: Any,
            context: MiddlewareContext,
    ) -> List[Dict[str, Any]]:
        self.call_order.append(f"{self.name}.before_query")
        return messages

    async def after_query(
            self,
            messages: List[Dict[str, Any]],
            request: Any,
            context: MiddlewareContext,
    ) -> None:
        self.call_order.append(f"{self.name}.after_query")

    async def on_error(
            self,
            messages: List[Dict[str, Any]],
            request: Any,
            error: Exception,
            context: MiddlewareContext,
    ) -> None:
        self.call_order.append(f"{self.name}.on_error")

    async def before_response(
            self,
            messages: List[Dict[str, Any]],
            request: Any,
            response: Any,
            context: MiddlewareContext,
    ) -> Any:
        self.call_order.append(f"{self.name}.before_response")
        return response


class ContextSharingMiddleware(Middleware):
    """用于测试上下文数据共享的中间件"""

    def __init__(self, name: str, set_key: str = None, set_value: Any = None, get_key: str = None):
        self.name = name
        self.set_key = set_key
        self.set_value = set_value
        self.get_key = get_key
        self.retrieved_value = None

    async def before_query(
            self,
            messages: List[Dict[str, Any]],
            request: Any,
            context: MiddlewareContext,
    ) -> List[Dict[str, Any]]:
        if self.set_key:
            context.set(self.set_key, self.set_value)
        if self.get_key:
            self.retrieved_value = context.get(self.get_key)
        return messages


class TestMiddlewareRegistration(unittest.TestCase):
    """测试中间件注册"""

    def test_add_middleware(self):
        """测试 add_middleware 方法注册中间件"""
        app = AgentApp("test_app")
        middleware = Middleware()

        result = app.add_middleware(middleware)

        self.assertEqual(len(app.middlewares), 1)
        self.assertIs(app.middlewares[0], middleware)
        self.assertIs(result, app)

    def test_middleware_decorator(self):
        """测试 middleware 装饰器注册中间件"""

        app = AgentApp("test_app")

        class CustomMiddleware(Middleware):
            pass

        instance = app.middleware(CustomMiddleware)

        self.assertEqual(len(app.middlewares), 1)
        self.assertIsInstance(app.middlewares[0], CustomMiddleware)
        self.assertIs(app.middlewares[0], instance)

    def test_chained_calls(self):
        """测试链式调用添加多个中间件"""
        app = AgentApp("test_app")
        mw1 = Middleware()
        mw2 = Middleware()

        result = app.add_middleware(mw1).add_middleware(mw2)

        self.assertEqual(len(app.middlewares), 2)
        self.assertIs(app.middlewares[0], mw1)
        self.assertIs(app.middlewares[1], mw2)
        self.assertIs(result, app)


class TestMiddlewareExecution(unittest.TestCase):
    """测试中间件执行"""

    def setUp(self):
        self.call_order = []

    def test_execution_order(self):
        """测试多个中间件按注册顺序执行"""
        call_order = []

        async def run_test():
            app = AgentApp("test_app")
            app.add_middleware(OrderTrackingMiddleware("mw1", call_order))
            app.add_middleware(OrderTrackingMiddleware("mw2", call_order))

            context = MiddlewareContext()
            messages = [{"role": "user", "content": "test"}]

            for mw in app.middlewares:
                messages = await mw.before_query(messages, None, context)

            for mw in app.middlewares:
                await mw.after_query(messages, None, context)

            for mw in app.middlewares:
                await mw.before_response(messages, None, "response", context)

        asyncio.run(run_test())

        expected = [
            "mw1.before_query",
            "mw2.before_query",
            "mw1.after_query",
            "mw2.after_query",
            "mw1.before_response",
            "mw2.before_response",
        ]
        self.assertEqual(call_order, expected)


class TestLifecycleHooks(unittest.TestCase):
    """测试生命周期钩子"""

    def test_before_query_hook(self):
        """测试 before_query 钩子被调用"""
        call_order = []

        async def run_test():
            mw = OrderTrackingMiddleware("test", call_order)
            context = MiddlewareContext()
            messages = [{"role": "user", "content": "test"}]

            result = await mw.before_query(messages, None, context)

            self.assertEqual(result, messages)

        asyncio.run(run_test())
        self.assertIn("test.before_query", call_order)

    def test_before_response_hook(self):
        """测试 before_response 钩子被调用"""
        call_order = []

        async def run_test():
            mw = OrderTrackingMiddleware("test", call_order)
            context = MiddlewareContext()
            messages = [{"role": "user", "content": "test"}]

            result = await mw.before_response(messages, None, "response", context)

            self.assertEqual(result, "response")

        asyncio.run(run_test())
        self.assertIn("test.before_response", call_order)

    def test_after_query_hook(self):
        """测试 after_query 钩子被调用"""
        call_order = []

        async def run_test():
            mw = OrderTrackingMiddleware("test", call_order)
            context = MiddlewareContext()
            messages = [{"role": "user", "content": "test"}]

            await mw.after_query(messages, None, context)

        asyncio.run(run_test())
        self.assertIn("test.after_query", call_order)

    def test_on_error_hook(self):
        """测试 on_error 钩子在异常时被调用"""
        call_order = []

        async def run_test():
            mw = OrderTrackingMiddleware("test", call_order)
            context = MiddlewareContext()
            messages = [{"role": "user", "content": "test"}]
            error = ValueError("test error")

            await mw.on_error(messages, None, error, context)

        asyncio.run(run_test())
        self.assertIn("test.on_error", call_order)


class TestContextDataSharing(unittest.TestCase):
    """测试上下文数据传递"""

    def test_context_sharing_between_middlewares(self):
        """测试中间件之间可以通过 context 共享数据"""
        retrieved_values = {}

        async def run_test():
            app = AgentApp("test_app")

            mw1 = ContextSharingMiddleware("mw1", set_key="shared_data", set_value="hello")
            mw2 = ContextSharingMiddleware("mw2", get_key="shared_data")

            app.add_middleware(mw1).add_middleware(mw2)

            context = MiddlewareContext()
            messages = [{"role": "user", "content": "test"}]

            for mw in app.middlewares:
                messages = await mw.before_query(messages, None, context)

            retrieved_values["mw2"] = mw2.retrieved_value

        asyncio.run(run_test())
        self.assertEqual(retrieved_values["mw2"], "hello")


class TestLoggingMiddleware(unittest.TestCase):
    """测试 LoggingMiddleware"""

    def test_logging_middleware_logs_correctly(self):
        """验证 LoggingMiddleware 可以正常工作"""

        class MockRequest:
            conversation_id = "test-conv-123"

        async def run_test():
            log_stream = StringIO()
            handler = logging.StreamHandler(log_stream)
            handler.setLevel(logging.INFO)
            logger = logging.getLogger("test_logger")
            logger.setLevel(logging.INFO)
            logger.addHandler(handler)

            mw = LoggingMiddleware(logger)
            context = MiddlewareContext()
            messages = [{"role": "user", "content": "test"}]
            request = MockRequest()

            await mw.before_query(messages, request, context)
            await mw.after_query(messages, request, context)
            await mw.before_response(messages, request, "response", context)

            error = ValueError("test error")
            await mw.on_error(messages, request, error, context)

            log_output = log_stream.getvalue()

            logger.removeHandler(handler)

            return log_output

        log_output = asyncio.run(run_test())

        self.assertIn("查询开始", log_output)
        self.assertIn("test-conv-123", log_output)
        self.assertIn("查询完成", log_output)
        self.assertIn("响应发送", log_output)
        self.assertIn("查询错误", log_output)
        self.assertIn("test error", log_output)


if __name__ == "__main__":
    unittest.main()
