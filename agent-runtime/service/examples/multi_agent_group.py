# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved

"""
Multi-Agent Group Example

演示多个 AgentApp 共享一个 FastAPI 服务。
"""

import os
import sys
from pathlib import Path
from typing import AsyncIterator, Tuple

# 添加项目根目录到路径
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from openjiuwen_runtime.foundation.log import get_logger
from openjiuwen_runtime.service import AgentApp, AppGroup

logger = get_logger(__name__)


class MockAgent:
    """Mock Agent for testing"""

    def __init__(self, name: str):
        self.name = name
        self.sessions = {}

    async def stream(self, messages, conversation_id: str) -> AsyncIterator[Tuple[dict, bool]]:
        """Echo messages back"""
        for msg in reversed(messages):
            if msg.get("role") == "user":
                content = msg.get("content", "")
                response_text = f"[{self.name}] 收到: {content}"

                yield {
                    "type": "text",
                    "content": response_text,
                }, True

                if conversation_id not in self.sessions:
                    self.sessions[conversation_id] = []
                self.sessions[conversation_id].append({"role": "assistant", "content": response_text})
                break

    async def clear_session(self, conversation_id: str):
        """Clear session"""
        if conversation_id in self.sessions:
            del self.sessions[conversation_id]


# 创建客服 Agent
customer_app = AgentApp(
    app_name="CustomerService",
    app_description="客服 Agent",
    version="1.0.0",
)


@customer_app.init
async def init_customer():
    customer_app.agent = MockAgent("客服")
    logger.info("[OK] 客服 Agent 初始化完成")


@customer_app.query
async def query_customer(msgs, request):
    async for msg, last in customer_app.agent.stream(
            messages=msgs,
            conversation_id=request.conversation_id,
    ):
        yield msg, last


# 创建助手 Agent
assistant_app = AgentApp(
    app_name="PersonalAssistant",
    app_description="个人助手 Agent",
    version="1.0.0",
)


@assistant_app.init
async def init_assistant():
    assistant_app.agent = MockAgent("助手")
    logger.info("[OK] 助手 Agent 初始化完成")


@assistant_app.query
async def query_assistant(msgs, request):
    async for msg, last in assistant_app.agent.stream(
            messages=msgs,
            conversation_id=request.conversation_id,
    ):
        yield msg, last


# 创建应用组并挂载
group = AppGroup(
    group_name="MultiAgentService",
    description="多 Agent 服务组",
    version="1.0.0",
)

group.mount("/customer", customer_app)
group.mount("/assistant", assistant_app)

if __name__ == "__main__":
    # 支持 --host 和 --port 命令行参数
    # python examples/multi_agent_group.py --port 8090
    group.run()
