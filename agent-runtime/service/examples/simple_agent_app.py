# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved

"""
Simple Agent App Example

This is a test example to verify openjiuwen-runtime-sdk works correctly.
It creates a simple agent that echoes messages back.
"""

import asyncio
from typing import AsyncIterator, Tuple

from openjiuwen_runtime.foundation.log import get_logger
from openjiuwen_runtime.service import AgentApp

logger = get_logger(__name__)


class MockAgent:
    """Mock Agent for testing purposes"""

    def __init__(self):
        self.sessions = {}

    async def stream(self, messages, conversation_id: str) -> AsyncIterator[Tuple[dict, bool]]:
        """
        Mock stream implementation that echoes messages

        Args:
            messages: List of message dictionaries
            conversation_id: Conversation ID for session tracking

        Yields:
            Tuple of (message_dict, is_last)
        """
        # Echo the last user message
        for msg in reversed(messages):
            if msg.get("role") == "user":
                content = msg.get("content", "")
                response_text = f"Echo: {content}"

                # Yield a text message
                yield {
                    "type": "text",
                    "content": response_text,
                }, True

                # Store session state
                if conversation_id not in self.sessions:
                    self.sessions[conversation_id] = []
                self.sessions[conversation_id].append({"role": "assistant", "content": response_text})
                break

    async def clear_session(self, conversation_id: str):
        """Clear session for a conversation"""
        if conversation_id in self.sessions:
            del self.sessions[conversation_id]


# Create the AgentApp
app = AgentApp(
    app_name="SimpleAgent",
    app_description="A simple echo agent for testing",
    version="0.1.0",
)


@app.init
async def init():
    """Initialize the mock agent"""
    app.agent = MockAgent()
    logger.info("[OK] MockAgent initialized")


@app.query
async def query(msgs, request):
    """Handle query requests"""
    async for msg, last in app.agent.stream(
            messages=msgs,
            conversation_id=request.conversation_id,
    ):
        yield msg, last


@app.shutdown
async def shutdown():
    """Cleanup resources"""
    if app.agent:
        logger.info("[OK] MockAgent cleaned up")


if __name__ == "__main__":
    # Run the app with command line args support
    # Usage: python simple_agent_app.py [--host HOST] [--port PORT]
    logger.info(
        "\nStarting SimpleAgent...\n"
        "Usage: python simple_agent_app.py [--host HOST] [--port PORT]\n"
        "Example: curl -X POST http://127.0.0.1:8091/query -H 'Content-Type: application/json' \\\n"
        "  -d '{\"messages\": [{\"role\": \"user\", \"content\": \"Hello\"}], \"conversation_id\": \"test-123\"}'\n"
    )
    app.run()
