# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved

"""
openjiuwen-runtime-sdk 的请求和响应模型
"""

from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field


class QueryRequest(BaseModel):
    """
    Agent 查询请求模型

    属性:
        messages: 当前消息列表（非完整历史）
        conversation_id: 对话 ID（必需）
        user_id: 用户 ID
        space_id: 工作空间 ID
        stream: 是否流式输出
    """

    messages: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="当前消息列表（非完整对话历史）"
    )
    conversation_id: str = Field(
        ...,
        description="用于会话跟踪的对话 ID"
    )
    user_id: str = Field(
        default="anonymous",
        description="用户 ID"
    )
    space_id: str = Field(
        default="default",
        description="工作空间 ID"
    )
    stream: bool = Field(
        default=True,
        description="是否使用流式输出"
    )


class ResetConversationRequest(BaseModel):
    """
    重置对话请求模型

    属性:
        conversation_id: 要重置的对话 ID（必需）
        user_id: 用户 ID（可选，用于授权）
    """

    conversation_id: str = Field(
        ...,
        description="要重置的对话 ID"
    )
    user_id: Optional[str] = Field(
        default=None,
        description="用于授权的用户 ID"
    )
