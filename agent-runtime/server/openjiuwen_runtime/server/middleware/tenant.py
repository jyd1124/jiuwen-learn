# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved

"""租户隔离中间件

从 HTTP Header 或 JWT Token 中提取 user_id 和 space_id，
并将其注入到请求状态中，供后续端点使用。
"""

from typing import Optional

from fastapi import Request, status
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp

from openjiuwen_runtime.foundation.log import get_logger

logger = get_logger(__name__)


class TenantContextMiddleware(BaseHTTPMiddleware):
    """租户上下文中间件

    从请求头中提取 user_id 和 space_id，验证后注入到 request.state
    """

    def __init__(self, app: ASGIApp, require_tenant: bool = False):
        """
        Args:
            app: ASGI 应用
            require_tenant: 是否强制要求租户信息（默认True）
        """
        super().__init__(app)
        self.require_tenant = require_tenant

    async def dispatch(self, request: Request, call_next):
        """处理请求，提取并验证租户信息"""

        # 提取 user_id 和 space_id
        user_id = request.headers.get("X-User-ID") or request.headers.get("x-user-id")
        space_id = request.headers.get("X-Space-ID") or request.headers.get("x-space-id")

        # 验证租户信息
        if self.require_tenant:
            if not user_id or not space_id:
                logger.warning(f"Missing tenant context: user_id={user_id}, space_id={space_id}")
                return JSONResponse(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    content={"detail": "Missing tenant context: X-User-ID and X-Space-ID headers are required"}
                )

        # 注入到 request.state
        request.state.user_id = user_id
        request.state.space_id = space_id

        logger.debug(f"Tenant context: user_id={user_id}, space_id={space_id}")

        response = await call_next(request)
        return response


def get_tenant_context(request: Request) -> tuple[Optional[str], Optional[str]]:
    """从请求中获取租户上下文

    Args:
        request: FastAPI 请求对象

    Returns:
        (user_id, space_id) 元组
    """
    user_id = getattr(request.state, "user_id", None)
    space_id = getattr(request.state, "space_id", None)
    return user_id, space_id
