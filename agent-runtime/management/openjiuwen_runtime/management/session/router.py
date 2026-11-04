# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved

"""Session 映射管理"""

import asyncio
from typing import Optional, Dict


class SessionRouter:
    """Session 映射管理，支持并发安全访问"""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._request_session: Dict[str, str] = {}

    async def get_request_session(self, request_id: str) -> Optional[str]:
        """获取 request_id 对应的 session_id"""
        async with self._lock:
            return self._request_session.get(request_id)

    async def set_request_session(self, request_id: str, session_id: str) -> None:
        """设置 request_id 到 session_id 的映射"""
        async with self._lock:
            self._request_session[request_id] = session_id

    async def delete_request_session(self, request_id: str) -> bool:
        """删除 request_id 的映射"""
        async with self._lock:
            if request_id in self._request_session:
                del self._request_session[request_id]
                return True
            return False

    async def clear(self) -> None:
        """清空 request → session 映射（服务缩容/销毁时调用）。"""
        async with self._lock:
            self._request_session.clear()

    async def get_request_session_size(self) -> int:
        """获取 request_session 映射大小"""
        async with self._lock:
            return len(self._request_session)


class ServiceRouter:
    """Service 映射管理，支持并发安全访问"""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._session_service: Dict[str, str] = {}

    async def get_session_service(self, session_id: str) -> Optional[str]:
        """获取 session_id 对应的 service_id"""
        async with self._lock:
            return self._session_service.get(session_id)

    async def set_session_service(self, session_id: str, service_id: str) -> None:
        """设置 session_id 到 service_id 的映射"""
        async with self._lock:
            self._session_service[session_id] = service_id

    async def delete_session_service(self, session_id: str) -> bool:
        """删除 session_id 的映射"""
        async with self._lock:
            if session_id in self._session_service:
                del self._session_service[session_id]
                return True
            return False

    async def delete_service_sessions(self, service_id: str) -> list[str]:
        """删除所有绑定到指定 service_id 的 session 映射，并返回被删除的 session_id。"""
        async with self._lock:
            session_ids = [
                session_id
                for session_id, mapped_service_id in self._session_service.items()
                if mapped_service_id == service_id
            ]
            for session_id in session_ids:
                del self._session_service[session_id]
            return session_ids

    async def clear(self) -> None:
        """清空 session_id → service_id 的亲和表（如进程退出/优雅停机）。"""
        async with self._lock:
            self._session_service.clear()
