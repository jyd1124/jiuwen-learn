# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved

"""OSS Client (Mock)

用于模拟 OSS 文件上传功能
"""

import logging
from typing import Optional

_LOG = logging.getLogger(__name__)


class MockOSSClient:
    """Mock OSS 客户端"""

    def __init__(
        self,
        endpoint: str = "",
        access_key: str = "",
        secret_key: str = "",
        bucket: Optional[str] = None,
    ):
        """初始化 Mock OSS 客户端"""
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.bucket = bucket

    async def upload_file(self, local_file_path: str, object_key: str) -> str:
        """
        上传文件到 OSS (Mock)

        Args:
            local_file_path: 本地文件路径
            object_key: OSS 对象键

        Returns:
            Mock OSS 文件 URL
        """
        # Mock: 假设文件已上传成功，返回一个 mock URL
        mock_oss_url = f"mock://oss.example.com/{object_key}"
        _LOG.info("[Mock OSS] Uploaded %s -> %s", local_file_path, mock_oss_url)
        return mock_oss_url

    async def delete_file(self, object_key: str) -> bool:
        """
        从 OSS 删除文件 (Mock)

        Args:
            object_key: OSS 对象键

        Returns:
            是否成功删除
        """
        _LOG.info("[Mock OSS] Deleted %s", object_key)
        return True
