# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved

import gzip
import os
import shutil
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional


class CompressedRotatingFileHandler(RotatingFileHandler):
    """
    支持压缩的日志轮转 Handler
    
    当日志文件达到指定大小时，自动轮转并压缩旧日志文件。
    压缩后的文件以 .gz 格式保存，保留最近 backupCount 个压缩文件。
    """

    def __init__(
            self,
            filename: str,
            mode: str = "a",
            maxBytes: int = 0,
            backupCount: int = 0,
            encoding: Optional[str] = None,
            delay: bool = False,
    ):
        self._ensure_log_dir(filename)
        super().__init__(
            filename, mode=mode, maxBytes=maxBytes, backupCount=backupCount, encoding=encoding, delay=delay
        )

    def _ensure_log_dir(self, filename: str) -> None:
        log_dir = os.path.dirname(filename)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir, exist_ok=True)

    def doRollover(self) -> None:
        """
        执行日志轮转
        
        1. 关闭当前日志文件
        2. 压缩旧日志文件
        3. 删除超出备份数量的压缩文件
        """
        if self.stream:
            self.stream.close()
            self.stream = None

        log_path = Path(self.baseFilename)
        log_dir = log_path.parent
        log_name = log_path.name

        compressed_files = sorted(
            log_dir.glob(f"{log_name}.*.gz"),
            key=lambda p: p.stat().st_mtime if p.exists() else 0,
        )

        while len(compressed_files) >= self.backupCount:
            oldest = compressed_files.pop(0)
            oldest.unlink()

        for i in range(self.backupCount - 1, 0, -1):
            src = log_dir / f"{log_name}.{i}.gz"
            if src.exists():
                dst = log_dir / f"{log_name}.{i + 1}.gz"
                src.rename(dst)

        self._compress_file(self.baseFilename, log_dir / f"{log_name}.1.gz")

        self.stream = self._open()

    def _compress_file(self, src_path: str, dst_path: Path) -> None:
        """压缩文件"""
        with open(src_path, "rb") as f_in:
            with gzip.open(dst_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)

        with open(src_path, "w", encoding=self.encoding) as f:
            f.truncate()
