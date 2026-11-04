# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved

import socket


def is_port_available(port: int, host: str = "0.0.0.0") -> bool:
    """检查端口是否可用（通过尝试 bind 检测，比 connect 更可靠）"""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((host, port))
            return True
    except OSError:
        return False


def allocate_port(
    start_port: int = 8090,
    max_port: int = 9090,
    host: str = "127.0.0.1",
    exclude_ports: set[int] | None = None,
) -> int:
    """分配可用端口

    Args:
        start_port: 起始端口
        max_port: 最大端口（不含）
        host: 检测主机
        exclude_ports: 需要排除的端口集合（如数据库中已分配的端口）
    """
    exclude = exclude_ports or set()
    for port in range(start_port, max_port):
        if port not in exclude and is_port_available(port, host):
            return port
    raise RuntimeError(f"No available port in range {start_port}-{max_port}")

