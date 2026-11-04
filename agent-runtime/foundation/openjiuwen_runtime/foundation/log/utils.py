#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved

# -*- coding: UTF-8 -*-
"""日志脱敏工具函数"""


def mask_userdata(userdata: str | None, max_bytes: int = 10) -> str:
    """
    对userdata进行脱敏处理，只保留前N字节，其余隐藏

    Args:
        userdata: 用户数据字符串
        max_bytes: 保留的最大字节数，默认10字节

    Returns:
        脱敏后的字符串
    """
    if userdata is None:
        return "None"

    if not isinstance(userdata, str):
        userdata = str(userdata)

    # 编码为字节获取准确长度
    userdata_bytes = userdata.encode('utf-8')

    if len(userdata_bytes) <= max_bytes:
        return userdata

    # 只保留前max_bytes字节
    masked_bytes = userdata_bytes[:max_bytes]
    try:
        masked_str = masked_bytes.decode('utf-8', errors='ignore')
    except UnicodeDecodeError:
        # 如果解码失败，直接返回截断前的原始字符串前几个字符
        masked_str = userdata[:max_bytes]

    return f"{masked_str}***"


def mask_cmd(cmd: list[str], sensitive_flags: list[str] | None = None) -> str:
    """
    对命令行参数中的敏感值进行脱敏

    Args:
        cmd: 命令行参数列表
        sensitive_flags: 需要脱敏的参数名列表，如 ["--userdata", "--api-key"]

    Returns:
        脱敏后的命令行字符串
    """
    if sensitive_flags is None:
        sensitive_flags = ["--userdata"]

    masked_parts = []
    i = 0
    while i < len(cmd):
        if cmd[i] in sensitive_flags and i + 1 < len(cmd):
            masked_parts.append(f"{cmd[i]} {mask_userdata(cmd[i + 1])}")
            i += 2
        else:
            masked_parts.append(cmd[i])
            i += 1
    return " ".join(masked_parts)
