#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved

# -*- coding: UTF-8 -*-
"""
Lowcode Agent Runner - 命令行入口

支持通过以下方式运行:
    python -m openjiuwen_runtime.examples.lowcode_agent --file config.json --port 8091
    lowcode-agent-runner --file config.json --port 8091
"""


def main():
    """命令行入口函数"""
    from .lowcode_agent_runner import app
    app.run()


if __name__ == "__main__":
    main()
