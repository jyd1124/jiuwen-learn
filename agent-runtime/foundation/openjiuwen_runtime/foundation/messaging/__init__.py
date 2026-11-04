# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved

"""AMQP / RabbitMQ 通用能力（JSON + Topic Exchange）。

安装可选依赖： ``pip install 'openjiuwen-runtime-foundation[amqp]'`` 或单独 ``aio-pika>=9.4``。
"""

from .amqp_json import AmqpTopicJsonPublisher, consume_topic_json_forever

__all__ = [
    "AmqpTopicJsonPublisher",
    "consume_topic_json_forever",
]
