# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved

"""Topic Exchange 上的 JSON 发布与消费（aio-pika）。"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from typing import Any

logger = logging.getLogger(__name__)


def _require_aio_pika() -> Any:
    try:
        import aio_pika  # type: ignore[import-untyped]
    except ImportError as exc:
        raise RuntimeError(
            "aio-pika is required for AMQP messaging. "
            "Install with: pip install 'openjiuwen-runtime-foundation[amqp]' "
            "or pip install 'aio-pika>=9.4'"
        ) from exc
    return aio_pika


class AmqpTopicJsonPublisher:
    """对单个 durable topic exchange 发布 JSON 消息（连接可复用）。"""

    def __init__(self, amqp_url: str) -> None:
        self._url = amqp_url
        self._conn: Any = None
        self._channel: Any = None
        self._exchanges: dict[str, Any] = {}
        self._lock = asyncio.Lock()

    async def _ensure_channel(self) -> Any:
        aio_pika = _require_aio_pika()
        if self._channel is not None:
            return aio_pika
        async with self._lock:
            if self._channel is not None:
                return aio_pika
            self._conn = await aio_pika.connect_robust(self._url)
            self._channel = await self._conn.channel()
            return aio_pika

    async def _ensure_exchange(self, exchange_name: str) -> Any:
        aio_pika = await self._ensure_channel()
        if exchange_name in self._exchanges:
            return self._exchanges[exchange_name]
        assert self._channel is not None
        ex = await self._channel.declare_exchange(
            exchange_name,
            aio_pika.ExchangeType.TOPIC,
            durable=True,
        )
        self._exchanges[exchange_name] = ex
        return ex

    async def publish(
        self,
        *,
        exchange_name: str,
        routing_key: str,
        body: dict[str, Any],
    ) -> None:
        aio_pika = await self._ensure_channel()
        ex = await self._ensure_exchange(exchange_name)
        msg = aio_pika.Message(
            body=json.dumps(body, ensure_ascii=False).encode("utf-8"),
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            content_type="application/json",
        )
        await ex.publish(msg, routing_key=routing_key)

    async def aclose(self) -> None:
        self._exchanges.clear()
        if self._conn is not None:
            await self._conn.close()
        self._conn = None
        self._channel = None


async def consume_topic_json_forever(
    *,
    amqp_url: str,
    exchange_name: str,
    routing_key_pattern: str,
    queue_name: str,
    prefetch_count: int,
    handler: Callable[[dict[str, Any], str], Awaitable[None]],
) -> None:
    """阻塞消费：每条消息 JSON 解析成功后调用 ``handler(body, routing_key)``。

    由调用方在独立 asyncio Task 中运行，并在进程退出时 cancel 该 Task。
    """
    aio_pika = _require_aio_pika()
    connection = await aio_pika.connect_robust(amqp_url)
    async with connection:
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=prefetch_count)
        exchange = await channel.declare_exchange(
            exchange_name,
            aio_pika.ExchangeType.TOPIC,
            durable=True,
        )
        queue = await channel.declare_queue(queue_name, durable=True, auto_delete=False)
        await queue.bind(exchange, routing_key=routing_key_pattern)

        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                routing_key = message.routing_key or ""
                async with message.process(requeue=False):
                    try:
                        raw = message.body.decode("utf-8")
                        body: Any = json.loads(raw)
                    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                        logger.warning(
                            "amqp_bad_encoding_or_json",
                            extra={"routing_key": routing_key, "error": str(exc)},
                        )
                        continue
                    if not isinstance(body, dict):
                        logger.warning("amqp_body_not_object", extra={"routing_key": routing_key})
                        continue
                    try:
                        await handler(body, routing_key)
                    except Exception:  # noqa: BLE001
                        logger.exception(
                            "amqp_handler_failed",
                            extra={"routing_key": routing_key},
                        )
