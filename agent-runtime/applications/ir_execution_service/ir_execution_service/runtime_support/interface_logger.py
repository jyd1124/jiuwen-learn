# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved

from __future__ import annotations

import contextvars
import json
import logging
import os
import socket
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

from openjiuwen_runtime.foundation.log import get_logger
from .core_log_bridge import CoreLogEvent, register_core_log_sink

_LOG = get_logger(__name__)

_REQUEST_ID: contextvars.ContextVar[str] = contextvars.ContextVar("interface_request_id", default="")
_SERVER_SOURCE_IP: contextvars.ContextVar[str] = contextvars.ContextVar("interface_server_source_ip", default="")

_TOOL_INTERFACE_T0: dict[str, float] = {}
_RUNNER_TOOL_INTERFACE_REGISTERED = False

_RUNNER_LLM_STREAM_INTERFACE_REGISTERED = False


def _llm_stream_output_looks_terminal(*, result: Any) -> bool:
    """判断是否为单次流式 HTTP 的收尾 payload（避免对 per_item 的每个 chunk 打 interface）。"""
    if isinstance(result, list):
        return True
    if result is None:
        return False
    if getattr(result, "usage_metadata", None) is not None:
        return True
    fr = getattr(result, "finish_reason", None)
    if fr is None:
        return False
    s = str(fr).strip().lower()
    return bool(s) and s not in ("null", "none")


def _model_name_from_configs(*, model_config: Any, model_client_config: Any) -> str:
    if model_config is not None:
        mn = getattr(model_config, "model_name", None)
        if mn:
            return str(mn)
        m = getattr(model_config, "model", None)
        if m:
            return str(m)
    return ""


def _model_provider_str(model_client_config: Any) -> str:
    if model_client_config is None:
        return ""
    cp = getattr(model_client_config, "client_provider", None)
    if cp is None:
        return ""
    return str(getattr(cp, "value", cp))


def set_request_context(*, request_id: str, source_ip: str = "") -> None:
    _REQUEST_ID.set(str(request_id or "").strip())
    _SERVER_SOURCE_IP.set(str(source_ip or "").strip())


def get_request_id() -> str:
    return str(_REQUEST_ID.get() or "").strip()


def _get_server_source_ip() -> str:
    return str(_SERVER_SOURCE_IP.get() or "").strip()


def _now_resp_time_str() -> str:
    dt = datetime.now(timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S.") + f"{int(dt.microsecond / 1000):03d}"


def _is_loopback(ip: str) -> bool:
    ip = (ip or "").strip()
    return ip in {"127.0.0.1", "::1"} or ip.startswith("127.")


def _resolve_local_ip() -> str:
    env_ip = (os.environ.get("IP") or "").strip()
    if env_ip and not _is_loopback(env_ip):
        return env_ip
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
            return ip if ip and not _is_loopback(ip) else ""
        finally:
            s.close()
    except Exception:
        try:
            ip = socket.gethostbyname(socket.gethostname())
            return ip if ip and not _is_loopback(ip) else ""
        except Exception:
            return ""


def _safe_ms(ms: float) -> str:
    return str(int(max(0.0, ms)))


def _env_int(name: str, default: int) -> int:
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
        return value if value > 0 else default
    except Exception:
        return default


@dataclass(frozen=True, slots=True)
class InterfaceLogRecord:
    version: str
    resp_time: str
    type: str
    interface_name: str
    source_ip: str
    dest_ip: str
    cost_time: str
    flag: bool
    return_code: int
    request_id: str
    return_info: str
    add_info: dict[str, Any]

    def to_json_line(self) -> str:
        return json.dumps(
            {
                "version": self.version,
                "respTime": self.resp_time,
                "type": self.type,
                "interface_name": self.interface_name,
                "source_ip": self.source_ip,
                "dest_ip": self.dest_ip,
                "cost_time": self.cost_time,
                "flag": self.flag,
                "return_code": self.return_code,
                "request_id": self.request_id,
                "return_info": self.return_info,
                "add_info": self.add_info or {},
            },
            ensure_ascii=False,
            default=str,
        )


class InterfaceLogger:
    def __init__(self, *, log_file: Path) -> None:
        self._logger = logging.getLogger("ir_execution_service.interface")
        self._logger.setLevel(logging.INFO)
        self._logger.propagate = False

        for h in list(self._logger.handlers):
            self._logger.removeHandler(h)
            try:
                h.close()
            except Exception as exc:
                _LOG.warning("failed to close logging handler: %s", exc)

        log_file.parent.mkdir(parents=True, exist_ok=True)
        max_bytes = _env_int("LOWCODE_INTERFACE_LOG_MAX_BYTES", 20 * 1024 * 1024)
        backup_count = _env_int("LOWCODE_INTERFACE_LOG_BACKUP_COUNT", 20)
        handler = RotatingFileHandler(
            filename=str(log_file),
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding="utf-8",
        )
        handler.setLevel(logging.INFO)
        handler.setFormatter(logging.Formatter("%(message)s"))
        self._logger.addHandler(handler)
        _LOG.info("Interface logger ready: %s", log_file)

    def write(self, record: InterfaceLogRecord) -> None:
        self._logger.info(record.to_json_line())


_INTERFACE: InterfaceLogger | None = None


def init_interface_logger_from_env() -> InterfaceLogger | None:
    global _INTERFACE
    if _INTERFACE is not None:
        return _INTERFACE
    raw = (
        os.environ.get("LOWCODE_INTERFACE_LOG_PATH")
        or os.environ.get("LOWCODE_DFX_LOG_DIR")
        or os.environ.get("DFX_LOG_DIR")
        or ""
    ).strip()
    if not raw:
        _LOG.info("Interface logger disabled (LOWCODE_INTERFACE_LOG_PATH not set).")
        return None
    p = Path(raw).expanduser().resolve()
    log_file = p / "interface.log" if p.suffix.lower() != ".log" else p
    _INTERFACE = InterfaceLogger(log_file=log_file)
    return _INTERFACE


def log_server(
    *,
    interface_name: str,
    cost_ms: float,
    ok: bool,
    return_code: int,
    return_info: str,
    source_ip: str | None = None,
    dest_ip: str | None = None,
    add_info: dict[str, Any] | None = None,
) -> None:
    if _INTERFACE is None:
        return
    src = _get_server_source_ip() if source_ip is None else str(source_ip or "").strip()
    dst = _resolve_local_ip() if dest_ip is None else str(dest_ip or "").strip()
    _INTERFACE.write(
        InterfaceLogRecord(
            version=(os.environ.get("LOWCODE_IR_EXECUTION_SERVICE_VERSION") or "").strip(),
            resp_time=_now_resp_time_str(),
            type="server",
            interface_name=str(interface_name),
            source_ip=src,
            dest_ip=dst,
            cost_time=_safe_ms(cost_ms),
            flag=bool(ok),
            return_code=int(return_code),
            request_id=get_request_id(),
            return_info=str(return_info or ""),
            add_info=add_info or {},
        )
    )


def log_client(
    *,
    interface_name: str,
    cost_ms: float,
    ok: bool,
    return_code: int,
    return_info: str,
    dest_ip: str = "",
    add_info: dict[str, Any] | None = None,
) -> None:
    if _INTERFACE is None:
        return
    src = _resolve_local_ip()
    dst = str(dest_ip or "").strip()
    _INTERFACE.write(
        InterfaceLogRecord(
            version=(os.environ.get("LOWCODE_IR_EXECUTION_SERVICE_VERSION") or "").strip(),
            resp_time=_now_resp_time_str(),
            type="client",
            interface_name=str(interface_name),
            source_ip=src,
            dest_ip=dst,
            cost_time=_safe_ms(cost_ms),
            flag=bool(ok),
            return_code=int(return_code),
            request_id=get_request_id(),
            return_info=str(return_info or ""),
            add_info=add_info or {},
        )
    )


_CORE_INTERFACE_SINK_INSTALLED = False


def install_core_interface_sink() -> None:
    global _CORE_INTERFACE_SINK_INSTALLED
    if _CORE_INTERFACE_SINK_INSTALLED:
        return

    def _sink(event: CoreLogEvent) -> None:
        if event.kind != "llm_upstream":
            return
        if event.request_id:
            _REQUEST_ID.set(event.request_id)
        payload = event.payload or {}
        add_info = {
            "event_type": payload.get("event_type"),
            "model_name": payload.get("model_name"),
            "model_provider": payload.get("model_provider"),
            "is_stream": payload.get("is_stream"),
        }
        if isinstance(payload.get("metadata"), dict):
            add_info["metadata"] = payload.get("metadata")
        if payload.get("exception"):
            add_info["exception"] = payload.get("exception")
        if payload.get("error_message"):
            add_info["error_message"] = payload.get("error_message")
        log_client(
            interface_name=event.interface_name or "llm.call",
            cost_ms=0.0,
            ok=bool(event.ok),
            return_code=0 if event.ok else 1,
            return_info=event.return_info,
            dest_ip="",
            add_info=add_info,
        )

    register_core_log_sink(_sink)
    _CORE_INTERFACE_SINK_INSTALLED = True


def install_runner_tool_interface_callbacks() -> None:
    global _RUNNER_TOOL_INTERFACE_REGISTERED
    if _RUNNER_TOOL_INTERFACE_REGISTERED:
        return

    from openjiuwen.core.runner import Runner
    from openjiuwen.core.runner.callback.events import ToolCallEvents

    fw = Runner.callback_framework

    async def _on_tool_started(*, tool_name: str = "", tool_id: str = "", **_: Any) -> None:
        tid = str(tool_id or "").strip()
        if tid:
            _TOOL_INTERFACE_T0[tid] = time.perf_counter()

    async def _on_tool_finished(*, tool_name: str = "", tool_id: str = "", result: Any = None, **_: Any) -> None:
        tid = str(tool_id or "").strip()
        t0 = _TOOL_INTERFACE_T0.pop(tid, None)
        cost_ms = (time.perf_counter() - t0) * 1000.0 if t0 is not None else 0.0
        base_label = ("" if tool_name is None else str(tool_name)).strip() or tid
        log_client(
            interface_name="tool.call",
            cost_ms=cost_ms,
            ok=True,
            return_code=0,
            return_info=f"{base_label} finished" if base_label else "finished",
            dest_ip="",
            add_info={
                "source": "runner_callback",
                "event_type": "tool_call_end",
                "tool_name": tool_name,
                "tool_id": tool_id,
                "has_result": result is not None,
            },
        )

    async def _on_tool_error(
        *,
        tool_name: str = "",
        tool_id: str = "",
        error: BaseException | None = None,
        **_: Any,
    ) -> None:
        tid = str(tool_id or "").strip()
        t0 = _TOOL_INTERFACE_T0.pop(tid, None)
        cost_ms = (time.perf_counter() - t0) * 1000.0 if t0 is not None else 0.0
        err_text = str(error) if error is not None else ""
        base_label = ("" if tool_name is None else str(tool_name)).strip() or tid
        if base_label:
            ret_fail = (
                f"{base_label} failed: {err_text}"
                if err_text
                else f"{base_label} failed"
            )
        else:
            ret_fail = "failed: " + err_text if err_text else "failed"
        log_client(
            interface_name="tool.call",
            cost_ms=cost_ms,
            ok=False,
            return_code=1,
            return_info=ret_fail,
            dest_ip="",
            add_info={
                "source": "runner_callback",
                "event_type": "tool_call_error",
                "tool_name": tool_name,
                "tool_id": tool_id,
                "error": err_text,
            },
        )

    fw.register_sync(ToolCallEvents.TOOL_CALL_STARTED, _on_tool_started, priority=-1000)
    fw.register_sync(ToolCallEvents.TOOL_CALL_FINISHED, _on_tool_finished, priority=-1000)
    fw.register_sync(ToolCallEvents.TOOL_CALL_ERROR, _on_tool_error, priority=-1000)
    _RUNNER_TOOL_INTERFACE_REGISTERED = True
    _LOG.info("Interface runner tool callbacks installed.")


def install_runner_llm_stream_interface_callbacks() -> None:
    """流式 LLM 成功结束时写一条 interface（补充 core 内 stream 无「API response received」日志的空档）。

    Model.stream 对 LLM_STREAM_OUTPUT 使用 emit_after 默认 per_item，每个 chunk 触发一次；
    仅在收尾 chunk（finish_reason / usage_metadata）或 once 模式下的 list 结果时记录，避免刷屏。
    耗时固定为 0（不在此维护起止时钟）。流式失败由 core_log_bridge 写 interface。
    """
    global _RUNNER_LLM_STREAM_INTERFACE_REGISTERED
    if _RUNNER_LLM_STREAM_INTERFACE_REGISTERED:
        return

    from openjiuwen.core.runner import Runner
    from openjiuwen.core.runner.callback.events import LLMCallEvents

    fw = Runner.callback_framework

    async def _on_llm_stream_output(
        *,
        result: Any = None,
        model_config: Any = None,
        model_client_config: Any = None,
        **_: Any,
    ) -> None:
        if not _llm_stream_output_looks_terminal(result=result):
            return
        mn = _model_name_from_configs(
            model_config=model_config, model_client_config=model_client_config
        )
        prov = _model_provider_str(model_client_config)
        add_info: dict[str, Any] = {
            "source": "runner_callback",
            "event_type": "llm_stream_end",
            "is_stream": True,
            "model_name": mn,
            "model_provider": prov,
        }
        if isinstance(result, list):
            add_info["chunk_count"] = len(result)
        log_client(
            interface_name="llm.call",
            cost_ms=0.0,
            ok=True,
            return_code=0,
            return_info="stream completed",
            dest_ip="",
            add_info=add_info,
        )

    fw.register_sync(LLMCallEvents.LLM_STREAM_OUTPUT, _on_llm_stream_output, priority=-1000)
    _RUNNER_LLM_STREAM_INTERFACE_REGISTERED = True
    _LOG.info("Interface runner LLM stream callbacks installed.")

