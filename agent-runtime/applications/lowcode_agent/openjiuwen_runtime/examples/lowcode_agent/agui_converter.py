#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved

# -*- coding: UTF-8 -*-

"""
AG-UI converter for agent streaming chunks.

Converts low-level streaming chunks (TraceSchema / OutputSchema) into
AG-UI SSE event objects, where each event maps to one ``data: <json>`` line.

"""

from __future__ import annotations

import json
import os
import uuid
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any, Dict, List, Optional


_AGUI_TEXT_DELTA_FLUSH_CHARS = max(1, int(os.environ.get("AGUI_TEXT_DELTA_FLUSH_CHARS", "24")))


def _agui_flush_tail_markers() -> tuple[str, ...]:
    raw = os.environ.get(
        "AGUI_TEXT_DELTA_FLUSH_ON_TAIL",
        ".,,,，,。,！,？,!,?,；,;,\n",
    )
    out: list[str] = []
    for marker in raw.split(","):
        m = marker.strip()
        if m:
            out.append(m)
    return tuple(out)


_AGUI_TEXT_DELTA_FLUSH_ON_TAIL = _agui_flush_tail_markers()


@dataclass
class AgUiStreamState:
    """AG-UI SSE event assembly state (one per run/conversation)."""

    thread_id: str
    run_id: str = field(default_factory=lambda: f"run_{uuid.uuid4().hex}")
    started: bool = False
    text_message_id: str = field(default_factory=lambda: f"msg_{uuid.uuid4().hex}")
    text_started: bool = False
    text_acc: str = ""
    finished: bool = False


def _to_jsonable(obj: Any) -> Any:
    """Recursively convert runtime objects to JSON-serializable values."""
    if obj is None or isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, dict):
        return {str(k): _to_jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_to_jsonable(x) for x in obj]
    model_dump = getattr(obj, "model_dump", None)
    if callable(model_dump):
        try:
            return _to_jsonable(model_dump(mode="json"))
        except TypeError:
            return _to_jsonable(model_dump())
    if hasattr(obj, "__dict__"):
        return {
            str(k): _to_jsonable(v)
            for k, v in vars(obj).items()
            if not str(k).startswith("_")
        }
    return str(obj)


def agui_trace_context(messages: list) -> Any:
    """Lightweight context for AG-UI conversion (`agent_input` feeds RUN_STARTED)."""
    return SimpleNamespace(
        trace_id=None,
        agent_input={"messages": messages, "tools": [], "context": []},
    )


def _get_or_create_agui_state(trace_context: Any, thread_id: str) -> AgUiStreamState:
    if not thread_id:
        thread_id = f"thread_{uuid.uuid4().hex}"

    state = getattr(trace_context, "_agui_state", None)
    if isinstance(state, AgUiStreamState) and state.thread_id == thread_id:
        return state
    state = AgUiStreamState(thread_id=thread_id)
    if getattr(trace_context, "trace_id", None):
        state.run_id = str(getattr(trace_context, "trace_id"))
    setattr(trace_context, "_agui_state", state)
    return state


def convert_chunk_to_agui_events(chunk: Any, trace_context: Any, conversation_id: str) -> List[Dict[str, Any]]:
    """
    Convert low-level streaming chunks to AG-UI SSE events (JSON data lines).

    Returns a list of event objects (the caller wraps them as ``data: <json>``).
    """
    from openjiuwen.core.session.stream.base import TraceSchema, OutputSchema

    state = _get_or_create_agui_state(trace_context, conversation_id)
    events: List[Dict[str, Any]] = []

    if not state.started:
        raw_input = getattr(trace_context, "agent_input", None) or {}
        if not isinstance(raw_input, dict):
            raw_input = {}
        events.append(
            {
                "type": "RUN_STARTED",
                "threadId": state.thread_id,
                "runId": state.run_id,
                "input": {
                    "threadId": state.thread_id,
                    "runId": state.run_id,
                    "messages": raw_input.get("messages", []),
                    "tools": raw_input.get("tools", []),
                    "context": raw_input.get("context", []),
                },
            }
        )
        state.started = True

    def _ensure_text_started() -> None:
        if not state.text_started:
            events.append(
                {"type": "TEXT_MESSAGE_START", "messageId": state.text_message_id, "role": "assistant"}
            )
            state.text_started = True

    def _emit_text_delta(delta: str) -> None:
        if not delta:
            return
        _ensure_text_started()
        state.text_acc += delta
        events.append(
            {"type": "TEXT_MESSAGE_CONTENT", "messageId": state.text_message_id, "delta": delta}
        )

    def _finish_if_possible(final_content: Optional[str] = None) -> None:
        if state.finished:
            return
        if final_content is not None and isinstance(final_content, str) and final_content.strip():
            if not state.text_acc.strip():
                state.text_acc = final_content
        if not state.text_acc.strip():
            return
        _ensure_text_started()
        events.append(
            {
                "type": "TEXT_MESSAGE_END",
                "messageId": state.text_message_id,
                "content": state.text_acc,
            }
        )
        events.append(
            {"type": "RUN_FINISHED", "threadId": state.thread_id, "runId": state.run_id, "result": None}
        )
        state.finished = True

    if isinstance(chunk, OutputSchema):
        payload = getattr(chunk, "payload", None) or {}
        if isinstance(payload, dict):
            if chunk.type in {"llm_output", "workflow_final", "end node stream"}:
                delta = (
                    payload.get("content")
                    or payload.get("output")
                    or payload.get("response")
                    or ""
                )
                if isinstance(delta, dict):
                    delta = json.dumps(delta, ensure_ascii=False, default=str)
                _emit_text_delta(str(delta))
            elif chunk.type == "llm_reasoning":
                rc = payload.get("content") or ""
                if isinstance(rc, str) and rc:
                    _emit_text_delta(rc)
            elif chunk.type == "answer":
                _finish_if_possible(payload.get("output"))
            elif chunk.type == "__interaction__":
                events.append(
                    {
                        "type": "CUSTOM",
                        "name": "REQUIRES_ACTION",
                        "value": {"threadId": state.thread_id, "runId": state.run_id},
                    }
                )

    if isinstance(chunk, TraceSchema) and getattr(chunk, "type", None) == "tracer_agent":
        payload = getattr(chunk, "payload", None) or {}
        final_content: Optional[str] = None
        if isinstance(payload, dict):
            outputs = payload.get("outputs")
            if isinstance(outputs, dict):
                outs = outputs.get("outputs")
                if isinstance(outs, list) and outs:
                    first = outs[0]
                    if isinstance(first, dict) and isinstance(first.get("content"), str):
                        final_content = first.get("content")
        if final_content:
            _finish_if_possible(final_content)
        else:
            end_time = payload.get("end_time") if isinstance(payload, dict) else None
            if end_time:
                _finish_if_possible(None)

    if isinstance(chunk, TraceSchema) and getattr(chunk, "type", None) == "tracer_workflow":
        payload = getattr(chunk, "payload", None) or {}
        if isinstance(payload, dict) and payload.get("status") == "finish":
            component_type = str(payload.get("componentType", "") or payload.get("component_type", "")).lower()
            outputs = payload.get("outputs")
            if isinstance(outputs, dict):
                if component_type == "llmexecutable":
                    delta = outputs.get("output")
                    if delta:
                        if isinstance(delta, dict):
                            delta = json.dumps(delta, ensure_ascii=False, default=str)
                        _emit_text_delta(str(delta))
                elif component_type == "end" or str(payload.get("invokeId", "")).startswith("end_"):
                    pass

    return [_to_jsonable(event) for event in events]


def flush_buffered_agui_text_events(
    buffered_text_event: dict[str, Any] | None,
    buffered_text_delta: str,
) -> tuple[list[dict[str, Any]], dict[str, Any] | None, str]:
    if not buffered_text_event or not buffered_text_delta:
        return [], buffered_text_event, buffered_text_delta
    flushed_event = dict(buffered_text_event)
    flushed_event["delta"] = buffered_text_delta
    return [flushed_event], None, ""


def merge_agui_events_for_stream(
    events: list[dict[str, Any]],
    buffered_text_event: dict[str, Any] | None,
    buffered_text_delta: str,
    *,
    force_flush: bool = False,
) -> tuple[list[dict[str, Any]], dict[str, Any] | None, str]:
    """
    Merge consecutive AG-UI text delta events into larger chunks.

    Long model responses may produce thousands of tiny ``TEXT_MESSAGE_CONTENT``
    events. Batching them keeps the stream responsive for downstream proxies and
    UIs without changing the AG-UI protocol.
    """
    merged_events: list[dict[str, Any]] = []

    for event in events:
        if not isinstance(event, dict):
            continue

        if event.get("type") == "TEXT_MESSAGE_CONTENT":
            delta = event.get("delta", "")
            if not isinstance(delta, str) or not delta:
                continue

            if buffered_text_event is None:
                buffered_text_event = dict(event)
                buffered_text_delta = delta
            else:
                buffered_text_delta += delta

            should_flush = len(buffered_text_delta) >= _AGUI_TEXT_DELTA_FLUSH_CHARS
            if not should_flush and delta:
                should_flush = any(delta.endswith(marker) for marker in _AGUI_TEXT_DELTA_FLUSH_ON_TAIL if marker)

            if should_flush:
                flushed, buffered_text_event, buffered_text_delta = flush_buffered_agui_text_events(
                    buffered_text_event,
                    buffered_text_delta,
                )
                merged_events.extend(flushed)
            continue

        flushed, buffered_text_event, buffered_text_delta = flush_buffered_agui_text_events(
            buffered_text_event,
            buffered_text_delta,
        )
        merged_events.extend(flushed)
        merged_events.append(event)

    if force_flush:
        flushed, buffered_text_event, buffered_text_delta = flush_buffered_agui_text_events(
            buffered_text_event,
            buffered_text_delta,
        )
        merged_events.extend(flushed)

    return merged_events, buffered_text_event, buffered_text_delta


def finalize_agui_stream(trace_context: Any, conversation_id: str) -> List[Dict[str, Any]]:
    """
    Call at the end of streaming iteration to emit missing end events when needed
    (for example, if upstream did not emit an ``answer`` chunk).
    """
    state = getattr(trace_context, "_agui_state", None)
    if not isinstance(state, AgUiStreamState) or state.finished:
        return []
    events: List[Dict[str, Any]] = []
    if state.text_acc.strip():
        if not state.text_started:
            events.append(
                {"type": "TEXT_MESSAGE_START", "messageId": state.text_message_id, "role": "assistant"}
            )
            state.text_started = True
        events.append(
            {
                "type": "TEXT_MESSAGE_END",
                "messageId": state.text_message_id,
                "content": state.text_acc,
            }
        )
    events.append(
        {"type": "RUN_FINISHED", "threadId": state.thread_id, "runId": state.run_id, "result": None}
    )
    state.finished = True
    return [_to_jsonable(event) for event in events]


def agui_error_events(
    trace_context: Any,
    conversation_id: str,
    message: str,
    *,
    code: str = "0101",
) -> List[Dict[str, Any]]:
    """
    Emit a terminal AG-UI error sequence.

    If part of the assistant text has already streamed, append the error message
    to that text so the UI does not get stuck showing only the partial output.
    """
    state = _get_or_create_agui_state(trace_context, conversation_id)
    events: List[Dict[str, Any]] = []

    if not state.started:
        raw_input = getattr(trace_context, "agent_input", None) or {}
        if not isinstance(raw_input, dict):
            raw_input = {}
        events.append(
            {
                "type": "RUN_STARTED",
                "threadId": state.thread_id,
                "runId": state.run_id,
                "input": {
                    "threadId": state.thread_id,
                    "runId": state.run_id,
                    "messages": raw_input.get("messages", []),
                    "tools": raw_input.get("tools", []),
                    "context": raw_input.get("context", []),
                },
            }
        )
        state.started = True

    text = (message or "").strip()
    if text:
        if state.text_acc.strip():
            state.text_acc = f"{state.text_acc.rstrip()}\n\n{text}"
        else:
            state.text_acc = text

    if state.text_acc.strip():
        if not state.text_started:
            events.append(
                {"type": "TEXT_MESSAGE_START", "messageId": state.text_message_id, "role": "assistant"}
            )
            state.text_started = True
        events.append(
            {
                "type": "TEXT_MESSAGE_END",
                "messageId": state.text_message_id,
                "content": state.text_acc,
            }
        )

    events.append(
        {
            "type": "RUN_ERROR",
            "message": text or "Agent execution failed",
            "code": code,
        }
    )
    state.finished = True
    return [_to_jsonable(event) for event in events]


def agui_assistant_text_as_answer_events(
    trace_context: Any,
    conversation_id: str,
    assistant_text: str,
) -> List[Dict[str, Any]]:
    """
    Convert a fixed assistant text into AG-UI events
    (RUN_STARTED + text + RUN_FINISHED).
    Useful for validation failures or exception messages when no low-level chunk
    stream is available.
    """
    from openjiuwen.core.session.stream.base import OutputSchema

    chunk = OutputSchema(
        type="answer",
        index=0,
        payload={"output": assistant_text, "result_type": "answer"},
    )
    out = convert_chunk_to_agui_events(chunk, trace_context, conversation_id)
    out.extend(finalize_agui_stream(trace_context, conversation_id))
    return out


def agui_append_text_and_finish_events(
    trace_context: Any,
    conversation_id: str,
    assistant_text: str,
) -> List[Dict[str, Any]]:
    """
    Append assistant text to any existing partial output and finish the run.
    """
    from openjiuwen.core.session.stream.base import OutputSchema

    state = _get_or_create_agui_state(trace_context, conversation_id)
    text = assistant_text or ""
    if state.text_acc.strip() and text and not text.startswith("\n"):
        text = f"\n\n{text}"

    chunk = OutputSchema(
        type="workflow_final",
        index=0,
        payload={"output": text, "result_type": "answer"},
    )
    out = convert_chunk_to_agui_events(chunk, trace_context, conversation_id)
    out.extend(finalize_agui_stream(trace_context, conversation_id))
    return out
