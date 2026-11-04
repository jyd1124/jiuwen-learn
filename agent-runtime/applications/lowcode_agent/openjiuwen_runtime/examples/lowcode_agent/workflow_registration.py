# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved

from __future__ import annotations

import copy
from typing import Callable, Iterable


def _normalize_workflow_id(workflow_id: str, workflow_version: str) -> str:
    """Keep workflow ids in raw form so version is appended exactly once later."""
    normalized_id = str(workflow_id or "").strip()
    normalized_version = str(workflow_version or "").strip()
    if not normalized_id or not normalized_version:
        return normalized_id

    suffix = f"_{normalized_version}"
    while normalized_id.endswith(suffix):
        normalized_id = normalized_id[: -len(suffix)]
    return normalized_id


def _attach_workflow_metadata(
    provider: Callable,
    *,
    workflow_id: str,
    workflow_version: str,
    workflow_name: str,
    workflow_description: str,
    input_params,
):
    """Attach the metadata expected by BaseAgent.add_workflows."""
    provider.id = _normalize_workflow_id(workflow_id, workflow_version)
    provider.version = workflow_version
    provider.name = workflow_name
    provider.description = workflow_description
    provider.input_params = input_params
    return provider


def _sanitize_input_params(input_params):
    """Normalize schema to avoid legacy controller choosing optional `query` first.

    Some runtime versions pick `query` before checking `required`. If a workflow has
    required fields like `city` and `query` is only an optional compatibility field,
    keep required fields unchanged and drop optional `query` to prevent mis-routing.
    """
    if not isinstance(input_params, dict):
        return input_params

    schema = copy.deepcopy(input_params)
    properties = schema.get("properties")
    required = schema.get("required")

    if not isinstance(properties, dict):
        return schema
    if not isinstance(required, list):
        return schema

    required_keys = [key for key in required if isinstance(key, str)]
    if not required_keys:
        return schema

    has_non_query_required = any(key != "query" for key in required_keys)
    query_is_optional = "query" in properties and "query" not in required_keys
    if has_non_query_required and query_is_optional:
        properties.pop("query", None)

    return schema


def normalize_workflow_providers_for_agent(
    workflow_providers: Iterable[tuple[object, Callable]],
) -> list[Callable]:
    """Convert runtime compiler output to the provider shape expected by BaseAgent.add_workflows.

    Runtime compiler already returns async workflow providers. Passing them through WorkflowFactory
    adds an extra callable layer, and ResourceMgr only awaits plain async functions. Returning the
    provider directly keeps registration concurrency-safe and retrievable.
    """
    normalized = []
    for workflow_card, workflow_provider in workflow_providers:
        normalized.append(
            _attach_workflow_metadata(
                workflow_provider,
                workflow_id=workflow_card.id,
                workflow_version=workflow_card.version,
                workflow_name=workflow_card.name,
                workflow_description=workflow_card.description or "",
                input_params=_sanitize_input_params(workflow_card.input_params),
            )
        )
    return normalized
