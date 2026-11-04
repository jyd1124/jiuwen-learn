#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved

# -*- coding: UTF-8 -*-

from __future__ import annotations

from typing import Any, Dict, Optional

from openjiuwen_studio.core.common.dsl import (
    McpConfig,
    McpTransport,
    Param,
    PluginCodeConfig,
    PluginType,
    RestfulApiSchema,
)
from openjiuwen_studio.core.executor.plugin.plugin_mgr import PluginManager
from openjiuwen_studio.core.executor.plugin.plugin_tools import CodeTool, McpTool, ServiceTool


def _normalize_plugin_type(plugin_type: Any) -> str:
    value = str(plugin_type or "").strip().lower()
    if value in {"1", "service", "api", "cloud_api", "plugin_type_cloud_api"}:
        return PluginType.SERVICE
    if value in {"3", "mcp", "plugin_type_mcp"}:
        return PluginType.MCP
    return PluginType.CODE


def _normalize_param_type(raw: Any) -> str:
    mapping = {
        1: "string",
        2: "integer",
        3: "number",
        4: "boolean",
        5: "object",
        6: "array",
        "1": "string",
        "2": "integer",
        "3": "number",
        "4": "boolean",
        "5": "object",
        "6": "array",
        "string": "string",
        "int": "integer",
        "integer": "integer",
        "float": "number",
        "number": "number",
        "bool": "boolean",
        "boolean": "boolean",
        "object": "object",
        "array": "array",
    }
    return mapping.get(raw, "string")


def _build_param(param: Dict[str, Any]) -> Param:
    return Param(
        name=str(param.get("name") or ""),
        description=str(param.get("desc") or param.get("description") or ""),
        type=_normalize_param_type(param.get("type")),
        required=bool(param.get("is_required") or param.get("required")),
        default_value=param.get("value"),
        method=str(param.get("method") or ""),
        runtime=bool(param.get("is_runtime", True)),
    )


def _build_code_tool(tool_data: Dict[str, Any]) -> CodeTool:
    return CodeTool(
        PluginCodeConfig(
            tool_id=str(tool_data.get("tool_id") or tool_data.get("id") or ""),
            name=str(tool_data.get("name") or ""),
            description=str(tool_data.get("desc") or tool_data.get("description") or ""),
            language=str(tool_data.get("language") or "python"),
            code=str(tool_data.get("code") or ""),
            input_params=[_build_param(p) for p in (tool_data.get("request_params") or tool_data.get("inputs") or [])],
        )
    )


def _build_service_tool(plugin_data: Dict[str, Any], tool_data: Dict[str, Any]) -> ServiceTool:
    method_map = {
        "1": "GET",
        "2": "POST",
        "3": "PUT",
        "4": "DELETE",
        "get": "GET",
        "post": "POST",
        "put": "PUT",
        "delete": "DELETE",
        "patch": "PATCH",
    }
    base_url = str(plugin_data.get("url") or plugin_data.get("base_url") or "").rstrip("/")
    path = str(tool_data.get("path") or tool_data.get("url") or "")
    if base_url and path and not path.startswith(("http://", "https://")):
        path = f"{base_url}/{path.lstrip('/')}"

    return ServiceTool(
        RestfulApiSchema(
            tool_id=str(tool_data.get("tool_id") or tool_data.get("id") or ""),
            name=str(tool_data.get("name") or ""),
            description=str(tool_data.get("desc") or tool_data.get("description") or ""),
            params=[_build_param(p) for p in (tool_data.get("request_params") or tool_data.get("inputs") or [])],
            path=path,
            headers=dict(tool_data.get("headers") or plugin_data.get("headers") or {}),
            method=method_map.get(
                str(tool_data.get("method") or "").lower(),
                str(tool_data.get("method") or "GET").upper(),
            ),
            response=[_build_param(p) for p in (tool_data.get("response_params") or tool_data.get("outputs") or [])],
        )
    )


def _build_mcp_tool(plugin_data: Dict[str, Any], tool_data: Dict[str, Any]) -> McpTool:
    transport_raw = str(tool_data.get("transport") or plugin_data.get("transport") or "stdio").lower()
    transport_map = {
        "stdio": McpTransport.STDIO,
        "sse": McpTransport.SSE,
        "streamable_http": McpTransport.STREAMABLE_HTTP,
        "streamable-http": McpTransport.STREAMABLE_HTTP,
        "openapi": McpTransport.OPENAPI,
        "playwright": McpTransport.PLAYWRIGHT,
    }
    return McpTool(
        McpConfig(
            tool_id=str(tool_data.get("tool_id") or tool_data.get("id") or ""),
            name=str(tool_data.get("name") or ""),
            description=str(tool_data.get("desc") or tool_data.get("description") or ""),
            transport=transport_map.get(transport_raw, McpTransport.STDIO),
            url=str(tool_data.get("url") or plugin_data.get("url") or ""),
            headers=dict(tool_data.get("headers") or plugin_data.get("headers") or {}),
            params=dict(tool_data.get("params") or plugin_data.get("params") or {}),
            mcp_tool_name=str(tool_data.get("mcp_tool_name") or tool_data.get("name") or ""),
            input_params=[_build_param(p) for p in (tool_data.get("request_params") or tool_data.get("inputs") or [])],
        )
    )


class RuntimePluginManager(PluginManager):
    """IR-backed PluginManager aligned with the standard Agent.compile workflow."""

    def __init__(self, export_data: Dict[str, Any]) -> None:
        super().__init__()
        plugins = export_data.get("dependencies", {}).get("plugins") or []
        self._tool_index: Dict[tuple[str, str, str], tuple[Dict[str, Any], Dict[str, Any], str]] = {}
        self._plugin_default_tool: Dict[tuple[str, str], tuple[Dict[str, Any], Dict[str, Any], str]] = {}

        for plugin in plugins:
            if not isinstance(plugin, dict):
                continue
            plugin_id = str(plugin.get("plugin_id") or plugin.get("id") or "")
            version = str(plugin.get("plugin_version") or plugin.get("version") or "draft")
            plugin_type = _normalize_plugin_type(plugin.get("plugin_type"))
            tools = (plugin.get("tools") or plugin.get("tool_list") or [])
            for tool in tools:
                if not isinstance(tool, dict):
                    continue
                tool_id = str(tool.get("tool_id") or tool.get("id") or "")
                if not tool_id:
                    continue
                self._tool_index[(plugin_id, tool_id, version)] = (plugin, tool, plugin_type)
                self._tool_index[(plugin_id, tool_id, "")] = (plugin, tool, plugin_type)
                self._plugin_default_tool.setdefault((plugin_id, version), (plugin, tool, plugin_type))
                self._plugin_default_tool.setdefault((plugin_id, ""), (plugin, tool, plugin_type))

    async def get_tool(
        self,
        tool_id: str,
        space_id: str,
        plugin_id: str,
        version: str,
        current_user: Optional[Dict[str, Any]],
    ) -> Any:
        record = self._tool_index.get((plugin_id, tool_id, str(version or "draft")))
        if record is None:
            record = self._tool_index.get((plugin_id, tool_id, ""))
        if record is None and tool_id == plugin_id:
            # Runtime-adapted PluginSchema currently carries plugin_id in the `id`
            # field, while Agent.compile asks PluginManager.get_tool(id=plugin_schema.id).
            # Fall back to the plugin's first exported tool so runtime matches the
            # Studio Agent.compile contract for single-tool plugins.
            record = self._plugin_default_tool.get((plugin_id, str(version or "draft")))
        if record is None and tool_id == plugin_id:
            record = self._plugin_default_tool.get((plugin_id, ""))
        if record is None:
            raise ValueError(
                f"Runtime plugin tool not found: plugin_id={plugin_id}, "
                f"tool_id={tool_id}, version={version}"
            )

        plugin_data, tool_data, plugin_type = record
        if plugin_type == PluginType.SERVICE:
            return _build_service_tool(plugin_data, tool_data)
        if plugin_type == PluginType.MCP:
            return _build_mcp_tool(plugin_data, tool_data)
        return _build_code_tool(tool_data)
