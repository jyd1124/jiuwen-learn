# coding: utf-8
# pylint: disable=protected-access
# 说明：本文件为单元测试，需要访问 VersatileAdapterRunner 的 protected 成员
# （如 _match_workflow 等）。
"""VersatileAdapterRunner 路由匹配测试。

验证 YAML 加载、controller/workflow 选择、target 匹配优先级。
"""
from __future__ import annotations

import pytest

import dispatcher.runner as runner_module
from adapters.versatile_controller import VersatileController
from adapters.versatile_workflow import VersatileWorkflow
from dispatcher.runner import VersatileAdapterRunner


# ════════════════════════════════════════════════════════════════════
# 默认配置路径解析
# ════════════════════════════════════════════════════════════════════


def test_resolve_default_config_path_uses_env(monkeypatch, tmp_path):
    custom_path = tmp_path / "custom.yaml"
    monkeypatch.setenv("VERSATILE_PROXY_CONFIG_PATH", str(custom_path))

    assert runner_module._resolve_default_config_path() == custom_path


def test_resolve_default_config_path_uses_deploy_when_exists(monkeypatch, tmp_path):
    deploy_path = tmp_path / "deploy.yaml"
    local_path = tmp_path / "local.yaml"
    deploy_path.write_text("adapters: []", encoding="utf-8")
    monkeypatch.delenv("VERSATILE_PROXY_CONFIG_PATH", raising=False)
    monkeypatch.setattr(runner_module, "_DEPLOY_DEFAULT_CONFIG_PATH", deploy_path)
    monkeypatch.setattr(runner_module, "_LOCAL_DEFAULT_CONFIG_PATH", local_path)

    assert runner_module._resolve_default_config_path() == deploy_path


def test_resolve_default_config_path_falls_back_to_local(monkeypatch, tmp_path):
    deploy_path = tmp_path / "missing.yaml"
    local_path = tmp_path / "local.yaml"
    monkeypatch.delenv("VERSATILE_PROXY_CONFIG_PATH", raising=False)
    monkeypatch.setattr(runner_module, "_DEPLOY_DEFAULT_CONFIG_PATH", deploy_path)
    monkeypatch.setattr(runner_module, "_LOCAL_DEFAULT_CONFIG_PATH", local_path)

    assert runner_module._resolve_default_config_path() == local_path


# YAML 加载
# ════════════════════════════════════════════════════════════════════


def test_explicit_config_path_takes_priority_over_env(monkeypatch, write_yaml, tmp_path):
    env_path = tmp_path / "env.yaml"
    monkeypatch.setenv("VERSATILE_PROXY_CONFIG_PATH", str(env_path))

    runner = VersatileAdapterRunner(config_path=write_yaml())

    assert [a.name for a in runner._adapters] == [
        "default_controller",
        "wf_knowledge_qa",
        "wf_wealth",
    ]


def test_workflow_defaults_are_inherited_by_workflow_adapter(write_yaml):
    runner = VersatileAdapterRunner(config_path=write_yaml("""
workflow_defaults:
  url_template: "http://mock-host/v1/workflows/{workflow_id}/conversations/{conversation_id}"
  timeout: 45
  headers_template:
    Accept: "text/event-stream"
  forward_header_whitelist:
    - x-user-id
adapters:
  - name: default_controller
    type: controller
    url_template: "http://mock-host/v1/agents/agent-a/conversations/{conversation_id}"
  - name: wf_wealth
    type: workflow
    workflow_id: wf_wealth
    intent: "理财推荐"
"""))

    workflow = next(a for a in runner._adapters if a.name == "wf_wealth")
    assert workflow.url_template == "http://mock-host/v1/workflows/{workflow_id}/conversations/{conversation_id}"
    assert workflow.timeout == 45
    assert workflow.headers_template == {"Accept": "text/event-stream"}
    assert workflow.forward_header_whitelist == {"x-user-id"}


def test_workflow_adapter_overrides_workflow_defaults(write_yaml):
    runner = VersatileAdapterRunner(config_path=write_yaml("""
workflow_defaults:
  url_template: "http://default/workflows/{workflow_id}/conversations/{conversation_id}"
  timeout: 600
  headers_template:
    Accept: "text/event-stream"
    X-App-Code: "common"
  forward_header_whitelist:
    - x-user-id
adapters:
  - name: default_controller
    type: controller
    url_template: "http://mock-host/v1/agents/agent-a/conversations/{conversation_id}"
  - name: wf_special
    type: workflow
    workflow_id: wf_special
    intent: special
    url_template: "http://special/workflows/{workflow_id}/conversations/{conversation_id}"
    timeout: 120
    headers_template:
      X-App-Code: "special"
      X-Scene: "special-flow"
    forward_header_whitelist:
      - authorization
"""))

    workflow = next(a for a in runner._adapters if a.name == "wf_special")
    assert workflow.url_template == "http://special/workflows/{workflow_id}/conversations/{conversation_id}"
    assert workflow.timeout == 120
    assert workflow.headers_template == {
        "Accept": "text/event-stream",
        "X-App-Code": "special",
        "X-Scene": "special-flow",
    }
    assert workflow.forward_header_whitelist == {"authorization"}


def test_workflow_defaults_do_not_apply_to_controller(write_yaml):
    runner = VersatileAdapterRunner(config_path=write_yaml("""
workflow_defaults:
  url_template: "http://default/workflows/{workflow_id}/conversations/{conversation_id}"
  timeout: 45
  headers_template:
    X-App-Code: "workflow-default"
adapters:
  - name: default_controller
    type: controller
    url_template: "http://controller/conversations/{conversation_id}"
    timeout: 60
  - name: wf_wealth
    type: workflow
    workflow_id: wf_wealth
    intent: "理财推荐"
"""))

    controller = runner._controller_cfg
    assert controller.url_template == "http://controller/conversations/{conversation_id}"
    assert controller.timeout == 60
    assert controller.headers_template == {}


def test_load_yaml_creates_all_adapters(write_yaml):
    """YAML 中 3 个 adapter 都被加载，名称类型正确。"""
    runner = VersatileAdapterRunner(config_path=write_yaml())
    names = [a.name for a in runner._adapters]
    assert names == ["default_controller", "wf_knowledge_qa", "wf_wealth"]
    types = [a.type for a in runner._adapters]
    assert types == ["controller", "workflow", "workflow"]


def test_load_yaml_controller_is_resolved(write_yaml):
    """第一个 type=controller 的 adapter 被选为 _controller_cfg。"""
    runner = VersatileAdapterRunner(config_path=write_yaml())
    assert runner._controller_cfg is not None
    assert runner._controller_cfg.name == "default_controller"


def test_missing_yaml_falls_back_to_settings_controller(tmp_path):
    """YAML 不存在时回退到 _build_from_settings，只生成 default_controller。"""
    missing = tmp_path / "no-such.yaml"
    runner = VersatileAdapterRunner(config_path=missing)
    assert len(runner._adapters) == 1
    assert runner._adapters[0].name == "default_controller"
    assert runner._adapters[0].type == "controller"


def test_header_whitelist_lowered_and_set(write_yaml):
    """forward_header_whitelist 被转为小写 set。"""
    runner = VersatileAdapterRunner(config_path=write_yaml())
    controller = runner._adapters[0]
    assert controller.forward_header_whitelist == {"x-user-id", "cookie"}


# ════════════════════════════════════════════════════════════════════
# 路由匹配
# ════════════════════════════════════════════════════════════════════


@pytest.fixture
def runner(write_yaml):
    return VersatileAdapterRunner(config_path=write_yaml())


def test_match_by_workflow_id(runner):
    cfg = runner._match_workflow({"workflow_id": "wf_knowledge_qa"})
    assert cfg is not None
    assert cfg.name == "wf_knowledge_qa"


def test_match_by_intent(runner):
    cfg = runner._match_workflow({"intent": "knowledge_qa"})
    assert cfg is not None
    assert cfg.name == "wf_knowledge_qa"


def test_match_chinese_intent(runner):
    cfg = runner._match_workflow({"intent": "理财推荐"})
    assert cfg is not None
    assert cfg.name == "wf_wealth"


def test_no_match_returns_none_then_controller_fallback(runner):
    cfg = runner._match_workflow({"intent": "unknown_intent"})
    assert cfg is None  # 后续 run_async 会用 _controller_cfg 兜底


def test_workflow_id_takes_priority_over_intent_when_distinct(runner):
    """workflow_id 与 intent 都给但分别匹配不同 adapter：按代码逻辑取首个命中。"""
    # workflow_id 在循环里先于 intent 判定，应命中 wf_knowledge_qa
    cfg = runner._match_workflow({"workflow_id": "wf_knowledge_qa", "intent": "理财推荐"})
    assert cfg is not None
    assert cfg.name == "wf_knowledge_qa"


def test_empty_target_returns_none(runner):
    cfg = runner._match_workflow({})
    assert cfg is None


# ════════════════════════════════════════════════════════════════════
# Adapter 实例创建
# ════════════════════════════════════════════════════════════════════


def test_create_workflow_adapter_instance(runner):
    cfg = runner._match_workflow({"intent": "knowledge_qa"})
    adapter = runner._create_adapter(cfg)
    assert isinstance(adapter, VersatileWorkflow)
    # 内部字段
    assert adapter._workflow_id == "wf_knowledge_qa"
    assert adapter._workflow_result_node == "WorkflowQAResponseNode"
    assert adapter._timeout == 30


def test_create_controller_adapter_instance(runner):
    adapter = runner._create_adapter(runner._controller_cfg)
    assert isinstance(adapter, VersatileController)
    assert adapter._workflow_result_node == "GXZQAResponseNode"

