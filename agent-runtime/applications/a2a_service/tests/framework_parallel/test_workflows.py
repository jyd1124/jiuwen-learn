# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved

"""子 Agent 侧并行工作流：超限全拒 + gather 聚合 + 单工作流生命周期/超时/取消。

关联用例：LIMIT-03（工作流超限直接全拒、不取前 N）、WF-01/02/05（并行提交、
各自 node_end、path=[entity,wf]）、WF-04（workflow_results 结构）、SSE-02/03、
FAIL-03（单工作流失败隔离）、TMO-02（工作流超时）、CANCEL-03（工作流取消）。
"""
# 单元测试以白盒方式直接验证 Executor 的内部实现（受保护成员），
# G.CLS.11（建议级）针对生产封装，不适用于此类白盒测试，故统一豁免。
# pylint: disable=protected-access
from __future__ import annotations

import asyncio

from google.protobuf.json_format import MessageToDict

from common.events import DelegateRequest, MultiDelegateRequest, WorkflowSpec

from tests.framework_parallel._helpers import (
    FakeAsyncStream,
    collect_sub_tasks,
    make_executor,
    make_turn_ctx,
)


def _wfs(*ids):
    return [WorkflowSpec(workflow_id=i, intent=f"intent-{i}", task_description=f"task-{i}") for i in ids]


# ════════════════════════════════════════════════════════════════════
# LIMIT-03：超限直接全拒绝（保护 VA 后端，不进 gather）
# ════════════════════════════════════════════════════════════════════


async def test_multi_delegate_over_limit_rejects_all_without_running():
    executor = make_executor(max_parallel_workflows_per_agent=3)
    called = []

    async def fake_one(spec, turn_ctx, cancel_event):
        called.append(spec.workflow_id)
        return {"workflow_id": spec.workflow_id, "status": "done"}

    executor._run_one_workflow = fake_one
    ctx = make_turn_ctx()

    cascade = await executor._handle_multi_delegate(
        MultiDelegateRequest(workflows=_wfs("w1", "w2", "w3", "w4")), ctx, asyncio.Event()
    )

    assert called == []  # 不进 gather
    results = cascade["workflow_results"]
    assert len(results) == 4
    assert all(r["status"] == "failed" for r in results)
    assert all("超限" in r["error"] for r in results)
    assert all(r["elapsed_ms"] == 0 for r in results)


async def test_multi_delegate_within_limit_aggregates():
    executor = make_executor(max_parallel_workflows_per_agent=3)

    async def fake_one(spec, turn_ctx, cancel_event):
        return {"workflow_id": spec.workflow_id, "status": "done",
                "result": {"url": f"u-{spec.workflow_id}"}, "error": "", "elapsed_ms": 10}

    executor._run_one_workflow = fake_one
    ctx = make_turn_ctx()

    cascade = await executor._handle_multi_delegate(
        MultiDelegateRequest(workflows=_wfs("wa", "wb")), ctx, asyncio.Event()
    )

    results = cascade["workflow_results"]
    assert {r["workflow_id"] for r in results} == {"wa", "wb"}
    assert all(r["status"] == "done" for r in results)


# ════════════════════════════════════════════════════════════════════
# 单工作流生命周期
# ════════════════════════════════════════════════════════════════════


def _set_fake_va(executor, *, returns=None, raises=None, sleep=None):
    async def fake_va(delegate, turn_ctx, wf_path, cancel_event):
        if sleep is not None:
            await asyncio.sleep(sleep)
        if raises is not None:
            raise raises
        return returns

    executor._drive_workflow_va = fake_va


async def test_run_one_workflow_done_path_and_elapsed():
    executor = make_executor()
    _set_fake_va(executor, returns={"url": "https://r", "node_type": "End"})
    ctx = make_turn_ctx(sub_task_path=("A",))

    result = await executor._run_one_workflow(_wfs("wf:a")[0], ctx, asyncio.Event())

    assert result["workflow_id"] == "wf:a"
    assert result["status"] == "done"
    assert result["result"] == {"url": "https://r", "node_type": "End"}
    assert isinstance(result["elapsed_ms"], int)

    envs = collect_sub_tasks(ctx.event_queue)
    # node_start：workflow 节点，path=[entity, workflow_id]，带 intent
    assert envs[0]["node_kind"] == "workflow"
    assert envs[0]["sub_task_path"] == ["A", "wf:a"]
    assert envs[0]["data"] == {"event": "node_start", "intent": "intent-wf:a"}
    # node_end done，result 内含 elapsed_ms
    assert envs[-1]["data"]["event"] == "node_end"
    assert envs[-1]["data"]["status"] == "done"
    assert "elapsed_ms" in envs[-1]["data"]["result"]


async def test_run_one_workflow_failure_isolated():
    executor = make_executor()
    _set_fake_va(executor, raises=RuntimeError("VA报错"))
    ctx = make_turn_ctx(sub_task_path=("A",))

    result = await executor._run_one_workflow(_wfs("wf:c")[0], ctx, asyncio.Event())

    assert result["status"] == "failed"
    assert "VA报错" in result["error"]
    envs = collect_sub_tasks(ctx.event_queue)
    assert envs[-1]["data"]["status"] == "failed"
    assert envs[-1]["sub_task_path"] == ["A", "wf:c"]


async def test_run_one_workflow_no_terminal_result_marks_failed():
    """问题 2：VA 流未给出任何终态（final_result is None）→ 判 failed，不静默 done。"""
    executor = make_executor()
    _set_fake_va(executor, returns=None)
    ctx = make_turn_ctx(sub_task_path=("A",))

    result = await executor._run_one_workflow(_wfs("wf:n")[0], ctx, asyncio.Event())

    assert result["status"] == "failed"
    assert result["result"] is None
    assert "未返回终态" in result["error"]
    envs = collect_sub_tasks(ctx.event_queue)
    assert envs[-1]["data"]["event"] == "node_end"
    assert envs[-1]["data"]["status"] == "failed"


async def test_run_one_workflow_timeout():
    executor = make_executor(workflow_timeout_seconds=0.05)
    _set_fake_va(executor, returns={"url": "late"}, sleep=1)
    ctx = make_turn_ctx(sub_task_path=("A",))

    result = await executor._run_one_workflow(_wfs("wf:t")[0], ctx, asyncio.Event())

    assert result["status"] == "timeout"
    assert "超时" in result["error"]
    envs = collect_sub_tasks(ctx.event_queue)
    assert envs[-1]["data"] == {"event": "node_end", "status": "timeout", "error": "工作流超时"}


async def test_run_one_workflow_cancelled():
    executor = make_executor()
    _set_fake_va(executor, returns={"url": "r"})
    ctx = make_turn_ctx(sub_task_path=("A",))
    cancel = asyncio.Event()
    cancel.set()  # VA 返回后检测到取消

    result = await executor._run_one_workflow(_wfs("wf:x")[0], ctx, cancel)

    assert result["status"] == "cancelled"
    envs = collect_sub_tasks(ctx.event_queue)
    assert envs[-1]["data"]["status"] == "cancelled"


# ════════════════════════════════════════════════════════════════════
# 并行委托 → VA 请求构造：intent 改写对齐 + target 仅用 intent 路由（决策 a/b）
# 这两个用例**不 stub** _drive_workflow_va，直接驱动真实函数体并捕获发往 VA 的请求。
# ════════════════════════════════════════════════════════════════════


def _capture_va_request(executor) -> dict:
    """让真实 _drive_workflow_va 跑起来：捕获发往 VA 的 SendMessageRequest，返回空流。"""
    captured: dict = {}

    def send_message(request):
        captured["request"] = request
        return FakeAsyncStream([])  # 空流 → async for 直接结束，请求已在迭代前构造

    executor._va_client.send_message = send_message
    return captured


def _va_data_part(request) -> dict:
    """从 SendMessageRequest 的 DataPart 还原 dict（target/headers/body/params/...）。"""
    for part in request.message.parts:
        if part.WhichOneof("content") == "data":
            return MessageToDict(part.data)
    return {}


def _va_text_part(request) -> str:
    for part in request.message.parts:
        if part.WhichOneof("content") == "text":
            return part.text
    return ""


async def test_drive_workflow_va_rewrites_intent_and_omits_workflow_id():
    """决策 a：推荐入口改写生效，body 入参与 target 的 intent 一致；
    决策 b：target 只用 intent 路由，不含模型生成的局部 workflow_id（wf_path[-1]）。"""
    executor = make_executor()
    captured = _capture_va_request(executor)
    ctx = make_turn_ctx(sub_task_path=("A",))
    delegate = DelegateRequest(intent="理财推荐", task_description="推荐理财产品")

    await executor._drive_workflow_va(delegate, ctx, ("A", "wf-1"), asyncio.Event())

    data = _va_data_part(captured["request"])
    # 决策 a：理财推荐 → 理财选品购买；query → 请推荐低风险理财产品（body 与 target 一致）
    assert data["target"]["intent"] == "理财选品购买"
    assert data["body"]["input"]["intent"] == "理财选品购买"
    assert data["body"]["input"]["query"] == "请推荐低风险理财产品"
    assert data["body"]["custom_data"]["inputs"]["intent"] == "理财选品购买"
    assert _va_text_part(captured["request"]) == "请推荐低风险理财产品"
    # 决策 b：target 不含 workflow_id（wf_path[-1]="wf-1" 仅用于节点盖章，不进路由）
    assert data["target"]["type"] == "workflow"
    assert "workflow_id" not in data["target"]


async def test_drive_workflow_va_passthrough_intent_no_rewrite():
    """非改写 intent：body 与 target 用原始 intent；仍不含 workflow_id，
    且 _build_va_message 注入 conversation_id。"""
    executor = make_executor()
    captured = _capture_va_request(executor)
    ctx = make_turn_ctx(conv_id="c", sub_task_path=("A",))
    delegate = DelegateRequest(intent="转账", task_description="给张三转100")

    await executor._drive_workflow_va(delegate, ctx, ("A", "wf-9"), asyncio.Event())

    data = _va_data_part(captured["request"])
    assert data["target"] == {"type": "workflow", "intent": "转账", "conversation_id": "c"}
    assert data["body"]["input"]["intent"] == "转账"
    assert data["body"]["input"]["query"] == "给张三转100"
