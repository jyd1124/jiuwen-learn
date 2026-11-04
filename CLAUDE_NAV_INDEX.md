# CLAUDE_NAV_INDEX — 源码导航索引（新会话先读这个）

Repo：`agent-core/`(框架) `jiuwenswarm/`(产品：gateway + agentserver) `agent-runtime/`(独立运行时)。`A`=`agent-core/openjiuwen`，`J`=`jiuwenswarm/jiuwenswarm`，`G`=`jiuwenswarm/gateway`。行号基于分支 `claude/epic-heisenberg-4kp62x`（打补丁前的代码），改代码前先 `grep -n` 复核。
深读：`00_MASTER_CHEATSHEET.md`(总览) `01`(架构) `02`(故障/FMEA) `04`(子系统+未接线) `patches/README.md`(6 个修复)。复现：`python3 scripts_diag/repro_and_bench.py`。外部包 `openjiuwen_protocol`（AgentRequest/E2AEnvelope）不在仓库中，本环境装不上。

## 1. 符号速查（40）

| 域 | 符号 | 位置 | Gotcha |
|---|---|---|---|
| Core | `DeepAgent._run_task_loop` | A/harness/deep_agent.py:313(类) :2938(循环) | 外轮上限硬编码 50；`add_token_usage` 无调用点 |
| Core | `DeepAgent._register_rail_selective` | A/harness/deep_agent.py:2483 | INVOKE 事件只挂外层，模型/工具事件挂内层 |
| Core | `ReActAgent._inner_invoke` | A/core/single_agent/agents/react_agent.py:548(类) :3095(循环) | DeepAgent 未配置时 max_iterations=sys.maxsize |
| Core | `ReActAgent._build_model_context_window` | react_agent.py:1186 | before_model_call 里改 `ctx.inputs.messages` 无效，窗口会重建 |
| Core | `AbilityManager.execute` | A/core/single_agent/ability_manager.py:1112 | 工具失败=软失败回灌；并行 lane 把 CancelledError 当返回值 |
| Rails | `AgentRail` | A/core/single_agent/rail/base.py:875 | priority 高者先执行；扁平链，不是洋葱 |
| Rails | `AgentCallbackManager.execute` | A/core/single_agent/agent_callback_manager.py:116 | 所有 rail 事件串行经过这里 |
| Rails | `AsyncCallbackFramework.trigger` | A/core/runner/callback/framework.py:1050 | 串行路径 :1129 不尊重 timeout（patch 0002）；:1190-1214 吞异常 |
| Rails | `ReliabilityRail` / `PingPongDetector` | A/agent_teams/reliability/rail.py:84 / detectors/pingpong.py:12 | config.py:109 默认关闭；只跟踪一对发送方/接收方 |
| Context | `ContextEngine` | A/core/context_engine/context_engine.py:28 | 溢出 `recover_from_model_exception` 只重试一次 |
| Context | `ContextProcessorRail` | A/harness/rails/context_engineer/context_processor_rail.py:52 | 装配压缩链；触发比例 0.8 |
| Context | `MessageSummaryOffloader` | A/core/context_engine/processor/forked/offloader/message_offloader.py:66 | 单条 >0.1×ctx 才卸载；TTL 路径会改写历史→KV 失效 |
| Context | `RuleContentRouter` / `RuleCompressionPipeline` | …/rule_compression/router.py:37 / pipeline.py:29 | `[ERROR]` 开头日志被 json_repair 误判为 JSON 数组（:96-101） |
| Context | `PromptAttachmentManager` | A/harness/prompts/prompt_attachment_manager.py:267 | 只追加增量；不要把动态内容写进 system prompt |
| Context | `SystemPromptBuilder` | A/harness/prompts/builder.py:28 | 每次调用重建；section 只放静态内容 |
| Goal | `TaskCompletionRail` / `GoalEvaluator` | A/harness/rails/task_completion_rail.py:75 / goal/evaluation.py:74 | 构造 GoalEvaluator 时不传 config，GoalStopConfig 注入不了 |
| Perm | `PermissionEngine.check_permission` | A/harness/security/permission_engine/core.py:266 | ASK→AbortError 中断本轮；无超时 |
| Team | `TeamAgentSpec.build` | A/agent_teams/schema/blueprint.py:198(类) :849 | 唯一装配入口 |
| Team | `TeamAgent` | A/agent_teams/agent/team_agent.py:173 | :687 丢弃非 InteractiveInput 的恢复输入 |
| Team | 审批兜底 `MessageHandler` | A/agent_teams/agent/coordination/handlers/message.py:258-268 | 传 dict + 先标已读（P0，patch 0001） |
| Team | `TeamApprovalOrchestrator` / `ApproveToolCallTool` | A/agent_teams/rails/team_permission_rail.py:43 / tools/tool_member.py:1446 | build_mode 下 approve_tool 被移除→无人可审批 |
| Team | `create_team_tools` | A/agent_teams/tools/tool_factory.py:184 | :423-429 按模式裁剪 Leader 工具 |
| Team | `TeamBackend.spawn_member` | A/agent_teams/tools/team.py:107(类) :800 | 只写 UNSTARTED，靠 CAS 拉起 |
| Team | `SpawnManager` | A/agent_teams/agent/spawn_manager.py:57 | inprocess / 子进程 / 外部 CLI 三路 |
| Team | `TaskDao.claim_task` | A/agent_teams/tools/database/task_dao.py:906 | CAS 正确；`update_task*`(:1300+) 先 SELECT 再改，跨进程丢更新 |
| Team | `TASK_TRANSITIONS` | A/agent_teams/schema/status.py:348 | 新状态必须登记在这里 |
| Team | `TeamScheduler` | A/agent_teams/agent/scheduling/scheduler.py:107 | reviewer `create_task` 不留引用(:766)；无失败重派 |
| Team | `EventBus` | A/agent_teams/agent/coordination/event_bus.py:78 | `_event_queue` 无界 |
| Msg | `create_messager` / `_Bus` | A/agent_teams/messager/base.py:61 / inprocess.py:21 | `_p2p` 是进程级全局表，按 member_name 作键 |
| Msg | `PyZmqMessager` | A/agent_teams/messager/pyzmq_backend.py:372 | 先跑 handler 再 ACK、固定 routing id、`_seen_ids` 未用（patch 0003） |
| Ext | `ExternalCliRuntime` / member_runtime | A/agent_teams/external/runtime.py:678 / member_runtime.py:256 | `auto_approve_tools=True` 硬编码（安全） |
| Flow | `run_workflow` | A/agent_teams/workflow/engine/runner.py:334 | :375 加载 journal 无保护 |
| Flow | `agent()` / `human()` | A/agent_teams/workflow/engine/facade.py:60 / :178 | 默认无超时；human 等内存 Future，`timeout=0` 会被当成 600 |
| Flow | `Journal` | A/agent_teams/workflow/engine/journal.py:105 | :195 严格 utf-8；签名不含外部状态（patch 0005） |
| Flow | primitives abort / 写 journal | A/agent_teams/workflow/engine/primitives.py:711-713 | 先查 abort 再写 journal→副作用会重复 |
| Flow | `BudgetLedger` | A/agent_teams/workflow/engine/budget.py:23 | 先检查后扣费（patch 0006 改预留制） |
| Flow | `SwarmflowTool` | A/agent_teams/workflow/tool_swarmflow.py:123 | Leader 进入 SwarmFlow 的入口 |
| Evolve | `EvolutionRail` / `SkillEvolutionRail` / `TrajectoryRail` | A/harness/rails/evolution/evolution_rail.py:241 / skill_evolution_rail.py:141 / trajectory_rail.py:23 | 后台任务 600s 超时；锁只在进程内 |
| Evolve | `append_record_transactional` | A/agent_evolving/checkpointing/store_records.py:138 | :84-89 解析失败返回空日志→覆盖清空历史 |
| Runtime | `MessageHandler._cancel_agent_work_for_session` | G/message_handler/message_handler.py:1395 | Web 断连不会触发取消（有意设计） |
| Runtime | `AgentWebSocketServer` / `AgentRuntime.cancel_request` | J/server/agent_ws_server.py:1241 / J/runtime/service.py:1299 | ws_server :3360-3365 吞 CancelledError |
| Runtime | `AgentManager` / `AgentWarmPool` | J/server/runtime/agent_manager.py:208 / agent_warm_pool.py:115 | 惰性回收只在请求路径 finally 执行；预热池默认关闭 |
| Runtime | `RuntimeSupervisor` | J/server/supervisor/runtime_supervisor.py:85 | Front 断连不通知 Worker(:343-406) |
| Runtime | `WorkerDispatcher` / `WorkerEventStore` | J/server/worker/dispatcher.py:60 / event_store.py:74 | `KIND_CANCEL`(:188) 没有发送方 |
| Runtime | `RuntimeSessionCoordinator` | J/runtime/session/coordinator.py:73 | 关闭只置 CLOSED(:926)（patch 0004） |
| Runtime | 模式名 | G/common/mode_matrix.py:33 | `agent.fast`→`agent.work.normal`；Plan 与 Normal 共用缓存键 |
| Sandbox | `SandboxGatewayClient` / `SandboxGateway` / `JiuwenBoxShellProvider` | A/core/sys_operation/sandbox/gateway/gateway_client.py:11 / gateway.py:53 / A/extensions/sys_operation/sandbox/providers/jiuwenbox.py:2520 | `release`(:60) 无调用方；exec 用 to_thread 包同步 httpx，取消不掉 |
| Memory | `LongTermMemory` | A/core/memory/long_term_memory.py:203,:317 | 加密未生效：codec 构造时已固定为无 key |

## 2. 改需求路由（How-To）

| 需求 | 改哪里 | 先看 | 千万别 |
|---|---|---|---|
| 新增自定义 Rail | 继承 `AgentRail`/`DeepAgentRail`，用 `register_rail` 注册 | rail/base.py 13 个事件；deep_agent.py:2483 分流 | 靠抛普通异常中断（会被吞）；中断用 `AbortError` 或 ctx 的 `request_force_finish` |
| 新增团队工具 | `TeamTool` 子类 + `create_team_tools` | tool_factory.py:184、:423-429 模式裁剪 | 只发消息不写 DB——必须送达的状态写 `TaskDao` |
| 调压缩阈值 | `ContextProcessorRail` 配置 / offloader config | message_offloader.py:33 配置；react_agent.py:1186 | 改 system 段或历史前缀（打掉 KV-Cache）；改 offloader 默认值前注意配置默认值与回退值不一致 |
| 排查流式卡死 | 挂 `scripts_diag/jiuwen_diag_probe.py`，`kill -USR2 <worker pid>` | 02 文档 §2 症状 1；`team` logger 搜 `dropping stale interrupt` | 只看 Gateway——推理在 Worker 进程里 |
| 改 SwarmFlow 原语 | engine/primitives.py + facade.py | journal.py 签名(:61-102)；`_next_ordinal` 路径键 | 引入 time/random/uuid（AST lint 禁止）；改变调用顺序会让旧 journal 失效 |
| 调权限规则 | permission_engine/core.py:266；team_permission_rail.py:43 | 审批闭环见 01 §4A | 忘记外部 CLI 成员 `auto_approve_tools=True` 会绕过审批 |
| 新增运行模式 | G/common/mode_matrix.py + J/runtime/mode_catalog.py | agent_manager.py 缓存键 | 让新模式与旧模式共用缓存键却持有不同状态 |
| 改沙箱 / 工具执行 | jiuwenbox.py:2520；gateway.py:53 | 04 §1 取消断点 B5-B7 | 在事件循环里做同步 HTTP；只 `proc.kill()` 不 `killpg` |
