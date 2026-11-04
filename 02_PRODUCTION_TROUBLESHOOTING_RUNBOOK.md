# 02 · 生产环境故障排查手册（Production Troubleshooting Runbook / FMEA）

> 视角：首席 SRE / 混沌工程。基于当前分支源码的 FMEA，每条结论附 `文件:行号`（`grep -n` / `sed -n` 核对）。
> 路径约定：`AC/` = `agent-core/openjiuwen/`，`AT/` = `agent-core/openjiuwen/agent_teams/`，`JS/` = `jiuwenswarm/jiuwenswarm/`。
> 证据等级：**[实锤]** 代码事实已核对且触发路径明确；**[条件]** 代码事实已核对，但仅在特定部署形态（多进程 / 分布式 / 开关开启）下触发；**[推断]** 依赖运行期行为、未复现。
> 配套：架构背景见 `01_ARCHITECTURE_BIBLE.md`；诊断探针源码见 `scripts_diag/jiuwen_diag_probe.py`（已在本仓库依赖下验证可挂载）。

## 目录

1. [生产环境十大高危爆雷点（Top 10）](#1-生产环境十大高危爆雷点top-10-production-failure-modes)
2. [线上故障极速定位矩阵](#2-线上故障极速定位矩阵symptom-to-root-cause-matrix)
3. [一键注入式生产级诊断探针](#3-一键注入式生产级诊断探针diagnostic-instrumentation)
4. [附录 A–C：分模块 FMEA 完整证据与补丁草案](#附录a-runtime-并发资源泄漏与-ipc分布式通信)

---

## 1. 生产环境十大高危爆雷点（Top 10 Production Failure Modes）

RPN 评分 = 严重度 S × 发生度 O × 难检测度 D（各 1–10）。按 RPN 降序。

| # | 模块 | 故障模式 | 证据 | S | O | D | RPN | 等级 |
|---|---|---|---|---|---|---|---|---|
| 1 | Swarm / 审批 | JSON 兜底审批把 `dict` 传给 `resume_interrupt`，校验要求 `InteractiveInput` → 输入被丢弃，但消息已标记已读 → Teammate 永久卡在中断态 | `AT/agent/coordination/handlers/message.py:267-268`；`AT/agent/team_agent.py:687-688`（`dropping stale interrupt resume input`）；`AT/harness/team_harness.py:440` | 9 | 6 | 8 | 432 | P0 [实锤] |
| 2 | Core / Rails | Rail 回调串行 `await`、无超时；`timeout` 参数只在 `trigger_parallel` 生效 → 慢 hook 阻塞主推理循环；普通异常被吞，安全/预算类 Rail 失败 = 静默绕过，且用 stdlib `logging` 而非 LogManager，生产可能看不到 | `AC/core/runner/callback/framework.py:1129`（直接 await）、`:1340`（仅并行版有 timeout）、`:1190-1214`（吞异常）；`AC/core/single_agent/agent_callback_manager.py:133` | 8 | 6 | 8 | 384 | P1 [实锤] |
| 3 | IPC / PyZMQ | P2P 接收循环先同步执行 handler 再回 ACK：一个慢 handler 堵住其后全部消息；发送端 10s `wait_for` 超时，消息仍会被延迟处理 → 重试即重复；handler 异常被 `contextlib.suppress` 吞后照常 ACK → 无日志丢消息；`_seen_ids` 去重集合声明未用 | `AT/messager/pyzmq_backend.py:126-161`、`:207` | 8 | 6 | 7 | 336 | P1 [实锤] |
| 4 | SwarmFlow / Replay | 副作用重复执行：pause 窗口内已完成调用先检查 abort 再写 journal，被丢弃后 resume 整体重跑；崩溃在 WAL 写入前同理；无 intent log / 幂等键 → `git commit`、外部 API 写操作执行两次 | `AT/workflow/engine/primitives.py:711`（abort 检查）→ `:713`（写 journal） | 8 | 5 | 8 | 320 | P1 [实锤] |
| 5 | SwarmFlow / Replay | 回放签名不含外部状态（文件内容、git HEAD、环境变量）→ 两次运行间修改依赖后 resume 命中陈旧缓存；WAL 追加不 fsync、不检查尾部换行，半行后续记录被静默跳过；中文半字符触发 `UnicodeDecodeError`（非可跳过的 `JSONDecodeError`）→ resume 直接崩 | `AT/workflow/engine/journal.py:61-102`（签名）、`:278-282`（追加写）、`:159-163`（静默跳过）、`:195`（严格 utf-8）；`AT/workflow/runner.py:375` | 8 | 5 | 7 | 280 | P1 [实锤] |
| 6 | Swarm / 审批 | 审批中断无超时、无 pending 表：Leader 自身等待用户时，Teammate 等 Leader 审批 → 循环等待；build_mode 下 `enable_permissions=False` 时挂了审批 Rail 却把 `approve_tool` 从 Leader 工具集删除 → 无人可审批 | `AT/agent/agent_configurator.py:775`；`AT/tools/tool_factory.py:423-429` | 8 | 4 | 8 | 256 | P1 [实锤] |
| 7 | Swarm / 消息风暴 | 防 ping-pong 弱：可靠性框架默认关闭；`PingPongDetector` 只记一对全局 `last_from/last_to`，第三方消息即清零，三方循环检测不到；处置是给 Leader 发文本（反而再起一轮）；全局无 hop 上限、消息限流或团队 Token 预算 | `AT/reliability/config.py:109`（`enabled: bool = False`）；`AT/reliability/detectors/pingpong.py:41-46`；`AT/reliability/handler.py:154-160` | 9 | 4 | 6 | 216 | P1 [实锤] |
| 8 | Runtime / 泄漏 | 后台 `asyncio.create_task` 不持有引用、不可取消：记忆抽取（持有活 adapter）、dreaming、code-agent 后台子代理在客户端断开后继续跑并占用沙箱与模型配额；Session 协调器关闭只置 CLOSED，`_sessions/_generations` 永不 pop → 随历史 session 数线性增长 | `JS/…/interface.py:1250`、`:1796`、`:1874`；`code_agent_rail.py:320`；`team_helpers.py:1967`、`:1977`；`runtime_supervisor.py:1137`；`JS/runtime/session/coordinator.py:88-115`、`:926` | 7 | 6 | 6 | 252 | P1 [实锤] |
| 9 | 自演进 / 存储 | `evolutions.json` 解析失败返回空日志，下一次 `append_record_transactional` 成功覆盖 → 整个 skill 演进历史被清空且回滚不恢复；per-skill 锁仅进程内 `asyncio.Lock`，跨进程并发写丢更新；SKILL.md 原地写非原子 | `AC/agent_evolving/checkpointing/store_records.py:84-89`、`:138`；`evolution_store.py:49-61`、`:378` | 7 | 4 | 7 | 196 | P1 [实锤] |
| 10 | Team DB | 跨进程 Lost Update：`update_task_status/update_task` 先 SELECT 再改 ORM，无 `with_for_update` / 无 `retry_on_locked`；写锁仅进程内 `asyncio.Lock`；swarm 侧还会降级到相对 cwd 的 `team.db` | `AT/…/task_dao.py:1300+`；`AT/…/engine.py:109`；`JS/agents/harness/team/distributed_runtime.py:198`、`:217` | 7 | 3 | 8 | 168 | P1 [条件] |

**候补（P2）**：ZMQ 接收循环 `except: continue` 无日志无退避（socket 损坏时疑似 100% CPU 忙等，`pyzmq_backend.py:127-133`、`:319-325`，[推断]）；ZMQ 未设 `MAXMSGSIZE`；`EventBus._event_queue` 无界；`human()` `timeout=0` 被 `or` 吞成 600s、回复仅存内存（`avatar_session_backend.py:1476`）；`agent()` 默认无超时（`primitives.py:835`、`:853`）；共享工作区锁为可选建议锁，`auto_commit` 整 index 提交会张冠李戴（`AT/team_workspace/manager.py:520-558`）；`TeamRuntimePool.add` 按 team_name 静默覆盖（`pool.py:93`）；空 `session_id` 使 topic 退化为 `session::team:`（`team_manager.py:1452`）。

### 1.1 按需求五大维度的结论

| 维度 | 结论 | 对应条目 |
|---|---|---|
| **Runtime 并发与资源泄漏** | 主流式队列和 IPC 帧有界（32MiB 帧上限、supervisor 转发队列 256、断连 drain 有超时），这部分是健康的。泄漏集中在**无主后台 Task**和**只增不删的 session 表**。客户端断开 → Gateway → Front → Worker → `DeepAgent` 的逐跳 `CancelledError` 传播，以及 jiuwenbox 沙箱 lease 是否释放，**本次未能逐跳闭环证实**，列为需用探针 B 段或混沌实验确认的开放项 | #8、候补 |
| **IPC / 分布式通信** | 不存在"无 timeout 无限挂起"的发送路径（ACK 10s、bootstrap 20s），但 **at-most-once + 慢 handler 队头阻塞 + 静默丢弃** 让"消息丢失却无日志"成为常态；A2X 无应用层心跳，节点卡死只能靠 ACK 超时与 30s 预留 TTL 感知 | #3、候补 |
| **Swarm 状态一致性与死锁** | 单进程内：claim / reassign / member status 用 CAS，正确。风险在：审批兜底 bug（P0）、审批无超时导致的循环等待、跨进程部署下的 Lost Update 与工作区覆盖 | #1、#6、#10 |
| **SwarmFlow 恢复与重放** | 无防"外部环境已改变"的机制；无"副作用已发生、日志未写"的幂等保护；WAL 无 fsync、半行处理有缺口；journal 无版本字段 | #4、#5 |
| **Core 循环与 Rail 异常** | 慢 Rail **阻塞**主循环（不是静默超时）；抛异常的 Rail **被静默吞掉**（仅 `AbortError` 穿透）。自演进写 SKILL.md 存在跨进程冲突与写坏后历史清空的风险 | #2、#9 |

---

## 2. 线上故障极速定位矩阵（Symptom-to-Root-Cause Matrix）

Logger 名均来自源码：`AC/core/common/logging/__init__.py` 中的 `LazyLogger(LogManager.get_logger("<name>"))`（`common`/`agent`/`team`(:185)/`workflow`/`performance`…），日志格式 `时间 | logger | trace_id | 级别 | 消息`。OTEL span 实际只有 `chat {model}`、`execute_tool {tool}`、`llm.reasoning` 与 trajectory 事件 span；**SwarmFlow 无独立 span，缓存命中调用不产生 span**。

### 症状 1：任务卡死一直转圈，无报错也无流式输出

| 步骤 | 内容 |
|---|---|
| **第一步看** | ① `team` logger：搜 `dropping stale interrupt resume input`、`deferring mailbox message`、`[TeamPermission] approval.request_sent`，以及有 `P2P sending` 却没有对应 `acknowledged` 的条目；② `common` logger：`[RailChain] slow callback`（注意：hook 一直不返回时**永远不会打**这条，要用探针 A 段"进行中调用"看）；③ `jiuwenswarm.server.supervisor.runtime_supervisor`：`[Supervisor] runtime diagnostics`、`Gateway forward drain timed out`；④ SwarmFlow：`[swarmflow] human session ... timed out`；TUI `/swarmflows`（`app-screen.ts:3673`）看各 agent 状态是否停在 running、Token 是否不再增长；⑤ OTEL：最后一个 `chat {model}` span 是否未闭合 |
| **核心怀疑代码** | 审批兜底 `AT/agent/coordination/handlers/message.py:268` → `AT/agent/team_agent.py:687`；Rail 串行无超时 `AC/core/runner/callback/framework.py:1129`；ZMQ 队头阻塞 `AT/messager/pyzmq_backend.py:126-161`；`agent()` 无默认超时 `AT/workflow/engine/primitives.py:835`；`human()` 等待 `avatar_session_backend.py:1476` |
| **临时止血** | 对卡住成员：由 Leader 重新发起 `approve_tool`（走 pub-sub 路径 `agent_lifecycle.py:116`，会正确构造 `InteractiveInput`）或中止该轮；SwarmFlow：pause → 同 `run_id` resume（journal 回放）；挂探针 `kill -USR2 <pid>` 取 Task await 链 + 线程栈，区分"在等 Future"与"事件循环被同步调用阻塞"（`[LOOP-BLOCKED]`） |
| **根因修复** | ① 兜底路径包装为 `InteractiveInput`：`await self._round.resume_interrupt(InteractiveInput(...from approval_data))`，且校验失败时**不**标记已读；② `AgentCallbackManager.execute` 外包 `asyncio.wait_for(…, rail.timeout or DEFAULT)`，超时按 Rail 声明的 fail-open / fail-close 处理；③ ZMQ：收到即 ACK，handler 用 `create_task` 异步执行 + 有界并发 + `_seen_ids` 去重；④ `agent()` 默认超时 + `human()` 使用 `timeout if timeout is not None else 600` |

### 症状 2：多智能体无限互发消息 / 死循环，Token 瞬间打爆

| 步骤 | 内容 |
|---|---|
| **第一步看** | `team` logger：`consecutive volleys between`（仅可靠性框架开启时才有）；消息频率：`[_Bus] no P2P handler`、`admitting approval message`；TUI `/swarmflows` 的 per-agent token 曲线；探针 D 段 `EventBus._event_queue` qsize 与 `TeamAgent.deliver_input` 调用次数（A 段 `team.deliver` n 值飙升） |
| **核心怀疑代码** | `AT/reliability/config.py:109`（默认关闭）；`AT/reliability/detectors/pingpong.py:41-46`（单对全局计数）；`AT/reliability/handler.py:154-160`（处置=再发消息）；core 外循环 Token 预算未接线（`LoopCoordinator.add_token_usage` 无调用点，见 01 文档 §2） |
| **临时止血** | 立即开启 `ReliabilityConfig.enabled=True`；对 Team 设置模型侧硬配额（网关 / provider 层限流）；必要时 `DeepAgent.abort` / 取消 team 会话 |
| **根因修复** | ① 检测器改为按 `(sender, recipient)` 无序对的滑动窗口计数，并检测 N 方环（消息图上的强连通分量）；② 每条消息携带 `hop_count`，超过阈值丢弃并告警；③ 团队级 `BudgetLedger`（复用 SwarmFlow 的账本）在 `deliver_input` 前检查；④ 处置动作从"发文本"改为"暂停相关成员轮次" |

### 症状 3：偶发性 Session 状态串台或上下文丢失

| 步骤 | 内容 |
|---|---|
| **第一步看** | 日志中 `session_id` 为空的请求（topic 出现 `session::team:`）；`common` logger `[EvolutionStore] parse ... failed`；Checkpoint 反序列化失败日志（静默以空状态重来，见 01 文档 §3）；探针 C 段 session 近似占用表中是否出现空 sid / 同 sid 对应多个 team |
| **核心怀疑代码** | `get_session_id()` 未设置返回 `""` → `build_session_scoped_team_name` 不加后缀（`team_manager.py:1452`）；`TeamRuntimePool.add` 静默覆盖（`pool.py:93`）；in-process 总线 `_p2p` 为进程级全局表、按 `member_name` 作键（`AT/messager/inprocess.py:58`）；AgentManager 缓存键 Plan/Normal 共用 root Agent（01 文档 §3） |
| **临时止血** | 网关层拒绝无 `session_id` 的 team 请求；同名 team 并发创建时串行化 |
| **根因修复** | `session_id` 为空直接 raise；`TeamRuntimePool.add` 遇已存在键时抛错或校验 session 归属；`_p2p` 键改为 `(session_id, team_name, member_name)`；Checkpoint 反序列化失败时**不**静默清空，而是把原始数据转存 `.corrupt` 并显式报错 |

### 症状 4：AgentServer 内存随运行时间线性增长直至 OOM

| 步骤 | 内容 |
|---|---|
| **第一步看** | `jiuwenswarm.server.supervisor.runtime_supervisor` 的 `[Supervisor] runtime diagnostics`；进程守护的重启记录（SIGKILL 后 600s 内最多重启 3 次）；探针：B 段 Task 按协程聚合 Δ 持续为正 → Task 泄漏；C 段 tracemalloc Top-N 行号 + session 数持续增长；D 段 zmq socket open 数 / fd 数 / 僵尸子进程 |
| **核心怀疑代码** | `JS/runtime/session/coordinator.py:88-115`、`:926`（`_sessions/_generations` 不 pop）；无主后台 Task：`interface.py:1250/1796/1874`、`code_agent_rail.py:320`、`team_helpers.py:1967/1977`；无状态 agent 缓存仅关停时清理（`JS/server/agent_ws_server.py:3756`、`JS/runtime/service.py:4351`，[条件]：取决于租户/通道 ID 基数）；AgentManager 惰性回收只在请求路径 finally 执行（无后台清扫，01 文档 §3） |
| **临时止血** | 提高回收频率：周期性请求触发惰性 GC 或缩短 TTL；按内存水位滚动重启 Worker（resident 模式下 Front 可重新挂接）；关闭记忆抽取 / dreaming 后台任务 |
| **根因修复** | 所有后台 `create_task` 注册到 `self._bg_tasks: set`，`add_done_callback(self._bg_tasks.discard)`，session 关闭时 `cancel()` + `gather(return_exceptions=True)`；coordinator 在 CLOSED 且无引用后 `pop`；AgentManager 增加后台定时清扫协程 |

### 症状 5：SwarmFlow 断点续跑（Replay）报状态反序列化失败

| 步骤 | 内容 |
|---|---|
| **第一步看** | `runner` / `workflow` logger 与 workflow log_sink：`CACHE_HIT` / `CACHE_MISS` 序列，定位最后一个 `CACHE_HIT` 后的第一条异常；异常类型区分：`pydantic.ValidationError`（schema 漂移）vs `UnicodeDecodeError`（WAL 半字符）vs 静默少记录（半行被跳过——表现为本该命中的步骤 `CACHE_MISS`）；TUI `/swarmflows` 看不到命中/WAL 状态，需看日志 |
| **核心怀疑代码** | `AT/workflow/engine/primitives.py:645`、`:950`（`model_validate` 无 try，validator 变了但 JSON Schema 不变 → 签名不变照样命中 → 校验抛错）；`AT/workflow/engine/journal.py:195`（严格 utf-8）、`:159-163`（静默跳过坏行）；`AT/workflow/runner.py:375`（load 无保护）；journal 记录无版本字段 |
| **临时止血** | 备份 journal/WAL 后截掉末尾不完整行（或末尾半字符）再 resume；若为 schema 漂移，用新 `run_id` 重跑（放弃缓存），或临时回退 schema 类 |
| **根因修复** | ① 缓存命中时 `try: model_validate(...) except ValidationError: 视为 CACHE_MISS 并记录 WARN`；② 读取 `errors="replace"` / 按字节逐行解析，坏行 WARN 并计数；③ 追加前检查文件尾是否以 `\n` 结尾，否则先补 `\n`；关键记录 `os.fsync`；④ 记录加 `schema_version`、签名纳入 schema 类的 validator 源码哈希与外部依赖（文件内容哈希 / git HEAD）；⑤ 副作用工具引入 intent 记录（先写 `intent{ks}` → 执行 → 写 `done{ks}`；resume 遇 intent 无 done 时转人工确认） |

---

## 3. 一键注入式"生产级诊断探针"（Diagnostic Instrumentation）

### 3.1 能力与实现方式

源码：[`scripts_diag/jiuwen_diag_probe.py`](scripts_diag/jiuwen_diag_probe.py)（无第三方依赖，纯标准库 + 可选 `zmq`）。

| 段 | 输出 | 实现 | 对应症状 |
|---|---|---|---|
| A. 慢调用 | 各类调用 n / p50 / p95 / max；`[SLOW]` 实时告警；**进行中调用**按持续时间降序（能发现永不返回的 hook，弥补 `[RailChain] slow callback` 要等返回才打印的盲区） | Monkey-patch 5 个真实异步方法：`AgentCallbackManager.execute`（Rail 分发）、`ReActAgent._call_model`（LLM）、`ReActAgent._execute_tool_call`（工具批）、`AbilityManager.execute`（单能力）、`TeamAgent.deliver_input`（团队投递） | 1、2 |
| B. 悬挂 Task | Task 总数、按协程名聚合与较上次的 Δ（持续正增长 = 泄漏）、存活超阈值 Task 的**完整 await 链**直到最内层 Future | `gc` 枚举 `asyncio.Task`，沿 `cr_await` 链下钻 | 1、4 |
| C. 内存 | RSS、tracemalloc Top-N（文件:行）、按 `session_id` 聚合的近似占用与 session 数 | `tracemalloc` + `gc` 扫描持有 `session_id/_session_id/conversation_id` 的对象 | 3、4 |
| D. 队列积压 | 所有 `asyncio.Queue` 的 qsize / maxsize（0=无界）/ getters / putters 及**所属对象.属性名**（如 `EventBus._event_queue`、`StreamController.stream_queue`）；`BudgetLedger` 花费比例与 Top phase；zmq socket 数；fd 分类；子进程与僵尸数 | `gc` 反查队列持有者；`/proc` | 2、4 |
| E. 线程栈 + 事件循环卡顿 | 每个线程最近 8 帧；后台看门狗每 5s 向运行中的 loop 投递 `call_soon_threadsafe`，超时打印 `[LOOP-BLOCKED]` 并自动 dump | 守护线程 | 1 |

### 3.2 挂载方式

```bash
JIUWEN_DIAG=1 JIUWEN_DIAG_INTERVAL=60 JIUWEN_DIAG_SLOW_MS=5000 PYTHONPATH=scripts_diag:$PYTHONPATH python -m jiuwenswarm.app
```

上述方式需在入口处 `import jiuwen_diag_probe`（模块在 `JIUWEN_DIAG=1` 时自动 `install()`）。不改业务代码的做法是放进 `sitecustomize.py`：

```bash
echo 'import jiuwen_diag_probe' > scripts_diag/sitecustomize.py
```

按需触发一次转储：

```bash
kill -USR2 <agentserver_or_worker_pid>
```

查看输出（默认 `/tmp/jiuwen_diag_<pid>.log`，同时写 stderr）：

```bash
tail -f /tmp/jiuwen_diag_<pid>.log
```

注意：Gateway、AgentServer Front、Runtime Worker 是三个独立进程，**需在每个进程分别挂载**（`PYTHONPATH` + `sitecustomize` 会被子进程继承）。推理循环与 Team 运行在 **Runtime Worker** 进程内，排查 1/2/4/5 类问题优先看 Worker 的 pid。

| 环境变量 | 默认 | 含义 |
|---|---|---|
| `JIUWEN_DIAG` | — | `=1` 时 import 即安装 |
| `JIUWEN_DIAG_INTERVAL` | 60 | 周期转储秒数，0 关闭 |
| `JIUWEN_DIAG_SLOW_MS` | 5000 | 慢调用 / 事件循环阻塞阈值 |
| `JIUWEN_DIAG_TASK_AGE_S` | 120 | Task 存活多久视为疑似悬挂 |
| `JIUWEN_DIAG_TOPN` | 15 | 各表 Top-N |
| `JIUWEN_DIAG_TRACEMALLOC` | 1 | 是否开启 tracemalloc（约 5–15% 开销，高负载生产可设 0） |
| `JIUWEN_DIAG_OUT` | `/tmp/jiuwen_diag_<pid>.log` | 输出文件 |

### 3.3 已做的验证与已知局限

- **已验证**：在本仓库 `agent-core` 依赖下实际 import，5 个 patch 目标全部命中（`patched=['AgentCallbackManager.execute', 'ReActAgent._call_model', 'ReActAgent._execute_tool_call', 'TeamAgent.deliver_input', 'AbilityManager.execute']`）。`[SLOW]` 告警、p50/p95、`BudgetLedger` 读数、积压队列 `<<< BACKLOG` 标记、悬挂 Task 的 await 链（下钻到 `asyncio.Queue.get`）、线程栈均已输出正确。**未在真实多进程线上负载下运行过。**
- **线程安全**：转储在守护线程中通过 `gc` 只读遍历对象；读取 `Task`/`Queue` 状态存在与事件循环线程的竞态，最坏结果是单次读数不准，不会修改业务状态。探针所有路径 `try/except`，自身异常不会传播到业务。
- **开销**：`gc.get_objects()` 全量扫描在百万对象级进程中可达数百毫秒（发生在守护线程，但持有 GIL）；高负载下建议 `JIUWEN_DIAG_INTERVAL=0` 仅用 `SIGUSR2` 按需触发。
- **卸载**：`jiuwen_diag_probe.uninstall()` 还原被 patch 的方法。
- Session 内存为**近似值**（浅层大小），用于看趋势和排名，不是精确保留大小；精确分析请配合 `objgraph` / `memray`。


---

## 附录A. Runtime 并发、资源泄漏与 IPC/分布式通信

> 范围：jiuwenswarm/jiuwenswarm/{server,runtime}，agent-core/openjiuwen/agent_teams/messager。所有行号用 grep -n / sed -n 核对过。
> 路径前缀缩写：`S=jiuwenswarm/jiuwenswarm/server`，`R=jiuwenswarm/jiuwenswarm/runtime`，`M=agent-core/openjiuwen/agent_teams/messager`。
> 没有逐跳核实的结论都标了【未确认】。

### 0. 已核实的正面防护（不算缺陷）
- IPC 帧大小有上限：`S/ipc/protocol.py:22 MAX_FRAME_BYTES = 32MB`，编码和解码两处都检查（:51、:57），`S/ipc/stream.py:150`、`:239 recv_bytes(maxlength=...)` 也检查。
- 认证帧有单独的小上限，并发认证连接数有限制：`S/ipc/loopback.py:48-52`、`:30 _MAX_AUTH_CONNECTIONS=16`。
- Supervisor→Gateway 转发队列有界：`S/supervisor/runtime_supervisor.py:67 _FORWARD_QUEUE_SIZE=256`、`:718`，EOF 时 drain 有超时（`_FORWARD_DRAIN_TIMEOUT`），writer 在 finally 里 cancel 并 gather（:741-743）。
- 主流式队列有界：`S/runtime/agent_adapter/interface.py:4456`（STREAM_QUEUE_MAXSIZE）、`team_helpers.py:167`、`R/session/coordinator.py:732`、`S/transports/http_sink.py:102`。
- P2P send 等待 ACK 有超时：`M/pyzmq_backend.py:106-109 wait_for(..., request_timeout)`，默认 10s（`M/base.py:45`）。
- AgentManager 用 borrowers、pins、retirement_tasks 跟踪借用和回收（`S/runtime/agent_manager.py:232-236`），对应的 pop 在 :427/:442/:459/:489。

### 1. 发现清单

#### F1 [P1] ZMQ P2P：ROUTER 收消息后串行等 handler 跑完才回 ACK，handler 异常被吞掉但仍然 ACK
证据 `M/pyzmq_backend.py:126-136, 143-161`：
```python
async def _recv_loop(self):
    while self._running and self._router is not None:
        frames = await self._router.recv_multipart()
        ...
        await self._handle_request(identity, payload)   # 串行
...
            with contextlib.suppress(Exception):
                await handler(message)                  # 异常被静默吞掉
...
        # Always send ACK                                 -> :161 send_multipart
```
- 触发场景：某个成员的 handler 很慢（比如进了 LLM 调用、锁等待，或者把消息排进下游时卡住）。这个节点上所有发给它的 P2P 消息会被队头阻塞，发送方 10s `wait_for` 超时后抛 TimeoutError（:106），但消息其实稍后还会被处理。如果上层重试，就会重复投递。反过来，handler 抛异常时仍然回 ACK，发送方以为送达，实际消息丢了，没有任何 warning。
- 影响范围：team 分布式模式（distributed_runtime / remote member）下 leader↔member 的指令和事件。表现为任务"看似派发成功但成员没动"，或者同一条消息被处理两次。
- 缓解：ACK 和处理解耦（先 ACK 再投进有界队列由 worker 消费，或者处理完再按结果回 ACK/NACK）；handler 异常要打日志，并回 NACK。
```diff
-                with contextlib.suppress(Exception):
-                    await handler(message)
+                try:
+                    await asyncio.wait_for(handler(message), self._config.request_timeout * 0.8)
+                    status = b"ok"
+                except Exception:
+                    team_logger.exception("P2P handler failed for {}", recipient_id)
+                    status = b"err"
-        # Always send ACK
+        await self._router.send_multipart([identity, status])
```
（发送端也要检查 ACK 内容，并在超时时用 message_id 做幂等去重。注意 `_seen_ids`（:207）只声明过、从未使用，grep 只命中这一行，说明去重本来就没有实现。）

#### F2 [P1] ZMQ recv 循环遇到异常直接 `continue`：没有日志、没有退避，可能把 CPU 打满
证据：`M/pyzmq_backend.py:127-133`（P2P）和 `:319-325`（PubSub）：
```python
            try:
                frames = await self._router.recv_multipart()
            except Exception:
                if not self._running:
                    break
                continue
```
- 触发场景：`_running` 仍为 True，但 socket 或 context 已经坏了（ctx 先于 stop() 被 term，或 fd 出错）。recv 每次立即抛异常，循环就变成没有 await 让出点的热循环，CPU 100%，事件循环饿死，而且没有任何日志。【未确认：需要实际复现 ZMQError 是否每次都同步立即抛出】
- 影响范围：整个进程的事件循环。对应症状(1)"任务转圈无输出"。
- 补丁：
```diff
-            except Exception:
-                if not self._running:
-                    break
-                continue
+            except zmq.ZMQError as exc:
+                if not self._running or exc.errno == zmq.ETERM:
+                    break
+                team_logger.warning("zmq recv failed: {}", exc)
+                await asyncio.sleep(min(backoff := backoff * 2 if backoff else 0.05, 2.0))
+                continue
```

#### F3 [P2] ZMQ 接收端没有设置消息大小上限，每次 send 都新建 DEALER
- `grep -n "MAXMSGSIZE\|RCVHWM\|SNDHWM" M/pyzmq_backend.py` 没有命中。ROUTER/SUB 用默认值（MAXMSGSIZE=-1，即不限），一条超大或恶意消息就能触发大块内存分配。
- `M/pyzmq_backend.py:97-111`：每次 send 都 `ctx.socket(DEALER)` + connect + close(linger=0)。高频发送时连接反复建立和拆除，ACK 超时后 linger=0 会直接丢掉未发出的帧。
- 补丁：`self._router.setsockopt(zmq.MAXMSGSIZE, 32<<20)`，SUB 同样设置，并按 peer 缓存 DEALER（加锁，每个 peer 同一时刻只有一个在途请求）。

#### F4 [P1] fire-and-forget 的 `asyncio.create_task` 没有保存引用，也没有挂到 session 的取消链上
证据（逐处 sed 核实）：
| 位置 | 代码 | 问题 |
|---|---|---|
| `S/runtime/agent_adapter/interface.py:1250` | `asyncio.create_task(_execute_auto_memory_extraction(...parent_agent=...))` | 没有引用；持有 live adapter，session 关闭后仍在跑 LLM 抽取 |
| `S/runtime/agent_adapter/interface.py:1796`、`:1874` | `asyncio.create_task(adapter.try_start_dreaming(...))` | 同上 |
| `S/runtime/agent_adapter/code_agent_rail.py:320` | `asyncio.create_task(self._run_async(subagent, ...))`（background 子 agent） | 客户端断开或 session 取消时无法 cancel，子 agent 会一直跑完（还会占用沙箱和模型配额）。`_run_async`（:349-363）只捕获 Exception |
| `S/runtime/agent_adapter/team_helpers.py:1967`、`:1977` | cron team finish 和 grace task（只有 name，没有引用） | 无法取消；重复 round 时只靠 `finish_scheduled` 标志去重 |
| `S/supervisor/runtime_supervisor.py:1131-1137` `_schedule` | `loop.create_task(coro)` | 结果被丢弃，异常只会以 "Task exception was never retrieved" 的形式出现 |
- 风险：(a) CPython 事件循环对 task 只持有弱引用，没有强引用的 task 可能在执行中被 GC 回收，属于官方文档明确警告的行为；(b) 不可取消导致泄漏：断开后后台 LLM 调用、子 agent、沙箱仍然存活；(c) 异常静默。
- 补丁（统一 helper）：
```diff
+_BG: set[asyncio.Task] = set()
+def spawn_bg(coro, *, name, owner_set=None):
+    t = asyncio.create_task(coro, name=name)
+    s = owner_set if owner_set is not None else _BG
+    s.add(t); t.add_done_callback(s.discard)
+    t.add_done_callback(lambda t: t.cancelled() or (t.exception() and logger.error("bg %s failed", name, exc_info=t.exception())))
+    return t
```
对 code_agent_rail:320 这一处，要把 task 登记到父 session（可以挂到 `ExecutionRegistry`，见 §3），close_session 时一并 cancel。

#### F5 [P2] RuntimeSessionCoordinator：`_sessions` / `_generations` 只增不删
证据：`R/session/coordinator.py:88-89` 声明；`:107-115` 插入；关闭路径 `:926-927` 只做 `record.state = RuntimeSessionState.CLOSED`。`grep -n "_sessions.pop\|_generations.pop"` 没有任何命中，`pop(` 只出现在 :564（deliveries）。
- 触发场景：长时间运行的 AgentServer，每个新 session_id 留下一个 `_SessionRecord`（含 asyncio.Event 和 deliveries dict）加一个 int。
- 影响：内存随累计 session 数线性增长。单条记录很小（几百字节量级，【未确认实测】），所以通常不是 OOM 的主因，但它是一条确定存在的"线性增长"曲线。另外，如果 deliveries 在异常路径上没被 pop，记录会连带持有更大的对象【未确认】。
- 补丁：CLOSED 之后 `self._sessions.pop(session_id, None)`。`_generations` 用来防止旧代消息重入，不能直接删，可以改成 LRU 或 TTL 有界结构（比如保留最近 N=10k 个），或者把 generation 持久化到 session 元数据里。

#### F6 [P2] 无状态 agent 缓存：只在 shutdown 时清理【基数待确认】
- `S/agent_ws_server.py:1363` `_stateless_fallback_agents`，在 `:3751-3756` 按 channel_id 插入，grep 不到任何 pop 或 clear。
- `R/service.py:417` `_stateless_agents`，在 `:4346-4351` 按 `tenant::channel` 插入，只在 `:3532-3539` 的关停路径上 clear。
- 如果 channel_id 是有限枚举（acp/tui/web…），这里不构成问题。如果多租户场景下 tenant 数量无界，就会线性增长。每个 JiuWenSwarm 的 `_adapter` 为 None，所以单个对象不大。需要确认 channel_id 的基数。

#### F7 [P2] 无界队列
- `R/symphony/service.py:2287` `_OrderedProgressDispatcher.queue = asyncio.Queue()` 无界。callback（往前端推 progress）比工具产出慢时，进度事件会堆积；不过生命周期限于单次工具调用，并且会在 tool 返回前 drain。
- `S/runtime/skill/skilldev/pipeline.py:72` `self._event_queue = asyncio.Queue()` 无界【消费者速率未核实】。
- `S/ipc/server.py:77` 的 accept 队列无界，但连接只来自本机 supervisor，风险低。

#### F8 断开连接后的取消传播链【只部分追踪，未完成】
- 已核实：Supervisor 的转发 writer 在 recv 异常的 finally 中 cancel（runtime_supervisor.py:741-743）；WS server 有多处显式处理 CancelledError（`S/agent_ws_server.py:561, 602, 1892, 1914, 2170, 2182, 2278`）；`_session_stream_tasks: dict[str, dict[Task, Event]]`（:1370）用来按 session 跟踪流任务。
- 确定会断链的地方：F4 列出的所有 fire-and-forget task 都不会收到取消，其中 code_agent_rail:320 的后台子 agent 是最直接的一处"断开后仍占用沙箱和模型"的路径。
- Gateway→Front→Worker→DeepAgent 每一跳是否都传播 CancelledError，以及 jiuwenbox / SandboxGatewayClient 的 lease 是否 release，本轮没有逐跳验证，不下结论。建议在探针里观察 §3 列出的 registry，在断开前后各打一次快照做对比。

### 2. 症状矩阵

| 症状 | 先看的日志 / logger | 可疑代码 | 缓解 | 补丁思路 |
|---|---|---|---|---|
| (1) 任务转圈无输出 | logger `team`（`team_logger`，agent-core/openjiuwen/core/common/logging/__init__.py:185）：关键字 `P2P sending` / `P2P send to agent .* acknowledged`（只有 sending 没有 acknowledged，说明 ACK 超时）、`P2P failed to parse`、`no handler for`；logger `jiuwenswarm.server.supervisor.runtime_supervisor`：`[Supervisor] runtime diagnostics`、`Gateway forward drain timed out`、`worker session ended`、`runtime snapshot pending query_id=`；logger `jiuwenswarm.server.ipc.stream` | F1 `M/pyzmq_backend.py:126-161`（队头阻塞、吞异常后仍 ACK）；F2 `:127-133/:319-325`（热循环）；F4 后台 task 异常被静默 | 用 py-spy dump 看是否卡在 `_handle_request`，或在 `_recv_loop` 里空转（CPU 100%） | handler 加超时并打日志，ACK 和处理解耦；recv 异常时退避并记日志 |
| (4) AgentServer 内存线性增长直至 OOM | tracemalloc / objgraph 按类型统计 `_SessionRecord`、`JiuWenSwarm`、`DeepAgent`、`asyncio.Task` 数量；supervisor 的 `runtime diagnostics` 快照 | F5 `R/session/coordinator.py:88-115,926`；F6 `S/agent_ws_server.py:3756`、`R/service.py:4351`；F4 后台 task 持有 adapter 和 messages（interface.py:1250）；F7 无界 Queue | 定期观察 §3 各容器的 len()，看哪个随 session 数单调上升 | CLOSED 时 pop 记录；后台 task 纳入有界集合并在关闭时 cancel；队列改为有界 |

### 3. 诊断探针内省点（属性名均已 grep 核实）
- AgentManager（`S/runtime/agent_manager.py`）：`agents`（:217，channel→cache_key→JiuWenSwarm）、`_agent_create_params`（:222）、`_client_capabilities_by_channel`（:223）、`_agent_borrowers`（:232）、`_agent_pins`（:233）、`_pending_tui_retirements`（:235）、`_retirement_tasks`（:236）、`_permissions_reload_tasks`（:237）、`_last_model_conn_configs`（:246）、`_session_create_tokens`（:247），以及 AgentWarmPool（:250 起导入）
- WS server（`S/agent_ws_server.py`）：`_acp_client_capabilities_by_ws`（:1312）、`_stateless_fallback_agents`（:1363）、`_session_stream_tasks`（:1370）、`_expert_stream_requests`（:1371）、`_model_cache`（:1376）
- RuntimeService（`R/service.py`）：`_pending_chat_requests`（:416）、`_stateless_agents`（:417）、`_agent_execution_owners`（:423）、`_activity_executions`（:427）
- Session 层：`R/session/coordinator.py` `_sessions`、`_generations`（:88-89）；`R/session/execution_registry.py` `_executions`、`_by_session`、`_by_request`（:24-26）；`R/session/work_scheduler.py` `_lanes`、`_closing_tasks`（:41-42）；`R/session_lifecycle.py` `_targets`（:137）
- Supervisor：`_loop_task`、`_side_tasks`（heartbeat/watchdog/process-watch，:1140-1145）、`_forward_queue`（qsize 与上限 256 对比）、`_generation`、`_last_rx`（可算出距上次收包的秒数）
- Worker：`S/worker/event_store.py:78` `_frames`（deque）
- ZMQ：P2P 层的 `_handlers`、`_router_task`；PubSub 层的 `_subscriptions`、`_handlers`、`_sub_task`、`_proxy_task`（`M/pyzmq_backend.py:200-210`）
- 全局：`len(asyncio.all_tasks())`，再按 `task.get_name()` 分组（如 `cron-team-finish-*`、`runtime-*`；F4 里没有命名的 task 会显示成 `Task-N`）


---

## 附录B. Swarm 状态一致性、审批与消息风暴

> 范围：`agent-core/openjiuwen/agent_teams`、`harness/security/permission_engine`、`jiuwenswarm/jiuwenswarm/agents/harness/team`、`jiuwenswarm/jiuwenswarm/runtime/team_controller.py`。
> 下文路径均相对 `agent-core/openjiuwen/agent_teams/`（另有说明的除外）。行号已逐条用 `grep -n` / `sed -n` 核对。
> 与 01_ARCHITECTURE_BIBLE §4A 的关系：本文是对它的补充，只写经过核实的结论；属于推断的地方标了「条件」。

---

### 0. 结论速览

| # | 级别 | 标题 | 状态 |
|---|---|---|---|
| F1 | **P0** | JSON 兜底审批把 dict 传给 `resume_interrupt`，被当作“过期输入”丢弃，而且消息已提前标记为已读 | 已确认（已知 bug，证据链完整） |
| F2 | **P1** | build_mode + `approval_required_tools` + `enable_permissions=False`：队员中断后等待 leader 审批，但 leader 的工具列表里没有 `approve_tool`，队员永久挂起 | 已确认 |
| F3 | **P1** | 审批中断没有超时，也没有 TTL，等待会无限期持续 | 已确认 |
| F4 | P1 | ping-pong 检测默认关闭；只跟踪一对“全局最后发送对”，容易被绕过；补救方式只是给 leader 投递一段文本，会继续制造回合 | 已确认 |
| F5 | P1（条件） | 跨进程写者：`DbSessions._write_lock` 是进程内 `asyncio.Lock`；`task_dao` 的写操作是“先 SELECT 再改 ORM”，既没有 `with_for_update`，也没有 `retry_on_locked` | 已确认代码；多进程场景属于条件触发 |
| F6 | P2 | 共享工作区的写锁是自愿加的建议锁；未加锁时后写者覆盖前写者。`auto_commit` 用整个 index 做 `git commit`，多个成员并发时会把别人的改动记到自己名下，`index.lock` 冲突时静默失败 | 已确认 |
| F7 | P2 | in-process `_Bus._p2p` 是进程级全局表，用 `member_name` 作 key，跨 session/team 同名成员会互相覆盖；目前没有找到 `messager.send()` 调用方，属于潜在隐患 | 已确认（潜在） |
| F8 | P2 | `get_session_id()` 未设置时返回 `""`，topic 退化为 `session::team:X:*`；swarm 侧 session_id 为空时团队名不带后缀 | 已确认 |
| F9 | P2 | `TeamRuntimePool.add` 按 team_name 静默覆盖旧条目，且 `EventBus._event_queue` 无上限 | 已确认 |

---

### 1. Team DB 并发读写

#### 1.1 现状（做得对的部分）
- `tools/database/engine.py:81-160` 的 `DbSessions`：在进程内用 `asyncio.Lock` 串行化所有写操作（`:109 self._write_lock = asyncio.Lock()`，`:149 async with self._write_lock:`），并设有 watchdog 超时。
- `task_dao.claim_task`（`task_dao.py:906-967`）是单条带条件的 UPDATE，`WHERE assignee IS NULL AND status=pending`，按 `rowcount==1` 判断是否抢到，CAS 写法正确。
- `reassign_task` 同样是 CAS。`member_dao.update_member_status`（`member_dao.py:434-475`）用 `WHERE status IN valid_from` 做 CAS，并经过 `retry_on_locked` 包装。
- 外部 CLI 队员（`spawn/external_cli_spawn.py`）的子进程不直接连 DB，DB 读写走父进程的 `teammate_backend.db`（`:599`）。所以“子进程队员共享 SQLite”这种情况**在当前代码里不存在**。

#### F5 [P1，条件] task_dao 的 read-modify-write 在跨进程时不安全
证据：
```python
# tools/database/task_dao.py:1300-1345  update_task_status
async with self._sessions.write() as session:
    result = await session.execute(select(team_task_model).where(...task_id == task_id))
    task = result.scalar_one_or_none()
    if not is_valid_transition(TaskStatus(task.status), TaskStatus(status), TASK_TRANSITIONS): ...
    task.status = status            # ORM 修改，没有 WHERE status=旧值
    ...
    await session.commit()
```
- `update_task`（`task_dao.py:1347-1420`）同样是 SELECT 后在 Python 里改字段，`revision = (task.revision or 0) + 1`（`:1405` 附近）。
- 用 `grep -n with_for_update tools/database/*.py` 搜不到任何结果。`task_dao.py` 里 `retry_on_locked` 出现 0 次，`member_dao` 2 次，`message_dao` 5 次。
- `engine.py:85` 的注释说明这把锁只是“process-wide ``asyncio.Lock``”。
- `config.py:24-29` 中 `db_timeout: int = 5`，也就是 SQLite busy_timeout 只有 5 秒。
- 多进程共用一个文件的路径：`jiuwenswarm/.../team/distributed_runtime.py:195-218` 把 PG 降级为 `{"type":"sqlite","params":{"connection_string":"team.db"}}`，这是**相对 cwd 的路径**。多个网关/worker 进程如果在同一个 cwd 下启动，就会共用同一个 team.db 文件。

触发场景：(a) 分布式 PG 部署（leader 和 remote member 是不同进程）下，两个进程并发调用 `update_task` 或 `update_task_status`；(b) 同一个 cwd 下有多个进程，共享 `team.db`。

后果：
- PG 默认是 READ COMMITTED，可能出现 lost update：revision 只 +1，另一方的 content 被覆盖。还可能出现非法状态迁移，比如 COMPLETED 被改回 IN_PROGRESS，因为迁移校验依据的是过期快照。
- SQLite 下跨进程写会遇到 `database is locked`。task 写操作没有重试，`OperationalError` 会直接抛给工具调用方。

影响范围：单个团队的任务板，包括依赖解析（`:1328` 的 dep resolve 可能被重复执行或遗漏）。

缓解：把状态迁移改成 CAS UPDATE（`WHERE status IN valid_predecessors`），revision 改成乐观锁（`WHERE revision = :seen`）；task 写操作统一接入 `retry_on_locked`；SQLite 路径改成绝对路径，并且限定单一写者进程。

```diff
--- a/agent_teams/tools/database/task_dao.py  (update_task_status)
-            result = await session.execute(select(team_task_model).where(team_task_model.task_id == task_id))
-            task = result.scalar_one_or_none()
-            ...
-            task.status = status
+            valid_from = _valid_predecessor_values(TaskStatus(status), TASK_TRANSITIONS)
+            result = await session.execute(
+                update(team_task_model)
+                .where(team_task_model.task_id == task_id,
+                       team_task_model.status.in_(valid_from))
+                .values(status=status, updated_at=now))
+            if result.rowcount != 1:
+                await session.rollback(); return False
--- update_task
-            task.revision = (task.revision or 0) + 1
+            stmt = update(M).where(M.task_id == task_id, M.revision == seen_rev).values(..., revision=seen_rev + 1)
+            if (await session.execute(stmt)).rowcount != 1: raise ConcurrentRevisionError
--- jiuwenswarm/.../distributed_runtime.py:198,217
-                "params": {"connection_string": "team.db"},
+                "params": {"connection_string": str(get_openjiuwen_home() / "team.db")},
```

---

### 2. 共享工作区写入

#### F6 [P2] 建议锁 + 整个 index 的 git commit
证据：
```python
# team_workspace/rails.py:134-145  写操作前只检查“别人是否持有锁”
lock = self._ws.get_lock(path)
if lock and lock.holder_id != self._member_name and not lock.is_expired(): ...reject
# 没有锁时直接放行，不会自动加锁
```
- 加锁只能由 LLM 主动调用 `team_workspace/tools.py:75-78` 的 `action=="lock"` → `acquire_lock`。
- 分布式模式下 `_send_lock_request`（`team_workspace/manager.py:746-757`）直接 `raise NotImplementedError("Distributed lock messaging is Phase 3")`。
- `auto_commit`（`team_workspace/manager.py:520-558`）：
```python
await _run_git(["add", relative_path], cwd=self.workspace_path)
status = await _run_git(["diff", "--cached", "--quiet"], ...)
result = await _run_git(["commit", "-m", msg], cwd=self.workspace_path)   # 提交整个 index，不限路径
if not result.ok:
    return None                         # 不打日志，静默失败
```

触发场景：两个队员几乎同时写同一个交付物，或者写不同文件。
- 同一文件：后写者覆盖前写者（OS 层不加任何文件锁）。
- 不同文件：A 的 `git add a`、B 的 `git add b`、A 的 `commit` 会把 b 也提交，并记在 A 名下；接着 B 的 `diff --cached --quiet` 认为没有改动，返回 None，B 的提交消失。
- 两个 git 进程并发会碰到 `.git/index.lock` 冲突，commit 失败但不留任何日志。

影响范围：团队共享 outputs 目录，以及审计/历史（`get_history`）的归属准确性。

缓解：rail 在写操作前自动加锁、写完释放；commit 限定路径；整个 add+commit 序列放在一把进程内锁之下；失败时打 warning。
```diff
--- a/agent_teams/team_workspace/manager.py
+        async with self._git_mutex:                      # 新增 asyncio.Lock
             await _run_git(["add", relative_path], ...)
-            status = await _run_git(["diff", "--cached", "--quiet"], ...)
-            result = await _run_git(["commit", "-m", msg], ...)
-            if not result.ok:
-                return None
+            status = await _run_git(["diff", "--cached", "--quiet", "--", relative_path], ...)
+            result = await _run_git(["commit", "-m", msg, "--", relative_path], ...)
+            if not result.ok:
+                team_logger.warning("workspace auto_commit failed path=%s out=%s", relative_path, result)
+                return None
--- a/agent_teams/team_workspace/rails.py before_tool_call
-            lock = self._ws.get_lock(path)
-            if lock and lock.holder_id != self._member_name ...
+            if not await self._ws.acquire_lock(path, self._member_name, self._member_name):
+                ...reject
+            ctx.extra["workspace_auto_lock"] = path   # after_tool_call 中 release_lock
```

---

### 3. 并发审批（PermissionEngine / Team approval）

架构说明：`harness/security/permission_engine/` 下没有 pending-future 表，也没有任何 timeout（`grep -rn "timeout\|_pending\|Future\|wait_for"` 只命中 `toolguard/tool_policy.py` 里的 shell 包装词 `timeout`）。团队审批走的是 **HITL 中断**：
- `rails/team_permission_rail.py:43-106` 的 `TeamApprovalOrchestrator.handle_approval_request`：给 leader 发一条纯文本消息，然后 `return "interrupt"`。
- leader 调用 `tools/team.py:1270 approve_tool`，结果分两路同时发出：(a) DB json 消息（`:1283-1300`），作为兜底；(b) `ToolApprovalResultEvent` pub-sub（`:1302-1315`）。
- 待审批状态按 `tool_call_id` 区分，存放在队员各自 session 的 `INTERRUPTION_KEY` 中（`harness/team_harness.py:432-448`），没有共享 map，所以不存在“key 冲突”问题。

#### F1 [P0] JSON 兜底路径必定失败，而且会吞掉这条审批消息（已知 bug，已核实）
证据链：
```python
# agent/coordination/handlers/message.py:257-269
if self._round.has_pending_interrupt():
    approval_data = self._try_parse_approval_payload(msg)      # -> dict
    if approval_data is not None:
        ...
        await self._infra.message_manager.mark_message_read(msg.message_id, member_name)  # 先标记已读
        await self._round.resume_interrupt(approval_data)                                  # 传入的是 dict
        continue
# message.py:732-747  _try_parse_approval_payload 返回 json.loads 得到的 dict
# agent/team_agent.py:681-689
async def resume_interrupt(self, user_input, ...):
    if not self._stream_controller.is_valid_interrupt_resume(user_input):
        team_logger.info("[{}] dropping stale interrupt resume input", ...)
        return
# agent/stream_controller.py:752-757 -> harness.is_pending_interrupt_resume_valid(user_input)
# harness/team_harness.py:439-442
def is_pending_interrupt_resume_valid(self, user_input):
    if not isinstance(user_input, InteractiveInput):
        return False
```
对照 pub-sub 路径：`agent/coordination/handlers/agent_lifecycle.py:90-116` 会正确构造 `InteractiveInput().update(tool_call_id, {...})`。

触发场景：`ToolApprovalResultEvent` 丢失，这正是兜底路径存在的意义。例如队员还没订阅、处于 pause_polls 期间、进程重启，或者 inprocess `_Bus` 中 handler 抛异常时 publish 只打 error（`messager/inprocess.py:45-52`）。

后果：
- 兜底消息先被 `mark_message_read`，再被当作过期输入丢弃，而且不会重新投递。
- `has_pending_interrupt()` 仍然为 True，所以之后**所有**非审批消息都会走 “deferring mailbox message … until pending interrupt is resolved” 分支（`message.py:270-276`），队员永久卡住。

影响范围：单个队员，但 leader 会等它的任务等不到结果，进而拖住整个团队回合。

日志指纹：`admitting approval message` 紧接着出现 `dropping stale interrupt resume input`，之后反复出现 `deferring mailbox message`。

```diff
--- a/agent_teams/agent/coordination/handlers/message.py
                     if approval_data is not None:
-                        await self._infra.message_manager.mark_message_read(msg.message_id, member_name)
-                        await self._round.resume_interrupt(approval_data)
+                        from openjiuwen.core.session import InteractiveInput
+                        ii = InteractiveInput()
+                        ii.update(approval_data.get("tool_call_id", ""), {
+                            "approved": bool(approval_data.get("approved")),
+                            "feedback": approval_data.get("feedback", ""),
+                            "auto_confirm": bool(approval_data.get("auto_confirm", False)),
+                        })
+                        if not self._round.is_valid_interrupt_resume(ii):   # 需在 Round 协议中暴露
+                            team_logger.warning("[{}] stale approval {} dropped", member_name, msg.message_id)
+                        else:
+                            await self._round.resume_interrupt(ii)
+                        await self._infra.message_manager.mark_message_read(msg.message_id, member_name)
                         continue
```
（建议在 pub-sub 和 json 两条路径之间抽出一个公共的 `_approval_to_interactive_input()`。另外，两条路径都送达时，第二次会被 `match_answers` 判为过期，这种情况是安全的。）

#### F2 [P1] build_mode 下审批请求没有人能处理，造成死等
证据：
```python
# agent/agent_configurator.py:771-782
approval_tools = agent_spec.approval_required_tools or []
can_request_approval = is_coordinated_teammate and self.team_backend and self.messager
if can_request_approval and approval_tools and not spec.enable_permissions:
    team_rail_specs.append(RailSpec(type=TEAM_TOOL_APPROVAL, ...))     # 不判断 teammate_mode
# tools/tool_factory.py:423-430
if teammate_mode != "plan_mode":
    excluded = {"approve_plan", "submit_plan"}
    if not team_permissions_enabled:
        excluded.add("approve_tool")          # leader 拿不到 approve_tool
```
`tools/locales/descs/en/member/approve_tool.md:1` 也写明：“Only exposed to the Leader when the team uses plan_mode”。

触发场景：`teammate_mode=build_mode`，某个成员配置了 `approval_required_tools`，`enable_permissions=False`。队员调用该工具后，`rails/tool_approval_rail.py:141-156` 发出请求并 `interrupt`；leader 收到文本，却没有工具可以回复。

后果：队员永久中断（与 F3 叠加）。leader 的 LLM 往往会反复回消息催促，又进一步耗费 token。

缓解：配置期 fail-fast，或者改成“只要挂了审批 rail 就暴露 approve_tool”。
```diff
--- a/agent_teams/tools/tool_factory.py
-        if not team_permissions_enabled:
+        if not (team_permissions_enabled or any_member_has_approval_required_tools):
             excluded.add("approve_tool")
--- a/agent_teams/agent/agent_configurator.py
-        if can_request_approval and approval_tools and not spec.enable_permissions:
+        if can_request_approval and approval_tools and not spec.enable_permissions:
+            if str(spec.teammate_mode) != "plan_mode":
+                raise ValueError("approval_required_tools requires plan_mode or enable_permissions")
```

#### F3 [P1] 审批中断没有超时
证据：`TeamApprovalOrchestrator.handle_approval_request`（`rails/team_permission_rail.py:64-106`）和 `TeamToolApprovalRail.resolve_interrupt`（`rails/tool_approval_rail.py:85-176`）都只调用 `return self.interrupt(...)`，没有 deadline。遇到无法识别的输入时会重新中断（`:165-173`）。

循环等待：leader 自身的 ASK 走用户确认，队员的 ASK 走 leader。如果 leader 此时也在等用户审批（leader 的 interrupt），而用户在等团队结果，三者就构成环。代码里没有任何超时可以打破它。

多个审批并发：每条请求都是一条独立的文本消息，leader 靠 LLM 从文本里抄出 `tool_call_id`。leader 可能抄错 id 或成员名，这时 `approve_tool` 会把结果送给错误的 tool_call_id，被 `match_answers` 判为未匹配后丢弃（`team_harness.py:446-447`），仍然没有超时兜底。

缓解：给 interrupt 记录 `requested_at`；StaleTaskHandler 或 EventBus 的 task poll 周期扫描超时的中断，自动 reject 并附 feedback “approval timed out”；同时通知 leader。
```diff
+# rails/team_permission_rail.py
+        request.metadata["approval_deadline"] = time.time() + cfg.approval_timeout_s  # 默认 600s
+# agent/coordination/handlers/stale_task.py (task poll)
+        if self._round.has_pending_interrupt() and _deadline_passed(pending):
+            await self._round.resume_interrupt(_auto_reject(pending, "approval timed out"))
```

---

### 4. 消息 ping-pong 与 token 失控

#### F4 [P1] 环路防护默认关闭，覆盖面窄，补救方式会继续加剧问题
证据：
- `reliability/config.py:109`：`enabled: bool = False`（整个可靠性框架默认是 opt-in）。
- `agent/coordination/dispatcher.py:263-279`：只有在 `reliability_cfg.enabled` 为真时才挂载 `ReliabilityHandler` 和 ping-pong 检测器。
- `reliability/detectors/pingpong.py:35-48`：
```python
is_reversal = sender == self._last_to and recipient == self._last_from
if is_reversal: self._count += 1
else:
    self._count = 1                 # 任何第三方消息都会把计数清零
```
  整个 leader 进程只有一个 `_last_from/_last_to`。A↔B 的往返中间只要夹一条 C→leader 的消息，计数就会清零。A→B→C→A 这样的三角环完全检测不到，广播也不计入。
- 阈值：`reliability/config.py:69-70`，`min_volleys: int = 6`，MEDIUM 为 6，HIGH 为 12（`pingpong.py:72-78`）。
- 补救：`reliability/handler.py:154-160` 的 `_route` 只做 `deliver_input(t("reliability.report_leader"...))`，即给 leader 再触发一轮 LLM 回合。如果 leader 本身就在环里，这一步会继续给环路加料。`remediation/policy.py:16-21` 中 MEDIUM 对应 REPORT_LEADER，HIGH 对应 LOCAL_STEER+REPORT_LEADER，没有任何“阻断发送/限流”动作。
- `grep -rni "hop_count|max_hops|max_messages|token_budget"`：在 agent_teams 中**没有** hop 上限、消息速率限制或团队级 token 预算。仅有的上限是审阅回合 `agent/scheduling/scheduler.py:604-615`（`review_round >= max_rounds`），以及成员内部检测器 `repeat_tool` 的 `loop_block: 20 / global_stop: 30`（`config.py:32-37`，同样受 enabled 控制）。

缓解：
1. 按无序成员对分别计数（`dict[frozenset, deque[ts]]`），在时间窗口内统计；
2. 在 message_manager.send_message 增加团队级速率限制，并给消息加 hop 计数（转发链长度）；
3. HIGH 级别触发 `pause_polls` 或拒绝该对之间继续发消息，而不只是汇报；
4. 默认开启 pingpong 检测器。
```diff
--- a/agent_teams/reliability/detectors/pingpong.py
-        is_reversal = sender == self._last_to and recipient == self._last_from
-        if is_reversal: self._count += 1
-        else: self._count = 1; self._fired_severity = None
+        key = frozenset((sender, recipient))
+        q = self._pairs.setdefault(key, deque())
+        now = time.monotonic(); q.append(now)
+        while q and now - q[0] > self._window_s: q.popleft()
+        self._count = len(q)
--- a/agent_teams/reliability/config.py:109
-    enabled: bool = False
+    enabled: bool = True   # 或者至少把 pingpong 拆出来单独默认开启
+# tools/message_manager.py send_message: 每 (team, from, to) 每分钟 N 条的令牌桶，超额返回错误给 LLM
```

---

### 5. 会话状态串扰 / 上下文丢失

| 点 | 证据 | 风险 |
|---|---|---|
| TeamManager 进程单例 | `jiuwenswarm/.../team/team_manager.py:4166-4171` `get_team_manager()`（“channel_id is ignored”），内部 `_team_agents` 等 20 多个 dict 以 session_id 为 key（`:349-385`） | 以 session 隔离；关键在于 session_id 不能为空或重复 |
| team_name 加 session 后缀 | `team_manager.py:1444-1456` `build_session_scoped_team_name`；**session_id 为空时直接返回 base_name**（`:1452-1453`）；`team_name_generator.py:67-69` 的兜底名是描述文本的 sha256，结果确定 | F8：空 session 时，相同描述会生成相同的 team_name → 共用池条目、DB 行和 topic |
| TeamRuntimePool | `runtime/pool.py:79-98`，`_teams: dict[str, ActiveTeam]` 以 team_name 为 key，`add()` 的说明是 “replacing any existing entry” | F9：同名团队静默覆盖，旧 runtime 成了孤儿，事件会被派发到新团队 |
| session contextvar | `context.py:17-20` 与 `:46-52` 的 `get_session_id()` 返回 `... or ""` | 在 `asyncio.create_task` 之后或 contextvar 未设置的线程里，topic 会变成 `session::team:…`（`schema/events.py:49-59`），事件发到“空 session”上，可能被其他空 session 的团队收到，也可能丢失 |
| in-process 总线全局 | `messager/inprocess.py:80-86` `_bus` 是进程单例；`_p2p: dict[agent_id, handler]`（`:31,58-62`）；agent_id 就是 `member_name`（`agent/agent_configurator.py:398-399` 把 `node_id` 覆盖成 member_name）；`kernel.py:658` 会注册 direct handler | F7：两个 session 中同名成员（如 `dev_1`）注册时后者覆盖前者，前者 `unregister_p2p` 时又会把后者删掉。目前没有找到 `messager.send()` 调用方，属于潜在问题 |
| 发布串行 | `inprocess.py:45-52` 对订阅者逐个 `await handler(message)` | handler 是 `_filter_self` 加 `EventBus.enqueue`，通常很快；一旦某个 handler 阻塞，会拖住所有订阅者 |

---

### 6. 症状矩阵

#### (2) 智能体之间无限互发消息 / token 暴涨
| 首见 logger / 关键词（已 grep） | 疑点 file:line | 缓解 | 补丁思路 |
|---|---|---|---|
| logger `team`（`core/common/logging/__init__.py:185`）：`processing {} unread messages (steer=` 高频出现（`handlers/message.py:242`） | `reliability/config.py:109` 默认关闭；`pingpong.py:41-46` 第三方消息会清零计数 | 打开 `spec.reliability.enabled`，调低 `min_volleys` | 按成员对 + 时间窗口计数（见 F4 diff） |
| `consecutive volleys between`（`pingpong.py` 中 Anomaly.summary），以及 i18n 文本 `reliability.report_leader` | `reliability/handler.py:154-160` 只做汇报 | HIGH 级别执行 `pause_polls` 或阻断该对 | 在 RemediationAction 中新增 `THROTTLE_PAIR` |
| `Teammate tool approval request` 文本消息反复出现，以及 `[TeamPermission] approval.request_sent`（`team_permission_rail.py:99-104`） | F2：leader 没有 approve_tool，只能用文字回复 | 配置校验 | 见 F2 diff |
| 审阅返工：`meta_rework`（`scheduling/render.py:101`） | `scheduler.py:604-615` 有上限 | 已有 `max_review_rounds` | 无 |

#### (3) 间歇性会话状态串扰 / 上下文丢失
| 首见 logger / 关键词 | 疑点 file:line | 缓解 | 补丁思路 |
|---|---|---|---|
| `dropping stale interrupt resume input`（`team_agent.py:688`），其前一条是 `admitting approval message`（`message.py:262`） | F1，`message.py:267-268` | 临时办法：重新发 ToolApprovalResultEvent | dict 转成 InteractiveInput，并在校验之后再标记已读 |
| `deferring mailbox message … until pending interrupt is resolved`（`message.py:271`）持续出现 | F1/F2/F3 | 人工 approve / 重启成员 | 中断超时自动 reject |
| `[_Bus] no P2P handler for agent_id=`（`inprocess.py:66`） | F7，`inprocess.py:58-62` | 不同 session 的成员不要重名 | p2p key 改为 `f"{session}:{team}:{member}"` |
| topic 字符串中出现 `session::team:`（可以在 `_bus._topic_subs.keys()` 里看到） | F8，`context.py:52` | 每个 create_task 入口都显式 `set_session_id` | `get_session_id()` 为空时在 publish/subscribe 处 raise 或打 warning |
| 团队“被替换”或旧团队无响应，`ActiveTeam` 丢失 | F9，`runtime/pool.py:93-96` | 确保 session_id 不为空 | `add()` 遇到已有条目且 session 不同时 raise |
| `DbSessions.write() exceeded the %.0fs watchdog`（`engine.py:154`），以及 `database is locked` | F5 | 单写者进程，使用绝对 DB 路径 | task_dao 接入 CAS 和 `retry_on_locked` |
| `Workspace push failed after retry`（`team_workspace/manager.py:556`）；commit 静默失败时没有日志 | F6 | 先手动 lock | commit 限定路径，并加 git 互斥锁 |

---

### 7. 诊断探针的内省点（属性名均已核对）

| 对象 | 模块 | 属性 / 方法 |
|---|---|---|
| EventBus | `openjiuwen.agent_teams.agent.coordination.event_bus.EventBus` | `_event_queue`（asyncio.Queue，可读 `qsize()`，无上限）、`_running`、`_polls_paused`、`_loop_task`、`_mailbox_poll_task`、`_task_poll_task`、`_mailbox_poll_interval`、`_task_poll_interval`、`_periodic_poll_enabled`、`_role`；属性 `is_running`、`polls_paused`、`role` |
| in-process 总线 | `openjiuwen.agent_teams.messager.inprocess` | 模块全局 `_bus`（`_get_bus()`）；`_bus._topic_subs: dict[topic, dict[agent_id, handler]]`、`_bus._p2p: dict[agent_id, handler]`；实例 `InProcessMessager._subscribed_topics`、`_config.node_id` |
| Hybrid messager | `openjiuwen.agent_teams.messager.hybrid` | `_lock`（`:48`） |
| DB | `openjiuwen.agent_teams.tools.database.engine.DbSessions` | `write_locked`（属性）、`_write_lock`；`TeamDatabase`（`tools/database/__init__.py`）上有 `_sessions`、`_initialized`、`_closed`、`_checkpoint_task`、`engine`、`read_engine`；共享实例表 `openjiuwen.agent_teams.spawn.shared_resources._db_instances`、`_runtime` |
| 运行时池 | `openjiuwen.agent_teams.runtime.manager.TeamRuntimeManager._pool` → `runtime.pool.TeamRuntimePool._teams`、`_lock`；`list_team_names()`、`teams_for_session()` |
| Swarm TeamManager | `jiuwenswarm.agents.harness.team.team_manager`：`get_team_manager()` → `_team_manager`；`_team_agents`、`_runner_team_agents`、`_team_monitors`、`_stream_tasks`、`_stream_task_generations`、`_session_generations`、`_inflight_requests`、`_round_ended_requests`、`_held_idle`、`_background_task_controllers`、`_cancel_requested`、`_active_pause_tasks`、`_active_team_names`、`_pending_team_names`（`:349-385`） |
| 工作区锁 | `openjiuwen.agent_teams.team_workspace.manager`（`_locks` 在 `:103`）：`_locks: dict[path, WorkspaceFileLock]`、`_lock_mutex`、`_pending_lock_requests` |
| ping-pong | `openjiuwen.agent_teams.reliability.detectors.pingpong.PingPongDetector`：`_count`、`_last_from`、`_last_to`、`_fired_severity`、`_min_volleys`；挂载位置为 dispatcher 的 `.reliability._pingpong`（`dispatcher.py:263`） |
| 中断状态 | `TeamAgent.has_pending_interrupt()`；harness 中 `_interrupt_session().get_state(INTERRUPTION_KEY)`（`harness/team_harness.py:432-452`） |
| session | `openjiuwen.agent_teams.context.get_session_id()`（`_session_id_context`） |


---

## 附录C. SwarmFlow 恢复重放与 Core Rail / 自演进存储

> 所有行号均已用 grep -n / sed -n 核对（仓库 /home/user/jiuwen-learn，路径相对 agent-core/openjiuwen/）。未经核实的推断明确标注为“推断”。

### A. SwarmFlow（agent_teams/workflow）

#### A1 [P1] 回放签名不包含外部文件内容/环境 → 读到过期数据的回放
- 证据：`engine/journal.py:61-102` `call_signature` 只做 `sha256(prompt + identity{label,phase,model,isolation,agent_type,builtin_model,effort,agent_template} + json_schema [+history])` 的哈希；不包含工作区文件、git HEAD、环境变量、工具版本，也不含 backend/model 的实际版本。
- 命中条件：`engine/primitives.py:637-645` 签名一致就判 CACHE_HIT，直接返回缓存结果，不再调用 backend。
- 触发场景：第 1 次运行中某个 agent 读取/审查了 repo，之后用户手动改了文件（或 `git pull`），再 resume/relaunch。prompt 文本没变，于是返回旧结论，下游决策以过期结果为依据。
- 影响范围：一个 run 内所有 prompt 不变的 agent() 调用（审查、规划、verify 投票等）。
- 缓解措施：增加可选的 `opts["inputs_digest"]`（脚本作者传入文件哈希 / git rev），或由引擎自动折入 `git rev-parse HEAD`；在 TUI 上标出命中缓存的节点。
```diff
--- engine/journal.py (call_signature)
+    if opts.get("inputs_digest"):
+        identity["inputs_digest"] = opts["inputs_digest"]   # 仅在设置时参与，保持旧缓存有效
```
（还需把 `inputs_digest` 加入 `primitives.py:503` 的允许 opts 集合。）

#### A2 [P1] “副作用已发生、journal 未写”窗口 → resume 时重复执行副作用
- 证据：`engine/primitives.py:711` `_check_abort(rt)  # pre-journal guard: a call finished mid-pause does not persist`，随后在 `:713` 才执行 `await rt.journal.use(...)`；docstring `:216-222` 明确写着暂停窗口内完成的调用“does NOT persist to the WAL — so a resume reruns it”。session turn 的同一逻辑见 `:1631-1632`。
- 另外：进程在 backend 返回之后、`_append_wal`（`journal.py:269-282`）完成之前崩溃，同样会丢记录。记录里没有 intent/idempotency key（字段见 `primitives.py:969-1000` 的 `_make_record`：key/sig/run_id/tokens/kind/result/raw_text…）。
- 触发场景：worker agent 已经执行了 `git commit`、发了消息或调用了外部 API，这时用户按下 pause（或进程被 kill），resume 后整个调用重跑，副作用发生两次；token 也会再计费一次（缓存命中时按 `cached.get("tokens")` 重新计账，而被丢弃的那次已经花掉的 token 不再出现在 ledger 里）。
- 影响范围：所有带工具副作用的 agent() / agent_session.send()；isolation=worktree 时影响较小（新开 worktree）。
- 缓解措施：在调用 backend 之前写入 `{"type":"intent","key":ks,"sig":sig}`；resume 时如果发现有 intent 但没有结果，就把 `call_key` 作为幂等键传给工具层，或要求人工确认；暂停窗口内完成的结果应该照常持久化（暂停只拦截新的调用）。
```diff
-    _check_abort(rt)  # pre-journal guard
-    await rt.journal.use(ks, _make_record(...))
+    await rt.journal.use(ks, _make_record(...))   # 已付费/已产生副作用的结果先落盘
+    _check_abort(rt)
```

#### A3 [P1] WAL 撕裂行：下一条追加会和残行拼在一起，有效记录被吞掉
- 证据：`journal.py:278` `line = json.dumps(record, ensure_ascii=False) + "\n"`，以 `"a"` 模式追加（`:280`），追加前不检查文件末尾是否是 `\n`；`_parse_records` 在 `:159-163` 遇到 `json.JSONDecodeError` 直接 `continue`。
- 触发场景：第 1 次运行被 kill/OOM/磁盘满，WAL 末尾留下半行 `{"key":"..."`（推断：单行很大，例如 raw_text 有数百 KB 时，aiofiles/BufferedWriter 可能分多次 write 系统调用）。resume 时，第一条新记录直接接在残行后面，变成同一行，解析失败后被静默跳过。这条“已完成”的调用下次 resume 还会重跑（又回到 A2 的重复副作用），而且日志里没有任何告警。
- 另外：没有 `os.fsync`（`:282` 只有 `flush`，`save()` 的 `:300-303` 在 `os.replace` 前同样没有 fsync），掉电后 rename 后的文件可能为空。
- 缓解措施：打开 WAL 时如果末字节不是 `\n`，先补一个 `\n`；解析失败时 `team_logger.warning("[wf] WAL torn line skipped path=%s lineno=%d")`；关键记录加 `os.fsync`。
```diff
     async with self._wal_lock:
         async with aiofiles.open(self._wal_path, "a", encoding="utf-8") as f:
+            if self._needs_newline_guard():   # 首次追加时检查文件末字节
+                await f.write("\n")
             await f.write(line)
             await f.flush()
+            await asyncio.to_thread(os.fsync, f.fileno())
```

#### A4 [P1] UTF-8 截断 / 非 UTF-8 字节 → 整个 Journal.load 抛异常，resume 直接失败
- 证据：`journal.py:195-196` `aiofiles.open(src, "r", encoding="utf-8")` 加 `await f.read()`，使用严格解码，外层没有 try。`ensure_ascii=False`（`:278`）会让中文 prompt/结果以多字节形式写入 WAL，撕裂点一旦落在多字节字符中间，就会触发 `UnicodeDecodeError`，而这不属于 `JSONDecodeError`，`_parse_records` 的“容错”根本走不到。
- 调用链：`engine/runner.py:375` `journal = await Journal.load(...)` 没有保护，异常直接让 run 启动失败。
- 结论：“容忍撕裂行”的设计声明（`:146-152`）只对 ASCII 内容成立。
- 缓解措施：改用 `errors="replace"` 读取，或者按字节逐行 `json.loads(line.decode("utf-8"))` 并对单行做 try。
```diff
-            async with aiofiles.open(src, "r", encoding="utf-8") as f:
+            async with aiofiles.open(src, "r", encoding="utf-8", errors="replace") as f:
```

#### A5 [P1] 缓存命中时反序列化失败会让整个 workflow 失败（对应症状 5）
- 证据：`primitives.py:645` `result = _rehydrate(cached, model_cls)` 外层没有 try；`:944-951` `_rehydrate` 在 `kind=="model"` 时执行 `model.model_validate(val)`。
- 触发场景：pydantic schema 类的 `field_validator`/`model_validator`/默认值发生变化，但 `model_json_schema()` 的输出没变（validator 不进 JSON Schema），于是 sig 仍然相同、缓存命中，`model_validate` 却抛 `ValidationError`。另一种情况是手工编辑过 journal、或旧版本留下的记录里 `result` 结构不同。异常一路冒泡到 `runner.py:308` 的 `except Exception`，产生 `"Workflow failed, exception: ..."`（`:313`）。
- 记录里没有 schema/version 字段（见 `_make_record`），journal 也没有迁移机制；grep `pickle`/`schema_version` 结果为 0（不使用 pickle，没有 RCE 风险，但也没有版本控制）。
- 缓解措施：反序列化失败时降级为 CACHE_MISS，重新执行调用。
```diff
-        result = _rehydrate(cached, model_cls)
+        try:
+            result = _rehydrate(cached, model_cls)
+        except Exception as exc:
+            rt.log_sink(f"[wf] agent key={ks} CACHE_CORRUPT {type(exc).__name__}: {exc}; re-running")
+            cached = None
+    if cached is not None:
+        ...
```
（并给 `_make_record` 增加 `"v": 1`。）

#### A6 [P2] agent() 默认没有超时 → 模型调用卡住时，整个 run 无限期挂起
- 证据：`primitives.py:835` `timeout = opts.get("timeout")`；`:853-857` 只有 `timeout is not None` 时才包一层 `asyncio.timeout`，默认 None，直接 `await make_call(feedback)`。docstring `:818` 也承认“a hung model call”。
- 缓解措施：Runtime 级别设一个默认 `default_agent_timeout`（例如 1800s），opts 未提供时使用它。

#### A7 [P2] human() 等待：默认 600s，超时只返回 None；回复缓冲只在内存
- 证据：`backends/avatar_session_backend.py:102` `_DEFAULT_HUMAN_TIMEOUT = 600.0`；`:1476-1484` 用 `wait_for` 等待，超时后 `team_logger.warning("[swarmflow] human session %s timed out after %ss waiting for a reply")` 并 `return None`。`_pending_reply_buffer`、`_human_reply_reuse`（`:1455-1467`）都是进程内 dict。
- 后果：如果 `opts["timeout"]=0`，`or` 会让它回退到 600s（`0 or 600`）。人类在进程重启前已经回复、但 turn 还没 journal 时，回复会丢失，resume 后要重新询问。（correlation id 是确定性的，见 `:1449-1451`，一旦 journal 写入就能对上。）
- 通知回调异常只打 `debug`（`:1461, :1474`），TUI 收不到提示时没有可见告警，表现为“卡住”。建议把这两处提升到 warning。

#### A8 TUI `/swarmflows` 监控
- `jiuwenswarm/jiuwenswarm/channels/tui/frontend/src/core/commands/builtins/swarmflows.ts:1-22` 只是一个占位实现（打印 “handled by the TUI”）；真正的处理在 `ui/app-screen.ts:3673` 的正则匹配里，动作通过 `swarmflow.${action}` RPC 发出（`:5816`），人类回复走 `chat.swarmflow_reply`（`:10264`）。
- 展示的字段（`core/workflows.ts`）：status ∈ planned/pending/running/completed/failed/stopped/paused/waiting_for_human（`:1-9`）；每个 agent 的 model/prompt/activity(tool_call/tool_result)/outcome/error/started_at/completed_at/token_count/duration_ms/correlation_id/human_prompt/human_reply（`:33-58`）；run 级的 agent_count/completed_agent_count/token_count/duration_ms/estimated_token_count/logs(+truncated)（`:83-99`）；budget{total,spent,remaining,scope,exhausted}（`:19-25`）、workflow_budget、budget_exhausted_scope（`:100-105`）；百分比与低额度告警见 `:134-146`。
- 缺口：看不到 cache hit/miss 计数（引擎只在 `log_sink` 里写了 CACHE_HIT/CACHE_MISS 文本），也没有 WAL 健康度或撕裂行计数。

### B. Core loop / Rails / 进化存储

#### B1 [P1] 回调注册的 `timeout` 在 `trigger()` 中不生效 → 慢 rail hook 阻塞主循环
- 证据：`core/runner/callback/framework.py:1050` `trigger()` 在 `:1121-1129` 里 `callback_result = callback(...)` 之后直接 `result = await callback_result`，没有超时。`cb_info.timeout` 只在 `trigger_parallel` 中被使用（`:1340-1341` `anyio.fail_after(cb_info.timeout)`）。
- Agent 的所有 rail 事件都通过 `core/single_agent/agent_callback_manager.py:133` `await Runner.callback_framework.trigger(agent_event, ctx)` 串行执行，所以 `on(..., timeout=)` / `register(timeout=)`（`:332, :883`）对 rail 来说是“声明了但不生效”。
- 触发场景：某个 rail（例如同步模式 `async_evolution=False` 的进化 rail，或做远端 HTTP 调用的 hook）卡住，ReAct 循环就停在 before/after_model_call、tool_call 上。
- 现有可观测性：只在 hook 结束之后才记录日志，`runner_logger.info("[RailChain] slow callback: event=%s callback=%s elapsed_ms=%.1f")`（`:1138-1143`，阈值 `SLOW_CALLBACK_SECONDS=1.0`，`:73`）；一直不返回的 hook 永远不会产生这条日志。
```diff
-                    result = await callback_result
+                    if callback_info.timeout:
+                        with anyio.fail_after(callback_info.timeout):
+                            result = await callback_result
+                    else:
+                        result = await callback_result
```

#### B2 [P1] 回调异常被吞掉，且记录到 LogManager 体系之外的 stdlib logger
- 证据：`framework.py:13` `import logging`，`:263` `self.logger = logging.getLogger(__name__)`，也就是 `openjiuwen.core.runner.callback.framework`；异常分支 `:1188-1214` 记录 `self.logger.error(f"Callback execution failed: {callback.__name__} - {e}", exc_info=True)` 后继续执行下一个回调，不会向上抛（只有 AbortError 会重抛，`:1186-1187`）。
- 后果：rail 失败（例如安全/预算 rail 抛了普通 Exception）时主流程照常继续，策略被静默绕过；而且这条日志走的是 stdlib root logger，没有经过 `LogManager.get_logger("runner")` 的配置（`common/logging/__init__.py:142`），生产环境可能看不到。
- 缓解措施：改成 `runner_logger`；对关键 rail 提供 `fail_closed=True` 选项，异常时重抛。

#### B3 [P2] 后台进化任务：默认不设总超时；cleanup 逐个等待，每个最多 600s
- 证据：`harness/rails/evolution/evolution_rail.py:1089-1091` 基类 `_get_evolution_total_timeout_secs` 返回 `None`，此时 `:1066-1068` 只做 `async with self._evolution_sem: await self.run_evolution(prepared)`，没有超时（`skill_evolution_rail.py:557-558` 子类返回 `_evolution_total_timeout_secs`，也可能为 None）。`cleanup_background_tasks` `:1218-1225` 是串行的 `for task in pending: await asyncio.wait_for(task.wait(), timeout=600.0)`，N 个卡住的任务最多拖住 N×600s。
- 同步模式：`:866` `if self._async_evolution:` 为 False 时，`:874` 直接 `await self._safe_run_evolution(prepared)`，进化的 LLM 调用跑在 hook 内，叠加 B1，主循环会被阻塞。
- 泄漏：`:871-872` 只在新任务加入时清理 done 的任务；异常日志是 `logger.warning("[EvolutionRail] background evolution failed: %s")`（`:1083`，logger 名为 `common`）。
- 缓解措施：默认总超时设为 300s；cleanup 改为 `asyncio.wait(pending, timeout=600)` 一次性等待，然后统一 cancel。

#### B4 [P1] evolutions.json 解析失败时返回空 log，下一次 append 会覆盖全部历史
- 证据：`agent_evolving/checkpointing/store_records.py:84-89` 在 `except Exception` 中 `logger.warning("[EvolutionStore] parse %s failed: %s")` 后 `return EvolutionLog.empty(skill_id=name)`。`append_record_transactional` `:163-167` 先 load（拿到空 log），append 一条，再 `save_evolution_log` 原子覆盖。整个事务“成功”，于是旧内容（`old_log_content`，`:157`）也不会回滚。
- 触发场景：手工编辑或另一进程写到一半导致 JSON 损坏，下一条进化记录就会把这个 skill 的全部经验清空，只剩一条，同时 SKILL.md 的进化索引也会按新 log 重新渲染（`:168`）。
- 缓解措施：在事务路径上，解析失败应抛异常，或者先把坏文件备份为 `.corrupt-<ts>`。
```diff
         except Exception as exc:
             logger.warning(...)
-            return EvolutionLog.empty(skill_id=name)
+            if strict:
+                raise_error(StatusCode.TOOLCHAIN_EVOLVING_SKILL_STORE_EXECUTION_ERROR,
+                            error_msg=f"corrupt {evo_path}", cause=exc)
+            return EvolutionLog.empty(skill_id=name)
```
（`append_record_transactional` 调用时传 `strict=True`。）

#### B5 [P1] 跨进程并发写 SKILL.md / evolutions.json 时，读-改-写没有文件锁
- 证据：`agent_evolving/checkpointing/evolution_store.py:54-61` `_get_skill_lock` 返回进程内的 `asyncio.Lock()`（`:49` `Dict[str, asyncio.Lock]`），`append_record` 在 `:443-444` 只持有这把锁。`store_records.py:138-186` 内部没有 fcntl/filelock。对比：`member_skill_workspace.py:46` 的 `ensure_member_skill_copy` 用了 `portalocker.Lock(..., timeout=30)`，而且通过 `asyncio.to_thread` 调用（`member_skill_evolution_rail.py:59-65`），但它只保护“复制”这一步，不保护之后的写入。
- 触发场景：多个 swarm 成员进程（或 TUI 与 gateway）对同一个全局 skill 并发进化，两边都读到 v1、各自追加后原子 rename，后写的一方会覆盖掉先写的记录（lost update）。另外，`write_skill_content`（`evolution_store.py:389-414`）和 `ensure_skill_id`（`:169-183`）都不持锁。
- 缓解措施：在 skill_dir 下用 `filelock.AsyncFileLock(skill_dir/".evolution.lock")` 包住整个事务（仓库里 `harness/tools/worktree/lock.py:30,102` 已有同类用法可以照搬）。

#### B6 [P2] SKILL.md 原地非原子写入，回滚本身也非原子，且在协程中做阻塞 I/O
- 证据：`evolution_store.py:378` `path.write_text(content, encoding="utf-8")`（截断后再写），被 `write_skill_content`（`:410`）和 `ensure_skill_id`（`:182`）使用。崩溃时会留下半截 SKILL.md，此后读取到的 skill 内容被截断（加载路径 `:140-145` 只检查文件是否存在）。回滚 `store_records.py:249-260` 的 `_restore_text_file`/`_restore_file_bytes` 同样是 `write_text`/`write_bytes` 原地写。`_snapshot_projection_files`（`:206-218`）会把 evolution/ 下所有文件同步读入内存，`:157` 的 `read_text` 也是同步的，都跑在事件循环线程上。
- 回滚正确性问题：`_remove_unbacked_files`（`:230-236`）会删除快照之后新出现的文件，如果另一进程在这期间写入了文件（见 B5），也会被一并删掉。
- 缓解措施：`write_file_text` 一律改为 tmp+`os.replace`（复用 `_write_file_text_atomic`，`:124-136`）；把快照/回滚放进 `asyncio.to_thread`。

### 症状矩阵

| # | 症状 | 首先查看的 logger / 关键字 | OTEL span | 可疑位置 file:line | 缓解 | 补丁思路 |
|---|---|---|---|---|---|---|
| 5 | SwarmFlow replay 报状态反序列化错误 | workflow log_sink：`[wf] journal loaded: prior_records=`（runner.py:376）、`CACHE_HIT sig=`（primitives.py:643）、`Workflow failed, exception:`（runner.py:313）；`UnicodeDecodeError`；`ValidationError` | 无专用 workflow span；只有 `chat {model}` / `execute_tool {tool}`（extensions/observability/callback_handler.py:1084, :873），且命中缓存时不会产生 | journal.py:195-196（严格 utf-8）；primitives.py:645 / :950（model_validate）；journal.py:278-282（撕裂行拼接） | 删除或修复 `wal/{run_id}.wal` 的最后一行；用新的 run_id 重新运行 | A4 `errors="replace"`；A5 rehydrate 失败降级为 MISS；A3 补换行 + fsync |
| 1 | 卡住：human() 等待 | `team` logger：`[swarmflow] human session %s timed out after %ss`（avatar_session_backend.py:1478）；`human-prompt notify failed`（debug，:1474） | 无 | avatar_session_backend.py:1476-1477（600s 默认；`timeout=0` 被 `or` 吞掉）；TUI 状态 `waiting_for_human` | 在 TUI 中回复；检查 `chat.swarmflow_reply` 是否送达 | 通知失败提升为 warning；用 `is None` 判断取代 `or` |
| 1 | 卡住：agent() 模型调用 | log_sink 最后一条是 `CACHE_MISS ... (live run)` 且之后没有 completed | `chat {model}` span 长时间未结束 | primitives.py:835, :853-857 | 为 agent 设置 `timeout` | Runtime 级默认超时 |
| 1 | 卡住：rail hook | `runner` logger：`[RailChain] slow callback: event=... elapsed_ms=`（framework.py:1138，只在结束后输出）；没有输出就说明 hook 还没返回 | 无 rail span | framework.py:1129（无超时 await）；evolution_rail.py:874（同步进化）；:1222（cleanup 每个任务 600s） | py-spy dump 查看卡在哪个 `__qualname__` | B1 应用 cb_info.timeout；B3 默认超时 |

### 诊断探针的插桩点（模块路径 + 函数）

- Rail 回调执行计时：`openjiuwen.core.runner.callback.framework.AsyncCallbackFramework.trigger`（framework.py:1050，逐个回调计时可参照 `:1116-1130`）；也可以只在 agent 一侧包装 `openjiuwen.core.single_agent.agent_callback_manager.AgentCallbackManager.execute`（:116-134）。慢链汇总函数是 `_log_callback_chain_breakdown`（:105）。
- LLM 调用：`openjiuwen.core.foundation.llm.model.Model.invoke`（model.py:125）、`Model.stream`（:192）。
- 工具调用：`openjiuwen.core.single_agent.ability_manager.AbilityManager.execute`（:1112）、`AbilityManager._execute_single_tool_call`（:1549）。
- SwarmFlow 调用：`openjiuwen.agent_teams.workflow.engine.primitives.agent`（:617 实现）、`_call_backend`（:762）；人类等待：`openjiuwen.agent_teams.workflow.backends.avatar_session_backend` 中的 `_await_human_reply`（:1436）。
- BudgetLedger：`openjiuwen.agent_teams.workflow.engine.budget.BudgetLedger`（budget.py:23；`add` :42、`add_phase` :47、`remaining` :56、`exhausted` :67）；Runtime 上有两个实例，`rt.budget`（runtime.py:65）和 `rt.workflow_budget`（:69）。
- Journal：`openjiuwen.agent_teams.workflow.engine.journal.Journal`（`load` :169、`get_cached` :201、`use` :238、`write_run_record` :250、`_append_wal` :269、`save` :284、`hits` :317）；Runtime 字段 `rt.journal`（runtime.py:51）、`rt.run_id`（:80）、`rt.abort_event`（primitives.py:227 使用）。
- 进化存储：`openjiuwen.agent_evolving.checkpointing.store_records`（`append_record_transactional` :138、`load_full_evolution_log` :74）；`EvolutionRail._safe_run_evolution` / `cleanup_background_tasks`（evolution_rail.py:1211）。
