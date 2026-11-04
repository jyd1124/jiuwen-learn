# patches/ — 可独立提交的缺陷修复补丁

每个补丁都基于当前分支 HEAD 生成（`git diff` 格式，a/ b/ 前缀，相对仓库根目录），可**单独**应用，也可按编号**顺序全部**应用。主工作树未被修改。

```bash
git apply --check patches/0001-fix-approval-fallback-dict.patch
```

```bash
for p in patches/*.patch; do git apply "$p"; done
```

复现与回归验证（baseline = 当前源码，patched = `git archive HEAD` 导出后应用全部补丁）：

```bash
python3 scripts_diag/repro_and_bench.py
```

```bash
python3 scripts_diag/repro_and_bench.py --only s3
```

结果写入 `scripts_diag/results/bench_results.{md,json}`。**口径：本地受控微基准**（真实仓库类，无 LLM、无网络，pyzmq 使用 localhost 真实 socket）。

## 补丁一览与实测

| 补丁 | 缺陷（02 文档编号） | 影响位置（baseline 行号） | 复现 | baseline → patched（实测） | 回归测试 |
|---|---|---|---|---|---|
| `0001-fix-approval-fallback-dict.patch` | P0 #1 审批兜底把 `dict` 传给 `resume_interrupt`，消息先标已读后被丢弃，Teammate 永久卡在中断态 | `agent-core/openjiuwen/agent_teams/agent/coordination/handlers/message.py` ~L258-268；丢弃点 `team_agent.py:687` | `--only s1` | 丢弃率 100% → 0%；恢复成功率 0% → 100%（N=1000） | 新增 3 个测试；相关套件修复前后失败集合一致（失败均为仓库缺失模块 `harness.manifest`） |
| `0002-fix-rail-serial-timeout.patch` | P1 #2 串行 `trigger()` 忽略回调 `timeout`，挂起的 Rail 卡死主循环 | `agent-core/openjiuwen/core/runner/callback/framework.py` ~L1129；`models.py` 新增 `fail_close` | `--only s4` | 挂起 hook：≥5.0s（达观测上限）→ 0.502s；2s hook P99 2.004s → 0.202s；新增可选 fail-close | `tests/unit_tests/core/runner/callback`：374 → 377 passed（+3 新测试） |
| `0003-fix-pyzmq-hol-blocking-dedupe.patch` | P1 #3 先跑 handler 再 ACK（队头阻塞）、吞异常照样 ACK、`_seen_ids` 未用、recv 循环空转；**新发现**：固定 DEALER routing id 导致并发发送被 ROUTER 静默丢弃 | `agent-core/openjiuwen/agent_teams/messager/pyzmq_backend.py` L98-102、L126-161、L130-133、L322-325、L338-339 | `--only s3` | ACK 超时 51/51 → 0；快消息 handler 启动 P99 1953 ms → 8.8 ms；recv 出错 300ms 内空转 661,654 次 → 3 次；静默丢失 5 → 0 | `test_messager.py` 16 passed |
| `0004-fix-session-coordinator-retention.patch` | P1 #8 会话关闭只置 CLOSED，`_sessions/_generations` 永不回收 | `jiuwenswarm/jiuwenswarm/runtime/session/coordinator.py` L88-119、L869-928 | `--only s2` | 稳态内存斜率 1095.3 → 2.4 KB / 千会话；残留记录 2001 → 512（有界）；generation 单调性与旧代际拒绝全部保持 | `test_runtime_session_coordinator.py` 38/38 → 39/39（+1 新测试；需协议包桩） |
| `0005-fix-journal-wal-torn-line.patch` | P1 #5 WAL 半个 UTF-8 字符导致 `UnicodeDecodeError`；半行后追加的记录被粘连丢失；不 fsync | `agent-core/openjiuwen/agent_teams/workflow/engine/journal.py` ~L146-166、~L195、~L269-282；`runner.py` ~L375 | `--only s5` | 多字节截断：load 崩溃 → 恢复 199/200，续跑命中率 0% → 99.5%；半行 + 追加：追加记录丢失 → 保留；fsync 代价 +0.8 ms/调用（可用 `JIUWEN_WAL_FSYNC=0` 关闭） | `workflow/` 473 → 480 passed（+3 本补丁新测试，另 4 个来自 0006），6 个失败修复前已存在 |
| `0006-feat-budget-reservation.patch` | P1 Story 3 预算先检查后扣费，并发分支同时过检 | `agent-core/openjiuwen/agent_teams/workflow/engine/budget.py` ~L23；`primitives.py` ~L679、~L1753 | `--only s6` | 16 并发分支、上限 10k、1500 token/调用：超支 230% → 5%（20 次试验均值 = 最大值）；耗尽后仍发起的调用 15 → 0 | 新增 4 个测试（计入上行 480） |

## 已知边界（提交 PR 前请读）

- **0001**：已过期的审批仍会被消费并标记已读（pub-sub 事件已经恢复过中断时的去重语义）；缺少 `tool_call_id` 的畸形审批会告警并标记已读。
- **0002**：`trigger_parallel` 与 `on()` 装饰器未改；fail-close 为新增可选参数，默认行为不变（fail-open）。
- **0003**：并发度 >1 时同一发送方的消息可能乱序执行，需要顺序时设 `p2p_max_concurrency=1`；ACK 语义改为"已接收并解码"；未处理 `MAXMSGSIZE` 与"每次发送新建 DEALER"（02 附录 A F3）。PubSub handler 仍串行。
- **0004**：最近 512 个已关闭会话仍可读取快照；更早被淘汰的会话 `snapshot_session()` 返回 None，旧代际 `close_session` 返回 `existed=False`（`service.py` 调用方已按"已关闭"处理）。
- **0005**：撕裂行本身不会被重写，只在每次加载时跳过并计数；fsync 默认开启。
- **0006**：临近上限时并发调用改为等待在途调用完成，而不是一起放行；`MockBackend` 只记会话级账本，per-run `workflow_token_limit` 未被 S6 覆盖。
- **测试环境**：外部依赖 `openjiuwen_protocol`（gitcode）在本环境不可安装，S2 场景与相关测试在缺包时使用最小桩（场景 notes 中注明）；部分套件因仓库内缺失模块 `openjiuwen.agent_teams.harness.manifest` 失败，修复前后失败集合一致。
