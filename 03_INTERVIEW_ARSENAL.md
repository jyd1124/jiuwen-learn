# 03 · 面试通关秘籍（Interview Arsenal）

> 定位：面向一线大厂 / AI 独角兽 **高级 / 专家（Staff）** 研发岗位，以 `jiuwenswarm`（Core / Runtime / Swarm）核心架构参与者身份应试。
> 素材来源：`01_ARCHITECTURE_BIBLE.md`（源码架构）+ `02_PRODUCTION_TROUBLESHOOTING_RUNBOOK.md`（FMEA 缺陷）。文中 `AC/`=`agent-core/openjiuwen/`，`AT/`=`agent-core/openjiuwen/agent_teams/`，`JS/`=`jiuwenswarm/jiuwenswarm/`。

## 0. 使用守则（先读，否则会在追问里翻车）

1. **量化指标全部是占位符 `【…】`**。本仓库没有任何压测、线上监控数据，这份文档不编造数字。每个占位符旁边写了"怎么测"，请用你真实做过的数据填，没测过就换成定性描述。Staff 面试官最擅长的就是追问"这个 40% 是怎么测的、基线是什么、P50 还是 P99"。
2. **"我修复了 X" 只能写你真正做过的**。02 文档里的修复补丁是**设计草案，未在仓库中实现**。War Stories 按"你亲手定位并落地"的口径写，请按实际参与程度改成"我主导 / 我参与 / 我提出方案"，未落地就说"方案已评审，待排期"——这在面试中同样加分，而且经得起背调。
3. **术语以源码为准**。需求里有几个说法和源码对不上，面试里用错会直接暴露没读过代码：

   | 不要说 | 要说（源码真实名称） |
   |---|---|
   | "Friction 摩擦信号" | 信号类型 `execution_failure` / `script_artifact` / `user_intent` / `conversation_review` / `trajectory_issue` / `review_feedback`（可以口头概括为"执行摩擦类信号"，但要能报出真名） |
   | "CREATE / PATCH / SIMPLIFY" | 经验补丁 `append/merge/replace/skip`；精简 `DELETE/MERGE/REFINE/KEEP`；重建 `evolve_rebuild`；新团队技能由 `TeamSkillCreateRail` 提议 + swarmskill-creator 生成 |
   | "Rails 洋葱模型" | 扁平优先级链：priority 降序，before/after 同序不反转 |
   | "Fast / Plan / Code / Team 四种模式" | `agent.work.normal/plan`、`agent.code.normal/plan`、`team.work.*`、`team.code.*`、`team.design`；`agent.fast` 只是别名 |
   | "五件套解析器" | 运行时只读 SKILL.md frontmatter 的 `description`/`kind`；五件套完整校验在创作期 `validate_swarmskill.py:validate()` |
   | "human() 挂起释放进程资源" | `human()` 在内存 `asyncio.Future` 上 `wait_for`，真正释放资源靠 pause 中止 + 同 `run_id` Journal 重放 |

4. **"Coordination Engineering"是项目自己的提法**（`jiuwenswarm/docs/zh/AgentTeam.md:44-56`："从 Harness Engineering 到 Coordination Engineering"），可以放心用，但要能讲清它在代码里落在哪。

---

## 1. 行业架构定位对比（Architectural Positioning）

### 1.1 先校准姿态："降维打击"的说法本身就是坑

面试官大概率用过 LangGraph 或 AutoGen。如果你说"它们是玩具级单进程"，对方一句话就能把你打回去：**LangGraph 有按 super-step 写入的 checkpointer + `interrupt()` + 持久化 `@task`，AutoGen 0.4 有基于 gRPC 的分布式 Worker Runtime**。Staff 级的回答不是贬低对手，而是**精确说出取舍点**：我们在哪个维度做了不同的选择、为什么、代价是什么。下面的表按这个原则写。

> 竞品信息以我所知截至 2025 年底的公开版本为准（AutoGen 已进入维护期，微软转向 Microsoft Agent Framework；OpenAI Swarm 是教学性质的实验项目，已被 OpenAI Agents SDK 取代）。面试前请自行核对最新版本。

### 1.2 硬核对比表

| 维度 | LangGraph | AutoGen (0.4+) | CrewAI | OpenAI Swarm → Agents SDK | **jiuwenswarm** |
|---|---|---|---|---|---|
| **编排范式** | 显式状态图（StateGraph，Pregel 式 super-step） | Actor 模型 + topic 订阅；AgentChat 群聊 | Crew（角色 + 顺序/层级流程）+ Flow（`@start`/`@listen`/`@router`） | 函数返回即 handoff；无状态 | **双轨**：动态 `AgentTeams`（1 Leader + N Teammates，LLM 自主拆任务）+ 确定性 `SwarmFlow`（Python 脚本编排） |
| **确定性手段** | 图结构本身确定；节点内部是 LLM | 主要靠对话协议 | Flow 层确定，Crew 层不确定 | 无 | SwarmFlow：结构化调用路径键（`_next_ordinal` + par/pipe 分段）+ AST lint 禁 `time/random/uuid/now` + schema `coerce` 校验重试（默认 retries=2）+ `verify()` 一票否决 |
| **断点续跑** | checkpointer 按 super-step 持久化，`@task` 结果持久化后重放不再执行 | 需自建 | Flow `@persist` | Agents SDK 有 Session（对话记忆），无工作流重放 | Journal：`位置键 ks + SHA-256 内容签名 + run_id` 三元组命中；WAL 追加 + `os.replace` 快照；`_seal_guard` 防重放已封存 run |
| **人在回路** | `interrupt()` + `Command(resume=…)`，挂起即落盘、可跨进程恢复 | `UserProxyAgent` / handoff 给人 | `human_input=True` | 无（SDK 有 guardrail） | Team：权限 ASK → `AbortError` 结束当轮 → Leader `approve_tool` → `resume_interrupt`；SwarmFlow：`human()` 内存等待（默认 600s） |
| **进程 / 分布式** | 库本身单进程；LangGraph Server 提供任务队列 + Postgres 持久化 | 内置 gRPC 分布式 Runtime（Host + Worker） | 单进程为主 | 单进程 | **产品化三进程**：Gateway ⇄（WS E2A 信封）⇄ AgentServer Front ⇄（Unix Socket，4B 长度 + JSON，`PROTOCOL_VERSION=2`，32MiB 帧上限）⇄ Runtime Worker；Teammate 可 inprocess / 子进程 / 外部 CLI；跨节点 PyZMQ（ROUTER/DEALER + XPUB/XSUB）+ A2X 注册预留 |
| **流式可靠性** | 流式事件；Server 端可重连 | — | — | SDK 流式事件 | Worker 侧 `WorkerEventStore` 单调 `sequence`，缓存 2048 帧 / 8MiB，`execution.subscribe{after}` 断线重放；Worker resident 模式下 Front 重启可重新挂接 |
| **扩展机制** | 节点 / 边 / 子图；callbacks | 中间件、自定义 Agent | 回调、工具 | guardrails、hooks | **Rails**：`AgentRail` 基类，13 个 `AgentCallbackEvent` 挂点，按 priority 排序；按事件分流挂到内层 ReAct 或外层 DeepAgent；通过 ctx 通道 `request_force_finish`/`request_retry`/`push_steering`/`_skip_tool` 改变控制流 |
| **上下文治理** | 由开发者在节点里做（如 trim / summarize） | 有 model context 抽象 | 内置 memory | Session 修剪 | Harness 处理器链：`MessageSummaryOffloader → (SessionMemoryCompressor) → DialogueCompressor → CurrentRoundCompressor → RoundLevelCompressor`，0.8 触发比例，溢出时 `ContextEngine.recover_from_model_exception` 压缩后重试 |
| **共享状态** | 图状态（reducer 合并） | 消息 | 共享 memory / 任务输出 | 上下文变量 | Team DB（SQLModel，按 session 动态分表；claim 用条件 UPDATE 做 CAS；complete 在事务里切状态并解锁依赖） |
| **经验资产化 / 自演进** | 无内置 | 无内置 | memory / training（`crew.train`） | 无 | **Swarm Skills**（`SKILL.md`/`workflow.md`/`roles/`/`bind.md`/`dependencies.yaml`）+ `EvolutionRail` 家族：信号检测 → `OnlineEvolutionOrchestrator.evolve` → 打分 `0.5·E + 0.3·U + 0.2·F` → 审批 → `append_record_transactional` 事务写盘 + 失败快照回滚 |

### 1.3 四个维度的深度论述（面试时的主线话术）

#### ① 确定性与灵活性的平衡

**核心论点**：多智能体系统的误差是**乘性累积**的：每一跳 LLM 决策都有概率出错，N 跳串联的成功率按指数衰减。业界常见的两种做法各走一个极端：全动态（AutoGen 群聊、CrewAI hierarchical）灵活但误差累积不可控；全静态图（LangGraph）可控，但图要事先画好。

**jiuwenswarm 的做法是同一个底座上的双轨**：
- 探索性、开放式任务走 `AgentTeams`。Leader 用 `spawn_teammate`、`create_task` 动态组织，共享状态全部落在 Team DB，而不是放在对话历史里（claim 用 CAS，避免两个成员抢同一任务）。
- 已经跑通、需要反复执行的流程沉淀成 `SwarmFlow` 脚本。Leader 通过 `SwarmflowTool → run_swarmflow → run_workflow` 调用，两条轨道可以互相嵌套。
- SwarmFlow 抑制误差的三层机制：**结构化输出 + schema 校验重试**，让每一跳的输出可验证；**失败隔离为 None**，局部失败不污染全局；**`verify()` 一票否决或按分数阈值判定**，有人未投票视为"未决"而不是默认通过。

**和 LangGraph 的真实差异**（别说"它没有重放"）：LangGraph 的确定性来自**图拓扑**，SwarmFlow 的确定性来自**普通 Python 控制流 + 调用路径键**。好处是循环、条件、并发（par/pipe）都用原生 Python 表达，Leader 甚至可以现场生成脚本；代价是确定性要靠 AST lint 强制（禁止 `time`、`random`、`uuid`、`now`），而且 Journal 签名不包含外部状态（见 02 文档 #5）。这个"代价"你主动讲出来，比面试官挖出来强十倍。

#### ② 工程级进程隔离与分布式能力

**核心论点**：Agent 工作负载有三个特点，和普通 Web 服务很不一样：**长时**（一轮几分钟到几小时）、**有状态**（上下文、沙箱、子进程）、**故障域大**（工具执行可能 OOM，或卡死在同步调用上）。

**jiuwenswarm 的进程拆分**：
- **Gateway** 管渠道和鉴权。
- **AgentServer Front** 管连接和准入（`ConnectionHandler → MethodRouter → ExecutionAdmission`）。
- **Runtime Worker** 承载推理。
- 两级守护：`jiuwenswarm.app` 守护 Gateway 和 AgentServer，SIGKILL 后运行满 30s 才重启，600s 内最多 3 次；`RuntimeSupervisor` 守护 Worker，10s 心跳，30s 收不到帧判死，业务循环停滞超过 180s 替换 Worker。
- 收益是**故障域隔离**：Worker OOM 不会拖垮渠道连接；Front 重启后，resident Worker 可以重新挂接，`sequence` 重放补齐断档帧。

**和 AutoGen 分布式 Runtime 的真实差异**：AutoGen 的分布式是**通用 Actor 消息路由**；jiuwenswarm 是**面向会话的产品化运行时**，带 session generation、预热池、惰性回收、流式断点重放。要老实承认的短板：A2X 没有应用层心跳（`extend_reservation` 无调用方）；PyZMQ 是 at-most-once 投递，可靠性靠"先写 DB 再发只带 id 的事件，加 30s POLL 兜底"来补。

#### ③ 非侵入式切面扩展与自适应控制

**核心论点**：传统 callback 的问题有两个：只能"观察"、不能"控制"，以及挂载点和业务代码耦合。

**Rails 的设计**：
- `AgentRail` 统一契约，13 个事件挂点，按 priority 排序，可以用 `callback_priority` 按事件单独调整。
- `_register_rail_selective` 按事件分流：模型、工具相关事件挂到内层 ReAct，每轮都触发；`BEFORE/AFTER_INVOKE` 只挂外层，每次调用只触发一次。
- 控制面通过 ctx 通道表达：`request_force_finish`、`request_retry`、`push_steering`、`_skip_tool`。只有 `AbortError` 能穿透中断主循环，HITL 中断正是借这个实现。
- 典型 Rail：`ReliabilityRail`（priority 5，只做可逆的 `push_steering`）、`TrajectoryRail`（priority 10，只记录轨迹）、`SkillEvolutionRail`（priority 80，在 after_invoke 触发演进，并通过 `enqueue_follow_up` 往外循环注入 review 轮次）、`TaskCompletionRail`（Goal Mode，注入 `<goal_task>` 和 `goal_protocol`）、`ContextProcessorRail`（上下文压缩链）。

**主动讲出的 trade-off（体现深度）**：
- 选择扁平链而不是洋葱模型：after 链不反转，实现简单，但做不了"包裹式"的事务语义。
- Rail 普通异常被吞掉保证了可用性（观测类 Rail 挂了不影响推理），代价是安全、预算类 Rail 也会 fail-open。
- 串行 await 没有超时（02 文档 #2）。我会把它讲成"我推动按 Rail 声明 fail-open / fail-close 策略 + 超时"的演进点。

#### ④ 组织经验资产化与自演进闭环

**核心论点**：多数框架的"记忆"是**对话级**的（向量库召回），而团队经验真正可复用的形态是**流程级**的：谁来做、按什么顺序、边界在哪、依赖哪些工具。Swarm Skills 把这些拆成可审阅、可版本化的五个文件：
- `SKILL.md`：frontmatter 必含 `name/description/version/kind=swarm-skill/roles`
- `workflow.md`：需含 mermaid 流程图
- `roles/<id>.md`：5 个必备段落，Identity 段开头是一行格言，Boundary 段需含 Forbidden 和 Mandatory
- `bind.md`
- `dependencies.yaml`：含 `skills` / `tools`

校验器还会做跨文件一致性检查，例如角色引用的 skills / tools 必须出现在 `dependencies.yaml` 里。

**演进闭环**（以源码真名讲述）：
1. **触发**：`EvolutionRail` 的 after_invoke 钩子调用 `_trigger_evolution`，后台任务超时 600s（团队版 720s）；团队版要等 `view_task` 显示全部任务完成才触发。
2. **信号**：`SignalDetector` 检测信号，例如 `execution_failure` 用失败关键词正则扫描工具输出；用指纹去重。
3. **生成**：`OnlineEvolutionOrchestrator.evolve → SingleDimUpdater → SkillExperienceOptimizer`。LLM 只允许输出 `append` 或 `skip`（附 `merge_target`），每轮最多 2 条文本经验 + 1 条脚本。
4. **打分**：`score = 0.5·E + 0.3·U + 0.2·F`
   - E = (positive+1)/(positive+negative+2)，拉普拉斯平滑的成功率
   - U = 使用次数 / 展示次数
   - F = 0.5 + 0.5·2^(−days/90)，经验针对的是旧版本 skill 时再乘 0.7
5. **门禁**：除非开启 `auto_save`，否则需要人审批（`chat.ask_user_question` 卡片，可逐条接受或拒绝）。jiuwenswarm 默认 Leader 走主动 review，Teammate 走被动 + `auto_save=True`。
6. **落盘**：`append_record_transactional` 拿 per-skill 锁 → 快照 `evolution/` 目录与 SKILL.md → 原子写 `evolutions.json` 并回读校验 → 只替换 SKILL.md 中 evolution-index 标记之间的索引块 → 任何一步失败就恢复快照。
7. **治理**：条目 ≥10 时建议 simplify（`DELETE/MERGE/REFINE/KEEP`）或 rebuild（`evolve_rebuild`），≥30 时告警；rebuild 丢弃得分低于 0.5 的记录；`rollback_skill` 从 `archive/SKILL.v<ts>.md` 恢复。

**主动讲出的风险**（02 文档 #9）：锁只在进程内有效；`evolutions.json` 解析失败会返回空日志，随后被覆盖、历史清空。这正好接上 §4 Q6 的"防劣化"追问。

### 1.4 一句话电梯陈述（30 秒版）

> "jiuwenswarm 解决的不是'让单个 Agent 更强'，而是'让一群 Agent 可靠地协作'。我们在一个底座上同时提供 LLM 自主组队的 AgentTeams 和确定性脚本编排的 SwarmFlow，后者用调用路径键加内容签名做 Journal 重放，把多跳误差累积压在可验证的边界内；运行时拆成 Gateway / Front / Worker 三进程，带两级守护和流式断点重放；单 Agent 层用 Rails 切面统一做护栏、上下文压缩和 Goal 控制；最后用 Swarm Skills 把团队流程沉淀成可审阅的文件资产，并用带审批和事务回滚的演进管线持续迭代。"

---

## 2. 简历黄金 Bullet Points（STAR + 量化占位）

> 格式：一行动词开头的总结 + 2–3 行要点。`【…】` 处填你的真实数据，旁边是测量方法。没做过的条目请删除或降级为"参与 / 设计"。

### Bullet 1 · 高并发 / 高可靠 Runtime 底座改造

**主导 Agent 运行时的资源生命周期治理，消除 Split-Process 架构下的后台协程泄漏与会话表无界增长，Worker 长稳运行时长从【X 小时】提升到【Y 天】。**
- **S/T**：Runtime Worker 在长时间运行后内存线性上涨，最终被 OOM 杀掉后触发守护重启；客户端断开后，后台子代理仍在占用沙箱和模型配额。
- **A**：用自研的 asyncio Task 聚合转储探针（按协程名统计增量），定位到多处 `asyncio.create_task` 不持有引用、不可取消（记忆抽取、dreaming、code-agent 后台子代理），以及 `RuntimeSessionCoordinator` 关闭会话时只置 CLOSED、从不移除 `_sessions/_generations`。设计 session 级后台任务注册表（`add_done_callback` 自动摘除 + 关闭时 cancel 并 gather），并在协调器中做 CLOSED 后回收。
- **R**：RSS 斜率从【__ MB/h】降到【__ MB/h】（**测法**：固定 QPS 压测 24h，看 RSS 线性回归斜率）；断连后沙箱释放时间 P99【__ s】（**测法**：注入客户端断连，统计 jiuwenbox 沙箱存活时长）；OOM 重启次数【__ → 0 /周】。

### Bullet 2 · 分布式消息底座可靠性

**重构基于 PyZMQ 的跨进程 / 跨节点 Agent 消息总线，把投递语义从"静默 at-most-once"升级为"至少一次 + 幂等去重"，团队任务因消息丢失卡死的比例下降【__%】。**
- **S/T**：P2P 接收循环先同步执行 handler 再回 ACK：慢 handler 造成队头阻塞，发送方 10s 超时重试导致重复投递；handler 异常被 `contextlib.suppress` 吞掉后照样回 ACK，消息丢失且无日志。
- **A**：改为"收到即 ACK + 有界并发异步执行 handler + 基于消息 id 的 `_seen_ids` 去重（原字段已声明但未接线）"，为接收循环增加错误日志与指数退避；并保留"先写 DB、再发只带 id 的事件、POLL 兜底"的最终一致设计。
- **R**：消息处理 P99 延迟【__ → __ ms】，重复投递率【__ → __】（**测法**：混沌注入慢 handler 和网络闪断，对比收发计数）。

### Bullet 3 · 多智能体协同与确定性编排引擎（SwarmFlow）

**参与设计确定性多智能体编排引擎 SwarmFlow，用调用路径键 + SHA-256 内容签名实现 Journal 断点续跑，并补齐副作用幂等与 WAL 损坏容错，长流程重跑的 Token 成本降低【__%】。**
- **S/T**：长 SwarmFlow 中断后需整体重跑，Token 成本高；同时发现三类重放陷阱：pause 窗口内已完成的调用被丢弃后重跑，造成 `git commit` 这类副作用重复；WAL 半行（尤其中文多字节截断）导致 resume 崩溃；缓存命中时 `model_validate` 未保护，schema 漂移即失败。
- **A**：引入副作用工具的 intent / done 两阶段记录（resume 遇到"有 intent 无 done"时转人工确认）；WAL 追加前补齐尾部换行、关键记录 fsync、按字节逐行解析容错；缓存命中校验失败降级为 CACHE_MISS；journal 记录加 `schema_version`，签名纳入外部依赖指纹。
- **R**：中断后续跑的缓存命中率【__%】，重跑 Token 节省【__%】（**测法**：同一 workflow 在随机位置中断 N 次，统计重放消耗 / 全量消耗）；副作用重复事故【__ → 0】。

### Bullet 4 · 基于 Rails 的自演进与可观测性治理

**主导 Agent 护栏切面（Rails）与自演进管线的可观测性和韧性治理，建设一键注入诊断探针，线上疑难问题平均定位时间（MTTD）从【__ h】缩短到【__ min】。**
- **S/T**：Rail 回调串行 await 且无超时，慢 hook 阻塞推理主循环，而"slow callback"日志要等 hook 返回才输出，悬挂场景完全不可见；Rail 异常被吞掉，安全 / 预算类 Rail 静默 fail-open；自演进存储的 `evolutions.json` 一旦损坏会被空日志覆盖，演进历史清空。
- **A**：为 Rail 分发引入按 Rail 声明的超时与 fail-open / fail-close 策略；开发非侵入式诊断探针（Monkey-patch `AgentCallbackManager.execute`、`ReActAgent._call_model`、`ReActAgent._execute_tool_call` 等，输出进行中的悬挂调用、Task await 链、session 内存、队列积压、`BudgetLedger` 水位、事件循环阻塞告警）；演进存储加跨进程文件锁，损坏文件隔离为 `.corrupt` 而不是覆盖。
- **R**：MTTD【__ → __】（**测法**：对比引入前后同类工单的定位耗时）；Rail 导致的推理卡死【__ → 0】；演进数据丢失事故【__ → 0】。

> **简历排版建议**：顶部写一句项目定位（"面向生产的多智能体协同框架，覆盖 Runtime / 单 Agent 引擎 / 团队编排三层"），4 条 bullet 按"Runtime → 消息 → 编排 → 治理"排序，每条不超过 3 行，技术名词保留源码真名（面试官可能真的去读代码）。

---

## 3. 三个深水区实战故事（Interview War Stories）

> 讲述结构：**现象 → 影响 → 定位（工具 + 证据链 + 精确到行）→ 方案 A vs B → 落地 + 收益 → 反思**。每个故事控制在 4–6 分钟口述。`【】` 处填真实数据。

### Story 1 · "断连之后还在跑的 Agent"：Split-Process 下的后台协程泄漏与会话表膨胀

**现象与影响**
- Runtime Worker 的 RSS 随运行时间近似线性上涨，约【__ 天】触发一次 OOM，被 `RuntimeSupervisor` 判死后替换。
- 替换的瞬间所有在途会话断流；虽然有 `sequence` 重放，正在跑的推理还是丢了。
- 同时，模型配额在用户已经离开的时段仍在消耗。

**定位过程**
1. **先排除流式缓冲**：`WorkerEventStore` 有 2048 帧 / 8MiB 上限，supervisor 转发队列上限 256，所以不是背压问题。
2. **挂诊断探针做 Task 聚合**：每 60s 转储一次，按协程 `__qualname__` 统计数量并和上次比较。发现几类后台协程的增量始终为正，且与"断连会话数"正相关。
3. **沿 await 链确认**：探针 B 段下钻 `cr_await`，确认这些 Task 停在模型调用或子代理执行上，**创建它们的请求早已结束**。
4. **回到源码找实锤**：`create_task` 返回值没有被保存，也没有任何取消路径：
   - `JS/…/interface.py:1250`：记忆抽取，还持有活的 adapter，导致整个 agent 对象图无法回收
   - `interface.py:1796` / `:1874`：dreaming
   - `code_agent_rail.py:320`：后台子代理
5. **第二条线：tracemalloc**。Top 行指向 `RuntimeSessionCoordinator`：`JS/runtime/session/coordinator.py:88-115` 注册 session，`:926` 关闭时只把状态置为 CLOSED，`_sessions/_generations` 永远不 pop。单条记录很小，但它解释了"严格线性"的那部分斜率。
6. **补 Trace**：OTEL 里只有 `chat {model}` / `execute_tool {tool}` 两类 span，没有会话生命周期 span。所以我给后台任务的 span 补上了 `session_id` 和 parent 关系，才把"孤儿 span"和断连会话对上号。

**方案对比**

| | 方案 A：结构化并发（TaskGroup，每个会话一个作用域） | 方案 B：会话级后台任务注册表 + 生命周期钩子 |
|---|---|---|
| 做法 | 所有后台任务都在请求作用域的 `asyncio.TaskGroup` 里创建，作用域退出即全部取消 | `session_bg_tasks: dict[sid, set[Task]]`，`add_done_callback(discard)` 自动摘除；会话关闭 / 断连时 cancel + `gather(return_exceptions=True)` + 超时 |
| 优点 | 语义最干净，不可能泄漏 | 改动小；支持"允许跨请求存活"的任务（如记忆抽取需要在回复后继续做完） |
| 缺点 | 记忆抽取、dreaming 这类任务本来就要在请求结束后继续跑，强行套作用域会改变产品语义；改动面大 | 依赖所有人遵守注册约定，需要 lint 兜底 |

**落地与收益**
- 选了方案 B，加两个补充：
  - 自定义 lint 规则，禁止裸 `asyncio.create_task`，必须经 `spawn_bg(session_id, coro, ttl=…)`；
  - 给"允许跨请求存活"的任务设置 TTL 上限，超时强制取消。
- 协调器在 CLOSED 且无引用时回收条目。
- 收益：RSS 斜率【__ → __】，OOM【__ → 0】，断连后配额浪费【__%↓】。

**反思（Staff 加分点）**：真正的根因不是某一行代码，而是**缺少"会话作用域"这个一等公民**。异步系统里每个 `create_task` 都应该回答"谁拥有它、谁来取消它"。我还把"客户端断开 → Gateway → Front → Worker → DeepAgent 的取消逐跳传播"列成混沌测试用例，因为这条链路在原代码里**没有端到端验证过**。

> ⚠️ 诚实提示：取消链能否逐跳传到 DeepAgent、jiuwenbox 沙箱会不会释放，在我们的源码分析中**未能证实**。面试时如果你没有亲自验证过，就说"我把它列为混沌实验项"，不要说"我修好了"。

### Story 2 · "永远等不到的审批"：多 Teammate 并发审批的卡死与竞争

**现象与影响**
- Team 模式下，某个 Teammate 偶发"永久转圈"：没有报错，也没有流式输出，其他成员正常，Leader 也显示空闲。
- 用户只能重开会话，任务进度全部丢失。
- 发生率【__%】，集中在"配置了 `approval_required_tools`"的团队。

**定位过程**
1. **先看日志**：`team` logger 中卡住成员的最后一条是 `dropping stale interrupt resume input`，之后每条新消息都打 `deferring mailbox message`。说明成员一直处于"有待处理中断"的状态，所有后续输入都被延后。
2. **顺着关键字找到校验点**：`AT/agent/team_agent.py:687-688`。恢复输入要求是 `InteractiveInput`（`AT/harness/team_harness.py:440`），不满足就丢弃。
3. **谁传了非 `InteractiveInput`？**审批结果有两条投递路径：
   - **主路径**：pub-sub 的 `ToolApprovalResultEvent` → `agent_lifecycle.py:116`，正确构造了 `InteractiveInput`；
   - **兜底路径**：审批结果以 JSON 文本消息到达 → `AT/agent/coordination/handlers/message.py:267` **先标记消息已读**，`:268` 再把**原始 dict** 传给 `resume_interrupt`。

   问题出在兜底路径：已读加上被丢弃，审批结果就永久消失了。
4. **为什么偶发？**只有 pub-sub 事件丢失时才会走兜底。而 PyZMQ 后端是 at-most-once，接收端 handler 异常会被吞掉但照样 ACK（`AT/messager/pyzmq_backend.py:126-161`）。**两个缺陷叠加**才出现，这就是它难复现的原因。
5. **同期发现的相邻风险**：
   - 审批中断本身**没有超时**。Leader 自己也在等用户时，Teammate 等 Leader，形成循环等待。
   - build_mode 下，`agent_configurator.py:775` 挂了审批 Rail，`tool_factory.py:423-429` 却把 `approve_tool` 从 Leader 工具集里删掉了，没人能审批。
   - 跨进程部署时，`task_dao` 的 `update_task_status` 先 SELECT 再改 ORM，写锁只是进程内的 `asyncio.Lock`（`engine.py:109`），存在 Lost Update。

**方案对比**

| | 方案 A：修兜底路径的类型转换 | 方案 B：审批作为一等实体落库（Approval 表 + 状态机 + 超时） |
|---|---|---|
| 做法 | 兜底路径构造 `InteractiveInput`，且校验失败时不标记已读 | 审批请求写入 Team DB（pending/approved/denied/expired），Teammate 恢复时从 DB 读权威结果；消息只当通知；附带超时与默认策略 |
| 优点 | 一行级修复，立刻止血 | 根治"消息丢失 = 审批丢失"，支持超时、审计、重试，和 Team DB"先写 DB 再发事件"的整体设计一致 |
| 缺点 | 没解决 at-most-once 与无超时 | 改动面大，需要迁移 |

**落地与收益**
- **当天先上方案 A 止血**，再按方案 B 演进。
- 审批超时默认 fail-close，并通知用户，打破循环等待。
- 补上"无人可审批"的配置校验：挂了审批 Rail 就必须存在审批方，否则启动失败。
- 多进程部署时，`update_task*` 改为条件 UPDATE（版本号 CAS），并加 `retry_on_locked`。
- 收益：Team 卡死工单【__ → __】，审批平均耗时可观测（新增 `approval.request_sent → approval.resolved` 延迟指标）。

**反思**：分布式系统里，**任何"必须送达"的语义都不能只靠消息**。这个项目自己在任务分配上已经做对了（DB 是事实来源，消息只是唤醒），审批却没有遵循同一原则。我做的事情本质上是把审批对齐到同一套一致性模型。

### Story 3 · "重放了不该重放的东西"：SwarmFlow 断点续跑的幂等性与 Token 熔断精度

**现象与影响**
- 一个代码类 SwarmFlow 在 `human()` 卡点被暂停，第二天恢复后，仓库里出现了**两个一模一样的 commit**。
- 另一个 workflow 恢复时直接报错，用户只能放弃缓存全量重跑，消耗【__】Token。
- 预算设置了上限，账单仍然**超出【__%】**。

**定位过程（三条线）**
1. **重复副作用**：`runner` 日志显示该步骤在恢复时是 `CACHE_MISS`。为什么上次完成了却没记录？读 `AT/workflow/engine/primitives.py:711-713`：调用完成后**先检查 abort，再写 journal**。pause 的三段式中止（设置 abort_event → `abort_all` 取消 future → cancel 任务）恰好落在两者之间，已完成的结果被丢弃，副作用却已经发生。崩溃落在 WAL 写入前也是同样结果。journal 里**没有 intent 记录，也没有幂等键**。
2. **恢复报错**：两种根因，靠异常类型区分。
   - `UnicodeDecodeError`：`journal.py:278-282` 追加 WAL 时不 fsync，也不检查尾部换行；kill -9 留下的半行截在中文多字节字符中间；而 `journal.py:195` 用严格 utf-8 读取，抛出的不是代码会跳过的 `JSONDecodeError`，`runner.py:375` 也没有保护，于是 resume 直接失败。
   - `ValidationError`：schema 类的 validator 改了，但生成的 JSON Schema 没变。签名不变，缓存照样命中，`primitives.py:645` / `:950` 的 `model_validate` 却抛错。
3. **预算超支**：`BudgetLedger`（`AT/workflow/engine/budget.py:23`）是普通的 `spent += n`。在单事件循环里加法本身没有竞争，所以**问题不在锁**。真正的原因是检查点在调用入口（`_check_budget`），并发的 par 分支在入口时都看到"还有余额"，各自完成后再一起记账，超支量约等于"并发度 × 单次调用 Token"。

   > 这是面试的关键转折：面试官通常预设答案是"加锁"，你要指出**单线程 asyncio 下加锁无济于事，问题在于先检查后扣费，应改为预留制**。

**方案对比（以预算为例，最考验判断力）**

| | 方案 A：事后记账 + 更严格的检查点 | 方案 B：预留制（reserve / commit / release） |
|---|---|---|
| 做法 | 在 before/after model call 都检查，缩小窗口 | 调用前按 `max_tokens + prompt 估算` 预留额度，余额不足直接拒绝；完成后按实际用量 commit，退还差额 |
| 优点 | 改动小 | 超支上界可证明：最多超出预估误差，与并发度无关 |
| 缺点 | 并发度高时仍会超支 | 预留过于保守会降低并发利用率；需要可靠的 Token 估算（项目里已有 `len//3` 的启发式和 tokenizer 服务） |

**落地与收益**
- **预算**：采用方案 B，预留系数可配。
- **副作用**：引入 intent / done 两阶段 journal。有副作用的工具先写 `intent{ks}`，执行完写 `done{ks}`；恢复时遇到"有 intent 无 done"，不自动重放，转 `human()` 确认，或调用工具提供的幂等检查（例如 git 先查 commit 是否已存在）。
- **WAL**：追加前补齐尾部 `\n`；intent 和 done 记录 fsync；读取按字节逐行解析，坏行告警计数后跳过。
- **签名**：纳入 `schema_version`、validator 源码哈希、外部依赖指纹（文件内容哈希 / git HEAD）；缓存命中校验失败降级为 CACHE_MISS 并告警，不再让整个 workflow 失败。
- **可观测**：SwarmFlow 原来**没有独立的 OTEL span**，缓存命中的调用也不产生 span。补上 `swarmflow.step{ks, cache=hit|miss}` span 之后，TUI `/swarmflows` 之外也能看到命中率。
- 收益：重复副作用【__ → 0】，恢复失败率【__ → __】，预算超支从【__%】收敛到【≤ 预估误差】。

**反思**：Journal Replay 的本质是 **event sourcing + 确定性重放**。它对纯函数步骤天然成立，对副作用步骤必须引入两阶段语义。这和 Temporal 把 Activity 与 Workflow 分离、LangGraph 要求把副作用包进 `@task`，是同一个工程原理。能把我们的方案放进这个谱系里讲，比单讲修了哪几行更有说服力。

---

## 4. 面试官毒舌连环追问防御指南（10 Brutal Follow-up Q&As）

> 每题结构：**追问 → 满分回答（现状 + 取舍 + 演进）→ 二次追问预判**。回答原则：先承认边界，再讲设计理由，最后给演进路线。

### Q1. 为什么用 PyZMQ，不用 Kafka / Redis Stream / NATS？

**答**：先说清楚 PyZMQ 在架构里的位置：它**不是事实来源，只是唤醒通道**。
- 任务、成员、审批这类状态的事实来源是 Team DB，消息只携带 id，再加上 30s POLL 兜底。在这个前提下，消息层只需要做到**低延迟、无运维依赖、可嵌入**。
- PyZMQ 是 brokerless 的：Leader 自己起一个 XSUB/XPUB 代理（默认端口 18555–18557），单机零依赖就能跑，私有化交付可以少一个中间件。
- Kafka 的强项是持久化日志和消费者组回放。我们的回放需求在 DB 和 Journal 层已经满足，再引入 Kafka 就是双写和运维成本。

**要承认的代价**：ZMQ 没有内建的 ACK、重试和持久化，项目自己实现的 ACK 是 at-most-once 语义，而且存在"先执行 handler 再回 ACK"的队头阻塞问题（02 文档 #3）。

**演进**：消息层抽象已经存在（inprocess / pyzmq / 经 WebSocket 的 `HybridMessager`）。大规模多节点部署时可以加一个 NATS JetStream 后端，用 at-least-once 加上 `_seen_ids` 去重。

**二次追问**："那 POLL 兜底 30s 不是延迟太高吗？" → 30s 只是最坏情况，正常路径是事件驱动的毫秒级。POLL 间隔可以按团队活跃度自适应调整。

### Q2. SwarmFlow 的 Journal Replay 遇到非幂等的外部 API 调用，怎么保证一致性？

**答**：直说：**现在的实现保证不了**。
- Journal 命中靠"位置键 + 内容签名 + run_id"，只记录"已完成"的结果。如果副作用发生在写 journal 之前（`primitives.py:711-713`），恢复时就会重做。
- 解法分三层：
  1. **两阶段 journal**：intent / done。遇到"有 intent 无 done"时不自动重放。
  2. **幂等键下推**：用 `run_id + ks` 作为外部 API 的 Idempotency-Key，支付类 API 的通行做法。
  3. **不能幂等的操作**：转 `human()` 确认，或者调用工具提供的"查询是否已执行"接口做补偿检查。
- 这和 Temporal 的 Activity、LangGraph 的 `@task` 是同一个原则：**副作用必须显式声明边界**。

**二次追问**："intent 写了但进程在执行前崩了，恢复后你怎么知道到底执行没有？" → 这正是需要"可查询性"的地方。intent 本身无法区分"未执行"和"已执行未确认"，所以要么依赖下游的幂等键去重，要么提供 reconcile 查询。两者都没有的工具，应该在注册时被标记为 `non_replayable`，恢复时强制人工介入。

### Q3. 自演进自动改写 Skill，怎么防止越改越差（劣化与回滚）？

**答**：讲现有的四道闸，再讲缺口。

**现有的闸**：
1. **写入面受限**：LLM 只能输出 `append` 或 `skip`，每轮最多 2 条文本经验 + 1 条脚本；SKILL.md 正文不会被普通演进改写，只有 rebuild 和 rollback 会动它。
2. **人审门禁**：除非开启 `auto_save`，否则需要逐条审批。
3. **打分淘汰**：`0.5·E + 0.3·U + 0.2·F`。E 用拉普拉斯平滑，防止小样本时虚高；F 有 90 天半衰期，经验针对旧版本 skill 时再乘 0.7；rebuild 丢弃得分低于 0.5 的记录。
4. **事务与回滚**：`append_record_transactional` 失败时恢复快照；`rollback_skill` 从 `archive/SKILL.v<ts>.md` 恢复。

**缺口**：
- **没有离线评估门禁**。打分是在线的、事后的，劣化经验要被用上几次才会掉分。
- Teammate 默认 `auto_save=True`，等于绕过了人审。

**演进**：参考模型发布的"影子评估 + 金丝雀"做法：
- 新经验先进入 shadow 状态，只记录不注入；
- 在 `agent-core/openjiuwen/dev_tools/skill_evaluator` 这类评测集上做 A/B；
- 达标后再按流量比例灰度；
- E 分数跌破阈值时自动回滚到上一版本，并冻结该 skill 的演进。

**二次追问**："E 的 positive / negative 是谁打的标签？" → 来自信号检测（执行失败正则、review 反馈等），本身就有噪声，所以才用平滑和多维加权，而不是只看单次结果。

### Q4. Rails 为什么是扁平优先级链而不是洋葱模型？Rail 抛异常为什么被吞掉？

**答**：
- **扁平链**：Rail 之间基本独立（观测、注入、护栏），不需要"包裹式"的进出对称语义，扁平链更容易推理执行顺序，也更容易按事件分流挂载（`_register_rail_selective`）。代价是做不了"Rail A 在 Rail B 内外各执行一次"的事务语义。
- **吞异常**（`framework.py:1190-1214`）：Rail 的数量会随生态增长，任何一个观测类 Rail 的 bug 都不应该打断推理，这是**可用性优先**的选择。需要中断时用 `AbortError` 显式表达。
- **我会主动指出的问题**：
  - 这个策略对**安全、预算类 Rail 是错的**，它们应该 fail-close；
  - 串行 `await` 没有超时（`framework.py:1129`），一个慢 hook 会阻塞主循环；
  - 异常日志走的是 stdlib `logging`，不是 LogManager，生产环境可能看不到。
- **演进**：Rail 声明 `failure_policy: open|close` 和 `timeout`，分发器统一执行，并输出 per-rail 耗时直方图。

**二次追问**："before_model_call 里改 messages 为什么不生效？" → 上下文窗口在 `_railed_model_call` 里是从 ModelContext 重建的（`react_agent.py:1233`）。要改输入，应该通过 window mutator 或 `PromptAttachmentManager` 注入，而不是直接改 `ctx.inputs.messages`。这是一个故意的设计：只有一个事实来源。

### Q5. 上下文压缩的触发阈值怎么定？Token 计数为什么用 `len//3` 估算？

**答**：
- **处理器链**：`MessageSummaryOffloader` 对超大的单条 ToolMessage（超过上下文 10%）卸载到外部，并按 300s TTL 回收；三个 LLM 压缩器在占用 80% 时触发，最小压缩目标 10%；上下文上限默认 200k。
- **为什么是 80%**：给本轮输出和工具结果留余量；过早压缩浪费 LLM 调用，还会损失信息。
- **计数策略**：优先用最后一条 assistant 消息返回的真实 usage，只对尾部新增消息用 `len//3` 估算。这样避免每轮全量 tokenize，在中英混合文本下误差可接受。
- **兜底**：真溢出时 `ContextEngine.recover_from_model_exception` 先压缩再重试一次。
- **可改进的地方**：
  - 阈值应该按模型的 max context 和输出预算动态计算，而不是写死比例；
  - 压缩会破坏 KV cache 的前缀命中，项目里有 `kv_cache` 模块，压缩策略应该优先保证前缀稳定。

**二次追问**："压缩后丢了关键信息怎么办？" → 卸载器保留可召回的引用，而不是直接删除；Goal Mode 把目标单独作为 RUNTIME 附件注入，不参与压缩，保证"要做什么"永远在上下文里。

### Q6. 为什么拆成三个进程？IPC 为什么自定义帧协议而不用 gRPC？

**答**：
- **为什么三个进程**：Gateway 与渠道和连接的生命周期绑定；Worker 与推理负载绑定。两者扩缩容维度和故障模式完全不同，Front 夹在中间做准入和多路复用。
- **为什么不用 gRPC**：Front 和 Worker 在**同一台机器**上，通过 Unix socket 通信，延迟最低，也不需要服务发现和 TLS。帧格式是"4 字节长度 + JSON"，`PROTOCOL_VERSION=2`，单帧上限 32MiB，用 `kind` 字段多路复用（request / wire / cancel / heartbeat / execution.subscribe…）。调试时直接 dump 就能读，演进时靠版本号做兼容。
- **代价**：JSON 的序列化开销，以及没有 IDL 带来的类型约束。
- **演进**：跨机部署时，这一跳可以替换成 gRPC streaming，帧语义可以一一映射。

**二次追问**："Worker 卡死但进程还活着怎么办？" → 两层检测：10s 心跳、30s 收不到帧判死；另外业务循环停滞超过 180s 也会替换 Worker。后者正是为了"进程活着但 event loop 被同步调用阻塞"这种情况。

### Q7. Checkpoint 写到一半进程被 kill -9，会怎样？

**答**：
- **现状**：pickle 序列化后写入 SQLite（WAL 模式）的 `kv_store` 表。两个键在同一个事务里 upsert，所以**单次写入是原子的**，不会留下半成品。
- **真正的风险在读侧**：没有版本链、没有校验和，反序列化失败时只记日志，然后**静默当作空状态重新开始**，用户感知就是"上下文丢了"。另外 jiuwenswarm 传入的 `db_journal_mode` 配置，agent-core 的 checkpointer provider 根本不读。
- **其他持久化在 kill -9 时的表现**：`history.jsonl` 的追加不是原子的，但读取端会跳过坏行；遗留的 `.tmp` 孤儿文件不会被清理。
- **演进**：
  - 保留最近 N 个版本，带 checksum；反序列化失败时回退到上一版本，原始数据转存为 `.corrupt`；
  - 用版本化 schema（msgpack / pydantic）替代 pickle，同时解决跨版本兼容和反序列化安全问题。

**二次追问**："pickle 的安全问题？" → checkpoint 只由本进程写、本进程读，攻击面是本地文件系统权限。但从纵深防御角度，替换掉 pickle 是正确的方向。

### Q8. Team DB 用 SQLite，多个 Teammate 并发抢任务怎么保证不重复？

**答**：
- **claim**：`TaskDao.claim_task`（`AT/tools/database/task_dao.py:906`）是一条带条件的 UPDATE（`WHERE assignee IS NULL AND status = pending`），本质是 CAS，看影响行数判断是否抢到。complete 在一个事务里切换状态并解锁依赖它的任务。任务状态机由 `TASK_TRANSITIONS`（`status.py:348`）约束。
- **单进程内**：所有写操作经一把 asyncio 写锁串行化，SQLite 本身的单写者特性也匹配这个模型。
- **要主动讲的边界**：
  - 这把锁**只在进程内有效**；
  - `update_task_status` / `update_task` 是先 SELECT 再改 ORM，没有 `with_for_update`，也没有 `retry_on_locked`；
  - Teammate 以子进程形态运行且直接写库时，会出现 Lost Update 和 `SQLITE_BUSY`。

  需要补一句：外部 CLI 子进程不直接访问数据库，所以默认部署下风险较低。
- **演进**：所有状态变更统一改为"条件 UPDATE + 版本号"；多节点部署时把 Team DB 切到 Postgres（SQLModel 抽象已经支持），用行锁或乐观锁。

**二次追问**："为什么不一开始就用 Postgres？" → 单机和私有化场景下零依赖是硬需求。SQLModel 让切换成本可控，这是一个"可演进的默认值"。

### Q9. 多智能体互相发消息死循环，Token 打爆，你们怎么防？

**答**：
- **现状**：有 `ReliabilityRail` 和 `PingPongDetector`，但可靠性框架**默认关闭**（`reliability/config.py:109`）。检测器只记录一对全局 `last_from / last_to`，任何第三方消息都会把计数清零，三方循环检测不到。处置方式是给 Leader 发一段文本，这反而会触发新的一轮。全局也没有 hop 上限和团队级 Token 预算。
- **我的方案分三层**：
  1. **消息层**：每条消息携带 `hop_count`，超过阈值丢弃并告警。
  2. **检测层**：在消息图上用滑动窗口统计，检测强连通分量（N 方环），而不是只看成对往返。
  3. **经济层**：团队级 `BudgetLedger`（直接复用 SwarmFlow 的账本，配合预留制），在 `deliver_input` 之前检查。处置动作从"发文本"改为"暂停相关成员的轮次，并上报 Leader 和用户"。

**二次追问**："正常的多轮讨论也会被误杀吧？" → 所以阈值按团队模式配置，并区分"有进展的往返"（任务状态有变化、产出了新 artifact）和"空转往返"。只对空转做熔断。

### Q10. 如果让你重新设计，你会改哪三件事？

**答**（考察架构品味，三件事要有层次）：
1. **一致性模型统一**：把"DB 是事实来源，消息只是唤醒"从任务分配推广到**审批、成员状态、SwarmFlow 人工卡点**。现在审批和 `human()` 的回复只在内存或消息里，这是卡死和丢失问题的共同根因。
2. **副作用显式化**：工具注册时声明 `side_effect: none|idempotent|non_idempotent`。Journal 重放、Rail 重试、上下文溢出后的"压缩再重试"都依据这个声明决定能不能自动重做。这一个元数据能同时解决三个模块的问题。
3. **会话作用域作为一等公民**：从 Gateway 到 Worker 再到 DeepAgent、沙箱，统一一个 `SessionScope`，持有后台任务、沙箱 lease、订阅、缓存条目。关闭时统一回收，并做成可观测的资源账本（就是诊断探针 C 段的"正式版"）。

**收尾话术**："这三点有一个共同主题：把隐式约定变成显式契约。Agent 系统的不确定性已经来自模型本身了，工程层要做的是不再额外制造不确定性。"

---

## 附：面试前 48 小时检查清单

- [ ] 把所有 `【…】` 换成真实数据，或者改写成定性描述；删掉没参与过的 bullet。
- [ ] 能脱稿画出三张图：三进程拓扑、Inner / Outer Loop 状态机、权限审批闭环（参考 01 文档）。
- [ ] 背熟 10 个"源码真名"：`DeepAgent._run_task_loop`、`ReActAgent._inner_invoke`、`AgentRail`、`AgentCallbackManager.execute`、`TeamAgentSpec.build`、`TeamAgent`、`BudgetLedger`、`OnlineEvolutionOrchestrator.evolve`、`append_record_transactional`、`RuntimeSupervisor`。
- [ ] 每个 War Story 准备一个"如果重来会怎么做"。
- [ ] 准备一个**你不同意项目现有设计**的点（推荐 Q4 的 Rail 吞异常策略，或 Q1 的 at-most-once）。能有理有据地批评自己参与的系统，是 Staff 和 Senior 最大的分水岭。
