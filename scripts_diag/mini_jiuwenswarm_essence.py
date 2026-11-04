"""Mini-JiuwenSwarm：jiuwenswarm 五大核心机制的单文件最小参考实现（仅标准库）。

这不是生产代码，而是"架构骨架"：每一节对应真实仓库中的一个子系统，注释里标出了对照的
真实类 / 文件，并且采用的是我们在 02 文档与 patches/ 中修复之后的"正确姿态"。

  §1 Core     ：Rail 扁平优先级链（timeout + fail_open/fail_close）+ Inner/Outer Loop
                 对照 AsyncCallbackFramework.trigger（patch 0002）、ReActAgent._inner_invoke、
                 DeepAgent._run_task_loop
  §2 Runtime  ：Front⇄Worker 长度前缀帧协议 + WorkerEventStore 单调 seq 环形缓冲断线重放
                 对照 server/ipc/protocol.py、server/worker/event_store.py
  §3 Swarm    ：SQLite Team DB 单条 UPDATE…WHERE CAS 抢单 + InteractiveInput 强校验、超时自动拒绝的审批
                 对照 TaskDao.claim_task、TeamApprovalOrchestrator（patch 0001）
  §4 SwarmFlow：作用域序号路径键 ks + 内容签名 sig 的 Journal Replay（intent/done 两阶段防重复副作用）
                 + 预留制 Token 账本   对照 workflow/engine/journal.py（patch 0005）、budget.py（patch 0006）
  §5 main()   ：组队 → 抢单 → 审批 → 并行 SwarmFlow → 模拟崩溃 → 断点续跑命中缓存

运行：python3 scripts_diag/mini_jiuwenswarm_essence.py
"""
from __future__ import annotations

import asyncio
import contextvars
import hashlib
import json
import os
import sqlite3
import struct
import tempfile
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Optional


def log(tag: str, msg: str) -> None:
    print(f"  [{tag:<9}] {msg}")


# ============================================================================ §1 Core：Rails + 双循环
class AbortError(Exception):
    """唯一能穿透 Rail 链、中断主循环的异常（真实仓库：rail 中抛 AbortError，其余异常被记录后吞掉）。"""


@dataclass
class Rail:
    """扁平优先级链中的一个切面。priority 越大越先执行；before/after 同序，不是洋葱模型。"""
    name: str
    hooks: dict[str, Callable[[dict], Awaitable[None]]]
    priority: int = 50
    timeout: Optional[float] = None   # 修复后：串行触发也尊重 timeout（原实现只在并行触发生效）
    fail_close: bool = False          # 安全 / 预算类 Rail 应 fail-close；观测类 Rail 默认 fail-open


class RailChain:
    def __init__(self, rails: list[Rail]):
        self.rails = sorted(rails, key=lambda r: -r.priority)

    async def fire(self, event: str, ctx: dict) -> None:
        for r in self.rails:
            hook = r.hooks.get(event)
            if hook is None:
                continue
            try:
                await asyncio.wait_for(hook(ctx), r.timeout) if r.timeout else await hook(ctx)
            except AbortError:
                raise
            except Exception as e:  # noqa: BLE001  —— TimeoutError 也走这里
                if r.fail_close:
                    raise AbortError(f"rail {r.name} failed closed: {type(e).__name__}") from e
                log("rail", f"{r.name}.{event} {type(e).__name__} → fail-open，继续执行后续 Rail")


async def fake_llm(messages: list[dict]) -> dict:
    """确定性假模型：第一轮请求调用工具，拿到工具结果后给出最终答案。"""
    await asyncio.sleep(0.01)
    if not any(m["role"] == "tool" for m in messages):
        return {"tool_call": {"name": "bash", "args": {"cmd": "echo hi"}}}
    return {"final": f"done({messages[-1]['content']})"}


class MiniDeepAgent:
    """Outer Loop（任务级轮次，对照 DeepAgent._run_task_loop）包着 Inner Loop（ReAct 迭代）。"""

    def __init__(self, name: str, rails: RailChain, tools: dict[str, Callable], max_iter: int = 4):
        self.name, self.rails, self.tools, self.max_iter = name, rails, tools, max_iter

    async def _inner(self, ctx: dict) -> Optional[str]:
        for _ in range(self.max_iter):                       # Inner Loop
            await self.rails.fire("before_model_call", ctx)
            out = await fake_llm(ctx["messages"])
            if "final" in out:
                return out["final"]
            call = out["tool_call"]
            ctx["tool_call"] = call
            await self.rails.fire("before_tool_call", ctx)
            try:
                result = await self.tools[call["name"]](**call["args"])
            except Exception as e:  # noqa: BLE001  —— 工具失败是"软失败"：错误回灌给模型
                result = f"ERROR: {e}"
            ctx["messages"].append({"role": "tool", "content": result})
            await self.rails.fire("after_tool_call", ctx)
        return None                                          # → MAX_ITERATIONS

    async def run(self, query: str, max_rounds: int = 3) -> str:
        ctx: dict = {"agent": self.name, "messages": [{"role": "user", "content": query}]}
        for rnd in range(max_rounds):                        # Outer Loop
            try:
                final = await self._inner(ctx)
            except AbortError as e:
                return f"ABORTED: {e}"
            except asyncio.CancelledError:
                log("loop", f"{self.name} 被取消：释放资源后重新抛出")   # 取消必须继续上抛，绝不吞掉
                raise
            if final is not None:
                return final
            log("loop", f"{self.name} 第 {rnd} 轮未完成 → 外循环续跑")
        return "MAX_ROUNDS"


# ============================================================================ §2 Runtime：帧协议 + 序号重放
def encode_frame(kind: str, body: dict) -> bytes:
    """4 字节大端长度 + JSON（对照 PROTOCOL_VERSION=2 的 Front⇄Worker Unix-socket 帧）。"""
    raw = json.dumps({"v": 2, "kind": kind, **body}, ensure_ascii=False).encode()
    return struct.pack(">I", len(raw)) + raw


def decode_frames(buf: bytearray) -> list[dict]:
    out = []
    while len(buf) >= 4:
        (n,) = struct.unpack(">I", buf[:4])
        if len(buf) < 4 + n:
            break                                            # 半帧：等下一次读取
        out.append(json.loads(bytes(buf[4:4 + n])))
        del buf[:4 + n]
    return out


class WorkerEventStore:
    """每帧分配单调递增 seq，环形缓冲保留最近 N 帧；重连时 subscribe(after) 补齐断档。"""

    def __init__(self, capacity: int = 2048):
        self.seq = 0
        self.ring: deque[tuple[int, bytes]] = deque(maxlen=capacity)

    def append(self, chunk: dict) -> int:
        self.seq += 1
        self.ring.append((self.seq, encode_frame("wire", {"seq": self.seq, "chunk": chunk})))
        return self.seq

    def subscribe(self, after: int) -> list[bytes]:
        if self.ring and self.ring[0][0] > after + 1:
            raise LookupError(f"gap: frames {after + 1}..{self.ring[0][0] - 1} already evicted")
        return [f for s, f in self.ring if s > after]


# ============================================================================ §3 Swarm：Team DB CAS + 审批闭环
class TeamDB:
    """事实来源。消息只负责唤醒；任何"必须送达"的语义（任务、审批）都落库。"""

    def __init__(self, path: str):
        self.path = path                                     # 文件库：每个并发抢单者用独立连接，CAS 才是真竞争
        self.db = sqlite3.connect(path, isolation_level=None, check_same_thread=False)
        self.db.execute("CREATE TABLE task(id TEXT PRIMARY KEY, title TEXT, status TEXT, assignee TEXT)")
        self.db.execute("CREATE TABLE approval(call_id TEXT PRIMARY KEY, member TEXT, tool TEXT, state TEXT)")

    def create_task(self, tid: str, title: str) -> None:
        self.db.execute("INSERT INTO task VALUES(?,?,'pending',NULL)", (tid, title))

    def claim_task(self, tid: str, member: str) -> bool:
        """单条条件 UPDATE = CAS：看影响行数判断是否抢到（对照 TaskDao.claim_task）。"""
        conn = sqlite3.connect(self.path, timeout=5, isolation_level=None)
        try:
            cur = conn.execute(
                "UPDATE task SET status='in_progress', assignee=? WHERE id=? AND status='pending' AND assignee IS NULL",
                (member, tid))
            return cur.rowcount == 1
        finally:
            conn.close()

    def complete_task(self, tid: str, member: str) -> bool:
        cur = self.db.execute("UPDATE task SET status='completed' WHERE id=? AND assignee=? AND status='in_progress'",
                              (tid, member))
        return cur.rowcount == 1


@dataclass(frozen=True)
class InteractiveInput:
    call_id: str
    approved: bool

    @classmethod
    def coerce(cls, raw: Any) -> "InteractiveInput":
        """修复后的兜底路径：dict 先转成强类型，而不是原样传进 resume 然后被丢弃（patch 0001）。"""
        if isinstance(raw, cls):
            return raw
        if isinstance(raw, dict) and "call_id" in raw:
            return cls(str(raw["call_id"]), bool(raw.get("approved")))
        raise TypeError(f"invalid resume input: {raw!r}")


class ApprovalBroker:
    def __init__(self, db: TeamDB, timeout: float):
        self.db, self.timeout = db, timeout
        self.pending: dict[str, asyncio.Future] = {}

    async def request(self, member: str, call_id: str, tool: str) -> bool:
        self.db.db.execute("INSERT INTO approval VALUES(?,?,?,'pending')", (call_id, member, tool))
        fut = self.pending[call_id] = asyncio.get_running_loop().create_future()
        try:
            return await asyncio.wait_for(fut, self.timeout)          # 无超时 = 循环等待的温床
        except asyncio.TimeoutError:
            self._settle(call_id, "expired")
            log("approve", f"{member}/{tool} 审批超时 → 自动拒绝（fail-close）")
            return False
        finally:
            self.pending.pop(call_id, None)

    def resolve(self, raw: Any) -> bool:
        """只有在校验通过、DB 状态迁移成功之后才算"已读"——顺序反过来就会丢审批。"""
        inp = InteractiveInput.coerce(raw)
        fut = self.pending.get(inp.call_id)
        if fut is None or fut.done():
            return False
        self._settle(inp.call_id, "approved" if inp.approved else "denied")
        fut.set_result(inp.approved)
        return True

    def _settle(self, call_id: str, state: str) -> None:
        self.db.db.execute("UPDATE approval SET state=? WHERE call_id=? AND state='pending'", (state, call_id))


# ============================================================================ §4 SwarmFlow：Journal + 预算
class BudgetExhausted(Exception):
    pass


class BudgetLedger:
    """预留制：先 reserve 估算值，完成后 commit 实际值。超支上界 = 估算误差，与并发度无关（patch 0006）。"""

    def __init__(self, total: int):
        self.total, self.spent, self.reserved = total, 0, 0
        self.cond = asyncio.Condition()

    async def reserve(self, est: int) -> None:
        async with self.cond:
            while self.spent + self.reserved + est > self.total:
                if self.reserved == 0:                       # 没有在途调用可等了：真的耗尽
                    raise BudgetExhausted(f"spent={self.spent} total={self.total}")
                await self.cond.wait()                       # 等在途调用结算后再判断
            self.reserved += est

    async def commit(self, est: int, actual: int) -> None:
        async with self.cond:
            self.reserved -= est
            self.spent += actual
            self.cond.notify_all()


_scope: contextvars.ContextVar[tuple[str, list[int]]] = contextvars.ContextVar("scope")


class Journal:
    """WAL 追加写 + 容错加载。命中条件：ks（结构化位置）+ sig（内容签名）+ run_id 三者都匹配。"""

    def __init__(self, path: str, run_id: str):
        self.path, self.run_id, self.cache, self.intents = path, run_id, {}, set()
        self.hits = self.misses = 0
        if os.path.exists(path):
            with open(path, "rb") as f:
                for line in f.read().split(b"\n"):
                    try:                                     # 坏行（撕裂行 / 半个 UTF-8 字符）跳过并计数，绝不整体崩溃
                        rec = json.loads(line.decode("utf-8"))
                    except (UnicodeDecodeError, json.JSONDecodeError):
                        continue
                    if rec["run"] != run_id:
                        continue
                    if rec["t"] == "intent":
                        self.intents.add(rec["ks"])
                    else:
                        self.cache[(rec["ks"], rec["sig"])] = rec["out"]
                        self.intents.discard(rec["ks"])

    def append(self, rec: dict) -> None:
        with open(self.path, "ab") as f:
            if f.tell() > 0:                                 # 修复撕裂尾行：先补换行再追加
                with open(self.path, "rb") as r:
                    r.seek(-1, os.SEEK_END)
                    if r.read(1) != b"\n":
                        f.write(b"\n")
            f.write(json.dumps({"run": self.run_id, **rec}, ensure_ascii=False).encode() + b"\n")
            f.flush()
            os.fsync(f.fileno())


class FlowRuntime:
    def __init__(self, journal: Journal, budget: BudgetLedger, crash_after: Optional[int] = None):
        self.j, self.budget, self.crash_after, self.calls = journal, budget, crash_after, 0

    @staticmethod
    def _next_ks() -> str:
        """作用域路径 + 本作用域内的调用序号：与完成顺序无关，只与代码结构有关 → 确定性。"""
        path, counter = _scope.get()
        counter[0] += 1
        return f"{path}/call{counter[0]}"

    async def agent(self, prompt: str, cost: int = 100) -> str:
        ks = self._next_ks()
        sig = hashlib.sha256(prompt.encode()).hexdigest()[:16]
        if (ks, sig) in self.j.cache:
            self.j.hits += 1
            return self.j.cache[(ks, sig)]
        self.j.misses += 1
        await self.budget.reserve(cost)
        actual = 0                                           # 失败的调用只释放预留，不计费
        try:
            if self.crash_after is not None and self.calls >= self.crash_after:
                raise RuntimeError("模拟进程崩溃（kill -9）")
            await asyncio.sleep(0.01)
            out, actual = f"answer<{prompt}>", cost
            self.calls += 1
        finally:
            await self.budget.commit(cost, actual)
        self.j.append({"t": "done", "ks": ks, "sig": sig, "out": out})
        self.j.cache[(ks, sig)] = out
        return out

    async def side_effect(self, name: str, fn: Callable[[], Awaitable[str]]) -> str:
        """intent/done 两阶段：有 intent 无 done = 副作用可能已发生 → 不自动重做，转人工确认。"""
        ks = self._next_ks()
        if (ks, name) in self.j.cache:
            self.j.hits += 1
            return self.j.cache[(ks, name)]
        if ks in self.j.intents:
            return f"NEEDS_HUMAN: {name} 可能已执行（有 intent 无 done），拒绝自动重放"
        self.j.append({"t": "intent", "ks": ks})
        out = await fn()
        self.j.append({"t": "done", "ks": ks, "sig": name, "out": out})
        return out

    async def parallel(self, *branches: Callable[[], Awaitable[Any]]) -> list[Any]:
        path, _ = _scope.get()

        async def run_branch(i: int, b: Callable[[], Awaitable[Any]]):
            _scope.set((f"{path}/par{i}", [0]))              # 每个分支独立的子作用域
            return await b()
        return await asyncio.gather(*(asyncio.create_task(run_branch(i, b)) for i, b in enumerate(branches)))

    async def run(self, flow: Callable[["FlowRuntime"], Awaitable[Any]]) -> Any:
        _scope.set(("root", [0]))
        return await flow(self)


# ============================================================================ §5 main：全流程自检演示
async def demo_flow(rt: FlowRuntime) -> list[str]:
    plan = await rt.agent("拆解需求：给仓库加一个健康检查接口")
    results = await rt.parallel(*[
        (lambda k=k: rt.agent(f"实现子任务 {k}（依据：{plan[:12]}）")) for k in range(4)])
    review = await rt.agent(f"评审 {len(results)} 份产出")

    async def git_commit() -> str:
        await asyncio.sleep(0)
        return "commit 3f2a9c"
    commit = await rt.side_effect("git_commit", git_commit)
    return [plan, *results, review, commit]


async def main() -> None:
    print("\n=== §1 Core：Rail 切面 + 双循环 ===")

    async def hung(ctx: dict) -> None:
        await asyncio.sleep(3600)                            # 永不返回的 hook

    async def audit(ctx: dict) -> None:
        log("rail", f"audit 看到工具调用 {ctx['tool_call']['name']}")

    async def bash(cmd: str) -> str:
        return f"$ {cmd} → hi"
    chain = RailChain([Rail("slow_observer", {"before_model_call": hung}, priority=90, timeout=0.2),
                       Rail("audit", {"before_tool_call": audit}, priority=10)])
    t0 = asyncio.get_running_loop().time()
    print("  结果:", await MiniDeepAgent("leader", chain, {"bash": bash}).run("打个招呼"),
          f"（耗时 {asyncio.get_running_loop().time() - t0:.2f}s：挂起的 Rail 被 0.2s 熔断，没有卡死主循环）")
    guard = RailChain([Rail("budget_guard", {"before_model_call": hung}, timeout=0.1, fail_close=True)])
    print("  fail-close:", await MiniDeepAgent("leader", guard, {"bash": bash}).run("x"))

    print("\n=== §2 Runtime：帧协议 + 断线重放 ===")
    store = WorkerEventStore(capacity=8)
    for i in range(6):
        store.append({"delta": f"token{i}"})
    buf = bytearray(b"".join(store.subscribe(after=0))[:-5])  # 模拟只收到一半，最后一帧被截断
    got = decode_frames(buf)
    last = got[-1]["seq"]
    log("front", f"断线前收到 seq 1..{last}，残留半帧 {len(buf)}B 丢弃")
    for i in range(6, 9):
        store.append({"delta": f"token{i}"})
    replay = decode_frames(bytearray(b"".join(store.subscribe(after=last))))
    log("front", f"重连 subscribe(after={last}) 补回 seq {[f['seq'] for f in replay]}")
    try:
        store.subscribe(after=0)
    except LookupError as e:
        log("front", f"太旧的游标 → {e}（应改为全量快照恢复）")

    print("\n=== §3 Swarm：组队 → CAS 抢单 → 审批 ===")
    db = TeamDB(os.path.join(tempfile.mkdtemp(prefix="mini_team_"), "team.db"))
    db.create_task("T1", "实现健康检查接口")
    members = ["alice", "bob", "carol"]
    wins = await asyncio.gather(*(asyncio.to_thread(db.claim_task, "T1", m) for m in members))
    winner = members[wins.index(True)]
    log("teamdb", f"3 名 Teammate 并发抢 T1 → 只有 {winner} 成功 {wins}")
    broker = ApprovalBroker(db, timeout=0.3)
    req = asyncio.create_task(broker.request(winner, "call-1", "bash"))
    await asyncio.sleep(0.01)
    ok = broker.resolve({"call_id": "call-1", "approved": True})  # Leader 走 JSON 兜底路径也不会丢
    log("approve", f"Leader 以 dict 批准 → coerce 为 InteractiveInput，resolve={ok}，结果={await req}")
    log("approve", f"bob 申请 rm -rf 无人批 → {await broker.request('bob', 'call-2', 'rm')}")
    log("teamdb", f"{winner} 完成 T1: {db.complete_task('T1', winner)}；"
        f"审批表 {db.db.execute('SELECT call_id, state FROM approval').fetchall()}")

    print("\n=== §4 SwarmFlow：并行 → 崩溃 → 断点续跑 ===")
    wal = os.path.join(tempfile.mkdtemp(prefix="mini_swarm_"), "run.wal")
    rt1 = FlowRuntime(Journal(wal, "run-42"), BudgetLedger(total=10_000), crash_after=3)
    try:
        await rt1.run(demo_flow)
    except RuntimeError as e:
        log("flow", f"第一次运行崩溃：{e}（崩溃前已完成并落盘 {rt1.calls} 次模型调用，评审与 commit 未执行）")
    with open(wal, "ab") as f:
        f.write('{"run":"run-42","t":"done","ks":"root/x","sig":"截'.encode()[:-1])  # 撕裂行 + 半个中文字符
    rt2 = FlowRuntime(Journal(wal, "run-42"), BudgetLedger(total=10_000))
    out = await rt2.run(demo_flow)
    log("flow", f"续跑：缓存命中 {rt2.j.hits}，新调用 {rt2.j.misses}，账本 spent={rt2.budget.spent}"
        f"（坏尾行被跳过，撕裂行后的追加不会粘连）")
    log("flow", f"副作用：{out[-1]}")
    rt3 = FlowRuntime(Journal(wal, "run-42"), BudgetLedger(total=10_000))
    await rt3.run(demo_flow)
    log("flow", f"第三次运行：命中 {rt3.j.hits}，新调用 {rt3.j.misses}（全部命中，零 Token）")

    log("flow", f"intent 无 done：{await _intent_demo(wal)}")

    async def burst(rt: FlowRuntime) -> list[str]:
        return await rt.parallel(*[(lambda k=k: rt.agent(f"q{k}", cost=1500)) for k in range(16)])
    rt4 = FlowRuntime(Journal(wal + ".b", "b"), BudgetLedger(total=10_000))
    try:
        await rt4.run(burst)
    except BudgetExhausted as e:
        log("budget", f"16 并发 × 1500 token，上限 10k → 熔断 {e}；超支 "
            f"{max(0, rt4.budget.spent - 10_000) / 100:.0f}%（预留制，事后累加版本实测为 230%）")
    print("\n全部机制自检完成。")


async def _intent_demo(wal: str) -> str:
    """单独演示 intent/done：在新 run 中只写 intent 不写 done，再次执行同一步骤时拒绝自动重放。"""
    j = Journal(wal, "run-44")
    j.append({"t": "intent", "ks": "root/call1"})
    rt = FlowRuntime(Journal(wal, "run-44"), BudgetLedger(10_000))
    _scope.set(("root", [0]))

    async def effect() -> str:
        return "commit again?!"
    return await rt.side_effect("git_commit", effect)


if __name__ == "__main__":
    asyncio.run(main())
