"""jiuwen_diag_probe —— 非侵入式生产诊断探针（Monkey-Patch + 周期/信号转储）。

挂载方式（任选其一，均无需改业务代码）：
  1) 启动前设置环境变量后在入口最早处 import：
       JIUWEN_DIAG=1 python -c "import jiuwen_diag_probe; jiuwen_diag_probe.install()" ...
  2) 放进 sitecustomize.py：  import jiuwen_diag_probe; jiuwen_diag_probe.install()
  3) 线上已运行进程：通过 py-spy / pyrasite / debugpy 注入执行 install()（需在事件循环线程外安全调用，
     本探针的 install() 只做 patch 与启动守护线程，不碰运行中的 loop 内部状态）。

触发转储：
  - 周期：JIUWEN_DIAG_INTERVAL 秒（默认 60，0 关闭周期转储）
  - 信号：kill -USR2 <pid>   （Windows 不支持，使用周期模式）
  - 输出：JIUWEN_DIAG_OUT（默认 /tmp/jiuwen_diag_<pid>.log），同时 stderr 打印摘要

能力：
  A. 慢调用耗时：包裹 Rail 回调分发、LLM 调用、工具调用，超过阈值打印（JIUWEN_DIAG_SLOW_MS，默认 5000）
     并统计 p50/p95/max；"进行中"的调用持续追踪，可发现永不返回的悬挂调用。
  B. 悬挂 asyncio.Task：枚举所有事件循环中的 Task，打印存活超过阈值（JIUWEN_DIAG_TASK_AGE_S，默认 120）
     的 Task 栈（含 await 链），并按协程名聚合数量——数量单调增长即 Task Leak。
  C. Session 内存：tracemalloc 按文件 Top-N + 基于 gc 引用图对持有 session_id 的对象做近似保留大小估算。
  D. Messager / 队列积压：扫描所有 asyncio.Queue（含 EventBus._event_queue、StreamController.stream_queue、
     WorkerEventStore 缓冲等），打印 qsize、getters/putters 数，以及 pyzmq socket 数量。

设计原则：所有 patch 均 try/except 包裹，目标不存在时静默跳过；探针自身异常绝不传播到业务。
"""
from __future__ import annotations

import asyncio
import collections
import gc
import importlib
import io
import os
import signal
import sys
import threading
import time
import traceback
import tracemalloc
import weakref
from typing import Any, Callable, Dict, List, Optional, Tuple

_SLOW_MS = float(os.getenv("JIUWEN_DIAG_SLOW_MS", "5000"))
_TASK_AGE_S = float(os.getenv("JIUWEN_DIAG_TASK_AGE_S", "120"))
_INTERVAL = float(os.getenv("JIUWEN_DIAG_INTERVAL", "60"))
_OUT = os.getenv("JIUWEN_DIAG_OUT", f"/tmp/jiuwen_diag_{os.getpid()}.log")
_TOPN = int(os.getenv("JIUWEN_DIAG_TOPN", "15"))

# --------------------------------------------------------------------------- A. 慢调用
# 目标：(模块路径, 类名, 方法名, 标签)。均为源码中真实存在的异步方法（见 01_ARCHITECTURE_BIBLE.md）。
_PATCH_TARGETS: List[Tuple[str, str, str, str]] = [
    # Rail 回调分发：每个 AgentCallbackEvent 的全部 rail 在此串行执行
    ("openjiuwen.core.single_agent.agent_callback_manager", "AgentCallbackManager", "execute", "rail"),
    # 内循环：LLM 调用（含 before/after_model_call rails）
    ("openjiuwen.core.single_agent.agents.react_agent", "ReActAgent", "_call_model", "llm"),
    # 内循环：工具批量执行
    ("openjiuwen.core.single_agent.agents.react_agent", "ReActAgent", "_execute_tool_call", "tool"),
    # 团队：成员输入投递
    ("openjiuwen.agent_teams.agent.team_agent", "TeamAgent", "deliver_input", "team.deliver"),
    # 单个能力（工具/子 agent）执行入口
    ("openjiuwen.core.single_agent.ability_manager", "AbilityManager", "execute", "ability"),
]

_stats: Dict[str, List[float]] = collections.defaultdict(list)
_inflight: Dict[int, Tuple[str, float, str]] = {}  # id -> (label, start, detail)
_lock = threading.Lock()


def _detail(label: str, args: tuple, kwargs: dict) -> str:
    try:
        if label == "rail" and args:
            ev = args[0]
            return str(getattr(ev, "value", ev))
        if label == "tool" and len(args) >= 2:
            names = []
            for tc in args[1] or []:
                names.append(getattr(tc, "name", None) or getattr(getattr(tc, "function", None), "name", "?"))
            return ",".join(map(str, names))[:200]
    except Exception:  # noqa: BLE001
        pass
    return ""


def _wrap(orig: Callable, label: str) -> Callable:
    async def wrapper(*args, **kwargs):
        detail = _detail(label, args[1:], kwargs)
        key = id(asyncio.current_task()) ^ id(args[0]) ^ int(time.monotonic_ns())
        t0 = time.monotonic()
        with _lock:
            _inflight[key] = (label, t0, detail)
        try:
            return await orig(*args, **kwargs)
        finally:
            dt = (time.monotonic() - t0) * 1000
            with _lock:
                _inflight.pop(key, None)
                s = _stats[label]
                s.append(dt)
                if len(s) > 5000:
                    del s[:2500]
            if dt >= _SLOW_MS:
                _emit(f"[SLOW] {label} {detail} {dt:.0f}ms task={_task_name()}")

    wrapper.__wrapped__ = orig  # type: ignore[attr-defined]
    wrapper.__jiuwen_diag__ = True  # type: ignore[attr-defined]
    return wrapper


def _task_name() -> str:
    try:
        t = asyncio.current_task()
        return t.get_name() if t else "-"
    except RuntimeError:
        return "-"


def _patch_all() -> List[str]:
    done = []
    for mod, cls, meth, label in _PATCH_TARGETS:
        try:
            m = importlib.import_module(mod)
            c = getattr(m, cls)
            orig = getattr(c, meth)
            if getattr(orig, "__jiuwen_diag__", False):
                continue
            setattr(c, meth, _wrap(orig, label))
            done.append(f"{cls}.{meth}")
        except Exception as e:  # noqa: BLE001
            done.append(f"SKIP {cls}.{meth}: {type(e).__name__}")
    return done


def _pct(v: List[float], p: float) -> float:
    if not v:
        return 0.0
    s = sorted(v)
    return s[min(len(s) - 1, int(len(s) * p))]


def _report_slow(buf: io.StringIO) -> None:
    buf.write("== A. 调用耗时（ms）\n")
    with _lock:
        for label, v in sorted(_stats.items()):
            buf.write(f"  {label:<14} n={len(v):<6} p50={_pct(v, .5):8.0f} p95={_pct(v, .95):8.0f} max={max(v or [0]):8.0f}\n")
        now = time.monotonic()
        hang = sorted(((now - t0, lb, d) for lb, t0, d in _inflight.values()), reverse=True)
    buf.write("  -- 进行中调用（疑似悬挂：持续时间降序）\n")
    for age, lb, d in hang[:_TOPN]:
        flag = "  <<< HANG?" if age * 1000 > _SLOW_MS * 4 else ""
        buf.write(f"    {lb:<14} {age:8.1f}s {d}{flag}\n")


# --------------------------------------------------------------------------- B. 悬挂 Task
_first_seen: "weakref.WeakKeyDictionary[asyncio.Task, float]" = weakref.WeakKeyDictionary()
_prev_counts: Dict[str, int] = {}


def _all_tasks() -> List[asyncio.Task]:
    tasks = []
    for obj in gc.get_objects():
        try:
            if isinstance(obj, asyncio.Task) and not obj.done():
                tasks.append(obj)
        except Exception:  # noqa: BLE001
            continue
    return tasks


def _coro_name(t: asyncio.Task) -> str:
    c = t.get_coro()
    return getattr(c, "__qualname__", None) or repr(c)[:80]


def _await_chain(t: asyncio.Task) -> str:
    # 沿 cr_await 链走到最内层 await 点（比 get_stack 更能看出卡在哪个 Future/Queue）
    out, c, depth = [], t.get_coro(), 0
    while c is not None and depth < 30:
        frame = getattr(c, "cr_frame", None) or getattr(c, "gi_frame", None)
        if frame is not None:
            out.append(f"      at {frame.f_code.co_filename}:{frame.f_lineno} {frame.f_code.co_name}")
        nxt = getattr(c, "cr_await", None) or getattr(c, "gi_yieldfrom", None)
        if nxt is None:
            break
        if not hasattr(nxt, "cr_frame") and not hasattr(nxt, "gi_frame"):
            out.append(f"      awaiting {type(nxt).__name__}: {repr(nxt)[:160]}")
            break
        c, depth = nxt, depth + 1
    return "\n".join(out)


def _report_tasks(buf: io.StringIO) -> None:
    now = time.monotonic()
    tasks = _all_tasks()
    counts = collections.Counter(_coro_name(t) for t in tasks)
    buf.write(f"== B. asyncio Task 总数 {len(tasks)}（按协程聚合，Δ=较上次增量，持续正增长=泄漏）\n")
    for name, n in counts.most_common(_TOPN):
        delta = n - _prev_counts.get(name, 0)
        buf.write(f"  {n:6d}  Δ{delta:+5d}  {name}\n")
    _prev_counts.clear()
    _prev_counts.update(counts)
    old = []
    for t in tasks:
        first = _first_seen.setdefault(t, now)
        if now - first >= _TASK_AGE_S:
            old.append((now - first, t))
    old.sort(key=lambda x: -x[0])
    buf.write(f"  -- 存活 ≥{_TASK_AGE_S:.0f}s 的 Task（Top {_TOPN}，含 await 链）\n")
    for age, t in old[:_TOPN]:
        buf.write(f"    [{age:7.0f}s] {t.get_name()} :: {_coro_name(t)}\n{_await_chain(t)}\n")


# --------------------------------------------------------------------------- C. Session 内存
_SESSION_ATTRS = ("session_id", "_session_id", "conversation_id", "sid")


def _report_memory(buf: io.StringIO) -> None:
    buf.write("== C. 内存\n")
    try:
        import resource  # noqa: PLC0415

        buf.write(f"  RSS(max) = {resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024:.0f} MB\n")
    except Exception:  # noqa: BLE001
        pass
    if tracemalloc.is_tracing():
        snap = tracemalloc.take_snapshot()
        buf.write(f"  tracemalloc Top {_TOPN}（按文件:行）\n")
        for st in snap.statistics("lineno")[:_TOPN]:
            buf.write(f"    {st.size / 1048576:8.2f} MB  {st.count:8d}  {st.traceback[0]}\n")
    # 近似：按 session_id 聚合"直接持有该 session 的对象"的浅层大小 + 其 __dict__ 中容器的浅层大小
    per: Dict[str, List[int]] = collections.defaultdict(lambda: [0, 0])
    for obj in gc.get_objects():
        try:
            d = getattr(obj, "__dict__", None)
            if not d:
                continue
            sid = None
            for a in _SESSION_ATTRS:
                v = d.get(a)
                if isinstance(v, str) and v:
                    sid = v
                    break
            if sid is None:
                continue
            size = sys.getsizeof(obj) + sum(sys.getsizeof(v) for v in d.values() if isinstance(v, (list, dict, str, bytes)))
            per[sid][0] += size
            per[sid][1] += 1
        except Exception:  # noqa: BLE001
            continue
    buf.write(f"  Session 近似占用 Top {_TOPN}（session 数={len(per)}；持续增长=会话未回收）\n")
    for sid, (sz, n) in sorted(per.items(), key=lambda x: -x[1][0])[:_TOPN]:
        buf.write(f"    {sz / 1024:10.1f} KB  objs={n:<6} {sid[:64]}\n")


# --------------------------------------------------------------------------- D. 队列积压
def _report_queues(buf: io.StringIO) -> None:
    buf.write("== D. 队列 / Messager 积压\n")
    rows = []
    owners: Dict[int, str] = {}
    for obj in gc.get_objects():
        try:
            d = getattr(obj, "__dict__", None)
            if not d:
                continue
            for k, v in d.items():
                if isinstance(v, asyncio.Queue):
                    owners[id(v)] = f"{type(obj).__name__}.{k}"
        except Exception:  # noqa: BLE001
            continue
    for obj in gc.get_objects():
        try:
            if isinstance(obj, asyncio.Queue):
                q = obj
                rows.append((q.qsize(), q.maxsize, len(getattr(q, "_getters", ())), len(getattr(q, "_putters", ())),
                             owners.get(id(q), type(q).__name__)))
        except Exception:  # noqa: BLE001
            continue
    rows.sort(key=lambda r: -r[0])
    buf.write(f"  asyncio.Queue 总数 {len(rows)}；maxsize=0 表示无界（背压缺失）\n")
    for qs, mx, g, p, owner in rows[:_TOPN]:
        flag = "  <<< BACKLOG" if qs > 1000 else ""
        buf.write(f"    qsize={qs:<8} max={mx:<6} getters={g:<3} putters={p:<3} {owner}{flag}\n")
    # pyzmq socket 统计（P2P 每消息新建 DEALER，数量持续上涨=socket 泄漏）
    try:
        import zmq  # noqa: PLC0415

        socks = [o for o in gc.get_objects() if isinstance(o, zmq.Socket)]
        open_s = [s for s in socks if not s.closed]
        kinds = collections.Counter(getattr(zmq.SocketType(s.type), "name", s.type) for s in open_s)
        buf.write(f"  zmq sockets open={len(open_s)} total={len(socks)} {dict(kinds)}\n")
    except Exception:  # noqa: BLE001
        pass
    # SwarmFlow 预算账本（BudgetLedger：spent/total/phase_tokens）
    try:
        from openjiuwen.agent_teams.workflow.engine.budget import BudgetLedger  # noqa: PLC0415

        for o in gc.get_objects():
            if isinstance(o, BudgetLedger):
                top = sorted(o.phase_tokens.items(), key=lambda x: -x[1])[:3]
                pct = f"{o.spent / o.total:.0%}" if o.total else "unbounded"
                buf.write(f"  BudgetLedger spent={o.spent} total={o.total} ({pct}) top_phases={top}\n")
    except Exception:  # noqa: BLE001
        pass
    # 文件描述符（Linux）
    try:
        fds = os.listdir("/proc/self/fd")
        kinds2 = collections.Counter()
        for fd in fds:
            try:
                tgt = os.readlink(f"/proc/self/fd/{fd}")
                kinds2[tgt.split(":")[0] if ":" in tgt else "file"] += 1
            except OSError:
                continue
        buf.write(f"  open fds={len(fds)} {dict(kinds2)}\n")
    except Exception:  # noqa: BLE001
        pass
    # 子进程（僵尸）
    try:
        kids = []
        for pid in os.listdir("/proc"):
            if pid.isdigit():
                with open(f"/proc/{pid}/stat", encoding="utf-8") as f:
                    parts = f.read().rsplit(")", 1)[1].split()
                if int(parts[1]) == os.getpid():
                    kids.append((pid, parts[0]))
        z = [k for k in kids if k[1] == "Z"]
        buf.write(f"  child processes={len(kids)} zombies={len(z)} {z[:10]}\n")
    except Exception:  # noqa: BLE001
        pass


# --------------------------------------------------------------------------- 输出 & 安装
def _emit(line: str) -> None:
    try:
        sys.stderr.write(line + "\n")
        with open(_OUT, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:  # noqa: BLE001
        pass


def dump(reason: str = "manual") -> str:
    buf = io.StringIO()
    buf.write(f"\n######## jiuwen_diag {time.strftime('%F %T')} pid={os.getpid()} reason={reason}\n")
    for fn in (_report_slow, _report_tasks, _report_memory, _report_queues):
        try:
            fn(buf)
        except Exception:  # noqa: BLE001
            buf.write(f"  !! {fn.__name__} failed: {traceback.format_exc(limit=2)}\n")
    # 线程栈（同步阻塞事件循环时，Task 视图看不到，线程栈能看到）
    buf.write("== E. 线程栈（事件循环被同步调用阻塞时看这里）\n")
    names = {th.ident: th.name for th in threading.enumerate()}
    for tid, frame in list(sys._current_frames().items()):  # noqa: SLF001
        try:
            stack = traceback.extract_stack(frame)[-8:]
            buf.write(f"  -- thread {tid} {names.get(tid, '?')}\n")
            for fs in stack:
                buf.write(f"    {fs.filename}:{fs.lineno} {fs.name}\n")
        except Exception as e:  # noqa: BLE001
            buf.write(f"  -- thread {tid}: <{type(e).__name__}>\n")
    text = buf.getvalue()
    _emit(text)
    return text


def _loop_lag_watchdog() -> None:
    """检测事件循环卡顿：向每个已知 loop 投递 call_soon_threadsafe，测量回调延迟。"""
    loops: "weakref.WeakSet[asyncio.AbstractEventLoop]" = weakref.WeakSet()
    while True:
        time.sleep(5)
        try:
            for o in gc.get_objects():
                if isinstance(o, asyncio.AbstractEventLoop) and o.is_running():
                    loops.add(o)
            for lp in list(loops):
                t0 = time.monotonic()
                ev = threading.Event()
                lp.call_soon_threadsafe(ev.set)
                if not ev.wait(_SLOW_MS / 1000):
                    _emit(f"[LOOP-BLOCKED] event loop {id(lp):x} 未在 {_SLOW_MS:.0f}ms 内响应 —— 有同步阻塞调用")
                    dump("loop-blocked")
                elif (lag := (time.monotonic() - t0) * 1000) > 500:
                    _emit(f"[LOOP-LAG] {lag:.0f}ms")
        except Exception:  # noqa: BLE001
            continue


_installed = False


def install() -> None:
    global _installed
    if _installed:
        return
    _installed = True
    if os.getenv("JIUWEN_DIAG_TRACEMALLOC", "1") == "1" and not tracemalloc.is_tracing():
        tracemalloc.start(int(os.getenv("JIUWEN_DIAG_TRACEMALLOC_FRAMES", "1")))
    patched = _patch_all()
    _emit(f"[jiuwen_diag] installed pid={os.getpid()} patched={patched} out={_OUT}")
    if hasattr(signal, "SIGUSR2") and threading.current_thread() is threading.main_thread():
        try:
            signal.signal(signal.SIGUSR2, lambda *_: threading.Thread(target=dump, args=("SIGUSR2",), daemon=True).start())
        except Exception:  # noqa: BLE001
            pass
    if _INTERVAL > 0:
        def _periodic():
            while True:
                time.sleep(_INTERVAL)
                dump("periodic")
        threading.Thread(target=_periodic, name="jiuwen-diag", daemon=True).start()
    threading.Thread(target=_loop_lag_watchdog, name="jiuwen-diag-lag", daemon=True).start()


def uninstall() -> None:
    for mod, cls, meth, _ in _PATCH_TARGETS:
        try:
            c = getattr(importlib.import_module(mod), cls)
            f = getattr(c, meth)
            if getattr(f, "__jiuwen_diag__", False):
                setattr(c, meth, f.__wrapped__)
        except Exception:  # noqa: BLE001
            continue


if os.getenv("JIUWEN_DIAG") == "1":
    install()
