"""s2 — RuntimeSessionCoordinator retains every closed Session forever (runbook #8, appendix A F4/F5).

Drives the REAL ``jiuwenswarm.runtime.session.coordinator.RuntimeSessionCoordinator``
(with its real ``SessionExecutionRegistry`` and ``SessionWorkScheduler``):

    register_session -> run_unary(CHAT_UNARY, trivial op) -> close_session

for N sessions with distinct ids, and measures what is left behind after close:
``len(_sessions)``, ``len(_generations)``, tracemalloc bytes (total, and the part
allocated from coordinator.py), slope KB / 1000 sessions (least squares over
checkpoints every 250 sessions), and asyncio tasks still alive after close.

It also checks generation semantics that any fix must preserve: re-registering a
closed id yields a strictly higher generation, and a stale close_session with the
old generation does not close the new one.

Environment shims (installed ONLY when the real module is unavailable, reported in notes):
  * ``openjiuwen_protocol`` (external, not installable here): the coordinator's import
    chain reaches it via ``runtime.session.model -> common.schema``.  A minimal
    sys.meta_path stub provides empty Enum classes / placeholder classes / string
    constants.  The coordinator never uses those symbols for this path.
  * ``jiuwenswarm.server.runtime.session.lifecycle.guard`` (file-backed durable fence,
    needs ruamel.yaml) — if it cannot be imported, a no-op ``guard`` is installed.
    It only reads on-disk lifecycle metadata; it has no in-memory retention role.
The scenario does not read REPRO_MODE; the same code runs on baseline and patched trees.
"""
from __future__ import annotations

import asyncio
import enum
import gc
import importlib.abc
import importlib.machinery
import importlib.util
import sys
import time
import tracemalloc
import types

NAME = "Session coordinator retention after close (2,000 sessions)"

N_SESSIONS = 2000
CHECKPOINT = 250

_notes: list[str] = []


# --------------------------------------------------------------------------- shims
class _AutoEnumMeta(enum.EnumMeta):
    def __getattr__(cls, name):  # create members on demand (stub only)
        if name.startswith("_"):
            raise AttributeError(name)
        member = object.__new__(cls)
        member._name_ = name
        member._value_ = name.lower()
        type.__setattr__(cls, name, member)
        cls._member_map_[name] = member
        cls._value2member_map_[member._value_] = member
        cls._member_names_.append(name)
        return member


class _AutoEnum(enum.Enum, metaclass=_AutoEnumMeta):
    pass


_ENUM_SUFFIXES = ("Method", "Type", "Kind", "Mode", "Status", "State", "Channel",
                  "Phase", "Reason", "Code", "Role", "Level")


class _ProtocolStubFinder(importlib.abc.MetaPathFinder, importlib.abc.Loader):
    def find_spec(self, name, path, target=None):
        if name == "openjiuwen_protocol" or name.startswith("openjiuwen_protocol."):
            return importlib.machinery.ModuleSpec(name, self, is_package=True)
        return None

    def create_module(self, spec):
        mod = types.ModuleType(spec.name)
        mod.__path__ = []
        cache: dict[str, object] = {}

        def __getattr__(attr, _c=cache, _n=spec.name):
            if attr.startswith("__"):
                raise AttributeError(attr)
            if attr not in _c:
                if attr.endswith(_ENUM_SUFFIXES):
                    _c[attr] = _AutoEnum(attr, [])
                elif attr.isupper():
                    _c[attr] = f"_stub_{attr.lower()}"
                else:
                    _c[attr] = type(attr, (), {"__module__": _n})
            return _c[attr]

        mod.__getattr__ = __getattr__
        return mod

    def exec_module(self, module):
        pass


def _install_shims() -> None:
    if importlib.util.find_spec("openjiuwen_protocol") is None:
        sys.meta_path.append(_ProtocolStubFinder())
        _notes.append("shim: real `openjiuwen_protocol` missing -> minimal sys.meta_path stub "
                      "(empty Enums/placeholder classes); coordinator logic itself is real")
    try:
        from jiuwenswarm.server.runtime.session.lifecycle import guard  # noqa: F401
    except Exception as exc:  # noqa: BLE001
        stub = types.ModuleType("jiuwenswarm.server.runtime.session.lifecycle")
        stub.guard = lambda session_id="", project_id="": None
        sys.modules[stub.__name__] = stub
        _notes.append(f"shim: lifecycle.guard import failed ({type(exc).__name__}: {exc}) -> "
                      "no-op guard (file-backed durable fence, no in-memory state)")


# --------------------------------------------------------------------------- helpers
def _slope_kb_per_1000(xs: list[float], ys_bytes: list[float]) -> float:
    n = len(xs)
    mx, my = sum(xs) / n, sum(ys_bytes) / n
    den = sum((x - mx) ** 2 for x in xs)
    if den == 0:
        return 0.0
    slope = sum((x - mx) * (y - my) for x, y in zip(xs, ys_bytes)) / den  # bytes / session
    return slope * 1000 / 1024


def _coordinator_bytes(snapshot: tracemalloc.Snapshot, coord_file: str) -> int:
    filt = snapshot.filter_traces([tracemalloc.Filter(True, coord_file)])
    return sum(stat.size for stat in filt.statistics("filename"))


async def _settle() -> None:
    for _ in range(5):
        await asyncio.sleep(0)


# --------------------------------------------------------------------------- scenario
async def run() -> dict:
    _install_shims()
    from jiuwenswarm.runtime.session import coordinator as coord_mod
    from jiuwenswarm.runtime.session import (
        RuntimeSessionCoordinator,
        RuntimeSessionState,
        SessionPersistencePolicy,
        SessionWorkKind,
    )

    coord_file = coord_mod.__file__
    coordinator = RuntimeSessionCoordinator(cancel_timeout=1.0)
    me = asyncio.current_task()
    tasks_before = {t for t in asyncio.all_tasks() if t is not me}

    async def trivial() -> str:
        await asyncio.sleep(0)
        return "ok"

    async def one(i: int) -> None:
        sid = f"s2-session-{i:05d}"
        await coordinator.register_session(sid, "bench", SessionPersistencePolicy.PERSISTENT)
        assert await coordinator.run_unary(sid, f"req-{i}", SessionWorkKind.CHAT_UNARY, trivial) == "ok"
        res = await coordinator.close_session(sid)
        assert res.existed and not res.timed_out, res

    # warm-up (allocator / import caches) outside the measured window
    await one(-1)
    await _settle()
    gc.collect()
    tracemalloc.start()
    base_total = tracemalloc.get_traced_memory()[0]
    base_coord = _coordinator_bytes(tracemalloc.take_snapshot(), coord_file)

    t0 = time.monotonic()
    xs: list[float] = []
    totals: list[float] = []
    coord_bytes: list[float] = []
    checkpoints: list[dict] = []
    for i in range(N_SESSIONS):
        await one(i)
        if (i + 1) % CHECKPOINT == 0:
            await _settle()
            gc.collect()
            total = tracemalloc.get_traced_memory()[0] - base_total
            cb = _coordinator_bytes(tracemalloc.take_snapshot(), coord_file) - base_coord
            xs.append(i + 1)
            totals.append(total)
            coord_bytes.append(cb)
            checkpoints.append({
                "n": i + 1,
                "sessions": len(coordinator._sessions),  # noqa: SLF001
                "generations": len(coordinator._generations),  # noqa: SLF001
                "registry_executions": len(coordinator._registry._executions),  # noqa: SLF001
                "total_kb": round(total / 1024, 1),
                "coord_kb": round(cb / 1024, 1),
            })
    elapsed = time.monotonic() - t0
    await _settle()
    gc.collect()

    alive = [t for t in asyncio.all_tasks() if t is not me and t not in tasks_before]
    lanes = len(coordinator._scheduler._lanes)  # noqa: SLF001
    retained_sessions = len(coordinator._sessions)  # noqa: SLF001  (includes the warm-up session)
    retained_generations = len(coordinator._generations)  # noqa: SLF001
    closed_snapshots = sum(
        1 for s in coordinator.snapshot_sessions() if s.state is RuntimeSessionState.CLOSED
    )
    final_total = totals[-1]
    final_coord = coord_bytes[-1]
    tracemalloc.stop()

    # ---- generation-semantics checks (must hold on both trees) ----
    # (a) recently closed id: snapshot still inspectable, re-register bumps generation
    recent = f"s2-session-{N_SESSIONS - 1:05d}"
    recent_snap = coordinator.snapshot_session(recent)
    recent_inspectable = recent_snap is not None and recent_snap.state is RuntimeSessionState.CLOSED
    # (b) oldest closed id (evicted on a bounded tree): generation must still be monotonic
    oldest = "s2-session-00000"
    old_gen = 1  # each id was registered exactly once in this run
    new = await coordinator.register_session(oldest, "bench", SessionPersistencePolicy.PERSISTENT)
    gen_monotonic = new.generation > old_gen
    stale = await coordinator.close_session(oldest, generation=old_gen)
    cur = coordinator.snapshot_session(oldest)
    stale_close_rejected = (not stale.existed) and cur is not None and cur.generation == new.generation \
        and cur.state is RuntimeSessionState.READY
    reuse_ok = await coordinator.run_unary(oldest, "again", SessionWorkKind.CHAT_UNARY, trivial) == "ok"
    await coordinator.close()

    _notes.append(f"sessions_closed={N_SESSIONS} (+1 warm-up, so record/generation counts include it)")
    _notes.append(f"checkpoints: {checkpoints}")
    _notes.append("execution_registry is already bounded (terminal_capacity=512, ttl=900s) and the "
                  "work_scheduler pops its lane on close_session; the unbounded maps are the "
                  "coordinator's _sessions/_generations")
    if not alive:
        _notes.append("background tasks: coordinator/scheduler path leaves 0 tasks after close (lane "
                      "processor is cancelled+awaited). Fire-and-forget create_task leaks live in "
                      "server/runtime/agent_adapter/interface.py & code_agent_rail.py (out of scope here)")
    return {
        "metrics": {
            "sessions_closed": N_SESSIONS,
            "retained_session_records": retained_sessions,
            "retained_generation_entries": retained_generations,
            "closed_snapshots_visible": closed_snapshots,
            "retained_total_kb": round(final_total / 1024, 1),
            "retained_coordinator_py_kb": round(final_coord / 1024, 1),
            "slope_total_kb_per_1000": round(_slope_kb_per_1000(xs, totals), 2),
            "slope_coordinator_kb_per_1000": round(_slope_kb_per_1000(xs, coord_bytes), 2),
            # steady state: checkpoints >= 1000, after the bounded registry history (512) has filled
            "slope_steady_total_kb_per_1000": round(_slope_kb_per_1000(xs[3:], totals[3:]), 2),
            "slope_steady_coordinator_kb_per_1000": round(
                _slope_kb_per_1000(xs[3:], coord_bytes[3:]), 2),
            "alive_asyncio_tasks_after_close": len(alive),
            "scheduler_lanes_after_close": lanes,
            "recent_closed_snapshot_inspectable": recent_inspectable,
            "regen_generation_monotonic": gen_monotonic,
            "stale_generation_close_rejected": stale_close_rejected,
            "reregistered_session_usable": reuse_ok,
            "loop_wall_s": round(elapsed, 2),
        },
        "notes": _notes,
    }
