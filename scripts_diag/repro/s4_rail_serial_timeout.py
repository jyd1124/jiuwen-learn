"""S4 — registered callback timeout ignored by serial AsyncCallbackFramework.trigger().

Real class under test: openjiuwen.core.runner.callback.framework.AsyncCallbackFramework.
Rails/agent callbacks are dispatched through ``trigger()`` (serial). The
``timeout=`` given at registration is only applied by ``trigger_parallel``.

Probes (all observe outcomes; an outer observation cap stands in for "forever"):
  A. hanging hook (sleep 1e6) registered with timeout=0.5, followed by a normal
     hook: wall time until trigger() returns (cap 5s), did the next hook run.
  B. slow-but-finite hook (sleep 2s) with timeout=0.2, x20 sequential trials:
     p50/p99 latency of trigger().
  C. fail-close: if register() accepts ``fail_close`` (feature detected via the
     real signature), a timed-out fail_close hook must abort trigger() with
     TimeoutError and stop the chain.
  D. control: a fast hook with timeout=0.5 still returns its result.
"""
from __future__ import annotations

import asyncio
import inspect
import logging
import statistics
import time

NAME = "S4 serial trigger() ignores callback timeout (hung rail blocks chain)"

CAP_S = 5.0
HANG_TIMEOUT_S = 0.5
SLOW_HOOK_S = 2.0
SLOW_TIMEOUT_S = 0.2
TRIALS = 20


def _fw():
    from openjiuwen.core.runner.callback.framework import AsyncCallbackFramework

    logging.getLogger("openjiuwen.core.runner.callback.framework").setLevel(logging.CRITICAL)
    return AsyncCallbackFramework(enable_metrics=True, enable_logging=False)


async def _observe(coro):
    """Run trigger() under an outer cap; return (elapsed, hung, result, exc)."""
    t0 = time.perf_counter()
    task = asyncio.ensure_future(coro)
    done, _ = await asyncio.wait({task}, timeout=CAP_S)
    elapsed = time.perf_counter() - t0
    if not done:
        task.cancel()
        try:
            await task
        except BaseException:  # noqa: BLE001 - cancelled on purpose
            pass
        return elapsed, True, None, None
    exc = task.exception()
    return elapsed, False, (None if exc else task.result()), exc


async def _probe_hang():
    fw = _fw()
    ran = []

    async def hanging_rail(*a, **k):
        await asyncio.sleep(1e6)

    async def next_rail(*a, **k):
        ran.append(True)
        return "next-ok"

    await fw.register("before_model_call", hanging_rail, priority=10, timeout=HANG_TIMEOUT_S)
    await fw.register("before_model_call", next_rail, priority=0)
    elapsed, hung, result, exc = await _observe(fw.trigger("before_model_call", ctx={}))
    return elapsed, hung, bool(ran), result, exc


async def _probe_slow():
    lat = []
    next_ran = 0
    for _ in range(TRIALS):
        fw = _fw()
        ran = []

        async def slow_rail(*a, **k):
            await asyncio.sleep(SLOW_HOOK_S)
            return "slow-done"

        async def next_rail(*a, **k):
            ran.append(True)

        await fw.register("after_tool_call", slow_rail, priority=10, timeout=SLOW_TIMEOUT_S)
        await fw.register("after_tool_call", next_rail, priority=0)
        elapsed, _hung, _r, _e = await _observe(fw.trigger("after_tool_call", ctx={}))
        lat.append(elapsed)
        next_ran += bool(ran)
    lat.sort()
    p99 = lat[min(len(lat) - 1, int(round(0.99 * (len(lat) - 1))))]
    return statistics.median(lat), p99, next_ran


async def _probe_fail_close():
    fw = _fw()
    if "fail_close" not in inspect.signature(fw.register).parameters:
        return False, None, None, None
    ran = []

    async def guard_rail(*a, **k):
        await asyncio.sleep(1e6)

    async def next_rail(*a, **k):
        ran.append(True)

    await fw.register("before_tool_call", guard_rail, priority=10, timeout=HANG_TIMEOUT_S, fail_close=True)
    await fw.register("before_tool_call", next_rail, priority=0)
    elapsed, hung, _r, exc = await _observe(fw.trigger("before_tool_call", ctx={}))
    return True, (type(exc).__name__ if exc else ("hung" if hung else "none")), bool(ran), elapsed


async def _probe_fast_control():
    fw = _fw()

    async def fast_rail(*a, **k):
        await asyncio.sleep(0.01)
        return 42

    await fw.register("evt", fast_rail, timeout=HANG_TIMEOUT_S)
    _e, hung, result, exc = await _observe(fw.trigger("evt"))
    return (not hung) and exc is None and result == [42]


async def _warmup():
    """First trigger() pays a one-time lazy import (_inject_session_if_needed); keep it out of the probes."""
    fw = _fw()

    async def noop(*a, **k):
        return None

    await fw.register("warmup", noop)
    t0 = time.perf_counter()
    await fw.trigger("warmup")
    return time.perf_counter() - t0


async def run() -> dict:
    warm = await _warmup()
    h_elapsed, h_hung, h_next, h_result, h_exc = await _probe_hang()
    p50, p99, slow_next = await _probe_slow()
    fc_supported, fc_outcome, fc_next_ran, fc_elapsed = await _probe_fail_close()
    fast_ok = await _probe_fast_control()
    metrics = {
        "hang_trigger_wall_s": round(h_elapsed, 3),
        "hang_hung_ge_cap": h_hung,
        "hang_cap_s": CAP_S,
        "hang_registered_timeout_s": HANG_TIMEOUT_S,
        "hang_next_callback_ran": h_next,
        "slow_hook_s": SLOW_HOOK_S,
        "slow_registered_timeout_s": SLOW_TIMEOUT_S,
        "slow_trials": TRIALS,
        "slow_p50_s": round(p50, 3),
        "slow_p99_s": round(p99, 3),
        "slow_next_callback_ran": f"{slow_next}/{TRIALS}",
        "fail_close_supported": fc_supported,
        "fail_close_outcome": fc_outcome or "-",
        "fail_close_next_ran": "-" if fc_next_ran is None else fc_next_ran,
        "fast_hook_control_ok": fast_ok,
        "first_trigger_warmup_s": round(warm, 3),
    }
    notes = [
        "real AsyncCallbackFramework.trigger(); hooks are plain asyncio.sleep coroutines",
        f"hang probe: trigger returned={not h_hung}, result={h_result}, exc={type(h_exc).__name__ if h_exc else None}",
    ]
    if fc_elapsed is not None:
        notes.append(f"fail_close probe wall={fc_elapsed:.3f}s")
    return {"metrics": metrics, "notes": notes}
