"""S6 — parallel() budget overrun: entry-gate check vs. post-hoc billing.

Baseline code paths under test (agent-core, workflow engine):

  * engine/budget.py ``BudgetLedger.add``     — ``spent += n`` only once a call returns
  * engine/primitives.py ``_check_budget``    — entry gate ``spent >= total`` -> BudgetExhausted
  * engine/primitives.py ``agent()``          — gate, then ``_call_backend``; nothing is
                                                 held for calls already in flight
  * engine/backends/mock.py ``MockBackend._report/_result`` — bills
    ``len(prompt)//4 + len(text)//4`` into the bound session ledger

Method (no LLM, no network): REAL ``run_workflow`` executes a script that fans
out 16 ``parallel()`` branches; each branch sleeps a random start jitter and then
makes up to 4 sequential ``agent()`` calls. The backend is the REAL
``MockBackend`` (its own ``_result`` token estimator and ``_report`` billing),
subclassed only to add a random per-call latency (5-30 ms) and to observe, at
the instant each call starts, the ledger state. The prompt is sized so the mock
estimator bills exactly 1500 tokens per call; the session ceiling is 10 000.

"Started after logical exhaustion": at call start, ``spent + in_flight * 1500 >=
total`` — i.e. tokens already committed or irrevocably in flight had used up
the ceiling. 20 trials with seeded random latencies/jitter.
"""
from __future__ import annotations

import asyncio
import random
import statistics
import tempfile
from pathlib import Path

NAME = "parallel() budget overrun (16 branches, 1500 tok/call, ceiling 10k)"

TOTAL = 10_000
BRANCHES = 16
CALLS_PER_BRANCH = 4
TRIALS = 20
OUT_TEXT = "完成：分支任务已结束，结果已写入共享上下文，等待汇总。" + "x" * 13  # 40 chars -> 10 tok
PROMPT_CHARS = 1490 * 4  # MockBackend: len(prompt)//4 -> 1490 tok; + 10 tok output = 1500

_SCRIPT = '''
import asyncio
from swarmflow import agent, parallel

META = {"name": "s6_par_budget", "description": "16-way fan-out burning a shared ceiling", "phases": []}

async def run(args):
    async def branch(i):
        await asyncio.sleep(args["jitter"][i])
        for j in range(args["calls"]):
            r = await agent(args["prompt"] + f"{i:02d}-{j:02d}", label=f"b{i}c{j}")
            if r is None:
                break
        return i
    return await parallel([lambda i=i: branch(i) for i in range(args["branches"])])
'''


def _make_backend(rng: random.Random, cost: int):
    from openjiuwen.agent_teams.workflow.engine.backends.mock import MockBackend

    class _LatencyMock(MockBackend):
        """Real MockBackend + random latency; records ledger state at call start."""

        def __init__(self):
            super().__init__(responder=lambda *a: OUT_TEXT)
            self.started = 0
            self.in_flight = 0
            self.max_in_flight = 0
            self.after_exhaust = 0
            self.billed: list[int] = []

        async def run(self, prompt, opts, schema_json, *, call_key=None):
            if self.budget.total is not None and self.budget.spent + self.in_flight * cost >= self.budget.total:
                self.after_exhaust += 1
            self.started += 1
            self.in_flight += 1
            self.max_in_flight = max(self.max_in_flight, self.in_flight)
            try:
                await asyncio.sleep(rng.uniform(0.005, 0.030))
                res = await super().run(prompt, opts, schema_json, call_key=call_key)
                self.billed.append(res.tokens or 0)
                return res
            finally:
                self.in_flight -= 1

    return _LatencyMock()


async def _trial(script: str, seed: int) -> dict:
    from openjiuwen.agent_teams.workflow.engine import run_workflow
    from openjiuwen.agent_teams.workflow.engine.budget import BudgetLedger

    rng = random.Random(seed)
    jitter = [rng.uniform(0.0, 0.010) for _ in range(BRANCHES)]
    prompt = ("预算压测" * PROMPT_CHARS)[: PROMPT_CHARS - 5]
    ledger = BudgetLedger(total=TOTAL)
    be = _make_backend(rng, 1500)
    err = None
    try:
        await run_workflow(script, backend=be, budget=ledger, cap=BRANCHES,
                           args={"jitter": jitter, "calls": CALLS_PER_BRANCH, "branches": BRANCHES,
                                 "prompt": prompt})
    except BaseException as e:  # noqa: BLE001 - BudgetExhausted is a BaseException
        err = type(e).__name__
    # parallel() uses gather(): branches already inside the backend keep running
    # (and keep billing) after the run raised; let them land before measuring.
    for _ in range(200):
        if be.in_flight == 0:
            break
        await asyncio.sleep(0.01)
    await asyncio.sleep(0.05)
    return {
        "err": err, "spent": ledger.spent, "started": be.started, "after_exhaust": be.after_exhaust,
        "max_in_flight": be.max_in_flight, "per_call": sorted(set(be.billed)),
        "reserved_left": getattr(ledger, "reserved", None),
    }


async def run() -> dict:
    from openjiuwen.agent_teams.workflow.engine.budget import BudgetLedger

    with tempfile.TemporaryDirectory(prefix="s6_budget_") as d:
        script = Path(d) / "wf.py"
        script.write_text(_SCRIPT, encoding="utf-8")
        trials = [await _trial(str(script), seed) for seed in range(TRIALS)]

    over = [100.0 * (t["spent"] - TOTAL) / TOTAL for t in trials]
    m = {
        "reserve_api_present": hasattr(BudgetLedger, "reserve"),
        "per_call_tokens(mock estimator)": trials[0]["per_call"],
        "trials": TRIALS,
        "spent_mean": round(statistics.mean(t["spent"] for t in trials), 1),
        "spent_max": max(t["spent"] for t in trials),
        "overrun_pct_mean": round(statistics.mean(over), 2),
        "overrun_pct_max": round(max(over), 2),
        "overrun_pct_min": round(min(over), 2),
        "calls_started_mean": round(statistics.mean(t["started"] for t in trials), 2),
        "calls_started_after_logical_exhaustion_mean": round(statistics.mean(t["after_exhaust"] for t in trials), 2),
        "calls_started_after_logical_exhaustion_max": max(t["after_exhaust"] for t in trials),
        "max_in_flight_max": max(t["max_in_flight"] for t in trials),
        "run_outcome": sorted({t["err"] or "completed" for t in trials}),
        "reserved_leak_after_run": max((t["reserved_left"] or 0) for t in trials),
    }
    notes = [
        f"ceiling {TOTAL}, {BRANCHES} branches x <= {CALLS_PER_BRANCH} calls, latency U(5,30)ms, jitter U(0,10)ms, cap={BRANCHES}",
        f"trial0: {trials[0]}",
        "overrun% = (final session ledger spent - total) / total, measured after in-flight calls landed",
        "budget = run_workflow(budget=BudgetLedger(total)) session ledger; MockBackend bills only the session "
        "ledger, so the per-run (workflow_token_limit) gate is not exercised here",
    ]
    return {"metrics": m, "notes": notes}
