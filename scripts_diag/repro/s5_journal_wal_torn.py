"""S5 — Journal WAL torn tail: mid-multibyte truncation + append-after-torn-line.

Runbook #4/#5 / symptom 5 / appendix C: a crash mid WAL-append leaves a torn
last line. Baseline code paths under test (agent-core, workflow engine):

  * journal.py ``Journal._parse_records``  — skips lines that fail json.loads
  * journal.py ``Journal.load``            — ``aiofiles.open(src, "r", encoding="utf-8")``
                                              strict decode of the WHOLE file
  * journal.py ``Journal._append_wal``     — appends ``json + "\\n"`` with no check
                                              that the file ends in "\\n", no fsync
  * engine/runner.py ``run_workflow``      — ``Journal.load(resume, wal_path=...)``

Method (no LLM, no network): a REAL ``run_workflow`` drives a 200-call workflow
script on the REAL ``MockBackend`` (responder returns Chinese text), journaling
every call to a per-run WAL. The run-level seal record is stripped to emulate a
crash before completion. Then three WAL variants are derived and each is resumed
twice with the real engine (same run_id):

  (a) last record truncated in the middle of a 3-byte UTF-8 char
  (b) last record truncated mid-line on an ASCII byte (decodes fine, JSON torn);
      resume #1 re-runs the lost call and the REAL ``_append_wal`` appends after
      the torn bytes; resume #2 shows whether that appended record survived
  (c) clean (control)

cache_hit_ratio = CACHE_HIT log lines / 200 (from the engine's own log_sink).
"""
from __future__ import annotations

import json
import os
import shutil
import tempfile
import time
from pathlib import Path

NAME = "Journal WAL torn tail (mid-UTF-8 truncation, append-after-torn-line)"

N_CALLS = 200
RUN_ID = "run-s5"

_SCRIPT = f'''
from swarmflow import agent

META = {{"name": "s5_wal", "description": "200 cached calls with Chinese content", "phases": []}}

async def run(args):
    out = []
    for i in range({N_CALLS}):
        out.append(await agent(f"第{{i}}号任务：请阅读九问团队的中文日志并给出结论（含标点“引号”）。", label=f"任务{{i}}"))
    return len([o for o in out if o])
'''


def _responder(prompt, opts, schema, rng):
    n = rng.randint(1000, 9999)
    return f"结论{n}：{opts.get('label')} 已完成，日志中未发现异常，建议继续观察——编号{n}。"


async def _run(script: str, wal: str):
    from openjiuwen.agent_teams.workflow.engine import run_workflow
    from openjiuwen.agent_teams.workflow.engine.backends.mock import MockBackend

    logs: list[str] = []
    err = None
    t0 = time.perf_counter()
    try:
        await run_workflow(script, backend=MockBackend(responder=_responder), wal_path=wal,
                           run_id=RUN_ID, log_sink=logs.append)
    except BaseException as e:  # noqa: BLE001 - we report the type
        err = f"{type(e).__name__}: {str(e)[:120]}"
    dt = time.perf_counter() - t0
    hits = sum(1 for l in logs if "CACHE_HIT" in l)
    misses = sum(1 for l in logs if "CACHE_MISS" in l)
    loaded = next((l for l in logs if "journal loaded" in l), "")
    return {"error": err, "hits": hits, "misses": misses, "wall_s": dt, "load_log": loaded}


async def _load(wal: str):
    from openjiuwen.agent_teams.workflow.engine.journal import Journal

    try:
        j = await Journal.load(None, wal_path=wal)
    except BaseException as e:  # noqa: BLE001
        return {"ok": False, "exc": type(e).__name__, "records": 0, "stats": None}
    calls = [k for k in j.prior if not k.startswith('[["__run__"')]
    return {"ok": True, "exc": None, "records": len(calls), "stats": getattr(j, "load_stats", None)}


def _strip_run_records(wal: Path) -> None:
    lines = wal.read_bytes().split(b"\n")
    keep = [l for l in lines if l and b'__run__' not in l[:40]]
    wal.write_bytes(b"\n".join(keep) + b"\n")


def _truncate_mid_multibyte(wal: Path) -> int:
    data = wal.read_bytes().rstrip(b"\n")
    start = data.rfind(b"\n") + 1
    last = data[start:]
    # last lead byte of a 3-byte UTF-8 sequence (0xE0..0xEF) in the last record
    idx = max(i for i, b in enumerate(last) if 0xE0 <= b <= 0xEF)
    cut = start + idx + 1  # keep the lead byte only -> invalid UTF-8 tail
    wal.write_bytes(data[:cut])
    return len(data) - cut


def _truncate_mid_line_ascii(wal: Path) -> int:
    data = wal.read_bytes().rstrip(b"\n")
    start = data.rfind(b"\n") + 1
    last = data[start:]
    pos = last.find(b'"label"')  # cut right before a pure-ASCII key: valid UTF-8, torn JSON
    cut = start + pos
    wal.write_bytes(data[:cut])
    return len(data) - cut


def _wal_tail_info(wal: Path) -> dict:
    b = wal.read_bytes()
    lines = [l for l in b.split(b"\n") if l]
    bad = 0
    for l in lines:
        try:
            json.loads(l.decode("utf-8"))
        except Exception:  # noqa: BLE001
            bad += 1
    return {"lines": len(lines), "bad_lines": bad, "ends_with_newline": b.endswith(b"\n")}


async def run() -> dict:
    tmp = Path(tempfile.mkdtemp(prefix="s5_wal_"))
    notes: list[str] = []
    m: dict = {}
    try:
        script = tmp / "wf.py"
        script.write_text(_SCRIPT, encoding="utf-8")
        base = tmp / "base.wal"
        r0 = await _run(str(script), str(base))
        if r0["error"]:
            raise RuntimeError(f"seed run failed: {r0['error']}")
        _strip_run_records(base)
        m["seed_run_ms_per_call"] = round(r0["wall_s"] * 1000 / N_CALLS, 3)
        m["seed_wal_records"] = _wal_tail_info(base)["lines"]

        for tag, mutate in (("a_mid_utf8", _truncate_mid_multibyte),
                            ("b_torn_then_append", _truncate_mid_line_ascii),
                            ("c_clean", None)):
            wal = tmp / f"{tag}.wal"
            shutil.copy(base, wal)
            cut = mutate(wal) if mutate else 0
            ld = await _load(str(wal))
            r1 = await _run(str(script), str(wal))
            after1 = _wal_tail_info(wal)
            r2 = await _run(str(script), str(wal))
            ld2 = await _load(str(wal))
            m[f"{tag}.load_ok"] = ld["ok"]
            m[f"{tag}.load_exc"] = ld["exc"] or "-"
            m[f"{tag}.records_recovered"] = ld["records"]
            m[f"{tag}.resume1_error"] = (r1["error"] or "-").split(":")[0]
            m[f"{tag}.resume1_hit_ratio"] = round(r1["hits"] / N_CALLS, 3)
            m[f"{tag}.resume1_reexecuted"] = r1["misses"]
            m[f"{tag}.resume2_hit_ratio"] = round(r2["hits"] / N_CALLS, 3)
            m[f"{tag}.resume2_reexecuted"] = r2["misses"]
            m[f"{tag}.records_after_resume2"] = ld2["records"]
            m[f"{tag}.wal_bad_lines_after_resume1"] = after1["bad_lines"]
            if tag != "c_clean":
                # The re-executed call's record appended by resume #1 is "lost"
                # iff resume #2 still has to re-execute it.
                m[f"{tag}.appended_record_lost"] = bool(r1["misses"] > 0 and r2["misses"] > 0) \
                    if not r1["error"] else "n/a (resume1 raised)"
            notes.append(f"{tag}: cut {cut} bytes; load={ld['exc'] or 'ok'} stats={ld['stats']}; "
                         f"resume1 err={r1['error']} hits={r1['hits']} miss={r1['misses']}; "
                         f"resume2 err={r2['error']} hits={r2['hits']} miss={r2['misses']}; "
                         f"wal_after_resume1={after1}")
        notes.append("hit ratio = engine CACHE_HIT log lines / 200; records = Journal.load(...).prior call keys")
        return {"metrics": m, "notes": notes}
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
