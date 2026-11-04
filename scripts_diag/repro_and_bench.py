"""repro_and_bench —— 故障复现与微基准测试套件（零外部依赖：无 LLM、无网络服务）。

工作方式（保证 "修复后" 数据来自真实补丁，而不是开关模拟）：
  1. baseline：直接使用当前仓库源码（agent-core/ 与 jiuwenswarm/）。
  2. patched ：用 `git archive HEAD` 把 agent-core/ 与 jiuwenswarm/ 导出到临时目录，
               再 `git apply patches/*.patch`，得到打过补丁的代码树（不触碰主工作树）。
  3. 每个场景在独立子进程中运行（PYTHONPATH 指向对应代码树），输出一行 JSON 指标。
  4. 汇总成对比表，写入 scripts_diag/results/bench_results.{json,md}。

场景模块约定（scripts_diag/repro/sN_*.py）：
  NAME: str                      —— 场景标题
  async def run() -> dict        —— 返回 {"metrics": {k: v}, "notes": [str, ...]}
  场景代码只能通过导入真实类来观测行为，不得读取 REPRO_MODE 来切换逻辑
  （REPRO_MODE 仅用于在输出里标注）。

用法：
  python3 scripts_diag/repro_and_bench.py                 # baseline + patched 全部场景
  python3 scripts_diag/repro_and_bench.py --only s3 s5    # 仅运行部分场景
  python3 scripts_diag/repro_and_bench.py --mode baseline # 只跑 baseline
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parent
SCEN_DIR = HERE / "repro"
PATCH_DIR = REPO / "patches"
RESULTS = HERE / "results"
CODE_DIRS = ("agent-core", "jiuwenswarm")
SCENARIO_TIMEOUT_S = int(os.getenv("REPRO_SCENARIO_TIMEOUT", "600"))


def build_patched_tree(dest: Path) -> list[str]:
    """Export HEAD's code dirs into dest and apply every patch in order."""
    dest.mkdir(parents=True, exist_ok=True)
    arch = subprocess.run(["git", "-C", str(REPO), "archive", "HEAD", *CODE_DIRS],
                          check=True, capture_output=True).stdout
    subprocess.run(["tar", "-x", "-C", str(dest)], input=arch, check=True)
    applied = []
    for p in sorted(glob.glob(str(PATCH_DIR / "*.patch"))):
        r = subprocess.run(["git", "apply", "--whitespace=nowarn", p], cwd=dest,
                           capture_output=True, text=True)
        if r.returncode != 0:
            raise SystemExit(f"patch failed to apply: {p}\n{r.stderr}")
        applied.append(Path(p).name)
    return applied


def run_scenario(mod_file: Path, code_root: Path, mode: str) -> dict:
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join([str(code_root / d) for d in CODE_DIRS] + [str(HERE)])
    env["REPRO_MODE"] = mode
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    env.setdefault("LOG_LEVEL", "ERROR")
    code = (
        "import asyncio, json, importlib.util, sys, io, contextlib\n"
        f"spec = importlib.util.spec_from_file_location('scen', {str(mod_file)!r})\n"
        "m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)\n"
        "res = asyncio.run(m.run())\n"
        "print('@@RESULT@@' + json.dumps({'name': m.NAME, **res}, ensure_ascii=False, default=str))\n"
    )
    t0 = time.monotonic()
    try:
        p = subprocess.run([sys.executable, "-c", code], env=env, cwd=str(code_root),
                           capture_output=True, text=True, timeout=SCENARIO_TIMEOUT_S)
    except subprocess.TimeoutExpired:
        return {"name": mod_file.stem, "error": f"timeout>{SCENARIO_TIMEOUT_S}s"}
    for line in p.stdout.splitlines():
        if line.startswith("@@RESULT@@"):
            out = json.loads(line[len("@@RESULT@@"):])
            out["wall_s"] = round(time.monotonic() - t0, 2)
            return out
    return {"name": mod_file.stem, "error": (p.stderr or p.stdout)[-3000:]}


def fmt(v) -> str:
    if isinstance(v, float):
        return f"{v:.3f}" if abs(v) < 100 else f"{v:.1f}"
    return str(v)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["baseline", "patched", "both"], default="both")
    ap.add_argument("--only", nargs="*", default=None, help="scenario prefixes, e.g. s1 s3")
    args = ap.parse_args()

    scen = sorted(SCEN_DIR.glob("s[0-9]*_*.py"))
    if args.only:
        scen = [s for s in scen if any(s.stem.startswith(o) for o in args.only)]
    if not scen:
        print("no scenarios found", file=sys.stderr)
        return 2

    roots: dict[str, Path] = {}
    tmp = None
    applied: list[str] = []
    if args.mode in ("baseline", "both"):
        roots["baseline"] = REPO
    if args.mode in ("patched", "both"):
        tmp = Path(tempfile.mkdtemp(prefix="jiuwen_patched_"))
        applied = build_patched_tree(tmp)
        roots["patched"] = tmp

    results: dict[str, dict[str, dict]] = {}
    try:
        for s in scen:
            results[s.stem] = {}
            for mode, root in roots.items():
                print(f"[run] {s.stem} ({mode}) ...", file=sys.stderr, flush=True)
                results[s.stem][mode] = run_scenario(s, root, mode)
    finally:
        if tmp is not None:
            shutil.rmtree(tmp, ignore_errors=True)

    RESULTS.mkdir(exist_ok=True)
    meta = {
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "python": sys.version.split()[0],
        "platform": sys.platform,
        "cpu_count": os.cpu_count(),
        "git_head": subprocess.run(["git", "-C", str(REPO), "rev-parse", "--short", "HEAD"],
                                   capture_output=True, text=True).stdout.strip(),
        "patches_applied": applied,
    }
    (RESULTS / "bench_results.json").write_text(
        json.dumps({"meta": meta, "results": results}, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = [f"# repro_and_bench results\n",
             f"- generated: {meta['generated_at']}  python {meta['python']}  cpus {meta['cpu_count']}  HEAD {meta['git_head']}",
             f"- patches applied (patched mode): {', '.join(applied) or '-'}\n"]
    for stem, by_mode in results.items():
        name = next((r.get("name") for r in by_mode.values() if r.get("name")), stem)
        lines.append(f"## {stem} — {name}\n")
        keys: list[str] = []
        for r in by_mode.values():
            for k in (r.get("metrics") or {}):
                if k not in keys:
                    keys.append(k)
        modes = list(by_mode)
        lines.append("| metric | " + " | ".join(modes) + " |")
        lines.append("|---|" + "---|" * len(modes))
        for k in keys:
            lines.append(f"| {k} | " + " | ".join(fmt((by_mode[m].get("metrics") or {}).get(k, "-")) for m in modes) + " |")
        for m in modes:
            r = by_mode[m]
            if r.get("error"):
                lines.append(f"\n**{m} ERROR**:\n```\n{r['error']}\n```")
            for n in r.get("notes") or []:
                lines.append(f"- ({m}) {n}")
        lines.append("")
    md = "\n".join(lines)
    (RESULTS / "bench_results.md").write_text(md, encoding="utf-8")
    print(md)
    return 1 if any(r.get("error") for bm in results.values() for r in bm.values()) else 0


if __name__ == "__main__":
    sys.exit(main())
