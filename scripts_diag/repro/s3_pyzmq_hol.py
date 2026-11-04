"""s3 — pyzmq P2P head-of-line blocking / silent loss / duplicate execution.

Uses the REAL ``openjiuwen.agent_teams.messager.pyzmq_backend.PyZmqMessager`` over real
localhost ZMQ sockets (ROUTER/DEALER), two instances: a sender and a receiver.

Phases (all identical code in baseline and patched trees; nothing branches on REPRO_MODE):

  A2. Same as A but from a sender without node_id (random routing id per send), isolating
     pure head-of-line blocking from the routing-id collision observed in A.
  A. HOL: one "slow" message (handler sleeps SLOW_S) is sent, then N_FAST "fast" messages are
     sent concurrently right after it (same sender instance, same node_id — the realistic
     leader->teammate shape). Per message we record:
       * ack latency     = send() call start -> send() returns (ACK received)
       * handler latency = send() call start -> receiver handler entered
     plus the number of sender-side ACK timeouts. The ACK timeout is the real config field
     ``MessagerTransportConfig.request_timeout`` (default 10.0s in base.py); we set it to
     ACK_TIMEOUT_S on the sender to keep runtime short (no monkeypatching needed).
  B. Duplicate execution: the public API has no transport-level retry, so a caller-side retry
     is simulated by calling ``send()`` a second time with the *same* EventMessage
     (payload.message_id identical, as a retry after an ACK timeout would). We count how many
     times the handler executes per message id.
  C. Silent loss: the handler raises for K_FAIL messages. We count sends that returned an ACK
     (no exception at the sender) while the handler failed, and how many warning/error/
     exception log calls the backend emitted for them. Log capture: the module-level
     ``team_logger`` reference inside pyzmq_backend is wrapped by a recording proxy that
     forwards every call to the real logger (same wrapper in both modes).
  D. recv-loop error path (F2): a ROUTER recv error is injected by making the receiver's
     ``recv_multipart`` raise for a short window; we count loop iterations (CPU spin) and
     whether the backend logged anything.
"""
from __future__ import annotations

import asyncio
import socket
import statistics
import time

NAME = "pyzmq P2P head-of-line blocking, silent loss, duplicate execution (F1/F2)"

SLOW_S = 2.0
N_FAST = 50
ACK_TIMEOUT_S = 1.0
N_DUP_IDS = 5
K_FAIL = 5


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _pct(vals: list[float], p: float) -> float | None:
    if not vals:
        return None
    vals = sorted(vals)
    k = max(0, min(len(vals) - 1, int(round(p / 100.0 * (len(vals) - 1)))))
    return round(vals[k], 2)


class _LogRecorder:
    """Proxy for the module-level team_logger: records warning/error/exception, forwards all."""

    def __init__(self, real):
        self._real = real
        self.records: list[tuple[str, str]] = []

    def __getattr__(self, name):
        attr = getattr(self._real, name)
        if name in ("warning", "error", "exception", "critical"):
            def wrapped(msg, *a, **kw):
                self.records.append((name, str(msg)))
                try:
                    return attr(msg, *a, **kw)
                except Exception:  # never let logging break the scenario
                    return None
            return wrapped
        return attr


async def run() -> dict:
    from openjiuwen.agent_teams.messager import pyzmq_backend as pzb
    from openjiuwen.agent_teams.messager.base import MessagerPeerConfig, MessagerTransportConfig
    from openjiuwen.agent_teams.schema.events import EventMessage

    rec = _LogRecorder(pzb.team_logger)
    pzb.team_logger = rec

    r_addr = f"tcp://127.0.0.1:{_free_port()}"
    recv_cfg = MessagerTransportConfig(
        backend="pyzmq", node_id="recv", direct_addr=r_addr,
        pubsub_publish_addr=f"tcp://127.0.0.1:{_free_port()}",
        pubsub_subscribe_addr=f"tcp://127.0.0.1:{_free_port()}",
    )
    send_cfg = MessagerTransportConfig(
        backend="pyzmq", node_id="sender",
        pubsub_publish_addr=f"tcp://127.0.0.1:{_free_port()}",
        pubsub_subscribe_addr=f"tcp://127.0.0.1:{_free_port()}",
        known_peers=[MessagerPeerConfig(agent_id="recv", addrs=[r_addr])],
        request_timeout=ACK_TIMEOUT_S,
    )
    # Second sender WITHOUT node_id: the real send() then uses a random DEALER routing id per call,
    # which isolates pure head-of-line blocking from the routing-id collision seen with sender #1.
    send_cfg2 = send_cfg.model_copy(update={
        "node_id": None,
        "pubsub_publish_addr": f"tcp://127.0.0.1:{_free_port()}",
        "pubsub_subscribe_addr": f"tcp://127.0.0.1:{_free_port()}",
    })
    receiver = pzb.PyZmqMessager(recv_cfg)
    sender = pzb.PyZmqMessager(send_cfg)
    sender2 = pzb.PyZmqMessager(send_cfg2)
    await receiver.start()
    await sender.start()
    await sender2.start()

    handler_start: dict[str, float] = {}
    exec_count: dict[str, int] = {}
    failed_ids: set[str] = set()

    async def handler(msg: EventMessage) -> None:
        p = msg.payload
        mid = p["message_id"]
        handler_start.setdefault(mid, time.monotonic())
        exec_count[mid] = exec_count.get(mid, 0) + 1
        kind = p.get("kind")
        if kind == "slow":
            await asyncio.sleep(SLOW_S)
        elif kind == "dup":
            await asyncio.sleep(0.05)
        elif kind == "fail":
            failed_ids.add(mid)
            raise RuntimeError(f"handler failure for {mid}")

    await receiver.register_direct_message_handler(handler)
    await asyncio.sleep(0.2)

    def mk(mid: str, kind: str) -> EventMessage:
        return EventMessage(event_type="message", payload={"message_id": mid, "kind": kind})

    async def timed_send(mid: str, kind: str, via=None) -> dict:
        t0 = time.monotonic()
        try:
            await (via or sender).send("recv", mk(mid, kind))
            return {"mid": mid, "t0": t0, "ack_ms": (time.monotonic() - t0) * 1000, "err": None}
        except asyncio.TimeoutError:
            return {"mid": mid, "t0": t0, "ack_ms": None, "err": "timeout"}
        except Exception as e:  # noqa: BLE001
            return {"mid": mid, "t0": t0, "ack_ms": None, "err": type(e).__name__}

    notes: list[str] = []
    t_phase = time.monotonic()

    # ---------------- Phase A: HOL ----------------
    slow_task = asyncio.create_task(timed_send("slow-0", "slow"))
    await asyncio.sleep(0.05)
    fast = await asyncio.gather(*[timed_send(f"fast-{i}", "fast") for i in range(N_FAST)])
    slow = await slow_task
    await asyncio.sleep(SLOW_S + 0.5)  # let a serial receiver drain its backlog
    fast_ack = [r["ack_ms"] for r in fast if r["ack_ms"] is not None]
    fast_h = [(handler_start[r["mid"]] - r["t0"]) * 1000 for r in fast if r["mid"] in handler_start]
    a_timeouts = sum(1 for r in fast + [slow] if r["err"] == "timeout")
    a_other_err = sum(1 for r in fast + [slow] if r["err"] not in (None, "timeout"))
    # handled-but-unacked: receiver ran the handler but sender saw a timeout => caller will retry => dup
    handled_unacked = sum(1 for r in fast + [slow] if r["err"] == "timeout" and r["mid"] in handler_start)
    phase_a_s = time.monotonic() - t_phase

    # ---------------- Phase A2: pure HOL (unique routing id per send) ----------------
    slow2_task = asyncio.create_task(timed_send("slow2-0", "slow", sender2))
    await asyncio.sleep(0.05)
    fast2 = await asyncio.gather(*[timed_send(f"fast2-{i}", "fast", sender2) for i in range(N_FAST)])
    slow2 = await slow2_task
    await asyncio.sleep(SLOW_S + 0.5)
    fast2_ack = [r["ack_ms"] for r in fast2 if r["ack_ms"] is not None]
    fast2_h = [(handler_start[r["mid"]] - r["t0"]) * 1000 for r in fast2 if r["mid"] in handler_start]
    a2_timeouts = sum(1 for r in fast2 + [slow2] if r["err"] == "timeout")
    a2_handled_unacked = sum(1 for r in fast2 + [slow2] if r["err"] == "timeout" and r["mid"] in handler_start)

    # ---------------- Phase B: duplicate execution on retry ----------------
    for i in range(N_DUP_IDS):
        mid = f"dup-{i}"
        await timed_send(mid, "dup")
        await timed_send(mid, "dup")  # caller retry of the same message id
    await asyncio.sleep(0.3)
    dup_execs = sum(exec_count.get(f"dup-{i}", 0) for i in range(N_DUP_IDS))

    # ---------------- Phase C: silent loss ----------------
    log_before = len(rec.records)
    fail_res = [await timed_send(f"fail-{i}", "fail") for i in range(K_FAIL)]
    await asyncio.sleep(0.3)
    fail_logs = [r for r in rec.records[log_before:] if r[0] in ("error", "exception", "warning")]
    acked_but_failed = sum(1 for r in fail_res if r["err"] is None and r["mid"] in failed_ids)
    silent = acked_but_failed if not fail_logs else max(0, acked_but_failed - len(fail_logs))

    # ---------------- Phase D: recv-loop error path ----------------
    router = receiver._p2p._router
    real_recv = router.recv_multipart
    spins = {"n": 0}
    window_end = time.monotonic() + 0.3

    async def bad_recv(*a, **kw):
        if time.monotonic() < window_end:
            spins["n"] += 1
            raise RuntimeError("injected recv failure")
        return await real_recv(*a, **kw)

    log_before_d = len(rec.records)
    router.recv_multipart = bad_recv
    # wake the loop: it is currently awaiting real_recv; send one message so it iterates again
    await timed_send("wake-d", "fast")
    await asyncio.sleep(0.45)
    d_logs = len(rec.records) - log_before_d
    router.recv_multipart = real_recv
    recovered = await timed_send("after-d", "fast")

    await sender.stop()
    await sender2.stop()
    await receiver.stop()

    metrics = {
        "fast_ack_p50_ms": _pct(fast_ack, 50),
        "fast_ack_p99_ms": _pct(fast_ack, 99),
        "fast_handler_start_p50_ms": _pct(fast_h, 50),
        "fast_handler_start_p99_ms": _pct(fast_h, 99),
        "slow_ack_ms": round(slow["ack_ms"], 1) if slow["ack_ms"] is not None else "timeout",
        "ack_timeouts_of_51": a_timeouts,
        "other_send_errors": a_other_err,
        "handled_but_unacked(=retry->dup)": handled_unacked,
        "fast_handled": len(fast_h),
        "A2_fast_ack_p50_ms": _pct(fast2_ack, 50),
        "A2_fast_ack_p99_ms": _pct(fast2_ack, 99),
        "A2_fast_handler_start_p50_ms": _pct(fast2_h, 50),
        "A2_fast_handler_start_p99_ms": _pct(fast2_h, 99),
        "A2_ack_timeouts_of_51": a2_timeouts,
        "A2_handled_but_unacked": a2_handled_unacked,
        "A2_fast_handled": len(fast2_h),
        "dup_ids_sent_twice": N_DUP_IDS,
        "dup_handler_executions": dup_execs,
        "fail_msgs": K_FAIL,
        "acked_but_handler_failed": acked_but_failed,
        "handler_failure_log_calls": len(fail_logs),
        "silent_loss(acked,failed,no log)": silent,
        "recv_err_loop_spins_300ms": spins["n"],
        "recv_err_log_calls": d_logs,
        "recv_loop_recovered": recovered["err"] is None,
        "phase_a_s": round(phase_a_s, 2),
    }
    notes += [
        f"ACK timeout = MessagerTransportConfig.request_timeout={ACK_TIMEOUT_S}s on the sender "
        f"(real config field, default 10.0s); slow handler sleeps {SLOW_S}s; {N_FAST} fast msgs sent "
        "concurrently 50ms after the slow one from one sender instance (node_id='sender').",
        "A = sender with node_id='sender' (every DEALER shares that routing id); A2 = sender with "
        "node_id=None (real code then picks a random routing id per send) -> pure HOL. Latency "
        "percentiles exclude timed-out sends (see *_ack_timeouts).",
        "Retry simulation: send() called twice with identical EventMessage (same payload.message_id); "
        "no transport-level retry exists in the API.",
        "Log capture: pyzmq_backend.team_logger wrapped by a forwarding recorder (identical in both modes).",
        "Phase D: receiver ROUTER.recv_multipart patched to raise for 300ms; spins = loop iterations in that window.",
    ]
    return {"metrics": metrics, "notes": notes}
