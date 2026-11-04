"""S1 — mailbox fallback for tool-approval results is dropped (runbook #1/#2, symptom 1).

Path under test (real code):
  MessageHandler.on_poll_mailbox -> _process_unread_messages
    -> (pending interrupt) _try_parse_approval_payload(msg)
    -> message_manager.mark_message_read(...)
    -> host.resume_interrupt(<approval input>)
  TeamAgent.resume_interrupt (real, unbound)  -> StreamController.is_valid_interrupt_resume (real, unbound)
    -> TeamHarness.is_pending_interrupt_resume_valid (real, unbound)
       -> match_answers / pending_interrupts over a real ToolInterruptionState.

Fakes (collaborators only): mailbox DAO (in-memory), the harness' session holder
and harness.send (which stands in for the DeepAgent resume round: on receipt of
an input it pops the interruption state, i.e. "the interrupt was resumed").

For N independent trials: a teammate has one pending tool interrupt on
call_<i>; the leader's approve_tool() fallback JSON row (same shape as
tools/team.py approve_tool) is the only unread message; one mailbox poll runs.
Outcomes are observed, not assumed.
"""
from __future__ import annotations

import json
import time
import types

NAME = "S1 approval mailbox fallback: dict passed to resume_interrupt"

N = 1000
MEMBER = "dev-1"


class _Msg:
    def __init__(self, message_id, content, ts):
        self.message_id = message_id
        self.content = content
        self.protocol = "json"
        self.timestamp = ts
        self.from_member_name = "leader-1"
        self.to_member_name = MEMBER
        self.broadcast = False


class _Mailbox:
    """In-memory stand-in for TeamMessageManager (only what the handler calls)."""

    def __init__(self, msgs):
        self.rows = {m.message_id: m for m in msgs}
        self.read: set[str] = set()

    async def get_messages(self, to_member_name=None, unread_only=False):
        return [m for mid, m in self.rows.items() if not (unread_only and mid in self.read)]

    async def get_broadcast_messages(self, member_name=None, unread_only=False):
        return []

    async def mark_message_read(self, message_id, member_name):
        self.read.add(message_id)
        return True

    async def mark_messages_read(self, msgs, member_name):
        for m in msgs:
            self.read.add(m.message_id)
        return len(msgs)


class _Session:
    def __init__(self):
        self._state = {}

    def get_state(self, key):
        return self._state.get(key)


def _make_interruption_state(call_id: str):
    from openjiuwen.core.foundation.llm import AssistantMessage
    from openjiuwen.core.foundation.llm.schema.tool_call import ToolCall
    from openjiuwen.core.single_agent.interrupt.response import InterruptRequest
    from openjiuwen.core.single_agent.interrupt.state import ToolInterruptEntry, ToolInterruptionState

    tc = ToolCall(id=call_id, type="function", name="bash", arguments='{"cmd":"rm -rf build"}')
    req = InterruptRequest(message="approve?", interrupt_id=f"intr_{call_id}",
                           interrupt_kind="confirm", interrupt_owner="permission_rail")
    entry = ToolInterruptEntry(tool_call=tc, interrupt_requests={call_id: req})
    return ToolInterruptionState(ai_message=AssistantMessage(content="", tool_calls=[tc]), iteration=1,
                                 interrupted_tools={call_id: entry})


def _build_host(session, sent: list):
    """A TeamAgent-shaped host whose resume/validation methods are the real ones."""
    from openjiuwen.agent_teams.agent.stream_controller import StreamController
    from openjiuwen.agent_teams.agent.team_agent import TeamAgent
    from openjiuwen.agent_teams.harness.team_harness import TeamHarness
    from openjiuwen.core.single_agent.interrupt.state import INTERRUPTION_KEY

    harness = types.SimpleNamespace()
    harness._interrupt_session = lambda: session
    harness.has_pending_interrupt = types.MethodType(TeamHarness.has_pending_interrupt, harness)
    harness.is_pending_interrupt_resume_valid = types.MethodType(
        TeamHarness.is_pending_interrupt_resume_valid, harness)

    async def send(user_input, **kw):  # stands in for the DeepAgent resume round
        sent.append(user_input)
        session._state.pop(INTERRUPTION_KEY, None)

    harness.send = send

    sc = types.SimpleNamespace(_resources=types.SimpleNamespace(harness=harness))
    sc.is_valid_interrupt_resume = types.MethodType(StreamController.is_valid_interrupt_resume, sc)
    sc.has_pending_interrupt = types.MethodType(StreamController.has_pending_interrupt, sc)

    host = types.SimpleNamespace(_stream_controller=sc, harness=harness)
    host._member_name = lambda: MEMBER
    host.has_pending_interrupt = types.MethodType(TeamAgent.has_pending_interrupt, host)
    host.resume_interrupt = types.MethodType(TeamAgent.resume_interrupt, host)
    host.has_in_flight_round = lambda: False
    delivered = []

    async def deliver_input(content, **kw):
        delivered.append(content)

    host.deliver_input = deliver_input
    host.delivered = delivered
    return host


async def _one_trial(i: int):
    from openjiuwen.agent_teams.agent.coordination.handlers.message import MessageHandler
    from openjiuwen.agent_teams.schema.team import TeamRole
    from openjiuwen.core.single_agent.interrupt.state import INTERRUPTION_KEY

    call_id = f"call_{i}"
    session = _Session()
    session._state[INTERRUPTION_KEY] = _make_interruption_state(call_id)
    sent: list = []
    host = _build_host(session, sent)
    # Same payload approve_tool() writes as the "fallback delivery path".
    payload = json.dumps({"type": "tool_approval_result", "tool_call_id": call_id,
                          "approved": True, "feedback": "", "auto_confirm": False})
    msg = _Msg(f"m{i}", payload, ts=i)
    mailbox = _Mailbox([msg])
    blueprint = types.SimpleNamespace(member_name=MEMBER, role=TeamRole.TEAMMATE)
    infra = types.SimpleNamespace(message_manager=mailbox, team_backend=None)
    handler = MessageHandler(host, blueprint, infra, types.SimpleNamespace())
    assert host.has_pending_interrupt()
    await handler.on_poll_mailbox(None)
    return {
        "read": msg.message_id in mailbox.read,
        "forwarded": len(sent) > 0,
        "forwarded_type": type(sent[0]).__name__ if sent else None,
        "pending_after": host.has_pending_interrupt(),
        "raw_json_delivered_to_llm": len(host.delivered) > 0,
    }


async def run() -> dict:
    t0 = time.perf_counter()
    outs = [await _one_trial(i) for i in range(N)]
    dt = time.perf_counter() - t0
    dropped = sum(1 for o in outs if not o["forwarded"])
    read_dropped = sum(1 for o in outs if o["read"] and not o["forwarded"])
    resumed = sum(1 for o in outs if o["forwarded"] and not o["pending_after"])
    still_pending = sum(1 for o in outs if o["pending_after"])
    types_seen = sorted({o["forwarded_type"] for o in outs if o["forwarded_type"]})
    return {
        "metrics": {
            "trials": N,
            "dropped_count": dropped,
            "drop_rate": dropped / N,
            "marked_read_but_dropped": read_dropped,
            "resumed_success_rate": resumed / N,
            "interrupt_still_pending_count": still_pending,
            "raw_json_fed_to_llm_count": sum(o["raw_json_delivered_to_llm"] for o in outs),
            "us_per_trial": round(dt / N * 1e6, 1),
        },
        "notes": [
            "real MessageHandler + real TeamAgent.resume_interrupt / StreamController / TeamHarness validation; "
            "mailbox and harness.send are in-memory fakes (send pops the interruption state)",
            f"resume input types that reached harness.send: {types_seen or 'none'}",
            "marked_read_but_dropped>0 means the approval row is consumed and will never be retried; "
            "the teammate stays parked on its interrupt (runbook symptom 1)",
        ],
    }
