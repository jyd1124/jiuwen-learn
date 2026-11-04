# repro_and_bench results

- generated: 2026-11-04 09:15:24  python 3.13.18  cpus 4  HEAD 78cbd2e
- patches applied (patched mode): 0001-fix-approval-fallback-dict.patch, 0002-fix-rail-serial-timeout.patch, 0003-fix-pyzmq-hol-blocking-dedupe.patch, 0004-fix-session-coordinator-retention.patch, 0005-fix-journal-wal-torn-line.patch, 0006-feat-budget-reservation.patch

## s1_approval_fallback — S1 approval mailbox fallback: dict passed to resume_interrupt

| metric | baseline | patched |
|---|---|---|
| trials | 1000 | 1000 |
| dropped_count | 1000 | 0 |
| drop_rate | 1.000 | 0.000 |
| marked_read_but_dropped | 1000 | 0 |
| resumed_success_rate | 0.000 | 1.000 |
| interrupt_still_pending_count | 1000 | 0 |
| raw_json_fed_to_llm_count | 0 | 0 |
| us_per_trial | 3782.3 | 5826.4 |
- (baseline) real MessageHandler + real TeamAgent.resume_interrupt / StreamController / TeamHarness validation; mailbox and harness.send are in-memory fakes (send pops the interruption state)
- (baseline) resume input types that reached harness.send: none
- (baseline) marked_read_but_dropped>0 means the approval row is consumed and will never be retried; the teammate stays parked on its interrupt (runbook symptom 1)
- (patched) real MessageHandler + real TeamAgent.resume_interrupt / StreamController / TeamHarness validation; mailbox and harness.send are in-memory fakes (send pops the interruption state)
- (patched) resume input types that reached harness.send: ['InteractiveInput']
- (patched) marked_read_but_dropped>0 means the approval row is consumed and will never be retried; the teammate stays parked on its interrupt (runbook symptom 1)

## s2_session_retention — Session coordinator retention after close (2,000 sessions)

| metric | baseline | patched |
|---|---|---|
| sessions_closed | 2000 | 2000 |
| retained_session_records | 2001 | 512 |
| retained_generation_entries | 2001 | 512 |
| closed_snapshots_visible | 2001 | 512 |
| retained_total_kb | 3234.5 | 1705.8 |
| retained_coordinator_py_kb | 373.2 | 252.1 |
| slope_total_kb_per_1000 | 1291.7 | 334.0 |
| slope_coordinator_kb_per_1000 | 158.4 | 62.140 |
| slope_steady_total_kb_per_1000 | 1095.3 | 2.400 |
| slope_steady_coordinator_kb_per_1000 | 146.7 | 0.000 |
| alive_asyncio_tasks_after_close | 0 | 0 |
| scheduler_lanes_after_close | 0 | 0 |
| recent_closed_snapshot_inspectable | True | True |
| regen_generation_monotonic | True | True |
| stale_generation_close_rejected | True | True |
| reregistered_session_usable | True | True |
| loop_wall_s | 1.270 | 0.930 |
- (baseline) shim: real `openjiuwen_protocol` missing -> minimal sys.meta_path stub (empty Enums/placeholder classes); coordinator logic itself is real
- (baseline) shim: lifecycle.guard import failed (ModuleNotFoundError: No module named 'ruamel') -> no-op guard (file-backed durable fence, no in-memory state)
- (baseline) sessions_closed=2000 (+1 warm-up, so record/generation counts include it)
- (baseline) checkpoints: [{'n': 250, 'sessions': 251, 'generations': 251, 'registry_executions': 251, 'total_kb': 774.6, 'coord_kb': 83.0}, {'n': 500, 'sessions': 501, 'generations': 501, 'registry_executions': 501, 'total_kb': 1542.4, 'coord_kb': 166.0}, {'n': 750, 'sessions': 751, 'generations': 751, 'registry_executions': 512, 'total_kb': 1890.9, 'coord_kb': 215.2}, {'n': 1000, 'sessions': 1001, 'generations': 1001, 'registry_executions': 512, 'total_kb': 2149.3, 'coord_kb': 236.7}, {'n': 1250, 'sessions': 1251, 'generations': 1251, 'registry_executions': 512, 'total_kb': 2408.1, 'coord_kb': 258.1}, {'n': 1500, 'sessions': 1501, 'generations': 1501, 'registry_executions': 512, 'total_kb': 2717.5, 'coord_kb': 330.3}, {'n': 1750, 'sessions': 1751, 'generations': 1751, 'registry_executions': 512, 'total_kb': 2976.0, 'coord_kb': 351.8}, {'n': 2000, 'sessions': 2001, 'generations': 2001, 'registry_executions': 512, 'total_kb': 3234.5, 'coord_kb': 373.2}]
- (baseline) execution_registry is already bounded (terminal_capacity=512, ttl=900s) and the work_scheduler pops its lane on close_session; the unbounded maps are the coordinator's _sessions/_generations
- (baseline) background tasks: coordinator/scheduler path leaves 0 tasks after close (lane processor is cancelled+awaited). Fire-and-forget create_task leaks live in server/runtime/agent_adapter/interface.py & code_agent_rail.py (out of scope here)
- (patched) shim: real `openjiuwen_protocol` missing -> minimal sys.meta_path stub (empty Enums/placeholder classes); coordinator logic itself is real
- (patched) shim: lifecycle.guard import failed (ModuleNotFoundError: No module named 'ruamel') -> no-op guard (file-backed durable fence, no in-memory state)
- (patched) sessions_closed=2000 (+1 warm-up, so record/generation counts include it)
- (patched) checkpoints: [{'n': 250, 'sessions': 251, 'generations': 251, 'registry_executions': 251, 'total_kb': 794.2, 'coord_kb': 101.2}, {'n': 500, 'sessions': 501, 'generations': 501, 'registry_executions': 501, 'total_kb': 1580.0, 'coord_kb': 202.3}, {'n': 750, 'sessions': 512, 'generations': 512, 'registry_executions': 512, 'total_kb': 1703.0, 'coord_kb': 252.1}, {'n': 1000, 'sessions': 512, 'generations': 512, 'registry_executions': 512, 'total_kb': 1703.4, 'coord_kb': 252.1}, {'n': 1250, 'sessions': 512, 'generations': 512, 'registry_executions': 512, 'total_kb': 1704.1, 'coord_kb': 252.1}, {'n': 1500, 'sessions': 512, 'generations': 512, 'registry_executions': 512, 'total_kb': 1704.9, 'coord_kb': 252.1}, {'n': 1750, 'sessions': 512, 'generations': 512, 'registry_executions': 512, 'total_kb': 1705.4, 'coord_kb': 252.1}, {'n': 2000, 'sessions': 512, 'generations': 512, 'registry_executions': 512, 'total_kb': 1705.8, 'coord_kb': 252.1}]
- (patched) execution_registry is already bounded (terminal_capacity=512, ttl=900s) and the work_scheduler pops its lane on close_session; the unbounded maps are the coordinator's _sessions/_generations
- (patched) background tasks: coordinator/scheduler path leaves 0 tasks after close (lane processor is cancelled+awaited). Fire-and-forget create_task leaks live in server/runtime/agent_adapter/interface.py & code_agent_rail.py (out of scope here)

## s3_pyzmq_hol — pyzmq P2P head-of-line blocking, silent loss, duplicate execution (F1/F2)

| metric | baseline | patched |
|---|---|---|
| fast_ack_p50_ms | None | 14.930 |
| fast_ack_p99_ms | None | 28.690 |
| fast_handler_start_p50_ms | None | 13.910 |
| fast_handler_start_p99_ms | None | 27.920 |
| slow_ack_ms | timeout | 2.700 |
| ack_timeouts_of_51 | 51 | 0 |
| other_send_errors | 0 | 0 |
| handled_but_unacked(=retry->dup) | 1 | 0 |
| fast_handled | 0 | 50 |
| A2_fast_ack_p50_ms | None | 7.470 |
| A2_fast_ack_p99_ms | None | 9.470 |
| A2_fast_handler_start_p50_ms | 1949.1 | 6.660 |
| A2_fast_handler_start_p99_ms | 1953.2 | 8.790 |
| A2_ack_timeouts_of_51 | 51 | 0 |
| A2_handled_but_unacked | 51 | 0 |
| A2_fast_handled | 50 | 50 |
| dup_ids_sent_twice | 5 | 5 |
| dup_handler_executions | 8 | 5 |
| fail_msgs | 5 | 5 |
| acked_but_handler_failed | 5 | 5 |
| handler_failure_log_calls | 0 | 5 |
| silent_loss(acked,failed,no log) | 5 | 0 |
| recv_err_loop_spins_300ms | 661654 | 3 |
| recv_err_log_calls | 0 | 3 |
| recv_loop_recovered | True | True |
| phase_a_s | 3.580 | 2.580 |
- (baseline) ACK timeout = MessagerTransportConfig.request_timeout=1.0s on the sender (real config field, default 10.0s); slow handler sleeps 2.0s; 50 fast msgs sent concurrently 50ms after the slow one from one sender instance (node_id='sender').
- (baseline) A = sender with node_id='sender' (every DEALER shares that routing id); A2 = sender with node_id=None (real code then picks a random routing id per send) -> pure HOL. Latency percentiles exclude timed-out sends (see *_ack_timeouts).
- (baseline) Retry simulation: send() called twice with identical EventMessage (same payload.message_id); no transport-level retry exists in the API.
- (baseline) Log capture: pyzmq_backend.team_logger wrapped by a forwarding recorder (identical in both modes).
- (baseline) Phase D: receiver ROUTER.recv_multipart patched to raise for 300ms; spins = loop iterations in that window.
- (patched) ACK timeout = MessagerTransportConfig.request_timeout=1.0s on the sender (real config field, default 10.0s); slow handler sleeps 2.0s; 50 fast msgs sent concurrently 50ms after the slow one from one sender instance (node_id='sender').
- (patched) A = sender with node_id='sender' (every DEALER shares that routing id); A2 = sender with node_id=None (real code then picks a random routing id per send) -> pure HOL. Latency percentiles exclude timed-out sends (see *_ack_timeouts).
- (patched) Retry simulation: send() called twice with identical EventMessage (same payload.message_id); no transport-level retry exists in the API.
- (patched) Log capture: pyzmq_backend.team_logger wrapped by a forwarding recorder (identical in both modes).
- (patched) Phase D: receiver ROUTER.recv_multipart patched to raise for 300ms; spins = loop iterations in that window.

## s4_rail_serial_timeout — S4 serial trigger() ignores callback timeout (hung rail blocks chain)

| metric | baseline | patched |
|---|---|---|
| hang_trigger_wall_s | 5.004 | 0.502 |
| hang_hung_ge_cap | True | False |
| hang_cap_s | 5.000 | 5.000 |
| hang_registered_timeout_s | 0.500 | 0.500 |
| hang_next_callback_ran | False | True |
| slow_hook_s | 2.000 | 2.000 |
| slow_registered_timeout_s | 0.200 | 0.200 |
| slow_trials | 20 | 20 |
| slow_p50_s | 2.003 | 0.201 |
| slow_p99_s | 2.004 | 0.202 |
| slow_next_callback_ran | 20/20 | 20/20 |
| fail_close_supported | False | True |
| fail_close_outcome | - | TimeoutError |
| fail_close_next_ran | - | False |
| fast_hook_control_ok | True | True |
| first_trigger_warmup_s | 1.468 | 1.827 |
- (baseline) real AsyncCallbackFramework.trigger(); hooks are plain asyncio.sleep coroutines
- (baseline) hang probe: trigger returned=False, result=None, exc=None
- (patched) real AsyncCallbackFramework.trigger(); hooks are plain asyncio.sleep coroutines
- (patched) hang probe: trigger returned=True, result=['next-ok'], exc=None
- (patched) fail_close probe wall=0.502s

## s5_journal_wal_torn — Journal WAL torn tail (mid-UTF-8 truncation, append-after-torn-line)

| metric | baseline | patched |
|---|---|---|
| seed_run_ms_per_call | 0.726 | 1.261 |
| seed_wal_records | 200 | 200 |
| a_mid_utf8.load_ok | False | True |
| a_mid_utf8.load_exc | UnicodeDecodeError | - |
| a_mid_utf8.records_recovered | 0 | 199 |
| a_mid_utf8.resume1_error | UnicodeDecodeError | - |
| a_mid_utf8.resume1_hit_ratio | 0.000 | 0.995 |
| a_mid_utf8.resume1_reexecuted | 0 | 1 |
| a_mid_utf8.resume2_hit_ratio | 0.000 | 1.000 |
| a_mid_utf8.resume2_reexecuted | 0 | 0 |
| a_mid_utf8.records_after_resume2 | 0 | 200 |
| a_mid_utf8.wal_bad_lines_after_resume1 | 1 | 1 |
| a_mid_utf8.appended_record_lost | n/a (resume1 raised) | False |
| b_torn_then_append.load_ok | True | True |
| b_torn_then_append.load_exc | - | - |
| b_torn_then_append.records_recovered | 199 | 199 |
| b_torn_then_append.resume1_error | - | - |
| b_torn_then_append.resume1_hit_ratio | 0.995 | 0.995 |
| b_torn_then_append.resume1_reexecuted | 1 | 1 |
| b_torn_then_append.resume2_hit_ratio | 0.995 | 1.000 |
| b_torn_then_append.resume2_reexecuted | 1 | 0 |
| b_torn_then_append.records_after_resume2 | 200 | 200 |
| b_torn_then_append.wal_bad_lines_after_resume1 | 1 | 1 |
| b_torn_then_append.appended_record_lost | True | False |
| c_clean.load_ok | True | True |
| c_clean.load_exc | - | - |
| c_clean.records_recovered | 200 | 200 |
| c_clean.resume1_error | - | - |
| c_clean.resume1_hit_ratio | 1.000 | 1.000 |
| c_clean.resume1_reexecuted | 0 | 0 |
| c_clean.resume2_hit_ratio | 1.000 | 1.000 |
| c_clean.resume2_reexecuted | 0 | 0 |
| c_clean.records_after_resume2 | 200 | 200 |
| c_clean.wal_bad_lines_after_resume1 | 0 | 0 |
- (baseline) a_mid_utf8: cut 4 bytes; load=UnicodeDecodeError stats=None; resume1 err=UnicodeDecodeError: 'utf-8' codec can't decode byte 0xe3 in position 96154: unexpected end of data hits=0 miss=0; resume2 err=UnicodeDecodeError: 'utf-8' codec can't decode byte 0xe3 in position 96154: unexpected end of data hits=0 miss=0; wal_after_resume1={'lines': 200, 'bad_lines': 1, 'ends_with_newline': False}
- (baseline) b_torn_then_append: cut 279 bytes; load=ok stats=None; resume1 err=None hits=199 miss=1; resume2 err=None hits=199 miss=1; wal_after_resume1={'lines': 201, 'bad_lines': 1, 'ends_with_newline': True}
- (baseline) c_clean: cut 0 bytes; load=ok stats=None; resume1 err=None hits=200 miss=0; resume2 err=None hits=200 miss=0; wal_after_resume1={'lines': 201, 'bad_lines': 0, 'ends_with_newline': True}
- (baseline) hit ratio = engine CACHE_HIT log lines / 200; records = Journal.load(...).prior call keys
- (patched) a_mid_utf8: cut 4 bytes; load=ok stats={'bad_utf8': 1}; resume1 err=None hits=199 miss=1; resume2 err=None hits=200 miss=0; wal_after_resume1={'lines': 202, 'bad_lines': 1, 'ends_with_newline': True}
- (patched) b_torn_then_append: cut 279 bytes; load=ok stats={'bad_json': 1}; resume1 err=None hits=199 miss=1; resume2 err=None hits=200 miss=0; wal_after_resume1={'lines': 202, 'bad_lines': 1, 'ends_with_newline': True}
- (patched) c_clean: cut 0 bytes; load=ok stats={}; resume1 err=None hits=200 miss=0; resume2 err=None hits=200 miss=0; wal_after_resume1={'lines': 201, 'bad_lines': 0, 'ends_with_newline': True}
- (patched) hit ratio = engine CACHE_HIT log lines / 200; records = Journal.load(...).prior call keys

## s6_parallel_budget_overrun — parallel() budget overrun (16 branches, 1500 tok/call, ceiling 10k)

| metric | baseline | patched |
|---|---|---|
| reserve_api_present | False | True |
| per_call_tokens(mock estimator) | [1500] | [1500] |
| trials | 20 | 20 |
| spent_mean | 33000 | 10500 |
| spent_max | 33000 | 10500 |
| overrun_pct_mean | 230.0 | 5.000 |
| overrun_pct_max | 230.0 | 5.000 |
| overrun_pct_min | 230.0 | 5.000 |
| calls_started_mean | 22 | 7 |
| calls_started_after_logical_exhaustion_mean | 15 | 0 |
| calls_started_after_logical_exhaustion_max | 15 | 0 |
| max_in_flight_max | 16 | 4 |
| run_outcome | ['BudgetExhausted'] | ['BudgetExhausted'] |
| reserved_leak_after_run | 0 | 0 |
- (baseline) ceiling 10000, 16 branches x <= 4 calls, latency U(5,30)ms, jitter U(0,10)ms, cap=16
- (baseline) trial0: {'err': 'BudgetExhausted', 'spent': 33000, 'started': 22, 'after_exhaust': 15, 'max_in_flight': 16, 'per_call': [1500], 'reserved_left': None}
- (baseline) overrun% = (final session ledger spent - total) / total, measured after in-flight calls landed
- (baseline) budget = run_workflow(budget=BudgetLedger(total)) session ledger; MockBackend bills only the session ledger, so the per-run (workflow_token_limit) gate is not exercised here
- (patched) ceiling 10000, 16 branches x <= 4 calls, latency U(5,30)ms, jitter U(0,10)ms, cap=16
- (patched) trial0: {'err': 'BudgetExhausted', 'spent': 10500, 'started': 7, 'after_exhaust': 0, 'max_in_flight': 4, 'per_call': [1500], 'reserved_left': 0}
- (patched) overrun% = (final session ledger spent - total) / total, measured after in-flight calls landed
- (patched) budget = run_workflow(budget=BudgetLedger(total)) session ledger; MockBackend bills only the session ledger, so the per-run (workflow_token_limit) gate is not exercised here
