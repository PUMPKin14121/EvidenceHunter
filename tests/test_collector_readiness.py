"""NEXT_DATASET_COLLECTOR_READINESS test file. Covers G1 (worker/thread
unexpected-death observability) and the validation logic of
EvidenceHunter_collector_readiness.py (G2 gap-ledger semantics, G3 repair/resync
provenance, G4 soak execution, G5 storage/throughput, supplied-gate shape
validation, and the G8 fold into one conclusion). Everything lives in this
one file per the scope_declaration's tool-consolidation rule -- no separate
test files are created this changeset."""
import json
import threading
import time

import pytest

import EvidenceHunter_collector as collector
import EvidenceHunter_collector_readiness as readiness
import EvidenceHunter_collector_supervisor as sup
import EvidenceHunter_collector_ws as ws_mod
import EvidenceHunter_gap_ledger as gap_ledger


def test_uncaught_exception_in_lifecycle_body_transitions_to_crashed_and_reraises(tmp_path):
    """G1 regression test: if code inside the lifecycle body itself raises
    (a genuine bug -- NOT a socket/on_message error, both of which
    WSConnection._run() already catches internally and turns into
    RECV_ERROR/ON_MESSAGE_HANDLER_ERROR events without ever escaping the
    thread), run_component_lifecycle must not swallow it silently -- it must
    first mark the component STATE_LIFECYCLE_THREAD_CRASHED (alerting +
    persisting via the normal transition() path), emit a matching
    LIFECYCLE_THREAD_CRASHED lifecycle event, and then re-raise so the
    exception is still visible. url_fn() is used as the fault-injection
    point here because it runs directly in the lifecycle thread itself,
    unlike recv()/on_message() which run inside WSConnection's own thread
    and its own defensive try/except."""
    supervisor = sup.Supervisor(state_file=tmp_path / "state.json")
    supervisor.register("aggTrade")

    def exploding_url_fn():
        raise RuntimeError("simulated unexpected bug, not a network error")

    events = []
    caught = {}

    def worker():
        try:
            sup.run_component_lifecycle(
                "aggTrade",
                url_fn=exploding_url_fn,
                route_class="market", stream_name="btcusdt@aggTrade",
                on_message=lambda payload, meta: None,
                supervisor=supervisor,
                stop_event=threading.Event(),  # never set -- only the crash should end this
                on_lifecycle_event=events.append,
            )
        except RuntimeError as error:
            caught["error"] = error

    t = threading.Thread(target=worker, daemon=True)
    t.start()
    t.join(timeout=5.0)

    assert not t.is_alive()
    assert "error" in caught  # original exception re-raised, not swallowed
    assert supervisor.get("aggTrade").state == sup.STATE_LIFECYCLE_THREAD_CRASHED

    crashed_events = [e for e in events if e.get("event_type") == "LIFECYCLE_THREAD_CRASHED"]
    assert len(crashed_events) == 1
    assert "RuntimeError" in crashed_events[0]["detail"]

    persisted = json.loads((tmp_path / "state.json").read_text(encoding="utf-8"))
    assert persisted["components"]["aggTrade"]["state"] == sup.STATE_LIFECYCLE_THREAD_CRASHED


def test_requested_shutdown_is_not_misclassified_as_crash(tmp_path):
    """A controlled stop (stop_event set) must leave the component in a
    normal state (HEALTHY, since it stops cleanly after being healthy) --
    never STATE_LIFECYCLE_THREAD_CRASHED."""
    supervisor = sup.Supervisor(state_file=tmp_path / "state.json")
    supervisor.register("aggTrade")

    class OneShotSocket:
        def __init__(self):
            self.sent = False

        def recv(self):
            if not self.sent:
                self.sent = True
                return json.dumps({"a": 1})
            time.sleep(0.02)
            raise ws_mod.websocket.WebSocketTimeoutException()

        def close(self):
            pass

    stop_event = threading.Event()

    def worker():
        sup.run_component_lifecycle(
            "aggTrade",
            url_fn=lambda: "wss://fstream.binance.com/market/ws/btcusdt@aggTrade",
            route_class="market", stream_name="btcusdt@aggTrade",
            on_message=lambda payload, meta: None,
            supervisor=supervisor,
            stop_event=stop_event,
            silent_stall_seconds=1000.0, stall_check_interval_seconds=0.05,
            connect_fn=lambda: OneShotSocket(),
        )

    t = threading.Thread(target=worker, daemon=True)
    t.start()
    time.sleep(0.3)
    stop_event.set()
    t.join(timeout=5.0)

    assert not t.is_alive()
    assert supervisor.get("aggTrade").state == sup.STATE_HEALTHY  # never CRASHED


def test_lifecycle_thread_crashed_is_terminal(tmp_path):
    supervisor = sup.Supervisor(state_file=tmp_path / "state.json")
    supervisor.register("aggTrade")
    supervisor.transition("aggTrade", sup.STATE_LIFECYCLE_THREAD_CRASHED, reason="TEST_INJECTED")
    with pytest.raises(sup.SupervisorError):
        supervisor.transition("aggTrade", sup.STATE_RECONNECTING, reason="SHOULD_NOT_BE_ALLOWED")


def test_crashed_transition_triggers_alert(tmp_path):
    alerts = []
    supervisor = sup.Supervisor(state_file=tmp_path / "state.json", alert_fn=alerts.append)
    supervisor.register("aggTrade")
    supervisor.transition("aggTrade", sup.STATE_LIFECYCLE_THREAD_CRASHED, reason="TEST_INJECTED")
    assert any(a["new_state"] == sup.STATE_LIFECYCLE_THREAD_CRASHED for a in alerts)


def test_crash_transition_failure_still_reraises_original_exception(tmp_path):
    """Even if the crash-transition call inside the except-handler itself
    fails for some reason, the ORIGINAL exception must still propagate --
    never silently absorbed and never replaced by the crash-transition's own
    failure."""
    real_supervisor = sup.Supervisor(state_file=tmp_path / "state.json")
    real_supervisor.register("aggTrade")

    class PoisonedSupervisor:
        """Forwards everything to the real Supervisor except a transition to
        STATE_LIFECYCLE_THREAD_CRASHED, which is made to fail -- simulating a
        crash-transition that itself cannot succeed."""

        def __getattr__(self, item):
            return getattr(real_supervisor, item)

        def transition(self, name, new_state, *, reason):
            if new_state == sup.STATE_LIFECYCLE_THREAD_CRASHED:
                raise sup.SupervisorError("SIMULATED_CRASH_TRANSITION_FAILURE")
            return real_supervisor.transition(name, new_state, reason=reason)

    def exploding_url_fn():
        raise RuntimeError("simulated unexpected bug")

    caught = {}

    def worker():
        try:
            sup.run_component_lifecycle(
                "aggTrade",
                url_fn=exploding_url_fn,
                route_class="market", stream_name="btcusdt@aggTrade",
                on_message=lambda payload, meta: None,
                supervisor=PoisonedSupervisor(),
                stop_event=threading.Event(),
            )
        except RuntimeError as error:
            caught["error"] = error

    t = threading.Thread(target=worker, daemon=True)
    t.start()
    t.join(timeout=5.0)

    assert not t.is_alive()
    assert "error" in caught
    assert "simulated unexpected bug" in str(caught["error"])


# ===========================================================================
# G2 -- gap ledger semantic validation
# ===========================================================================

def _clean_agg_gap(ledger_path):
    """One aggTrade gap, opened and REST-repaired: the normal happy path."""
    gap_id = gap_ledger.open_gap(
        ledger_path, component="aggTrade", gap_start=101, gap_end=104,
        reason="AGGTRADE_ID_SEQUENCE_GAP", last_good_identifier=100,
        first_good_identifier_after=105, recoverability="RECOVERABLE_VIA_BACKFILL",
    )
    gap_ledger.mark_gap_repaired(ledger_path, gap_id, repair_source="REST_AGGTRADES_FROM_ID")
    return gap_id


def test_g2_clean_ledger_passes(tmp_path):
    ledger = tmp_path / "gap_ledger.jsonl"
    _clean_agg_gap(ledger)
    gate = readiness.validate_gap_ledger_semantics(ledger)
    assert gate["status"] == readiness.STATUS_PASS
    assert gate["stats"]["gap_count"] == 1
    assert gate["stats"]["by_repair_status"]["REPAIRED"] == 1


def test_g2_missing_ledger_is_fail_not_unknown(tmp_path):
    gate = readiness.validate_gap_ledger_semantics(tmp_path / "nope.jsonl")
    assert gate["status"] == readiness.STATUS_FAIL
    assert any(f["code"] == "GAP_LEDGER_MISSING" for f in gate["findings"])


def test_g2_corrupt_ledger_line_is_blocking(tmp_path):
    ledger = tmp_path / "gap_ledger.jsonl"
    _clean_agg_gap(ledger)
    with ledger.open("a", encoding="utf-8") as f:
        f.write("{not json at all\n")
    gate = readiness.validate_gap_ledger_semantics(ledger)
    assert gate["status"] == readiness.STATUS_FAIL
    assert any(f["code"] == "GAP_LEDGER_CORRUPT" for f in gate["findings"])


def test_g2_update_before_open_is_blocking(tmp_path):
    ledger = tmp_path / "gap_ledger.jsonl"
    gap_ledger.update_gap(ledger, "GAP_aggTrade_orphan", repair_status="REPAIRED")
    gate = readiness.validate_gap_ledger_semantics(ledger)
    assert gate["status"] == readiness.STATUS_FAIL
    assert any(f["code"] == "GAP_LEDGER_EVENT_ORDER_INVALID" for f in gate["findings"])


def test_g2_gap_recorded_for_force_order_is_blocking(tmp_path):
    """forceOrder is a windowed-snapshot stream: a derived continuity gap for
    it would mean 'no message' was read as 'missing data', which the project
    forbids outright."""
    ledger = tmp_path / "gap_ledger.jsonl"
    gap_ledger.open_gap(
        ledger, component="forceOrder", gap_start=1, gap_end=2,
        reason="NO_MESSAGE_IN_WINDOW", last_good_identifier=0,
    )
    gate = readiness.validate_gap_ledger_semantics(ledger)
    assert gate["status"] == readiness.STATUS_FAIL
    assert any(
        f["code"] == "CONTINUITY_GAP_DERIVED_FOR_NON_SEQUENTIAL_STREAM"
        for f in gate["findings"]
    )


def test_g2_aggtrade_gap_start_must_follow_last_good_identifier(tmp_path):
    ledger = tmp_path / "gap_ledger.jsonl"
    gap_ledger.open_gap(
        ledger, component="aggTrade", gap_start=999, gap_end=1000,
        reason="AGGTRADE_ID_SEQUENCE_GAP", last_good_identifier=100,
    )
    gate = readiness.validate_gap_ledger_semantics(ledger)
    assert gate["status"] == readiness.STATUS_FAIL
    assert any(f["code"] == "AGGTRADE_GAP_START_INCONSISTENT" for f in gate["findings"])


def test_g2_inverted_gap_range_is_blocking(tmp_path):
    ledger = tmp_path / "gap_ledger.jsonl"
    gap_ledger.open_gap(
        ledger, component="diff_depth", gap_start=500, gap_end=400,
        reason="PU_CONTINUITY_MISMATCH", last_good_identifier=499,
    )
    gate = readiness.validate_gap_ledger_semantics(ledger)
    assert gate["status"] == readiness.STATUS_FAIL
    assert any(f["code"] == "GAP_RANGE_INVERTED" for f in gate["findings"])


def test_g2_duplicate_gap_opened_is_blocking(tmp_path):
    ledger = tmp_path / "gap_ledger.jsonl"
    gap_ledger.open_gap(
        ledger, component="aggTrade", gap_start=101, gap_end=104,
        reason="AGGTRADE_ID_SEQUENCE_GAP", last_good_identifier=100, gap_id="GAP_dup",
    )
    gap_ledger.open_gap(
        ledger, component="aggTrade", gap_start=201, gap_end=204,
        reason="AGGTRADE_ID_SEQUENCE_GAP", last_good_identifier=200, gap_id="GAP_dup",
    )
    gate = readiness.validate_gap_ledger_semantics(ledger)
    assert gate["status"] == readiness.STATUS_FAIL
    assert any(f["code"] == "DUPLICATE_GAP_OPENED" for f in gate["findings"])


def test_g2_still_open_gap_is_surfaced_but_not_blocking(tmp_path):
    """A gap still OPEN at shutdown is a valid ledger state, not a semantic
    violation -- it must be reported without failing the gate."""
    ledger = tmp_path / "gap_ledger.jsonl"
    gap_ledger.open_gap(
        ledger, component="aggTrade", gap_start=101, gap_end=104,
        reason="AGGTRADE_ID_SEQUENCE_GAP", last_good_identifier=100,
    )
    gate = readiness.validate_gap_ledger_semantics(ledger)
    assert gate["status"] == readiness.STATUS_PASS
    assert gate["stats"]["open_at_end_count"] == 1
    open_findings = [f for f in gate["findings"] if f["code"] == "GAPS_STILL_OPEN_AT_END_OF_RUN"]
    assert len(open_findings) == 1
    assert open_findings[0]["severity"] == readiness.SEVERITY_NON_BLOCKING


def test_g2_invalid_enum_value_written_directly_is_blocking(tmp_path):
    """The ledger API validates enums on write, so an invalid value can only
    arrive by a direct/foreign write -- the validator must still catch it."""
    ledger = tmp_path / "gap_ledger.jsonl"
    record = {
        "event_type": "GAP_OPENED", "gap_id": "GAP_x", "component": "aggTrade",
        "gap_start": 101, "gap_end": 104, "detection_time": "2026-09-07T00:00:00+00:00",
        "reason": "AGGTRADE_ID_SEQUENCE_GAP", "last_good_identifier": 100,
        "first_good_identifier_after": 105, "reconnect_attempts": 0,
        "recoverability": "TOTALLY_MADE_UP", "repair_status": "OPEN",
        "repair_source": None, "resync_status": "NOT_APPLICABLE",
    }
    ledger.write_text(json.dumps(record) + "\n", encoding="utf-8")
    gate = readiness.validate_gap_ledger_semantics(ledger)
    assert gate["status"] == readiness.STATUS_FAIL
    assert any(f["code"] == "INVALID_RECOVERABILITY" for f in gate["findings"])


# ===========================================================================
# G3 -- repair / resync provenance validation
# ===========================================================================

def _write_raw(raw_dir, filename, records):
    raw_dir.mkdir(parents=True, exist_ok=True)
    with (raw_dir / filename).open("w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")


def test_g3_clean_backfill_provenance_passes(tmp_path):
    ledger = tmp_path / "gap_ledger.jsonl"
    gap_id = _clean_agg_gap(ledger)
    records = [{"stream": "aggTrade", "raw": {"a": 100}, "acquisition_mode": "LIVE"}]
    # The claimed range is [101, 104] -- every one of those ids must be present.
    records += [
        {"stream": "aggTrade", "raw": {"a": i}, "acquisition_mode": "REST_BACKFILL",
         "original_gap_id": gap_id, "repaired_at": "2026-09-07T00:00:00+00:00",
         "source_provenance": "GET /fapi/v1/aggTrades?fromId="}
        for i in range(101, 105)
    ]
    _write_raw(tmp_path / "raw", "aggTrade.jsonl", records)
    gate = readiness.validate_repair_resync_provenance(ledger, tmp_path / "raw")
    assert gate["status"] == readiness.STATUS_PASS, gate["findings"]
    assert gate["stats"]["repaired_gap_count"] == 1
    assert gate["stats"]["by_acquisition_mode"]["REST_BACKFILL"] == 4
    assert gate["stats"]["backfill_coverage"][gap_id]["missing_count"] == 0


def test_g3_partially_backfilled_gap_marked_repaired_is_blocking(tmp_path):
    """Presence of SOME backfilled rows is not completeness: a gap marked
    REPAIRED over [101,104] with only 102 present is a partial repair
    presented as a finished one."""
    ledger = tmp_path / "gap_ledger.jsonl"
    gap_id = _clean_agg_gap(ledger)
    _write_raw(tmp_path / "raw", "aggTrade.jsonl", [
        {"stream": "aggTrade", "raw": {"a": 102}, "acquisition_mode": "REST_BACKFILL",
         "original_gap_id": gap_id, "repaired_at": "2026-09-07T00:00:00+00:00",
         "source_provenance": "GET /fapi/v1/aggTrades?fromId="},
    ])
    gate = readiness.validate_repair_resync_provenance(ledger, tmp_path / "raw")
    assert gate["status"] == readiness.STATUS_FAIL
    assert any(f["code"] == "REPAIRED_GAP_BACKFILL_INCOMPLETE" for f in gate["findings"])


def test_g3_repaired_gap_without_repair_source_is_blocking(tmp_path):
    ledger = tmp_path / "gap_ledger.jsonl"
    gap_id = gap_ledger.open_gap(
        ledger, component="aggTrade", gap_start=101, gap_end=104,
        reason="AGGTRADE_ID_SEQUENCE_GAP", last_good_identifier=100,
    )
    gap_ledger.update_gap(ledger, gap_id, repair_status="REPAIRED", repair_source=None)
    gate = readiness.validate_repair_resync_provenance(ledger, tmp_path / "raw")
    assert gate["status"] == readiness.STATUS_FAIL
    assert any(f["code"] == "REPAIRED_GAP_WITHOUT_REPAIR_SOURCE" for f in gate["findings"])


def test_g3_repaired_gap_without_backfill_evidence_is_blocking(tmp_path):
    """A REST-backfill repair asserted over a non-empty id range with zero
    supporting rows is a provenance hole, not a repair."""
    ledger = tmp_path / "gap_ledger.jsonl"
    _clean_agg_gap(ledger)
    _write_raw(tmp_path / "raw", "aggTrade.jsonl", [
        {"stream": "aggTrade", "raw": {"a": 100}, "acquisition_mode": "LIVE"},
    ])
    gate = readiness.validate_repair_resync_provenance(ledger, tmp_path / "raw")
    assert gate["status"] == readiness.STATUS_FAIL
    assert any(f["code"] == "REPAIRED_GAP_WITHOUT_BACKFILL_EVIDENCE" for f in gate["findings"])


def test_g3_backfill_record_missing_provenance_is_blocking(tmp_path):
    ledger = tmp_path / "gap_ledger.jsonl"
    gap_id = _clean_agg_gap(ledger)
    _write_raw(tmp_path / "raw", "aggTrade.jsonl", [
        {"stream": "aggTrade", "raw": {"a": 102}, "acquisition_mode": "REST_BACKFILL",
         "original_gap_id": gap_id},  # no repaired_at / source_provenance
    ])
    gate = readiness.validate_repair_resync_provenance(ledger, tmp_path / "raw")
    assert gate["status"] == readiness.STATUS_FAIL
    assert any(f["code"] == "BACKFILL_RECORD_MISSING_PROVENANCE" for f in gate["findings"])


def test_g3_backfill_referencing_unknown_gap_is_blocking(tmp_path):
    ledger = tmp_path / "gap_ledger.jsonl"
    _clean_agg_gap(ledger)
    _write_raw(tmp_path / "raw", "aggTrade.jsonl", [
        {"stream": "aggTrade", "raw": {"a": 102}, "acquisition_mode": "REST_BACKFILL",
         "original_gap_id": "GAP_does_not_exist", "repaired_at": "2026-09-07T00:00:00+00:00",
         "source_provenance": "GET /fapi/v1/aggTrades?fromId="},
    ])
    gate = readiness.validate_repair_resync_provenance(ledger, tmp_path / "raw")
    assert gate["status"] == readiness.STATUS_FAIL
    assert any(f["code"] == "BACKFILL_RECORD_REFERENCES_UNKNOWN_GAP" for f in gate["findings"])


def test_g3_raw_record_with_invalid_acquisition_mode_is_blocking(tmp_path):
    ledger = tmp_path / "gap_ledger.jsonl"
    _write_raw(tmp_path / "raw", "depth.jsonl", [
        {"stream": "depth", "raw": {"u": 1}, "acquisition_mode": "GUESSED"},
    ])
    gate = readiness.validate_repair_resync_provenance(ledger, tmp_path / "raw")
    assert gate["status"] == readiness.STATUS_FAIL
    assert any(f["code"] == "RAW_RECORD_INVALID_ACQUISITION_MODE" for f in gate["findings"])


def test_g3_unrecoverable_gap_requires_detail_and_matching_recoverability(tmp_path):
    ledger = tmp_path / "gap_ledger.jsonl"
    gap_id = gap_ledger.open_gap(
        ledger, component="aggTrade", gap_start=101, gap_end=104,
        reason="AGGTRADE_ID_SEQUENCE_GAP", last_good_identifier=100,
    )
    # UNRECOVERABLE_CONFIRMED but recoverability left untouched and no detail
    gap_ledger.update_gap(ledger, gap_id, repair_status="UNRECOVERABLE_CONFIRMED")
    gate = readiness.validate_repair_resync_provenance(ledger, tmp_path / "raw")
    assert gate["status"] == readiness.STATUS_FAIL
    codes = {f["code"] for f in gate["findings"]}
    assert "UNRECOVERABLE_GAP_RECOVERABILITY_MISMATCH" in codes
    assert "UNRECOVERABLE_GAP_WITHOUT_DETAIL" in codes


def test_g3_marked_unrecoverable_via_api_passes(tmp_path):
    ledger = tmp_path / "gap_ledger.jsonl"
    gap_id = gap_ledger.open_gap(
        ledger, component="aggTrade", gap_start=101, gap_end=104,
        reason="AGGTRADE_ID_SEQUENCE_GAP", last_good_identifier=100,
    )
    gap_ledger.mark_gap_unrecoverable(ledger, gap_id, detail="REST_NETWORK_ERROR")
    gate = readiness.validate_repair_resync_provenance(ledger, tmp_path / "raw")
    assert gate["status"] == readiness.STATUS_PASS
    assert gate["stats"]["unrecoverable_gap_count"] == 1


def test_g3_repaired_depth_gap_must_have_completed_resync(tmp_path):
    ledger = tmp_path / "gap_ledger.jsonl"
    gap_id = gap_ledger.open_gap(
        ledger, component="diff_depth", gap_start=10, gap_end=20,
        reason="PU_CONTINUITY_MISMATCH", last_good_identifier=9,
    )
    gap_ledger.update_gap(
        ledger, gap_id, repair_status="REPAIRED",
        repair_source="DIFF_DEPTH_RESYNC", resync_status="PENDING",
    )
    gate = readiness.validate_repair_resync_provenance(ledger, tmp_path / "raw")
    assert gate["status"] == readiness.STATUS_FAIL
    assert any(f["code"] == "REPAIRED_DEPTH_GAP_RESYNC_NOT_COMPLETED" for f in gate["findings"])


# ===========================================================================
# G4 / G5 -- soak execution and storage/throughput
# ===========================================================================

def _good_summary(**overrides):
    summary = {
        "schema": readiness.SOAK_SUMMARY_SCHEMA,
        "planned_duration_seconds": 3600.0,
        "actual_duration_seconds": 3605.0,
        "streams": {
            "aggTrade": {
                "component": "aggTrade", "message_count": 50000, "raw_bytes": 9000000,
                "raw_lines": 50000, "bytes_per_second": 2497.2,
                "projected_gb_per_day": 0.2, "final_state": "HEALTHY",
                "reconnect_count": 0, "stale_events": 0,
            },
            "diff_depth": {
                "component": "diff_depth", "message_count": 36000, "raw_bytes": 48000000,
                "raw_lines": 36000, "bytes_per_second": 13318.9,
                "projected_gb_per_day": 1.07, "final_state": "HEALTHY",
                "reconnect_count": 0, "stale_events": 0,
            },
            "forceOrder": {
                "component": "forceOrder", "message_count": 0, "raw_bytes": 0,
                "raw_lines": 0, "bytes_per_second": 0.0,
                "projected_gb_per_day": 0.0, "final_state": "HEALTHY",
                "reconnect_count": 0, "stale_events": 0,
            },
        },
        "writers": {
            "aggTrade": {"submitted_count": 50000, "written_count": 50000, "dropped_count": 0,
                         "queue_maxsize": 10000, "queue_depth_final": 0,
                         "queue_depth_peak": 12, "backlog_at_stop": 0},
            "diff_depth": {"submitted_count": 36000, "written_count": 36000, "dropped_count": 0,
                           "queue_maxsize": 10000, "queue_depth_final": 0,
                           "queue_depth_peak": 31, "backlog_at_stop": 0},
            "forceOrder": {"submitted_count": 0, "written_count": 0, "dropped_count": 0,
                           "queue_maxsize": 10000, "queue_depth_final": 0,
                           "queue_depth_peak": 0, "backlog_at_stop": 0},
        },
        "totals": {
            "message_count": 86000, "raw_bytes": 57000000,
            "bytes_per_second": 15816.1, "projected_gb_per_day": 1.27,
        },
    }
    summary.update(overrides)
    return summary


def _good_samples(values=(50_000_000, 52_000_000, 51_500_000, 52_400_000, 52_100_000), verified=True):
    return {
        "schema": readiness.PROCESS_SAMPLES_SCHEMA,
        "pid": 4242,
        "identity_verification": {
            "launcher_pid": 4200, "worker_pid": 4242,
            "method": "Win32_Process ParentProcessId query", "verified": verified,
        },
        "samples": [
            {"elapsed_seconds": i * 300.0, "working_set_bytes": v,
             "private_memory_bytes": v - 1000, "cpu_total_seconds": 10.0 + i * 5.0}
            for i, v in enumerate(values)
        ],
    }


def _good_exit_evidence(exit_code=0):
    return {"schema": readiness.PROCESS_EXIT_EVIDENCE_SCHEMA, "launcher_pid": 4200,
            "worker_pid": 4242, "exit_code": exit_code}


def test_g4_good_soak_passes(tmp_path):
    gate = readiness.validate_soak_execution(_good_summary(), _good_exit_evidence())
    assert gate["status"] == readiness.STATUS_PASS
    assert gate["stats"]["collector_runtime_behavior"] == readiness.STATUS_PASS


def test_g4_force_order_with_zero_messages_is_not_a_failure():
    """A whole hour with no liquidation message is a legitimate market
    outcome for a windowed-snapshot stream, never a collector failure."""
    summary = _good_summary()
    assert summary["streams"]["forceOrder"]["message_count"] == 0
    gate = readiness.validate_soak_execution(summary, _good_exit_evidence())
    assert gate["status"] == readiness.STATUS_PASS


def test_g4_short_run_is_blocking():
    gate = readiness.validate_soak_execution(
        _good_summary(actual_duration_seconds=600.0), _good_exit_evidence()
    )
    assert gate["status"] == readiness.STATUS_FAIL
    assert any(f["code"] == "SOAK_TOO_SHORT" for f in gate["findings"])
    assert gate["stats"]["collector_runtime_behavior"] == readiness.STATUS_FAIL


def test_g4_core_stream_without_messages_is_blocking():
    summary = _good_summary()
    summary["streams"]["diff_depth"]["message_count"] = 0
    gate = readiness.validate_soak_execution(summary, _good_exit_evidence())
    assert gate["status"] == readiness.STATUS_FAIL
    assert any(f["code"] == "SOAK_CORE_STREAM_NO_MESSAGES" for f in gate["findings"])


def test_g4_core_stream_not_healthy_at_end_is_blocking():
    summary = _good_summary()
    summary["streams"]["aggTrade"]["final_state"] = "RECONNECTING"
    gate = readiness.validate_soak_execution(summary, _good_exit_evidence())
    assert gate["status"] == readiness.STATUS_FAIL
    assert any(f["code"] == "SOAK_CORE_STREAM_NOT_HEALTHY_AT_END" for f in gate["findings"])


def test_g4_lifecycle_thread_crashed_during_soak_is_blocking():
    summary = _good_summary()
    summary["streams"]["forceOrder"]["final_state"] = "LIFECYCLE_THREAD_CRASHED"
    gate = readiness.validate_soak_execution(summary, _good_exit_evidence())
    assert gate["status"] == readiness.STATUS_FAIL
    assert any(
        f["code"] == "SOAK_COMPONENT_LIFECYCLE_THREAD_CRASHED" for f in gate["findings"]
    )


def test_g4_dropped_records_are_blocking():
    summary = _good_summary()
    summary["writers"]["diff_depth"]["dropped_count"] = 7
    gate = readiness.validate_soak_execution(summary, _good_exit_evidence())
    assert gate["status"] == readiness.STATUS_FAIL
    assert any(f["code"] == "SOAK_WRITER_DROPPED_RECORDS" for f in gate["findings"])


def test_g4_queue_saturation_is_blocking():
    summary = _good_summary()
    summary["writers"]["diff_depth"]["queue_depth_peak"] = 10000
    gate = readiness.validate_soak_execution(summary, _good_exit_evidence())
    assert gate["status"] == readiness.STATUS_FAIL
    assert any(f["code"] == "SOAK_WRITER_QUEUE_SATURATED" for f in gate["findings"])


def test_g4_writer_backlog_at_stop_is_blocking():
    summary = _good_summary()
    summary["writers"]["aggTrade"]["backlog_at_stop"] = 250
    gate = readiness.validate_soak_execution(summary, _good_exit_evidence())
    assert gate["status"] == readiness.STATUS_FAIL
    assert any(f["code"] == "SOAK_WRITER_BACKLOG_AT_STOP" for f in gate["findings"])


def test_g4_missing_summary_is_fail_not_unknown():
    gate = readiness.validate_soak_execution(None, _good_exit_evidence())
    assert gate["status"] == readiness.STATUS_FAIL
    assert any(f["code"] == "SOAK_SUMMARY_MISSING" for f in gate["findings"])
    assert gate["stats"]["collector_runtime_behavior"] == readiness.STATUS_FAIL


def test_g4_missing_exit_evidence_is_blocking_but_does_not_impugn_runtime_behavior():
    """This is exactly the scenario this changeset hit for real: the soak
    itself behaved (streams HEALTHY, writers kept pace, stderr empty), but the
    harness could not prove the process's exit code (a venv launcher/worker
    PID mismatch). The gate must FAIL overall -- required evidence is missing
    -- while still recording that the collector's own behavior was fine, so a
    reader does not misread this as '60-minute collector crashed'."""
    gate = readiness.validate_soak_execution(_good_summary(), None)
    assert gate["status"] == readiness.STATUS_FAIL
    assert any(f["code"] == "REQUIRED_EXIT_STATUS_EVIDENCE_UNAVAILABLE" for f in gate["findings"])
    assert gate["stats"]["collector_runtime_behavior"] == readiness.STATUS_PASS


def test_g4_nonzero_exit_code_is_blocking():
    gate = readiness.validate_soak_execution(_good_summary(), _good_exit_evidence(exit_code=1))
    assert gate["status"] == readiness.STATUS_FAIL
    assert any(f["code"] == "SOAK_PROCESS_NONZERO_EXIT" for f in gate["findings"])


def test_g4_exit_evidence_present_but_code_not_captured_is_blocking():
    """A supplied exit_evidence record whose exit_code field itself is null
    (e.g. Start-Process -PassThru resolved to a launcher/shim handle) must
    still trip REQUIRED_EXIT_STATUS_EVIDENCE_UNAVAILABLE, not silently pass
    just because a dict was present."""
    gate = readiness.validate_soak_execution(
        _good_summary(), {"schema": readiness.PROCESS_EXIT_EVIDENCE_SCHEMA, "exit_code": None}
    )
    assert gate["status"] == readiness.STATUS_FAIL
    assert any(f["code"] == "REQUIRED_EXIT_STATUS_EVIDENCE_UNAVAILABLE" for f in gate["findings"])


def test_g4_process_samples_no_longer_evaluated_here():
    """Resource sampling moved to G5 -- this gate must not resurrect the old
    PROCESS_SAMPLES_MISSING / PROCESS_RSS_MONOTONIC_RUNAWAY findings."""
    gate = readiness.validate_soak_execution(_good_summary(), _good_exit_evidence())
    codes = {f["code"] for f in gate["findings"]}
    assert "PROCESS_SAMPLES_MISSING" not in codes
    assert "PROCESS_RSS_MONOTONIC_RUNAWAY" not in codes
    assert "process" not in gate["stats"]


def test_g5_good_summary_and_samples_pass():
    gate = readiness.validate_storage_throughput(_good_summary(), _good_samples())
    assert gate["status"] == readiness.STATUS_PASS, gate["findings"]
    assert gate["stats"]["totals"]["raw_bytes"] == 57000000
    assert gate["stats"]["process"]["cpu_total_seconds_delta"] == 20.0
    assert gate["stats"]["identity_verification"]["verified"] is True


def test_g5_unmeasured_metric_is_blocking():
    summary = _good_summary()
    summary["streams"]["aggTrade"]["projected_gb_per_day"] = None
    gate = readiness.validate_storage_throughput(summary, _good_samples())
    assert gate["status"] == readiness.STATUS_FAIL
    assert any(f["code"] == "STORAGE_METRIC_NOT_MEASURED" for f in gate["findings"])


def test_g5_totals_inconsistent_with_streams_is_blocking():
    summary = _good_summary()
    summary["totals"]["raw_bytes"] = 1
    gate = readiness.validate_storage_throughput(summary, _good_samples())
    assert gate["status"] == readiness.STATUS_FAIL
    assert any(f["code"] == "TOTAL_STORAGE_INCONSISTENT" for f in gate["findings"])


def test_g5_writer_count_disagreeing_with_file_is_blocking():
    """If the in-process counter and the on-disk file disagree, every derived
    byte-rate is untrustworthy."""
    summary = _good_summary()
    summary["streams"]["aggTrade"]["raw_lines"] = 49000
    gate = readiness.validate_storage_throughput(summary, _good_samples())
    assert gate["status"] == readiness.STATUS_FAIL
    assert any(f["code"] == "WRITER_COUNT_DISAGREES_WITH_FILE" for f in gate["findings"])


def test_g5_messages_but_zero_bytes_is_blocking():
    summary = _good_summary()
    summary["streams"]["forceOrder"]["message_count"] = 5
    summary["streams"]["forceOrder"]["raw_lines"] = 5
    summary["writers"]["forceOrder"]["written_count"] = 5
    gate = readiness.validate_storage_throughput(summary, _good_samples())
    assert gate["status"] == readiness.STATUS_FAIL
    assert any(f["code"] == "STREAM_HAS_MESSAGES_BUT_NO_BYTES" for f in gate["findings"])


def test_g5_missing_process_samples_is_blocking():
    gate = readiness.validate_storage_throughput(_good_summary(), None)
    assert gate["status"] == readiness.STATUS_FAIL
    assert any(f["code"] == "PROCESS_SAMPLES_MISSING" for f in gate["findings"])


def test_g5_monotonic_rss_runaway_is_blocking():
    samples = _good_samples(values=(50_000_000, 90_000_000, 140_000_000, 200_000_000, 260_000_000))
    gate = readiness.validate_storage_throughput(_good_summary(), samples)
    assert gate["status"] == readiness.STATUS_FAIL
    assert any(f["code"] == "PROCESS_RSS_MONOTONIC_RUNAWAY" for f in gate["findings"])


def test_g5_mild_monotonic_growth_is_not_a_runaway():
    """Strictly increasing but modest growth must not trip the detector --
    the gate asks for an OBVIOUS runaway, not any upward trend at all."""
    samples = _good_samples(values=(50_000_000, 51_000_000, 52_000_000, 53_000_000, 54_000_000))
    gate = readiness.validate_storage_throughput(_good_summary(), samples)
    assert gate["status"] == readiness.STATUS_PASS, gate["findings"]
    assert gate["stats"]["process"]["rss_strictly_monotonic_increasing"] is True
    assert gate["stats"]["process"]["obvious_monotonic_runaway"] is False


def test_g5_unverified_process_identity_is_blocking():
    samples = _good_samples(verified=False)
    gate = readiness.validate_storage_throughput(_good_summary(), samples)
    assert gate["status"] == readiness.STATUS_FAIL
    assert any(f["code"] == "RESOURCE_SAMPLE_PROCESS_IDENTITY_UNVERIFIED" for f in gate["findings"])


def test_g5_absent_identity_verification_block_is_blocking():
    """No identity_verification key at all -- exactly what this changeset's
    real STEP 22 soak produced -- must trip the same finding as an explicit
    verified=False, not be silently treated as trustworthy."""
    samples = _good_samples()
    del samples["identity_verification"]
    gate = readiness.validate_storage_throughput(_good_summary(), samples)
    assert gate["status"] == readiness.STATUS_FAIL
    assert any(f["code"] == "RESOURCE_SAMPLE_PROCESS_IDENTITY_UNVERIFIED" for f in gate["findings"])


def test_g5_zero_cpu_delta_with_real_workload_is_blocking_regardless_of_identity():
    """This is the exact real-world defect this changeset uncovered: a fully
    verified-looking identity block cannot rescue a CPU-delta-vs-workload
    contradiction -- the two forms of evidence disagree about the same
    process, and that disagreement is disqualifying on its own."""
    samples = _good_samples(values=(50_000_000,) * 5)
    for s in samples["samples"]:
        s["cpu_total_seconds"] = 0.0
        s["private_memory_bytes"] = 811008
    summary = _good_summary(actual_duration_seconds=3604.467)
    summary["totals"]["message_count"] = 78635
    gate = readiness.validate_storage_throughput(summary, samples)
    assert gate["status"] == readiness.STATUS_FAIL
    codes = {f["code"] for f in gate["findings"]}
    assert "RESOURCE_SAMPLE_INCONSISTENT_WITH_WORKLOAD" in codes
    assert "RESOURCE_SAMPLE_SUSPICIOUS_CONSTANT_MEMORY" in codes


def test_g5_zero_cpu_delta_on_short_or_idle_run_is_not_automatically_blocking():
    """The consistency rule must never fire on a genuinely short/idle run --
    it requires BOTH a long-enough runtime AND real completed message volume
    from the collector's own counters, not a bare CPU-delta-is-zero rule."""
    samples = _good_samples(values=(50_000_000,) * 5)
    for s in samples["samples"]:
        s["cpu_total_seconds"] = 0.0
    summary = _good_summary(actual_duration_seconds=30.0)  # short run
    summary["totals"]["message_count"] = 0
    gate = readiness.validate_storage_throughput(summary, samples)
    assert not any(
        f["code"] == "RESOURCE_SAMPLE_INCONSISTENT_WITH_WORKLOAD" for f in gate["findings"]
    )


def test_g5_constant_memory_alone_is_non_blocking():
    """Flat private memory by itself -- CPU delta genuinely nonzero, identity
    verified -- must be recorded but must NOT fail the gate on its own."""
    samples = _good_samples()
    for s in samples["samples"]:
        s["private_memory_bytes"] = 5_000_000
    gate = readiness.validate_storage_throughput(_good_summary(), samples)
    const_findings = [f for f in gate["findings"]
                       if f["code"] == "RESOURCE_SAMPLE_SUSPICIOUS_CONSTANT_MEMORY"]
    assert len(const_findings) == 1
    assert const_findings[0]["severity"] == readiness.SEVERITY_NON_BLOCKING
    assert gate["status"] == readiness.STATUS_PASS


def test_g5_too_few_samples_is_blocking():
    samples = _good_samples(values=(1, 2))
    samples["samples"] = samples["samples"][:2]
    gate = readiness.validate_storage_throughput(_good_summary(), samples)
    assert gate["status"] == readiness.STATUS_FAIL
    assert any(f["code"] == "PROCESS_SAMPLES_TOO_FEW" for f in gate["findings"])


# ===========================================================================
# Supplied gates (G1 / G6 / G7) and the G8 fold
# ===========================================================================

def test_supplied_gate_missing_is_fail_not_unknown():
    gate = readiness.validate_supplied_gate("G6_PROCESS_RESTART_RECOVERY", None)
    assert gate["status"] == readiness.STATUS_FAIL
    assert any(f["code"] == "SUPPLIED_GATE_MISSING" for f in gate["findings"])


def test_supplied_gate_without_evidence_is_rejected():
    gate = readiness.validate_supplied_gate(
        "G7_ENVIRONMENT_DEPENDENCIES_FROZEN", {"status": "PASS", "evidence": []}
    )
    assert gate["status"] == readiness.STATUS_FAIL
    assert any(f["code"] == "SUPPLIED_GATE_WITHOUT_EVIDENCE" for f in gate["findings"])


def test_supplied_gate_reporting_fail_stays_fail():
    gate = readiness.validate_supplied_gate(
        "G6_PROCESS_RESTART_RECOVERY",
        {"status": "FAIL", "evidence": ["restart test output"],
         "detail": "restart gap was not recorded"},
    )
    assert gate["status"] == readiness.STATUS_FAIL
    assert any(f["code"] == "SUPPLIED_GATE_REPORTED_FAIL" for f in gate["findings"])


def test_supplied_gate_with_evidence_passes():
    gate = readiness.validate_supplied_gate(
        "G1_THREAD_CRASH_OBSERVABILITY",
        {"status": "PASS", "evidence": ["pytest tests/test_collector_readiness.py: 5 passed"]},
    )
    assert gate["status"] == readiness.STATUS_PASS


def _all_passing_gate_results():
    return {
        name: {"gate": name, "status": readiness.STATUS_PASS, "findings": [], "stats": {}}
        for name in readiness.REQUIRED_GATES
        if name != "G8_FORMAL_DATASET_STARTUP_GUARD"
    }


def test_g8_all_gates_passing_yields_readiness_pass():
    gate_results = _all_passing_gate_results()
    gate_8, blockers = readiness.evaluate_startup_gate(gate_results)
    gate_results["G8_FORMAL_DATASET_STARTUP_GUARD"] = gate_8
    report = readiness.build_readiness_report(gate_results=gate_results, blockers=blockers)
    assert blockers == []
    assert report["readiness"] == readiness.STATUS_PASS
    assert report["formal_dataset_start_allowed"] is True
    assert set(report["gates"]) == set(readiness.REQUIRED_GATES)


def test_g8_one_failing_gate_yields_readiness_fail_with_blockers():
    gate_results = _all_passing_gate_results()
    gate_results["G4_60_MINUTE_SOAK"] = {
        "gate": "G4_60_MINUTE_SOAK", "status": readiness.STATUS_FAIL,
        "findings": [{"code": "SOAK_TOO_SHORT", "severity": readiness.SEVERITY_BLOCKING,
                      "detail": "ran 600s"}],
        "stats": {},
    }
    gate_8, blockers = readiness.evaluate_startup_gate(gate_results)
    gate_results["G8_FORMAL_DATASET_STARTUP_GUARD"] = gate_8
    report = readiness.build_readiness_report(gate_results=gate_results, blockers=blockers)
    assert report["readiness"] == readiness.STATUS_FAIL
    assert report["formal_dataset_start_allowed"] is False
    assert len(blockers) == 1
    assert "SOAK_TOO_SHORT" in blockers[0]


def test_g8_missing_gate_is_treated_as_fail():
    gate_results = _all_passing_gate_results()
    del gate_results["G6_PROCESS_RESTART_RECOVERY"]
    gate_8, blockers = readiness.evaluate_startup_gate(gate_results)
    assert gate_8["status"] == readiness.STATUS_FAIL
    assert any("G6_PROCESS_RESTART_RECOVERY" in b for b in blockers)
    gate_results["G8_FORMAL_DATASET_STARTUP_GUARD"] = gate_8
    report = readiness.build_readiness_report(gate_results=gate_results, blockers=blockers)
    assert report["formal_dataset_start_allowed"] is False


def test_g8_non_blocking_findings_do_not_create_blockers():
    gate_results = _all_passing_gate_results()
    gate_results["G2_GAP_LEDGER_SEMANTICS"] = {
        "gate": "G2_GAP_LEDGER_SEMANTICS", "status": readiness.STATUS_PASS,
        "findings": [{"code": "GAPS_STILL_OPEN_AT_END_OF_RUN",
                      "severity": readiness.SEVERITY_NON_BLOCKING, "detail": "1 open"}],
        "stats": {},
    }
    _, blockers = readiness.evaluate_startup_gate(gate_results)
    assert blockers == []


# ===========================================================================
# End-to-end: a real soak directory produced by the real collector code
# ===========================================================================

def test_soak_summary_built_by_collector_is_accepted_by_the_validator(tmp_path):
    """build_soak_summary() and the readiness validator must agree on the
    schema -- built from the real RawPayloadWriter/Supervisor objects, no
    network, no formal Dataset."""
    components = collector.build_components("BTCUSDT", tmp_path)
    supervisor = components["supervisor"]
    for writer in components["writers"]:
        writer.start()

    meta = {"received_local_wall_ms": 1, "received_monotonic_ms": 1}
    for name in ("aggTrade", "diff_depth", "forceOrder"):
        supervisor.transition(name, sup.STATE_STREAM_NOT_READY, reason="TEST")
        supervisor.transition(name, sup.STATE_HEALTHY, reason="TEST")
    components["agg_collector"].handle_message({"a": 1}, meta)
    components["agg_collector"].handle_message({"a": 2}, meta)
    components["force_order_collector"].handle_message({"o": {}}, meta)

    deadline = time.monotonic() + 5.0
    while components["writers_by_component"]["aggTrade"].written_count < 2 \
            and time.monotonic() < deadline:
        time.sleep(0.01)
    for writer in components["writers"]:
        writer.stop()

    summary = collector.build_soak_summary(
        components, symbol="BTCUSDT", runtime_dir=tmp_path,
        started_at_iso="2026-09-07T00:00:00+00:00", ended_at_iso="2026-09-07T01:00:00+00:00",
        planned_duration_seconds=3600.0, actual_duration_seconds=3600.0,
    )

    assert summary["schema"] == readiness.SOAK_SUMMARY_SCHEMA
    assert summary["formal_dataset_created"] is False
    assert summary["streams"]["aggTrade"]["message_count"] == 2
    assert summary["streams"]["aggTrade"]["raw_lines"] == 2
    assert summary["totals"]["raw_bytes"] > 0
    assert summary["writers"]["aggTrade"]["backlog_at_stop"] == 0

    # The validator must find every metric it requires present and coherent.
    gate = readiness.validate_storage_throughput(summary, _good_samples())
    assert gate["status"] == readiness.STATUS_PASS, gate["findings"]

    soak_gate = readiness.validate_soak_execution(summary, _good_exit_evidence())
    assert not any(
        f["code"] in (
            "SOAK_SUMMARY_SCHEMA_MISMATCH", "SOAK_STREAM_MISSING",
            "SOAK_WRITER_METRICS_MISSING", "SOAK_WRITER_BACKLOG_AT_STOP",
            "SOAK_WRITER_DROPPED_RECORDS",
        )
        for f in soak_gate["findings"]
    ), soak_gate["findings"]


def test_run_readiness_evaluation_on_empty_dir_fails_closed(tmp_path):
    """No evidence at all must produce READINESS = FAIL with blockers, never
    a PASS and never an ambiguous third state."""
    report, _ = readiness.run_readiness_evaluation(soak_dir=tmp_path, supplied_gates={})
    assert report["schema"] == readiness.READINESS_REPORT_SCHEMA
    assert report["readiness"] == readiness.STATUS_FAIL
    assert report["formal_dataset_start_allowed"] is False
    assert report["blockers"]
    assert set(report["gates"]) == set(readiness.REQUIRED_GATES)


# ===========================================================================
# G6 -- process restart / recovery (continuity checkpoint semantics)
# ===========================================================================

class _FakeSupervisor:
    """Minimal supervisor stand-in: records messages, never transitions."""

    def __init__(self):
        self.messages = {}

    def record_message(self, name, *, local_wall_ms):
        self.messages[name] = self.messages.get(name, 0) + 1


def _drain(writer, expected, timeout=5.0):
    deadline = time.monotonic() + timeout
    while writer.written_count < expected and time.monotonic() < deadline:
        time.sleep(0.005)


def test_checkpoint_high_water_is_durably_persisted_not_merely_received(tmp_path):
    """The checkpoint must record what actually reached the file. A record
    that has been submitted but is still sitting in the queue must NOT yet
    count -- otherwise a kill at that instant leaves a checkpoint claiming an
    id the next process cannot find on disk, and the real gap is hidden."""
    writer = collector.RawPayloadWriter(
        tmp_path / "raw" / "aggTrade.jsonl", key_fn=collector.agg_trade_key_fn
    )
    assert writer.persisted_high_water is None

    # Submitted but the writer thread has not been started: nothing is on disk.
    writer.submit({"stream": "aggTrade", "raw": {"a": 500}, "acquisition_mode": "LIVE"})
    assert writer.persisted_high_water is None, \
        "high-water moved before the record was written+flushed"

    writer.start()
    _drain(writer, 1)
    assert writer.persisted_high_water == 500

    # Backfilled rows are written later but carry LOWER ids: the high-water
    # mark must never move backwards.
    writer.submit({"stream": "aggTrade", "raw": {"a": 480},
                   "acquisition_mode": "REST_BACKFILL"})
    _drain(writer, 2)
    assert writer.persisted_high_water == 500
    writer.stop()


def test_checkpoint_atomic_write_and_read_roundtrip(tmp_path):
    writers = {
        "aggTrade": collector.RawPayloadWriter(tmp_path / "raw" / "aggTrade.jsonl",
                                               key_fn=collector.agg_trade_key_fn),
        "diff_depth": collector.RawPayloadWriter(tmp_path / "raw" / "depth.jsonl",
                                                 key_fn=collector.depth_key_fn),
    }
    writers["aggTrade"].persisted_high_water = 777
    writers["diff_depth"].persisted_high_water = 999

    collector.write_continuity_checkpoint(tmp_path, writers)

    # Atomic write leaves no .tmp behind.
    assert not (tmp_path / "continuity_checkpoint.json.tmp").exists()

    payload = collector.read_continuity_checkpoint(tmp_path)
    assert payload["schema"] == collector.CONTINUITY_CHECKPOINT_SCHEMA
    assert payload["authoritative_for_restore"] is False
    assert payload["components"]["aggTrade"]["last_durably_persisted_trade_id"] == 777
    assert payload["components"]["diff_depth"]["last_durably_persisted_update_id"] == 999


def test_missing_checkpoint_reads_as_none_and_corrupt_one_raises(tmp_path):
    assert collector.read_continuity_checkpoint(tmp_path) is None
    (tmp_path / "continuity_checkpoint.json").write_text("{broken", encoding="utf-8")
    with pytest.raises(collector.CollectorError):
        collector.read_continuity_checkpoint(tmp_path)


def test_restart_does_not_restore_component_state_to_healthy(tmp_path):
    """A checkpoint left by a prior process must never put a component back
    into HEALTHY. The new process starts every component at CONNECTING and
    has to earn HEALTHY again through the normal sequence."""
    writers = {
        "aggTrade": collector.RawPayloadWriter(tmp_path / "raw" / "aggTrade.jsonl"),
        "diff_depth": collector.RawPayloadWriter(tmp_path / "raw" / "depth.jsonl"),
    }
    writers["aggTrade"].persisted_high_water = 100
    writers["diff_depth"].persisted_high_water = 200
    collector.write_continuity_checkpoint(tmp_path, writers)

    components = collector.build_components("BTCUSDT", tmp_path)
    supervisor = components["supervisor"]

    assert components["prior_checkpoint"] is not None  # evidence was read
    for name in ("aggTrade", "diff_depth", "forceOrder"):
        assert supervisor.get(name).state == sup.STATE_CONNECTING

    # And the normal sequence is still required to reach HEALTHY.
    supervisor.transition("aggTrade", sup.STATE_STREAM_NOT_READY, reason="TEST")
    supervisor.transition("aggTrade", sup.STATE_HEALTHY, reason="TEST")
    assert supervisor.get("aggTrade").state == sup.STATE_HEALTHY

    # The checkpoint values were wired in as gap-detection input only.
    assert components["agg_collector"].restart_checkpoint_id == 100
    assert components["depth_collector"].restart_checkpoint_update_id == 200


def test_aggtrade_restart_gap_detected_backfilled_and_continuity_verified(tmp_path):
    """checkpoint=100, first post-restart live id=105 -> gap [101,104],
    backfilled via fromId, deduped, and only then marked REPAIRED."""
    ledger = tmp_path / "gap_ledger.jsonl"
    writer = collector.RawPayloadWriter(
        tmp_path / "raw" / "aggTrade.jsonl", key_fn=collector.agg_trade_key_fn
    ).start()

    calls = []

    def fake_rest(path, params):
        calls.append(params)
        from_id = int(params["fromId"])
        # Deliberately overlaps the checkpoint and repeats a row, so dedupe
        # and range-filtering are both exercised.
        return [{"a": i, "p": "1", "q": "1"} for i in range(from_id - 2, from_id + 8)]

    agg = collector.AggTradeCollector(
        "BTCUSDT", gap_ledger_path=ledger, writer=writer,
        supervisor=_FakeSupervisor(), request_fn=fake_rest, restart_checkpoint_id=100,
    )
    meta = {"received_local_wall_ms": 1, "received_monotonic_ms": 1}
    agg.handle_message({"a": 105}, meta)

    assert agg.restart_gap_id is not None
    _drain(writer, 5)
    writer.stop()

    gaps = gap_ledger.materialize_latest(ledger)
    assert len(gaps) == 1
    gap = gaps[0]
    assert gap["reason"] == "PROCESS_RESTART_SEQUENCE_GAP"
    assert gap["gap_start"] == 101 and gap["gap_end"] == 104
    assert gap["last_good_identifier"] == 100
    assert gap["first_good_identifier_after"] == 105
    assert gap["repair_status"] == "REPAIRED"
    assert gap["repair_source"] == "REST_AGGTRADES_FROM_ID"
    assert gap["continuity_verified"] is True
    assert gap["backfilled_id_count"] == 4

    # Exactly the four missing ids were persisted as backfill, no duplicates,
    # nothing outside the range.
    lines = [json.loads(l) for l in
             (tmp_path / "raw" / "aggTrade.jsonl").read_text(encoding="utf-8").splitlines() if l]
    backfilled = sorted(r["raw"]["a"] for r in lines if r.get("acquisition_mode") == "REST_BACKFILL")
    assert backfilled == [101, 102, 103, 104]

    # And the readiness validator agrees this is complete, verified provenance.
    gate = readiness.validate_repair_resync_provenance(ledger, tmp_path / "raw")
    assert gate["status"] == readiness.STATUS_PASS, gate["findings"]


def test_aggtrade_contiguous_restart_creates_no_false_gap(tmp_path):
    """checkpoint=100, first post-restart live id=101 -> the restart cost us
    nothing. Inventing a gap here would corrupt the evidence just as badly as
    missing one."""
    ledger = tmp_path / "gap_ledger.jsonl"
    writer = collector.RawPayloadWriter(tmp_path / "raw" / "aggTrade.jsonl").start()

    def fake_rest(path, params):
        raise AssertionError("no backfill should be attempted for a contiguous restart")

    agg = collector.AggTradeCollector(
        "BTCUSDT", gap_ledger_path=ledger, writer=writer,
        supervisor=_FakeSupervisor(), request_fn=fake_rest, restart_checkpoint_id=100,
    )
    agg.handle_message({"a": 101}, {"received_local_wall_ms": 1, "received_monotonic_ms": 1})
    writer.stop()

    assert agg.restart_gap_id is None
    assert gap_ledger.materialize_latest(ledger) == []


def test_aggtrade_restart_check_runs_only_once(tmp_path):
    """The restart boundary is a single event. Later messages must go through
    the ordinary sequence-gap path, not re-open a restart gap."""
    ledger = tmp_path / "gap_ledger.jsonl"
    writer = collector.RawPayloadWriter(tmp_path / "raw" / "aggTrade.jsonl").start()

    def fake_rest(path, params):
        from_id = int(params["fromId"])
        return [{"a": i} for i in range(from_id, from_id + 50)]

    agg = collector.AggTradeCollector(
        "BTCUSDT", gap_ledger_path=ledger, writer=writer,
        supervisor=_FakeSupervisor(), request_fn=fake_rest, restart_checkpoint_id=100,
    )
    meta = {"received_local_wall_ms": 1, "received_monotonic_ms": 1}
    agg.handle_message({"a": 105}, meta)   # restart gap
    agg.handle_message({"a": 106}, meta)   # contiguous
    agg.handle_message({"a": 110}, meta)   # ordinary sequence gap
    writer.stop()

    reasons = [g["reason"] for g in gap_ledger.materialize_latest(ledger)]
    assert reasons.count("PROCESS_RESTART_SEQUENCE_GAP") == 1
    assert reasons.count("AGGTRADE_ID_SEQUENCE_GAP") == 1


def test_aggtrade_incomplete_restart_backfill_is_not_called_repaired(tmp_path):
    """If REST cannot supply the whole range, the gap must NOT be marked
    REPAIRED -- a repair claim that is not backed by data ends the
    investigation with a false answer."""
    ledger = tmp_path / "gap_ledger.jsonl"
    writer = collector.RawPayloadWriter(tmp_path / "raw" / "aggTrade.jsonl").start()

    def fake_rest(path, params):
        return [{"a": 101}]  # only one of the four missing ids, then no progress

    agg = collector.AggTradeCollector(
        "BTCUSDT", gap_ledger_path=ledger, writer=writer,
        supervisor=_FakeSupervisor(), request_fn=fake_rest, restart_checkpoint_id=100,
    )
    agg.handle_message({"a": 105}, {"received_local_wall_ms": 1, "received_monotonic_ms": 1})
    writer.stop()

    gap = gap_ledger.materialize_latest(ledger)[0]
    assert gap["repair_status"] == "UNRECOVERABLE_CONFIRMED"
    assert gap["recoverability"] == "UNRECOVERABLE"
    assert "INCOMPLETE_BACKFILL" in gap["unrecoverable_detail"]
    assert gap["continuity_verified"] is False


def test_aggtrade_restart_backfill_rest_failure_is_recorded_not_swallowed(tmp_path):
    ledger = tmp_path / "gap_ledger.jsonl"
    writer = collector.RawPayloadWriter(tmp_path / "raw" / "aggTrade.jsonl").start()

    def failing_rest(path, params):
        raise collector.CollectorError("REST_NETWORK_ERROR path=/fapi/v1/aggTrades")

    agg = collector.AggTradeCollector(
        "BTCUSDT", gap_ledger_path=ledger, writer=writer,
        supervisor=_FakeSupervisor(), request_fn=failing_rest, restart_checkpoint_id=100,
    )
    agg.handle_message({"a": 105}, {"received_local_wall_ms": 1, "received_monotonic_ms": 1})
    writer.stop()

    gap = gap_ledger.materialize_latest(ledger)[0]
    assert gap["repair_status"] == "UNRECOVERABLE_CONFIRMED"
    assert "REST_NETWORK_ERROR" in gap["unrecoverable_detail"]


def test_diff_depth_restart_closes_old_segment_and_starts_a_new_one(tmp_path):
    """A killed process's diff chain is broken by definition: the old segment
    is CLOSED, a fresh REST snapshot starts a new one, and the break is
    recorded as UNRECOVERABLE -- meaning the two segments cannot be joined,
    NOT that specific depth events are confirmed missing."""
    ledger = tmp_path / "gap_ledger.jsonl"
    writer = collector.RawPayloadWriter(tmp_path / "raw" / "depth.jsonl").start()

    def fake_snapshot(symbol, limit):
        return {"lastUpdateId": 5000, "bids": [["1", "1"]], "asks": [["2", "1"]]}

    depth = collector.DiffDepthCollector(
        "BTCUSDT", gap_ledger_path=ledger, writer=writer, supervisor=_FakeSupervisor(),
        request_fn=fake_snapshot, restart_checkpoint_update_id=4000,
    )
    new_id = depth.ensure_initial_sync()
    writer.stop()

    assert new_id == 5000
    assert depth.order_book.last_update_id == 5000  # fresh snapshot, not resumed

    gap = gap_ledger.materialize_latest(ledger)[0]
    assert gap["reason"] == "PROCESS_RESTART_ORDERBOOK_DISCONTINUITY"
    assert gap["recoverability"] == "UNRECOVERABLE"
    assert gap["repair_status"] == "UNRECOVERABLE_CONFIRMED"
    assert gap["last_good_identifier"] == 4000
    assert gap["first_good_identifier_after"] == 5000
    assert gap["extra"]["continuity_status"] == "BROKEN_BY_PROCESS_RESTART"
    assert gap["extra"]["old_segment"] == "CLOSED"
    assert gap["extra"]["update_id_span_crossed"] == 1000
    assert "NOT a claim" in gap["extra"]["unrecoverable_semantics"]

    gate = readiness.validate_repair_resync_provenance(ledger, tmp_path / "raw")
    assert gate["status"] == readiness.STATUS_PASS, gate["findings"]


def test_diff_depth_restart_without_advancing_snapshot_asserts_no_missed_events(tmp_path):
    """If the new snapshot's lastUpdateId did not advance past the checkpoint,
    it is still a new continuity segment -- but the record must not claim any
    depth event was missed."""
    ledger = tmp_path / "gap_ledger.jsonl"
    writer = collector.RawPayloadWriter(tmp_path / "raw" / "depth.jsonl").start()

    def fake_snapshot(symbol, limit):
        return {"lastUpdateId": 4000, "bids": [], "asks": []}

    depth = collector.DiffDepthCollector(
        "BTCUSDT", gap_ledger_path=ledger, writer=writer, supervisor=_FakeSupervisor(),
        request_fn=fake_snapshot, restart_checkpoint_update_id=4000,
    )
    depth.ensure_initial_sync()
    writer.stop()

    gap = gap_ledger.materialize_latest(ledger)[0]
    assert gap["extra"]["update_id_span_crossed"] == 0
    assert "no skipped updates are asserted" in gap["extra"]["note"]
    assert gap["gap_start"] == 4000 and gap["gap_end"] == 4000  # never inverted

    # The ledger must still be semantically valid.
    g2 = readiness.validate_gap_ledger_semantics(ledger)
    assert not any(f["code"] == "GAP_RANGE_INVERTED" for f in g2["findings"])


def test_fresh_start_records_no_restart_discontinuity(tmp_path):
    """With no prior checkpoint, a first start must not manufacture a restart
    record for either stream."""
    ledger = tmp_path / "gap_ledger.jsonl"
    writer = collector.RawPayloadWriter(tmp_path / "raw" / "depth.jsonl").start()

    def fake_snapshot(symbol, limit):
        return {"lastUpdateId": 5000, "bids": [], "asks": []}

    depth = collector.DiffDepthCollector(
        "BTCUSDT", gap_ledger_path=ledger, writer=writer, supervisor=_FakeSupervisor(),
        request_fn=fake_snapshot, restart_checkpoint_update_id=None,
    )
    depth.ensure_initial_sync()
    writer.stop()

    assert depth.restart_gap_id is None
    assert gap_ledger.materialize_latest(ledger) == []

