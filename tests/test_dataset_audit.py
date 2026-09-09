"""Static/synthetic tests for the read-only Dataset Audit Framework V1."""

from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path

import EvidenceHunter_dataset_audit as audit


START = datetime(2026, 1, 1, tzinfo=timezone.utc)
DATASET = "synthetic-dataset"


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def write_jsonl(path, values):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(value) + "\n" for value in values), encoding="utf-8")


def raw(stream, payload, wall, mono):
    return {"stream": stream, "raw": payload,
            "collector_received_local_wall_ms": wall,
            "collector_received_monotonic_ms": mono, "acquisition_mode": "LIVE"}


def refresh_session(root, session_id):
    directory = root / "operations" / "sessions" / session_id
    record = json.loads((directory / "session.json").read_text("utf-8"))
    evidence = {}
    for name in ("final_checkpoint.json", "final_summary.json"):
        path = directory / name
        evidence[name] = {"sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                          "source_sha256": hashlib.sha256(
                              (root / ("continuity_checkpoint.json" if name == "final_checkpoint.json"
                                       else "soak_summary.json")).read_bytes()).hexdigest()}
    record["evidence"] = evidence
    write_json(directory / "session.json", record)


def add_session(root, session_id, start, end, *, running=False):
    owner = {"worker_pid": 10, "creation_filetime": 20, "image_path": "python.exe",
             "dataset_id": DATASET, "runtime_root": str(root.resolve()),
             "collection_session_id": session_id}
    record = {"status": "RUNNING", "dataset_id": DATASET,
              "runtime_root": str(root.resolve()), "collection_session_id": session_id,
              "session_start_utc": start.isoformat(), "owner_identity": owner}
    directory = root / "operations" / "sessions" / session_id
    if not running:
        record.update(status="FINALIZED", session_end_utc=end.isoformat(),
                      session_end_cleanliness="CLEAN",
                      machine_observed_end_cause="NATURAL_DURATION_COMPLETE",
                      finalization_checks={"threads_stopped": True, "writers_drained": True},
                      stop_ack=None)
        write_json(directory / "final_checkpoint.json",
                   json.loads((root / "continuity_checkpoint.json").read_text("utf-8")))
        write_json(directory / "final_summary.json", json.loads((root / "soak_summary.json").read_text("utf-8")))
    write_json(directory / "session.json", record)
    write_json(root / "operations" / "owner_identity.json", owner)
    if not running:
        refresh_session(root, session_id)


def fixture(root, *, seconds=60, running=False, force_orders=0):
    writers = {name: {"dropped_count": 0, "backlog_at_stop": 0}
               for name in ("aggTrade", "diff_depth", "forceOrder", "depth_snapshot",
                            "clock_observations", "rest_telemetry")}
    summary = {"schema": "COLLECTOR_SOAK_SUMMARY_V1", "writers": writers,
               "started_at_iso": START.isoformat(),
               "ended_at_iso": (START + timedelta(seconds=seconds)).isoformat(),
               "actual_duration_seconds": seconds,
               "streams": {"aggTrade": {"reconnect_count": 0},
                           "diff_depth": {"reconnect_count": 0, "resync_count": 0},
                           "forceOrder": {"reconnect_count": 0}},
               "evidence_sink_failures": {"diff_depth_order_book": [], "aggTrade_rest": []}}
    write_json(root / "continuity_checkpoint.json", {
        "schema": "COLLECTOR_CONTINUITY_CHECKPOINT_V1",
        "components": {
            "aggTrade": {"last_durably_persisted_trade_id": 2},
            "diff_depth": {"last_durably_persisted_update_id": 110},
        },
    })
    write_json(root / "soak_summary.json", summary)
    write_jsonl(root / "raw" / "aggTrade.jsonl", [
        raw("aggTrade", {"a": 1, "E": 1000, "T": 999}, 1001, 1),
        raw("aggTrade", {"a": 2, "E": 1002, "T": 1001}, 1003, 2),
    ])
    write_jsonl(root / "raw" / "depth.jsonl", [
        raw("depth", {"U": 101, "u": 105, "pu": 100, "E": 1000}, 1001, 3),
        raw("depth", {"U": 106, "u": 110, "pu": 105, "E": 1002}, 1003, 4),
    ])
    write_jsonl(root / "raw" / "forceOrder.jsonl", [
        raw("forceOrder", {"e": "forceOrder", "o": {}}, 1004, 5)
        for _ in range(force_orders)
    ])
    snapshot = {"schema": "COLLECTOR_DEPTH_SNAPSHOT_EVIDENCE_V1", "rest_request_id": "r1",
                "last_update_id": 100, "parse_status": "PARSED",
                "received_local_wall_ms": 1000, "received_monotonic_ms": 1}
    write_jsonl(root / "raw" / "depth_snapshot.jsonl", [snapshot])
    clock = {"schema": "COLLECTOR_CLOCK_OBSERVATION_V1", "observed_local_wall_ms": 1000,
             "observed_monotonic_ms": 1, "availability": "AVAILABLE",
             "sync_status": {"source": "clock", "stratum": "1", "leap_indicator": "0"}}
    write_jsonl(root / "quality" / "clock_observations.jsonl", [clock])
    rest = {"schema": "COLLECTOR_REST_TELEMETRY_V1", "rest_request_id": "r1",
            "outcome": "SUCCESS", "requested_local_wall_ms": 999,
            "requested_monotonic_ms": 0, "received_local_wall_ms": 1000,
            "received_monotonic_ms": 1, "network_route_events": []}
    write_jsonl(root / "quality" / "rest_telemetry.jsonl", [rest])
    write_jsonl(root / "quality" / "gap_ledger.jsonl", [])
    write_jsonl(root / "alerts" / "collector_alerts.jsonl", [])
    add_session(root, "00000000-0000-0000-0000-000000000001", START,
                START + timedelta(seconds=seconds), running=running)
    return root


def update_summary(root, mutate):
    summary = json.loads((root / "soak_summary.json").read_text("utf-8"))
    mutate(summary)
    write_json(root / "soak_summary.json", summary)
    session = next((root / "operations" / "sessions").iterdir())
    write_json(session / "final_summary.json", summary)
    refresh_session(root, session.name)


def update_checkpoint(root, *, agg=2, depth=110):
    checkpoint = {"schema": "COLLECTOR_CONTINUITY_CHECKPOINT_V1",
                  "components": {
                      "aggTrade": {"last_durably_persisted_trade_id": agg},
                      "diff_depth": {"last_durably_persisted_update_id": depth},
                  }}
    write_json(root / "continuity_checkpoint.json", checkpoint)
    sessions = sorted((root / "operations" / "sessions").iterdir())
    latest = sessions[-1]
    if (latest / "final_checkpoint.json").exists():
        write_json(latest / "final_checkpoint.json", checkpoint)
        refresh_session(root, latest.name)


def gap(gap_id, component, start, end, status, reason):
    return {"event_type": "GAP_OPENED", "gap_id": gap_id, "component": component,
            "gap_start": start, "gap_end": end, "detection_time": START.isoformat(),
            "reason": reason, "last_good_identifier": start, "first_good_identifier_after": end,
            "reconnect_attempts": 1, "recoverability": "UNRECOVERABLE",
            "repair_status": status, "repair_source": None,
            "resync_status": "COMPLETED" if status == "REPAIRED" else "NOT_APPLICABLE"}


def test_clean_finalized_runtime_passes(tmp_path):
    report = audit.audit_runtime(fixture(tmp_path))
    assert report["audit_status"] == "PASS"
    assert report["AggTradeRecords"] == 2 and report["DepthRecords"] == 2
    assert report["orderbook_continuity_status"] == "PASS"
    assert report["finalization_status"] == "CLEAN"


def test_running_runtime_is_incomplete(tmp_path):
    report = audit.audit_runtime(fixture(tmp_path, running=True))
    assert report["audit_status"] == "INCOMPLETE"
    assert report["FreezeEligible"] is False


def test_malformed_required_jsonl_fails(tmp_path):
    root = fixture(tmp_path)
    with (root / "raw" / "aggTrade.jsonl").open("a", encoding="utf-8") as handle:
        handle.write("{broken\n")
    report = audit.audit_runtime(root)
    assert report["audit_status"] == "FAIL" and report["MalformedRecords"] == 1


def test_duplicate_aggtrade_identity_fails(tmp_path):
    root = fixture(tmp_path)
    with (root / "raw" / "aggTrade.jsonl").open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(raw("aggTrade", {"a": 2}, 1004, 5)) + "\n")
    report = audit.audit_runtime(root)
    assert report["audit_status"] == "FAIL" and report["aggtrade_duplicate_count"] == 1


def test_unexplained_aggtrade_gap_fails(tmp_path):
    root = fixture(tmp_path)
    values = [raw("aggTrade", {"a": value}, 1000 + value, value) for value in (1, 3)]
    write_jsonl(root / "raw" / "aggTrade.jsonl", values)
    report = audit.audit_runtime(root)
    assert report["audit_status"] == "FAIL"
    assert report["aggtrade_unexplained_continuity_errors"] == 1


def test_known_aggtrade_gap_is_pass_with_gaps(tmp_path):
    root = fixture(tmp_path)
    write_jsonl(root / "raw" / "aggTrade.jsonl",
                [raw("aggTrade", {"a": value}, 1000 + value, value) for value in (1, 3)])
    write_jsonl(root / "quality" / "gap_ledger.jsonl",
                [gap("g1", "aggTrade", 2, 2, "UNRECOVERABLE_CONFIRMED",
                     "AGGTRADE_ID_SEQUENCE_GAP")])
    update_checkpoint(root, agg=3)
    report = audit.audit_runtime(root)
    assert report["audit_status"] == "PASS_WITH_GAPS"
    assert report["EffectiveObservedCoverageSeconds"] == audit.NOT_EVALUABLE


def test_valid_depth_resync_boundary_is_not_fail(tmp_path):
    root = fixture(tmp_path)
    values = [raw("depth", {"U": 101, "u": 105, "pu": 100}, 1001, 3),
              raw("depth", {"U": 200, "u": 210, "pu": 199}, 1003, 4)]
    write_jsonl(root / "raw" / "depth.jsonl", values)
    write_jsonl(root / "raw" / "depth_snapshot.jsonl", [
        {"schema": "COLLECTOR_DEPTH_SNAPSHOT_EVIDENCE_V1", "rest_request_id": f"r{index}",
         "last_update_id": value, "parse_status": "PARSED",
         "received_local_wall_ms": 1000, "received_monotonic_ms": monotonic}
        for index, (value, monotonic) in enumerate(((100, 2), (199, 3.5)), 1)
    ])
    rest_path = root / "quality" / "rest_telemetry.jsonl"
    rest_records = [json.loads(line) for line in rest_path.read_text("utf-8").splitlines() if line]
    rest_records.append({**rest_records[0], "rest_request_id": "r2"})
    write_jsonl(rest_path, rest_records)
    write_jsonl(root / "quality" / "gap_ledger.jsonl",
                [gap("g1", "diff_depth", 105, 210, "REPAIRED", "PU_CONTINUITY_MISMATCH")])
    update_checkpoint(root, depth=210)
    report = audit.audit_runtime(root)
    assert report["audit_status"] != "FAIL"
    assert report["OrderBookContinuityStatus"] == "PASS_WITH_GAPS"


def test_unexplained_depth_break_fails(tmp_path):
    root = fixture(tmp_path)
    values = [raw("depth", {"U": 101, "u": 105, "pu": 100}, 1001, 3),
              raw("depth", {"U": 106, "u": 110, "pu": 999}, 1003, 4)]
    write_jsonl(root / "raw" / "depth.jsonl", values)
    assert audit.audit_runtime(root)["audit_status"] == "FAIL"


def test_writer_drop_fails(tmp_path):
    root = fixture(tmp_path)
    update_summary(root, lambda value: value["writers"]["aggTrade"].update(dropped_count=1))
    assert audit.audit_runtime(root)["WriterDrops"] == 1
    assert audit.audit_runtime(root)["audit_status"] == "FAIL"


def test_writer_backlog_fails(tmp_path):
    root = fixture(tmp_path)
    update_summary(root, lambda value: value["writers"]["diff_depth"].update(backlog_at_stop=1))
    report = audit.audit_runtime(root)
    assert report["BacklogAtFinalization"] == 1 and report["audit_status"] == "FAIL"


def test_impossible_session_order_fails(tmp_path):
    root = fixture(tmp_path)
    path = next((root / "operations" / "sessions").glob("*/session.json"))
    value = json.loads(path.read_text("utf-8"))
    value["session_end_utc"] = (START - timedelta(seconds=1)).isoformat()
    write_json(path, value)
    assert audit.audit_runtime(root)["audit_status"] == "FAIL"


def test_zero_forceorder_is_not_failure(tmp_path):
    report = audit.audit_runtime(fixture(tmp_path, force_orders=0))
    assert report["ForceOrderRecords"] == 0 and report["audit_status"] == "PASS"


def test_interval_union_does_not_double_count():
    intervals = [(START, START + timedelta(seconds=10)),
                 (START + timedelta(seconds=5), START + timedelta(seconds=20)),
                 (START + timedelta(seconds=30), START + timedelta(seconds=40))]
    merged, seconds = audit._merge_intervals(intervals)
    assert seconds == 30 and len(merged) == 2


def test_168h_wall_span_with_session_downtime_is_not_freeze_eligible(tmp_path):
    root = fixture(tmp_path, seconds=300000)
    add_session(root, "00000000-0000-0000-0000-000000000002",
                START + timedelta(seconds=301000), START + timedelta(seconds=604800))
    report = audit.audit_runtime(root)
    assert report["WallClockSpanSeconds"] == 604800
    assert report["EffectiveObservedCoverageSeconds"] == 603800
    assert report["FreezeEligible"] is False


def test_clean_168h_runtime_is_freeze_eligible(tmp_path):
    report = audit.audit_runtime(fixture(tmp_path, seconds=604800))
    assert report["audit_status"] == "PASS"
    assert report["FreezeEligible"] is True


def test_cli_report_is_deterministic_and_runtime_remains_unchanged(tmp_path):
    root = fixture(tmp_path / "runtime")
    before = {path.relative_to(root): hashlib.sha256(path.read_bytes()).hexdigest()
              for path in root.rglob("*") if path.is_file()}
    first, second = tmp_path / "first.json", tmp_path / "second.json"
    audit.main(["--runtime-dir", str(root), "--output", str(first)])
    audit.main(["--runtime-dir", str(root), "--output", str(second)])
    after = {path.relative_to(root): hashlib.sha256(path.read_bytes()).hexdigest()
             for path in root.rglob("*") if path.is_file()}
    assert first.read_bytes() == second.read_bytes()
    assert before == after
    assert json.loads(first.read_text("utf-8"))["audit_status"] == "PASS"


def test_latest_final_evidence_source_hash_and_content_are_verified(tmp_path):
    root = fixture(tmp_path)
    write_json(root / "soak_summary.json", {"changed_after_finalization": True})
    codes = {item["code"] for item in audit.audit_runtime(root)["issues"]}
    assert "FINAL_SOURCE_HASH_MISMATCH" in codes
    assert "FINAL_COPY_SOURCE_CONTENT_MISMATCH" in codes

