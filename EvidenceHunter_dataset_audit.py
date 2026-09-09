"""Read-only integrity audit for a completed BTC HUNTER collector runtime."""

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path


NOT_EVALUABLE = "NOT_EVALUABLE"
REQUIRED_SPAN_SECONDS = 604800
COVERAGE_METHOD_VERSION = "EXPLICIT_SESSION_INTERVAL_UNION_V1"
DISCLAIMER = (
    "Dataset audit PASS verifies data-integrity and collection-quality conditions only. "
    "It does not prove predictive edge, economic edge, OOS validity, or trade readiness."
)

JSONL_CONTRACTS = {
    "aggTrade": ("raw/aggTrade.jsonl", ("stream", "raw", "collector_received_local_wall_ms",
                                         "collector_received_monotonic_ms", "acquisition_mode")),
    "depth": ("raw/depth.jsonl", ("stream", "raw", "collector_received_local_wall_ms",
                                   "collector_received_monotonic_ms", "acquisition_mode")),
    "forceOrder": ("raw/forceOrder.jsonl", ("stream", "raw", "collector_received_local_wall_ms",
                                             "collector_received_monotonic_ms", "acquisition_mode")),
    "depth_snapshot": ("raw/depth_snapshot.jsonl", ("schema", "rest_request_id", "last_update_id",
                                                     "parse_status", "received_local_wall_ms",
                                                     "received_monotonic_ms")),
    "clock_observations": ("quality/clock_observations.jsonl", ("schema", "observed_local_wall_ms",
                                                                 "observed_monotonic_ms", "availability",
                                                                 "sync_status")),
    "rest_telemetry": ("quality/rest_telemetry.jsonl", ("schema", "rest_request_id", "outcome",
                                                         "requested_local_wall_ms", "requested_monotonic_ms",
                                                         "received_local_wall_ms", "received_monotonic_ms")),
}


def _iso(value):
    if not isinstance(value, str):
        raise ValueError("timestamp is not a string")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timestamp has no timezone")
    return parsed.astimezone(timezone.utc)


def _same_path(left, right):
    return os.path.normcase(str(Path(left).resolve())) == os.path.normcase(str(Path(right).resolve()))


def _sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _merge_intervals(intervals):
    """Return sorted union and total seconds for aware datetime intervals."""
    ordered = sorted((start, end) for start, end in intervals if end > start)
    merged = []
    for start, end in ordered:
        if merged and start <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        else:
            merged.append((start, end))
    return merged, sum((end - start).total_seconds() for start, end in merged)


def _read_json(path, issues, *, required=True):
    path = Path(path)
    if not path.exists():
        if required:
            issues.append({"code": "MISSING_REQUIRED_FILE", "source": str(path)})
        return None
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        issues.append({"code": "MALFORMED_REQUIRED_JSON", "source": str(path),
                       "detail": type(error).__name__})
        return None
    if not isinstance(value, dict):
        issues.append({"code": "REQUIRED_JSON_NOT_OBJECT", "source": str(path)})
        return None
    return value


def _read_jsonl(path, required_fields, issues, time_errors, *, required=True):
    path = Path(path)
    records = []
    malformed = 0
    missing = 0
    if not path.exists():
        if required:
            issues.append({"code": "MISSING_REQUIRED_FILE", "source": str(path)})
        return records, malformed, missing, 0
    byte_size = path.stat().st_size
    try:
        with path.open("r", encoding="utf-8") as handle:
            for line_no, line in enumerate(handle, 1):
                if not line.strip():
                    continue
                try:
                    record = json.loads(line)
                except (UnicodeError, json.JSONDecodeError):
                    malformed += 1
                    issues.append({"code": "MALFORMED_JSONL_RECORD", "source": str(path),
                                   "line": line_no})
                    continue
                if not isinstance(record, dict):
                    malformed += 1
                    issues.append({"code": "JSONL_RECORD_NOT_OBJECT", "source": str(path),
                                   "line": line_no})
                    continue
                absent = [field for field in required_fields if field not in record]
                if absent:
                    missing += 1
                    issues.append({"code": "REQUIRED_RECORD_FIELDS_MISSING", "source": str(path),
                                   "line": line_no, "fields": absent})
                for key in ("collector_received_local_wall_ms", "collector_received_monotonic_ms",
                            "observed_local_wall_ms", "observed_monotonic_ms", "requested_local_wall_ms",
                            "requested_monotonic_ms", "received_local_wall_ms", "received_monotonic_ms"):
                    value = record.get(key)
                    if value is not None and (not isinstance(value, (int, float)) or value < 0):
                        time_errors.append({"code": "INVALID_TIME_VALUE", "source": str(path),
                                            "line": line_no, "field": key})
                for key in ("at", "logged_at", "detection_time", "updated_at", "wall_time_iso"):
                    value = record.get(key)
                    if value is not None:
                        try:
                            _iso(value)
                        except (TypeError, ValueError):
                            time_errors.append({"code": "TIMESTAMP_PARSE_ERROR", "source": str(path),
                                                "line": line_no, "field": key})
                requested = record.get("requested_monotonic_ms")
                received = record.get("received_monotonic_ms")
                if isinstance(requested, (int, float)) and isinstance(received, (int, float)) \
                        and received < requested:
                    time_errors.append({"code": "NEGATIVE_REQUEST_DURATION", "source": str(path),
                                        "line": line_no})
                records.append(record)
    except (OSError, UnicodeError) as error:
        issues.append({"code": "REQUIRED_JSONL_UNREADABLE", "source": str(path),
                       "detail": type(error).__name__})
    return records, malformed, missing, byte_size


def _materialize_gaps(records, issues):
    states = {}
    order = []
    for index, record in enumerate(records, 1):
        gap_id = record.get("gap_id")
        kind = record.get("event_type")
        if not gap_id:
            issues.append({"code": "GAP_EVENT_MISSING_ID", "line": index})
        elif kind == "GAP_OPENED":
            if gap_id in states:
                issues.append({"code": "DUPLICATE_GAP_OPEN", "gap_id": gap_id})
            else:
                states[gap_id] = dict(record)
                order.append(gap_id)
        elif kind == "GAP_UPDATE":
            if gap_id not in states:
                issues.append({"code": "GAP_UPDATE_BEFORE_OPEN", "gap_id": gap_id})
            else:
                states[gap_id].update({key: value for key, value in record.items()
                                       if key != "event_type"})
        else:
            issues.append({"code": "UNKNOWN_GAP_EVENT_TYPE", "line": index})
    return [states[gap_id] for gap_id in order]


def _gap_covers(gaps, component, start, end):
    matches = []
    for gap in gaps:
        if gap.get("component") != component:
            continue
        try:
            if int(gap.get("gap_start")) <= start and int(gap.get("gap_end")) >= end:
                matches.append(gap)
        except (TypeError, ValueError):
            continue
    return matches


def _audit_sessions(root, issues, warnings):
    session_paths = sorted((root / "operations" / "sessions").glob("*/session.json"))
    sessions = []
    identity_consistent = True
    latest_watermark = None
    for path in session_paths:
        record = _read_json(path, issues)
        if record is None:
            continue
        session_id = path.parent.name
        if record.get("collection_session_id") != session_id:
            identity_consistent = False
            issues.append({"code": "SESSION_IDENTITY_MISMATCH", "source": str(path)})
        if not _same_path(record.get("runtime_root", ""), root):
            identity_consistent = False
            issues.append({"code": "SESSION_RUNTIME_ROOT_MISMATCH", "source": str(path)})
        owner = record.get("owner_identity") or {}
        if owner.get("collection_session_id") != session_id or owner.get("dataset_id") != record.get("dataset_id"):
            identity_consistent = False
            issues.append({"code": "SESSION_OWNER_IDENTITY_MISMATCH", "source": str(path)})
        missing_identity = [field for field in ("worker_pid", "creation_filetime", "image_path")
                            if owner.get(field) is None]
        if missing_identity:
            identity_consistent = False
            issues.append({"code": "WORKER_IDENTITY_FIELDS_MISSING", "source": str(path),
                           "fields": missing_identity})
        try:
            start = _iso(record.get("session_start_utc"))
        except (TypeError, ValueError):
            start = None
            issues.append({"code": "SESSION_START_TIME_INVALID", "source": str(path)})
        end = None
        if record.get("session_end_utc") is not None:
            try:
                end = _iso(record["session_end_utc"])
            except (TypeError, ValueError):
                issues.append({"code": "SESSION_END_TIME_INVALID", "source": str(path)})
        if start and end and end < start:
            issues.append({"code": "SESSION_END_BEFORE_START", "source": str(path)})
        for stamp in (start, end):
            if stamp is not None and (latest_watermark is None or stamp > latest_watermark):
                latest_watermark = stamp
        sessions.append({"path": path, "record": record, "start": start, "end": end})

    if not session_paths:
        warnings.append({"code": "NO_SESSION_RECORDS", "source": str(root)})
    datasets = {item["record"].get("dataset_id") for item in sessions}
    if len(datasets) > 1:
        identity_consistent = False
        issues.append({"code": "MULTIPLE_DATASET_IDS", "values": sorted(map(str, datasets))})

    intervals = sorted((item["start"], item["end"], item) for item in sessions
                       if item["start"] is not None and item["end"] is not None)
    for previous, current in zip(intervals, intervals[1:]):
        if current[0] < previous[1]:
            issues.append({"code": "OVERLAPPING_SESSIONS",
                           "first": previous[2]["record"].get("collection_session_id"),
                           "second": current[2]["record"].get("collection_session_id")})

    finalized = 0
    clean = 0
    running = False
    summaries = []
    finalized_items = [item for item in sessions if item["record"].get("status") == "FINALIZED"]
    latest_finalized = max(
        finalized_items,
        key=lambda item: item["end"] or item["start"] or datetime.min.replace(tzinfo=timezone.utc),
        default=None,
    )
    for item in sessions:
        record, path = item["record"], item["path"]
        status = record.get("status")
        if status == "RUNNING":
            running = True
            continue
        if status != "FINALIZED":
            issues.append({"code": "SESSION_NOT_CLEANLY_FINALIZED", "source": str(path),
                           "status": status})
            continue
        finalized += 1
        if record.get("session_end_cleanliness") != "CLEAN":
            issues.append({"code": "SESSION_FINALIZATION_NOT_CLEAN", "source": str(path)})
            continue
        checks = record.get("finalization_checks")
        if not isinstance(checks, dict) or not checks or not all(value is True for value in checks.values()):
            issues.append({"code": "FINALIZATION_CHECK_FAILED", "source": str(path)})
            continue
        cause = record.get("machine_observed_end_cause")
        if cause not in ("GRACEFUL_STOP_CONFIRMED", "NATURAL_DURATION_COMPLETE"):
            issues.append({"code": "UNAPPROVED_SESSION_END_CAUSE", "source": str(path)})
            continue
        if cause == "GRACEFUL_STOP_CONFIRMED":
            ack = record.get("stop_ack") or {}
            if ack.get("target_collection_session_id") != record.get("collection_session_id"):
                issues.append({"code": "MATCHING_STOP_ACK_MISSING", "source": str(path)})
                continue

        evidence_ok = True
        for filename in ("final_checkpoint.json", "final_summary.json"):
            copy_path = path.parent / filename
            copy = _read_json(copy_path, issues)
            binding = (record.get("evidence") or {}).get(filename) or {}
            if copy is None or binding.get("sha256") != (_sha256(copy_path) if copy_path.exists() else None):
                evidence_ok = False
                issues.append({"code": "FINAL_EVIDENCE_HASH_MISMATCH", "source": str(copy_path)})
            if item is latest_finalized:
                source_path = root / ("continuity_checkpoint.json" if filename == "final_checkpoint.json"
                                      else "soak_summary.json")
                source = _read_json(source_path, issues)
                if source is None or binding.get("source_sha256") != (
                        _sha256(source_path) if source_path.exists() else None):
                    evidence_ok = False
                    issues.append({"code": "FINAL_SOURCE_HASH_MISMATCH", "source": str(source_path)})
                if source is not None and copy is not None and source != copy:
                    evidence_ok = False
                    issues.append({"code": "FINAL_COPY_SOURCE_CONTENT_MISMATCH",
                                   "source": str(source_path)})
            if filename == "final_summary.json" and copy is not None:
                summaries.append(copy)
        if evidence_ok:
            clean += 1

    owner = _read_json(root / "operations" / "owner_identity.json", issues,
                       required=bool(sessions))
    if owner and sessions:
        latest = max(sessions, key=lambda item: item["start"] or datetime.min.replace(tzinfo=timezone.utc))
        if owner.get("collection_session_id") != latest["record"].get("collection_session_id") \
                or owner.get("dataset_id") != latest["record"].get("dataset_id") \
                or not _same_path(owner.get("runtime_root", ""), root):
            identity_consistent = False
            issues.append({"code": "CURRENT_OWNER_IDENTITY_MISMATCH",
                           "source": str(root / "operations" / "owner_identity.json")})

    return {
        "sessions": sessions,
        "summaries": summaries,
        "session_count": len(session_paths),
        "finalized_session_count": finalized,
        "clean_session_count": clean,
        "session_identity_consistent": identity_consistent,
        "running": running,
        "watermark": latest_watermark,
    }


def audit_runtime(runtime_root: Path):
    root = Path(runtime_root).resolve()
    issues, warnings, time_errors = [], [], []
    session = _audit_sessions(root, issues, warnings)

    records = {}
    malformed = missing_fields = 0
    sizes = {}
    for name, (relative, required_fields) in JSONL_CONTRACTS.items():
        values, bad, missing, size = _read_jsonl(root / relative, required_fields, issues, time_errors)
        records[name] = values
        malformed += bad
        missing_fields += missing
        sizes[name] = size

    gap_records, bad, missing, size = _read_jsonl(
        root / "quality" / "gap_ledger.jsonl", (), issues, time_errors, required=False)
    malformed += bad
    missing_fields += missing
    sizes["gap_ledger"] = size
    gaps = _materialize_gaps(gap_records, issues)
    alerts, bad, missing, size = _read_jsonl(
        root / "alerts" / "collector_alerts.jsonl", ("component", "new_state", "reason", "at"),
        issues, time_errors, required=False)
    malformed += bad
    missing_fields += missing
    sizes["collector_alerts"] = size

    checkpoint = _read_json(root / "continuity_checkpoint.json", issues,
                            required=not session["running"] and bool(session["sessions"]))
    root_summary = _read_json(root / "soak_summary.json", issues,
                              required=not session["running"] and bool(session["sessions"]))

    agg_ids = set()
    duplicate_agg = 0
    live_regressions = 0
    previous_live = None
    for index, record in enumerate(records["aggTrade"], 1):
        raw = record.get("raw")
        trade_id = raw.get("a") if isinstance(raw, dict) else None
        try:
            trade_id = int(trade_id)
        except (TypeError, ValueError):
            issues.append({"code": "AGGTRADE_ID_MISSING_OR_INVALID", "line": index})
            continue
        if trade_id in agg_ids:
            duplicate_agg += 1
        agg_ids.add(trade_id)
        if record.get("acquisition_mode") == "LIVE":
            if previous_live is not None and trade_id < previous_live:
                live_regressions += 1
            previous_live = trade_id

    missing_ranges = []
    if agg_ids:
        ordered = sorted(agg_ids)
        for left, right in zip(ordered, ordered[1:]):
            if right > left + 1:
                missing_ranges.append([left + 1, right - 1])
    known_agg_ranges = []
    unexplained_agg = live_regressions
    for start, end in missing_ranges:
        matched = _gap_covers(gaps, "aggTrade", start, end)
        if matched:
            known_agg_ranges.append([start, end])
            if any(gap.get("repair_status") == "REPAIRED" for gap in matched):
                issues.append({"code": "REPAIRED_AGGTRADE_GAP_STILL_MISSING_DATA",
                               "range": [start, end]})
        else:
            unexplained_agg += 1
            issues.append({"code": "UNEXPLAINED_AGGTRADE_ID_GAP", "range": [start, end]})
    if duplicate_agg:
        issues.append({"code": "DUPLICATE_AGGTRADE_ID", "count": duplicate_agg})
    if live_regressions:
        issues.append({"code": "AGGTRADE_LIVE_ID_REGRESSION", "count": live_regressions})
    if session["sessions"] and not session["running"] and not agg_ids:
        issues.append({"code": "PRIMARY_STREAM_EMPTY", "stream": "aggTrade"})

    snapshots = []
    for record in records["depth_snapshot"]:
        if record.get("parse_status") == "PARSED" and record.get("last_update_id") is not None:
            try:
                snapshots.append((float(record["received_monotonic_ms"]),
                                  int(record["last_update_id"])))
            except (TypeError, ValueError):
                issues.append({"code": "DEPTH_SNAPSHOT_ID_INVALID"})
    snapshots.sort()
    snapshot_ids = [snapshot_id for _, snapshot_id in snapshots]
    if session["sessions"] and not session["running"] and not snapshot_ids:
        issues.append({"code": "NO_USABLE_DEPTH_SNAPSHOT"})

    depth_events = []
    depth_duplicates = 0
    depth_seen = set()
    for index, record in enumerate(records["depth"], 1):
        raw = record.get("raw")
        try:
            event = (int(raw["U"]), int(raw["u"]), int(raw["pu"]),
                     float(record["collector_received_monotonic_ms"]))
        except (TypeError, ValueError, KeyError):
            issues.append({"code": "DEPTH_IDENTITY_MISSING_OR_INVALID", "line": index})
            continue
        identity = event[:2]
        if identity in depth_seen:
            depth_duplicates += 1
        depth_seen.add(identity)
        depth_events.append(event)
    if depth_duplicates:
        issues.append({"code": "DUPLICATE_DEPTH_IDENTITY", "count": depth_duplicates})
    if session["sessions"] and not session["running"] and not depth_events:
        issues.append({"code": "PRIMARY_STREAM_EMPTY", "stream": "diff_depth"})

    checkpoint_components = (checkpoint or {}).get("components") or {}
    if checkpoint is not None and not session["running"]:
        expected_agg = max(agg_ids) if agg_ids else None
        expected_depth = max((event[1] for event in depth_events), default=None)
        actual_agg = (checkpoint_components.get("aggTrade") or {}).get(
            "last_durably_persisted_trade_id")
        actual_depth = (checkpoint_components.get("diff_depth") or {}).get(
            "last_durably_persisted_update_id")
        if actual_agg != expected_agg:
            issues.append({"code": "AGGTRADE_CHECKPOINT_HIGH_WATER_MISMATCH",
                           "expected": expected_agg, "actual": actual_agg})
        if actual_depth != expected_depth:
            issues.append({"code": "DEPTH_CHECKPOINT_HIGH_WATER_MISMATCH",
                           "expected": expected_depth, "actual": actual_depth})

    unexplained_depth = 0
    known_depth_breaks = 0
    if depth_events and snapshots:
        first_U, first_u, _, first_mono = depth_events[0]
        prior_snapshots = [snapshot_id for mono, snapshot_id in snapshots if mono <= first_mono]
        if not prior_snapshots or not first_U <= prior_snapshots[-1] + 1 <= first_u:
            unexplained_depth += 1
            issues.append({"code": "INITIAL_DEPTH_EVENT_NOT_ANCHORED_TO_SNAPSHOT"})
    for previous, current in zip(depth_events, depth_events[1:]):
        previous_u = previous[1]
        current_U, current_u, current_pu, current_mono = current
        intervening = [snapshot_id for mono, snapshot_id in snapshots
                       if previous[3] < mono <= current_mono]
        if intervening:
            if current_U <= intervening[-1] + 1 <= current_u:
                continue
            unexplained_depth += 1
            issues.append({"code": "DEPTH_EVENT_NOT_ANCHORED_AFTER_RESYNC",
                           "snapshot_last_update_id": intervening[-1],
                           "current_U": current_U, "current_u": current_u})
            continue
        if current_pu == previous_u:
            continue
        matched = _gap_covers(gaps, "diff_depth", previous_u, current_u)
        if matched and any(gap.get("repair_status") in ("REPAIRED", "UNRECOVERABLE_CONFIRMED")
                           for gap in matched):
            known_depth_breaks += 1
        else:
            unexplained_depth += 1
            issues.append({"code": "UNEXPLAINED_DEPTH_CONTINUITY_BREAK",
                           "previous_u": previous_u, "current_pu": current_pu,
                           "current_u": current_u})

    open_gaps = [gap for gap in gaps if gap.get("repair_status") == "OPEN"]
    if open_gaps and not session["running"]:
        issues.append({"code": "UNRESOLVED_GAPS_AT_FINALIZATION", "count": len(open_gaps)})
    unusable_gaps = [gap for gap in gaps
                     if gap.get("repair_status") == "UNRECOVERABLE_CONFIRMED"]

    writer_drops = backlog = sink_failures = reconnects = 0
    required_writers = {"aggTrade", "diff_depth", "forceOrder", "depth_snapshot",
                        "clock_observations", "rest_telemetry"}
    summaries_to_audit = session["summaries"]
    if not summaries_to_audit and not session["running"] and root_summary:
        summaries_to_audit = [root_summary]
    for summary in summaries_to_audit:
        try:
            summary_start = _iso(summary.get("started_at_iso"))
            summary_end = _iso(summary.get("ended_at_iso"))
            summary_duration = float(summary.get("actual_duration_seconds"))
            if summary_end < summary_start or summary_duration < 0:
                raise ValueError("invalid summary time order")
        except (TypeError, ValueError):
            issues.append({"code": "SUMMARY_TIME_EVIDENCE_INVALID"})
        summary_writers = summary.get("writers") or {}
        missing_writers = sorted(required_writers - set(summary_writers))
        if missing_writers:
            issues.append({"code": "SUMMARY_WRITER_EVIDENCE_MISSING", "writers": missing_writers})
        for writer_name, writer in summary_writers.items():
            absent = [key for key in ("dropped_count", "backlog_at_stop") if key not in writer]
            if absent:
                issues.append({"code": "SUMMARY_WRITER_COUNTER_MISSING",
                               "writer": writer_name, "fields": absent})
            writer_drops += int(writer.get("dropped_count") or 0)
            backlog += int(writer.get("backlog_at_stop") or 0)
        for stream_name, stream in (summary.get("streams") or {}).items():
            reconnects += int(stream.get("reconnect_count") or 0)
            if stream.get("final_state") == "LIFECYCLE_THREAD_CRASHED":
                issues.append({"code": "COMPONENT_FINAL_STATE_CRASHED", "stream": stream_name})
        sink_evidence = summary.get("evidence_sink_failures")
        if not isinstance(sink_evidence, dict):
            issues.append({"code": "SUMMARY_SINK_EVIDENCE_MISSING"})
            sink_evidence = {}
        for failures in sink_evidence.values():
            sink_failures += len(failures or [])
    if writer_drops:
        issues.append({"code": "WRITER_DROPS_PRESENT", "count": writer_drops})
    if backlog:
        issues.append({"code": "WRITER_BACKLOG_AT_FINALIZATION", "count": backlog})
    if sink_failures:
        issues.append({"code": "EVIDENCE_SINK_FAILURES_PRESENT", "count": sink_failures})

    network_loss = recovery = 0
    for record in records["rest_telemetry"]:
        for event in record.get("network_route_events") or []:
            network_loss += event.get("event_type") == "CONNECTIVITY_LOST"
            recovery += event.get("event_type") == "CONNECTIVITY_RESTORED"
    fatal_alerts = sum(alert.get("new_state") == "LIFECYCLE_THREAD_CRASHED"
                       or str(alert.get("severity", "")).upper() == "FATAL" for alert in alerts)
    if fatal_alerts:
        issues.append({"code": "FATAL_ALERTS_PRESENT", "count": fatal_alerts})

    available_clocks = sum(record.get("availability") == "AVAILABLE"
                           for record in records["clock_observations"])
    if session["sessions"] and not session["running"] and available_clocks == 0:
        issues.append({"code": "NO_AVAILABLE_CLOCK_OBSERVATION"})

    telemetry_ids = {record.get("rest_request_id") for record in records["rest_telemetry"]}
    uncorrelated_snapshots = sum(
        bool(record.get("rest_request_id")) and record.get("rest_request_id") not in telemetry_ids
        for record in records["depth_snapshot"]
    )
    if uncorrelated_snapshots:
        issues.append({"code": "DEPTH_SNAPSHOT_REST_CORRELATION_MISSING",
                       "count": uncorrelated_snapshots})
    rest_ids = [record.get("rest_request_id") for record in records["rest_telemetry"]
                if record.get("rest_request_id")]
    snapshot_request_ids = [record.get("rest_request_id") for record in records["depth_snapshot"]
                            if record.get("rest_request_id")]
    duplicate_transactions = ((len(rest_ids) - len(set(rest_ids)))
                              + (len(snapshot_request_ids) - len(set(snapshot_request_ids))))
    if duplicate_transactions:
        issues.append({"code": "DUPLICATE_REST_REQUEST_ID", "count": duplicate_transactions})

    if time_errors:
        issues.extend(time_errors)

    finalized_intervals = [(item["start"], item["end"]) for item in session["sessions"]
                           if item["record"].get("status") == "FINALIZED"
                           and item["start"] is not None and item["end"] is not None]
    merged_active, effective_seconds = _merge_intervals(finalized_intervals)
    wall_span = NOT_EVALUABLE
    known_intervals = []
    known_unusable_seconds = 0.0
    largest_gap = 0.0
    if merged_active:
        wall_span = (merged_active[-1][1] - merged_active[0][0]).total_seconds()
        for (_, left_end), (right_start, _) in zip(merged_active, merged_active[1:]):
            seconds = (right_start - left_end).total_seconds()
            if seconds > 0:
                known_intervals.append({"start_utc": left_end.isoformat(),
                                        "end_utc": right_start.isoformat(),
                                        "seconds": seconds,
                                        "source": "operations/sessions/*/session.json"})
        known_unusable_seconds = sum(item["seconds"] for item in known_intervals)
        largest_gap = max((item["seconds"] for item in known_intervals), default=0.0)
    coverage_unknown_gaps = [
        gap for gap in gaps
        if gap.get("repair_status") == "UNRECOVERABLE_CONFIRMED"
        or gap.get("component") == "diff_depth"
    ]
    if coverage_unknown_gaps:
        effective_seconds = NOT_EVALUABLE
        known_unusable_seconds = NOT_EVALUABLE
        largest_gap = NOT_EVALUABLE
        warnings.append({
            "code": "GAP_DURATION_NOT_EVALUABLE",
            "detail": "gap ledger persists identifier ranges and detection time, not exact unusable wall-clock intervals",
            "gap_ids": [gap.get("gap_id") for gap in coverage_unknown_gaps],
        })

    incomplete = session["running"] or not session["sessions"]
    hard_failure = bool(issues)
    known_gap_condition = bool(unusable_gaps or known_intervals or known_depth_breaks
                               or known_agg_ranges
                               or any(gap.get("component") == "diff_depth" for gap in gaps))
    if hard_failure:
        audit_status = "FAIL"
    elif incomplete:
        audit_status = "INCOMPLETE"
    elif known_gap_condition:
        audit_status = "PASS_WITH_GAPS"
    else:
        audit_status = "PASS"

    if audit_status in ("FAIL", "INCOMPLETE"):
        freeze_eligible = False
    elif effective_seconds == NOT_EVALUABLE or wall_span == NOT_EVALUABLE:
        freeze_eligible = NOT_EVALUABLE
    else:
        freeze_eligible = (wall_span >= REQUIRED_SPAN_SECONDS
                           and effective_seconds >= REQUIRED_SPAN_SECONDS)

    dataset_ids = {item["record"].get("dataset_id") for item in session["sessions"]
                   if item["record"].get("dataset_id") is not None}
    dataset_id = next(iter(dataset_ids)) if len(dataset_ids) == 1 else NOT_EVALUABLE
    generated = session["watermark"].isoformat() if session["watermark"] else NOT_EVALUABLE
    finalization_status = (
        "INCOMPLETE" if incomplete else
        "CLEAN" if session["clean_session_count"] == session["session_count"] else "INVALID"
    )
    orderbook_status = ("FAIL" if unexplained_depth else
                        "PASS_WITH_GAPS" if known_depth_breaks or any(
                            gap.get("component") == "diff_depth" for gap in gaps) else
                        "PASS" if depth_events and snapshot_ids else NOT_EVALUABLE)
    time_issue_codes = {"SESSION_START_TIME_INVALID", "SESSION_END_TIME_INVALID",
                        "SESSION_END_BEFORE_START", "SUMMARY_TIME_EVIDENCE_INVALID"}
    time_status = "FAIL" if time_errors or any(issue["code"] in time_issue_codes for issue in issues) else "PASS"
    operational_status = "FAIL" if writer_drops or backlog or sink_failures or fatal_alerts else "PASS"

    return {
        "schema_version": "DATASET_AUDIT_REPORT_V1",
        "dataset_id": dataset_id,
        "runtime_root": str(root),
        "audit_generated_at_utc": generated,
        "audit_status": audit_status,
        "WallClockSpanSeconds": wall_span,
        "EffectiveObservedCoverageSeconds": effective_seconds,
        "CoverageMethodVersion": COVERAGE_METHOD_VERSION,
        "RequiredWallClockSpanSeconds": REQUIRED_SPAN_SECONDS,
        "RequiredEffectiveObservedCoverageSeconds": REQUIRED_SPAN_SECONDS,
        "KnownUnusableIntervals": known_intervals,
        "KnownUnusableSeconds": known_unusable_seconds,
        "LargestKnownGapSeconds": largest_gap,
        "AggTradeRecords": len(records["aggTrade"]),
        "DepthRecords": len(records["depth"]),
        "ForceOrderRecords": len(records["forceOrder"]),
        "DepthSnapshotRecords": len(records["depth_snapshot"]),
        "MalformedRecords": malformed + missing_fields,
        "DuplicateRecords": duplicate_agg + depth_duplicates + duplicate_transactions,
        "GapCount": len(gaps),
        "aggtrade_first_id": min(agg_ids) if agg_ids else None,
        "aggtrade_last_id": max(agg_ids) if agg_ids else None,
        "aggtrade_duplicate_count": duplicate_agg,
        "aggtrade_missing_ranges": missing_ranges,
        "aggtrade_unexplained_continuity_errors": unexplained_agg,
        "snapshot_count": len(snapshot_ids),
        "resync_count": sum(int(((summary.get("streams") or {}).get("diff_depth") or {})
                                .get("resync_count") or 0) for summary in session["summaries"]),
        "known_depth_gaps": sum(gap.get("component") == "diff_depth" for gap in gaps),
        "unexplained_depth_continuity_errors": unexplained_depth,
        "OrderBookContinuityStatus": orderbook_status,
        "orderbook_continuity_status": orderbook_status,
        "clock_observation_count": len(records["clock_observations"]),
        "clock_available_count": available_clocks,
        "timestamp_parse_errors": sum(issue["code"] in {
            "TIMESTAMP_PARSE_ERROR", "SESSION_START_TIME_INVALID", "SESSION_END_TIME_INVALID",
            "SUMMARY_TIME_EVIDENCE_INVALID",
        } for issue in issues),
        "time_order_errors": len(time_errors),
        "time_quality_status": time_status,
        "ReconnectCount": reconnects,
        "NetworkLossCount": network_loss,
        "RecoveryCount": recovery,
        "network_event_count_scope": "REST_TELEMETRY_PERSISTED_ROUTE_EVENTS_ONLY",
        "WriterDrops": writer_drops,
        "BacklogAtFinalization": backlog,
        "EvidenceSinkFailures": sink_failures,
        "FatalAlerts": fatal_alerts,
        "operational_quality_status": operational_status,
        "session_count": session["session_count"],
        "finalized_session_count": session["finalized_session_count"],
        "clean_session_count": session["clean_session_count"],
        "session_identity_consistent": session["session_identity_consistent"],
        "FinalizationStatus": finalization_status,
        "finalization_status": finalization_status,
        "FreezeEligible": freeze_eligible,
        "file_bytes": sizes,
        "issues": issues,
        "warnings": warnings,
        "disclaimer": DISCLAIMER,
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description="Read-only BTC HUNTER Dataset audit V1")
    parser.add_argument("--runtime-dir", required=True, type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    runtime = args.runtime_dir.resolve()
    if args.output is not None:
        output = args.output.resolve()
        try:
            output.relative_to(runtime)
        except ValueError:
            pass
        else:
            parser.error("--output must be outside --runtime-dir")
    report = audit_runtime(runtime)
    encoded = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if args.output is None:
        print(encoded, end="")
    else:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(encoded, encoding="utf-8")
    return report


if __name__ == "__main__":
    main()
