# -*- coding: utf-8 -*-
"""BTC HUNTER collector readiness validator (NEXT_DATASET_COLLECTOR_READINESS).

This module answers exactly one question: is the collector implemented under
NEXT_DATASET_COLLECTOR_IMPLEMENTATION (CLOSED, commit 4d5446a) qualified to
start a formal Dataset? It does NOT collect data, does NOT create a Dataset,
and does NOT change any collector behaviour.

READ-ONLY CONTRACT
------------------
Every function here only reads evidence (gap ledger JSONL, raw payload JSONL,
soak summary JSON, process sample JSON). Nothing in this module ever mutates,
truncates, repairs or re-writes collected evidence. The single file it may
write is its own output report, and only when an explicit output path is
given on the command line.

GATES
-----
G1 THREAD_CRASH_OBSERVABILITY   supplied (pytest evidence)      -> validated shape only
G2 GAP_LEDGER_SEMANTICS         computed here from the ledger
G3 REPAIR_RESYNC_PROVENANCE     computed here from ledger + raw
G4 60_MINUTE_SOAK               computed here from soak summary + exit-status evidence
G5 STORAGE_THROUGHPUT_MEASURED  computed here from soak summary + process/resource samples
G6 PROCESS_RESTART_RECOVERY     supplied (restart-test evidence) -> validated shape only
G7 ENVIRONMENT_DEPENDENCIES_FROZEN supplied (manifest evidence)  -> validated shape only
G8 FORMAL_DATASET_STARTUP_GUARD folds G1..G7 into exactly one conclusion

The final conclusion is exactly one of READINESS = PASS or READINESS = FAIL
with a non-empty BLOCKERS list. There is deliberately no third, ambiguous
state: a gate that could not be evaluated is FAIL, never "unknown".
"""

import argparse
import json
import math
from datetime import datetime
from pathlib import Path

import EvidenceHunter_gap_ledger as gap_ledger

READINESS_REPORT_SCHEMA = "COLLECTOR_READINESS_REPORT_V1"
SOAK_SUMMARY_SCHEMA = "COLLECTOR_SOAK_SUMMARY_V1"
PROCESS_SAMPLES_SCHEMA = "COLLECTOR_PROCESS_SAMPLES_V1"
PROCESS_EXIT_EVIDENCE_SCHEMA = "COLLECTOR_PROCESS_EXIT_EVIDENCE_V1"

REQUIRED_GATES = (
    "G1_THREAD_CRASH_OBSERVABILITY",
    "G2_GAP_LEDGER_SEMANTICS",
    "G3_REPAIR_RESYNC_PROVENANCE",
    "G4_60_MINUTE_SOAK",
    "G5_STORAGE_THROUGHPUT_MEASURED",
    "G6_PROCESS_RESTART_RECOVERY",
    "G7_ENVIRONMENT_DEPENDENCIES_FROZEN",
    "G8_FORMAL_DATASET_STARTUP_GUARD",
)

SUPPLIED_GATES = (
    "G1_THREAD_CRASH_OBSERVABILITY",
    "G6_PROCESS_RESTART_RECOVERY",
    "G7_ENVIRONMENT_DEPENDENCIES_FROZEN",
)

SEVERITY_BLOCKING = "BLOCKING"
SEVERITY_NON_BLOCKING = "NON_BLOCKING"

STATUS_PASS = "PASS"
STATUS_FAIL = "FAIL"

# A windowed-snapshot stream (forceOrder) has no sequential identifier, so no
# continuity gap can ever be *derived* for it. A gap record naming it would
# mean the collector inferred "no message => missing data", which is exactly
# the inference the project forbids.
NON_SEQUENTIAL_COMPONENTS = ("forceOrder",)

SOAK_REQUIRED_DURATION_SECONDS = 3600.0
SOAK_DURATION_TOLERANCE_SECONDS = 120.0
MIN_PROCESS_SAMPLES = 4
# Deliberately coarse: this is a runaway detector, not a capacity threshold.
# The raw samples are always recorded in the report so a human can judge.
RSS_RUNAWAY_GROWTH_RATIO = 2.0


class ReadinessError(RuntimeError):
    pass


def _finding(code, severity, detail, **extra):
    finding = {"code": code, "severity": severity, "detail": detail}
    if extra:
        finding.update(extra)
    return finding


def _status_from(findings):
    blocking = [f for f in findings if f["severity"] == SEVERITY_BLOCKING]
    return STATUS_FAIL if blocking else STATUS_PASS


def _gate(name, findings, stats):
    return {
        "gate": name,
        "status": _status_from(findings),
        "findings": findings,
        "stats": stats,
    }


def _is_number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _parse_iso(value):
    try:
        datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return True
    except (TypeError, ValueError):
        return False


# ---------------------------------------------------------------------------
# G2 - gap ledger semantic validation
# ---------------------------------------------------------------------------

def validate_gap_ledger_semantics(ledger_path):
    """Every gap record must be internally coherent and unambiguous.

    This gate is about SEMANTIC VALIDITY, not about the number of gaps: a
    correctly recorded, still-OPEN gap is valid evidence, whereas a REPAIRED
    gap with no repair_source is a contract violation regardless of count.
    """
    findings = []
    ledger_path = Path(ledger_path)
    stats = {
        "ledger_path": str(ledger_path),
        "ledger_exists": ledger_path.exists(),
        "event_count": 0,
        "gap_count": 0,
        "by_component": {},
        "by_repair_status": {},
        "by_recoverability": {},
        "by_resync_status": {},
        "open_at_end_count": 0,
    }

    if not ledger_path.exists():
        # An absent ledger is only acceptable if the run genuinely recorded
        # nothing; the caller cross-checks that against the soak summary. On
        # its own an absent file cannot be validated, so it is not a PASS.
        findings.append(_finding(
            "GAP_LEDGER_MISSING", SEVERITY_BLOCKING,
            f"gap ledger file does not exist: {ledger_path}",
        ))
        return _gate("G2_GAP_LEDGER_SEMANTICS", findings, stats)

    try:
        events = gap_ledger.read_events(ledger_path)
    except gap_ledger.GapLedgerError as error:
        findings.append(_finding(
            "GAP_LEDGER_CORRUPT", SEVERITY_BLOCKING, str(error),
        ))
        return _gate("G2_GAP_LEDGER_SEMANTICS", findings, stats)
    stats["event_count"] = len(events)

    try:
        gaps = gap_ledger.materialize_latest(ledger_path)
    except gap_ledger.GapLedgerError as error:
        # e.g. GAP_UPDATE_BEFORE_OPEN / UNKNOWN_GAP_EVENT_TYPE
        findings.append(_finding(
            "GAP_LEDGER_EVENT_ORDER_INVALID", SEVERITY_BLOCKING, str(error),
        ))
        return _gate("G2_GAP_LEDGER_SEMANTICS", findings, stats)
    stats["gap_count"] = len(gaps)

    seen_ids = set()
    for event in events:
        if event.get("event_type") == "GAP_OPENED":
            gap_id = event.get("gap_id")
            if gap_id in seen_ids:
                findings.append(_finding(
                    "DUPLICATE_GAP_OPENED", SEVERITY_BLOCKING,
                    f"gap_id opened more than once: {gap_id}", gap_id=gap_id,
                ))
            seen_ids.add(gap_id)

    for gap in gaps:
        gap_id = gap.get("gap_id")
        component = gap.get("component")
        stats["by_component"][component] = stats["by_component"].get(component, 0) + 1

        missing = [f for f in gap_ledger.REQUIRED_OPEN_FIELDS if f not in gap]
        if missing:
            findings.append(_finding(
                "GAP_MISSING_REQUIRED_FIELDS", SEVERITY_BLOCKING,
                f"gap {gap_id} missing required fields: {missing}", gap_id=gap_id,
            ))

        recoverability = gap.get("recoverability")
        repair_status = gap.get("repair_status")
        resync_status = gap.get("resync_status")
        stats["by_repair_status"][repair_status] = stats["by_repair_status"].get(repair_status, 0) + 1
        stats["by_recoverability"][recoverability] = stats["by_recoverability"].get(recoverability, 0) + 1
        stats["by_resync_status"][resync_status] = stats["by_resync_status"].get(resync_status, 0) + 1

        if recoverability not in gap_ledger.RECOVERABILITY_VALUES:
            findings.append(_finding(
                "INVALID_RECOVERABILITY", SEVERITY_BLOCKING,
                f"gap {gap_id} recoverability={recoverability!r}", gap_id=gap_id,
            ))
        if repair_status not in gap_ledger.REPAIR_STATUS_VALUES:
            findings.append(_finding(
                "INVALID_REPAIR_STATUS", SEVERITY_BLOCKING,
                f"gap {gap_id} repair_status={repair_status!r}", gap_id=gap_id,
            ))
        if resync_status not in gap_ledger.RESYNC_STATUS_VALUES:
            findings.append(_finding(
                "INVALID_RESYNC_STATUS", SEVERITY_BLOCKING,
                f"gap {gap_id} resync_status={resync_status!r}", gap_id=gap_id,
            ))

        if not _parse_iso(gap.get("detection_time")):
            findings.append(_finding(
                "INVALID_DETECTION_TIME", SEVERITY_BLOCKING,
                f"gap {gap_id} detection_time is not ISO-8601: {gap.get('detection_time')!r}",
                gap_id=gap_id,
            ))

        attempts = gap.get("reconnect_attempts")
        if not _is_number(attempts) or attempts < 0:
            findings.append(_finding(
                "INVALID_RECONNECT_ATTEMPTS", SEVERITY_BLOCKING,
                f"gap {gap_id} reconnect_attempts={attempts!r}", gap_id=gap_id,
            ))

        start, end = gap.get("gap_start"), gap.get("gap_end")
        if _is_number(start) and _is_number(end) and start > end:
            findings.append(_finding(
                "GAP_RANGE_INVERTED", SEVERITY_BLOCKING,
                f"gap {gap_id} gap_start={start} > gap_end={end}", gap_id=gap_id,
            ))

        last_good = gap.get("last_good_identifier")
        if gap.get("reason") == "AGGTRADE_ID_SEQUENCE_GAP" and _is_number(last_good) and _is_number(start):
            if start != last_good + 1:
                findings.append(_finding(
                    "AGGTRADE_GAP_START_INCONSISTENT", SEVERITY_BLOCKING,
                    f"gap {gap_id} gap_start={start} != last_good_identifier+1={last_good + 1}",
                    gap_id=gap_id,
                ))

        if component in NON_SEQUENTIAL_COMPONENTS:
            findings.append(_finding(
                "CONTINUITY_GAP_DERIVED_FOR_NON_SEQUENTIAL_STREAM", SEVERITY_BLOCKING,
                f"gap {gap_id} was recorded for {component}, a windowed-snapshot stream "
                "with no sequential identifier -- absence of a message must never be "
                "treated as evidence of missing data",
                gap_id=gap_id, component=component,
            ))

        if repair_status == "OPEN":
            stats["open_at_end_count"] += 1

    if stats["open_at_end_count"]:
        findings.append(_finding(
            "GAPS_STILL_OPEN_AT_END_OF_RUN", SEVERITY_NON_BLOCKING,
            f"{stats['open_at_end_count']} gap(s) were still OPEN when the run ended. "
            "This is a valid ledger state (e.g. a gap opened shortly before shutdown), "
            "not a semantic violation, but it is surfaced for human review.",
        ))

    return _gate("G2_GAP_LEDGER_SEMANTICS", findings, stats)


# ---------------------------------------------------------------------------
# G3 - repair / resync provenance validation
# ---------------------------------------------------------------------------

RAW_STREAM_FILES = {
    "aggTrade": "aggTrade.jsonl",
    "depth": "depth.jsonl",
    "forceOrder": "forceOrder.jsonl",
}

REPAIR_PROVENANCE_FIELDS = ("original_gap_id", "repaired_at", "source_provenance")


def _iter_raw_records(raw_dir):
    """Yield (stream, line_no, record) for every raw payload record."""
    raw_dir = Path(raw_dir)
    for stream, filename in RAW_STREAM_FILES.items():
        path = raw_dir / filename
        if not path.exists():
            continue
        with path.open("r", encoding="utf-8") as f:
            for line_no, line in enumerate(f, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    yield stream, line_no, json.loads(line)
                except json.JSONDecodeError as error:
                    raise ReadinessError(
                        f"CORRUPT_RAW_PAYLOAD_LINE stream={stream} path={path} line_no={line_no}"
                    ) from error


def validate_repair_resync_provenance(ledger_path, raw_dir):
    """Every claim of repair must be backed by provenance, and every non-LIVE
    raw record must say exactly where it came from and which gap it repairs."""
    findings = []
    stats = {
        "raw_dir": str(raw_dir),
        "raw_record_count": 0,
        "by_acquisition_mode": {},
        "repaired_gap_count": 0,
        "unrecoverable_gap_count": 0,
        "backfill_records_by_gap_id": {},
    }

    try:
        gaps = gap_ledger.materialize_latest(ledger_path) if Path(ledger_path).exists() else []
    except gap_ledger.GapLedgerError as error:
        findings.append(_finding("GAP_LEDGER_UNREADABLE", SEVERITY_BLOCKING, str(error)))
        return _gate("G3_REPAIR_RESYNC_PROVENANCE", findings, stats)

    gaps_by_id = {g.get("gap_id"): g for g in gaps}
    backfilled_ids = {}

    for gap in gaps:
        gap_id = gap.get("gap_id")
        repair_status = gap.get("repair_status")

        if repair_status == "REPAIRED":
            stats["repaired_gap_count"] += 1
            if not gap.get("repair_source"):
                findings.append(_finding(
                    "REPAIRED_GAP_WITHOUT_REPAIR_SOURCE", SEVERITY_BLOCKING,
                    f"gap {gap_id} is REPAIRED but has no repair_source", gap_id=gap_id,
                ))
            if gap.get("component") == "diff_depth" and gap.get("resync_status") != "COMPLETED":
                findings.append(_finding(
                    "REPAIRED_DEPTH_GAP_RESYNC_NOT_COMPLETED", SEVERITY_BLOCKING,
                    f"gap {gap_id} is REPAIRED but resync_status={gap.get('resync_status')!r}",
                    gap_id=gap_id,
                ))

        elif repair_status == "UNRECOVERABLE_CONFIRMED":
            stats["unrecoverable_gap_count"] += 1
            if gap.get("recoverability") != "UNRECOVERABLE":
                findings.append(_finding(
                    "UNRECOVERABLE_GAP_RECOVERABILITY_MISMATCH", SEVERITY_BLOCKING,
                    f"gap {gap_id} is UNRECOVERABLE_CONFIRMED but "
                    f"recoverability={gap.get('recoverability')!r}", gap_id=gap_id,
                ))
            if not gap.get("unrecoverable_detail"):
                findings.append(_finding(
                    "UNRECOVERABLE_GAP_WITHOUT_DETAIL", SEVERITY_BLOCKING,
                    f"gap {gap_id} is UNRECOVERABLE_CONFIRMED but records no "
                    "unrecoverable_detail explaining why", gap_id=gap_id,
                ))

    try:
        for stream, line_no, record in _iter_raw_records(raw_dir):
            stats["raw_record_count"] += 1
            mode = record.get("acquisition_mode")
            stats["by_acquisition_mode"][mode] = stats["by_acquisition_mode"].get(mode, 0) + 1

            if mode not in gap_ledger.ACQUISITION_MODES:
                findings.append(_finding(
                    "RAW_RECORD_INVALID_ACQUISITION_MODE", SEVERITY_BLOCKING,
                    f"{stream}.jsonl line {line_no}: acquisition_mode={mode!r} "
                    f"not in {gap_ledger.ACQUISITION_MODES}",
                    stream=stream, line_no=line_no,
                ))
                continue

            if mode == "LIVE":
                continue

            missing = [f for f in REPAIR_PROVENANCE_FIELDS if not record.get(f)]
            if missing:
                findings.append(_finding(
                    "BACKFILL_RECORD_MISSING_PROVENANCE", SEVERITY_BLOCKING,
                    f"{stream}.jsonl line {line_no}: acquisition_mode={mode} but "
                    f"missing provenance fields {missing}",
                    stream=stream, line_no=line_no,
                ))
                continue

            gap_id = record.get("original_gap_id")
            stats["backfill_records_by_gap_id"][gap_id] = \
                stats["backfill_records_by_gap_id"].get(gap_id, 0) + 1
            identifier = (record.get("raw") or {}).get("a")
            if identifier is not None:
                backfilled_ids.setdefault(gap_id, set()).add(int(identifier))
            if gap_id not in gaps_by_id:
                findings.append(_finding(
                    "BACKFILL_RECORD_REFERENCES_UNKNOWN_GAP", SEVERITY_BLOCKING,
                    f"{stream}.jsonl line {line_no}: original_gap_id={gap_id!r} "
                    "does not exist in the gap ledger",
                    stream=stream, line_no=line_no, gap_id=gap_id,
                ))
    except ReadinessError as error:
        findings.append(_finding("RAW_PAYLOAD_CORRUPT", SEVERITY_BLOCKING, str(error)))

    # A gap claimed REPAIRED over a genuinely non-empty identifier range, with
    # zero backfilled records carrying its gap_id, is a provenance hole: the
    # repair is asserted but nothing supports it.
    for gap in gaps:
        gap_id = gap.get("gap_id")
        if gap.get("repair_status") != "REPAIRED":
            continue
        if gap.get("repair_source") != "REST_AGGTRADES_FROM_ID":
            continue  # depth resync repairs are evidenced by the book, not by rows
        start, end = gap.get("gap_start"), gap.get("gap_end")
        if not (_is_number(start) and _is_number(end) and end >= start):
            continue
        if stats["backfill_records_by_gap_id"].get(gap_id, 0) == 0:
            findings.append(_finding(
                "REPAIRED_GAP_WITHOUT_BACKFILL_EVIDENCE", SEVERITY_BLOCKING,
                f"gap {gap_id} claims REST-backfill repair of the non-empty range "
                f"[{start}, {end}] but no raw record carries original_gap_id={gap_id}",
                gap_id=gap_id,
            ))
            continue

        # Presence is not completeness. A gap marked REPAIRED must have every
        # identifier in its claimed range actually present in the raw
        # evidence -- a partially filled gap that says REPAIRED silently ends
        # the investigation into the rest of the range.
        covered = backfilled_ids.get(gap_id, set())
        expected = set(range(int(start), int(end) + 1))
        missing = expected - covered
        stats.setdefault("backfill_coverage", {})[gap_id] = {
            "expected_count": len(expected),
            "covered_count": len(expected & covered),
            "missing_count": len(missing),
        }
        if missing:
            sample = sorted(missing)[:10]
            findings.append(_finding(
                "REPAIRED_GAP_BACKFILL_INCOMPLETE", SEVERITY_BLOCKING,
                f"gap {gap_id} is marked REPAIRED over [{start}, {end}] but "
                f"{len(missing)} of {len(expected)} identifier(s) have no backfilled "
                f"raw record (first missing: {sample})",
                gap_id=gap_id,
            ))

    return _gate("G3_REPAIR_RESYNC_PROVENANCE", findings, stats)


# ---------------------------------------------------------------------------
# G4 - bounded 60-minute soak
# ---------------------------------------------------------------------------

def analyse_process_samples(samples):
    """Summarise RSS / private-memory / CPU samples. Deliberately reports
    numbers rather than applying a capacity threshold: the runaway check asks
    only whether resource usage shows an OBVIOUS monotonic runaway.

    Private-memory constancy is reported here too (all_identical / distinct
    count) but is deliberately NOT itself a runaway/capacity judgment -- a
    process that is genuinely idle some of the time could show flat private
    memory. Whether flat-plus-zero-CPU is suspicious is decided by the
    caller, which can cross-check it against how much work the collector
    actually did (see validate_storage_throughput).
    """
    values = [s.get("working_set_bytes") for s in samples if _is_number(s.get("working_set_bytes"))]
    cpu = [s.get("cpu_total_seconds") for s in samples if _is_number(s.get("cpu_total_seconds"))]
    priv = [s.get("private_memory_bytes") for s in samples if _is_number(s.get("private_memory_bytes"))]
    result = {
        "sample_count": len(samples),
        "rss_sample_count": len(values),
        "cpu_sample_count": len(cpu),
        "rss_first_bytes": values[0] if values else None,
        "rss_last_bytes": values[-1] if values else None,
        "rss_max_bytes": max(values) if values else None,
        "rss_min_bytes": min(values) if values else None,
        "cpu_total_seconds_delta": (cpu[-1] - cpu[0]) if len(cpu) >= 2 else None,
        "rss_strictly_monotonic_increasing": None,
        "rss_first_quarter_mean_bytes": None,
        "rss_last_quarter_mean_bytes": None,
        "rss_growth_ratio": None,
        "obvious_monotonic_runaway": None,
        "runaway_growth_ratio_threshold": RSS_RUNAWAY_GROWTH_RATIO,
        "private_memory_sample_count": len(priv),
        "private_memory_first_bytes": priv[0] if priv else None,
        "private_memory_last_bytes": priv[-1] if priv else None,
        "private_memory_distinct_count": len(set(priv)) if priv else None,
        "private_memory_all_identical": (len(set(priv)) == 1) if len(priv) >= 2 else None,
    }
    if len(values) < MIN_PROCESS_SAMPLES:
        return result

    quarter = max(1, len(values) // 4)
    first_mean = sum(values[:quarter]) / quarter
    last_mean = sum(values[-quarter:]) / quarter
    strictly_increasing = all(values[i + 1] > values[i] for i in range(len(values) - 1))
    ratio = (last_mean / first_mean) if first_mean else None

    result["rss_first_quarter_mean_bytes"] = first_mean
    result["rss_last_quarter_mean_bytes"] = last_mean
    result["rss_strictly_monotonic_increasing"] = strictly_increasing
    result["rss_growth_ratio"] = ratio
    result["obvious_monotonic_runaway"] = bool(
        strictly_increasing and ratio is not None and ratio > RSS_RUNAWAY_GROWTH_RATIO
    )
    return result


def validate_soak_execution(summary, exit_evidence=None):
    """G4 -- the single bounded 60-minute real-network soak actually ran to
    length, the core streams really delivered data, nothing crashed, the
    writer kept pace, no queue grew without bound, and the run's own exit
    status was actually captured (not merely assumed from a clean-looking log
    tail).

    `collector_runtime_behavior` in stats is reported SEPARATELY from the
    gate's overall status: it answers "did the collector itself behave",
    independent of whether every piece of REQUIRED verification evidence
    (such as a verified process exit code) was actually captured by the
    harness running it. A gate can be collector_runtime_behavior=PASS and
    still FAIL overall if required evidence about that run was not obtained
    -- that is a gap in what was proven, not a claim that the collector broke.

    Resource measurement (RSS/CPU/private-memory) is NOT this gate's concern
    -- it lives entirely in G5 (STORAGE_THROUGHPUT_MEASURED), because a
    resource-sample defect is a measurement-tooling problem, not a soak
    duration/health problem.
    """
    findings = []
    runtime_findings = []  # duration/stream/writer/lifecycle only
    stats = {}

    if summary is None:
        findings.append(_finding(
            "SOAK_SUMMARY_MISSING", SEVERITY_BLOCKING,
            "no soak summary was supplied; the 60-minute soak cannot be evaluated",
        ))
        stats["collector_runtime_behavior"] = STATUS_FAIL
        return _gate("G4_60_MINUTE_SOAK", findings, stats)

    if summary.get("schema") != SOAK_SUMMARY_SCHEMA:
        findings.append(_finding(
            "SOAK_SUMMARY_SCHEMA_MISMATCH", SEVERITY_BLOCKING,
            f"expected schema {SOAK_SUMMARY_SCHEMA}, got {summary.get('schema')!r}",
        ))
        stats["collector_runtime_behavior"] = STATUS_FAIL
        return _gate("G4_60_MINUTE_SOAK", findings, stats)

    duration = summary.get("actual_duration_seconds")
    stats["actual_duration_seconds"] = duration
    stats["planned_duration_seconds"] = summary.get("planned_duration_seconds")
    if not _is_number(duration):
        runtime_findings.append(_finding(
            "SOAK_DURATION_NOT_RECORDED", SEVERITY_BLOCKING,
            f"actual_duration_seconds={duration!r}",
        ))
    elif duration < SOAK_REQUIRED_DURATION_SECONDS - SOAK_DURATION_TOLERANCE_SECONDS:
        runtime_findings.append(_finding(
            "SOAK_TOO_SHORT", SEVERITY_BLOCKING,
            f"soak ran {duration:.1f}s, below the required "
            f"{SOAK_REQUIRED_DURATION_SECONDS:.0f}s (tolerance "
            f"{SOAK_DURATION_TOLERANCE_SECONDS:.0f}s)",
        ))

    streams = summary.get("streams") or {}
    stats["streams"] = {}
    for name in ("aggTrade", "diff_depth", "forceOrder"):
        info = streams.get(name)
        if info is None:
            runtime_findings.append(_finding(
                "SOAK_STREAM_MISSING", SEVERITY_BLOCKING,
                f"soak summary has no entry for stream {name}", stream=name,
            ))
            continue
        stats["streams"][name] = {
            "message_count": info.get("message_count"),
            "final_state": info.get("final_state"),
            "reconnect_count": info.get("reconnect_count"),
            "stale_events": info.get("stale_events"),
        }

        if info.get("final_state") == "LIFECYCLE_THREAD_CRASHED":
            runtime_findings.append(_finding(
                "SOAK_COMPONENT_LIFECYCLE_THREAD_CRASHED", SEVERITY_BLOCKING,
                f"component {name} ended the soak in LIFECYCLE_THREAD_CRASHED",
                stream=name,
            ))

        # forceOrder is a windowed-snapshot stream: zero messages in an hour is
        # a legitimate market outcome, never a failure. Only the two continuous
        # streams must actually have delivered data.
        if name in ("aggTrade", "diff_depth"):
            if info.get("final_state") != "HEALTHY":
                runtime_findings.append(_finding(
                    "SOAK_CORE_STREAM_NOT_HEALTHY_AT_END", SEVERITY_BLOCKING,
                    f"component {name} ended the soak in state "
                    f"{info.get('final_state')!r}, expected HEALTHY", stream=name,
                ))
            if not _is_number(info.get("message_count")) or info.get("message_count") <= 0:
                runtime_findings.append(_finding(
                    "SOAK_CORE_STREAM_NO_MESSAGES", SEVERITY_BLOCKING,
                    f"component {name} received {info.get('message_count')!r} messages",
                    stream=name,
                ))

    writers = summary.get("writers") or {}
    stats["writers"] = {}
    if not writers:
        runtime_findings.append(_finding(
            "SOAK_WRITER_METRICS_MISSING", SEVERITY_BLOCKING,
            "soak summary records no writer metrics, so queue growth and writer "
            "progress cannot be evaluated",
        ))
    for name, info in sorted(writers.items()):
        maxsize = info.get("queue_maxsize")
        peak = info.get("queue_depth_peak")
        backlog = info.get("backlog_at_stop")
        dropped = info.get("dropped_count")
        stats["writers"][name] = {
            "written_count": info.get("written_count"),
            "dropped_count": dropped,
            "queue_maxsize": maxsize,
            "queue_depth_peak": peak,
            "backlog_at_stop": backlog,
        }
        if _is_number(dropped) and dropped > 0:
            runtime_findings.append(_finding(
                "SOAK_WRITER_DROPPED_RECORDS", SEVERITY_BLOCKING,
                f"writer {name} dropped {dropped} record(s): the queue saturated "
                "and evidence was lost", writer=name,
            ))
        if _is_number(peak) and _is_number(maxsize) and maxsize > 0 and peak >= maxsize:
            runtime_findings.append(_finding(
                "SOAK_WRITER_QUEUE_SATURATED", SEVERITY_BLOCKING,
                f"writer {name} queue peaked at {peak} of maxsize {maxsize}: "
                "unbounded growth / writer did not keep pace", writer=name,
            ))
        if _is_number(backlog) and backlog > 0:
            runtime_findings.append(_finding(
                "SOAK_WRITER_BACKLOG_AT_STOP", SEVERITY_BLOCKING,
                f"writer {name} still had {backlog} queued record(s) when the run "
                "stopped: the writer did not keep pace", writer=name,
            ))

    stats["collector_runtime_behavior"] = _status_from(runtime_findings)
    findings.extend(runtime_findings)

    # Exit-status evidence: a harness-captured process exit code for the
    # specific soak process, obtained externally (this module never launches
    # or observes processes itself). Its absence does not mean the collector
    # misbehaved -- stats["collector_runtime_behavior"] already answers that
    # from the collector's own log/summary evidence -- it means one of the
    # gate's REQUIRED verification items was not obtained.
    stats["exit_evidence"] = exit_evidence
    if exit_evidence is None:
        findings.append(_finding(
            "REQUIRED_EXIT_STATUS_EVIDENCE_UNAVAILABLE", SEVERITY_BLOCKING,
            "no verified process exit-code evidence was supplied for the soak run "
            "(e.g. a harness's Start-Process handle resolved to a launcher/venv-shim "
            "PID rather than the actual worker process, so its exit code could not "
            "be captured); this is a gap in required verification evidence, not a "
            "claim that the collector itself failed -- see collector_runtime_behavior",
        ))
    elif exit_evidence.get("schema") != PROCESS_EXIT_EVIDENCE_SCHEMA:
        findings.append(_finding(
            "EXIT_EVIDENCE_SCHEMA_MISMATCH", SEVERITY_BLOCKING,
            f"expected schema {PROCESS_EXIT_EVIDENCE_SCHEMA}, got {exit_evidence.get('schema')!r}",
        ))
    elif not _is_number(exit_evidence.get("exit_code")):
        findings.append(_finding(
            "REQUIRED_EXIT_STATUS_EVIDENCE_UNAVAILABLE", SEVERITY_BLOCKING,
            f"exit_evidence supplied but exit_code={exit_evidence.get('exit_code')!r} "
            "was not actually captured",
        ))
    elif exit_evidence["exit_code"] != 0:
        findings.append(_finding(
            "SOAK_PROCESS_NONZERO_EXIT", SEVERITY_BLOCKING,
            f"soak process exited with code {exit_evidence['exit_code']}",
        ))

    return _gate("G4_60_MINUTE_SOAK", findings, stats)


# ---------------------------------------------------------------------------
# G5 - storage / throughput measured
# ---------------------------------------------------------------------------

# Reuses the soak's own duration bar: the consistency check below only fires
# once the run was long enough that a genuinely-idle process is implausible.
RESOURCE_CONSISTENCY_MIN_RUNTIME_SECONDS = SOAK_REQUIRED_DURATION_SECONDS - SOAK_DURATION_TOLERANCE_SECONDS


def validate_storage_throughput(summary, process_samples=None):
    """G5 -- storage growth must be MEASURED, not guessed, and the resource
    samples (RSS/private-memory/CPU) claimed to describe the collector's
    process must actually be trustworthy evidence about THAT process.

    This gate deliberately applies no fixed GB/day or absolute-RSS pass/fail
    threshold for storage -- capacity planning happens later, from these
    recorded numbers. But it DOES require that resource-sample evidence
    prove its own identity: a sample set that never demonstrates it was taken
    from the actual collector worker (as opposed to, say, a venv launcher
    shim whose child process does the real work) is not evidence at all, no
    matter how clean the numbers look.
    """
    findings = []
    stats = {"streams": {}, "totals": {}}

    if summary is None or summary.get("schema") != SOAK_SUMMARY_SCHEMA:
        findings.append(_finding(
            "SOAK_SUMMARY_MISSING", SEVERITY_BLOCKING,
            "no valid soak summary supplied; storage/throughput cannot be measured",
        ))
        return _gate("G5_STORAGE_THROUGHPUT_MEASURED", findings, stats)

    streams = summary.get("streams") or {}
    required_metrics = ("raw_bytes", "raw_lines", "bytes_per_second", "projected_gb_per_day")
    summed_bytes = 0

    for name, info in sorted(streams.items()):
        entry = {metric: info.get(metric) for metric in required_metrics}
        entry["message_count"] = info.get("message_count")
        stats["streams"][name] = entry

        for metric in required_metrics:
            value = info.get(metric)
            if not _is_number(value):
                findings.append(_finding(
                    "STORAGE_METRIC_NOT_MEASURED", SEVERITY_BLOCKING,
                    f"stream {name} has {metric}={value!r}", stream=name, metric=metric,
                ))
            elif isinstance(value, float) and not math.isfinite(value):
                findings.append(_finding(
                    "STORAGE_METRIC_NOT_FINITE", SEVERITY_BLOCKING,
                    f"stream {name} has {metric}={value!r}", stream=name, metric=metric,
                ))
            elif value < 0:
                findings.append(_finding(
                    "STORAGE_METRIC_NEGATIVE", SEVERITY_BLOCKING,
                    f"stream {name} has {metric}={value!r}", stream=name, metric=metric,
                ))

        if _is_number(info.get("raw_bytes")):
            summed_bytes += info["raw_bytes"]

        # Written lines must match what the writer actually persisted; a
        # mismatch means the on-disk evidence disagrees with the in-process
        # counters, which makes every derived byte-rate untrustworthy.
        written = (summary.get("writers") or {}).get(name, {}).get("written_count")
        if _is_number(written) and _is_number(info.get("raw_lines")) and written != info["raw_lines"]:
            findings.append(_finding(
                "WRITER_COUNT_DISAGREES_WITH_FILE", SEVERITY_BLOCKING,
                f"stream {name}: writer reported {written} written record(s) but the "
                f"raw file contains {info['raw_lines']} line(s)", stream=name,
            ))

        if _is_number(info.get("message_count")) and info["message_count"] > 0:
            if _is_number(info.get("raw_bytes")) and info["raw_bytes"] <= 0:
                findings.append(_finding(
                    "STREAM_HAS_MESSAGES_BUT_NO_BYTES", SEVERITY_BLOCKING,
                    f"stream {name} counted {info['message_count']} message(s) but "
                    "persisted 0 bytes", stream=name,
                ))

    totals = summary.get("totals") or {}
    stats["totals"] = dict(totals)
    if not _is_number(totals.get("raw_bytes")):
        findings.append(_finding(
            "TOTAL_STORAGE_NOT_MEASURED", SEVERITY_BLOCKING,
            f"totals.raw_bytes={totals.get('raw_bytes')!r}",
        ))
    elif totals["raw_bytes"] != summed_bytes:
        findings.append(_finding(
            "TOTAL_STORAGE_INCONSISTENT", SEVERITY_BLOCKING,
            f"totals.raw_bytes={totals['raw_bytes']} != sum of per-stream "
            f"raw_bytes={summed_bytes}",
        ))
    if not _is_number(totals.get("projected_gb_per_day")):
        findings.append(_finding(
            "TOTAL_PROJECTION_NOT_MEASURED", SEVERITY_BLOCKING,
            f"totals.projected_gb_per_day={totals.get('projected_gb_per_day')!r}",
        ))

    stats["threshold_policy"] = (
        "no fixed GB/day or absolute-RSS pass/fail threshold is applied by this gate; "
        "the gate requires only that storage growth AND resource evidence are "
        "measured, internally consistent, and provably about the right process"
    )

    # --- Resource measurement (RSS / private-memory / CPU) -----------------
    samples = (process_samples or {}).get("samples") if process_samples else None
    if not samples:
        findings.append(_finding(
            "PROCESS_SAMPLES_MISSING", SEVERITY_BLOCKING,
            "no process RSS/CPU samples were supplied; resource behaviour over the "
            "soak cannot be evaluated",
        ))
        return _gate("G5_STORAGE_THROUGHPUT_MEASURED", findings, stats)

    analysis = analyse_process_samples(samples)
    stats["process"] = analysis

    if analysis["sample_count"] < MIN_PROCESS_SAMPLES:
        findings.append(_finding(
            "PROCESS_SAMPLES_TOO_FEW", SEVERITY_BLOCKING,
            f"{analysis['sample_count']} sample(s) supplied, need at least "
            f"{MIN_PROCESS_SAMPLES} to judge a trend",
        ))
        return _gate("G5_STORAGE_THROUGHPUT_MEASURED", findings, stats)

    # Primary rule: the sample set must prove it describes the actual
    # collector worker, not merely whatever PID a harness happened to hold a
    # handle to. A venv/launcher shim process is a common false positive: it
    # looks like "the collector process" to a naive PID-based sampler but
    # never does any of the collector's actual work.
    identity = process_samples.get("identity_verification")
    stats["identity_verification"] = identity
    identity_verified = bool(identity) and identity.get("verified") is True
    if not identity_verified:
        findings.append(_finding(
            "RESOURCE_SAMPLE_PROCESS_IDENTITY_UNVERIFIED", SEVERITY_BLOCKING,
            "process_samples.json carries no identity_verification evidence proving "
            "the sampled PID is the actual collector worker process (as opposed to, "
            "e.g., a venv launcher/shim whose real work happens in a child process); "
            "expected identity_verification={launcher_pid, worker_pid, method, "
            "verified: true}" if not identity else
            f"identity_verification present but verified={identity.get('verified')!r}, "
            "not True -- the sampled process's identity as the collector worker was "
            "not established",
        ))

    # Consistency rule: this is an evidence-internal-contradiction check, not
    # a CPU-usage threshold. It never fires on genuinely idle processes,
    # because it also requires the collector's OWN counters (independent of
    # the resource sampler) to show a substantial amount of completed work.
    totals = summary.get("totals") or {}
    duration = summary.get("actual_duration_seconds")
    total_messages = totals.get("message_count")
    cpu_delta = analysis["cpu_total_seconds_delta"]
    if (
        _is_number(duration) and duration >= RESOURCE_CONSISTENCY_MIN_RUNTIME_SECONDS
        and _is_number(total_messages) and total_messages > 0
        and cpu_delta is not None and cpu_delta == 0
    ):
        findings.append(_finding(
            "RESOURCE_SAMPLE_INCONSISTENT_WITH_WORKLOAD", SEVERITY_BLOCKING,
            f"the soak ran {duration:.1f}s and the collector's own counters recorded "
            f"{total_messages} processed message(s), but the sampled process "
            f"accumulated 0.0s of CPU time over that period -- the resource sample "
            "and the collector's own workload evidence cannot both be true of the "
            "same process",
        ))
    elif cpu_delta is None:
        findings.append(_finding(
            "PROCESS_CPU_DELTA_NOT_MEASURED", SEVERITY_BLOCKING,
            "fewer than two CPU-time samples; the required CPU-time delta "
            "measurement is absent",
        ))

    # Non-blocking: flat private memory is a symptom worth recording, but is
    # deliberately not sufficient on its own to fail the gate -- a briefly
    # idle process could show this without anything being wrong.
    if analysis["private_memory_all_identical"]:
        findings.append(_finding(
            "RESOURCE_SAMPLE_SUSPICIOUS_CONSTANT_MEMORY", SEVERITY_NON_BLOCKING,
            f"private_memory_bytes was identical ({analysis['private_memory_first_bytes']}) "
            f"across all {analysis['private_memory_sample_count']} sample(s) with a "
            "measured value; on its own this does not fail the gate, but combined with "
            "an unverified process identity or a zero CPU delta it is corroborating "
            "evidence that the wrong process was sampled",
        ))

    if analysis["obvious_monotonic_runaway"]:
        findings.append(_finding(
            "PROCESS_RSS_MONOTONIC_RUNAWAY", SEVERITY_BLOCKING,
            f"working set grew strictly monotonically across every sample and "
            f"the last quarter averaged {analysis['rss_growth_ratio']:.2f}x the "
            f"first quarter (threshold {RSS_RUNAWAY_GROWTH_RATIO}x)",
        ))

    return _gate("G5_STORAGE_THROUGHPUT_MEASURED", findings, stats)


# ---------------------------------------------------------------------------
# Supplied gates (G1 / G6 / G7) - shape validation only
# ---------------------------------------------------------------------------

def validate_supplied_gate(name, supplied):
    """G1/G6/G7 conclusions come from evidence this module cannot itself
    produce (a pytest run, a real process kill, a dependency manifest). This
    module still refuses to accept a bare assertion: a supplied gate must
    carry an explicit status and at least one evidence entry."""
    findings = []
    stats = {}

    if supplied is None:
        findings.append(_finding(
            "SUPPLIED_GATE_MISSING", SEVERITY_BLOCKING,
            f"{name} was not supplied; an unevaluated gate is FAIL, never unknown",
        ))
        return _gate(name, findings, stats)

    status = supplied.get("status")
    evidence = supplied.get("evidence")
    stats["supplied_status"] = status
    stats["evidence"] = evidence

    if status not in (STATUS_PASS, STATUS_FAIL):
        findings.append(_finding(
            "SUPPLIED_GATE_INVALID_STATUS", SEVERITY_BLOCKING,
            f"{name} status={status!r}, expected {STATUS_PASS} or {STATUS_FAIL}",
        ))
    if not evidence:
        findings.append(_finding(
            "SUPPLIED_GATE_WITHOUT_EVIDENCE", SEVERITY_BLOCKING,
            f"{name} carries no evidence entries",
        ))
    for item in supplied.get("findings") or []:
        findings.append(item)

    if status == STATUS_FAIL:
        findings.append(_finding(
            "SUPPLIED_GATE_REPORTED_FAIL", SEVERITY_BLOCKING,
            supplied.get("detail") or f"{name} was supplied with status FAIL",
        ))

    return _gate(name, findings, stats)


# ---------------------------------------------------------------------------
# G8 - formal Dataset startup guard
# ---------------------------------------------------------------------------

def evaluate_startup_gate(gate_results):
    """Fold G1..G7 into exactly one conclusion. No third state exists: a gate
    that is missing, unevaluated or FAIL makes the whole readiness FAIL."""
    findings = []
    blockers = []
    stats = {"gate_status": {}}

    for name in REQUIRED_GATES:
        if name == "G8_FORMAL_DATASET_STARTUP_GUARD":
            continue
        gate = gate_results.get(name)
        if gate is None:
            stats["gate_status"][name] = "MISSING"
            blockers.append(f"{name}: gate was never evaluated")
            findings.append(_finding(
                "REQUIRED_GATE_MISSING", SEVERITY_BLOCKING,
                f"{name} is required but absent from the gate results",
            ))
            continue
        stats["gate_status"][name] = gate["status"]
        if gate["status"] != STATUS_PASS:
            for item in gate["findings"]:
                if item["severity"] == SEVERITY_BLOCKING:
                    blockers.append(f"{name}: {item['code']} - {item['detail']}")

    gate = _gate("G8_FORMAL_DATASET_STARTUP_GUARD", findings, stats)
    return gate, blockers


def build_readiness_report(
    *, gate_results, blockers, changeset_id="NEXT_DATASET_COLLECTOR_READINESS",
    evidence_sources=None, generated_at=None,
):
    readiness = STATUS_PASS if not blockers else STATUS_FAIL
    report = {
        "schema": READINESS_REPORT_SCHEMA,
        "changeset_id": changeset_id,
        "generated_at": generated_at or datetime.now().astimezone().isoformat(),
        "evidence_sources": evidence_sources or {},
        "gates": {name: gate_results.get(name) for name in REQUIRED_GATES},
        "readiness": readiness,
        "formal_dataset_start_allowed": readiness == STATUS_PASS,
        "blockers": blockers,
        "interpretation": (
            "A future changeset that creates a formal Dataset MUST read this file "
            "first and refuse to start unless readiness == PASS and "
            "formal_dataset_start_allowed is true. This field expresses only "
            "the readiness gate, not project-owner authorization. There is no third, "
            "ambiguous state: an unevaluated gate counts as FAIL."
        ),
    }
    return report


def run_readiness_evaluation(
    *, soak_dir=None, supplied_gates=None, changeset_id="NEXT_DATASET_COLLECTOR_READINESS",
    generated_at=None,
):
    """Evaluate every gate and return (report, gate_results)."""
    supplied_gates = supplied_gates or {}
    evidence_sources = {}
    summary = None
    process_samples = None
    exit_evidence = None
    ledger_path = None
    raw_dir = None

    if soak_dir is not None:
        soak_dir = Path(soak_dir)
        ledger_path = soak_dir / "quality" / "gap_ledger.jsonl"
        raw_dir = soak_dir / "raw"
        summary_path = soak_dir / "soak_summary.json"
        samples_path = soak_dir / "process_samples.json"
        exit_evidence_path = soak_dir / "process_exit_evidence.json"
        evidence_sources = {
            "soak_dir": str(soak_dir),
            "gap_ledger": str(ledger_path),
            "raw_dir": str(raw_dir),
            "soak_summary": str(summary_path),
            "process_samples": str(samples_path),
            "process_exit_evidence": str(exit_evidence_path),
        }
        if summary_path.exists():
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
        if samples_path.exists():
            process_samples = json.loads(samples_path.read_text(encoding="utf-8"))
        if exit_evidence_path.exists():
            exit_evidence = json.loads(exit_evidence_path.read_text(encoding="utf-8"))

    gate_results = {}
    gate_results["G2_GAP_LEDGER_SEMANTICS"] = validate_gap_ledger_semantics(
        ledger_path if ledger_path is not None else Path("gap_ledger.jsonl")
    )
    gate_results["G3_REPAIR_RESYNC_PROVENANCE"] = validate_repair_resync_provenance(
        ledger_path if ledger_path is not None else Path("gap_ledger.jsonl"),
        raw_dir if raw_dir is not None else Path("raw"),
    )
    gate_results["G4_60_MINUTE_SOAK"] = validate_soak_execution(summary, exit_evidence)
    gate_results["G5_STORAGE_THROUGHPUT_MEASURED"] = validate_storage_throughput(summary, process_samples)
    for name in SUPPLIED_GATES:
        gate_results[name] = validate_supplied_gate(name, supplied_gates.get(name))

    gate_8, blockers = evaluate_startup_gate(gate_results)
    gate_results["G8_FORMAL_DATASET_STARTUP_GUARD"] = gate_8

    report = build_readiness_report(
        gate_results=gate_results, blockers=blockers, changeset_id=changeset_id,
        evidence_sources=evidence_sources, generated_at=generated_at,
    )
    return report, gate_results


def parse_args():
    parser = argparse.ArgumentParser(
        description="Evaluate collector readiness for formal Dataset startup "
                    "(NEXT_DATASET_COLLECTOR_READINESS). Read-only with respect to "
                    "all collected evidence."
    )
    parser.add_argument(
        "--soak-dir", required=True,
        help="Directory holding this changeset's soak evidence (quality/gap_ledger.jsonl, "
             "raw/*.jsonl, soak_summary.json, process_samples.json).",
    )
    parser.add_argument(
        "--supplied-gates", default=None,
        help="JSON file supplying the G1/G6/G7 conclusions and their evidence.",
    )
    parser.add_argument(
        "--report-out", default=None,
        help="Where to write the machine-readable readiness report. The ONLY file "
             "this tool ever writes.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    supplied = None
    if args.supplied_gates:
        supplied = json.loads(Path(args.supplied_gates).read_text(encoding="utf-8"))

    report, _ = run_readiness_evaluation(soak_dir=args.soak_dir, supplied_gates=supplied)

    print(json.dumps(report, ensure_ascii=False, indent=2))
    print()
    print(f"READINESS = {report['readiness']}")
    if report["blockers"]:
        print(f"BLOCKERS = {json.dumps(report['blockers'], ensure_ascii=False, indent=2)}")

    if args.report_out:
        out_path = Path(args.report_out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(
            json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        print(f"REPORT_WRITTEN: {out_path}")

    return 0 if report["readiness"] == STATUS_PASS else 1


if __name__ == "__main__":
    raise SystemExit(main())

