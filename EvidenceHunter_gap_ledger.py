# -*- coding: utf-8 -*-
"""BTC HUNTER collector gap ledger - append-only, evidence-preserving.

Records every detected data-continuity gap (aggTrade id gap, WS disconnect,
depth resync, silent stall, ...) as an append-only JSONL stream. Nothing is
ever rewritten in place: an update to an existing gap is itself a new
appended event referencing the same gap_id. The latest state of a gap is
reconstructed by folding its GAP_OPENED event with all later GAP_UPDATE
events for the same gap_id, in file order (materialize_latest()).

This module has no network access and no dependency on any specific stream
type; EvidenceHunter_orderbook.py / EvidenceHunter_collector.py call it whenever they
detect or resolve a gap.
"""

import json
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path

from EvidenceHunter_clock import local_wall_ms, monotonic_ms

REQUIRED_OPEN_FIELDS = (
    "gap_id", "component", "gap_start", "gap_end", "detection_time",
    "reason", "last_good_identifier", "first_good_identifier_after",
    "reconnect_attempts", "recoverability", "repair_status",
    "repair_source", "resync_status",
)

RECOVERABILITY_VALUES = ("PENDING_ASSESSMENT", "RECOVERABLE_VIA_BACKFILL", "UNRECOVERABLE")
REPAIR_STATUS_VALUES = ("OPEN", "REPAIRED", "UNRECOVERABLE_CONFIRMED")
RESYNC_STATUS_VALUES = ("NOT_APPLICABLE", "PENDING", "COMPLETED", "FAILED")
ACQUISITION_MODES = ("LIVE", "REST_BACKFILL", "ARCHIVE_BACKFILL")

_locks_guard = threading.Lock()
_path_locks = {}


class GapLedgerError(RuntimeError):
    pass


def _lock_for(path):
    key = str(Path(path))
    with _locks_guard:
        lock = _path_locks.get(key)
        if lock is None:
            lock = threading.Lock()
            _path_locks[key] = lock
        return lock


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _append_jsonl(path, record):
    path = Path(path)
    lock = _lock_for(path)
    line = json.dumps(record, ensure_ascii=False, sort_keys=True)
    with lock:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as f:
            f.write(line + "\n")
            f.flush()


def new_gap_id(component):
    return f"GAP_{component}_{uuid.uuid4().hex[:12]}"


def open_gap(
    path,
    *,
    component,
    gap_start,
    gap_end,
    reason,
    last_good_identifier,
    first_good_identifier_after=None,
    reconnect_attempts=0,
    recoverability="PENDING_ASSESSMENT",
    gap_id=None,
    extra=None,
):
    if recoverability not in RECOVERABILITY_VALUES:
        raise GapLedgerError(f"INVALID_RECOVERABILITY: {recoverability}")
    gap_id = gap_id or new_gap_id(component)
    record = {
        "event_type": "GAP_OPENED",
        "gap_id": gap_id,
        "component": component,
        "gap_start": gap_start,
        "gap_end": gap_end,
        "detection_time": _now_iso(),
        "detection_time_local_wall_ms": local_wall_ms(),
        "detection_time_monotonic_ms": monotonic_ms(),
        "reason": reason,
        "last_good_identifier": last_good_identifier,
        "first_good_identifier_after": first_good_identifier_after,
        "reconnect_attempts": int(reconnect_attempts),
        "recoverability": recoverability,
        "repair_status": "OPEN",
        "repair_source": None,
        "resync_status": "NOT_APPLICABLE",
    }
    if extra:
        record["extra"] = extra
    missing = [f for f in REQUIRED_OPEN_FIELDS if f not in record]
    if missing:
        raise GapLedgerError(f"GAP_RECORD_MISSING_FIELDS: {missing}")
    _append_jsonl(path, record)
    return gap_id


def update_gap(path, gap_id, **fields):
    if "repair_status" in fields and fields["repair_status"] not in REPAIR_STATUS_VALUES:
        raise GapLedgerError(f"INVALID_REPAIR_STATUS: {fields['repair_status']}")
    if "resync_status" in fields and fields["resync_status"] not in RESYNC_STATUS_VALUES:
        raise GapLedgerError(f"INVALID_RESYNC_STATUS: {fields['resync_status']}")
    if "recoverability" in fields and fields["recoverability"] not in RECOVERABILITY_VALUES:
        raise GapLedgerError(f"INVALID_RECOVERABILITY: {fields['recoverability']}")
    record = {
        "event_type": "GAP_UPDATE",
        "gap_id": gap_id,
        "updated_at": _now_iso(),
        "updated_at_local_wall_ms": local_wall_ms(),
        "updated_at_monotonic_ms": monotonic_ms(),
    }
    record.update(fields)
    _append_jsonl(path, record)


def mark_gap_repaired(path, gap_id, *, repair_source, first_good_identifier_after=None):
    fields = {"repair_status": "REPAIRED", "repair_source": repair_source, "resync_status": "COMPLETED"}
    if first_good_identifier_after is not None:
        fields["first_good_identifier_after"] = first_good_identifier_after
    update_gap(path, gap_id, **fields)


def mark_gap_unrecoverable(path, gap_id, *, detail):
    update_gap(
        path,
        gap_id,
        repair_status="UNRECOVERABLE_CONFIRMED",
        recoverability="UNRECOVERABLE",
        resync_status="NOT_APPLICABLE",
        repair_source=None,
        unrecoverable_detail=detail,
    )


def read_events(path):
    path = Path(path)
    if not path.exists():
        return []
    events = []
    with path.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                events.append(json.loads(line))
            except json.JSONDecodeError as error:
                raise GapLedgerError(
                    f"CORRUPT_GAP_LEDGER_LINE path={path} line_no={line_no}"
                ) from error
    return events


def materialize_latest(path):
    """Fold GAP_OPENED + GAP_UPDATE* events into one latest-state dict per
    gap_id, in first-opened order."""
    states = {}
    order = []
    for event in read_events(path):
        gap_id = event.get("gap_id")
        if gap_id is None:
            raise GapLedgerError(f"GAP_EVENT_MISSING_GAP_ID: {event}")
        event_type = event.get("event_type")
        if event_type == "GAP_OPENED":
            states[gap_id] = dict(event)
            order.append(gap_id)
        elif event_type == "GAP_UPDATE":
            if gap_id not in states:
                raise GapLedgerError(f"GAP_UPDATE_BEFORE_OPEN: {gap_id}")
            states[gap_id].update({k: v for k, v in event.items() if k != "event_type"})
        else:
            raise GapLedgerError(f"UNKNOWN_GAP_EVENT_TYPE: {event_type}")
    return [states[g] for g in order]


def build_repair_provenance(*, acquisition_mode, original_gap_id, source_provenance):
    if acquisition_mode not in ACQUISITION_MODES:
        raise GapLedgerError(f"INVALID_ACQUISITION_MODE: {acquisition_mode}")
    return {
        "acquisition_mode": acquisition_mode,
        "original_gap_id": original_gap_id,
        "repaired_at": _now_iso(),
        "source_provenance": source_provenance,
    }

