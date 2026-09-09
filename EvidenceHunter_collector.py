# -*- coding: utf-8 -*-
"""BTC HUNTER next-dataset collector V1 - entrypoint and component wiring.

Implementation-only changeset (NEXT_DATASET_COLLECTOR_IMPLEMENTATION). This
module wires together:
  - EvidenceHunter_collector_ws.WSConnection                  (raw WS lifecycle)
  - EvidenceHunter_orderbook.LocalOrderBook                    (diff depth continuity)
  - EvidenceHunter_gap_ledger                                  (append-only gap evidence)
  - EvidenceHunter_collector_supervisor.Supervisor /
    run_component_lifecycle                                (the one recovery authority)

into three collector components (aggTrade, diff depth, forceOrder) plus a
bounded-queue raw payload writer.

WHAT THIS FILE DOES NOT DO
---------------------------
- Does NOT create a formal Dataset. --runtime-dir must be an explicit
  scratch/non-formal path; nothing here reads or writes dataset_lifecycle.json
  or any Frozen path.
- Does NOT run forever by default: __main__ always exits after
  --duration-seconds (default 60s), so an accidental bare invocation cannot
  become an unattended long-running collection.
- Does NOT implement markPriceUpdate. DEFER_WITHOUT_BLOCKING_CORE_DATASET
  this changeset -- see research/governance/SCOPE_DECLARATION_NEXT_DATASET_
  COLLECTOR_IMPLEMENTATION_20260907.json. The /market route reservation for
  it is documented in EvidenceHunter_collector_ws.py but no markPrice stream
  code exists here.

forceOrder semantics note: the official forceOrder stream pushes at most one
message per symbol per ~1000ms window (the latest liquidation order in that
window), not a guaranteed complete per-event liquidation history. The
absence of a forceOrder message for a given window must never be read by any
downstream research code as proof that no liquidation occurred -- this file
tags every persisted forceOrder record with an explicit semantics_note to
keep that distinction visible in the raw evidence itself.

USAGE
-----
    python EvidenceHunter_collector.py --dataset-id synthetic-smoke --runtime-dir runtime\\collector_smoke_20260907 --duration-seconds 60
"""

import argparse
import json
import queue
import threading
import time
import urllib.parse
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request

from EvidenceHunter_clock import local_wall_ms, monotonic_ms
from EvidenceHunter_collector_network import open_url
from EvidenceHunter_clock_observer import ClockObserver
from EvidenceHunter_collector_ws import (
    ROUTE_MARKET,
    ROUTE_PUBLIC,
    STATE_HEALTHY as WS_STATE_HEALTHY,
    WSConnection,
    resolve_stream_url,
)
from EvidenceHunter_orderbook import (
    LocalOrderBook,
    OrderBookError,
    OrderBookNetworkError,
    safe_errno,
    headers_to_dict,
)
from EvidenceHunter_collector_supervisor import (
    STATE_CONNECTING,
    STATE_RECONNECTING,
    STATE_STREAM_NOT_READY,
    Supervisor,
    run_component_lifecycle,
)
import EvidenceHunter_gap_ledger as gap_ledger

REST_BASE_URL = "https://fapi.binance.com"
REQUEST_TIMEOUT_SECONDS = 10
AGG_TRADE_BACKFILL_LIMIT = 1000
MAX_DEPTH_RESYNC_ATTEMPTS = 5
INITIAL_DEPTH_SYNC_RETRY_SECONDS = 2.0
SILENT_STALL_SECONDS = 30.0
STALL_CHECK_INTERVAL_SECONDS = 5.0
RAW_PAYLOAD_QUEUE_MAXSIZE = 10000
RAW_PAYLOAD_WRITER_PUT_TIMEOUT_SECONDS = 2.0
# COLLECTOR_DAY1_COMPLETENESS_FIX: three Day-1 evidence streams that were
# previously discarded. They live beside the existing raw/quality output and
# are written through the same bounded-queue writer, so a stalled evidence
# writer is counted and visible exactly like a stalled market-data writer.
DEPTH_SNAPSHOT_EVIDENCE_FILENAME = "raw/depth_snapshot.jsonl"
CLOCK_OBSERVATION_FILENAME = "quality/clock_observations.jsonl"
REST_TELEMETRY_FILENAME = "quality/rest_telemetry.jsonl"
REST_TELEMETRY_SCHEMA = "COLLECTOR_REST_TELEMETRY_V1"
CLOCK_OBSERVER_COMPONENT = "clock_observer"
EVIDENCE_STREAMS = ("depth_snapshot", "clock_observations", "rest_telemetry")
# Only the clock observer is a supervised component in its own right; the other
# two evidence streams are produced as a side effect of REST calls made by the
# existing components, so they have no separate component health.
EVIDENCE_STREAM_COMPONENT = {"clock_observations": CLOCK_OBSERVER_COMPONENT}
# Fail-closed allow-lists for REST telemetry. Anything not named here is
# redacted rather than stored: a parameter that cannot be proven non-secret is
# treated as secret. Request headers are never copied at all, which is where
# an Authorization header would otherwise leak from.
REST_TELEMETRY_ALLOWED_PARAMS = ("symbol", "limit", "fromId")
REST_TELEMETRY_SECRET_PARAM_MARKERS = (
    "key", "secret", "signature", "token", "password", "passphrase", "auth", "cookie",
)
REST_TELEMETRY_ALLOWED_HEADER_PREFIXES = ("x-mbx-",)
REST_TELEMETRY_ALLOWED_HEADERS = ("retry-after",)
REDACTED = "<REDACTED>"
CONTINUITY_CHECKPOINT_FILENAME = "continuity_checkpoint.json"
CONTINUITY_CHECKPOINT_SCHEMA = "COLLECTOR_CONTINUITY_CHECKPOINT_V1"
CONTINUITY_CHECKPOINT_INTERVAL_SECONDS = 5.0
# A restart gap can span far more than one REST page, so the restart backfill
# paginates -- but always within an explicit, bounded budget rather than
# looping until it happens to finish.
MAX_RESTART_BACKFILL_PAGES = 20

FORCE_ORDER_SEMANTICS_NOTE = (
    "windowed snapshot push (<=1 message per symbol per ~1000ms); "
    "absence of a message is NOT evidence that no liquidation occurred"
)


class CollectorError(RuntimeError):
    pass


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def console_and_file_alert(alert_dir):
    """V1 Windows/local alert interface: console print + local JSONL log."""
    alert_dir = Path(alert_dir)

    def _alert_fn(alert):
        print(f"[COLLECTOR_ALERT] {json.dumps(alert, ensure_ascii=False)}")
        alert_dir.mkdir(parents=True, exist_ok=True)
        line = json.dumps({**alert, "logged_at": _now_iso()}, ensure_ascii=False)
        with (alert_dir / "collector_alerts.jsonl").open("a", encoding="utf-8") as f:
            f.write(line + "\n")

    return _alert_fn


def _sanitize_rest_telemetry(event):
    """The ONE place that decides what REST telemetry may reach disk.

    COLLECTOR_DAY1_COMPLETENESS_FIX / E4. Sanitisation lives here, at the sink,
    rather than at each call site, so there is a single auditable definition of
    what is allowed rather than several that can drift apart. It is fail-closed:
    an unrecognised parameter name is redacted, an unrecognised response header
    is dropped, and request headers are never copied in the first place.
    """
    params = {}
    for key, value in (event.get("params") or {}).items():
        lowered = str(key).lower()
        if any(marker in lowered for marker in REST_TELEMETRY_SECRET_PARAM_MARKERS):
            params[key] = REDACTED
        elif key in REST_TELEMETRY_ALLOWED_PARAMS:
            params[key] = value
        else:
            params[key] = REDACTED

    headers = {}
    for key, value in (event.get("response_headers") or {}).items():
        lowered = str(key).lower()
        if lowered.startswith(REST_TELEMETRY_ALLOWED_HEADER_PREFIXES) or (
            lowered in REST_TELEMETRY_ALLOWED_HEADERS
        ):
            headers[key] = value

    network_route_events = []
    for route_event in event.get("network_route_events") or []:
        network_route_events.append({
            "event_type": route_event.get("event_type"),
            "effective_network_mode": route_event.get("effective_network_mode"),
            "proxy_host": route_event.get("proxy_host"),
            "proxy_port": route_event.get("proxy_port"),
            "proxy_type": route_event.get("proxy_type"),
            "failure_exception_type": route_event.get("failure_exception_type"),
        })

    return {
        "schema": REST_TELEMETRY_SCHEMA,
        "rest_request_id": event.get("rest_request_id"),
        "request_class": event.get("request_class"),
        "endpoint_path": event.get("endpoint_path"),
        "params": params,
        "outcome": event.get("outcome"),
        "http_status": event.get("http_status"),
        "response_headers": headers,
        "network_route_events": network_route_events,
        # F3: free-text failure messages are NEVER persisted -- an exception
        # string can carry a signature/token/cookie past the params and header
        # allow-lists. Only structured, non-textual provenance is kept.
        "failure_code": event.get("failure_code"),
        "failure_exception_type": event.get("failure_exception_type"),
        "failure_errno": event.get("failure_errno") if isinstance(
            event.get("failure_errno"), int) else None,
        "requested_local_wall_ms": event.get("requested_local_wall_ms"),
        "requested_monotonic_ms": event.get("requested_monotonic_ms"),
        "received_local_wall_ms": event.get("received_local_wall_ms"),
        "received_monotonic_ms": event.get("received_monotonic_ms"),
    }


def make_rest_telemetry_sink(writer):
    """Sanitise then persist. Returns None if there is no writer, which keeps
    every telemetry call site inert by default."""
    if writer is None:
        return None

    def _sink(event):
        writer.submit(_sanitize_rest_telemetry(event))

    return _sink


def make_evidence_sink(writer):
    if writer is None:
        return None

    def _sink(record):
        writer.submit(record)

    return _sink


def _rest_request_json(path, params, *, request_fn=None, request_class=None,
                       telemetry_sink=None, sink_failures=None, http_fn=None):
    """COLLECTOR_DAY1_COMPLETENESS_FIX / GAP3: every REST request now leaves
    telemetry behind -- success, HTTP error and network error alike -- carrying
    a rest_request_id, the HTTP status, the response headers, timing and
    failure provenance. The raw (unsanitised) event goes to the sink, which
    applies the allow-list before anything is written. Error handling and the
    exact CollectorError messages are unchanged, so no caller's behaviour and
    no gap classification moves."""
    rest_request_id = uuid.uuid4().hex
    requested_local_wall_ms = local_wall_ms()
    requested_monotonic_ms = monotonic_ms()
    network_route_events = []

    def _telemetry(outcome, *, received_pair, http_status=None, response_headers=None,
                   failure_code=None, exception_type=None, errno=None):
        if telemetry_sink is None:
            return
        received_wall, received_monotonic = received_pair
        try:
            telemetry_sink({
                "rest_request_id": rest_request_id,
                "request_class": request_class or "UNSPECIFIED",
                "endpoint_path": path,
                "params": dict(params or {}),
                "outcome": outcome,
                "http_status": http_status,
                "response_headers": response_headers or {},
                "network_route_events": list(network_route_events),
                # F3: structured failure provenance only; never str(error).
                "failure_code": failure_code,
                "failure_exception_type": exception_type,
                "failure_errno": errno,
                "requested_local_wall_ms": requested_local_wall_ms,
                "requested_monotonic_ms": requested_monotonic_ms,
                "received_local_wall_ms": received_wall,
                "received_monotonic_ms": received_monotonic,
            })
        except Exception as error:  # noqa: BLE001 -- telemetry never breaks collection
            if sink_failures is not None:
                sink_failures.append(f"{type(error).__name__}: {error}")

    if request_fn is not None:
        result = request_fn(path, params)
        _telemetry("INJECTED_REQUEST_FN_NO_HTTP_LAYER",
                   received_pair=(local_wall_ms(), monotonic_ms()))
        return result
    query = urllib.parse.urlencode(params)
    url = f"{REST_BASE_URL}{path}?{query}"
    request = Request(
        url,
        headers={"User-Agent": "BTC-AI-Hunter-Collector/1.0", "Accept": "application/json"},
        method="GET",
    )
    opener = http_fn if http_fn is not None else (
        lambda req, timeout: open_url(
            req, timeout=timeout, on_event=network_route_events.append,
        )
    )
    try:
        with opener(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
            body_bytes = response.read()
            # F2: ONE receive instant, captured immediately after the body.
            received_pair = (local_wall_ms(), monotonic_ms())
            status = getattr(response, "status", None) or getattr(response, "code", None)
            headers = headers_to_dict(getattr(response, "headers", None))
    except HTTPError as error:
        _telemetry(
            "HTTP_ERROR", received_pair=(local_wall_ms(), monotonic_ms()),
            http_status=getattr(error, "code", None),
            response_headers=headers_to_dict(getattr(error, "headers", None)),
            failure_code="REST_HTTP_ERROR", exception_type=type(error).__name__,
        )
        raise CollectorError(f"REST_HTTP_ERROR path={path} status={error.code}") from error
    except URLError as error:
        _telemetry(
            "NETWORK_ERROR", received_pair=(local_wall_ms(), monotonic_ms()),
            failure_code="REST_NETWORK_ERROR", exception_type=type(error).__name__,
            errno=safe_errno(error),
        )
        raise CollectorError(f"REST_NETWORK_ERROR path={path} reason={error.reason}") from error
    _telemetry("SUCCESS", received_pair=received_pair, http_status=status,
               response_headers=headers)
    return json.loads(body_bytes.decode("utf-8"))


class RawPayloadWriter:
    """Bounded-queue, single background-thread append-only JSONL writer.

    A stalled writer (disk contention, etc.) must not silently swallow
    messages nor block the WS receive thread forever: submit() uses a
    bounded put timeout, and every timed-out put increments dropped_count
    and invokes on_drop -- so a stall is always visible, even though the
    specific dropped record is (necessarily) lost.
    """

    def __init__(
        self, path, *, maxsize=RAW_PAYLOAD_QUEUE_MAXSIZE,
        put_timeout_seconds=RAW_PAYLOAD_WRITER_PUT_TIMEOUT_SECONDS, on_drop=None,
        key_fn=None,
    ):
        self.path = Path(path)
        self.maxsize = maxsize
        self._queue = queue.Queue(maxsize=maxsize)
        self._put_timeout = put_timeout_seconds
        self._on_drop = on_drop or (lambda record: None)
        # NEXT_DATASET_COLLECTOR_READINESS / G6: key_fn extracts the stream's
        # sequential identifier from a record. It is applied by the writer
        # thread AFTER the record has been written and flushed, so
        # persisted_high_water is a DURABLY PERSISTED high-water mark, not
        # "the last id seen in memory". That distinction matters: a
        # checkpoint holding an id that was received but never reached the
        # file would make the next process re-open (and try to backfill) a
        # range that is already on disk, manufacturing a false gap.
        #
        # Durability boundary: written + flushed to the OS file. That survives
        # process termination -- the fault model of the G6 restart test
        # (Stop-Process -Force) -- but NOT host power loss, since no fsync is
        # issued per record. The checkpoint file states this boundary
        # explicitly rather than claiming unqualified durability.
        self._key_fn = key_fn
        self.persisted_high_water = None
        self.dropped_count = 0
        self.written_count = 0
        self.submitted_count = 0
        # NEXT_DATASET_COLLECTOR_READINESS / G4: "no unbounded queue growth"
        # cannot be judged from a final depth of 0, because a queue that
        # briefly saturated and then drained looks identical to one that never
        # filled. The peak depth is the only observation that distinguishes
        # them, so it is tracked continuously rather than sampled.
        self.queue_depth_peak = 0
        self.last_drain_monotonic_ms = None
        self._stop_requested = threading.Event()
        self._thread = None

    def start(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._thread = threading.Thread(target=self._run, name="raw-payload-writer", daemon=True)
        self._thread.start()
        return self

    def queue_depth(self):
        return self._queue.qsize()

    def submit(self, record):
        try:
            self._queue.put(record, timeout=self._put_timeout)
        except queue.Full:
            self.dropped_count += 1
            self._on_drop(record)
            return
        self.submitted_count += 1
        depth = self._queue.qsize()
        if depth > self.queue_depth_peak:
            self.queue_depth_peak = depth

    def _run(self):
        with self.path.open("a", encoding="utf-8") as f:
            while not (self._stop_requested.is_set() and self._queue.empty()):
                try:
                    record = self._queue.get(timeout=1.0)
                except queue.Empty:
                    continue
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
                f.flush()
                self.written_count += 1
                if self._key_fn is not None:
                    key = self._key_fn(record)
                    # max(), not "last": REST-backfilled rows are written
                    # after live rows but carry LOWER identifiers, so the
                    # high-water mark must never move backwards.
                    if key is not None and (
                        self.persisted_high_water is None or key > self.persisted_high_water
                    ):
                        self.persisted_high_water = key
                self.last_drain_monotonic_ms = monotonic_ms()
                self._queue.task_done()

    def is_stalled(self, max_silence_seconds):
        if self.last_drain_monotonic_ms is None:
            return False
        if self._queue.empty():
            return False
        return (monotonic_ms() - self.last_drain_monotonic_ms) / 1000.0 > max_silence_seconds

    def stop(self):
        self._stop_requested.set()
        if self._thread is not None:
            self._thread.join(timeout=10.0)


def agg_trade_key_fn(record):
    """Durable high-water key for the aggTrade raw stream: the aggregate
    trade id ('a')."""
    raw = record.get("raw") or {}
    value = raw.get("a")
    return int(value) if value is not None else None


def depth_key_fn(record):
    """Durable high-water key for the diff-depth raw stream: the final
    update id of the event ('u')."""
    raw = record.get("raw") or {}
    value = raw.get("u")
    return int(value) if value is not None else None


def continuity_checkpoint_path(runtime_dir):
    return Path(runtime_dir) / CONTINUITY_CHECKPOINT_FILENAME


def write_continuity_checkpoint(runtime_dir, writers_by_component):
    """Atomically record each stream's DURABLY PERSISTED high-water mark.

    This file is recovery evidence, never restored runtime state. Nothing in
    the collector reads it to put a component back into HEALTHY: a restarted
    process always re-walks CONNECTING -> STREAM_NOT_READY -> HEALTHY and
    always takes a fresh REST depth snapshot. Its only uses are (a) detecting
    whether the restart left a continuity gap, and (b) providing the
    provenance identifiers for the record of that gap.
    """
    path = continuity_checkpoint_path(runtime_dir)
    payload = {
        "schema": CONTINUITY_CHECKPOINT_SCHEMA,
        "written_at_iso": _now_iso(),
        "written_at_local_wall_ms": local_wall_ms(),
        "authoritative_for_restore": False,
        "durability_boundary": (
            "values are high-water marks of records already written AND flushed to "
            "their raw JSONL file; they survive process termination (the G6 restart "
            "fault model) but not host power loss, as no per-record fsync is issued"
        ),
        "semantics": (
            "diagnostic / gap-detection / recovery-provenance evidence only; a "
            "restarted process must never use this file to resume a component's "
            "state or to skip a fresh order-book snapshot"
        ),
        "components": {
            "aggTrade": {
                "last_durably_persisted_trade_id":
                    writers_by_component["aggTrade"].persisted_high_water,
            },
            "diff_depth": {
                "last_durably_persisted_update_id":
                    writers_by_component["diff_depth"].persisted_high_water,
            },
        },
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    with temp_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
        f.flush()
    temp_path.replace(path)
    return payload


def read_continuity_checkpoint(runtime_dir):
    """Read a prior run's checkpoint as evidence. Returns None when absent
    (a genuine first start). A corrupt checkpoint raises rather than being
    silently ignored -- silently treating it as 'no prior run' would hide a
    restart boundary instead of recording it."""
    path = continuity_checkpoint_path(runtime_dir)
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        raise CollectorError(f"CORRUPT_CONTINUITY_CHECKPOINT: {error}") from error
    if payload.get("schema") != CONTINUITY_CHECKPOINT_SCHEMA:
        raise CollectorError(
            f"UNKNOWN_CONTINUITY_CHECKPOINT_SCHEMA: {payload.get('schema')!r}"
        )
    return payload


def rest_agg_trades_from_id(symbol, from_id, *, limit=AGG_TRADE_BACKFILL_LIMIT, request_fn=None,
                            telemetry_sink=None, sink_failures=None, http_fn=None):
    """GET /fapi/v1/aggTrades?fromId=... for aggTrade-ID-gap backfill."""
    rows = _rest_request_json(
        "/fapi/v1/aggTrades", {"symbol": symbol, "fromId": int(from_id), "limit": int(limit)},
        request_fn=request_fn, request_class="AGG_TRADES_BACKFILL",
        telemetry_sink=telemetry_sink, sink_failures=sink_failures, http_fn=http_fn,
    )
    return rows or []


class AggTradeCollector:
    """Sequential aggTrade id ('a' field) continuity + REST-backfill on gap."""

    def __init__(
        self, symbol, *, gap_ledger_path, writer, supervisor, component_name="aggTrade",
        request_fn=None, restart_checkpoint_id=None, telemetry_sink=None,
    ):
        self.symbol = symbol
        self.gap_ledger_path = gap_ledger_path
        self.writer = writer
        self.supervisor = supervisor
        self.component_name = component_name
        self._request_fn = request_fn
        # COLLECTOR_DAY1_COMPLETENESS_FIX: inert by default; when supplied,
        # every REST backfill request leaves telemetry behind.
        self._telemetry_sink = telemetry_sink
        self.evidence_sink_failures = []
        self.last_trade_id = None
        self.gap_count = 0
        # G6: the durably-persisted high-water mark left by the previous
        # process, or None on a genuine first start. It is used exactly once,
        # against the first live message, and never to restore state.
        self.restart_checkpoint_id = restart_checkpoint_id
        self._restart_boundary_checked = restart_checkpoint_id is None
        self.restart_gap_id = None

    def handle_message(self, payload, meta):
        trade_id = payload.get("a")
        if trade_id is None:
            return
        trade_id = int(trade_id)
        self.supervisor.record_message(self.component_name, local_wall_ms=meta["received_local_wall_ms"])
        self.writer.submit({
            "stream": "aggTrade", "raw": payload,
            "collector_received_local_wall_ms": meta["received_local_wall_ms"],
            "collector_received_monotonic_ms": meta["received_monotonic_ms"],
            "acquisition_mode": "LIVE",
        })

        if not self._restart_boundary_checked:
            self._restart_boundary_checked = True
            self._handle_restart_boundary(trade_id)

        if self.last_trade_id is not None and trade_id > self.last_trade_id + 1:
            self._handle_gap(self.last_trade_id, trade_id)
        self.last_trade_id = trade_id if self.last_trade_id is None else max(self.last_trade_id, trade_id)

    def _handle_restart_boundary(self, first_live_id):
        """Compare the first post-restart live message against the previous
        process's durably-persisted high-water mark.

        Contiguous (first_live_id == checkpoint_id + 1) means the restart cost
        us nothing, and NO gap record is created -- a fabricated gap would be
        as damaging to the evidence as a missed one.
        """
        checkpoint_id = self.restart_checkpoint_id
        if checkpoint_id is None or first_live_id <= checkpoint_id + 1:
            return None

        low, high = checkpoint_id + 1, first_live_id - 1
        self.gap_count += 1
        gap_id = gap_ledger.open_gap(
            self.gap_ledger_path,
            component=self.component_name,
            gap_start=low,
            gap_end=high,
            reason="PROCESS_RESTART_SEQUENCE_GAP",
            last_good_identifier=checkpoint_id,
            first_good_identifier_after=first_live_id,
            recoverability="RECOVERABLE_VIA_BACKFILL",
            extra={
                "continuity_status": "BROKEN_BY_PROCESS_RESTART",
                "checkpoint_semantics": "last durably persisted aggTrade id of the prior process",
            },
        )
        self.restart_gap_id = gap_id
        self._backfill_verified_range(gap_id, low, high)
        return gap_id

    def _backfill_verified_range(self, gap_id, low, high):
        """Paginated fromId backfill of the inclusive range [low, high], with
        dedupe and an explicit continuity verification.

        The gap is only marked REPAIRED when every id in the range was
        actually retrieved. A partial result is recorded as unrecoverable with
        the shortfall stated -- never as a repair, because a REPAIRED gap that
        is not actually filled is worse than an open one: it silently ends the
        investigation.
        """
        collected = set()
        cursor = low
        pages = 0
        provenance = gap_ledger.build_repair_provenance(
            acquisition_mode="REST_BACKFILL", original_gap_id=gap_id,
            source_provenance="GET /fapi/v1/aggTrades?fromId= (paginated, inclusive fromId)",
        )
        while cursor <= high and pages < MAX_RESTART_BACKFILL_PAGES:
            try:
                rows = rest_agg_trades_from_id(
                    self.symbol, cursor, request_fn=self._request_fn,
                    telemetry_sink=self._telemetry_sink,
                    sink_failures=self.evidence_sink_failures,
                )
            except CollectorError as error:
                gap_ledger.mark_gap_unrecoverable(self.gap_ledger_path, gap_id, detail=str(error))
                return False
            pages += 1
            if not rows:
                break
            max_seen = cursor - 1
            for row in rows:
                row_id = row.get("a")
                if row_id is None:
                    continue
                row_id = int(row_id)
                if row_id > max_seen:
                    max_seen = row_id
                if low <= row_id <= high and row_id not in collected:  # dedupe
                    collected.add(row_id)
                    self.writer.submit({
                        "stream": "aggTrade", "raw": row,
                        "collector_received_local_wall_ms": local_wall_ms(),
                        "collector_received_monotonic_ms": monotonic_ms(),
                        **provenance,
                    })
            if max_seen < cursor:
                break  # no forward progress; stop rather than loop forever
            cursor = max_seen + 1

        expected = high - low + 1
        if len(collected) == expected:
            gap_ledger.mark_gap_repaired(
                self.gap_ledger_path, gap_id,
                repair_source="REST_AGGTRADES_FROM_ID",
                first_good_identifier_after=high + 1,
            )
            gap_ledger.update_gap(
                self.gap_ledger_path, gap_id,
                continuity_verified=True, backfilled_id_count=len(collected),
                expected_id_count=expected, backfill_pages_used=pages,
            )
            return True

        gap_ledger.mark_gap_unrecoverable(
            self.gap_ledger_path, gap_id,
            detail=(
                f"INCOMPLETE_BACKFILL expected={expected} collected={len(collected)} "
                f"pages_used={pages} page_budget={MAX_RESTART_BACKFILL_PAGES}"
            ),
        )
        gap_ledger.update_gap(
            self.gap_ledger_path, gap_id,
            continuity_verified=False, backfilled_id_count=len(collected),
            expected_id_count=expected, backfill_pages_used=pages,
        )
        return False

    def _handle_gap(self, last_good_id, next_seen_id):
        self.gap_count += 1
        gap_id = gap_ledger.open_gap(
            self.gap_ledger_path,
            component=self.component_name,
            gap_start=last_good_id + 1,
            gap_end=next_seen_id - 1,
            reason="AGGTRADE_ID_SEQUENCE_GAP",
            last_good_identifier=last_good_id,
            first_good_identifier_after=next_seen_id,
            recoverability="RECOVERABLE_VIA_BACKFILL",
        )
        try:
            rows = rest_agg_trades_from_id(self.symbol, last_good_id + 1, request_fn=self._request_fn)
        except CollectorError as error:
            gap_ledger.mark_gap_unrecoverable(self.gap_ledger_path, gap_id, detail=str(error))
            return gap_id

        provenance = gap_ledger.build_repair_provenance(
            acquisition_mode="REST_BACKFILL", original_gap_id=gap_id,
            source_provenance="GET /fapi/v1/aggTrades?fromId=",
        )
        for row in rows:
            row_id = int(row.get("a", -1))
            if last_good_id < row_id < next_seen_id:
                self.writer.submit({
                    "stream": "aggTrade", "raw": row,
                    "collector_received_local_wall_ms": local_wall_ms(),
                    "collector_received_monotonic_ms": monotonic_ms(),
                    **provenance,
                })
        gap_ledger.mark_gap_repaired(self.gap_ledger_path, gap_id, repair_source="REST_AGGTRADES_FROM_ID")
        return gap_id


class DiffDepthCollector:
    """Feeds diff-depth payloads into a LocalOrderBook; opens/repairs gaps on
    desync, with a bounded resync-attempt budget before marking unrecoverable."""

    def __init__(
        self, symbol, *, gap_ledger_path, writer, supervisor, component_name="diff_depth",
        max_resync_attempts=MAX_DEPTH_RESYNC_ATTEMPTS, request_fn=None,
        restart_checkpoint_update_id=None, snapshot_sink=None, telemetry_sink=None,
        http_fn=None,
    ):
        self.symbol = symbol
        self.gap_ledger_path = gap_ledger_path
        self.writer = writer
        self.supervisor = supervisor
        self.component_name = component_name
        self.max_resync_attempts = max_resync_attempts
        # COLLECTOR_DAY1_COMPLETENESS_FIX: the sinks are passed straight
        # through to the order book, which emits snapshot + REST telemetry
        # evidence from the single place a snapshot is ever fetched
        # (start_resync), covering the first initialization and every later
        # resync alike. With sinks left as None the book behaves exactly as
        # before, so no resync/gap/restart semantics move.
        self.order_book = LocalOrderBook(
            symbol, request_fn=request_fn, snapshot_sink=snapshot_sink,
            telemetry_sink=telemetry_sink, http_fn=http_fn,
        )
        self._open_gap_id = None
        self._resync_attempts = 0
        self.restart_checkpoint_update_id = restart_checkpoint_update_id
        self.restart_gap_id = None

    def ensure_initial_sync(self):
        """Obtain one fresh initial snapshot or raise the exact failure."""
        new_last_update_id = self.order_book.start_resync()
        if self.restart_checkpoint_update_id is not None and self.restart_gap_id is None:
            self._record_restart_discontinuity(new_last_update_id)
        return new_last_update_id

    def ensure_initial_sync_with_network_retry(
        self, *, retry_network_errors=False,
        retry_delay_seconds=INITIAL_DEPTH_SYNC_RETRY_SECONDS,
        retry_wait_fn=None, on_retry=None,
    ):
        """Always a FRESH REST snapshot -- the pre-restart in-memory book is
        never resumed. Binance's documented procedure requires that a broken
        pu-chain be re-anchored from a new snapshot, and a killed process's
        WebSocket diff chain is broken by definition.

        Startup may retry transport-only failures. Every attempt calls the
        normal snapshot path again, so AUTO proxy discovery is refreshed.
        HTTP, parsing, snapshot-shape and continuity failures are deliberately
        not caught here and remain fatal/data-integrity failures.
        """
        retry_wait_fn = retry_wait_fn or time.sleep
        attempts = 0
        while True:
            try:
                new_last_update_id = self.ensure_initial_sync()
                break
            except OrderBookNetworkError as error:
                if not retry_network_errors:
                    raise
                attempts += 1
                self.supervisor.transition(
                    self.component_name,
                    STATE_RECONNECTING,
                    reason=f"INITIAL_DEPTH_SNAPSHOT_NETWORK_UNAVAILABLE attempt={attempts}",
                )
                if on_retry is not None:
                    on_retry({
                        "event_type": "INITIAL_DEPTH_SNAPSHOT_RETRY",
                        "component": self.component_name,
                        "attempt": attempts,
                        "failure_exception_type": type(error).__name__,
                        "retry_delay_seconds": retry_delay_seconds,
                    })
                retry_wait_fn(retry_delay_seconds)
                self.supervisor.transition(
                    self.component_name,
                    STATE_CONNECTING,
                    reason=f"INITIAL_DEPTH_SNAPSHOT_RETRY attempt={attempts + 1}",
                )
        if attempts:
            self.supervisor.transition(
                self.component_name,
                STATE_STREAM_NOT_READY,
                reason=(
                    "INITIAL_DEPTH_SNAPSHOT_CONNECTIVITY_RESTORED "
                    f"attempt={attempts + 1}"
                ),
            )
        return new_last_update_id

    def _record_restart_discontinuity(self, new_snapshot_last_update_id):
        """Record that the pre-restart order-book continuity segment is closed
        and a new one has started.

        recoverability = UNRECOVERABLE means precisely: event-level continuity
        BETWEEN the old and new segments cannot be reconstructed after the
        fact. It is deliberately NOT a claim that specific depth events were
        confirmed missing -- the collector has no way to prove that, and
        asserting it would corrupt later audits.
        """
        checkpoint_id = self.restart_checkpoint_update_id
        extra = {
            "continuity_status": "BROKEN_BY_PROCESS_RESTART",
            "old_segment": "CLOSED",
            "new_segment": "STARTED_FROM_FRESH_REST_SNAPSHOT",
            "new_snapshot_last_update_id": new_snapshot_last_update_id,
            "unrecoverable_semantics": (
                "event-level continuity between the pre-restart and post-restart "
                "order-book segments cannot be reconstructed; this is NOT a claim "
                "that specific depth events were confirmed missing"
            ),
        }
        if new_snapshot_last_update_id > checkpoint_id:
            extra["update_id_span_crossed"] = new_snapshot_last_update_id - checkpoint_id
            gap_end = new_snapshot_last_update_id
        else:
            # Still a new continuity segment, but nothing was demonstrably
            # skipped -- say exactly that instead of inventing a span.
            extra["update_id_span_crossed"] = 0
            extra["note"] = (
                "new snapshot lastUpdateId did not advance beyond the checkpoint; "
                "a new continuity segment still began, but no skipped updates are asserted"
            )
            gap_end = checkpoint_id

        gap_id = gap_ledger.open_gap(
            self.gap_ledger_path,
            component=self.component_name,
            gap_start=checkpoint_id,
            gap_end=gap_end,
            reason="PROCESS_RESTART_ORDERBOOK_DISCONTINUITY",
            last_good_identifier=checkpoint_id,
            first_good_identifier_after=new_snapshot_last_update_id,
            recoverability="UNRECOVERABLE",
            extra=extra,
        )
        gap_ledger.mark_gap_unrecoverable(
            self.gap_ledger_path, gap_id,
            detail="PROCESS_RESTART_ORDERBOOK_SEGMENT_BREAK",
        )
        self.restart_gap_id = gap_id
        return gap_id

    def handle_message(self, payload, meta):
        self.supervisor.record_message(self.component_name, local_wall_ms=meta["received_local_wall_ms"])
        self.writer.submit({
            "stream": "depth", "raw": payload,
            "collector_received_local_wall_ms": meta["received_local_wall_ms"],
            "collector_received_monotonic_ms": meta["received_monotonic_ms"],
            "acquisition_mode": "LIVE",
        })
        result = self.order_book.ingest(payload)
        if result == "DESYNCED":
            self._handle_desync(payload)
        elif result in ("SYNCHRONIZED", "APPLIED") and self._open_gap_id is not None:
            gap_ledger.mark_gap_repaired(self.gap_ledger_path, self._open_gap_id, repair_source="DIFF_DEPTH_RESYNC")
            self._open_gap_id = None
            self._resync_attempts = 0
        return result

    def _handle_desync(self, payload):
        if self._open_gap_id is None:
            self._open_gap_id = gap_ledger.open_gap(
                self.gap_ledger_path,
                component=self.component_name,
                gap_start=self.order_book.last_update_id,
                gap_end=payload.get("u"),
                reason="PU_CONTINUITY_MISMATCH",
                last_good_identifier=self.order_book.last_update_id,
                first_good_identifier_after=None,
                recoverability="RECOVERABLE_VIA_BACKFILL",
            )
            gap_ledger.update_gap(self.gap_ledger_path, self._open_gap_id, resync_status="PENDING")

        self._resync_attempts += 1
        if self._resync_attempts > self.max_resync_attempts:
            gap_ledger.mark_gap_unrecoverable(
                self.gap_ledger_path, self._open_gap_id,
                detail=f"EXCEEDED_MAX_RESYNC_ATTEMPTS={self.max_resync_attempts}",
            )
            self._open_gap_id = None
            self._resync_attempts = 0
            return
        try:
            self.order_book.start_resync()
        except OrderBookError:
            # Leave the gap OPEN with resync_status PENDING; the next depth
            # message (or a future timer-driven retry) will try again.
            return


class ForceOrderCollector:
    """No continuity validation is possible (not a sequential id stream);
    just persists raw payloads with explicit windowed-snapshot provenance."""

    def __init__(self, *, writer, supervisor, component_name="forceOrder"):
        self.writer = writer
        self.supervisor = supervisor
        self.component_name = component_name

    def handle_message(self, payload, meta):
        self.supervisor.record_message(self.component_name, local_wall_ms=meta["received_local_wall_ms"])
        self.writer.submit({
            "stream": "forceOrder", "raw": payload,
            "collector_received_local_wall_ms": meta["received_local_wall_ms"],
            "collector_received_monotonic_ms": meta["received_monotonic_ms"],
            "acquisition_mode": "LIVE",
            "semantics_note": FORCE_ORDER_SEMANTICS_NOTE,
        })


def parse_args():
    parser = argparse.ArgumentParser(
        description="BTC HUNTER next-dataset collector V1 (implementation-only; "
                    "does not create a formal Dataset or authorize unattended live "
                    "collection by itself -- it always exits after --duration-seconds)."
    )
    parser.add_argument("--dataset-id", required=True, help="Explicit Dataset identity; never inferred or registered by this collector.")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument(
        "--runtime-dir", required=True,
        help="Scratch/non-formal runtime directory for this run's raw payload + gap "
             "ledger + supervisor state output. Must NOT be a Frozen or formal Dataset path.",
    )
    parser.add_argument(
        "--duration-seconds", type=float, default=60.0,
        help="This script always exits after this many seconds; it is not a "
             "long-running daemon by default.",
    )
    return parser.parse_args()


def build_components(symbol, runtime_dir):
    runtime_dir = Path(runtime_dir)
    gap_ledger_path = runtime_dir / "quality" / "gap_ledger.jsonl"
    supervisor = Supervisor(
        state_file=runtime_dir / "supervisor_state.json",
        alert_fn=console_and_file_alert(runtime_dir / "alerts"),
    )
    for name in ("aggTrade", "diff_depth", "forceOrder"):
        supervisor.register(name)
    # COLLECTOR_DAY1_COMPLETENESS_FIX / GAP2 + G1: the clock observer is a
    # registered Supervisor component, so an uncaught exception in its thread
    # becomes STATE_LIFECYCLE_THREAD_CRASHED through the same alert+persist
    # path as the WebSocket components instead of dying silently.
    supervisor.register(CLOCK_OBSERVER_COMPONENT)

    agg_writer = RawPayloadWriter(runtime_dir / "raw" / "aggTrade.jsonl", key_fn=agg_trade_key_fn)
    depth_writer = RawPayloadWriter(runtime_dir / "raw" / "depth.jsonl", key_fn=depth_key_fn)
    force_order_writer = RawPayloadWriter(runtime_dir / "raw" / "forceOrder.jsonl")
    snapshot_writer = RawPayloadWriter(runtime_dir / DEPTH_SNAPSHOT_EVIDENCE_FILENAME)
    clock_writer = RawPayloadWriter(runtime_dir / CLOCK_OBSERVATION_FILENAME)
    rest_telemetry_writer = RawPayloadWriter(runtime_dir / REST_TELEMETRY_FILENAME)
    snapshot_sink = make_evidence_sink(snapshot_writer)
    rest_telemetry_sink = make_rest_telemetry_sink(rest_telemetry_writer)

    # G6: a prior process's checkpoint is read as EVIDENCE only. No component
    # state, no order book and no HEALTHY status is restored from it.
    checkpoint = read_continuity_checkpoint(runtime_dir)
    checkpoint_components = (checkpoint or {}).get("components") or {}
    restart_trade_id = (checkpoint_components.get("aggTrade") or {}).get(
        "last_durably_persisted_trade_id"
    )
    restart_update_id = (checkpoint_components.get("diff_depth") or {}).get(
        "last_durably_persisted_update_id"
    )

    agg_collector = AggTradeCollector(
        symbol, gap_ledger_path=gap_ledger_path, writer=agg_writer, supervisor=supervisor,
        restart_checkpoint_id=restart_trade_id, telemetry_sink=rest_telemetry_sink,
    )
    depth_collector = DiffDepthCollector(
        symbol, gap_ledger_path=gap_ledger_path, writer=depth_writer, supervisor=supervisor,
        restart_checkpoint_update_id=restart_update_id,
        snapshot_sink=snapshot_sink, telemetry_sink=rest_telemetry_sink,
    )
    force_order_collector = ForceOrderCollector(writer=force_order_writer, supervisor=supervisor)
    clock_observer = ClockObserver(
        supervisor=supervisor, writer=clock_writer, component_name=CLOCK_OBSERVER_COMPONENT,
    )

    return {
        "supervisor": supervisor,
        "gap_ledger_path": gap_ledger_path,
        "runtime_dir": runtime_dir,
        "prior_checkpoint": checkpoint,
        # Evidence writers are started/stopped with the market-data writers, so
        # their drop/backlog counters are produced and reported the same way.
        "writers": [
            agg_writer, depth_writer, force_order_writer,
            snapshot_writer, clock_writer, rest_telemetry_writer,
        ],
        "writers_by_component": {
            "aggTrade": agg_writer, "diff_depth": depth_writer, "forceOrder": force_order_writer,
        },
        "evidence_writers_by_stream": {
            "depth_snapshot": snapshot_writer,
            "clock_observations": clock_writer,
            "rest_telemetry": rest_telemetry_writer,
        },
        "agg_collector": agg_collector,
        "depth_collector": depth_collector,
        "force_order_collector": force_order_collector,
        "clock_observer": clock_observer,
    }


def _count_file_bytes_and_lines(path):
    path = Path(path)
    if not path.exists():
        return 0, 0
    size = path.stat().st_size
    lines = 0
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                lines += 1
    return size, lines


def build_soak_summary(
    components, *, symbol, runtime_dir, started_at_iso, ended_at_iso,
    planned_duration_seconds, actual_duration_seconds,
):
    """Measure this run: per-stream volume, throughput, queue behaviour and
    gap-ledger outcome.

    Produced so a readiness evaluation has MEASURED numbers to reason about
    instead of estimates. It deliberately applies no pass/fail threshold of
    its own -- EvidenceHunter_collector_readiness.py is the only place a verdict
    is formed. Writing this file creates no formal Dataset and no Dataset id.
    """
    runtime_dir = Path(runtime_dir)
    supervisor = components["supervisor"]
    writers_by_component = components["writers_by_component"]
    snapshot = supervisor.snapshot()

    seconds = actual_duration_seconds if actual_duration_seconds > 0 else None
    streams = {}
    writers = {}
    total_bytes = 0
    total_messages = 0

    for name, writer in writers_by_component.items():
        health = snapshot.get(name, {})
        raw_bytes, raw_lines = _count_file_bytes_and_lines(writer.path)
        bytes_per_second = (raw_bytes / seconds) if seconds else None
        projected_gb_per_day = (
            (bytes_per_second * 86400.0) / (1024.0 ** 3) if bytes_per_second is not None else None
        )
        stale_events = sum(
            1 for entry in supervisor.get(name).history if entry.get("to") == "STALE"
        )
        total_bytes += raw_bytes
        total_messages += health.get("messages_received", 0) or 0

        streams[name] = {
            "component": name,
            "raw_file": str(writer.path),
            "message_count": health.get("messages_received"),
            "raw_bytes": raw_bytes,
            "raw_lines": raw_lines,
            "bytes_per_second": bytes_per_second,
            "projected_gb_per_day": projected_gb_per_day,
            "final_state": health.get("state"),
            "reconnect_count": health.get("reconnect_count"),
            "resync_count": health.get("resync_count"),
            "stale_events": stale_events,
            "last_transition_reason": health.get("last_transition_reason"),
        }
        writers[name] = {
            "submitted_count": writer.submitted_count,
            "written_count": writer.written_count,
            "dropped_count": writer.dropped_count,
            "queue_maxsize": writer.maxsize,
            "queue_depth_final": writer.queue_depth(),
            "queue_depth_peak": writer.queue_depth_peak,
            "backlog_at_stop": writer.submitted_count - writer.written_count,
        }

    # COLLECTOR_DAY1_COMPLETENESS_FIX / E6: ALL collector-persisted evidence
    # bytes are counted, not just the three market streams. They are reported
    # as additional entries in the existing streams/writers objects, which
    # keeps the COLLECTOR_SOAK_SUMMARY_V1 schema unchanged: the readiness
    # tool's G4 checks only the three named market streams, while G5 sums
    # whatever streams are present -- so the totals stay internally consistent
    # AND now describe real storage growth instead of understating it.
    for name, writer in sorted((components.get("evidence_writers_by_stream") or {}).items()):
        raw_bytes, raw_lines = _count_file_bytes_and_lines(writer.path)
        bytes_per_second = (raw_bytes / seconds) if seconds else None
        projected_gb_per_day = (
            (bytes_per_second * 86400.0) / (1024.0 ** 3) if bytes_per_second is not None else None
        )
        total_bytes += raw_bytes
        component_name = EVIDENCE_STREAM_COMPONENT.get(name)
        health = snapshot.get(component_name, {}) if component_name else {}
        streams[name] = {
            "component": name,
            "stream_kind": "EVIDENCE",
            "raw_file": str(writer.path),
            "message_count": writer.written_count,
            "raw_bytes": raw_bytes,
            "raw_lines": raw_lines,
            "bytes_per_second": bytes_per_second,
            "projected_gb_per_day": projected_gb_per_day,
            "final_state": health.get("state"),
            "reconnect_count": None,
            "resync_count": None,
            "stale_events": None,
            "last_transition_reason": health.get("last_transition_reason"),
        }
        writers[name] = {
            "submitted_count": writer.submitted_count,
            "written_count": writer.written_count,
            "dropped_count": writer.dropped_count,
            "queue_maxsize": writer.maxsize,
            "queue_depth_final": writer.queue_depth(),
            "queue_depth_peak": writer.queue_depth_peak,
            "backlog_at_stop": writer.submitted_count - writer.written_count,
        }

    try:
        gaps = gap_ledger.materialize_latest(components["gap_ledger_path"])
    except gap_ledger.GapLedgerError as error:
        gaps = []
        gap_stats = {"error": str(error)}
    else:
        gap_stats = {
            "gap_count": len(gaps),
            "repaired_count": sum(1 for g in gaps if g.get("repair_status") == "REPAIRED"),
            "unrecoverable_count": sum(
                1 for g in gaps if g.get("repair_status") == "UNRECOVERABLE_CONFIRMED"
            ),
            "open_at_end_count": sum(1 for g in gaps if g.get("repair_status") == "OPEN"),
        }

    total_bps = (total_bytes / seconds) if seconds else None
    return {
        "schema": "COLLECTOR_SOAK_SUMMARY_V1",
        "symbol": symbol,
        "runtime_dir": str(runtime_dir),
        "formal_dataset_id": None,
        "formal_dataset_created": False,
        "started_at_iso": started_at_iso,
        "ended_at_iso": ended_at_iso,
        "planned_duration_seconds": planned_duration_seconds,
        "actual_duration_seconds": actual_duration_seconds,
        "streams": streams,
        "writers": writers,
        "totals": {
            "message_count": total_messages,
            "raw_bytes": total_bytes,
            "bytes_per_second": total_bps,
            "projected_gb_per_day": (
                (total_bps * 86400.0) / (1024.0 ** 3) if total_bps is not None else None
            ),
            # E6 scope statement: the legacy field name raw_bytes is kept so the
            # unmodified readiness tool still validates it, but it now covers
            # every persisted evidence stream as well. message_count is left
            # deliberately narrow -- it counts market messages, and inflating it
            # with evidence records would corrupt a market-rate metric.
            "raw_bytes_scope": (
                "ALL collector-persisted evidence bytes: market streams "
                "(aggTrade, diff_depth, forceOrder) PLUS evidence streams "
                "(depth_snapshot, clock_observations, rest_telemetry)"
            ),
            "message_count_scope": "market stream messages only; excludes evidence records",
        },
        "evidence_streams": list(EVIDENCE_STREAMS),
        "evidence_sink_failures": {
            "diff_depth_order_book": list(
                getattr(getattr(components.get("depth_collector"), "order_book", None),
                        "evidence_sink_failures", []) or []
            ),
            "aggTrade_rest": list(
                getattr(components.get("agg_collector"), "evidence_sink_failures", []) or []
            ),
        },
        "clock_observation_count": getattr(
            components.get("clock_observer"), "observation_count", None),
        "gap_ledger": gap_stats,
        "supervisor_final_snapshot": snapshot,
        # G6 evidence: what the previous process left behind, and what this
        # process concluded from it.
        "prior_checkpoint": components.get("prior_checkpoint"),
        "restart_boundary": {
            "was_restart": components.get("prior_checkpoint") is not None,
            "aggTrade_restart_gap_id": getattr(
                components.get("agg_collector"), "restart_gap_id", None),
            "diff_depth_restart_gap_id": getattr(
                components.get("depth_collector"), "restart_gap_id", None),
        },
        "final_checkpoint": read_continuity_checkpoint(runtime_dir),
    }


def main():
    from EvidenceHunter_collector_runtime_control import Session
    args = parse_args()
    session = Session(args.dataset_id, args.runtime_dir)
    args.runtime_dir = str(session.root)
    try:
        _run_session(args, session)
    except BaseException as error:
        session.fail(error)
        raise
    finally:
        session.close()


def _run_session(args, session):
    print("COLLECTOR_STATUS: STARTING")
    print("SYMBOL             :", args.symbol)
    print("RUNTIME_DIR        :", args.runtime_dir)
    print("TRADE_PERMISSION   : False")
    print("FORMAL_DATASET     : NOT_CREATED (this script never creates one)")
    print("MARK_PRICE_STREAM  : DEFERRED (not implemented this changeset)")
    print("DAY1_EVIDENCE      :", ", ".join(EVIDENCE_STREAMS))
    print("EXTERNAL_NTP_QUERY : NONE (Windows OS-native time-sync evidence only)")

    components = None
    threads = []
    checkpoint_thread = clock_thread = None
    stop_event = threading.Event()
    checkpoint_stop = threading.Event()
    clock_stop = threading.Event()
    sink_before = None
    try:
        components = build_components(args.symbol, args.runtime_dir)
        prior = components["prior_checkpoint"]
        print("PRIOR_CHECKPOINT   :", "NONE (fresh start)" if prior is None else
              json.dumps(prior["components"], ensure_ascii=False))
        print("CHECKPOINT_USE     : evidence + gap detection only; never restores state")

        started_at_iso = _now_iso()
        started_monotonic_ms = monotonic_ms()
        for writer in components["writers"]:
            writer.start()
        components["depth_collector"].ensure_initial_sync_with_network_retry(
            retry_network_errors=True,
            on_retry=lambda event: print(
                "[INITIAL_DEPTH_SYNC] " + json.dumps(event, ensure_ascii=False)
            ),
        )

        supervisor = components["supervisor"]
        symbol_lower = args.symbol.lower()

        def lifecycle_logger(component_name):
            def _log(event):
                print(f"[WS_LIFECYCLE] component={component_name} {json.dumps(event, ensure_ascii=False)}")
            return _log

        specs = [
            ("aggTrade", ROUTE_MARKET, f"{symbol_lower}@aggTrade", components["agg_collector"].handle_message),
            ("diff_depth", ROUTE_PUBLIC, f"{symbol_lower}@depth", components["depth_collector"].handle_message),
            ("forceOrder", ROUTE_MARKET, f"{symbol_lower}@forceOrder", components["force_order_collector"].handle_message),
        ]

        for name, route_class, stream_name, handler in specs:
            thread = threading.Thread(
                target=run_component_lifecycle,
                kwargs=dict(
                    name=name,
                    url_fn=lambda rc=route_class, sn=stream_name: resolve_stream_url(rc, [sn]),
                    route_class=route_class,
                    stream_name=stream_name,
                    on_message=handler,
                    supervisor=supervisor,
                    stop_event=stop_event,
                    silent_stall_seconds=SILENT_STALL_SECONDS,
                    stall_check_interval_seconds=STALL_CHECK_INTERVAL_SECONDS,
                    on_lifecycle_event=lifecycle_logger(name),
                ),
                name=f"lifecycle-{name}", daemon=True,
            )
            thread.start()
            threads.append(thread)

        # G6: the checkpoint is refreshed on a short cycle so that a process kill
        # at an arbitrary moment still leaves a recent, durable high-water mark
        # behind for the next process to compare against.

        def checkpoint_loop():
            while not checkpoint_stop.wait(CONTINUITY_CHECKPOINT_INTERVAL_SECONDS):
                try:
                    write_continuity_checkpoint(args.runtime_dir, components["writers_by_component"])
                except OSError as error:
                    print(f"[CHECKPOINT_WRITE_ERROR] {error}")

        checkpoint_thread = threading.Thread(
            target=checkpoint_loop, name="continuity-checkpoint", daemon=True
        )
        checkpoint_thread.start()

        # COLLECTOR_DAY1_COMPLETENESS_FIX / GAP2: periodic OS-native clock
        # observation, supervised like any other component.
        clock_thread = threading.Thread(
            target=components["clock_observer"].run,
            kwargs=dict(stop_event=clock_stop, on_lifecycle_event=lifecycle_logger("clock_observer")),
            name="clock-observer", daemon=True,
        )
        clock_thread.start()
        cause = session.wait(args.duration_seconds)
    finally:
        # Reuse the shutdown path even after partial startup or a stop-ACK error.
        if components is not None:
            sink_before = {
                "diff_depth_order_book": list(getattr(getattr(components.get("depth_collector"), "order_book", None), "evidence_sink_failures", []) or []),
                "aggTrade_rest": list(getattr(components.get("agg_collector"), "evidence_sink_failures", []) or []),
            }
        stop_event.set()
        checkpoint_stop.set()
        clock_stop.set()
        for thread in [clock_thread, checkpoint_thread, *threads]:
            if thread is not None:
                thread.join(timeout=15.0)
        if components is not None:
            for writer in components["writers"]:
                writer.stop()
    # Final checkpoint AFTER the writers have drained, so it reflects
    # everything actually on disk.
    write_continuity_checkpoint(args.runtime_dir, components["writers_by_component"])

    ended_at_iso = _now_iso()
    actual_duration_seconds = (monotonic_ms() - started_monotonic_ms) / 1000.0

    print("COLLECTOR_STATUS: STOPPED (" + ("operator request" if cause == "GRACEFUL_STOP_CONFIRMED" else "duration elapsed") + ")")
    print(json.dumps(supervisor.snapshot(), ensure_ascii=False, indent=2))

    summary = build_soak_summary(
        components,
        symbol=args.symbol,
        runtime_dir=args.runtime_dir,
        started_at_iso=started_at_iso,
        ended_at_iso=ended_at_iso,
        planned_duration_seconds=args.duration_seconds,
        actual_duration_seconds=actual_duration_seconds,
    )
    summary_path = Path(args.runtime_dir) / "soak_summary.json"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    checks = {
        "component_threads_stopped": all(not t.is_alive() for t in [clock_thread, checkpoint_thread, *threads] if t is not None),
        "writer_threads_stopped": all(w._thread is not None and not w._thread.is_alive() for w in components["writers"]),
        "writers_drained": all(w.queue_depth() == 0 and w.written_count == w.submitted_count for w in components["writers"]),
        "no_unresolved_finalization_sink_failure": summary["evidence_sink_failures"] == sink_before,
    }
    session.finalize(cause, checks, continuity_checkpoint_path(args.runtime_dir), summary_path)
    print(f"SOAK_SUMMARY_WRITTEN: {summary_path}")
    print("TOTAL_RAW_BYTES    :", summary["totals"]["raw_bytes"], "(includes evidence streams)")
    print("CLOCK_OBSERVATIONS :", summary.get("clock_observation_count"))
    print("PROJECTED_GB_PER_DAY:", summary["totals"]["projected_gb_per_day"])


if __name__ == "__main__":
    main()

