# -*- coding: utf-8 -*-
"""BTC HUNTER local order book reconstruction - diff depth + REST snapshot.

Implements Binance's official U/u/pu continuity procedure for maintaining a
local order book from the diff depth WebSocket stream:

  1. Buffer diff depth events while a REST snapshot (GET /fapi/v1/depth) is
     fetched.
  2. Discard buffered events whose u <= snapshot lastUpdateId.
  3. The first applied event must satisfy U <= lastUpdateId + 1 <= u.
  4. Every event after that must have pu == previous applied event's u;
     otherwise the local book is desynchronized and a resync (back to step 1)
     is required.

This module does not open the WebSocket itself (EvidenceHunter_collector_ws.py
does); it only consumes already-decoded diff depth payloads pushed to it via
ingest(), plus a REST snapshot fetch function it owns. It never resyncs
itself automatically -- ingest() reports DESYNCED and the caller (
EvidenceHunter_collector.DiffDepthCollector, under Supervisor's authority)
decides whether/when to call start_resync() again, with its own bounded
retry budget before giving up.
"""

import base64
import json
import time
import urllib.parse
import uuid
from urllib.error import HTTPError, URLError
from urllib.request import Request

from EvidenceHunter_clock import local_wall_ms, monotonic_ms
from EvidenceHunter_collector_network import open_url

REST_BASE_URL = "https://fapi.binance.com"
REQUEST_TIMEOUT_SECONDS = 10
DEPTH_SNAPSHOT_LIMIT = 1000
DEPTH_SNAPSHOT_ENDPOINT = "/fapi/v1/depth"
DEPTH_SNAPSHOT_RECORD_SCHEMA = "COLLECTOR_DEPTH_SNAPSHOT_EVIDENCE_V1"

RESYNC_NOT_STARTED = "NOT_STARTED"
RESYNC_BUFFERING = "BUFFERING"
RESYNC_SYNCHRONIZED = "SYNCHRONIZED"
RESYNC_DESYNCED = "DESYNCED"


class OrderBookError(RuntimeError):
    pass


class OrderBookNetworkError(OrderBookError):
    """A snapshot failed before any HTTP response/data could be obtained."""

    pass


def _emit_evidence(sink, event, sink_failures):
    """COLLECTOR_DAY1_COMPLETENESS_FIX: evidence sinks are OBSERVERS.

    A sink must never be able to change order-book control flow -- if writing
    evidence could raise into start_resync(), an evidence hiccup would be
    turned into a resync failure and then classified as a market gap, which is
    exactly the gap/restart semantic drift this changeset forbids. So a failing
    sink is recorded (never silently swallowed: the count surfaces in
    snapshot() and in the run summary) and control flow continues unchanged.
    """
    if sink is None:
        return
    try:
        sink(event)
    except Exception as error:  # noqa: BLE001 -- deliberate: see docstring
        if sink_failures is not None:
            sink_failures.append(f"{type(error).__name__}: {error}")


def fetch_depth_snapshot(
    symbol, *, limit=DEPTH_SNAPSHOT_LIMIT, request_fn=None,
    snapshot_sink=None, telemetry_sink=None, http_fn=None, sink_failures=None,
):
    """GET /fapi/v1/depth. Injectable request_fn(symbol, limit) for tests --
    no real network is used when request_fn is provided.

    COLLECTOR_DAY1_COMPLETENESS_FIX (GAP1/GAP3): when sinks are supplied, this
    also emits Day-1 raw truth that used to be discarded:

      * snapshot_sink  -- the HTTP response body EXACTLY as received, captured
        BEFORE json parsing, plus lastUpdateId and request/receive provenance.
        The body is never re-serialised from the parsed object: the parsed
        object is produced FROM the captured text, so the two are provably the
        same input, and a parser change can never silently rewrite what the
        "raw" field claims the exchange sent.
      * telemetry_sink -- one record per REST request (success, HTTP error or
        network error) carrying the rest_request_id, endpoint, parameters,
        HTTP status, response headers and timing. Sanitisation/redaction is
        performed by the sink, which is the single place that decides what may
        be persisted (see EvidenceHunter_collector._sanitize_rest_telemetry).

    Both records carry the SAME rest_request_id, giving transaction-level
    provenance between a stored snapshot and the request that produced it.

    http_fn injects the transport (urlopen) rather than the parsed result, so
    tests can exercise this real code path with a response body whose exact
    bytes are known. The default transport uses the collector's shared AUTO
    system-proxy/direct policy.
    """
    rest_request_id = uuid.uuid4().hex
    params = {"symbol": symbol, "limit": limit}
    requested_local_wall_ms = local_wall_ms()
    requested_monotonic_ms = monotonic_ms()
    network_route_events = []

    def _telemetry(outcome, *, received_pair, http_status=None, response_headers=None,
                   failure_code=None, exception_type=None, errno=None):
        received_wall, received_monotonic = received_pair
        _emit_evidence(telemetry_sink, {
            "rest_request_id": rest_request_id,
            "request_class": "DEPTH_SNAPSHOT",
            "endpoint_path": DEPTH_SNAPSHOT_ENDPOINT,
            "params": dict(params),
            "outcome": outcome,
            "http_status": http_status,
            "response_headers": response_headers or {},
            "network_route_events": list(network_route_events),
            # F3: structured, fail-closed failure provenance only. The free-text
            # exception message is deliberately NOT part of the persisted record:
            # an exception string can carry a signature/token/cookie from a URL
            # or header and would bypass the params/header allow-lists entirely.
            # The runtime exceptions raised to callers keep their original text.
            "failure_code": failure_code,
            "failure_exception_type": exception_type,
            "failure_errno": errno,
            "requested_local_wall_ms": requested_local_wall_ms,
            "requested_monotonic_ms": requested_monotonic_ms,
            "received_local_wall_ms": received_wall,
            "received_monotonic_ms": received_monotonic,
        }, sink_failures)

    def _snapshot_record(*, received_pair, raw_body, raw_body_unavailable_reason,
                         http_status, parse_status, last_update_id=None,
                         parse_error_type=None, raw_body_base64=None):
        received_wall, received_monotonic = received_pair
        return {
            "schema": DEPTH_SNAPSHOT_RECORD_SCHEMA,
            "rest_request_id": rest_request_id,
            "symbol": symbol,
            "endpoint_path": DEPTH_SNAPSHOT_ENDPOINT,
            "params": dict(params),
            "last_update_id": last_update_id,
            "raw_body": raw_body,
            "raw_body_base64": raw_body_base64,
            "raw_body_representation": (
                "UTF8_TEXT" if raw_body is not None else
                "BASE64_BYTES" if raw_body_base64 is not None else None
            ),
            "raw_body_unavailable_reason": raw_body_unavailable_reason,
            "http_status": http_status,
            # F1: a snapshot attempt leaves evidence whether or not the body
            # parsed. PARSED is the only value that counts as a usable Day-1
            # order-book snapshot; the others exist so an unparsable or failed
            # response is preserved instead of vanishing.
            "parse_status": parse_status,
            "parse_error_type": parse_error_type,
            "acquisition_mode": "LIVE",
            "requested_local_wall_ms": requested_local_wall_ms,
            "requested_monotonic_ms": requested_monotonic_ms,
            "received_local_wall_ms": received_wall,
            "received_monotonic_ms": received_monotonic,
        }

    if request_fn is not None:
        parsed = request_fn(symbol, limit)
        # F2: ONE receive instant per REST transaction, shared by both records.
        received_pair = (local_wall_ms(), monotonic_ms())
        # An injected request_fn hands back an already-parsed object, so there
        # is no HTTP body to capture. That is recorded as unavailable rather
        # than faked by re-serialising the parsed object.
        _telemetry("INJECTED_REQUEST_FN_NO_HTTP_LAYER", received_pair=received_pair)
        _emit_evidence(snapshot_sink, _snapshot_record(
            received_pair=received_pair, raw_body=None,
            raw_body_unavailable_reason="INJECTED_REQUEST_FN_NO_HTTP_BODY",
            http_status=None, parse_status="PARSED",
            last_update_id=parsed.get("lastUpdateId") if isinstance(parsed, dict) else None,
        ), sink_failures)
        return parsed

    query = urllib.parse.urlencode(params)
    url = f"{REST_BASE_URL}{DEPTH_SNAPSHOT_ENDPOINT}?{query}"
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
            # F2: captured immediately after the body is in hand, exactly once.
            received_pair = (local_wall_ms(), monotonic_ms())
            http_status = getattr(response, "status", None)
            if http_status is None:
                http_status = getattr(response, "code", None)
            response_headers = headers_to_dict(getattr(response, "headers", None))
    except HTTPError as error:
        received_pair = (local_wall_ms(), monotonic_ms())
        error_status = getattr(error, "code", None)
        _telemetry(
            "HTTP_ERROR", received_pair=received_pair, http_status=error_status,
            response_headers=headers_to_dict(getattr(error, "headers", None)),
            failure_code="DEPTH_SNAPSHOT_HTTP_ERROR", exception_type=type(error).__name__,
        )
        # F1: an HTTP error still carries a response body on Binance (the error
        # JSON). Retain it verbatim rather than discarding evidence of what the
        # exchange actually said.
        error_body, error_body_base64, error_body_reason = _read_error_body(error)
        _emit_evidence(snapshot_sink, _snapshot_record(
            received_pair=received_pair, raw_body=error_body,
            raw_body_base64=error_body_base64,
            raw_body_unavailable_reason=error_body_reason, http_status=error_status,
            parse_status="NOT_PARSED_HTTP_ERROR",
        ), sink_failures)
        raise OrderBookError(f"DEPTH_SNAPSHOT_HTTP_ERROR status={error.code}") from error
    except URLError as error:
        received_pair = (local_wall_ms(), monotonic_ms())
        _telemetry(
            "NETWORK_ERROR", received_pair=received_pair,
            failure_code="DEPTH_SNAPSHOT_NETWORK_ERROR", exception_type=type(error).__name__,
            errno=safe_errno(error),
        )
        _emit_evidence(snapshot_sink, _snapshot_record(
            received_pair=received_pair, raw_body=None,
            raw_body_unavailable_reason="NO_HTTP_RESPONSE", http_status=None,
            parse_status="NOT_PARSED_NO_RESPONSE",
        ), sink_failures)
        raise OrderBookNetworkError(
            f"DEPTH_SNAPSHOT_NETWORK_ERROR reason={error.reason}"
        ) from error

    _telemetry("SUCCESS", received_pair=received_pair, http_status=http_status,
               response_headers=response_headers)

    # F1: decode/parse failures must not destroy the raw body. The evidence is
    # emitted on BOTH paths, and the original exception is re-raised unchanged
    # so no caller's control flow or gap classification moves.
    try:
        raw_body_text = body_bytes.decode("utf-8")
    except UnicodeDecodeError as error:
        _emit_evidence(snapshot_sink, _snapshot_record(
            received_pair=received_pair, raw_body=None,
            raw_body_base64=base64.b64encode(body_bytes).decode("ascii"),
            raw_body_unavailable_reason=None, http_status=http_status,
            parse_status="NOT_PARSED_DECODE_FAILED", parse_error_type=type(error).__name__,
        ), sink_failures)
        raise

    try:
        parsed = json.loads(raw_body_text)
    except ValueError as error:
        _emit_evidence(snapshot_sink, _snapshot_record(
            received_pair=received_pair, raw_body=raw_body_text,
            raw_body_unavailable_reason=None, http_status=http_status,
            parse_status="PARSE_FAILED", parse_error_type=type(error).__name__,
        ), sink_failures)
        raise

    _emit_evidence(snapshot_sink, _snapshot_record(
        received_pair=received_pair, raw_body=raw_body_text,
        raw_body_unavailable_reason=None, http_status=http_status,
        parse_status="PARSED",
        last_update_id=parsed.get("lastUpdateId") if isinstance(parsed, dict) else None,
    ), sink_failures)
    return parsed


def _read_error_body(error):
    """Return (UTF-8 text, base64 bytes, unavailable reason).

    F6: existing non-UTF8 bytes remain lossless evidence. Retrieval failure
    must not replace the original HTTPError control flow.
    """
    if getattr(error, "fp", None) is None:
        return None, None, "HTTP_ERROR_NO_BODY"
    try:
        body = error.read()
    except Exception:  # noqa: BLE001 -- preserve the original HTTP error
        return None, None, "HTTP_ERROR_BODY_UNREADABLE"
    if body is None:
        return None, None, "HTTP_ERROR_NO_BODY"
    try:
        return body.decode("utf-8"), None, None
    except UnicodeDecodeError:
        return None, base64.b64encode(body).decode("ascii"), None


def safe_errno(error):
    """Extract an integer errno, or None. Structured failure detail only.

    Like headers_to_dict() this holds no policy and must never grow any: it
    exists so persisted failure provenance can be structured rather than a
    free-text exception message, which is what F3 forbids on disk."""
    value = getattr(getattr(error, "reason", None), "errno", None)
    return value if isinstance(value, int) else None


def headers_to_dict(headers):
    """HTTP response-header normalization helper. Nothing more.

    Turns a response's header container into a plain str->str dict. It holds
    NO policy: it does not filter, allow-list, redact or classify anything,
    and it must never grow such logic -- that would create a second, divergent
    copy of a security-relevant decision. Redaction and the persisted-field
    allow-list live in exactly one place, the telemetry sink
    (EvidenceHunter_collector._sanitize_rest_telemetry), which is the only thing
    standing between these raw headers and disk."""
    if headers is None:
        return {}
    try:
        return {str(k): str(v) for k, v in headers.items()}
    except Exception:  # noqa: BLE001 -- a header mapping must never break a fetch
        return {}


def _apply_side(levels, updates):
    for price_str, qty_str in updates:
        qty = float(qty_str)
        if qty == 0.0:
            levels.pop(price_str, None)
        else:
            levels[price_str] = qty_str


class LocalOrderBook:
    """One symbol's locally-reconstructed order book with continuity tracking."""

    def __init__(self, symbol, *, request_fn=None, snapshot_sink=None,
                 telemetry_sink=None, http_fn=None):
        self.symbol = symbol
        self._request_fn = request_fn
        # COLLECTOR_DAY1_COMPLETENESS_FIX: optional, inert-by-default evidence
        # sinks. With all three left as None this class behaves exactly as
        # before, which is why the existing order-book tests are unaffected.
        self._snapshot_sink = snapshot_sink
        self._telemetry_sink = telemetry_sink
        self._http_fn = http_fn
        self.evidence_sink_failures = []
        self.bids = {}
        self.asks = {}
        self.last_update_id = None
        self.status = RESYNC_NOT_STARTED
        self._buffer = []
        self.resync_count = 0
        self.last_snapshot_at_monotonic = None

    def start_resync(self):
        """Fetch a fresh REST snapshot and enter BUFFERING, then immediately
        try to anchor against anything already buffered from before this
        call (nothing is lost while a snapshot fetch was in flight).

        COLLECTOR_DAY1_COMPLETENESS_FIX: this is the ONE place a snapshot is
        obtained, for the first initialization and for every later resync
        alike, so persisting evidence here covers "every initialization and
        resync, not just the first" without adding a second code path.
        """
        snapshot = fetch_depth_snapshot(
            self.symbol, request_fn=self._request_fn,
            snapshot_sink=self._snapshot_sink, telemetry_sink=self._telemetry_sink,
            http_fn=self._http_fn, sink_failures=self.evidence_sink_failures,
        )
        self.bids = {p: q for p, q in snapshot["bids"]}
        self.asks = {p: q for p, q in snapshot["asks"]}
        self.last_update_id = int(snapshot["lastUpdateId"])
        self.status = RESYNC_BUFFERING
        self.resync_count += 1
        self.last_snapshot_at_monotonic = time.monotonic()
        self._drain_buffer_against_snapshot()
        return self.last_update_id

    def _drain_buffer_against_snapshot(self):
        while self._buffer and self.status == RESYNC_BUFFERING:
            event = self._buffer[0]
            u = int(event["u"])
            if u <= self.last_update_id:
                self._buffer.pop(0)
                continue
            U = int(event["U"])
            if U <= self.last_update_id + 1 <= u:
                self._buffer.pop(0)
                self._apply_event_unchecked(event)
                self.status = RESYNC_SYNCHRONIZED
                remaining, self._buffer = self._buffer, []
                for later_event in remaining:
                    self.ingest(later_event)
                return
            # The first still-relevant buffered event does not straddle
            # lastUpdateId + 1 as Binance's documented procedure requires --
            # the snapshot is unusable against this buffer; caller must
            # resync again with a fresh snapshot.
            self.status = RESYNC_DESYNCED
            self._buffer.pop(0)
            return

    def _apply_event_unchecked(self, event):
        _apply_side(self.bids, event.get("b", []))
        _apply_side(self.asks, event.get("a", []))
        self.last_update_id = int(event["u"])

    def ingest(self, event):
        """Route one diff-depth event based on current status. Returns one
        of "APPLIED", "SYNCHRONIZED", "DESYNCED", or "BUFFERED"."""
        if self.status == RESYNC_SYNCHRONIZED:
            pu = event.get("pu")
            if pu is None:
                raise OrderBookError("EVENT_MISSING_PU_FIELD")
            if int(pu) != int(self.last_update_id):
                self.status = RESYNC_DESYNCED
                return "DESYNCED"
            self._apply_event_unchecked(event)
            return "APPLIED"

        # NOT_STARTED / BUFFERING / DESYNCED: buffer until a resync anchors it.
        self._buffer.append(event)
        if self.status == RESYNC_BUFFERING:
            self._drain_buffer_against_snapshot()
            if self.status == RESYNC_SYNCHRONIZED:
                return "SYNCHRONIZED"
            if self.status == RESYNC_DESYNCED:
                return "DESYNCED"
        return "BUFFERED"

    def apply_event(self, event):
        """Direct single-event apply, used by tests to exercise the
        not-synchronized guard explicitly. ingest() is the normal entrypoint."""
        if self.status != RESYNC_SYNCHRONIZED:
            raise OrderBookError(f"APPLY_EVENT_WHILE_NOT_SYNCHRONIZED: status={self.status}")
        return self.ingest(event)

    def snapshot(self):
        return {
            "symbol": self.symbol,
            "status": self.status,
            "last_update_id": self.last_update_id,
            "bid_levels": len(self.bids),
            "ask_levels": len(self.asks),
            "resync_count": self.resync_count,
            "buffered_events": len(self._buffer),
            # Never silently zero: a failing evidence sink is visible here and
            # in the run summary even though it cannot disturb reconstruction.
            "evidence_sink_failure_count": len(self.evidence_sink_failures),
        }

