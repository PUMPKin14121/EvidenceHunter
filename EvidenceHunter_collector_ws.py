# -*- coding: utf-8 -*-
"""BTC HUNTER collector WebSocket wrapper - USDS-M Futures split-route V1.

Binance restructured USDS-M Futures WebSocket market streams onto three
routed base paths (independently verified 2026-09-07 against
developers.binance.com's websocket-market-streams / change-log /
Important-WebSocket-Change-Notice pages; PROJECT_OWNER/GPT decision recorded
the same day):

    MARKET  -> aggTrade, forceOrder, markPrice (markPrice stream itself is
               DEFERRED this changeset -- WS field-level schema could not be
               cleanly verified from the primary docs page; only the route
               classification is documented/reserved here)
    PUBLIC  -> diff depth, book ticker, partial depth

No fallback to the pre-restructuring flat wss://fstream.binance.com/ws/...
path is implemented -- see
research/governance/SCOPE_DECLARATION_NEXT_DATASET_COLLECTOR_IMPLEMENTATION_20260907.json.

This module deliberately does NOT use websocket-client's own reconnect
facilities (e.g. run_forever(reconnect=...)). Every WSConnection instance
represents exactly one connection attempt with no internal retry: when its
receive loop ends for any reason, it transitions to DISCONNECTED and stops.
Deciding whether/when to create a new WSConnection (i.e. reconnect) is the
exclusive responsibility of EvidenceHunter_collector_supervisor.run_component_lifecycle
(via EvidenceHunter_collector.py) -- this keeps every recovery decision in one
auditable, testable place instead of two independently-acting mechanisms.

A connection is not considered HEALTHY on TCP/WebSocket handshake alone. It
must additionally receive at least one *valid* (JSON object) payload within
CONNECTION_READY_TIMEOUT_SECONDS of connecting. Until then it is
STREAM_NOT_READY; if the timeout elapses with no valid payload, the
connection gives up (DISCONNECTED) rather than being silently trusted.
"""

import json
import threading
import time
from datetime import datetime, timezone

try:
    import websocket
except ImportError as error:  # pragma: no cover - exercised only when the
    # dependency is genuinely missing, which local self-tests stub out.
    raise ImportError(
        "MISSING_REQUIRED_DEPENDENCY: websocket-client==1.9.2 is required for "
        "EvidenceHunter_collector_ws.py but is not importable in this environment. "
        "Install it into the project's own .venv only, e.g.: "
        "D:\\EvidenceHunter\\.venv\\Scripts\\python.exe -m pip install --no-deps websocket-client==1.9.2"
    ) from error

from EvidenceHunter_clock import local_wall_ms, monotonic_ms
from EvidenceHunter_collector_network import network_routes, route_evidence, websocket_options

BASE_WS_HOST = "wss://fstream.binance.com"
ROUTE_MARKET = "market"
ROUTE_PUBLIC = "public"
VALID_ROUTES = (ROUTE_MARKET, ROUTE_PUBLIC)

STATE_CONNECTING = "CONNECTING"
STATE_STREAM_NOT_READY = "STREAM_NOT_READY"
STATE_HEALTHY = "HEALTHY"
STATE_DISCONNECTED = "DISCONNECTED"

CONNECTION_READY_TIMEOUT_SECONDS = 10.0
RECV_SOCKET_TIMEOUT_SECONDS = 5.0
# Binance closes WS connections after this long regardless of activity; this
# is normal operating behavior (see Binance's documented ~24h connection
# lifetime), not a fault, and must be logged distinctly from a real error.
BINANCE_MAX_CONNECTION_LIFETIME_SECONDS = 24 * 60 * 60


class WSConnectionError(RuntimeError):
    pass


def resolve_stream_url(route_class, stream_names):
    """Build the current split-route WS URL for one or more stream names.

    A single stream name uses the raw-stream URL form (.../ws/<name>); more
    than one uses the combined-stream form (.../stream?streams=a/b/c). Both
    live under the route-classified base path (/market or /public).
    """
    if route_class not in VALID_ROUTES:
        raise WSConnectionError(f"INVALID_ROUTE_CLASS: {route_class}")
    names = list(stream_names)
    if not names:
        raise WSConnectionError("NO_STREAM_NAMES_GIVEN")
    if len(names) == 1:
        return f"{BASE_WS_HOST}/{route_class}/ws/{names[0]}"
    return f"{BASE_WS_HOST}/{route_class}/stream?streams=" + "/".join(names)


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


class WSConnection:
    """One connection attempt's lifecycle and raw message pump.

    on_message(payload_dict, meta) is called from the background receive
    thread for every successfully JSON-decoded object payload. meta carries
    route_class/stream_name/received_local_wall_ms/received_monotonic_ms.

    on_lifecycle_event(event) is called for every notable lifecycle moment
    (connect, first payload, error, close) with a dict that always includes
    resolved_base_url/route_class/stream_name, so the caller can persist WS
    lifecycle logging without this class knowing about any particular log
    format or storage backend.

    connect_fn can replace the whole connection step in older fault-injection
    tests. network_routes_fn/websocket_connect_fn test the real AUTO routing
    path without network access.
    """

    def __init__(
        self,
        url,
        *,
        route_class,
        stream_name,
        on_message,
        on_lifecycle_event=None,
        connection_ready_timeout_seconds=CONNECTION_READY_TIMEOUT_SECONDS,
        recv_socket_timeout_seconds=RECV_SOCKET_TIMEOUT_SECONDS,
        max_connection_lifetime_seconds=BINANCE_MAX_CONNECTION_LIFETIME_SECONDS,
        connect_fn=None,
        network_routes_fn=None,
        websocket_connect_fn=None,
        network_mode_state=None,
    ):
        self.url = url
        self.route_class = route_class
        self.stream_name = stream_name
        self._on_message = on_message
        self._on_lifecycle_event = on_lifecycle_event or (lambda event: None)
        self._ready_timeout = connection_ready_timeout_seconds
        self._recv_timeout = recv_socket_timeout_seconds
        self._max_lifetime = max_connection_lifetime_seconds
        self._connect_fn = connect_fn
        self._network_routes_fn = network_routes_fn or network_routes
        self._websocket_connect_fn = websocket_connect_fn or websocket.create_connection
        self._network_mode_state = network_mode_state if network_mode_state is not None else {}

        self.state = STATE_CONNECTING
        self.resolved_base_url = url
        self.connected_at = None
        self.first_payload_at = None
        self.first_payload_valid = None
        self.last_message_monotonic_ms = None
        self.close_reason = None
        self.effective_network_mode = None

        self._ws = None
        self._thread = None
        self._stop_requested = threading.Event()
        self._ready_event = threading.Event()

    def _emit(self, event_type, **fields):
        event = {
            "event_type": event_type,
            "route_class": self.route_class,
            "stream_name": self.stream_name,
            "resolved_base_url": self.resolved_base_url,
            "wall_time_ms": local_wall_ms(),
            "monotonic_ms": monotonic_ms(),
            "wall_time_iso": _now_iso(),
        }
        if self.effective_network_mode is not None:
            event["effective_network_mode"] = self.effective_network_mode
        event.update(fields)
        self._on_lifecycle_event(event)

    def start(self):
        if self._thread is not None:
            raise WSConnectionError("ALREADY_STARTED")
        self._thread = threading.Thread(target=self._run, name=f"ws-{self.stream_name}", daemon=True)
        self._thread.start()
        return self

    def _connect_with_network_policy(self):
        last_error = None
        for route in self._network_routes_fn(self.url):
            evidence = route_evidence(route)
            self._emit("CONNECTIVITY_ATTEMPT", **evidence)
            try:
                connection = self._websocket_connect_fn(
                    self.url,
                    timeout=self._recv_timeout,
                    **websocket_options(route),
                )
            except Exception as error:  # websocket-client uses several network exception types
                last_error = error
                self._emit(
                    "CONNECTIVITY_LOST",
                    **evidence,
                    failure_exception_type=type(error).__name__,
                )
                continue

            previous_mode = self._network_mode_state.get("effective_network_mode")
            self.effective_network_mode = route["mode"]
            if previous_mode is not None and previous_mode != self.effective_network_mode:
                self._emit(
                    "NETWORK_MODE_CHANGED",
                    previous_network_mode=previous_mode,
                    **evidence,
                )
            self._network_mode_state["effective_network_mode"] = self.effective_network_mode
            self._emit("CONNECTIVITY_RESTORED", **evidence)
            return connection

        if last_error is not None:
            raise WSConnectionError(
                f"ALL_NETWORK_ROUTES_FAILED:{type(last_error).__name__}"
            ) from last_error
        raise WSConnectionError("NO_NETWORK_ROUTE_AVAILABLE")

    def _run(self):
        try:
            self._ws = (
                self._connect_fn()
                if self._connect_fn is not None
                else self._connect_with_network_policy()
            )
        except Exception as error:
            self.state = STATE_DISCONNECTED
            self.close_reason = f"CONNECT_FAILED: {type(error).__name__}: {error}"
            self._emit("CONNECT_FAILED", detail=self.close_reason)
            self._ready_event.set()
            return

        self.connected_at = _now_iso()
        self.state = STATE_STREAM_NOT_READY
        self._emit("CONNECTED", connected_at=self.connected_at)

        ready_deadline = time.monotonic() + self._ready_timeout
        lifetime_deadline = time.monotonic() + self._max_lifetime

        while not self._stop_requested.is_set():
            if self.state == STATE_STREAM_NOT_READY and time.monotonic() > ready_deadline:
                self.state = STATE_DISCONNECTED
                self.close_reason = "STREAM_NOT_READY_TIMEOUT"
                self._emit("STREAM_NOT_READY_TIMEOUT")
                self._ready_event.set()
                break
            if time.monotonic() > lifetime_deadline:
                self.state = STATE_DISCONNECTED
                self.close_reason = "MAX_CONNECTION_LIFETIME_REACHED"
                self._emit("MAX_CONNECTION_LIFETIME_REACHED")
                break
            try:
                raw = self._ws.recv()
            except websocket.WebSocketTimeoutException:
                continue
            except Exception as error:
                self.state = STATE_DISCONNECTED
                self.close_reason = f"RECV_ERROR: {type(error).__name__}: {error}"
                self._emit("RECV_ERROR", detail=self.close_reason)
                self._ready_event.set()
                break

            if not raw:
                self.state = STATE_DISCONNECTED
                self.close_reason = "EMPTY_FRAME_CLOSED"
                self._emit("CLOSED", detail=self.close_reason)
                self._ready_event.set()
                break

            self.last_message_monotonic_ms = monotonic_ms()
            valid = False
            payload = None
            try:
                payload = json.loads(raw)
                valid = isinstance(payload, dict)
            except Exception:
                valid = False

            # first_payload_at/first_payload_valid record the very first
            # message received AT ALL (even if invalid), for diagnostics.
            # The readiness gate (HEALTHY + ready_event) below is separate
            # and deliberately keyed on the first VALID payload instead --
            # an invalid frame must never satisfy "handshake + subscription +
            # first valid payload within a bounded timeout".
            if self.first_payload_at is None:
                self.first_payload_at = _now_iso()
                self.first_payload_valid = valid
                self._emit(
                    "FIRST_PAYLOAD",
                    first_payload_at=self.first_payload_at,
                    first_payload_valid=valid,
                )

            if valid:
                if self.state != STATE_HEALTHY:
                    self.state = STATE_HEALTHY
                    self._emit("HEALTHY")
                    self._ready_event.set()
                try:
                    self._on_message(payload, {
                        "route_class": self.route_class,
                        "stream_name": self.stream_name,
                        "received_local_wall_ms": local_wall_ms(),
                        "received_monotonic_ms": monotonic_ms(),
                    })
                except Exception as error:
                    self._emit("ON_MESSAGE_HANDLER_ERROR", detail=f"{type(error).__name__}: {error}")
            else:
                self._emit("INVALID_PAYLOAD_DROPPED", raw_preview=str(raw)[:200])

        try:
            if self._ws is not None:
                self._ws.close()
        except Exception:
            pass
        if self.state != STATE_DISCONNECTED:
            self.state = STATE_DISCONNECTED
        self._emit("DISCONNECTED", close_reason=self.close_reason)

    def wait_until_ready_or_failed(self, timeout=None):
        """Block until HEALTHY, or the connection has given up (DISCONNECTED)."""
        effective_timeout = self._ready_timeout + 1.0 if timeout is None else timeout
        self._ready_event.wait(timeout=effective_timeout)
        return self.state

    def stop(self):
        self._stop_requested.set()
        try:
            if self._ws is not None:
                self._ws.close()
        except Exception:
            pass
        if self._thread is not None:
            self._thread.join(timeout=5.0)

    def is_silent(self, max_silence_seconds):
        """True when HEALTHY but no message has arrived for too long -- the
        'component silently stopped without any error' failure mode."""
        if self.state != STATE_HEALTHY or self.last_message_monotonic_ms is None:
            return False
        return (monotonic_ms() - self.last_message_monotonic_ms) / 1000.0 > max_silence_seconds


