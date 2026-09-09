# -*- coding: utf-8 -*-
"""BTC HUNTER collector supervisor - single project-level recovery authority.

Owns, for each named component (e.g. "aggTrade", "diff_depth", "forceOrder"):
  - a state machine: CONNECTING -> STREAM_NOT_READY -> HEALTHY -> STALE ->
    RECONNECTING -> (RESYNCING, diff-depth only) -> HEALTHY. Only the
    transitions listed in ALLOWED_TRANSITIONS are permitted; anything else
    raises SupervisorError rather than silently accepting an invalid jump.
  - health/progress metrics (messages received, last message time, reconnect
    count, resync count, current state, state history).
  - persistence of that state to a local JSON file so a process restart can
    report what it last knew. This is diagnostic evidence ONLY -- per the
    frozen COLLECTOR_RESILIENCE_AND_GAP_RECOVERY_V1 principle (发现坏了 !=
    重新启动 != 恢复连续状态 != 把缺的数据补回来), loading a persisted state
    file never auto-resumes a component; a fresh process always starts every
    component from CONNECTING again.
  - a pluggable alert_fn(alert) hook for a Windows/local alert interface;
    this module has no notification-backend opinion, the caller supplies
    alert_fn (EvidenceHunter_collector.console_and_file_alert is the V1 default).

run_component_lifecycle() is the one place in this project that decides to
create a *new* WSConnection after an old one ends -- i.e. the actual
reconnect decision. websocket-client's own reconnect facilities (e.g.
run_forever(reconnect=...)) are never used anywhere in this project's
collector, so this stays the only, fully auditable recovery authority.
"""

import json
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

from EvidenceHunter_collector_ws import STATE_DISCONNECTED, STATE_HEALTHY as WS_STATE_HEALTHY, WSConnection

STATE_CONNECTING = "CONNECTING"
STATE_STREAM_NOT_READY = "STREAM_NOT_READY"
STATE_HEALTHY = "HEALTHY"
STATE_STALE = "STALE"
STATE_RECONNECTING = "RECONNECTING"
STATE_RESYNCING = "RESYNCING"
# NEXT_DATASET_COLLECTOR_READINESS / G1 (worker/thread unexpected-death
# observability): a component's run_component_lifecycle background thread
# can in principle exit via an uncaught exception (a genuine bug -- expected
# network failures are already absorbed into RECONNECTING/STALE by the
# normal control flow). Before this state existed, such a thread simply died
# with its last-known state left showing stale HEALTHY/STREAM_NOT_READY, as
# if it were still working -- see COLLECTOR_SUPERVISOR_CONCURRENT_PERSIST_FIX
# _20260907 for a real example. STATE_LIFECYCLE_THREAD_CRASHED makes that
# failure explicit and Supervisor-visible instead. It is reachable from every
# other state and has no outgoing transitions: it is terminal for the life
# of this process (a fresh process, not an auto-resume, is the only way out).
STATE_LIFECYCLE_THREAD_CRASHED = "LIFECYCLE_THREAD_CRASHED"

VALID_STATES = (
    STATE_CONNECTING, STATE_STREAM_NOT_READY, STATE_HEALTHY,
    STATE_STALE, STATE_RECONNECTING, STATE_RESYNCING,
    STATE_LIFECYCLE_THREAD_CRASHED,
)

ALLOWED_TRANSITIONS = {
    STATE_CONNECTING: {STATE_STREAM_NOT_READY, STATE_RECONNECTING, STATE_LIFECYCLE_THREAD_CRASHED},
    STATE_STREAM_NOT_READY: {STATE_HEALTHY, STATE_RECONNECTING, STATE_LIFECYCLE_THREAD_CRASHED},
    STATE_HEALTHY: {STATE_STALE, STATE_RECONNECTING, STATE_RESYNCING, STATE_LIFECYCLE_THREAD_CRASHED},
    STATE_STALE: {STATE_RECONNECTING, STATE_HEALTHY, STATE_LIFECYCLE_THREAD_CRASHED},
    STATE_RECONNECTING: {STATE_CONNECTING, STATE_STREAM_NOT_READY, STATE_RECONNECTING, STATE_LIFECYCLE_THREAD_CRASHED},
    STATE_RESYNCING: {STATE_HEALTHY, STATE_RECONNECTING, STATE_LIFECYCLE_THREAD_CRASHED},
    STATE_LIFECYCLE_THREAD_CRASHED: set(),
}

DEFAULT_SILENT_STALL_SECONDS = 30.0
DEFAULT_STALL_CHECK_INTERVAL_SECONDS = 5.0
DEFAULT_RECONNECT_BACKOFF_SECONDS = 2.0
DEFAULT_MAX_RECONNECT_BACKOFF_SECONDS = 30.0


class SupervisorError(RuntimeError):
    pass


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


class ComponentHealth:
    def __init__(self, name):
        self.name = name
        self.state = STATE_CONNECTING
        self.state_entered_at = _now_iso()
        self.messages_received = 0
        self.reconnect_count = 0
        self.resync_count = 0
        self.last_message_wall_ms = None
        self.last_transition_reason = None
        self.history = []

    def transition(self, new_state, *, reason):
        if new_state not in VALID_STATES:
            raise SupervisorError(f"UNKNOWN_STATE: {new_state}")
        allowed = ALLOWED_TRANSITIONS.get(self.state, set())
        if new_state != self.state and new_state not in allowed:
            raise SupervisorError(
                f"ILLEGAL_TRANSITION component={self.name} from={self.state} to={new_state}"
            )
        entry = {"from": self.state, "to": new_state, "reason": reason, "at": _now_iso()}
        self.history.append(entry)
        if new_state == STATE_RECONNECTING:
            self.reconnect_count += 1
        if new_state == STATE_RESYNCING:
            self.resync_count += 1
        self.state = new_state
        self.state_entered_at = entry["at"]
        self.last_transition_reason = reason
        return entry

    def record_message(self, *, local_wall_ms):
        self.messages_received += 1
        self.last_message_wall_ms = local_wall_ms

    def to_dict(self):
        return {
            "name": self.name,
            "state": self.state,
            "state_entered_at": self.state_entered_at,
            "messages_received": self.messages_received,
            "reconnect_count": self.reconnect_count,
            "resync_count": self.resync_count,
            "last_message_wall_ms": self.last_message_wall_ms,
            "last_transition_reason": self.last_transition_reason,
        }


class Supervisor:
    def __init__(self, *, state_file, alert_fn=None):
        self.state_file = Path(state_file)
        self._alert_fn = alert_fn or (lambda alert: None)
        self._components = {}
        # RLock (not Lock): transition() persists to disk while still holding
        # this lock (see transition() below), and persist() calls snapshot(),
        # which re-acquires the same lock from the same thread. A plain Lock
        # would self-deadlock there; RLock allows that same-thread re-entry.
        self._lock = threading.RLock()

    def register(self, name):
        with self._lock:
            if name not in self._components:
                self._components[name] = ComponentHealth(name)
            return self._components[name]

    def get(self, name):
        with self._lock:
            component = self._components.get(name)
            if component is None:
                raise SupervisorError(f"UNKNOWN_COMPONENT: {name}")
            return component

    def transition(self, name, new_state, *, reason):
        # persist() is deliberately called INSIDE this same critical section
        # (not after releasing the lock). Multiple component lifecycle
        # threads share this one Supervisor instance and all write the same
        # state_file via the same fixed .tmp path; persisting outside the
        # lock let two threads race on that .tmp file, which on Windows can
        # raise PermissionError ([WinError 32]) from os.replace() when one
        # thread's replace collides with another thread's still-open temp
        # file -- an uncaught exception in that (daemon) lifecycle thread,
        # silently ending that component's reconnect loop for good. Holding
        # the lock across transition+persist fully serializes every write.
        with self._lock:
            component = self._components.get(name)
            if component is None:
                raise SupervisorError(f"UNKNOWN_COMPONENT: {name}")
            entry = component.transition(new_state, reason=reason)
            self.persist()
        if new_state in (STATE_STALE, STATE_RECONNECTING, STATE_RESYNCING, STATE_LIFECYCLE_THREAD_CRASHED):
            self._alert_fn({"component": name, "new_state": new_state, "reason": reason, "at": entry["at"]})
        return entry

    def record_message(self, name, *, local_wall_ms):
        with self._lock:
            component = self._components.get(name)
            if component is None:
                raise SupervisorError(f"UNKNOWN_COMPONENT: {name}")
            component.record_message(local_wall_ms=local_wall_ms)

    def check_silent_stall(self, name, *, max_silence_seconds, now_wall_ms):
        """True if HEALTHY but no message for longer than max_silence_seconds.
        Detection only -- never transitions state itself, so every state
        change still goes through transition() (and therefore persist() +
        alerting) exactly once."""
        component = self.get(name)
        if component.state != STATE_HEALTHY or component.last_message_wall_ms is None:
            return False
        return (now_wall_ms - component.last_message_wall_ms) / 1000.0 > max_silence_seconds

    def snapshot(self):
        with self._lock:
            return {name: component.to_dict() for name, component in self._components.items()}

    def persist(self):
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        payload = {"persisted_at": _now_iso(), "components": self.snapshot()}
        temp_file = self.state_file.with_suffix(self.state_file.suffix + ".tmp")
        with temp_file.open("w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        temp_file.replace(self.state_file)

    def load_persisted(self):
        """Read back the last-persisted state as diagnostic evidence only.
        Does NOT mutate self._components."""
        if not self.state_file.exists():
            return None
        try:
            return json.loads(self.state_file.read_text(encoding="utf-8"))
        except Exception as error:
            raise SupervisorError(f"CORRUPT_SUPERVISOR_STATE_FILE: {error}") from error


def run_component_lifecycle(
    name,
    *,
    url_fn,
    route_class,
    stream_name,
    on_message,
    supervisor,
    stop_event,
    silent_stall_seconds=DEFAULT_SILENT_STALL_SECONDS,
    stall_check_interval_seconds=DEFAULT_STALL_CHECK_INTERVAL_SECONDS,
    reconnect_backoff_seconds=DEFAULT_RECONNECT_BACKOFF_SECONDS,
    max_reconnect_backoff_seconds=DEFAULT_MAX_RECONNECT_BACKOFF_SECONDS,
    connect_fn=None,
    network_routes_fn=None,
    websocket_connect_fn=None,
    on_lifecycle_event=None,
):
    """Own the full connect -> monitor -> (on failure) reconnect cycle for
    one named component, blocking until stop_event is set. Every reconnect
    creates a brand new WSConnection; the previous one's background thread
    has already exited by the time this function decides to reconnect. This
    is the ONLY place a new connection attempt is created after the first
    one, keeping the actual reconnect decision inside this one auditable
    loop rather than inside WSConnection itself.

    G1 (NEXT_DATASET_COLLECTOR_READINESS, worker/thread unexpected-death
    observability): the actual loop lives in _run_component_lifecycle_body();
    this wrapper's only job is to catch any exception that escapes it (a
    genuine bug -- expected network failures never reach here, WSConnection
    and the loop below already turn those into RECONNECTING/STALE
    transitions), record it as the explicit STATE_LIFECYCLE_THREAD_CRASHED
    component state (which alerts + persists via the normal transition()
    path -- no new incident-storage mechanism), emit a matching
    LIFECYCLE_THREAD_CRASHED lifecycle event, and then re-raise so the
    traceback is still visible. A requested shutdown (stop_event set) or
    normal loop completion is not an exception and can never be
    misclassified as a crash by this wrapper.
    """
    try:
        _run_component_lifecycle_body(
            name,
            url_fn=url_fn,
            route_class=route_class,
            stream_name=stream_name,
            on_message=on_message,
            supervisor=supervisor,
            stop_event=stop_event,
            silent_stall_seconds=silent_stall_seconds,
            stall_check_interval_seconds=stall_check_interval_seconds,
            reconnect_backoff_seconds=reconnect_backoff_seconds,
            max_reconnect_backoff_seconds=max_reconnect_backoff_seconds,
            connect_fn=connect_fn,
            network_routes_fn=network_routes_fn,
            websocket_connect_fn=websocket_connect_fn,
            on_lifecycle_event=on_lifecycle_event,
        )
    except Exception as error:
        detail = f"{type(error).__name__}: {error}"
        try:
            supervisor.transition(
                name, STATE_LIFECYCLE_THREAD_CRASHED, reason=f"UNCAUGHT_EXCEPTION: {detail}"
            )
        except SupervisorError:
            # Even the crash-transition itself failed (e.g. an unknown
            # component name) -- still emit the lifecycle event below and
            # re-raise so this is never silent.
            pass
        if on_lifecycle_event is not None:
            on_lifecycle_event({
                "event_type": "LIFECYCLE_THREAD_CRASHED",
                "component": name,
                "detail": detail,
                "wall_time_iso": _now_iso(),
            })
        raise


def _run_component_lifecycle_body(
    name,
    *,
    url_fn,
    route_class,
    stream_name,
    on_message,
    supervisor,
    stop_event,
    silent_stall_seconds,
    stall_check_interval_seconds,
    reconnect_backoff_seconds,
    max_reconnect_backoff_seconds,
    connect_fn=None,
    network_routes_fn=None,
    websocket_connect_fn=None,
    on_lifecycle_event=None,
):
    backoff = reconnect_backoff_seconds
    network_mode_state = {}
    while not stop_event.is_set():
        supervisor.transition(name, STATE_STREAM_NOT_READY, reason="CONNECTION_ATTEMPT_STARTED")
        conn = WSConnection(
            url_fn(),
            route_class=route_class,
            stream_name=stream_name,
            on_message=on_message,
            on_lifecycle_event=on_lifecycle_event,
            connect_fn=connect_fn,
            network_routes_fn=network_routes_fn,
            websocket_connect_fn=websocket_connect_fn,
            network_mode_state=network_mode_state,
        ).start()
        conn.wait_until_ready_or_failed()
        effective_network_mode = getattr(conn, "effective_network_mode", None)
        mode_suffix = (
            f" network_mode={effective_network_mode}"
            if effective_network_mode is not None else ""
        )

        if conn.state == WS_STATE_HEALTHY:
            backoff = reconnect_backoff_seconds
            supervisor.transition(
                name, STATE_HEALTHY,
                reason="FIRST_VALID_PAYLOAD_RECEIVED" + mode_suffix,
            )
            stalled_or_disconnected = False
            while not stop_event.is_set():
                time.sleep(stall_check_interval_seconds)
                if conn.state == STATE_DISCONNECTED:
                    supervisor.transition(
                        name, STATE_RECONNECTING,
                        reason=f"CONNECTION_DISCONNECTED: {conn.close_reason}" + mode_suffix,
                    )
                    stalled_or_disconnected = True
                    break
                if conn.is_silent(silent_stall_seconds):
                    supervisor.transition(name, STATE_STALE, reason="SILENT_STALL_DETECTED")
                    conn.stop()
                    supervisor.transition(name, STATE_RECONNECTING, reason="STALE_FORCED_RECONNECT")
                    stalled_or_disconnected = True
                    break
            if not stalled_or_disconnected:
                conn.stop()
                break
        else:
            supervisor.transition(
                name, STATE_RECONNECTING,
                reason=f"CONNECTION_NOT_HEALTHY: {conn.state} ({conn.close_reason})" + mode_suffix,
            )
            conn.stop()

        if stop_event.is_set():
            break
        stop_event.wait(backoff)
        backoff = min(max_reconnect_backoff_seconds, backoff * 2)

