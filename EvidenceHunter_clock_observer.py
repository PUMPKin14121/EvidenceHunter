# -*- coding: utf-8 -*-
"""BTC HUNTER local clock / time-sync observation component.

COLLECTOR_DAY1_COMPLETENESS_FIX / GAP2. OF2_PRESTART_DESIGN_V1 classifies
"local NTP sync status / system clock offset (periodic sampling)" as
must_collect_from_day_1 with irrecoverable_if_missing = true: it is what
later tells a researcher how far the wall-clock receive timestamps of a given
period can be trusted. It was never implemented, so this module adds it.

Three rules govern this module, all of them frozen by PROJECT_OWNER + GPT:

  * WINDOWS OS-NATIVE EVIDENCE ONLY. The observation comes from the operating
    system's own time-synchronization report (w32tm /query /status /verbose). This
    changeset does NOT query any external or public NTP server; that option
    is explicitly out of scope this round.
  * NEVER FABRICATE. If the OS cannot supply a value, the record says so --
    availability=UNAVAILABLE with a reason, offset_seconds=None,
    offset_available=False. A missing offset is never defaulted to 0, never
    interpolated, and never carried forward from an earlier observation.
  * OBSERVE, NEVER CORRECT. Nothing here adjusts, corrects or smooths any
    collected market timestamp. OF2_PRESTART_DESIGN_V1 forbids smoothed or
    averaged timestamps outright; this is evidence ABOUT the clock, sitting
    beside the data, not a correction applied to it.

Component health vs. evidence availability are deliberately separate: the
component is HEALTHY while it is faithfully observing and recording, even if
what it records is "the OS could not tell me". A failing OS query is an
observation, not a component failure. Only an uncaught exception escaping the
loop is a component failure, and it is reported through the existing
Supervisor state machine as STATE_LIFECYCLE_THREAD_CRASHED -- the same G1
guarantee the WebSocket components have, reached through the Supervisor's
public API without modifying EvidenceHunter_collector_supervisor.py.
"""

import re
import subprocess
import sys

from EvidenceHunter_clock import local_wall_ms, monotonic_ms
from EvidenceHunter_collector_supervisor import (
    STATE_HEALTHY,
    STATE_LIFECYCLE_THREAD_CRASHED,
    STATE_STREAM_NOT_READY,
    SupervisorError,
)

CLOCK_OBSERVATION_SCHEMA = "COLLECTOR_CLOCK_OBSERVATION_V1"
CLOCK_OBSERVATION_INTERVAL_SECONDS = 60.0
CLOCK_QUERY_TIMEOUT_SECONDS = 10
OBSERVATION_SOURCE = "WINDOWS_W32TM_QUERY_STATUS"
EXTERNAL_TIME_SERVER_QUERIED = False

AVAILABILITY_AVAILABLE = "AVAILABLE"
AVAILABILITY_UNAVAILABLE = "UNAVAILABLE"

# "Phase Offset: 0.0001234s" / "Phase Offset: -0.0072s". Localised Windows
# builds may print a different label, in which case the offset is reported as
# unparsable rather than guessed at.
_PHASE_OFFSET_RE = re.compile(r"^\s*(?:Phase Offset|相位偏移)\s*:\s*(-?[0-9]+(?:\.[0-9]+)?)\s*s\s*$",
                              re.IGNORECASE | re.MULTILINE)
_SIMPLE_FIELDS = {
    "leap_indicator": ("Leap Indicator", "Leap 指示符"),
    "stratum": ("Stratum", "层次"),
    "precision": ("Precision", "精度"),
    "root_delay": ("Root Delay", "根延迟"),
    "root_dispersion": ("Root Dispersion", "根分散"),
    "reference_id": ("ReferenceId", "引用 ID"),
    "last_successful_sync_time": ("Last Successful Sync Time", "上次成功同步时间"),
    "source": ("Source", "源"),
}


def _extract_field(text, labels):
    pattern = re.compile(r"^[ \t]*(?:" + "|".join(re.escape(label) for label in labels)
                         + r")[ \t]*:[ \t]*([^\r\n]+?)[ \t]*\r?$",
                         re.IGNORECASE | re.MULTILINE)
    match = pattern.search(text or "")
    return (match.group(1).strip() or None) if match else None


def default_query_fn():
    """Run the OS's own time-sync status query. Returns
    (returncode, stdout, stderr). Never raises for a non-zero exit code --
    that is data, not an error."""
    completed = subprocess.run(
        ["w32tm", "/query", "/status", "/verbose"],
        capture_output=True, text=True, timeout=CLOCK_QUERY_TIMEOUT_SECONDS,
    )
    return completed.returncode, completed.stdout, completed.stderr


def observe_clock(*, query_fn=None, platform=None):
    """Produce exactly one clock observation record.

    Returns a record in every case. There is no code path that returns
    nothing, and no code path that invents an offset.
    """
    platform = platform if platform is not None else sys.platform
    record = {
        "schema": CLOCK_OBSERVATION_SCHEMA,
        "observed_local_wall_ms": local_wall_ms(),
        "observed_monotonic_ms": monotonic_ms(),
        "source": OBSERVATION_SOURCE,
        "external_time_server_queried": EXTERNAL_TIME_SERVER_QUERIED,
        "platform": platform,
        "availability": AVAILABILITY_UNAVAILABLE,
        "unavailable_reason": None,
        "offset_seconds": None,
        "offset_available": False,
        "sync_status": {},
        "raw_output": None,
        "returncode": None,
    }

    if not str(platform).startswith("win"):
        record["unavailable_reason"] = (
            f"OS_NATIVE_TIME_SYNC_QUERY_NOT_AVAILABLE_ON_PLATFORM:{platform}"
        )
        return record

    query = query_fn if query_fn is not None else default_query_fn
    try:
        returncode, stdout, stderr = query()
    except Exception as error:  # noqa: BLE001 -- an OS query failure is data
        record["unavailable_reason"] = f"CLOCK_QUERY_FAILED:{type(error).__name__}: {error}"
        return record

    record["returncode"] = returncode
    record["raw_output"] = stdout
    if returncode != 0:
        record["unavailable_reason"] = (
            f"CLOCK_QUERY_NONZERO_EXIT:{returncode}:{(stderr or '').strip()[:400]}"
        )
        return record

    record["sync_status"] = {
        key: _extract_field(stdout, label) for key, label in _SIMPLE_FIELDS.items()
    }
    if not all(record["sync_status"][key] for key in ("source", "stratum", "leap_indicator")):
        record["unavailable_reason"] = "SYNC_STATUS_NOT_PARSED"
        return record
    # Availability means real OS state was captured, including unsynchronized state.
    record["availability"] = AVAILABILITY_AVAILABLE

    match = _PHASE_OFFSET_RE.search(stdout or "")
    if match is None:
        # The OS answered, but did not report an offset in a form this parser
        # recognises. Recorded as exactly that -- not as zero.
        record["unavailable_reason"] = "OFFSET_NOT_REPORTED_OR_NOT_PARSABLE"
        return record
    try:
        record["offset_seconds"] = float(match.group(1))
    except ValueError:
        record["unavailable_reason"] = f"OFFSET_NOT_NUMERIC:{match.group(1)!r}"
        return record
    record["offset_available"] = True
    return record


class ClockObserver:
    """Periodic clock observation as a Supervisor-registered component.

    The caller registers component_name with the Supervisor and runs run() in
    its own thread. Crash observability matches the WebSocket components': an
    exception escaping the loop transitions the component to
    STATE_LIFECYCLE_THREAD_CRASHED (which alerts and persists through the
    Supervisor's normal transition path), emits a lifecycle event, and is
    re-raised so the traceback stays visible. A requested shutdown is not an
    exception and can never be misclassified as a crash.
    """

    def __init__(self, *, supervisor, writer, component_name="clock_observer",
                 interval_seconds=CLOCK_OBSERVATION_INTERVAL_SECONDS,
                 query_fn=None, platform=None):
        self.supervisor = supervisor
        self.writer = writer
        self.component_name = component_name
        self.interval_seconds = interval_seconds
        self._query_fn = query_fn
        self._platform = platform
        self.observation_count = 0

    def observe_once(self):
        record = observe_clock(query_fn=self._query_fn, platform=self._platform)
        self.writer.submit(record)
        self.observation_count += 1
        self.supervisor.record_message(
            self.component_name, local_wall_ms=record["observed_local_wall_ms"]
        )
        return record

    def run(self, *, stop_event, on_lifecycle_event=None):
        try:
            self._run_body(stop_event=stop_event)
        except Exception as error:  # noqa: BLE001 -- G1: never a silent death
            detail = f"{type(error).__name__}: {error}"
            try:
                self.supervisor.transition(
                    self.component_name, STATE_LIFECYCLE_THREAD_CRASHED,
                    reason=f"UNCAUGHT_EXCEPTION: {detail}",
                )
            except SupervisorError:
                pass
            if on_lifecycle_event is not None:
                on_lifecycle_event({
                    "event_type": "LIFECYCLE_THREAD_CRASHED",
                    "component": self.component_name,
                    "detail": detail,
                })
            raise

    def _run_body(self, *, stop_event):
        self.supervisor.transition(
            self.component_name, STATE_STREAM_NOT_READY, reason="CLOCK_OBSERVER_STARTING"
        )
        first = True
        while True:
            self.observe_once()
            if first:
                # HEALTHY means "this observer is faithfully recording", which
                # includes recording that the OS could not answer. Evidence
                # availability is a field in the record, never a component state.
                self.supervisor.transition(
                    self.component_name, STATE_HEALTHY, reason="CLOCK_OBSERVATION_RECORDED"
                )
                first = False
            if stop_event.wait(self.interval_seconds):
                return

