"""EvidenceHunter_collector_supervisor.Supervisor: state-machine legality, alert
firing, restart-state persistence (diagnostic-only, never auto-resume), and
EvidenceHunter_collector.RawPayloadWriter's writer/queue-stall behavior."""
import json
import threading
import time

import pytest

import EvidenceHunter_collector_supervisor as sup
from EvidenceHunter_collector_supervisor import Supervisor, SupervisorError
import EvidenceHunter_collector as collector


def test_valid_transition_sequence_and_persistence(tmp_path):
    state_file = tmp_path / "supervisor_state.json"
    alerts = []
    supervisor = Supervisor(state_file=state_file, alert_fn=alerts.append)
    supervisor.register("aggTrade")
    supervisor.transition("aggTrade", sup.STATE_STREAM_NOT_READY, reason="CONNECTED")
    supervisor.transition("aggTrade", sup.STATE_HEALTHY, reason="FIRST_VALID_PAYLOAD_RECEIVED")
    assert state_file.exists()
    persisted = json.loads(state_file.read_text(encoding="utf-8"))
    assert persisted["components"]["aggTrade"]["state"] == sup.STATE_HEALTHY
    assert alerts == []  # HEALTHY/STREAM_NOT_READY are not alerting transitions


def test_illegal_transition_raises(tmp_path):
    supervisor = Supervisor(state_file=tmp_path / "state.json")
    supervisor.register("aggTrade")
    with pytest.raises(SupervisorError):
        supervisor.transition("aggTrade", sup.STATE_RESYNCING, reason="INVALID_FROM_CONNECTING")


def test_unknown_state_raises(tmp_path):
    supervisor = Supervisor(state_file=tmp_path / "state.json")
    supervisor.register("aggTrade")
    with pytest.raises(SupervisorError):
        supervisor.transition("aggTrade", "NOT_A_REAL_STATE", reason="X")


def test_unknown_component_raises(tmp_path):
    supervisor = Supervisor(state_file=tmp_path / "state.json")
    with pytest.raises(SupervisorError):
        supervisor.transition("never_registered", sup.STATE_STREAM_NOT_READY, reason="X")


def test_stale_and_reconnect_trigger_alert(tmp_path):
    alerts = []
    supervisor = Supervisor(state_file=tmp_path / "state.json", alert_fn=alerts.append)
    supervisor.register("diff_depth")
    supervisor.transition("diff_depth", sup.STATE_STREAM_NOT_READY, reason="CONNECTED")
    supervisor.transition("diff_depth", sup.STATE_HEALTHY, reason="FIRST_VALID_PAYLOAD_RECEIVED")
    supervisor.transition("diff_depth", sup.STATE_STALE, reason="SILENT_STALL_DETECTED")
    supervisor.transition("diff_depth", sup.STATE_RECONNECTING, reason="STALE_TIMEOUT")
    reasons = [a["reason"] for a in alerts]
    assert "SILENT_STALL_DETECTED" in reasons
    assert "STALE_TIMEOUT" in reasons
    assert supervisor.get("diff_depth").reconnect_count == 1


def test_resync_count_increments_on_resyncing_transition(tmp_path):
    supervisor = Supervisor(state_file=tmp_path / "state.json")
    supervisor.register("diff_depth")
    supervisor.transition("diff_depth", sup.STATE_STREAM_NOT_READY, reason="CONNECTED")
    supervisor.transition("diff_depth", sup.STATE_HEALTHY, reason="FIRST_VALID_PAYLOAD_RECEIVED")
    supervisor.transition("diff_depth", sup.STATE_RESYNCING, reason="PU_MISMATCH")
    assert supervisor.get("diff_depth").resync_count == 1


def test_check_silent_stall_detects_without_mutating_state(tmp_path):
    supervisor = Supervisor(state_file=tmp_path / "state.json")
    supervisor.register("aggTrade")
    supervisor.transition("aggTrade", sup.STATE_STREAM_NOT_READY, reason="CONNECTED")
    supervisor.transition("aggTrade", sup.STATE_HEALTHY, reason="FIRST_VALID_PAYLOAD_RECEIVED")
    supervisor.record_message("aggTrade", local_wall_ms=1000)
    assert supervisor.check_silent_stall("aggTrade", max_silence_seconds=1.0, now_wall_ms=1000) is False
    assert supervisor.check_silent_stall("aggTrade", max_silence_seconds=1.0, now_wall_ms=5000) is True
    assert supervisor.get("aggTrade").state == sup.STATE_HEALTHY  # detection alone never transitions


def test_check_silent_stall_false_when_not_healthy(tmp_path):
    supervisor = Supervisor(state_file=tmp_path / "state.json")
    supervisor.register("aggTrade")
    assert supervisor.check_silent_stall("aggTrade", max_silence_seconds=0.0, now_wall_ms=999999) is False


def test_restart_persistence_reports_prior_run_without_resuming_it(tmp_path):
    state_file = tmp_path / "state.json"
    first_run = Supervisor(state_file=state_file)
    first_run.register("aggTrade")
    first_run.transition("aggTrade", sup.STATE_STREAM_NOT_READY, reason="CONNECTED")
    first_run.transition("aggTrade", sup.STATE_HEALTHY, reason="FIRST_VALID_PAYLOAD_RECEIVED")
    first_run.transition("aggTrade", sup.STATE_STALE, reason="PROCESS_ABOUT_TO_CRASH_SIMULATED")

    second_run = Supervisor(state_file=state_file)
    prior = second_run.load_persisted()
    assert prior["components"]["aggTrade"]["state"] == sup.STATE_STALE

    # A fresh process always starts each component from CONNECTING again --
    # the persisted file is diagnostic evidence, not an auto-resume point.
    second_run.register("aggTrade")
    assert second_run.get("aggTrade").state == sup.STATE_CONNECTING


def test_load_persisted_returns_none_when_no_file_yet(tmp_path):
    supervisor = Supervisor(state_file=tmp_path / "never_written.json")
    assert supervisor.load_persisted() is None


def test_corrupt_state_file_raises_on_load(tmp_path):
    state_file = tmp_path / "state.json"
    state_file.write_text("{not valid json", encoding="utf-8")
    supervisor = Supervisor(state_file=state_file)
    with pytest.raises(SupervisorError):
        supervisor.load_persisted()


def test_raw_payload_writer_drains_normally(tmp_path):
    out_path = tmp_path / "raw" / "aggTrade.jsonl"
    writer = collector.RawPayloadWriter(out_path)
    writer.start()
    for i in range(5):
        writer.submit({"i": i})
    deadline = time.monotonic() + 2.0
    while writer.written_count < 5 and time.monotonic() < deadline:
        time.sleep(0.05)
    writer.stop()
    lines = out_path.read_text(encoding="utf-8").strip().split("\n")
    assert len(lines) == 5
    assert writer.dropped_count == 0


def test_raw_payload_writer_queue_stall_drops_visibly_not_silently(tmp_path):
    """Simulate a stalled writer: the background thread is never started, so
    once the bounded queue fills up, submit() must drop with a visible
    on_drop callback -- never an unbounded block and never a silent loss."""
    out_path = tmp_path / "raw" / "aggTrade.jsonl"
    dropped = []
    writer = collector.RawPayloadWriter(
        out_path, maxsize=2, put_timeout_seconds=0.1, on_drop=dropped.append,
    )
    # Deliberately do not call writer.start().
    for i in range(5):
        writer.submit({"i": i})
    assert writer.dropped_count == 3
    assert len(dropped) == 3


def test_concurrent_transitions_from_multiple_threads_do_not_race_on_persist(tmp_path):
    """Regression test for a live-smoke-discovered concurrency bug
    (COLLECTOR_SUPERVISOR_CONCURRENT_PERSIST_FIX_20260907): multiple
    component lifecycle threads sharing one Supervisor instance used to call
    persist() *outside* the lock, so they could race on the same fixed
    '<state_file>.tmp' path. On Windows this could raise PermissionError
    ([WinError 32]) out of os.replace() when one thread's replace collided
    with another thread's still-open temp file -- an uncaught exception in
    that (daemon) lifecycle thread, which silently killed that component's
    reconnect loop for good. persist() is now called inside the same RLock
    critical section as the state mutation in transition(), so every writer
    is fully serialized. A threading.Barrier is used to force every worker
    thread's first transition() call to fire at (as close to) the same
    instant as possible, reproducing the original race deterministically."""
    state_file = tmp_path / "state.json"
    supervisor = Supervisor(state_file=state_file)
    names = [f"component_{i}" for i in range(8)]
    for name in names:
        supervisor.register(name)

    errors = []
    barrier = threading.Barrier(len(names))

    def worker(name):
        try:
            barrier.wait(timeout=5.0)
            supervisor.transition(name, sup.STATE_STREAM_NOT_READY, reason="CONNECTION_ATTEMPT_STARTED")
            supervisor.transition(name, sup.STATE_HEALTHY, reason="FIRST_VALID_PAYLOAD_RECEIVED")
        except Exception as error:  # pragma: no cover - only populated on failure
            errors.append((name, repr(error)))

    threads = [threading.Thread(target=worker, args=(name,)) for name in names]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10.0)

    assert errors == []  # zero concurrent transition exceptions

    persisted = json.loads(state_file.read_text(encoding="utf-8"))  # valid persisted JSON
    for name in names:
        assert persisted["components"][name]["state"] == sup.STATE_HEALTHY  # all expected transitions accounted for
        assert supervisor.get(name).state == sup.STATE_HEALTHY

    tmp_file = state_file.with_suffix(state_file.suffix + ".tmp")
    assert not tmp_file.exists()  # no leftover supervisor_state.json.tmp after completion


def test_raw_payload_writer_is_stalled_detection(tmp_path):
    out_path = tmp_path / "raw" / "aggTrade.jsonl"
    writer = collector.RawPayloadWriter(out_path)
    assert writer.is_stalled(max_silence_seconds=0.0) is False  # nothing drained yet, nothing queued
    writer.start()
    writer.submit({"i": 1})
    deadline = time.monotonic() + 2.0
    while writer.written_count < 1 and time.monotonic() < deadline:
        time.sleep(0.02)
    writer.stop()
    assert writer.is_stalled(max_silence_seconds=1000.0) is False

