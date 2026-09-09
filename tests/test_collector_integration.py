"""End-to-end wiring smoke tests using fakes only -- no real network. Exercises
AggTradeCollector / DiffDepthCollector / ForceOrderCollector composed with a
real RawPayloadWriter, real Supervisor, and real gap_ledger against tmp_path,
plus the full run_component_lifecycle reconnect cycle driven by a fake
WebSocket-like object."""
import json
import threading
import time

import EvidenceHunter_collector as collector
import EvidenceHunter_collector_ws as ws_mod
import EvidenceHunter_gap_ledger as gl
from EvidenceHunter_collector_supervisor import (
    STATE_HEALTHY,
    STATE_RECONNECTING,
    STATE_STREAM_NOT_READY,
    Supervisor,
    run_component_lifecycle,
)
from EvidenceHunter_collector_ws import ROUTE_MARKET, ROUTE_PUBLIC, resolve_stream_url


def _meta(wall_ms=0):
    return {"received_local_wall_ms": wall_ms, "received_monotonic_ms": wall_ms}


def _healthy_supervisor(tmp_path, name):
    supervisor = Supervisor(state_file=tmp_path / "supervisor.json")
    supervisor.register(name)
    supervisor.transition(name, STATE_STREAM_NOT_READY, reason="CONNECTED")
    supervisor.transition(name, STATE_HEALTHY, reason="FIRST_VALID_PAYLOAD_RECEIVED")
    return supervisor


def test_aggtrade_collector_detects_gap_and_backfills_via_injected_rest(tmp_path):
    gap_path = tmp_path / "quality" / "gap_ledger.jsonl"
    writer = collector.RawPayloadWriter(tmp_path / "raw" / "aggTrade.jsonl")
    writer.start()
    supervisor = _healthy_supervisor(tmp_path, "aggTrade")

    backfilled_rows = [{"a": 101, "p": "100.0", "q": "1", "T": 1, "m": False}]

    def fake_rest(path, params):
        assert path == "/fapi/v1/aggTrades"
        assert params["fromId"] == 101
        return backfilled_rows

    agg = collector.AggTradeCollector(
        "BTCUSDT", gap_ledger_path=gap_path, writer=writer, supervisor=supervisor,
        request_fn=fake_rest,
    )
    agg.handle_message({"a": 100}, _meta(1))
    agg.handle_message({"a": 102}, _meta(2))  # 101 is missing -> gap

    deadline = time.monotonic() + 2.0
    while writer.written_count < 3 and time.monotonic() < deadline:
        time.sleep(0.02)
    writer.stop()

    latest = gl.materialize_latest(gap_path)
    assert len(latest) == 1
    assert latest[0]["repair_status"] == "REPAIRED"
    assert latest[0]["gap_start"] == 101
    assert latest[0]["gap_end"] == 101

    lines = [json.loads(l) for l in (tmp_path / "raw" / "aggTrade.jsonl").read_text(encoding="utf-8").strip().split("\n")]
    backfilled = [l for l in lines if l.get("acquisition_mode") == "REST_BACKFILL"]
    assert len(backfilled) == 1
    assert backfilled[0]["raw"]["a"] == 101
    live = [l for l in lines if l.get("acquisition_mode") == "LIVE"]
    assert len(live) == 2


def test_diff_depth_collector_full_desync_and_recovery_cycle(tmp_path):
    gap_path = tmp_path / "quality" / "gap_ledger.jsonl"
    writer = collector.RawPayloadWriter(tmp_path / "raw" / "depth.jsonl")
    writer.start()
    supervisor = _healthy_supervisor(tmp_path, "diff_depth")

    snapshot_calls = {"n": 0}

    def fake_snapshot(symbol, limit):
        snapshot_calls["n"] += 1
        base = 1000 if snapshot_calls["n"] == 1 else 2000
        return {"lastUpdateId": base, "bids": [], "asks": []}

    depth = collector.DiffDepthCollector(
        "BTCUSDT", gap_ledger_path=gap_path, writer=writer, supervisor=supervisor,
        request_fn=fake_snapshot,
    )
    depth.ensure_initial_sync()
    depth.handle_message({"U": 999, "u": 1005, "pu": 998, "b": [], "a": []}, _meta(1))
    result = depth.handle_message({"U": 1006, "u": 1010, "pu": 9999, "b": [], "a": []}, _meta(2))
    assert result == "DESYNCED"

    latest = gl.materialize_latest(gap_path)
    assert len(latest) == 1
    assert latest[0]["resync_status"] == "PENDING"

    result2 = depth.handle_message({"U": 1999, "u": 2005, "pu": 1998, "b": [], "a": []}, _meta(3))
    assert result2 == "SYNCHRONIZED"
    latest = gl.materialize_latest(gap_path)
    assert latest[0]["repair_status"] == "REPAIRED"
    writer.stop()


def test_diff_depth_collector_marks_unrecoverable_after_exhausting_resync_budget(tmp_path):
    gap_path = tmp_path / "quality" / "gap_ledger.jsonl"
    writer = collector.RawPayloadWriter(tmp_path / "raw" / "depth.jsonl")
    writer.start()
    supervisor = _healthy_supervisor(tmp_path, "diff_depth")

    def always_stale_snapshot(symbol, limit):
        # Every fresh snapshot is immediately unusable against the next
        # injected event's U/u, forcing repeated desyncs.
        return {"lastUpdateId": 1, "bids": [], "asks": []}

    depth = collector.DiffDepthCollector(
        "BTCUSDT", gap_ledger_path=gap_path, writer=writer, supervisor=supervisor,
        request_fn=always_stale_snapshot, max_resync_attempts=2,
    )
    depth.ensure_initial_sync()
    depth.handle_message({"U": 1, "u": 5, "pu": 0, "b": [], "a": []}, _meta(1))
    assert depth.order_book.status == "SYNCHRONIZED"

    for u in (100, 200, 300):
        depth.handle_message({"U": u, "u": u + 5, "pu": 99999, "b": [], "a": []}, _meta(u))

    latest = gl.materialize_latest(gap_path)
    assert latest[0]["repair_status"] == "UNRECOVERABLE_CONFIRMED"
    writer.stop()


def test_force_order_collector_persists_with_semantics_note(tmp_path):
    writer = collector.RawPayloadWriter(tmp_path / "raw" / "forceOrder.jsonl")
    writer.start()
    supervisor = _healthy_supervisor(tmp_path, "forceOrder")

    force_order = collector.ForceOrderCollector(writer=writer, supervisor=supervisor)
    force_order.handle_message({"e": "forceOrder", "o": {"s": "BTCUSDT"}}, _meta(1))

    deadline = time.monotonic() + 2.0
    while writer.written_count < 1 and time.monotonic() < deadline:
        time.sleep(0.02)
    writer.stop()

    line = json.loads((tmp_path / "raw" / "forceOrder.jsonl").read_text(encoding="utf-8").strip())
    assert "absence of a message" in line["semantics_note"]
    assert line["acquisition_mode"] == "LIVE"


def test_resolve_stream_url_matches_approved_route_architecture():
    assert resolve_stream_url(ROUTE_MARKET, ["btcusdt@aggTrade"]).startswith("wss://fstream.binance.com/market/")
    assert resolve_stream_url(ROUTE_MARKET, ["btcusdt@forceOrder"]).startswith("wss://fstream.binance.com/market/")
    assert resolve_stream_url(ROUTE_PUBLIC, ["btcusdt@depth"]).startswith("wss://fstream.binance.com/public/")


def test_run_component_lifecycle_reconnects_after_silent_stall(tmp_path):
    """Full reconnect-cycle demonstration: Supervisor + run_component_lifecycle
    + WSConnection collaborating, exercising the same 24h-lifetime-style
    'connection ends, a brand new one is created' path without real network."""
    connect_calls = {"n": 0}

    class StallingSocket:
        def __init__(self, index):
            self.index = index
            self.sent_first = False

        def recv(self):
            if not self.sent_first:
                self.sent_first = True
                return json.dumps({"a": self.index})
            time.sleep(0.02)
            raise ws_mod.websocket.WebSocketTimeoutException()

        def close(self):
            pass

    def connect_fn():
        connect_calls["n"] += 1
        return StallingSocket(connect_calls["n"])

    supervisor = Supervisor(state_file=tmp_path / "supervisor.json")
    supervisor.register("aggTrade")
    stop_event = threading.Event()
    received = []

    thread = threading.Thread(
        target=run_component_lifecycle,
        kwargs=dict(
            name="aggTrade", url_fn=lambda: "wss://fstream.binance.com/market/ws/btcusdt@aggTrade",
            route_class="market", stream_name="btcusdt@aggTrade",
            on_message=lambda payload, meta: received.append(payload),
            supervisor=supervisor, stop_event=stop_event,
            silent_stall_seconds=0.1, stall_check_interval_seconds=0.05,
            reconnect_backoff_seconds=0.05, max_reconnect_backoff_seconds=0.1,
            connect_fn=connect_fn,
        ),
        daemon=True,
    )
    thread.start()
    deadline = time.monotonic() + 5.0
    while connect_calls["n"] < 2 and time.monotonic() < deadline:
        time.sleep(0.05)
    stop_event.set()
    thread.join(timeout=5.0)

    assert connect_calls["n"] >= 2  # at least one real reconnect happened
    assert len(received) >= 2
    reconnect_reasons = [
        h["reason"] for h in supervisor.get("aggTrade").history if h["to"] == STATE_RECONNECTING
    ]
    assert any("STALE" in r or "SILENT" in r for r in reconnect_reasons)

