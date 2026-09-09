"""Fault-injection coverage for EvidenceHunter_collector_ws.WSConnection: no real
network is used anywhere in this file -- every test injects connect_fn with a
FakeSocket standing in for the real websocket-client connection object."""
import json
import threading
import time

import pytest

import EvidenceHunter_collector_ws as ws_mod
from EvidenceHunter_collector_ws import (
    ROUTE_MARKET,
    ROUTE_PUBLIC,
    STATE_DISCONNECTED,
    STATE_HEALTHY,
    WSConnection,
    WSConnectionError,
    resolve_stream_url,
)


def test_resolve_stream_url_single_and_combined():
    assert resolve_stream_url(ROUTE_MARKET, ["btcusdt@aggTrade"]) == "wss://fstream.binance.com/market/ws/btcusdt@aggTrade"
    combined = resolve_stream_url(ROUTE_PUBLIC, ["btcusdt@depth", "ethusdt@depth"])
    assert combined == "wss://fstream.binance.com/public/stream?streams=btcusdt@depth/ethusdt@depth"


def test_resolve_stream_url_rejects_unknown_route():
    with pytest.raises(WSConnectionError):
        resolve_stream_url("private", ["btcusdt@aggTrade"])


def test_resolve_stream_url_rejects_empty_names():
    with pytest.raises(WSConnectionError):
        resolve_stream_url(ROUTE_MARKET, [])


class FakeSocket:
    """Yields frames from a list, then blocks (simulating a silent stall),
    raises, or returns an empty frame, depending on after_frames_behavior."""

    def __init__(self, frames, after_frames_behavior="block"):
        self._frames = list(frames)
        self._behavior = after_frames_behavior
        self.closed = False

    def recv(self):
        if self._frames:
            return self._frames.pop(0)
        if self._behavior == "block":
            time.sleep(0.02)
            raise ws_mod.websocket.WebSocketTimeoutException()
        if self._behavior == "error":
            raise RuntimeError("SIMULATED_RECV_ERROR")
        if self._behavior == "empty":
            return ""
        raise AssertionError("unreachable")

    def close(self):
        self.closed = True


def _messages_collector():
    received = []
    lock = threading.Lock()

    def on_message(payload, meta):
        with lock:
            received.append(payload)

    return received, on_message


def test_first_valid_payload_marks_healthy_and_logs_lifecycle():
    frames = [json.dumps({"e": "aggTrade", "a": 1})]
    events = []
    received, on_message = _messages_collector()
    conn = WSConnection(
        "wss://fstream.binance.com/market/ws/btcusdt@aggTrade",
        route_class="market", stream_name="btcusdt@aggTrade",
        on_message=on_message, on_lifecycle_event=events.append,
        connect_fn=lambda: FakeSocket(frames, after_frames_behavior="block"),
        connection_ready_timeout_seconds=1.0,
    )
    conn.start()
    state = conn.wait_until_ready_or_failed()
    assert state == STATE_HEALTHY
    assert received == [{"e": "aggTrade", "a": 1}]
    event_types = [e["event_type"] for e in events]
    assert "CONNECTED" in event_types
    assert "FIRST_PAYLOAD" in event_types
    assert "HEALTHY" in event_types
    for event in events:
        assert event["route_class"] == "market"
        assert event["stream_name"] == "btcusdt@aggTrade"
    conn.stop()


def test_stream_not_ready_timeout_when_no_payload_ever_arrives():
    """Component silent stall BEFORE ever becoming healthy: handshake
    succeeds but no valid payload arrives within the bounded timeout."""
    received, on_message = _messages_collector()
    conn = WSConnection(
        "wss://fstream.binance.com/market/ws/btcusdt@aggTrade",
        route_class="market", stream_name="btcusdt@aggTrade",
        on_message=on_message,
        connect_fn=lambda: FakeSocket([], after_frames_behavior="block"),
        connection_ready_timeout_seconds=0.3,
    )
    conn.start()
    state = conn.wait_until_ready_or_failed()
    assert state == STATE_DISCONNECTED
    assert conn.close_reason == "STREAM_NOT_READY_TIMEOUT"
    conn.stop()


def test_silent_stall_detected_after_healthy():
    """Component goes HEALTHY, then stops sending anything -- is_silent()
    must detect this without any recv() error ever being raised."""
    frames = [json.dumps({"e": "aggTrade", "a": 1})]
    received, on_message = _messages_collector()
    conn = WSConnection(
        "wss://fstream.binance.com/market/ws/btcusdt@aggTrade",
        route_class="market", stream_name="btcusdt@aggTrade",
        on_message=on_message,
        connect_fn=lambda: FakeSocket(frames, after_frames_behavior="block"),
        connection_ready_timeout_seconds=1.0,
    )
    conn.start()
    conn.wait_until_ready_or_failed()
    assert conn.state == STATE_HEALTHY
    time.sleep(0.05)  # ensure measurable elapsed time has actually passed
    assert conn.is_silent(max_silence_seconds=0.0) is True
    assert conn.is_silent(max_silence_seconds=1000.0) is False
    conn.stop()


def test_recv_error_marks_disconnected():
    """Simulated WebSocket disconnect (recv raises)."""
    received, on_message = _messages_collector()
    events = []
    conn = WSConnection(
        "wss://fstream.binance.com/market/ws/btcusdt@aggTrade",
        route_class="market", stream_name="btcusdt@aggTrade",
        on_message=on_message, on_lifecycle_event=events.append,
        connect_fn=lambda: FakeSocket([], after_frames_behavior="error"),
        connection_ready_timeout_seconds=1.0,
    )
    conn.start()
    conn.wait_until_ready_or_failed()
    assert conn.state == STATE_DISCONNECTED
    assert "SIMULATED_RECV_ERROR" in conn.close_reason
    assert any(e["event_type"] == "RECV_ERROR" for e in events)
    conn.stop()


def test_max_connection_lifetime_reached_is_a_normal_closure_not_an_error():
    """24h-style reconnect path: the connection closes itself once its max
    lifetime is reached, distinctly logged (not conflated with RECV_ERROR)."""
    frames = [json.dumps({"e": "aggTrade", "a": 1})]
    events = []
    received, on_message = _messages_collector()
    conn = WSConnection(
        "wss://fstream.binance.com/market/ws/btcusdt@aggTrade",
        route_class="market", stream_name="btcusdt@aggTrade",
        on_message=on_message, on_lifecycle_event=events.append,
        connect_fn=lambda: FakeSocket(frames, after_frames_behavior="block"),
        connection_ready_timeout_seconds=1.0,
        max_connection_lifetime_seconds=0.2,
    )
    conn.start()
    conn.wait_until_ready_or_failed()
    time.sleep(0.5)
    assert conn.state == STATE_DISCONNECTED
    assert conn.close_reason == "MAX_CONNECTION_LIFETIME_REACHED"
    assert any(e["event_type"] == "MAX_CONNECTION_LIFETIME_REACHED" for e in events)
    conn.stop()


def test_no_second_reconnect_authority_is_used_by_this_wrapper():
    """WSConnection must not itself decide to reconnect -- once DISCONNECTED
    it stays DISCONNECTED forever; only a Supervisor creating a brand new
    WSConnection counts as a reconnect."""
    received, on_message = _messages_collector()
    conn = WSConnection(
        "wss://fstream.binance.com/market/ws/btcusdt@aggTrade",
        route_class="market", stream_name="btcusdt@aggTrade",
        on_message=on_message,
        connect_fn=lambda: FakeSocket([], after_frames_behavior="empty"),
        connection_ready_timeout_seconds=1.0,
    )
    conn.start()
    conn.wait_until_ready_or_failed()
    assert conn.state == STATE_DISCONNECTED
    time.sleep(0.2)
    assert conn.state == STATE_DISCONNECTED
    conn.stop()


def test_connect_failure_marks_disconnected_without_ever_being_ready():
    def broken_connect():
        raise RuntimeError("SIMULATED_HANDSHAKE_FAILURE")

    received, on_message = _messages_collector()
    events = []
    conn = WSConnection(
        "wss://fstream.binance.com/market/ws/btcusdt@aggTrade",
        route_class="market", stream_name="btcusdt@aggTrade",
        on_message=on_message, on_lifecycle_event=events.append,
        connect_fn=broken_connect, connection_ready_timeout_seconds=1.0,
    )
    conn.start()
    state = conn.wait_until_ready_or_failed()
    assert state == STATE_DISCONNECTED
    assert "SIMULATED_HANDSHAKE_FAILURE" in conn.close_reason
    assert any(e["event_type"] == "CONNECT_FAILED" for e in events)
    conn.stop()


def test_invalid_json_payload_is_dropped_and_does_not_count_as_first_payload():
    frames = ["not json at all", json.dumps({"e": "aggTrade", "a": 1})]
    received, on_message = _messages_collector()
    events = []
    conn = WSConnection(
        "wss://fstream.binance.com/market/ws/btcusdt@aggTrade",
        route_class="market", stream_name="btcusdt@aggTrade",
        on_message=on_message, on_lifecycle_event=events.append,
        connect_fn=lambda: FakeSocket(frames, after_frames_behavior="block"),
        connection_ready_timeout_seconds=1.0,
    )
    conn.start()
    state = conn.wait_until_ready_or_failed()
    assert state == STATE_HEALTHY
    assert received == [{"e": "aggTrade", "a": 1}]
    assert conn.first_payload_valid is False  # the invalid frame arrived first
    assert any(e["event_type"] == "INVALID_PAYLOAD_DROPPED" for e in events)
    conn.stop()

