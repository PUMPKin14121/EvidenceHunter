import json
import threading
import time
from urllib.error import URLError

import pytest

import EvidenceHunter_collector as collector
import EvidenceHunter_collector_network as network
import EvidenceHunter_collector_ws as ws_mod
import EvidenceHunter_orderbook as orderbook
from EvidenceHunter_collector_network import network_routes, open_url
from EvidenceHunter_collector_supervisor import Supervisor, run_component_lifecycle


class Response:
    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


class DepthResponse(Response):
    status = 200
    headers = {}

    def read(self):
        return b'{"lastUpdateId":1000,"bids":[],"asks":[]}'


def test_direct_route_when_system_proxy_is_absent():
    routes = network_routes(
        "https://fapi.binance.com/fapi/v1/time",
        proxies_fn=lambda: {},
        bypass_fn=lambda _host: False,
    )

    assert routes == [{"mode": "DIRECT"}]


def test_windows_system_proxy_route_precedes_direct_fallback():
    routes = network_routes(
        "wss://fstream.binance.com/market/ws/btcusdt@aggTrade",
        proxies_fn=lambda: {"https": "http://user:secret@127.0.0.1:7897"},
        bypass_fn=lambda _host: False,
    )

    assert routes[0]["mode"] == "SYSTEM_PROXY"
    assert routes[0]["proxy_host"] == "127.0.0.1"
    assert routes[0]["proxy_port"] == 7897
    assert routes[0]["proxy_auth"] == ("user", "secret")
    assert routes[1] == {"mode": "DIRECT"}


def test_proxy_unavailable_falls_back_to_direct_and_records_safe_evidence():
    attempts = []
    events = []

    def open_route(_request, _timeout, route):
        attempts.append(route["mode"])
        if route["mode"] == "SYSTEM_PROXY":
            raise URLError("proxy unavailable")
        return Response()

    response = open_url(
        "https://fapi.binance.com/fapi/v1/time",
        timeout=1,
        proxies_fn=lambda: {"https": "http://user:secret@127.0.0.1:7897"},
        bypass_fn=lambda _host: False,
        open_route_fn=open_route,
        on_event=events.append,
    )

    assert isinstance(response, Response)
    assert attempts == ["SYSTEM_PROXY", "DIRECT"]
    assert [event["event_type"] for event in events] == [
        "CONNECTIVITY_ATTEMPT",
        "CONNECTIVITY_LOST",
        "CONNECTIVITY_ATTEMPT",
        "CONNECTIVITY_RESTORED",
    ]
    assert all("secret" not in json.dumps(event) for event in events)
    persisted = collector._sanitize_rest_telemetry({"network_route_events": events})
    assert persisted["network_route_events"][-1]["effective_network_mode"] == "DIRECT"
    assert "secret" not in json.dumps(persisted)


def test_proxy_and_direct_unavailable_raise_last_network_error():
    events = []

    with pytest.raises(URLError, match="ALL_NETWORK_ROUTES_FAILED"):
        open_url(
            "https://fapi.binance.com/fapi/v1/time",
            timeout=1,
            proxies_fn=lambda: {"https": "127.0.0.1:7897"},
            bypass_fn=lambda _host: False,
            open_route_fn=lambda _request, _timeout, route: (_ for _ in ()).throw(
                URLError("proxy unavailable" if route["mode"] == "SYSTEM_PROXY" else "direct unavailable")
            ),
            on_event=events.append,
        )

    assert [event["effective_network_mode"] for event in events if event["event_type"] == "CONNECTIVITY_LOST"] == [
        "SYSTEM_PROXY",
        "DIRECT",
    ]


class OneMessageSocket:
    def __init__(self, payload):
        self.payload = payload
        self.sent = False

    def recv(self):
        if not self.sent:
            self.sent = True
            return json.dumps(self.payload)
        raise RuntimeError("route changed")

    def close(self):
        pass


def test_reconnect_reloads_network_policy_and_records_mode_change(tmp_path):
    route_calls = {"count": 0}
    connection_modes = []
    events = []
    received = []
    stop_event = threading.Event()

    def routes_fn(_url):
        route_calls["count"] += 1
        if route_calls["count"] == 1:
            return [{
                "mode": "SYSTEM_PROXY",
                "proxy_host": "127.0.0.1",
                "proxy_port": 7897,
                "proxy_type": "http",
                "proxy_auth": None,
            }, {"mode": "DIRECT"}]
        return [{"mode": "DIRECT"}]

    def websocket_connect(_url, timeout=None, **options):
        mode = "SYSTEM_PROXY" if options.get("http_proxy_host") else "DIRECT"
        connection_modes.append(mode)
        return OneMessageSocket({"a": len(connection_modes)})

    def on_message(payload, _meta):
        received.append(payload)
        if len(received) == 2:
            stop_event.set()

    supervisor = Supervisor(state_file=tmp_path / "supervisor.json")
    supervisor.register("aggTrade")
    thread = threading.Thread(
        target=run_component_lifecycle,
        kwargs={
            "name": "aggTrade",
            "url_fn": lambda: "wss://fstream.binance.com/market/ws/btcusdt@aggTrade",
            "route_class": "market",
            "stream_name": "btcusdt@aggTrade",
            "on_message": on_message,
            "supervisor": supervisor,
            "stop_event": stop_event,
            "reconnect_backoff_seconds": 0.01,
            "max_reconnect_backoff_seconds": 0.01,
            "network_routes_fn": routes_fn,
            "websocket_connect_fn": websocket_connect,
            "on_lifecycle_event": events.append,
        },
        daemon=True,
    )
    thread.start()
    thread.join(timeout=3)

    assert not thread.is_alive()
    assert connection_modes[:2] == ["SYSTEM_PROXY", "DIRECT"]
    assert len(received) == 2
    assert any(
        event["event_type"] == "NETWORK_MODE_CHANGED"
        and event["previous_network_mode"] == "SYSTEM_PROXY"
        and event["effective_network_mode"] == "DIRECT"
        for event in events
    )
    reasons = [entry["reason"] for entry in supervisor.get("aggTrade").history]
    assert any("network_mode=SYSTEM_PROXY" in reason for reason in reasons)
    assert any("network_mode=DIRECT" in reason for reason in reasons)


def _depth_collector(tmp_path, *, request_fn=None):
    supervisor = Supervisor(state_file=tmp_path / "supervisor.json")
    supervisor.register("diff_depth")
    return collector.DiffDepthCollector(
        "BTCUSDT",
        gap_ledger_path=tmp_path / "quality" / "gap_ledger.jsonl",
        writer=collector.RawPayloadWriter(tmp_path / "raw" / "depth.jsonl"),
        supervisor=supervisor,
        request_fn=request_fn,
    )


def test_startup_network_unavailable_then_restored_proceeds(tmp_path):
    attempts = {"count": 0}
    waits = []

    def snapshot(_symbol, _limit):
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise orderbook.OrderBookNetworkError("temporary outage")
        return {"lastUpdateId": 1000, "bids": [], "asks": []}

    depth = _depth_collector(tmp_path, request_fn=snapshot)
    last_update_id = depth.ensure_initial_sync_with_network_retry(
        retry_network_errors=True,
        retry_wait_fn=waits.append,
        retry_delay_seconds=0.01,
    )

    assert last_update_id == 1000
    assert attempts["count"] == 2
    assert waits == [0.01]
    assert depth.order_book.status == orderbook.RESYNC_BUFFERING
    assert depth.order_book.resync_count == 1
    reasons = [entry["reason"] for entry in depth.supervisor.get("diff_depth").history]
    assert any("INITIAL_DEPTH_SNAPSHOT_NETWORK_UNAVAILABLE" in reason for reason in reasons)
    assert any("INITIAL_DEPTH_SNAPSHOT_CONNECTIVITY_RESTORED" in reason for reason in reasons)


def test_initial_snapshot_retry_rereads_proxy_policy(monkeypatch, tmp_path):
    proxy_reads = {"count": 0}
    route_attempts = []
    telemetry = []

    def changing_open_url(request, *, timeout, on_event):
        proxy_reads["count"] += 1
        proxies = (
            {"https": "http://127.0.0.1:7897"}
            if proxy_reads["count"] == 1 else {}
        )

        def open_route(_request, _timeout, route):
            route_attempts.append(route["mode"])
            if proxy_reads["count"] == 1:
                raise URLError("temporarily unavailable")
            return DepthResponse()

        return network.open_url(
            request,
            timeout=timeout,
            proxies_fn=lambda: proxies,
            bypass_fn=lambda _host: False,
            open_route_fn=open_route,
            on_event=on_event,
        )

    monkeypatch.setattr(orderbook, "open_url", changing_open_url)
    depth = _depth_collector(tmp_path)
    depth.order_book._telemetry_sink = telemetry.append

    assert depth.ensure_initial_sync_with_network_retry(
        retry_network_errors=True,
        retry_wait_fn=lambda _delay: None,
        retry_delay_seconds=0.01,
    ) == 1000

    assert proxy_reads["count"] == 2
    assert route_attempts == ["SYSTEM_PROXY", "DIRECT", "DIRECT"]
    assert [row["outcome"] for row in telemetry] == ["NETWORK_ERROR", "SUCCESS"]
    restored = telemetry[-1]["network_route_events"][-1]
    assert restored["event_type"] == "CONNECTIVITY_RESTORED"
    assert restored["effective_network_mode"] == "DIRECT"


def test_initial_sync_does_not_retry_non_network_order_book_error(tmp_path):
    attempts = {"count": 0}

    def invalid_snapshot(_symbol, _limit):
        attempts["count"] += 1
        raise orderbook.OrderBookError("INVALID_SNAPSHOT")

    depth = _depth_collector(tmp_path, request_fn=invalid_snapshot)

    with pytest.raises(orderbook.OrderBookError, match="INVALID_SNAPSHOT"):
        depth.ensure_initial_sync_with_network_retry(
            retry_network_errors=True,
            retry_wait_fn=lambda _delay: pytest.fail("non-network failure retried"),
        )

    assert attempts["count"] == 1

