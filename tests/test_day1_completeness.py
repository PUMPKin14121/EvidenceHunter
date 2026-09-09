"""COLLECTOR_DAY1_COMPLETENESS_FIX: targeted tests for the three Day-1 raw-truth
gaps only (REST depth snapshot evidence, OS-native clock observation, REST
telemetry). No network, no collector run, no soak, no Dataset."""
import base64
import copy
import io
import email.message
import json
import threading
from urllib.error import HTTPError, URLError

import pytest

import EvidenceHunter_clock_observer as clock_observer
import EvidenceHunter_collector as collector
import EvidenceHunter_orderbook as orderbook
from EvidenceHunter_collector_supervisor import STATE_LIFECYCLE_THREAD_CRASHED, Supervisor


class FakeResponse:
    def __init__(self, body_text, *, status=200, headers=None):
        self._body = body_text if isinstance(body_text, bytes) else body_text.encode("utf-8")
        self.status = status
        message = email.message.Message()
        for key, value in (headers or {}).items():
            message[key] = value
        self.headers = message

    def read(self):
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def make_http_fn(bodies, *, status=200, headers=None):
    """Transport-level injection: hands back exact bytes, so the pre-parse
    capture can be tested on the real code path."""
    calls = {"n": 0}

    def _http_fn(request, timeout=None):
        index = min(calls["n"], len(bodies) - 1)
        calls["n"] += 1
        return FakeResponse(bodies[index], status=status, headers=headers)

    _http_fn.calls = calls
    return _http_fn


# Deliberately NOT json.dumps output: extra spaces, newlines, and key order
# that a re-serialisation would silently normalise away.
BODY_1 = '{"lastUpdateId":  1000,\n   "bids": [["100.00", "1.000"]], "asks": [["101.00", "2.000"]]}'
BODY_2 = '{"lastUpdateId":2000,   "bids": [], "asks": []}'


def collect_sink():
    records = []
    return records, records.append


# --------------------------------------------------------------------------
# GAP1 -- REST depth snapshot evidence
# --------------------------------------------------------------------------

def test_snapshot_evidence_persisted_on_initial_sync_and_on_every_resync():
    records, sink = collect_sink()
    book = orderbook.LocalOrderBook(
        "BTCUSDT", snapshot_sink=sink, http_fn=make_http_fn([BODY_1, BODY_2]),
    )
    book.start_resync()
    book.ingest({"U": 999, "u": 1005, "pu": 998, "b": [], "a": []})
    book.ingest({"U": 1006, "u": 1010, "pu": 9999, "b": [], "a": []})  # desync
    book.start_resync()  # every resync, not just the first

    assert len(records) == 2
    assert [r["last_update_id"] for r in records] == [1000, 2000]
    for record in records:
        assert record["schema"] == orderbook.DEPTH_SNAPSHOT_RECORD_SCHEMA
        assert record["endpoint_path"] == "/fapi/v1/depth"
        assert record["params"] == {"symbol": "BTCUSDT", "limit": orderbook.DEPTH_SNAPSHOT_LIMIT}
        assert record["http_status"] == 200
        assert record["raw_body_unavailable_reason"] is None
        for field in ("requested_local_wall_ms", "requested_monotonic_ms",
                      "received_local_wall_ms", "received_monotonic_ms"):
            assert isinstance(record[field], (int, float))


def test_raw_body_is_captured_before_parsing_and_never_reserialised():
    records, sink = collect_sink()
    book = orderbook.LocalOrderBook("BTCUSDT", snapshot_sink=sink, http_fn=make_http_fn([BODY_1]))
    book.start_resync()

    raw = records[0]["raw_body"]
    assert raw == BODY_1                                  # byte-for-byte as received
    assert raw != json.dumps(json.loads(BODY_1))          # not a re-serialisation
    assert json.loads(raw)["lastUpdateId"] == 1000        # and still the same input


def test_injected_request_fn_reports_raw_body_unavailable_instead_of_faking_it():
    records, sink = collect_sink()
    book = orderbook.LocalOrderBook(
        "BTCUSDT", snapshot_sink=sink,
        request_fn=lambda symbol, limit: {"lastUpdateId": 7, "bids": [], "asks": []},
    )
    book.start_resync()
    assert records[0]["raw_body"] is None
    assert records[0]["raw_body_unavailable_reason"] == "INJECTED_REQUEST_FN_NO_HTTP_BODY"


def test_snapshot_and_rest_telemetry_share_one_rest_request_id():
    snapshots, snapshot_sink = collect_sink()
    telemetry, telemetry_sink = collect_sink()
    book = orderbook.LocalOrderBook(
        "BTCUSDT", snapshot_sink=snapshot_sink, telemetry_sink=telemetry_sink,
        http_fn=make_http_fn([BODY_1, BODY_2]),
    )
    book.start_resync()
    book.start_resync()

    assert snapshots[0]["rest_request_id"] == telemetry[0]["rest_request_id"]
    assert snapshots[1]["rest_request_id"] == telemetry[1]["rest_request_id"]
    assert snapshots[0]["rest_request_id"] != snapshots[1]["rest_request_id"]


def test_evidence_sinks_do_not_change_order_book_reconstruction():
    events = [
        {"U": 999, "u": 1005, "pu": 998, "b": [["100.00", "0.500"]], "a": []},
        {"U": 1006, "u": 1009, "pu": 1005, "b": [["99.00", "3.000"]], "a": []},
    ]
    with_sink = orderbook.LocalOrderBook(
        "BTCUSDT", snapshot_sink=lambda record: None, telemetry_sink=lambda event: None,
        http_fn=make_http_fn([BODY_1]),
    )
    without_sink = orderbook.LocalOrderBook("BTCUSDT", http_fn=make_http_fn([BODY_1]))
    for book in (with_sink, without_sink):
        book.start_resync()
        for event in events:
            book.ingest(event)

    assert with_sink.bids == without_sink.bids
    assert with_sink.asks == without_sink.asks
    assert with_sink.last_update_id == without_sink.last_update_id
    assert with_sink.status == without_sink.status


def test_failing_evidence_sink_is_counted_and_never_breaks_the_resync():
    def exploding_sink(record):
        raise RuntimeError("disk on fire")

    book = orderbook.LocalOrderBook(
        "BTCUSDT", snapshot_sink=exploding_sink, http_fn=make_http_fn([BODY_1]),
    )
    assert book.start_resync() == 1000                     # resync unaffected
    assert book.status == orderbook.RESYNC_BUFFERING
    assert book.snapshot()["evidence_sink_failure_count"] == 1   # and never silent


# --------------------------------------------------------------------------
# GAP2 -- OS-native clock observation
# --------------------------------------------------------------------------

W32TM_OK = (
    "Leap Indicator: 0(no warning)\n"
    "Stratum: 3 (secondary reference - syncd by (S)NTP)\n"
    "Precision: -23 (119.209ns per tick)\n"
    "Root Delay: 0.0301638s\n"
    "Root Dispersion: 0.7857654s\n"
    "ReferenceId: 0xC0A80001 (source IP:  192.168.0.1)\n"
    "Last Successful Sync Time: 2026/9/8 09:00:00\n"
    "Source: time.windows.com\n"
    "Phase Offset: -0.0072123s\n"
    "Poll Interval: 10 (1024s)\n"
)


def test_clock_observation_records_os_reported_state_and_offset():
    record = clock_observer.observe_clock(
        query_fn=lambda: (0, W32TM_OK, ""), platform="win32",
    )
    assert record["schema"] == clock_observer.CLOCK_OBSERVATION_SCHEMA
    assert record["availability"] == clock_observer.AVAILABILITY_AVAILABLE
    assert record["offset_available"] is True
    assert record["offset_seconds"] == pytest.approx(-0.0072123)
    assert record["sync_status"]["stratum"].startswith("3")
    assert record["sync_status"]["source"] == "time.windows.com"
    assert record["raw_output"] == W32TM_OK
    assert record["external_time_server_queried"] is False


@pytest.mark.parametrize("query_fn,expected_marker", [
    (lambda: (1, "", "The service has not been started."), "CLOCK_QUERY_NONZERO_EXIT"),
    (lambda: (0, "Leap Indicator: 0(no warning)\nStratum: 3\nSource: time.windows.com\n", ""), "OFFSET_NOT_REPORTED_OR_NOT_PARSABLE"),
])
def test_clock_observation_unavailable_never_fabricates_an_offset(query_fn, expected_marker):
    record = clock_observer.observe_clock(query_fn=query_fn, platform="win32")
    assert record["offset_seconds"] is None      # not 0, not carried forward
    assert record["offset_available"] is False
    assert expected_marker in record["unavailable_reason"]


def test_clock_observation_records_a_failed_query_as_evidence_not_an_exception():
    def boom():
        raise OSError("w32tm not found")

    record = clock_observer.observe_clock(query_fn=boom, platform="win32")
    assert record["availability"] == clock_observer.AVAILABILITY_UNAVAILABLE
    assert "CLOCK_QUERY_FAILED" in record["unavailable_reason"]
    assert record["offset_seconds"] is None


def test_clock_observer_queries_no_external_time_server():
    source = (clock_observer.__file__).replace(".pyc", ".py")
    with open(source, encoding="utf-8") as handle:
        text = handle.read()
    for forbidden in ("socket", "ntp.org", "urlopen", "requests", "123"):
        assert forbidden not in text.split('"""')[-1], forbidden
    assert clock_observer.EXTERNAL_TIME_SERVER_QUERIED is False


def test_clock_observation_does_not_mutate_market_timestamps():
    """The observer only ever writes its own record; nothing in its path can
    reach a market record's timestamps."""
    written = []

    class Writer:
        def submit(self, record):
            written.append(record)

    class FakeSupervisor:
        def record_message(self, name, *, local_wall_ms):
            pass

    observer = clock_observer.ClockObserver(
        supervisor=FakeSupervisor(), writer=Writer(),
        query_fn=lambda: (0, W32TM_OK, ""), platform="win32",
    )
    market_record = {
        "stream": "aggTrade", "raw": {"a": 17, "T": 1234567},
        "collector_received_local_wall_ms": 1234568,
        "collector_received_monotonic_ms": 7654321,
    }
    original = copy.deepcopy(market_record)
    record = observer.observe_once()
    assert market_record == original
    assert json.dumps(market_record, sort_keys=True) == json.dumps(original, sort_keys=True)
    assert written == [record]
    assert set(record) == {
        "schema", "observed_local_wall_ms", "observed_monotonic_ms", "source",
        "external_time_server_queried", "platform", "availability", "unavailable_reason",
        "offset_seconds", "offset_available", "sync_status", "raw_output", "returncode",
    }


def test_clock_observer_thread_crash_becomes_visible_through_the_supervisor(tmp_path):
    class ExplodingWriter:
        def submit(self, record):
            raise RuntimeError("evidence writer exploded")

    supervisor = Supervisor(state_file=tmp_path / "supervisor_state.json")
    supervisor.register("clock_observer")
    observer = clock_observer.ClockObserver(
        supervisor=supervisor, writer=ExplodingWriter(),
        query_fn=lambda: (0, W32TM_OK, ""), platform="win32",
    )
    events = []
    with pytest.raises(RuntimeError):
        observer.run(stop_event=threading.Event(), on_lifecycle_event=events.append)

    assert supervisor.snapshot()["clock_observer"]["state"] == STATE_LIFECYCLE_THREAD_CRASHED
    assert events[-1]["event_type"] == "LIFECYCLE_THREAD_CRASHED"


# --------------------------------------------------------------------------
# GAP3 -- REST telemetry
# --------------------------------------------------------------------------

def test_rest_telemetry_written_for_success_http_error_and_network_failure():
    telemetry, sink = collect_sink()
    book = orderbook.LocalOrderBook(
        "BTCUSDT", telemetry_sink=sink,
        http_fn=make_http_fn([BODY_1], headers={"x-mbx-used-weight-1m": "12", "Retry-After": "3"}),
    )
    book.start_resync()

    headers = email.message.Message()
    headers["x-mbx-used-weight-1m"] = "999"
    headers["Retry-After"] = "30"

    def http_429(request, timeout=None):
        raise HTTPError(request.full_url, 429, "Too Many Requests", headers, None)

    def http_down(request, timeout=None):
        raise URLError("connection refused")

    for http_fn in (http_429, http_down):
        broken = orderbook.LocalOrderBook("BTCUSDT", telemetry_sink=sink, http_fn=http_fn)
        with pytest.raises(orderbook.OrderBookError):
            broken.start_resync()

    outcomes = [event["outcome"] for event in telemetry]
    assert outcomes == ["SUCCESS", "HTTP_ERROR", "NETWORK_ERROR"]
    assert telemetry[0]["http_status"] == 200
    assert telemetry[0]["response_headers"]["x-mbx-used-weight-1m"] == "12"
    assert telemetry[1]["http_status"] == 429
    assert telemetry[1]["response_headers"]["Retry-After"] == "30"
    assert telemetry[2]["failure_code"] == "DEPTH_SNAPSHOT_NETWORK_ERROR"
    assert telemetry[2]["failure_exception_type"] == "URLError"
    for event in telemetry:
        assert isinstance(event["requested_monotonic_ms"], (int, float))
        assert isinstance(event["received_monotonic_ms"], (int, float))


def test_rest_telemetry_sink_redacts_secrets_and_drops_unknown_headers():
    written = []

    class Writer:
        def submit(self, record):
            written.append(record)

    sink = collector.make_rest_telemetry_sink(Writer())
    sink({
        "rest_request_id": "abc",
        "request_class": "AGG_TRADES_BACKFILL",
        "endpoint_path": "/fapi/v1/aggTrades",
        "params": {
            "symbol": "BTCUSDT", "fromId": 5, "limit": 100,
            "signature": "deadbeef", "apiKey": "SECRET", "timestamp": 1,
        },
        "outcome": "SUCCESS",
        "http_status": 200,
        "response_headers": {
            "x-mbx-used-weight-1m": "7", "Retry-After": "1",
            "Set-Cookie": "session=abc", "Authorization": "Bearer TOKEN",
            "Server": "nginx",
        },
        "failure_code": None,
        "failure_exception_type": None,
        "failure_errno": None,
        "requested_local_wall_ms": 1, "requested_monotonic_ms": 2,
        "received_local_wall_ms": 3, "received_monotonic_ms": 4,
    })

    record = written[0]
    blob = json.dumps(record)
    for secret in ("deadbeef", "SECRET", "session=abc", "Bearer TOKEN"):
        assert secret not in blob
    assert record["params"]["symbol"] == "BTCUSDT"
    assert record["params"]["fromId"] == 5
    assert record["params"]["signature"] == collector.REDACTED
    assert record["params"]["apiKey"] == collector.REDACTED
    assert record["params"]["timestamp"] == collector.REDACTED   # fail-closed default
    assert set(record["response_headers"]) == {"x-mbx-used-weight-1m", "Retry-After"}
    assert record["schema"] == collector.REST_TELEMETRY_SCHEMA


def test_collector_module_never_imports_config():
    source = collector.__file__.replace(".pyc", ".py")
    with open(source, encoding="utf-8") as handle:
        text = handle.read()
    assert "import EvidenceHunter_config" not in text
    assert "import config" not in text


def test_no_derived_or_interpretive_fields_in_the_new_evidence_records():
    forbidden = (
        "absorption", "replenishment", "liquidity_regime", "attack", "response_score",
        "session_edge", "signal", "forward_return", "outcome_label", "smoothed",
    )
    snapshots, snapshot_sink = collect_sink()
    telemetry, telemetry_sink = collect_sink()
    book = orderbook.LocalOrderBook(
        "BTCUSDT", snapshot_sink=snapshot_sink, telemetry_sink=telemetry_sink,
        http_fn=make_http_fn([BODY_1]),
    )
    book.start_resync()
    clock_record = clock_observer.observe_clock(
        query_fn=lambda: (0, W32TM_OK, ""), platform="win32",
    )

    for record in (snapshots[0], telemetry[0], clock_record):
        keys = json.dumps(sorted(record.keys())).lower()
        for token in forbidden:
            assert token not in keys, (token, record.keys())


# F1-F5 regression bodies reconstructed from the approved review assertions.
# These are not claimed to be a byte-identical export of the cloud test file.
def test_raw_snapshot_evidence_survives_a_json_parse_failure():
    malformed = '{"lastUpdateId": 1000, "bids": [[  BROKEN'
    records, sink = collect_sink()
    book = orderbook.LocalOrderBook(
        "BTCUSDT", snapshot_sink=sink, http_fn=make_http_fn([malformed]))
    with pytest.raises(ValueError):
        book.start_resync()
    record = records[0]
    assert record["raw_body"] == malformed
    assert record["parse_status"] == "PARSE_FAILED"
    assert record["parse_error_type"] == "JSONDecodeError"
    assert record["last_update_id"] is None
    assert record["http_status"] == 200


def test_http_error_body_is_retained_and_network_failure_says_no_response():
    error_body = b'{"code": -1003, "msg": "Too many requests"}'
    records, sink = collect_sink()

    def http_429(request, timeout=None):
        raise HTTPError(request.full_url, 429, "Too Many Requests",
                        email.message.Message(), io.BytesIO(error_body))

    def http_down(request, timeout=None):
        raise URLError(OSError(111, "connection refused"))

    for http_fn in (http_429, http_down):
        with pytest.raises(orderbook.OrderBookError):
            orderbook.fetch_depth_snapshot("BTCUSDT", snapshot_sink=sink, http_fn=http_fn)
    assert records[0]["raw_body"] == error_body.decode("utf-8")
    assert records[0]["parse_status"] == "NOT_PARSED_HTTP_ERROR"
    assert records[1]["raw_body"] is None
    assert records[1]["raw_body_unavailable_reason"] == "NO_HTTP_RESPONSE"
    assert records[1]["parse_status"] == "NOT_PARSED_NO_RESPONSE"


def test_one_receive_instant_is_shared_by_snapshot_and_telemetry(monkeypatch):
    wall = iter(range(1000, 1020))
    mono = iter(range(2000, 2020))
    monkeypatch.setattr(orderbook, "local_wall_ms", lambda: next(wall))
    monkeypatch.setattr(orderbook, "monotonic_ms", lambda: next(mono))
    snapshots, snapshot_sink = collect_sink()
    telemetry, telemetry_sink = collect_sink()
    orderbook.fetch_depth_snapshot(
        "BTCUSDT", snapshot_sink=snapshot_sink, telemetry_sink=telemetry_sink,
        http_fn=make_http_fn([BODY_1]))
    for field in ("requested_local_wall_ms", "requested_monotonic_ms",
                  "received_local_wall_ms", "received_monotonic_ms"):
        assert snapshots[0][field] == telemetry[0][field], field
    assert snapshots[0]["received_local_wall_ms"] == 1001
    assert snapshots[0]["received_monotonic_ms"] == 2001


def test_exception_text_containing_a_secret_can_never_reach_disk():
    written = []

    class Writer:
        def submit(self, record):
            written.append(record)

    def http_down(request, timeout=None):
        raise URLError("connect failed for signature=deadbeef&token=hunter2")

    with pytest.raises(orderbook.OrderBookError) as raised:
        orderbook.fetch_depth_snapshot(
            "BTCUSDT", http_fn=http_down,
            telemetry_sink=collector.make_rest_telemetry_sink(Writer()))
    assert "deadbeef" in str(raised.value)
    blob = json.dumps(written)
    assert "deadbeef" not in blob
    assert "hunter2" not in blob
    assert written[0]["failure_code"] == "DEPTH_SNAPSHOT_NETWORK_ERROR"
    assert "failure" not in written[0]


def test_agg_trade_backfill_rest_path_emits_telemetry_for_all_three_outcomes():
    telemetry, sink = collect_sink()
    rows = collector.rest_agg_trades_from_id(
        "BTCUSDT", 5, telemetry_sink=sink,
        http_fn=make_http_fn(['[{"a": 5}]'],
                            headers={"x-mbx-used-weight-1m": "4", "Retry-After": "2"}))
    assert rows == [{"a": 5}]
    headers = email.message.Message()
    headers["x-mbx-used-weight-1m"] = "1200"

    def http_418(request, timeout=None):
        raise HTTPError(request.full_url, 418, "Blocked", headers, None)

    def http_down(request, timeout=None):
        raise URLError(OSError(111, "connection refused"))

    for http_fn, marker in ((http_418, "REST_HTTP_ERROR"),
                            (http_down, "REST_NETWORK_ERROR")):
        with pytest.raises(collector.CollectorError) as raised:
            collector.rest_agg_trades_from_id(
                "BTCUSDT", 5, telemetry_sink=sink, http_fn=http_fn)
        assert marker in str(raised.value)
    assert [e["outcome"] for e in telemetry] == ["SUCCESS", "HTTP_ERROR", "NETWORK_ERROR"]
    assert telemetry[0]["response_headers"]["Retry-After"] == "2"
    assert telemetry[1]["http_status"] == 418
    assert telemetry[1]["response_headers"]["x-mbx-used-weight-1m"] == "1200"
    assert telemetry[2]["failure_code"] == "REST_NETWORK_ERROR"
    assert telemetry[2]["failure_errno"] == 111


def test_non_utf8_http_bodies_are_lossless_without_changing_control_flow():
    body = b"\xff\xfe\x00raw response"
    for is_http_error in (False, True):
        records, sink = collect_sink()
        original = HTTPError("https://example.invalid/depth", 429, "Too Many Requests",
                             email.message.Message(), io.BytesIO(body))

        def http_error(request, timeout=None):
            raise original

        expected = orderbook.OrderBookError if is_http_error else UnicodeDecodeError
        with pytest.raises(expected) as raised:
            orderbook.fetch_depth_snapshot(
                "BTCUSDT", snapshot_sink=sink,
                http_fn=http_error if is_http_error else make_http_fn([body]))
        if is_http_error:
            assert raised.value.__cause__ is original
            assert str(raised.value) == "DEPTH_SNAPSHOT_HTTP_ERROR status=429"
            assert records[0]["parse_status"] == "NOT_PARSED_HTTP_ERROR"
        else:
            assert raised.value.object == body
            assert records[0]["parse_status"] == "NOT_PARSED_DECODE_FAILED"
            assert records[0]["parse_error_type"] == "UnicodeDecodeError"
        assert records[0]["raw_body"] is None
        assert records[0]["raw_body_representation"] == "BASE64_BYTES"
        assert records[0]["raw_body_unavailable_reason"] is None
        assert base64.b64decode(records[0]["raw_body_base64"], validate=True) == body

# Real host output: btc_time_passive120_635d0a0432c7451ba89ae6d6117ed219.json.
W32TM_ZH_REAL = """Leap 指示符: 3(未同步)
层次: 0 (未指定)
精度: -23 (每刻度 119.209ns)
根延迟: 0.1008697s
根分散: 11.6789273s
引用 ID: 0x00000000 (未指定)
上次成功同步时间: 2026/9/8 11:23:21
源: time.windows.com,0x9 
轮询间隔: 10 (1024s)

相位偏移: -3.9089111s
ClockRate: 0.0156250s
计算机状态: 0 (unset)
时间源标志:0 (无)
服务器角色: 0 (无)
上次同步错误: 1 (此计算机没有重新同步，因为没有可用的时间数据。)
上次成功同步时间后的时间: 2182.9990745s
"""


def test_real_chinese_unsynchronized_clock_state_is_available():
    record = clock_observer.observe_clock(
        query_fn=lambda: (0, W32TM_ZH_REAL, ""), platform="win32")
    assert record["availability"] == "AVAILABLE"
    assert record["sync_status"]["source"] == "time.windows.com,0x9"
    assert record["sync_status"]["stratum"] == "0 (未指定)"
    assert record["sync_status"]["leap_indicator"] == "3(未同步)"
    assert record["sync_status"]["last_successful_sync_time"] == "2026/9/8 11:23:21"
    assert record["offset_seconds"] == -3.9089111
    assert record["offset_available"] is True
    assert record["raw_output"] == W32TM_ZH_REAL


@pytest.mark.parametrize("stdout", [
    "unrecognized successful response",
    "Leap Indicator: 3\nStratum: 0\n",
    "Source: time.windows.com\nStratum: 0\n",
    "Source: time.windows.com\nLeap Indicator: 3\n",
])
def test_successful_query_without_required_clock_fields_is_unavailable(stdout):
    record = clock_observer.observe_clock(
        query_fn=lambda: (0, stdout, ""), platform="win32")
    assert record["availability"] == "UNAVAILABLE"
    assert record["unavailable_reason"] == "SYNC_STATUS_NOT_PARSED"
    assert record["raw_output"] == stdout

