"""EvidenceHunter_orderbook.LocalOrderBook: pu continuity mismatch + snapshot
resync, using an injected request_fn -- no real network anywhere here."""
import pytest

from EvidenceHunter_orderbook import (
    LocalOrderBook,
    OrderBookError,
    RESYNC_BUFFERING,
    RESYNC_DESYNCED,
    RESYNC_SYNCHRONIZED,
)


def fake_snapshot(last_update_id, bids=None, asks=None):
    def _fn(symbol, limit):
        return {
            "lastUpdateId": last_update_id,
            "bids": bids or [["100.00", "1.000"]],
            "asks": asks or [["101.00", "2.000"]],
        }
    return _fn


def test_start_resync_sets_buffering_and_snapshot_state():
    book = LocalOrderBook("BTCUSDT", request_fn=fake_snapshot(1000))
    last_id = book.start_resync()
    assert last_id == 1000
    assert book.status == RESYNC_BUFFERING
    assert book.bids == {"100.00": "1.000"}
    assert book.asks == {"101.00": "2.000"}


def test_anchor_event_synchronizes_and_applies_diff():
    book = LocalOrderBook("BTCUSDT", request_fn=fake_snapshot(1000))
    book.start_resync()
    anchor_event = {"U": 999, "u": 1005, "pu": 998, "b": [["100.00", "0.500"]], "a": []}
    result = book.ingest(anchor_event)
    assert result == "SYNCHRONIZED"
    assert book.status == RESYNC_SYNCHRONIZED
    assert book.last_update_id == 1005
    assert book.bids["100.00"] == "0.500"


def test_stale_pre_snapshot_events_are_discarded_not_anchored():
    book = LocalOrderBook("BTCUSDT", request_fn=fake_snapshot(1000))
    book.start_resync()
    stale_event = {"U": 900, "u": 950, "pu": 899, "b": [], "a": []}
    result = book.ingest(stale_event)
    assert result == "BUFFERED"
    assert book.status == RESYNC_BUFFERING


def test_pu_continuity_mismatch_triggers_desync():
    book = LocalOrderBook("BTCUSDT", request_fn=fake_snapshot(1000))
    book.start_resync()
    book.ingest({"U": 999, "u": 1005, "pu": 998, "b": [], "a": []})
    assert book.status == RESYNC_SYNCHRONIZED
    bad_event = {"U": 1006, "u": 1010, "pu": 9999, "b": [], "a": []}
    result = book.ingest(bad_event)
    assert result == "DESYNCED"
    assert book.status == RESYNC_DESYNCED


def test_missing_pu_field_raises_instead_of_silently_accepting():
    book = LocalOrderBook("BTCUSDT", request_fn=fake_snapshot(1000))
    book.start_resync()
    book.ingest({"U": 999, "u": 1005, "pu": 998, "b": [], "a": []})
    with pytest.raises(OrderBookError):
        book.ingest({"U": 1006, "u": 1010, "b": [], "a": []})


def test_apply_event_while_not_synchronized_raises():
    book = LocalOrderBook("BTCUSDT", request_fn=fake_snapshot(1000))
    with pytest.raises(OrderBookError):
        book.apply_event({"U": 1, "u": 2, "pu": 1, "b": [], "a": []})


def test_resync_after_desync_uses_fresh_snapshot():
    calls = {"count": 0}

    def snapshot_fn(symbol, limit):
        calls["count"] += 1
        return {"lastUpdateId": 1000 if calls["count"] == 1 else 2000, "bids": [], "asks": []}

    book = LocalOrderBook("BTCUSDT", request_fn=snapshot_fn)
    book.start_resync()
    book.ingest({"U": 999, "u": 1005, "pu": 998, "b": [], "a": []})
    book.ingest({"U": 1006, "u": 1010, "pu": 9999, "b": [], "a": []})  # desync
    assert book.status == RESYNC_DESYNCED

    book.start_resync()
    assert book.status == RESYNC_BUFFERING
    assert book.last_update_id == 2000
    assert book.resync_count == 2
    anchor = {"U": 1999, "u": 2005, "pu": 1998, "b": [], "a": []}
    result = book.ingest(anchor)
    assert result == "SYNCHRONIZED"
    assert book.last_update_id == 2005


def test_zero_quantity_removes_price_level():
    book = LocalOrderBook("BTCUSDT", request_fn=fake_snapshot(1000, bids=[["100.00", "1.000"]]))
    book.start_resync()
    book.ingest({"U": 999, "u": 1001, "pu": 998, "b": [["100.00", "0"]], "a": []})
    assert "100.00" not in book.bids


def test_snapshot_reports_counters():
    book = LocalOrderBook("BTCUSDT", request_fn=fake_snapshot(1000))
    book.start_resync()
    book.ingest({"U": 999, "u": 1005, "pu": 998, "b": [], "a": []})
    info = book.snapshot()
    assert info["status"] == RESYNC_SYNCHRONIZED
    assert info["last_update_id"] == 1005
    assert info["resync_count"] == 1

