# -*- coding: utf-8 -*-
"""
ARCHIVE_AGG_INDEX_FIRST_ANCHOR_FIX -- regression tests.

BUG BEING FIXED (EvidenceHunter_archive.py, _build_agg_index /
_query_indexed_agg_csv): the sparse aggTrades index is sampled every
AGG_INDEX_STRIDE=10000 *raw file lines*, starting from line_no=0. When the
CSV has a header row, line_no=0 IS the header. Parsing the header as a data
row (int(parts[5])) raises, and the exception is silently swallowed --
so the anchor slot that should have captured "offset of the file's real
start" is silently dropped. The next anchor only appears at line_no=10000,
i.e. ~10000 real trades (many minutes, for a liquid symbol) into the file.

Any query whose start_ms precedes that first surviving anchor's timestamp
computes bisect_right(timestamps, start_ms) - 1 == -1, which is clamped by
max(0, pos) to 0 -- i.e. it seeks to the offset of the *first surviving*
anchor (already well past the true start of the file), not to the true
start. Reading forward from there, every row's ts is already > end_ms, so
the read loop's `if ts > end_ms: break` fires immediately and the query
returns an empty list -- even though the requested window is fully covered
by real data sitting earlier in the file, before the seek point.

This file proves the bug against ORIGINAL_BUG-vintage index files (no
builder_version field) are rejected/rebuilt, and that the FIXED
_build_agg_index always anchors on the first successfully-parsed DATA row
(never a header / never a dropped, skipped-past anchor), regardless of
whether the source CSV has a header row at all.

ORACLE CLASSIFICATION:
  - test_*_index_first_anchor_is_real_first_data_row: SPECIFICATION_ORACLE
    (the fixed contract: first anchor == first parseable data row, always).
  - test_query_before_first_stride_anchor_returns_real_early_data,
    test_indexed_query_matches_direct_sequential_scan_*: CONTRACT_ORACLE via
    an independent, from-scratch linear scan of the raw CSV bytes (does not
    call any EvidenceHunter_archive internals), used as ground truth to compare
    against the indexed query path.
  - test_stale_old_format_index_is_rejected_and_rebuilt,
    test_rebuilt_index_is_deterministic: CHARACTERIZATION_ORACLE (asserts
    the new, explicitly-designed cache-invalidation behavior).

Convention matched from tests/test_audit_order.py: flat `import
EvidenceHunter_archive as archive`, no conftest.py, no package __init__.py
(relies on pytest/cwd sys.path insertion when run from the project root).
"""
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import EvidenceHunter_archive as archive  # noqa: E402


# ---------------------------------------------------------------------------
# Synthetic aggTrades CSV fixture builder (independent of any archive.py
# internals -- this is deliberately hand-rolled so it can serve as ground
# truth, not something that could share a bug with the code under test).
# ---------------------------------------------------------------------------

AGG_HEADER = b"agg_trade_id,price,quantity,first_trade_id,last_trade_id,transact_time,is_buyer_maker\n"

BASE_TS = 1_700_000_000_000  # arbitrary fixed epoch ms, deterministic across runs


def _row_bytes(agg_id, ts, price=100.0, qty=1.0, is_buyer_maker=False):
    return (
        f"{agg_id},{price},{qty},{agg_id},{agg_id},{ts},"
        f"{'true' if is_buyer_maker else 'false'}\n"
    ).encode("ascii")


def write_synthetic_csv(path: Path, n_rows: int, with_header: bool, gap_after_row=None, gap_ms=0):
    """Writes n_rows synthetic trades at 1ms apart (ts = BASE_TS + i), each
    with a unique, increasing agg_trade_id, optionally preceded by the real
    Binance aggTrades header line, optionally with a genuine time gap
    inserted after row index `gap_after_row` (to create a real, no-data
    window for the "empty genuine window" test)."""
    ts = BASE_TS
    with path.open("wb") as f:
        if with_header:
            f.write(AGG_HEADER)
        for i in range(n_rows):
            f.write(_row_bytes(agg_id=1_000_000 + i, ts=ts))
            if gap_after_row is not None and i == gap_after_row:
                ts += gap_ms
            ts += 1
    return path


def direct_sequential_scan(path: Path, start_ms: int, end_ms: int):
    """Ground-truth oracle: linear scan of the raw file from byte 0, with NO
    index, NO seeking, and its own from-scratch parsing -- deliberately not
    reusing any EvidenceHunter_archive code, so it cannot share a bug with it."""
    rows = []
    with path.open("rb") as f:
        for line in f:
            parts = line.rstrip(b"\r\n").split(b",")
            if len(parts) < 7:
                continue
            try:
                agg_id = int(parts[0])
                price = float(parts[1])
                ts = int(parts[5])
            except Exception:
                continue
            if ts < start_ms or ts > end_ms:
                continue
            rows.append((agg_id, ts, price))
    return rows


def _write_source_stamp(csv_path: Path, stamp: str = "TESTSTAMP"):
    meta_path = csv_path.with_suffix(csv_path.suffix + ".SOURCE")
    meta_path.write_text(stamp, encoding="utf-8")
    return stamp


N = archive.AGG_INDEX_STRIDE + 500  # comfortably spans >1 stride boundary


# ---------------------------------------------------------------------------
# 1) First-anchor correctness (the core fix), header-present and headerless.
# ---------------------------------------------------------------------------

def test_header_csv_index_first_anchor_is_real_first_data_row(tmp_path):
    csv_path = tmp_path / "with_header.csv"
    write_synthetic_csv(csv_path, N, with_header=True)
    _write_source_stamp(csv_path)

    data = archive._build_agg_index(csv_path)
    assert data["points"], "index must not be empty"
    first_ts, first_offset = data["points"][0]

    # The first anchor's timestamp must be the FIRST real data row's ts
    # (BASE_TS), never a header artifact and never skipped past real data.
    assert first_ts == BASE_TS
    # And its byte offset, read forward, must land exactly on that first
    # real data row -- not on the header, not past it.
    with csv_path.open("rb") as f:
        f.seek(first_offset)
        line = f.readline()
    assert line == _row_bytes(1_000_000, BASE_TS)


def test_headerless_csv_index_first_anchor_is_real_first_data_row(tmp_path):
    csv_path = tmp_path / "headerless.csv"
    write_synthetic_csv(csv_path, N, with_header=False)
    _write_source_stamp(csv_path)

    data = archive._build_agg_index(csv_path)
    first_ts, first_offset = data["points"][0]
    assert first_ts == BASE_TS
    assert first_offset == 0


# ---------------------------------------------------------------------------
# 2) The actual regression: a query whose window falls before the first
#    stride-multiple anchor must still find the real data that precedes it.
# ---------------------------------------------------------------------------

def test_query_before_first_stride_anchor_returns_real_early_data(tmp_path):
    csv_path = tmp_path / "with_header.csv"
    write_synthetic_csv(csv_path, N, with_header=True)
    _write_source_stamp(csv_path)

    # A window entirely inside the first ~500 rows -- i.e. entirely before
    # the second index anchor (which only appears at row index
    # AGG_INDEX_STRIDE). Under the pre-fix code this returned [] every time.
    start_ms = BASE_TS + 100
    end_ms = BASE_TS + 105
    got = archive._query_indexed_agg_csv(csv_path, start_ms, end_ms)
    expected = direct_sequential_scan(csv_path, start_ms, end_ms)

    assert len(got) == len(expected) == 6
    got_tuples = [(r["trade_id"], r["time"], r["price"]) for r in got]
    assert got_tuples == expected


def test_query_exactly_at_first_real_trade(tmp_path):
    csv_path = tmp_path / "with_header.csv"
    write_synthetic_csv(csv_path, N, with_header=True)
    _write_source_stamp(csv_path)

    got = archive._query_indexed_agg_csv(csv_path, BASE_TS, BASE_TS)
    assert len(got) == 1
    assert got[0]["trade_id"] == 1_000_000
    assert got[0]["time"] == BASE_TS


def test_query_after_first_anchor_still_works(tmp_path):
    csv_path = tmp_path / "with_header.csv"
    write_synthetic_csv(csv_path, N, with_header=True)
    _write_source_stamp(csv_path)

    # A window well past the first real stride anchor (row ~10000) -- the
    # "already worked before the fix" case; must keep working after it.
    start_ms = BASE_TS + archive.AGG_INDEX_STRIDE + 50
    end_ms = BASE_TS + archive.AGG_INDEX_STRIDE + 60
    got = archive._query_indexed_agg_csv(csv_path, start_ms, end_ms)
    expected = direct_sequential_scan(csv_path, start_ms, end_ms)
    got_tuples = [(r["trade_id"], r["time"], r["price"]) for r in got]
    assert got_tuples == expected
    assert len(expected) == 11


def test_query_near_file_end(tmp_path):
    csv_path = tmp_path / "with_header.csv"
    write_synthetic_csv(csv_path, N, with_header=True)
    _write_source_stamp(csv_path)

    last_ts = BASE_TS + N - 1
    start_ms = last_ts - 5
    end_ms = last_ts + 1000  # deliberately runs off the end of the file
    got = archive._query_indexed_agg_csv(csv_path, start_ms, end_ms)
    expected = direct_sequential_scan(csv_path, start_ms, end_ms)
    got_tuples = [(r["trade_id"], r["time"], r["price"]) for r in got]
    assert got_tuples == expected
    assert len(expected) == 6


def test_empty_genuine_window_stays_empty(tmp_path):
    # A real gap in the data (e.g. an illiquid moment) must still correctly
    # come back empty -- the fix must not turn into a "always find
    # something" false positive.
    csv_path = tmp_path / "with_gap.csv"
    gap_row = 300
    gap_ms = 60_000  # a genuine 60-second gap in trading after row 300
    write_synthetic_csv(csv_path, N, with_header=True, gap_after_row=gap_row, gap_ms=gap_ms)
    _write_source_stamp(csv_path)

    gap_start = BASE_TS + gap_row + 1  # ts right after row `gap_row`, but
    gap_query_start = BASE_TS + gap_row + 10_000  # squarely inside the gap
    gap_query_end = BASE_TS + gap_row + 20_000
    got = archive._query_indexed_agg_csv(csv_path, gap_query_start, gap_query_end)
    expected = direct_sequential_scan(csv_path, gap_query_start, gap_query_end)
    assert got == [] and expected == []


def test_malformed_line_is_skipped_not_crashed_and_never_included(tmp_path):
    csv_path = tmp_path / "malformed.csv"
    write_synthetic_csv(csv_path, 50, with_header=True)
    # Inject a corrupt line in the middle (wrong field count / non-numeric).
    with csv_path.open("ab") as f:
        f.write(b"garbage,not,a,valid,row\n")
        f.write(b"agg_trade_id,price,quantity,first_trade_id,last_trade_id,transact_time,is_buyer_maker\n")
    # ...followed by more real rows so the corrupt lines are mid-file.
    ts = BASE_TS + 50
    with csv_path.open("ab") as f:
        for i in range(50, 60):
            f.write(_row_bytes(agg_id=1_000_000 + i, ts=ts))
            ts += 1
    _write_source_stamp(csv_path)

    # Must not raise, and the index must still be built correctly.
    data = archive._build_agg_index(csv_path)
    assert data["points"][0][0] == BASE_TS

    got = archive._query_indexed_agg_csv(csv_path, BASE_TS, BASE_TS + 200)
    ids = [r["trade_id"] for r in got]
    assert ids == sorted(ids)
    assert all(1_000_000 <= i < 1_000_060 for i in ids)


# ---------------------------------------------------------------------------
# 3) Stale-cache invalidation: an old-format (.index.json without
#    builder_version, or a stale/wrong one) must never be silently reused.
# ---------------------------------------------------------------------------

def test_stale_old_format_index_is_rejected_and_rebuilt(tmp_path):
    csv_path = tmp_path / "with_header.csv"
    write_synthetic_csv(csv_path, N, with_header=True)
    stamp = _write_source_stamp(csv_path)

    # Simulate exactly what the pre-fix builder would have written: same
    # source_stamp/csv_size (so the old cache-hit check would have matched),
    # but NO builder_version key, and points[0] deliberately wrong (as the
    # old buggy builder would have produced: first anchor far past the true
    # start, because the header ate the offset-0 slot).
    idx_path = csv_path.with_suffix(csv_path.suffix + ".index.json")
    bogus_first_offset = 999_999_999  # obviously not the real file start
    old_style = {
        "source_stamp": stamp,
        "csv_size": csv_path.stat().st_size,
        "stride": archive.AGG_INDEX_STRIDE,
        "points": [[BASE_TS + archive.AGG_INDEX_STRIDE, bogus_first_offset]],
        # no "builder_version" key at all -- this is the old, pre-fix shape.
    }
    idx_path.write_text(json.dumps(old_style), encoding="utf-8")

    data = archive._build_agg_index(csv_path)
    assert data.get("builder_version") == archive.AGG_INDEX_BUILDER_VERSION
    assert data["points"][0] == [BASE_TS, 0] if False else data["points"][0][0] == BASE_TS
    # The stale/bogus offset must be gone, replaced by the real one.
    assert data["points"][0][1] != bogus_first_offset


def test_rebuilt_index_is_deterministic(tmp_path):
    csv_path = tmp_path / "with_header.csv"
    write_synthetic_csv(csv_path, N, with_header=True)
    _write_source_stamp(csv_path)

    data1 = archive._build_agg_index(csv_path)
    idx_path = csv_path.with_suffix(csv_path.suffix + ".index.json")
    idx_path.unlink()  # force a genuine rebuild, not a cache hit
    data2 = archive._build_agg_index(csv_path)

    assert data1["points"] == data2["points"]
    assert data1["builder_version"] == data2["builder_version"] == archive.AGG_INDEX_BUILDER_VERSION


# ---------------------------------------------------------------------------
# 4) Oracle differential test: indexed query vs. from-scratch sequential
#    scan, at the beginning / middle / end of a larger synthetic file.
# ---------------------------------------------------------------------------

def test_indexed_query_matches_direct_sequential_scan_beginning_middle_end(tmp_path):
    csv_path = tmp_path / "with_header.csv"
    total = archive.AGG_INDEX_STRIDE * 2 + 777
    write_synthetic_csv(csv_path, total, with_header=True)
    _write_source_stamp(csv_path)

    windows = [
        ("beginning", BASE_TS + 10, BASE_TS + 40),                          # before any stride anchor
        ("middle", BASE_TS + archive.AGG_INDEX_STRIDE + 200,
                   BASE_TS + archive.AGG_INDEX_STRIDE + 260),               # spans a stride boundary
        ("end", BASE_TS + total - 30, BASE_TS + total + 5000),              # runs off the end
    ]
    for label, start_ms, end_ms in windows:
        got = archive._query_indexed_agg_csv(csv_path, start_ms, end_ms)
        expected = direct_sequential_scan(csv_path, start_ms, end_ms)
        got_tuples = [(r["trade_id"], r["time"], r["price"]) for r in got]
        assert got_tuples == expected, f"parity mismatch in {label} window"
        assert len(expected) > 0, f"{label} window must have real expected data (test fixture sanity)"

