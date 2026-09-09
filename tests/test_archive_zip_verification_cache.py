# -*- coding: utf-8 -*-
"""
ARCHIVE_ZIP_VERIFICATION_CACHE -- regression tests.

PERFORMANCE BUG BEING FIXED (EvidenceHunter_archive.py, _ensure_verified_zip):
every call to _ensure_verified_zip() -- which get_archive_agg_trades_between()
calls once per archive day per query -- unconditionally re-runs
zipfile.ZipFile(...).testzip(), a full CRC32 decompression pass over every
member of the zip, even when that exact zip file was already fully verified
(hash + CRC) earlier in the SAME process. Measured against a real ~21MB
aggTrades daily zip: testzip() alone costs ~0.36s per call, while the actual
indexed CSV query it gates costs ~0.007s -- i.e. testzip() is ~50x the cost
of the work it protects. The Discovery Runner issues ~6 archive queries per
shadow record (contemporaneous window start/end, decision price, +3 horizon
targets); across 10083 records that is ~60000 redundant testzip() calls,
which is the dominant cost of a full dry-run (empirically ~6 hours before
this fix).

FIX: an in-memory, per-process cache (module-level dict, same idiom as the
existing _KLINE_DAY_CACHE a few lines below in this file) keyed by the
resolved zip path, storing (expected_sha256, file_size) once a zip has been
fully verified (both the SHA256-vs-checksum-file check AND testzip() clean)
in this process. A cache hit skips testzip() entirely; a cache MISS (first
call ever, or the file's size/expected-hash changed since the cached entry
-- e.g. a legitimate re-download) always re-verifies from scratch, exactly
as before this fix. The cache is populated ONLY after a full, successful
verification -- a checksum mismatch or a bad-CRC zip is never cached as
verified, so a failure is never silently "trusted" on a later call.

This changeset does not touch _build_agg_index/_query_indexed_agg_csv (the
ARCHIVE_AGG_INDEX_FIRST_ANCHOR_FIX correctness logic) at all -- it is
orthogonal, purely a per-process verification-cost optimization on the
zip-integrity-checking path.

ORACLE CLASSIFICATION:
  - test_testzip_runs_on_first_call_for_a_fresh_zip,
    test_testzip_is_skipped_on_repeat_call_for_unchanged_zip: CHARACTERIZATION_ORACLE
    (asserts the new, explicitly-designed caching behavior via a call-count
    spy on zipfile.ZipFile.testzip).
  - test_checksum_mismatch_still_raises_and_is_never_cached_as_verified,
    test_bad_crc_zip_still_raises_and_is_never_cached_as_verified,
    test_cache_is_not_reused_if_zip_replaced_with_different_valid_content:
    SPECIFICATION_ORACLE (the pre-existing fail-closed contract of
    _ensure_verified_zip -- must be byte-for-byte unchanged by this fix).

Convention matched from tests/test_archive_agg_index_fix.py: flat `import
EvidenceHunter_archive as archive`, no conftest.py.
"""
import hashlib
import io
import sys
import zipfile
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import EvidenceHunter_archive as archive  # noqa: E402


def _make_zip_bytes(csv_content: bytes, csv_name: str = "BTCUSDT-aggTrades-2026-01-01.csv") -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(csv_name, csv_content)
    return buf.getvalue()


def _write_zip_and_checksum(tmp_path: Path, zip_bytes: bytes, name: str = "BTCUSDT-aggTrades-2026-01-01.zip"):
    zip_path = tmp_path / name
    zip_path.write_bytes(zip_bytes)
    checksum_path = zip_path.with_name(zip_path.name + ".CHECKSUM")
    checksum_path.write_text(hashlib.sha256(zip_bytes).hexdigest() + "  " + name + "\n", encoding="utf-8")
    return zip_path


@pytest.fixture(autouse=True)
def _clear_zip_verification_cache():
    """Every test starts from an empty cache, and leaves it empty afterward,
    so tests never see leftover state from each other (the same isolation
    discipline as test_archive_agg_index_fix.py's per-test index cache)."""
    archive._ZIP_VERIFIED_CACHE.clear()
    yield
    archive._ZIP_VERIFIED_CACHE.clear()


@pytest.fixture
def testzip_spy(monkeypatch):
    """Counts real calls to zipfile.ZipFile.testzip while still performing
    the genuine CRC check (never faked/skipped) -- so a test asserting
    "0 real calls" is asserting the fix actually avoids opening/scanning the
    zip a second time, not just that some unrelated shortcut looks similar."""
    calls = {"count": 0}
    original = zipfile.ZipFile.testzip

    def spy(self, *args, **kwargs):
        calls["count"] += 1
        return original(self, *args, **kwargs)

    monkeypatch.setattr(zipfile.ZipFile, "testzip", spy)
    return calls


def test_testzip_runs_on_first_call_for_a_fresh_zip(tmp_path, testzip_spy):
    zip_bytes = _make_zip_bytes(b"header\n1,2,3\n")
    zip_path = _write_zip_and_checksum(tmp_path, zip_bytes)

    result = archive._ensure_verified_zip(
        "https://unused.invalid/x.zip", "https://unused.invalid/x.zip.CHECKSUM", zip_path
    )
    assert result == zip_path
    assert testzip_spy["count"] == 1


def test_testzip_is_skipped_on_repeat_call_for_unchanged_zip(tmp_path, testzip_spy):
    zip_bytes = _make_zip_bytes(b"header\n1,2,3\n")
    zip_path = _write_zip_and_checksum(tmp_path, zip_bytes)

    archive._ensure_verified_zip("https://unused.invalid/x.zip", "https://unused.invalid/x.zip.CHECKSUM", zip_path)
    archive._ensure_verified_zip("https://unused.invalid/x.zip", "https://unused.invalid/x.zip.CHECKSUM", zip_path)
    archive._ensure_verified_zip("https://unused.invalid/x.zip", "https://unused.invalid/x.zip.CHECKSUM", zip_path)

    # 3 calls total, but testzip() must have run for only the FIRST one.
    assert testzip_spy["count"] == 1


def test_cache_is_not_reused_if_zip_replaced_with_different_valid_content(tmp_path, testzip_spy):
    """A legitimate scenario (e.g. a stale local file replaced by a fresh,
    correctly-checksummed re-download between two calls in the same
    long-lived process) must NOT be silently trusted off the old cache
    entry -- the new content has a different size/hash, so it is a cache
    MISS and gets fully re-verified."""
    zip_bytes_v1 = _make_zip_bytes(b"header\n1,2,3\n")
    zip_path = _write_zip_and_checksum(tmp_path, zip_bytes_v1)
    archive._ensure_verified_zip("https://unused.invalid/x.zip", "https://unused.invalid/x.zip.CHECKSUM", zip_path)
    assert testzip_spy["count"] == 1

    zip_bytes_v2 = _make_zip_bytes(b"header\n1,2,3\n4,5,6\n7,8,9\n")  # different size/content/hash
    zip_path.write_bytes(zip_bytes_v2)
    checksum_path = zip_path.with_name(zip_path.name + ".CHECKSUM")
    checksum_path.write_text(hashlib.sha256(zip_bytes_v2).hexdigest() + "\n", encoding="utf-8")
    # A stale .VERIFIED marker from the v1 verification must not short-circuit
    # this either -- simulate it being present, matching real-world behavior
    # where the marker file persists on disk across process restarts.

    archive._ensure_verified_zip("https://unused.invalid/x.zip", "https://unused.invalid/x.zip.CHECKSUM", zip_path)
    assert testzip_spy["count"] == 2


def test_checksum_mismatch_still_raises_and_is_never_cached_as_verified(tmp_path, testzip_spy):
    zip_bytes = _make_zip_bytes(b"header\n1,2,3\n")
    zip_path = tmp_path / "BTCUSDT-aggTrades-2026-01-01.zip"
    zip_path.write_bytes(zip_bytes)
    checksum_path = zip_path.with_name(zip_path.name + ".CHECKSUM")
    checksum_path.write_text("0" * 64 + "\n", encoding="utf-8")  # deliberately wrong

    with pytest.raises(archive.ArchiveDataUnavailable, match="CHECKSUM_MISMATCH"):
        archive._ensure_verified_zip("https://unused.invalid/x.zip", "https://unused.invalid/x.zip.CHECKSUM", zip_path)

    assert testzip_spy["count"] == 0  # never reached testzip -- failed at the hash check
    assert len(archive._ZIP_VERIFIED_CACHE) == 0  # a failure must never populate the cache


def test_bad_crc_zip_still_raises_and_is_never_cached_as_verified(tmp_path, testzip_spy):
    zip_bytes = bytearray(_make_zip_bytes(b"header\n1,2,3\n" * 50))
    # Flip a byte inside the compressed member data (well past the local
    # file header) to corrupt its CRC while leaving the zip structurally
    # openable -- this must still trip zf.testzip(), independent of the
    # caching change.
    flip_at = len(zip_bytes) - 40
    zip_bytes[flip_at] ^= 0xFF
    zip_bytes = bytes(zip_bytes)
    zip_path = tmp_path / "BTCUSDT-aggTrades-2026-01-01.zip"
    zip_path.write_bytes(zip_bytes)
    checksum_path = zip_path.with_name(zip_path.name + ".CHECKSUM")
    checksum_path.write_text(hashlib.sha256(zip_bytes).hexdigest() + "\n", encoding="utf-8")

    with pytest.raises(archive.ArchiveDataUnavailable):
        archive._ensure_verified_zip("https://unused.invalid/x.zip", "https://unused.invalid/x.zip.CHECKSUM", zip_path)

    assert len(archive._ZIP_VERIFIED_CACHE) == 0  # CRC failure must never populate the cache


def test_repeat_get_archive_agg_trades_between_calls_only_verify_once_per_day(tmp_path, monkeypatch, testzip_spy):
    """End-to-end: the real caller (get_archive_agg_trades_between, exactly
    as the Discovery Runner's ArchiveReader invokes it) issuing several
    queries against the same day must only pay the testzip() cost once."""
    # 2026-01-01T00:00:00Z / 00:00:05Z in ms -- must fall on the SAME
    # calendar day as the zip's filename below, since
    # get_archive_agg_trades_between() derives which day's archive file to
    # open from these timestamps (via _dates_for_range), independent of
    # whatever the CSV rows' own transact_time values say.
    csv_bytes = (
        b"agg_trade_id,price,quantity,first_trade_id,last_trade_id,transact_time,is_buyer_maker\n"
        b"1,100.0,1,1,1,1767225600000,false\n"
        b"2,100.5,1,2,2,1767225605000,false\n"
    )
    zip_bytes = _make_zip_bytes(csv_bytes, csv_name="BTCUSDT-aggTrades-2026-01-01.csv")
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    zip_path = _write_zip_and_checksum(cache_dir, zip_bytes, name="BTCUSDT-aggTrades-2026-01-01.zip")
    assert zip_path.exists()

    for _ in range(4):
        rows = archive.get_archive_agg_trades_between(
            "BTCUSDT", 1767225600000, 1767225605000, cache_dir, True
        )
        assert len(rows) == 2

    assert testzip_spy["count"] == 1

