# -*- coding: utf-8 -*-
"""
Binance official Public Data Archive helper for USD-M Futures.

Supports:
- daily aggTrades
- daily klines (e.g. 1m)

Design goals:
- official data.binance.vision only
- SHA256 CHECKSUM verification
- local cache
- indexed local aggTrades extraction for efficient repeated historical queries
- no trading / no private API
"""

from __future__ import annotations

import bisect
import csv
import hashlib
import io
import json
import os
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

ARCHIVE_ROOT = "https://data.binance.vision/data/futures/um/daily"
REQUEST_TIMEOUT = 60
AGG_INDEX_STRIDE = 10000
# v2 (ARCHIVE_AGG_INDEX_FIRST_ANCHOR_FIX): the first index anchor is now
# guaranteed to be the first successfully-parsed DATA row's own (timestamp,
# offset) -- never a header row, and never silently dropped in a way that
# lets the anchor list's first entry point past real, existing data. Any
# on-disk .index.json without this exact version is treated as stale and
# rebuilt; see _build_agg_index().
AGG_INDEX_BUILDER_VERSION = 2
_KLINE_DAY_CACHE = {}
# ARCHIVE_ZIP_VERIFICATION_CACHE: in-memory, per-process cache of zips that
# have already passed a FULL verification (SHA256-vs-CHECKSUM match AND a
# clean zipfile.testzip() CRC pass) earlier in this same process. Keyed by
# the zip's resolved absolute path, storing the (expected_sha256, file_size)
# pair that was verified. A cache hit lets _ensure_verified_zip() skip the
# expensive testzip() CRC-decompression pass entirely; a cache MISS (first
# call ever, or the file's expected hash / size changed since the cached
# entry -- e.g. a legitimate re-download) always re-verifies from scratch,
# exactly as before this cache existed. Populated ONLY after a full,
# successful verification -- a checksum mismatch or a bad-CRC zip is never
# written here, so a failure is never silently "trusted" on a later call.
_ZIP_VERIFIED_CACHE = {}


class ArchiveDataUnavailable(RuntimeError):
    pass


def _bool_value(value):
    return str(value).strip().lower() in ("true", "1", "yes")


def _date_from_ms(value_ms):
    return datetime.fromtimestamp(int(value_ms) / 1000.0, tz=timezone.utc).date()


def _dates_for_range(start_ms, end_ms):
    day = _date_from_ms(start_ms)
    last = _date_from_ms(end_ms)
    while day <= last:
        yield day
        day += timedelta(days=1)


def _http_bytes(url, timeout=REQUEST_TIMEOUT):
    req = Request(
        url=url,
        headers={"User-Agent": "BTC-AI-Hunter-Archive-Recovery/2.0", "Accept": "*/*"},
        method="GET",
    )
    try:
        with urlopen(req, timeout=timeout) as response:
            return response.read()
    except HTTPError as error:
        try:
            body = error.read().decode("utf-8", errors="replace")
        except Exception:
            body = ""
        raise ArchiveDataUnavailable(
            f"ARCHIVE_HTTP_ERROR status={error.code} url={url} body={body[:300]}"
        ) from error
    except URLError as error:
        raise ArchiveDataUnavailable(
            f"ARCHIVE_NETWORK_ERROR url={url} reason={error.reason}"
        ) from error


def _sha256_file(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _ensure_verified_zip(zip_url, checksum_url, zip_path):
    zip_path = Path(zip_path)
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    checksum_path = zip_path.with_name(zip_path.name + ".CHECKSUM")
    verified_path = zip_path.with_name(zip_path.name + ".VERIFIED")

    if not zip_path.exists():
        payload = _http_bytes(zip_url)
        temp = zip_path.with_suffix(zip_path.suffix + ".part")
        temp.write_bytes(payload)
        os.replace(temp, zip_path)

    if not checksum_path.exists():
        checksum_path.write_bytes(_http_bytes(checksum_url))

    checksum_text = checksum_path.read_text(encoding="utf-8", errors="replace").strip()
    expected = checksum_text.split()[0].strip().lower() if checksum_text else ""
    if len(expected) != 64:
        raise ArchiveDataUnavailable(
            f"INVALID_CHECKSUM_FORMAT path={checksum_path} content={checksum_text[:200]}"
        )

    stamp = None
    if verified_path.exists():
        try:
            parts = verified_path.read_text(encoding="utf-8").strip().split()
            if len(parts) == 2:
                stamp = (parts[0].lower(), int(parts[1]))
        except Exception:
            stamp = None

    current_size = zip_path.stat().st_size
    if stamp != (expected, current_size):
        actual = _sha256_file(zip_path)
        if actual.lower() != expected:
            for p in (zip_path, verified_path):
                try:
                    p.unlink()
                except Exception:
                    pass
            raise ArchiveDataUnavailable(
                f"CHECKSUM_MISMATCH file={zip_path.name} expected={expected} actual={actual}"
            )
        verified_path.write_text(f"{expected} {current_size}\n", encoding="utf-8")

    cache_key = str(zip_path.resolve())
    if _ZIP_VERIFIED_CACHE.get(cache_key) == (expected, current_size):
        # Already fully verified (hash + CRC) earlier in this process, and
        # the file's expected hash / size have not changed since -- skip the
        # expensive testzip() CRC-decompression pass entirely.
        return zip_path

    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            bad = zf.testzip()
            if bad is not None:
                raise ArchiveDataUnavailable(
                    f"ZIP_CRC_ERROR file={zip_path.name} member={bad}"
                )
    except zipfile.BadZipFile as error:
        raise ArchiveDataUnavailable(f"BAD_ZIP file={zip_path}") from error

    # Full verification (hash + CRC) succeeded -- record it so a later call
    # against this same unchanged file can skip testzip(). Never reached on
    # a failure path above (both raise before this line).
    _ZIP_VERIFIED_CACHE[cache_key] = (expected, current_size)

    return zip_path


def aggtrade_urls(symbol, day):
    d = day.isoformat()
    filename = f"{symbol.upper()}-aggTrades-{d}.zip"
    url = f"{ARCHIVE_ROOT}/aggTrades/{symbol.upper()}/{filename}"
    return filename, url, url + ".CHECKSUM"


def kline_urls(symbol, interval, day):
    d = day.isoformat()
    filename = f"{symbol.upper()}-{interval}-{d}.zip"
    url = f"{ARCHIVE_ROOT}/klines/{symbol.upper()}/{interval}/{filename}"
    return filename, url, url + ".CHECKSUM"


def ensure_archive_day(symbol, day, cache_dir, require_checksum=True):
    # Backward-compatible aggTrades entrypoint from V1.
    filename, url, checksum = aggtrade_urls(symbol, day)
    path = Path(cache_dir) / filename
    if require_checksum:
        return _ensure_verified_zip(url, checksum, path)
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(_http_bytes(url))
    return path


def ensure_kline_archive_day(symbol, interval, day, cache_dir):
    filename, url, checksum = kline_urls(symbol, interval, day)
    path = Path(cache_dir) / filename
    return _ensure_verified_zip(url, checksum, path)


def _extract_single_csv(zip_path):
    zip_path = Path(zip_path)
    csv_path = zip_path.with_suffix(".csv")
    marker = zip_path.with_name(zip_path.name + ".VERIFIED")
    marker_text = marker.read_text(encoding="utf-8").strip() if marker.exists() else ""

    meta_path = csv_path.with_suffix(csv_path.suffix + ".SOURCE")
    if csv_path.exists() and meta_path.exists():
        if meta_path.read_text(encoding="utf-8", errors="replace").strip() == marker_text:
            return csv_path

    with zipfile.ZipFile(zip_path, "r") as zf:
        members = [x for x in zf.namelist() if x.lower().endswith(".csv")]
        if not members:
            raise ArchiveDataUnavailable(f"NO_CSV_IN_ZIP file={zip_path}")
        if len(members) != 1:
            # Daily Binance files should have exactly one CSV.
            members.sort()
        member = members[0]
        temp = csv_path.with_suffix(csv_path.suffix + ".part")
        with zf.open(member, "r") as src, temp.open("wb") as dst:
            while True:
                block = src.read(1024 * 1024)
                if not block:
                    break
                dst.write(block)
        os.replace(temp, csv_path)
        meta_path.write_text(marker_text, encoding="utf-8")
    return csv_path


def _build_agg_index(csv_path):
    """Build (or reuse) the sparse (timestamp, byte_offset) index used by
    _query_indexed_agg_csv to seek near a requested window instead of
    scanning the whole day's file.

    ARCHIVE_AGG_INDEX_FIRST_ANCHOR_FIX (builder v2): the anchor slots are
    assigned by counting successfully-PARSED DATA ROWS, not raw file lines.
    A header row (or any other unparseable line) never consumes an anchor
    slot and is never itself recorded as an anchor. This guarantees the
    first anchor in `points` is always the first real data row -- whether
    or not the CSV happens to have a header line at all. Previously (v1),
    anchors were assigned by raw line_no % STRIDE; a header at line_no=0
    silently failed to parse and its anchor slot was dropped entirely
    (never retried at another line), so `points[0]` ended up being the
    stride-th *line* rather than the first data row -- and any query whose
    start_ms preceded that first surviving anchor's timestamp would seek
    straight past real, existing early data and incorrectly return empty.
    """
    csv_path = Path(csv_path)
    idx_path = csv_path.with_suffix(csv_path.suffix + ".index.json")
    source_meta = csv_path.with_suffix(csv_path.suffix + ".SOURCE")
    source_stamp = source_meta.read_text(encoding="utf-8").strip() if source_meta.exists() else ""
    size = csv_path.stat().st_size

    if idx_path.exists():
        try:
            data = json.loads(idx_path.read_text(encoding="utf-8"))
            if (
                data.get("source_stamp") == source_stamp
                and data.get("csv_size") == size
                and data.get("builder_version") == AGG_INDEX_BUILDER_VERSION
            ):
                return data
        except Exception:
            pass
        # Falls through to a full rebuild below whenever the cached index is
        # missing, unreadable, stamped for a different file, OR simply
        # written by an older builder_version -- an old index is never
        # silently trusted just because source_stamp/csv_size still match.

    points = []
    valid_row_no = 0
    with csv_path.open("rb") as f:
        while True:
            offset = f.tell()
            line = f.readline()
            if not line:
                break
            try:
                parts = line.rstrip(b"\r\n").split(b",")
                ts = int(parts[5])
            except Exception:
                # Not a parseable data row (e.g. the header line, or a
                # corrupt line). Never counted as a data row and never
                # consumes an anchor slot -- this is exactly the condition
                # that let a header row silently swallow the file's
                # first-anchor slot in builder v1.
                continue
            if valid_row_no == 0 or valid_row_no % AGG_INDEX_STRIDE == 0:
                # FIRST_VALID_DATA_ROW is always a mandatory anchor;
                # subsequent anchors are sampled every AGG_INDEX_STRIDE
                # valid data rows thereafter.
                points.append([ts, offset])
            valid_row_no += 1

    data = {
        "source_stamp": source_stamp,
        "csv_size": size,
        "stride": AGG_INDEX_STRIDE,
        "builder_version": AGG_INDEX_BUILDER_VERSION,
        "points": points,
    }
    tmp = idx_path.with_suffix(idx_path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, separators=(",", ":")), encoding="utf-8")
    os.replace(tmp, idx_path)
    return data


def _query_indexed_agg_csv(csv_path, start_ms, end_ms):
    index = _build_agg_index(csv_path)
    points = index.get("points") or []
    timestamps = [x[0] for x in points]
    pos = bisect.bisect_right(timestamps, int(start_ms)) - 1
    offset = points[max(0, pos)][1] if points else 0

    rows = []
    with Path(csv_path).open("rb") as f:
        f.seek(offset)
        while True:
            line = f.readline()
            if not line:
                break
            parts = line.rstrip(b"\r\n").split(b",")
            if len(parts) < 7:
                continue
            try:
                trade_id = int(parts[0])
                price = float(parts[1])
                qty = float(parts[2])
                ts = int(parts[5])
            except Exception:
                continue
            if ts < start_ms:
                continue
            if ts > end_ms:
                break
            rows.append({
                "trade_id": trade_id,
                "time": ts,
                "price": price,
                "quantity": qty,
                "normal_quantity": qty,
                "buyer_is_maker": _bool_value(parts[6].decode("ascii", errors="ignore")),
                "source": "BINANCE_PUBLIC_DATA_ARCHIVE",
            })
    return rows


def get_archive_agg_trades_between(symbol, start_ms, end_ms, cache_dir, require_checksum=True):
    start_ms = int(start_ms)
    end_ms = int(end_ms)
    if end_ms < start_ms:
        return []

    result = []
    for day in _dates_for_range(start_ms, end_ms):
        zip_path = ensure_archive_day(symbol, day, cache_dir, require_checksum=require_checksum)
        csv_path = _extract_single_csv(zip_path)
        result.extend(_query_indexed_agg_csv(csv_path, start_ms, end_ms))

    unique = {x["trade_id"]: x for x in result}
    return sorted(unique.values(), key=lambda x: (x["time"], x["trade_id"]))


def _load_kline_day(zip_path):
    zip_path = Path(zip_path)
    marker = zip_path.with_name(zip_path.name + ".VERIFIED")
    key = (str(zip_path), marker.read_text(encoding="utf-8").strip() if marker.exists() else "")
    if key in _KLINE_DAY_CACHE:
        return _KLINE_DAY_CACHE[key]

    rows = []
    with zipfile.ZipFile(zip_path, "r") as zf:
        members = [x for x in zf.namelist() if x.lower().endswith(".csv")]
        if not members:
            raise ArchiveDataUnavailable(f"NO_CSV_IN_ZIP file={zip_path}")
        with zf.open(sorted(members)[0], "r") as raw:
            text = io.TextIOWrapper(raw, encoding="utf-8-sig", newline="")
            reader = csv.reader(text)
            for row in reader:
                if len(row) < 11:
                    continue
                try:
                    open_time = int(row[0])
                    close_time = int(row[6])
                except Exception:
                    continue
                rows.append({
                    "open_time": open_time,
                    "open": float(row[1]),
                    "high": float(row[2]),
                    "low": float(row[3]),
                    "close": float(row[4]),
                    "volume": float(row[5]),
                    "close_time": close_time,
                    "quote_volume": float(row[7]),
                    "trades": int(row[8]),
                    "taker_buy_volume": float(row[9]),
                    "taker_buy_quote": float(row[10]),
                    "source": "BINANCE_PUBLIC_DATA_ARCHIVE",
                })
    rows.sort(key=lambda x: x["open_time"])
    _KLINE_DAY_CACHE.clear()
    _KLINE_DAY_CACHE[key] = rows
    return rows


def get_archive_klines_range(symbol, interval, start_ms, end_ms, cache_dir):
    start_ms = int(start_ms)
    end_ms = int(end_ms)
    if end_ms < start_ms:
        return []
    result = []
    for day in _dates_for_range(start_ms, end_ms):
        zip_path = ensure_kline_archive_day(symbol, interval, day, cache_dir)
        for candle in _load_kline_day(zip_path):
            if candle["close_time"] < start_ms:
                continue
            if candle["open_time"] > end_ms:
                break
            result.append(candle)
    unique = {x["open_time"]: x for x in result}
    return sorted(unique.values(), key=lambda x: x["open_time"])
