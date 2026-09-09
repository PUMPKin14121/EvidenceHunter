# -*- coding: utf-8 -*-
"""Prefetch official Binance historical archives required by mature unlocked outcomes."""

from datetime import datetime, timezone

from EvidenceHunter_archive import (
    ensure_archive_day,
    ensure_kline_archive_day,
    get_archive_agg_trades_between,
)
from EvidenceHunter_config import RUNTIME_DIR, SYMBOL, get_dataset_id
from EvidenceHunter_clock import exchange_now_ms
from EvidenceHunter_shadow_outcome import HORIZONS, load_existing_outcomes, load_records, parse_time

ROOT = RUNTIME_DIR / "binance_public_data_cache" / "futures_um"
AGG_DIR = ROOT / "aggTrades" / SYMBOL
KLINE_DIR = ROOT / "klines" / SYMBOL / "1m"


def day_from_ms(ms):
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).date()


def main():
    dataset_id = get_dataset_id()
    if not dataset_id:
        print("DATASET_NOT_INITIALIZED")
        return

    records = load_records(dataset_id)
    outcomes = load_existing_outcomes(dataset_id)
    now_ms = exchange_now_ms()
    today = day_from_ms(now_ms)
    days = set()

    for record in records:
        rid = record.get("record_id")
        if not rid:
            continue
        out = outcomes.get(rid, {})
        created = parse_time(record.get("timestamp"))
        if created is None:
            continue
        start_ms = record.get("price_exchange_event_time_ms")
        if start_ms is None:
            start_ms = record.get("exchange_event_time_ms")
        if start_ms is None:
            start_ms = int(created.timestamp() * 1000)
        start_ms = int(start_ms)

        for name, seconds in HORIZONS.items():
            target_ms = start_ms + seconds * 1000
            if now_ms >= target_ms and out.get("locked_" + name) is not True:
                for ms in (start_ms, target_ms):
                    d = day_from_ms(ms)
                    if d < today:
                        days.add(d)

    print("ARCHIVE_ROOT:", ROOT)
    print("COMPLETED_UTC_DAYS_NEEDED:", len(days))

    for day in sorted(days):
        print("Downloading/verifying:", day.isoformat())

        kp = ensure_kline_archive_day(SYMBOL, "1m", day, KLINE_DIR)
        print("  KLINE  OK:", kp)

        ap = ensure_archive_day(SYMBOL, day, AGG_DIR, require_checksum=True)
        print("  AGG    OK:", ap)

        # Build local extracted CSV + sparse index once now, not during backfill.
        # A tiny query is enough to trigger extraction/indexing.
        day_start = int(datetime(day.year, day.month, day.day, tzinfo=timezone.utc).timestamp() * 1000)
        get_archive_agg_trades_between(
            SYMBOL, day_start, day_start + 1000, AGG_DIR, require_checksum=True
        )
        print("  INDEX  OK")

    if not days:
        print("No completed UTC day currently requires historical archive prefetch.")


if __name__ == "__main__":
    main()


