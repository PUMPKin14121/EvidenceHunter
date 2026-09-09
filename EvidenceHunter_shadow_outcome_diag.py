# -*- coding: utf-8 -*-
"""Read-only diagnostic for BTC HUNTER historical outcome gaps and archive fallback."""

from datetime import datetime, timezone

from EvidenceHunter_config import OUTCOMES_FILE_V2, SHADOW_FILE_V2, get_dataset_id, research_outcomes_path
from EvidenceHunter_clock import exchange_now_ms
from EvidenceHunter_market import get_agg_trades_between
from EvidenceHunter_shadow_outcome import HORIZONS, load_existing_outcomes, load_records, parse_time


def iso_ms(ms):
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).isoformat()


def main():
    dataset_id = get_dataset_id()
    print("=" * 80)
    print("BTC HUNTER | SHADOW OUTCOME ARCHIVE DIAGNOSTIC")
    print("=" * 80)
    print("DATASET_ID:", dataset_id)
    if not dataset_id:
        return

    records = load_records(dataset_id)
    outcomes = load_existing_outcomes(dataset_id, research_outcomes_path(dataset_id, OUTCOMES_FILE_V2))
    now_ms = exchange_now_ms()

    candidates = []
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
                candidates.append((start_ms, target_ms, rid, name))

    print("MATURE_UNLOCKED:", len(candidates))
    if not candidates:
        print("No mature unlocked outcome found.")
        return

    candidates.sort()
    start_ms, target_ms, rid, name = candidates[0]
    age_hours = (now_ms - start_ms) / 3_600_000
    print("FIRST_CANDIDATE:")
    print("  record_id :", rid)
    print("  horizon   :", name)
    print("  start     :", iso_ms(start_ms))
    print("  target    :", iso_ms(target_ms))
    print("  age_hours :", round(age_hours, 3))
    print()
    print("Testing exact aggTrades for the first 5 seconds of that historical path.")
    try:
        trades = get_agg_trades_between(start_ms, min(target_ms, start_ms + 5000))
        sources = sorted({x.get("source") for x in trades if x.get("source")})
        print("AGGTRADES_TEST: PASS")
        print("  rows    :", len(trades))
        print("  sources :", sources)
        if trades:
            print("  first   :", trades[0])
            print("  last    :", trades[-1])
    except Exception as error:
        print("AGGTRADES_TEST: FAIL")
        print(type(error).__name__, str(error))

    print()
    print("FILES:")
    print("  records :", SHADOW_FILE_V2)
    print("  research outcomes:", research_outcomes_path(dataset_id, OUTCOMES_FILE_V2))
    print("  legacy operational path (frozen, writes blocked):", OUTCOMES_FILE_V2)


if __name__ == "__main__":
    main()


