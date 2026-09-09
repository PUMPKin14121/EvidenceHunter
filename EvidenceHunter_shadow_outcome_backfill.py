# -*- coding: utf-8 -*-
"""Checkpointed historical Outcome backfill for BTC HUNTER V1.2.3.

- Only fills mature horizons whose locked_<h> is not True.
- Uses the patched market layer: completed UTC days come from official Binance archive.
- Writes an atomic checkpoint every BATCH_RECORDS processed records.
- Safe to stop and resume.
- Does not alter shadow_records.jsonl or dataset_id.
"""

import json
import os
from datetime import datetime, timezone

from EvidenceHunter_config import OUTCOME_VERSION, OUTCOMES_FILE_V2, get_dataset_id
from EvidenceHunter_clock import exchange_now_ms, exchange_utc_now
from EvidenceHunter_shadow_outcome import (
    HORIZONS,
    calculate_return,
    load_existing_outcomes,
    load_records,
    parse_time,
    reconstruct_path,
)

BATCH_RECORDS = 25
from EvidenceHunter_config import assert_dataset_writable


def _write_checkpoint(dataset_id, outcomes):
    assert_dataset_writable(dataset_id, OUTCOMES_FILE_V2)
    temp = OUTCOMES_FILE_V2.with_suffix(OUTCOMES_FILE_V2.suffix + ".backfill.tmp")
    with temp.open("w", encoding="utf-8") as f:
        for rid, outcome in outcomes.items():
            f.write(json.dumps({
                "dataset_id": dataset_id,
                "record_id": rid,
                "future_outcome": outcome,
            }, ensure_ascii=False) + "\n")
        f.flush()
        os.fsync(f.fileno())
    os.replace(temp, OUTCOMES_FILE_V2)


def _start_ms(record, created):
    value = record.get("price_exchange_event_time_ms")
    if value is None:
        value = record.get("exchange_event_time_ms")
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(created.timestamp() * 1000)


def main():
    dataset_id = get_dataset_id()
    assert_dataset_writable(dataset_id, OUTCOMES_FILE_V2)
    if not dataset_id:
        raise RuntimeError("DATASET_NOT_INITIALIZED")

    records = load_records(dataset_id)
    outcomes = load_existing_outcomes(dataset_id)
    now_ms = exchange_now_ms()
    now = datetime.fromtimestamp(now_ms / 1000.0, tz=timezone.utc)

    tasks = 0
    for record in records:
        rid = record.get("record_id")
        created = parse_time(record.get("timestamp"))
        if not rid or created is None:
            continue
        start_ms = _start_ms(record, created)
        out = outcomes.get(rid, {})
        for name, seconds in HORIZONS.items():
            if now_ms >= start_ms + seconds * 1000 and out.get("locked_" + name) is not True:
                tasks += 1

    print("=" * 80)
    print("BTC HUNTER V1.2.3 | CHECKPOINTED OUTCOME BACKFILL")
    print("=" * 80)
    print("DATASET_ID           :", dataset_id)
    print("SHADOW_RECORDS       :", len(records))
    print("MATURE MISSING TASKS :", tasks)
    print("CHECKPOINT EVERY     :", BATCH_RECORDS, "records")
    print("=" * 80)

    completed_tasks = 0
    touched_records = 0
    since_checkpoint = 0

    records = sorted(
        records,
        key=lambda r: (
            int(r.get("price_exchange_event_time_ms") or r.get("exchange_event_time_ms") or 0),
            str(r.get("record_id") or ""),
        ),
    )

    try:
        for record in records:
            rid = record.get("record_id")
            if not rid:
                continue
            created = parse_time(record.get("timestamp"))
            if created is None:
                continue
            start_price = float(record.get("price", 0) or 0)
            if start_price <= 0:
                continue
            start_ms = _start_ms(record, created)

            outcome = dict(outcomes.get(rid, {}))
            plan = record.get("trade_plan") or {}
            direction = plan.get("direction", "FLAT")
            stop = plan.get("stop")
            tp1 = plan.get("tp1")

            changed_this_record = False
            outcome["outcome_version"] = OUTCOME_VERSION
            outcome["last_checked"] = exchange_utc_now()
            outcome["age_seconds"] = round((now - created).total_seconds(), 3)

            for name, seconds in HORIZONS.items():
                if outcome.get("locked_" + name) is True:
                    continue
                target_ms = start_ms + seconds * 1000
                if now_ms < target_ms:
                    continue

                path = reconstruct_path(start_ms, target_ms, direction, tp1, stop, start_price)
                outcome["target_time_" + name] = datetime.fromtimestamp(
                    target_ms / 1000, tz=timezone.utc
                ).isoformat()
                outcome["actual_observation_time_" + name] = exchange_utc_now()
                outcome["delay_seconds_" + name] = round((now_ms - target_ms) / 1000.0, 3)
                outcome["path_status_" + name] = path.get("status")
                outcome["path_precision_" + name] = path.get("path_precision")
                outcome["ambiguous_segments_" + name] = path.get("ambiguous_segments")
                outcome["path_reason_" + name] = path.get("reason")
                outcome["path_detail_" + name] = path.get("detail")
                outcome["agg_trade_sources_" + name] = path.get("agg_trade_sources")

                result_key = "result_" + name
                if path.get("status") != "OK":
                    outcome[result_key] = "PATH_UNAVAILABLE"
                    # Keep unlocked so a future rerun can recover it.
                    continue

                final_price = path["final_price"]
                high, low = path["high"], path["low"]
                outcome["price_" + name] = final_price
                outcome["high_" + name] = high
                outcome["low_" + name] = low
                outcome["final_price_source_" + name] = path.get("final_price_source")
                outcome["barrier_time_ms_" + name] = path.get("barrier_time_ms")
                outcome[result_key] = path.get("result")

                raw_return = calculate_return(start_price, final_price)
                outcome["long_return_" + name] = raw_return
                outcome["short_return_" + name] = (
                    round(-raw_return, 8) if raw_return is not None else None
                )
                long_mfe = calculate_return(start_price, high)
                long_mae = calculate_return(start_price, low)
                short_low_return = calculate_return(start_price, low)
                short_high_return = calculate_return(start_price, high)
                outcome["mfe_long_" + name] = long_mfe
                outcome["mae_long_" + name] = long_mae
                outcome["mfe_short_" + name] = (
                    round(-short_low_return, 8) if short_low_return is not None else None
                )
                outcome["mae_short_" + name] = (
                    round(-short_high_return, 8) if short_high_return is not None else None
                )
                outcome["locked_" + name] = True
                completed_tasks += 1
                changed_this_record = True

            outcome["status"] = (
                "COMPLETE"
                if all(outcome.get("locked_" + h) is True for h in HORIZONS)
                else "PENDING"
            )
            outcomes[rid] = outcome

            if changed_this_record:
                touched_records += 1
                since_checkpoint += 1

            if since_checkpoint >= BATCH_RECORDS:
                _write_checkpoint(dataset_id, outcomes)
                since_checkpoint = 0
                remaining = max(0, tasks - completed_tasks)
                print(
                    f"[CHECKPOINT] touched_records={touched_records} "
                    f"completed_tasks={completed_tasks} remaining~={remaining}"
                )

    except KeyboardInterrupt:
        print("\nBackfill interrupted by user; saving checkpoint...")
        _write_checkpoint(dataset_id, outcomes)
        print("Checkpoint saved. Safe to resume later.")
        return
    except Exception as error:
        print("\nBackfill error:", type(error).__name__, str(error))
        print("Saving checkpoint before exit...")
        _write_checkpoint(dataset_id, outcomes)
        raise

    _write_checkpoint(dataset_id, outcomes)
    print("=" * 80)
    print("BACKFILL FINISHED")
    print("TOUCHED_RECORDS :", touched_records)
    print("COMPLETED_TASKS :", completed_tasks)
    print("OUTCOMES_FILE   :", OUTCOMES_FILE_V2)
    print("=" * 80)


if __name__ == "__main__":
    main()


