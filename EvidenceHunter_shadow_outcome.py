# -*- coding: utf-8 -*-

"""BTC HUNTER Shadow Outcome Evaluator V2.3.

Key change: labels are reconstructed from historical exchange data for the
exact target window instead of sampling only the current price every 30s.
Full 1-minute candles are used for efficient path coverage; partial boundary
minutes and any candle where both TP and Stop are touched are resolved with
ordered aggregate trades. If path order cannot be established, the label is
AMBIGUOUS rather than silently preferring TP.
"""

import json
import time
from EvidenceHunter_config import assert_dataset_writable
from datetime import datetime, timezone

from EvidenceHunter_config import OUTCOME_VERSION, OUTCOMES_FILE_V2, SHADOW_FILE_V2, ensure_directories, get_dataset_id
from EvidenceHunter_clock import exchange_now_ms, exchange_utc_now
from EvidenceHunter_market import AggTradeDataUnavailable, get_agg_trades_between, get_klines_range

CHECK_SECONDS = 30
HORIZONS = {"5m": 300, "15m": 900, "30m": 1800}


def utc_now():
    return exchange_utc_now()


def parse_time(value):
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None


def dt_to_ms(dt):
    return int(dt.timestamp() * 1000)


def calculate_return(start_price, end_price):
    if start_price <= 0 or end_price <= 0:
        return None
    return round((end_price - start_price) / start_price, 8)


def _get_agg_trades_strict(start_ms, end_ms):
    try:
        return get_agg_trades_between(start_ms, end_ms), None
    except AggTradeDataUnavailable as error:
        return None, str(error)


def load_records(dataset_id):
    if not SHADOW_FILE_V2.exists():
        return []
    records = []
    with SHADOW_FILE_V2.open("r", encoding="utf-8") as f:
        for line in f:
            try:
                rec = json.loads(line)
                if isinstance(rec, dict) and rec.get("dataset_id") == dataset_id:
                    records.append(rec)
            except Exception:
                continue
    return records


def load_existing_outcomes(dataset_id, path=None):
    path = OUTCOMES_FILE_V2 if path is None else path
    if not path.exists():
        return {}
    existing = {}
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            try:
                row = json.loads(line)
                if row.get("dataset_id") != dataset_id:
                    continue
                rid = row.get("record_id")
                outcome = row.get("future_outcome")
                if rid and isinstance(outcome, dict):
                    existing[rid] = outcome
            except Exception:
                continue
    return existing


def _scan_trades_for_barrier(trades, direction, tp1, stop):
    if direction not in ("LONG", "SHORT") or not tp1 or not stop:
        return None, None
    for trade in trades:
        price = trade.get("price", 0)
        if direction == "LONG":
            if price >= tp1:
                return "TP_FIRST", trade.get("time")
            if price <= stop:
                return "STOP_FIRST", trade.get("time")
        else:
            if price <= tp1:
                return "TP_FIRST", trade.get("time")
            if price >= stop:
                return "STOP_FIRST", trade.get("time")
    return None, None


def reconstruct_path(start_ms, end_ms, direction, tp1, stop, start_price):
    minute_ms = 60_000
    first_minute = (start_ms // minute_ms) * minute_ms
    candles = get_klines_range("1m", first_minute, end_ms, limit=1000)
    if not candles:
        return {"status": "PATH_UNAVAILABLE", "reason": "NO_KLINES"}

    high = start_price
    low = start_price
    final_price = start_price
    final_price_source = "START_PRICE"
    first_barrier = None
    first_barrier_time_ms = None
    full_candles_used = 0
    agg_trade_segments = 0
    ambiguous_segments = 0
    agg_trade_sources = set()

    for candle in candles:
        open_ms = candle["open_time"]
        close_ms = candle["close_time"]
        if close_ms < start_ms or open_ms > end_ms:
            continue
        seg_start = max(start_ms, open_ms)
        seg_end = min(end_ms, close_ms)
        is_full = seg_start <= open_ms and seg_end >= close_ms

        if is_full:
            seg_high = candle["high"]
            seg_low = candle["low"]
            seg_close = candle["close"]
            full_candles_used += 1
            high = max(high, seg_high)
            low = min(low, seg_low)
            final_price = seg_close
            final_price_source = "1M_KLINE_CLOSE"

            if first_barrier is None and direction in ("LONG", "SHORT") and tp1 and stop:
                if direction == "LONG":
                    tp_hit, stop_hit = seg_high >= tp1, seg_low <= stop
                else:
                    tp_hit, stop_hit = seg_low <= tp1, seg_high >= stop
                if tp_hit and stop_hit:
                    trades, agg_error = _get_agg_trades_strict(seg_start, seg_end)
                    if agg_error is not None:
                        return {"status": "PATH_UNAVAILABLE", "reason": "AGGTRADES_UNAVAILABLE", "detail": agg_error}
                    agg_trade_segments += 1
                    agg_trade_sources.update(x.get("source") for x in trades if x.get("source"))
                    event, event_time = _scan_trades_for_barrier(trades, direction, tp1, stop)
                    if event:
                        first_barrier, first_barrier_time_ms = event, event_time
                    else:
                        ambiguous_segments += 1
                        first_barrier = "AMBIGUOUS"
                elif tp_hit:
                    first_barrier = "TP_FIRST"
                    first_barrier_time_ms = open_ms
                elif stop_hit:
                    first_barrier = "STOP_FIRST"
                    first_barrier_time_ms = open_ms
        else:
            trades, agg_error = _get_agg_trades_strict(seg_start, seg_end)
            if agg_error is not None:
                return {"status": "PATH_UNAVAILABLE", "reason": "AGGTRADES_UNAVAILABLE", "detail": agg_error}
            agg_trade_segments += 1
            agg_trade_sources.update(x.get("source") for x in trades if x.get("source"))
            if trades:
                prices = [x["price"] for x in trades if x.get("price", 0) > 0]
                if prices:
                    high = max(high, max(prices))
                    low = min(low, min(prices))
                    final_price = prices[-1]
                    final_price_source = "AGG_TRADE"
                if first_barrier is None:
                    event, event_time = _scan_trades_for_barrier(trades, direction, tp1, stop)
                    if event:
                        first_barrier, first_barrier_time_ms = event, event_time
            else:
                # Avoid inventing a precise partial-minute path from a whole candle.
                ambiguous_segments += 1

    if direction not in ("LONG", "SHORT") or not tp1 or not stop:
        result = "NO_PLAN"
    elif first_barrier is None:
        result = "TIMEOUT"
    else:
        result = first_barrier

    return {
        "status": "OK",
        "result": result,
        "barrier_time_ms": first_barrier_time_ms,
        "high": high,
        "low": low,
        "final_price": final_price,
        "final_price_source": final_price_source,
        "full_candles_used": full_candles_used,
        "agg_trade_segments": agg_trade_segments,
        "ambiguous_segments": ambiguous_segments,
        "agg_trade_sources": sorted(agg_trade_sources),
        "path_precision": "1M_KLINE_PLUS_AGG_TRADES_BOUNDARY_AND_DOUBLE_HIT_ARCHIVE_CAPABLE",
    }


def update_records():
    assert_dataset_writable(get_dataset_id(), OUTCOMES_FILE_V2)
    dataset_id = get_dataset_id()
    if not dataset_id:
        return {"updated": False, "reason": "DATASET_NOT_INITIALIZED"}
    records = load_records(dataset_id)
    if not records:
        return {"updated": False, "reason": "NO_RECORDS"}

    existing = load_existing_outcomes(dataset_id)
    now_ms = exchange_now_ms()
    now = datetime.fromtimestamp(now_ms / 1000.0, tz=timezone.utc)
    changed = 0
    new_outcomes = dict(existing)

    for record in records:
        rid = record.get("record_id")
        if not rid:
            continue
        outcome = dict(existing.get(rid, {}))
        start_price = float(record.get("price", 0) or 0)
        created = parse_time(record.get("timestamp"))
        if start_price <= 0 or created is None:
            continue
        # The entry price came from the ticker-price source. Use that source's
        # exchange transaction time as the path origin. The aggregate
        # exchange_event_time_ms can be newer because it is max(price/funding/OI).
        start_ms = record.get("price_exchange_event_time_ms")
        if start_ms is None:
            start_ms = record.get("exchange_event_time_ms")
        try:
            start_ms = int(start_ms)
        except (TypeError, ValueError):
            start_ms = dt_to_ms(created)

        plan = record.get("trade_plan") or {}
        direction = plan.get("direction", "FLAT")
        stop = plan.get("stop")
        tp1 = plan.get("tp1")

        outcome["outcome_version"] = OUTCOME_VERSION
        outcome["last_checked"] = utc_now()
        outcome["age_seconds"] = round((now - created).total_seconds(), 3)

        for name, seconds in HORIZONS.items():
            result_key = "result_" + name
            if outcome.get("locked_" + name) is True:
                continue
            target_ms = start_ms + seconds * 1000
            if now_ms < target_ms:
                continue

            path = reconstruct_path(start_ms, target_ms, direction, tp1, stop, start_price)
            outcome["target_time_" + name] = datetime.fromtimestamp(target_ms / 1000, tz=timezone.utc).isoformat()
            outcome["actual_observation_time_" + name] = utc_now()
            outcome["delay_seconds_" + name] = round((now_ms - target_ms) / 1000.0, 3)
            outcome["path_status_" + name] = path.get("status")
            outcome["path_precision_" + name] = path.get("path_precision")
            outcome["ambiguous_segments_" + name] = path.get("ambiguous_segments")
            outcome["path_reason_" + name] = path.get("reason")
            outcome["path_detail_" + name] = path.get("detail")
            outcome["agg_trade_sources_" + name] = path.get("agg_trade_sources")

            if path.get("status") != "OK":
                outcome[result_key] = "PATH_UNAVAILABLE"
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
            outcome["short_return_" + name] = round(-raw_return, 8) if raw_return is not None else None
            # Excursion sign convention:
            # MFE is favorable and therefore positive for both LONG and SHORT.
            # MAE is adverse and therefore negative for both LONG and SHORT.
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
            changed += 1

        # Overall status is complete only when every horizon is locked.
        outcome["status"] = "COMPLETE" if all(outcome.get("locked_" + h) is True for h in HORIZONS) else "PENDING"
        new_outcomes[rid] = outcome

    ensure_directories()
    temp = OUTCOMES_FILE_V2.with_suffix(OUTCOMES_FILE_V2.suffix + ".tmp")
    with temp.open("w", encoding="utf-8") as f:
        for rid, outcome in new_outcomes.items():
            f.write(json.dumps({
                "dataset_id": dataset_id,
                "record_id": rid,
                "future_outcome": outcome,
            }, ensure_ascii=False) + "\n")
    temp.replace(OUTCOMES_FILE_V2)

    return {
        "updated": True,
        "dataset_id": dataset_id,
        "changed_horizons": changed,
        "record_count": len(records),
        "outcomes_file": str(OUTCOMES_FILE_V2),
    }


def run():
    assert_dataset_writable(get_dataset_id(), OUTCOMES_FILE_V2)
    print("=" * 72)
    print("BTC AI HUNTER V1.2.3 | OUTCOME EVALUATOR V2.3")
    print("=" * 72)
    print("MODE                : SHADOW_ONLY")
    print("PATH                : 1m kline + aggTrades disambiguation")
    print("TP/STOP TIE         : AMBIGUOUS, never forced TP_FIRST")
    print("REAL_ORDER_SEND     : False")
    print("=" * 72)
    while True:
        try:
            print(update_records())
            time.sleep(CHECK_SECONDS)
        except KeyboardInterrupt:
            print("Outcome evaluator stopped.")
            break
        except Exception as error:
            print("Outcome error:", type(error).__name__, str(error))
            time.sleep(CHECK_SECONDS)


if __name__ == "__main__":
    run()


