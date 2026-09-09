# -*- coding: utf-8 -*-

"""BTC HUNTER Shadow Observation Recorder V2.2.

Research-only. No OPEN_ALERT, no order submission, no trade permission.
"""

import json
import time
import uuid
from EvidenceHunter_config import assert_dataset_writable

from EvidenceHunter_config import (
    COLLECTOR_VERSION,
    FEATURE_VERSION,
    OUTCOME_VERSION,
    RUN_ID,
    SCHEMA_VERSION,
    SHADOW_FILE_V2,
    ensure_directories,
    get_dataset_id,
)
from EvidenceHunter_clock import exchange_minute, exchange_now_ms, exchange_utc_now, monotonic_seconds
from EvidenceHunter_state import build_unified_state
from EvidenceHunter_features import build_features

OBSERVATION_INTERVAL_SECONDS = 60
WATCHDOG_SECONDS = 180
FAST_RETRY_SECONDS = 5
ERROR_RETRY_SECONDS = 10


def utc_now():
    return exchange_utc_now()


def current_minute():
    return exchange_minute()


def sleep_until_next_exchange_minute(offset_seconds=1.0):
    """Avoid cumulative drift from `build time + 60s` sleeps.

    The next attempt is aligned to Binance-adjusted minute boundaries.  If the
    machine is slow, this prevents a few seconds of processing time from
    accumulating until an entire observation minute is skipped.
    """
    now_seconds = exchange_now_ms() / 1000.0
    next_boundary = (int(now_seconds // 60) + 1) * 60 + float(offset_seconds)
    delay = max(0.5, next_boundary - now_seconds)
    time.sleep(delay)


def load_existing_minutes(dataset_id):
    result = set()
    if not SHADOW_FILE_V2.exists():
        return result
    try:
        with SHADOW_FILE_V2.open("r", encoding="utf-8") as f:
            for line in f:
                try:
                    row = json.loads(line)
                    if row.get("dataset_id") != dataset_id:
                        continue
                    minute = row.get("observation_minute")
                    if minute:
                        result.add(minute)
                except Exception:
                    continue
    except Exception:
        pass
    return result


def append_record(record):
    assert_dataset_writable(record.get("dataset_id"), SHADOW_FILE_V2)
    ensure_directories()
    with SHADOW_FILE_V2.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")
        f.flush()


def build_observation():
    dataset_id = get_dataset_id()
    assert_dataset_writable(dataset_id, SHADOW_FILE_V2)
    if not dataset_id:
        return {"recorded": False, "reason": "DATASET_NOT_INITIALIZED"}

    state = build_unified_state()
    if state.get("status") != "DATA_READY":
        return {
            "recorded": False,
            "reason": "UNIFIED_DATA_NOT_READY",
            "state_status": state.get("status"),
            "quality": state.get("data_quality_dimensions"),
        }

    features = build_features()
    orderflow = features.get("orderflow", {})
    quality = features.get("quality", {})
    if not quality.get("orderflow_ready", False):
        return {"recorded": False, "reason": "ORDERFLOW_NOT_READY"}

    minute = current_minute()
    if minute in load_existing_minutes(dataset_id):
        return {
            "recorded": False,
            "reason": "CURRENT_MINUTE_ALREADY_RECORDED",
            "observation_minute": minute,
        }

    market = state.get("market") or {}
    position = features.get("position") or {}
    market_regime = state.get("market_regime", "UNKNOWN")
    market_regime_status = state.get("market_regime_status", "UNKNOWN")

    price = float(market.get("price", 0) or 0)
    atr_5m = float((market.get("periods") or {}).get("5m", {}).get("atr14", 0) or 0)
    atr_15m = float((market.get("periods") or {}).get("15m", {}).get("atr14", 0) or 0)

    # Research label only. Not a live signal or candidate gate.
    orderflow_state = features.get("classification", {}).get("orderflow_state", "UNKNOWN")
    if orderflow_state == "NO_CONTRADICTION":
        taker_imb = float(orderflow.get("taker_imbalance", 0) or 0)
        depth_imb = float(orderflow.get("depth100_imbalance", 0) or 0)
        if taker_imb > 0 and depth_imb > 0:
            research_direction = "LONG"
        elif taker_imb < 0 and depth_imb < 0:
            research_direction = "SHORT"
        else:
            research_direction = "FLAT"
    else:
        research_direction = "FLAT"

    entry = price
    stop_distance = max(atr_5m * 1.5, atr_15m, price * 0.003) if price > 0 else 0.0
    stop = tp1 = tp2 = None
    if research_direction == "LONG" and stop_distance > 0:
        stop = round(entry - stop_distance, 2)
        tp1 = round(entry + stop_distance * 1.5, 2)
        tp2 = round(entry + stop_distance * 3.0, 2)
    elif research_direction == "SHORT" and stop_distance > 0:
        stop = round(entry + stop_distance, 2)
        tp1 = round(entry - stop_distance * 1.5, 2)
        tp2 = round(entry - stop_distance * 3.0, 2)

    trade_plan = {
        "plan_id": "BASELINE_TRADE_PLAN_V1.2.3",
        "direction": research_direction,
        "entry": entry,
        "stop": stop,
        "tp1": tp1,
        "tp2": tp2,
        "initial_risk_r": 1.0,
        "tp1_r": 1.5,
        "tp2_r": 3.0,
        "lifecycle_policy": "BASELINE_TP1_TP2_PATH_RESEARCH",
        "holding_time": "30m",
        "research_only": True,
        "is_signal": False,
    }

    observation = {
        "orderflow_state": orderflow_state,
        "taker_imbalance": orderflow.get("taker_imbalance"),
        "depth100_imbalance": orderflow.get("depth100_imbalance"),
        "depth1000_imbalance": orderflow.get("depth1000_imbalance"),
        "taker_depth100_contradiction": orderflow.get("taker_depth100_contradiction"),
        "taker_depth1000_contradiction": orderflow.get("taker_depth1000_contradiction"),
        "bid_wall_count": orderflow.get("bid_wall_count", 0),
        "ask_wall_count": orderflow.get("ask_wall_count", 0),
        "book_bid_reduction_usdt": orderflow.get("book_bid_reduction_usdt"),
        "book_ask_reduction_usdt": orderflow.get("book_ask_reduction_usdt"),
        "position_side": position.get("side", "FLAT"),
    }

    now_iso = utc_now()
    time_consistency = market.get("time_consistency") or {}
    price_detail = market.get("price_detail") or {}
    record = {
        "record_id": str(uuid.uuid4()),
        "dataset_id": dataset_id,
        "schema_version": SCHEMA_VERSION,
        "feature_version": FEATURE_VERSION,
        "collector_version": COLLECTOR_VERSION,
        "outcome_version": OUTCOME_VERSION,
        "run_id": RUN_ID,
        "version": "SHADOW_V2.3",
        "timestamp": now_iso,
        "snapshot_created_time": now_iso,
        # Aggregate market timestamp is kept for cross-source consistency checks.
        "exchange_event_time_ms": time_consistency.get("exchange_event_time_ms"),
        "collector_received_time_ms": time_consistency.get("collector_received_time_ms"),
        "source_time_skew_ms": time_consistency.get("source_time_skew_ms"),
        "source_event_spread_ms": time_consistency.get("source_event_spread_ms"),
        "clock_offset_ms": time_consistency.get("clock_offset_ms"),
        "local_clock_ahead_ms": time_consistency.get("local_clock_ahead_ms"),
        "clock_rtt_ms": time_consistency.get("clock_rtt_ms"),
        "clock_sync_age_seconds": time_consistency.get("clock_sync_age_seconds"),
        "clock_policy": (market.get("clock") or {}).get("clock_policy"),
        # Outcome path must start from the timestamp of the observed entry price,
        # not from the newest timestamp among price/funding/OI.
        "price_source": price_detail.get("source"),
        "price_trade_id": price_detail.get("canonical_trade_id"),
        "price_exchange_event_time_ms": price_detail.get("exchange_event_time_ms"),
        "price_collector_received_time_ms": price_detail.get("collector_received_time_ms"),
        "observation_minute": minute,
        "symbol": state.get("symbol", "BTCUSDT"),
        "mode": "SHADOW_ONLY",
        "real_order_send": False,
        "market_regime": market_regime,
        "market_regime_status": market_regime_status,
        "signal_source": "orderflow_label",
        "research_direction": research_direction,
        "price": price,
        "mark_price": (market.get("funding") or {}).get("mark_price"),
        "funding_rate": (market.get("funding") or {}).get("last_funding_rate"),
        "next_funding_time": (market.get("funding") or {}).get("next_funding_time"),
        "funding_interval_hours": (market.get("funding") or {}).get("funding_interval_hours"),
        "funding_interval_minutes": (market.get("funding") or {}).get("funding_interval_minutes"),
        "time_to_funding_seconds": (market.get("funding") or {}).get("time_to_funding_seconds"),
        "open_interest": market.get("open_interest"),
        "market_snapshot_time": market.get("timestamp"),
        "data_quality_dimensions": state.get("data_quality_dimensions", {}),
        "features": features,
        "observation": observation,
        "trade_plan": trade_plan,
        "future_outcome": {"status": "PENDING"},
    }
    append_record(record)
    return {
        "recorded": True,
        "record_id": record["record_id"],
        "dataset_id": dataset_id,
        "observation_minute": minute,
        "price": price,
        "research_direction": research_direction,
        "market_regime": market_regime,
    }


def run():
    dataset_id = get_dataset_id()
    assert_dataset_writable(dataset_id, SHADOW_FILE_V2)
    if not dataset_id:
        raise RuntimeError("DATASET_NOT_INITIALIZED: run EvidenceHunter_dataset.py prepare --yes")
    ensure_directories()
    print("=" * 72)
    print("BTC AI HUNTER V1.2.3 | SHADOW RECORDER V2.3")
    print("=" * 72)
    print("DATASET_ID          :", dataset_id)
    print("MODE                : SHADOW_ONLY")
    print("REAL_ORDER_SEND     : False")
    print("SCHEMA_VERSION      :", SCHEMA_VERSION)
    print("RUN_ID              :", RUN_ID)
    print("=" * 72)

    last_success = 0.0
    while True:
        try:
            result = build_observation()
            if result.get("recorded"):
                last_success = monotonic_seconds()
                print("RECORDED:", result)
                sleep_until_next_exchange_minute(offset_seconds=1.0)
            else:
                print("SKIP:", result)
                if result.get("reason") == "CURRENT_MINUTE_ALREADY_RECORDED":
                    time.sleep(FAST_RETRY_SECONDS)
                else:
                    time.sleep(FAST_RETRY_SECONDS)
            if last_success and monotonic_seconds() - last_success > WATCHDOG_SECONDS:
                print("WATCHDOG: no successful record for", round(monotonic_seconds() - last_success, 1), "seconds")
        except KeyboardInterrupt:
            print("Shadow recorder stopped.")
            break
        except Exception as e:
            print("Shadow recorder error:", type(e).__name__, str(e))
            time.sleep(ERROR_RETRY_SECONDS)


if __name__ == "__main__":
    run()


