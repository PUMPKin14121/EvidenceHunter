# -*- coding: utf-8 -*-

"""BTC HUNTER unified state layer V2.2 - exchange-clock aware, read only."""

import json
from datetime import datetime, timezone

from EvidenceHunter_clock import exchange_now_ms, exchange_utc_now
from EvidenceHunter_config import (
    DATA_DEGRADED_MAX_AGE,
    ORDERFLOW_FILE_V2,
    ORDERFLOW_STATE_MAX_AGE,
    SOURCE_TIME_SKEW_MAX_MS,
    STATE_FILE,
    SYMBOL,
    ensure_directories,
    get_dataset_id,
)
from EvidenceHunter_market import build_market_snapshot
from EvidenceHunter_account import build_account_snapshot


def utc_now():
    return exchange_utc_now()


def load_json(path):
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _parse_time(value):
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None


def get_orderflow_state():
    data = load_json(ORDERFLOW_FILE_V2)
    if not isinstance(data, dict):
        return {
            "ready": False,
            "status": "MISSING",
            "data_quality": "UNAVAILABLE",
            "age_seconds": None,
            "source": {},
        }
    dt = _parse_time(data.get("timestamp"))
    exchange_now = datetime.fromtimestamp(exchange_now_ms() / 1000.0, tz=timezone.utc)
    age = (exchange_now - dt).total_seconds() if dt else None
    ready = (
        data.get("analysis_status") == "READY"
        and data.get("ready") is True
        and age is not None
        and age <= ORDERFLOW_STATE_MAX_AGE
        and data.get("trade_gap") is False
    )
    return {
        "ready": ready,
        "status": "READY" if ready else ("GAP" if data.get("trade_gap") else "STALE"),
        "data_quality": data.get("data_quality", "UNKNOWN"),
        "age_seconds": round(age, 3) if age is not None else None,
        "source": data,
    }


def determine_market_regime_heuristic(market):
    """Legacy descriptive heuristic only; NOT a validated Regime Engine."""
    if not isinstance(market, dict):
        return "UNKNOWN"
    periods = market.get("periods", {})
    p5, p15 = periods.get("5m", {}), periods.get("15m", {})
    atr5, atr15, price = p5.get("atr14"), p15.get("atr14"), market.get("price", 0)
    if not atr5 or not atr15 or not price or price <= 0:
        return "UNKNOWN"
    vol5, vol15 = atr5 / price, atr15 / price
    if vol5 > 0.002 or vol15 > 0.003:
        return "HIGH_VOL"
    if vol5 < 0.0008 and vol15 < 0.0012:
        return "LOW_VOL"
    structure = p15.get("structure", {}) or {}
    support, resistance = structure.get("support"), structure.get("resistance")
    if support and resistance:
        mid = (support + resistance) / 2.0
        if price > mid:
            return "TREND_UP"
        if price < mid:
            return "TREND_DOWN"
    return "RANGE"


def _market_coherence(market):
    if not isinstance(market, dict):
        return False, {"reason": "MARKET_MISSING"}
    price = float(market.get("price", 0) or 0)
    mark = float((market.get("funding") or {}).get("mark_price", 0) or 0)
    time_consistency = market.get("time_consistency") or {}
    quality = market.get("data_quality") or {}
    event_spread = time_consistency.get("source_event_spread_ms", time_consistency.get("source_time_skew_ms"))
    price_mark_gap = abs(price - mark) / price if price > 0 and mark > 0 else None
    pass_price_mark = price_mark_gap is not None and price_mark_gap <= 0.02
    pass_clock = quality.get("clock_sync_valid") is True
    pass_freshness = quality.get("source_freshness_valid") is True

    # Cross-endpoint event-time spread is informational only.  Ticker, funding
    # and OI can update at different moments.  Coherence gates on a healthy
    # Binance-synchronized clock plus source-specific freshness instead.
    return pass_price_mark and pass_clock and pass_freshness, {
        "price_mark_gap_ratio": round(price_mark_gap, 8) if price_mark_gap is not None else None,
        "price_mark_gap_limit": 0.02,
        "source_event_spread_ms": event_spread,
        "source_event_spread_gate": "INFORMATIONAL_ONLY",
        "clock_sync_valid": pass_clock,
        "source_freshness_valid": pass_freshness,
        "clock_offset_ms": time_consistency.get("clock_offset_ms"),
        "local_clock_ahead_ms": time_consistency.get("local_clock_ahead_ms"),
    }


def build_unified_state():
    ensure_directories()
    dataset_id = get_dataset_id()

    market = account = None
    market_error = account_error = None

    # Account is read first; the market snapshot is deliberately built last so
    # the Shadow entry price is as close as possible to the final state time.
    try:
        account = build_account_snapshot()
    except Exception as e:
        account_error = {"type": type(e).__name__, "message": str(e)}
    try:
        market = build_market_snapshot()
    except Exception as e:
        market_error = {"type": type(e).__name__, "message": str(e)}

    orderflow = get_orderflow_state()
    market_quality = market.get("data_quality", {}) if isinstance(market, dict) else {}
    account_info = account.get("account", {}) if isinstance(account, dict) else {}
    account_position = account.get("position", {}) if isinstance(account, dict) else {}
    position_coherent = not bool(account_position.get("position_ambiguous", False))

    validity = (
        isinstance(market, dict)
        and market_quality.get("price_valid", False)
        and market_quality.get("funding_valid", False)
        and market_quality.get("oi_valid", False)
        and isinstance(account, dict)
        and float(account_info.get("total_margin_balance", 0) or 0) > 0
        and market_error is None
        and account_error is None
    )
    completeness = (
        isinstance(market, dict)
        and isinstance(account, dict)
        and isinstance(orderflow.get("source"), dict)
        and bool(orderflow.get("source"))
    )
    continuity = orderflow.get("source", {}).get("trade_gap") is False
    freshness = bool(orderflow.get("ready"))
    coherence, coherence_detail = _market_coherence(market)
    coherence = coherence and position_coherent
    coherence_detail["account_position_coherent"] = position_coherent
    coherence_detail["account_position_side"] = account_position.get("side", "UNKNOWN")

    dimensions = {
        "freshness": {"pass": freshness, "orderflow_age_seconds": orderflow.get("age_seconds")},
        "completeness": {"pass": completeness},
        "continuity": {"pass": continuity, "trade_gap": orderflow.get("source", {}).get("trade_gap")},
        "coherence": {"pass": coherence, **coherence_detail},
        "validity": {"pass": validity},
    }

    all_pass = all(v.get("pass") for v in dimensions.values())
    severe_stale = orderflow.get("age_seconds") is not None and orderflow["age_seconds"] > DATA_DEGRADED_MAX_AGE
    if all_pass:
        status = "DATA_READY"
    elif severe_stale or market_error or account_error or not continuity or not coherence:
        status = "DATA_DEGRADED"
    else:
        status = "DATA_INCOMPLETE"

    regime = determine_market_regime_heuristic(market)
    state = {
        "version": "STATE_V2.2",
        "timestamp": utc_now(),
        "dataset_id": dataset_id,
        "symbol": SYMBOL,
        "market_type": "USD_M_FUTURES",
        "status": status,
        "market_regime": regime,
        "market_regime_status": "UNVALIDATED_HEURISTIC",
        "market_regime_source": "LEGACY_5M_15M_ATR_STRUCTURE_HEURISTIC",
        "data_quality_dimensions": dimensions,
        "layers": {
            "market": {"ready": validity and isinstance(market, dict)},
            "account": {"ready": isinstance(account, dict) and account_error is None},
            "orderflow": {"ready": orderflow.get("ready", False)},
        },
        "market": market,
        "account": account,
        "orderflow": orderflow,
        "decision": {
            "action": "WAIT",
            "reason": "决策层尚未接入；当前仅采样/研究",
            "ev_status": "NOT_CALIBRATED",
        },
        "errors": {"market": market_error, "account": account_error},
        "safety": {
            "read_only": True,
            "paper_only": True,
            "auto_trade": False,
            "auto_cancel": False,
            "manual_confirmation_required": True,
        },
    }

    temp = STATE_FILE.with_suffix(STATE_FILE.suffix + ".tmp")
    with temp.open("w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    temp.replace(STATE_FILE)
    return state


if __name__ == "__main__":
    try:
        result = build_unified_state()
        print("STATUS:", result["status"])
        print("DATASET_ID:", result.get("dataset_id"))
        print("REGIME_HEURISTIC:", result.get("market_regime"), result.get("market_regime_status"))
        print("QUALITY:", result.get("data_quality_dimensions"))
    except Exception as error:
        print("STATE_STATUS: ERROR")
        print(type(error).__name__ + ": " + str(error))


