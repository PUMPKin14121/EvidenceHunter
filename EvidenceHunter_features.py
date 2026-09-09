# -*- coding: utf-8 -*-

"""
BTC AI Hunter V1.2.2
Unified Feature Layer V2.1

读取：
- 永续市场状态
- 账户状态
- REST订单流状态

输出：
- 多周期指标
- 支撑阻力距离
- TOP100深度失衡
- TOP1000深度失衡
- 主动成交失衡
- 订单流矛盾状态

本模块：
- 不产生交易方向；
- 不计算最终EV；
- 不发出开仓警报；
- 不下单；
- 不撤单。
"""

import json
from datetime import datetime, timezone

from EvidenceHunter_clock import exchange_utc_now
from EvidenceHunter_config import (
    STATE_FILE,
    RUNTIME_DIR,
    ensure_directories,
)


FEATURE_FILE = RUNTIME_DIR / "features_snapshot.json"


def utc_now():
    return exchange_utc_now()


def safe_float(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def load_state():
    if not STATE_FILE.exists():
        raise FileNotFoundError(
            f"找不到统一状态文件: {STATE_FILE}"
        )

    with STATE_FILE.open(
        "r",
        encoding="utf-8",
    ) as file:
        return json.load(file)


def get_period(state, name):
    market = state.get("market") or {}
    periods = market.get("periods") or {}

    value = periods.get(name)

    if isinstance(value, dict):
        return value

    return {}


def distance_to_support_percent(price, support):
    price = safe_float(price)
    support = safe_float(support)

    if price <= 0 or support <= 0:
        return None

    return round(
        (price - support)
        / price
        * 100,
        6,
    )


def distance_to_resistance_percent(price, resistance):
    price = safe_float(price)
    resistance = safe_float(resistance)

    if price <= 0 or resistance <= 0:
        return None

    return round(
        (resistance - price)
        / price
        * 100,
        6,
    )


def build_timeframe_features(state):
    market = state.get("market") or {}
    price = safe_float(
        market.get("price")
    )

    result = {}

    for timeframe in [
        "1d",
        "4h",
        "1h",
        "15m",
        "5m",
    ]:
        period = get_period(
            state,
            timeframe,
        )

        structure = period.get(
            "structure",
            {},
        )

        if not isinstance(structure, dict):
            structure = {}

        support = structure.get(
            "support"
        )

        resistance = structure.get(
            "resistance"
        )

        result[timeframe] = {
            "rsi14": period.get(
                "rsi14"
            ),

            "atr14": period.get(
                "atr14"
            ),

            "volume_ratio": period.get(
                "volume_ratio"
            ),

            "taker_buy_ratio": period.get(
                "taker_buy_ratio"
            ),

            "last_closed_price": period.get(
                "last_closed_price"
            ),

            "support": support,
            "resistance": resistance,

            "distance_to_support_percent": (
                distance_to_support_percent(
                    price,
                    support,
                )
            ),

            "distance_to_resistance_percent": (
                distance_to_resistance_percent(
                    price,
                    resistance,
                )
            ),

            "candle_count": period.get(
                "candle_count",
                0,
            ),
        }

    return result


def get_depth_layer(orderbook, key):
    value = orderbook.get(key)

    if isinstance(value, dict):
        return value

    return {
        "levels_used": 0,
        "bid_usdt": 0.0,
        "ask_usdt": 0.0,
        "imbalance": 0.0,
    }


def build_orderflow_features(state):
    orderflow_state = state.get(
        "orderflow",
        {},
    )

    source = orderflow_state.get(
        "source",
        {},
    )

    if not isinstance(source, dict):
        source = {}

    trade_flow = source.get(
        "trade_flow",
        {},
    )

    orderbook = source.get(
        "orderbook",
        {},
    )

    if not isinstance(trade_flow, dict):
        trade_flow = {}

    if not isinstance(orderbook, dict):
        orderbook = {}

    # 新版字段
    depth_100 = get_depth_layer(
        orderbook,
        "depth_100",
    )

    depth_1000 = get_depth_layer(
        orderbook,
        "depth_1000",
    )

    # 兼容旧版字段
    if depth_100.get("levels_used", 0) == 0:
        depth_100 = get_depth_layer(
            orderbook,
            "depth",
        )

    if depth_1000.get("levels_used", 0) == 0:
        depth_1000 = depth_100

    walls = orderbook.get(
        "walls",
        {},
    )

    reductions = orderbook.get(
        "reductions",
        {},
    )

    if not isinstance(walls, dict):
        walls = {}

    if not isinstance(reductions, dict):
        reductions = {}

    taker_imbalance = safe_float(
        trade_flow.get(
            "taker_imbalance"
        )
    )

    depth_100_imbalance = safe_float(
        depth_100.get(
            "imbalance"
        )
    )

    depth_1000_imbalance = safe_float(
        depth_1000.get(
            "imbalance"
        )
    )

    # 主动成交和近端盘口方向是否相反
    contradiction_100 = (
        taker_imbalance
        * depth_100_imbalance
        < 0
    )

    # 主动成交和深层盘口方向是否相反
    contradiction_1000 = (
        taker_imbalance
        * depth_1000_imbalance
        < 0
    )

    # 兼容旧字段名称
    bid_reduction = reductions.get(
        "book_bid_reduction_usdt",
        reductions.get(
            "bid_reduction_usdt",
            0,
        ),
    )

    ask_reduction = reductions.get(
        "book_ask_reduction_usdt",
        reductions.get(
            "ask_reduction_usdt",
            0,
        ),
    )

    return {
        "transport": source.get(
            "transport",
            "REST_POLLING",
        ),

        "data_quality": orderflow_state.get(
            "data_quality",
            source.get(
                "data_quality",
                "UNKNOWN",
            ),
        ),

        "ready": orderflow_state.get(
            "ready",
            False,
        ),

        "last_trade_price": safe_float(
            source.get(
                "last_trade_price"
            )
        ),

        "aggressive_buy_usdt": safe_float(
            trade_flow.get(
                "aggressive_buy_usdt"
            )
        ),

        "aggressive_sell_usdt": safe_float(
            trade_flow.get(
                "aggressive_sell_usdt"
            )
        ),

        "taker_imbalance": taker_imbalance,

        "depth100_bid_usdt": safe_float(
            depth_100.get(
                "bid_usdt"
            )
        ),

        "depth100_ask_usdt": safe_float(
            depth_100.get(
                "ask_usdt"
            )
        ),

        "depth100_imbalance": (
            depth_100_imbalance
        ),

        "depth1000_bid_usdt": safe_float(
            depth_1000.get(
                "bid_usdt"
            )
        ),

        "depth1000_ask_usdt": safe_float(
            depth_1000.get(
                "ask_usdt"
            )
        ),

        "depth1000_imbalance": (
            depth_1000_imbalance
        ),

        "bid_wall_count": len(
            walls.get(
                "bid_walls",
                [],
            )
        ),

        "ask_wall_count": len(
            walls.get(
                "ask_walls",
                [],
            )
        ),

        "book_bid_reduction_usdt": safe_float(
            bid_reduction
        ),

        "book_ask_reduction_usdt": safe_float(
            ask_reduction
        ),

        "book_reduction_interpretation": (
            reductions.get(
                "interpretation",
                "UNKNOWN",
            )
        ),

        "taker_depth100_contradiction": (
            contradiction_100
        ),

        "taker_depth1000_contradiction": (
            contradiction_1000
        ),

        "depth_levels_100": depth_100.get(
            "levels_used",
            0,
        ),

        "depth_levels_1000": depth_1000.get(
            "levels_used",
            0,
        ),
    }


def build_position_features(state):
    account = state.get(
        "account"
    ) or {}

    position = account.get(
        "position"
    ) or {}

    exposure = account.get(
        "exposure"
    ) or {}

    return {
        "side": position.get(
            "side",
            "FLAT",
        ),

        "amount": safe_float(
            position.get(
                "amount"
            )
        ),

        "entry_price": safe_float(
            position.get(
                "entry_price"
            )
        ),

        "mark_price": safe_float(
            position.get(
                "mark_price"
            )
        ),

        "unrealized_pnl": safe_float(
            position.get(
                "unrealized_pnl"
            )
        ),

        "liquidation_price": safe_float(
            position.get(
                "liquidation_price"
            )
        ),

        "effective_leverage": safe_float(
            exposure.get(
                "effective_leverage"
            )
        ),

        "open_order_count": len(
            account.get(
                "open_orders",
                [],
            )
        ),
    }


def build_features():
    state = load_state()

    market = state.get(
        "market"
    ) or {}

    price = safe_float(
        market.get("price")
    )

    timeframe_features = (
        build_timeframe_features(
            state
        )
    )

    orderflow_features = (
        build_orderflow_features(
            state
        )
    )

    position_features = (
        build_position_features(
            state
        )
    )

    layers = state.get(
        "layers",
        {},
    )

    quality = {
        "state_ready": (
            state.get("status")
            == "DATA_READY"
        ),

        "market_ready": layers.get(
            "market",
            {},
        ).get(
            "ready",
            False,
        ),

        "account_ready": layers.get(
            "account",
            {},
        ).get(
            "ready",
            False,
        ),

        "orderflow_ready": (
            orderflow_features.get(
                "ready",
                False,
            )
        ),

        "orderflow_approximate": (
            orderflow_features.get(
                "data_quality"
            )
            == "APPROXIMATE"
        ),
    }

    if (
        orderflow_features.get(
            "taker_depth100_contradiction"
        )
        or orderflow_features.get(
            "taker_depth1000_contradiction"
        )
    ):
        orderflow_state = "CONTRADICTION"
    else:
        orderflow_state = "NO_CONTRADICTION"

    feature_state = {
        "version": "FEATURES_V2.2",
        "dataset_id": state.get("dataset_id"),
        "timestamp": utc_now(),
        "symbol": state.get(
            "symbol",
            "BTCUSDT",
        ),
        "price": price,
        "timeframes": timeframe_features,
        "orderflow": orderflow_features,
        "position": position_features,
        "quality": quality,
        "data_quality_dimensions": state.get("data_quality_dimensions", {}),
        "market_regime": state.get("market_regime", "UNKNOWN"),
        "market_regime_status": state.get("market_regime_status", "UNKNOWN"),
        "funding": {
            "last_funding_rate": (market.get("funding") or {}).get("last_funding_rate"),
            "next_funding_time": (market.get("funding") or {}).get("next_funding_time"),
            "funding_interval_hours": (market.get("funding") or {}).get("funding_interval_hours"),
            "funding_interval_minutes": (market.get("funding") or {}).get("funding_interval_minutes"),
            "time_to_funding_seconds": (market.get("funding") or {}).get("time_to_funding_seconds"),
        },
        "time_consistency": market.get("time_consistency", {}),

        "classification": {
            "orderflow_state": (
                orderflow_state
            ),
            "decision_status": (
                "OBSERVATION_ONLY"
            ),
        },
    }

    ensure_directories()

    temp_file = FEATURE_FILE.with_suffix(
        FEATURE_FILE.suffix
        + ".tmp"
    )

    with temp_file.open(
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(
            feature_state,
            file,
            ensure_ascii=False,
            indent=2,
        )

    temp_file.replace(
        FEATURE_FILE
    )

    return feature_state


def print_features(features):
    orderflow = features[
        "orderflow"
    ]

    quality = features[
        "quality"
    ]

    classification = features[
        "classification"
    ]

    print("=" * 72)
    print("BTC AI HUNTER V1 | FEATURE SNAPSHOT V2")
    print("=" * 72)
    print(
        f"PRICE               : "
        f"{features['price']}"
    )
    print(
        "MARKET READY        : "
        f"{quality['market_ready']}"
    )
    print(
        "ACCOUNT READY       : "
        f"{quality['account_ready']}"
    )
    print(
        "ORDERFLOW READY     : "
        f"{quality['orderflow_ready']}"
    )
    print(
        "ORDERFLOW QUALITY   : "
        f"{orderflow['data_quality']}"
    )
    print("-" * 72)
    print(
        "TAKER IMBALANCE     : "
        f"{orderflow['taker_imbalance']}"
    )
    print(
        "DEPTH100 IMBALANCE  : "
        f"{orderflow['depth100_imbalance']}"
    )
    print(
        "DEPTH1000 IMBALANCE : "
        f"{orderflow['depth1000_imbalance']}"
    )
    print(
        "BUY WALL COUNT      : "
        f"{orderflow['bid_wall_count']}"
    )
    print(
        "SELL WALL COUNT     : "
        f"{orderflow['ask_wall_count']}"
    )
    print(
        "TAKER/DEPTH100     : "
        f"{orderflow['taker_depth100_contradiction']}"
    )
    print(
        "TAKER/DEPTH1000    : "
        f"{orderflow['taker_depth1000_contradiction']}"
    )
    print("-" * 72)
    print(
        "ORDERFLOW STATE     : "
        f"{classification['orderflow_state']}"
    )
    print(
        "DECISION STATUS     : "
        f"{classification['decision_status']}"
    )
    print(
        f"FILE                : "
        f"{FEATURE_FILE}"
    )
    print("FEATURE_STATUS      : READY")
    print("=" * 72)


if __name__ == "__main__":
    try:
        result = build_features()
        print_features(result)

    except Exception as error:
        print("=" * 72)
        print("FEATURE_STATUS: ERROR")
        print(
            type(error).__name__
            + ": "
            + str(error)
        )
        print("=" * 72)

