# -*- coding: utf-8 -*-

"""BTC HUNTER USD-M Futures account layer - read only."""

import json
from datetime import datetime, timezone

from binance_api import client
from EvidenceHunter_clock import exchange_utc_now
from EvidenceHunter_config import RUNTIME_DIR, SYMBOL, ensure_directories

ACCOUNT_FILE = RUNTIME_DIR / "account_snapshot.json"


def utc_now():
    return exchange_utc_now()


def safe_float(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def safe_int(value, default=None):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def read_position(account):
    """Return a conservative BTC position summary.

    In hedge mode Binance can expose simultaneous LONG and SHORT rows. Do not
    silently pick one side; mark the summary as HEDGE/ambiguous and retain the
    per-side rows for research/data-quality checks.
    """
    rows = []
    for position in account.get("positions", []) or []:
        if position.get("symbol") != SYMBOL:
            continue
        raw_amount = safe_float(position.get("positionAmt"))
        if raw_amount == 0:
            continue
        rows.append({
            "position_side": str(position.get("positionSide", "BOTH")),
            "side": "LONG" if raw_amount > 0 else "SHORT",
            "amount": abs(raw_amount),
            "signed_amount": raw_amount,
            "entry_price": safe_float(position.get("entryPrice")),
            "break_even_price": safe_float(position.get("breakEvenPrice")),
            "mark_price": safe_float(position.get("markPrice")),
            "unrealized_pnl": safe_float(position.get("unRealizedProfit", position.get("unrealizedProfit", 0))),
            "liquidation_price": safe_float(position.get("liquidationPrice")),
            "leverage_setting": safe_int(position.get("leverage"), 0) or 0,
            "margin_type": str(position.get("marginType", "UNKNOWN")),
            "notional": abs(safe_float(position.get("notional"))),
            "initial_margin": safe_float(position.get("initialMargin")),
            "maint_margin": safe_float(position.get("maintMargin")),
            "position_initial_margin": safe_float(position.get("positionInitialMargin")),
            "open_order_initial_margin": safe_float(position.get("openOrderInitialMargin")),
            "adl": safe_int(position.get("adl")),
            "update_time": safe_int(position.get("updateTime")),
        })

    empty = {
        "side": "FLAT",
        "amount": 0.0,
        "signed_amount": 0.0,
        "entry_price": 0.0,
        "break_even_price": 0.0,
        "mark_price": 0.0,
        "unrealized_pnl": 0.0,
        "liquidation_price": 0.0,
        "leverage_setting": 0,
        "margin_type": "UNKNOWN",
        "notional": 0.0,
        "initial_margin": 0.0,
        "maint_margin": 0.0,
        "position_initial_margin": 0.0,
        "open_order_initial_margin": 0.0,
        "adl": None,
        "update_time": None,
        "has_position": False,
        "position_count": 0,
        "position_ambiguous": False,
        "position_rows": [],
    }
    if not rows:
        return empty

    if len(rows) == 1:
        result = dict(rows[0])
        result.update({
            "has_position": True,
            "position_count": 1,
            "position_ambiguous": False,
            "position_rows": rows,
        })
        return result

    # Simultaneous BTC sides: preserve totals but do not invent one entry/stop side.
    return {
        **empty,
        "side": "HEDGE",
        "amount": sum(r["amount"] for r in rows),
        "signed_amount": sum(r["signed_amount"] for r in rows),
        "unrealized_pnl": sum(r["unrealized_pnl"] for r in rows),
        "notional": sum(r["notional"] for r in rows),
        "initial_margin": sum(r["initial_margin"] for r in rows),
        "maint_margin": sum(r["maint_margin"] for r in rows),
        "position_initial_margin": sum(r["position_initial_margin"] for r in rows),
        "open_order_initial_margin": sum(r["open_order_initial_margin"] for r in rows),
        "has_position": True,
        "position_count": len(rows),
        "position_ambiguous": True,
        "position_rows": rows,
    }


def read_open_orders():
    orders = client.futures_get_open_orders(symbol=SYMBOL)
    result = []
    for order in orders or []:
        result.append({
            "order_id": order.get("orderId"),
            "client_order_id": order.get("clientOrderId"),
            "symbol": order.get("symbol"),
            "side": order.get("side"),
            "position_side": order.get("positionSide"),
            "type": order.get("type"),
            "orig_type": order.get("origType"),
            "status": order.get("status"),
            "time_in_force": order.get("timeInForce"),
            "working_type": order.get("workingType"),
            "price": safe_float(order.get("price")),
            "avg_price": safe_float(order.get("avgPrice")),
            "stop_price": safe_float(order.get("stopPrice")),
            "original_quantity": safe_float(order.get("origQty")),
            "executed_quantity": safe_float(order.get("executedQty")),
            "cum_quote": safe_float(order.get("cumQuote")),
            "reduce_only": bool(order.get("reduceOnly", False)),
            "close_position": bool(order.get("closePosition", False)),
            "order_time": safe_int(order.get("time")),
            "update_time": safe_int(order.get("updateTime")),
            "good_till_date": safe_int(order.get("goodTillDate")),
        })
    return result


def build_account_snapshot():
    ensure_directories()
    account = client.futures_account()

    total_wallet_balance = safe_float(account.get("totalWalletBalance"))
    total_margin_balance = safe_float(account.get("totalMarginBalance"))
    available_balance = safe_float(account.get("availableBalance"))
    total_unrealized_pnl = safe_float(account.get("totalUnrealizedProfit"))
    total_initial_margin = safe_float(account.get("totalInitialMargin"))
    total_maint_margin = safe_float(account.get("totalMaintMargin"))
    total_position_initial_margin = safe_float(account.get("totalPositionInitialMargin"))
    total_open_order_initial_margin = safe_float(account.get("totalOpenOrderInitialMargin"))

    position = read_position(account)
    open_orders = read_open_orders()

    nominal_value = position["notional"]
    if nominal_value <= 0 and position["entry_price"] > 0:
        nominal_value = position["entry_price"] * position["amount"]

    effective_leverage = nominal_value / total_margin_balance if total_margin_balance > 0 and nominal_value > 0 else 0.0

    snapshot = {
        "version": "ACCOUNT_V1.2.2",
        "timestamp": utc_now(),
        "symbol": SYMBOL,
        "mode": "READ_ONLY",
        "account": {
            "total_wallet_balance": total_wallet_balance,
            "total_margin_balance": total_margin_balance,
            "available_balance": available_balance,
            "total_unrealized_pnl": total_unrealized_pnl,
            "total_initial_margin": total_initial_margin,
            "total_maint_margin": total_maint_margin,
            "total_position_initial_margin": total_position_initial_margin,
            "total_open_order_initial_margin": total_open_order_initial_margin,
            "account_update_time": safe_int(account.get("updateTime")),
        },
        "position": position,
        "open_orders": open_orders,
        "exposure": {
            "nominal_value": round(nominal_value, 8),
            "effective_leverage": round(effective_leverage, 6),
            "open_order_count": len(open_orders),
        },
        "safety": {
            "read_only": True,
            "real_order_send": False,
            "cancel_order": False,
            "modify_order": False,
        },
    }

    temp_file = ACCOUNT_FILE.with_suffix(ACCOUNT_FILE.suffix + ".tmp")
    with temp_file.open("w", encoding="utf-8") as f:
        json.dump(snapshot, f, ensure_ascii=False, indent=2)
    temp_file.replace(ACCOUNT_FILE)
    return snapshot


def print_snapshot(snapshot):
    account = snapshot["account"]
    position = snapshot["position"]
    print("=" * 72)
    print("BTC AI HUNTER V1.2.2 | ACCOUNT SNAPSHOT")
    print("=" * 72)
    print("EQUITY              :", account["total_margin_balance"])
    print("AVAILABLE           :", account["available_balance"])
    print("POSITION SIDE       :", position["side"])
    print("POSITION AMOUNT     :", position["amount"])
    print("ENTRY PRICE         :", position["entry_price"])
    print("BREAK EVEN PRICE    :", position["break_even_price"])
    print("LIQUIDATION PRICE   :", position["liquidation_price"])
    print("OPEN ORDERS         :", len(snapshot["open_orders"]))
    print("REAL_ORDER_SEND     : False")
    print("=" * 72)


if __name__ == "__main__":
    try:
        print_snapshot(build_account_snapshot())
    except Exception as error:
        print("ACCOUNT_STATUS: ERROR")
        print(type(error).__name__ + ": " + str(error))


