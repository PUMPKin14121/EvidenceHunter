# -*- coding: utf-8 -*-

"""BTC HUNTER REST orderflow collector V1.2.2 with exchange-clock timing.

Read-only. Adds restart continuity, sticky gap detection, exchange timestamps,
and dataset identity. REST order book remains approximate by design.
"""

import json
import time
from collections import deque
from datetime import datetime, timezone

from binance_api import client
from EvidenceHunter_clock import exchange_now_ms, exchange_utc_now, get_clock_status, monotonic_seconds
from EvidenceHunter_config import (
    ORDERBOOK_DEPTH_LIMIT,
    ORDERFLOW_FILE_V2,
    SYMBOL,
    ensure_directories,
    get_dataset_id,
)

OUTPUT_FILE = ORDERFLOW_FILE_V2
ORDERBOOK_REFRESH_SECONDS = 5
TRADES_REFRESH_SECONDS = 2
OUTPUT_REFRESH_SECONDS = 2
TRADE_WINDOW_SECONDS = 60
TRADE_REQUEST_LIMIT = 1000
MAX_CATCHUP_PAGES_PER_POLL = 2
WALL_USD_THRESHOLD = 20_000_000
MAX_ORDERBOOK_AGE = 12
MAX_TRADES_AGE = 8


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


class RestOrderflowCollector:
    def __init__(self):
        self.dataset_id = get_dataset_id()
        if not self.dataset_id:
            raise RuntimeError(
                "DATASET_NOT_INITIALIZED: run `python EvidenceHunter_dataset.py prepare --yes` first"
            )
        self.bids = {}
        self.asks = {}
        self.previous_bids = {}
        self.previous_asks = {}
        self.trades = deque()
        self.seen_trade_ids = set()
        self.last_trade_id = None
        self.last_price = 0.0
        self.last_orderbook_time = None
        self.last_trade_time = None
        self.last_orderbook_fetch = 0.0
        self.last_trade_fetch = 0.0
        self.last_orderbook_exchange_time_ms = None
        self.last_orderbook_message_time_ms = None
        self.last_orderbook_update_id = None
        self.last_trade_exchange_time_ms = None
        self.orderbook_error = None
        self.trade_error = None
        self.trade_gap = False  # sticky within a dataset
        self.gap_expected_id = None
        self.gap_first_returned_id = None
        self.restored_cursor = False
        self._restore_cursor()

    def _restore_cursor(self):
        if not OUTPUT_FILE.exists():
            return
        try:
            with OUTPUT_FILE.open("r", encoding="utf-8") as f:
                old = json.load(f)
            if old.get("dataset_id") != self.dataset_id:
                return
            old_id = safe_int(old.get("last_trade_id"))
            if old_id is not None and old_id > 0:
                self.last_trade_id = old_id
                self.restored_cursor = True
            if old.get("trade_gap") is True:
                self.trade_gap = True
                continuity = old.get("continuity") or {}
                self.gap_expected_id = safe_int(continuity.get("gap_expected_id"))
                self.gap_first_returned_id = safe_int(continuity.get("gap_first_returned_id"))
        except Exception:
            return

    def fetch_orderbook(self):
        data = client.futures_order_book(symbol=SYMBOL, limit=ORDERBOOK_DEPTH_LIMIT)
        bids, asks = {}, {}
        for row in data.get("bids", []):
            if len(row) >= 2:
                p, q = safe_float(row[0]), safe_float(row[1])
                if p > 0 and q > 0:
                    bids[p] = q
        for row in data.get("asks", []):
            if len(row) >= 2:
                p, q = safe_float(row[0]), safe_float(row[1])
                if p > 0 and q > 0:
                    asks[p] = q
        self.previous_bids = self.bids
        self.previous_asks = self.asks
        self.bids = bids
        self.asks = asks
        self.last_orderbook_time = utc_now()
        self.last_orderbook_fetch = monotonic_seconds()
        self.last_orderbook_message_time_ms = safe_int(data.get("E"))
        self.last_orderbook_exchange_time_ms = safe_int(data.get("T"))
        self.last_orderbook_update_id = safe_int(data.get("lastUpdateId"))
        self.orderbook_error = None

    def fetch_recent_trades(self):
        """Fetch fromId-contiguous aggTrades and catch up after short stalls.

        The cursor is ID-based, so Windows clock drift cannot create a trade ID
        gap.  Up to MAX_CATCHUP_PAGES_PER_POLL pages are drained immediately to
        recover faster after CPU/network pauses.
        """
        expected = self.last_trade_id + 1 if self.last_trade_id is not None else None
        pages = 0
        any_data = False

        while pages < MAX_CATCHUP_PAGES_PER_POLL:
            params = {"symbol": SYMBOL, "limit": TRADE_REQUEST_LIMIT}
            if expected is not None:
                params["fromId"] = expected

            data = client.futures_aggregate_trades(**params)
            pages += 1
            self.last_trade_fetch = monotonic_seconds()
            self.last_trade_time = utc_now()
            self.trade_error = None

            if not data:
                break
            any_data = True

            if expected is not None:
                first_id = safe_int(data[0].get("a"), 0) or 0
                if first_id > expected:
                    self.trade_gap = True
                    if self.gap_expected_id is None:
                        self.gap_expected_id = expected
                        self.gap_first_returned_id = first_id

            for row in data:
                trade_id = safe_int(row.get("a"), 0) or 0
                if trade_id <= 0 or trade_id in self.seen_trade_ids:
                    continue
                self.seen_trade_ids.add(trade_id)
                price = safe_float(row.get("p"))
                quantity = safe_float(row.get("q"))
                normal_quantity = safe_float(row.get("nq", quantity))
                if price <= 0 or quantity <= 0:
                    continue
                raw_ts = safe_int(row.get("T"))
                trade_time = (raw_ts / 1000.0) if raw_ts else (exchange_now_ms() / 1000.0)
                side = "AGGRESSIVE_SELL" if bool(row.get("m")) else "AGGRESSIVE_BUY"
                self.trades.append({
                    "trade_id": trade_id,
                    "time": trade_time,
                    "exchange_time": raw_ts,
                    "price": price,
                    "quantity": quantity,
                    "normal_quantity": normal_quantity,
                    "quote_value": price * quantity,
                    "side": side,
                })
                self.last_price = price
                self.last_trade_exchange_time_ms = raw_ts or self.last_trade_exchange_time_ms

            self.last_trade_id = safe_int(data[-1].get("a"), self.last_trade_id)
            expected = self.last_trade_id + 1 if self.last_trade_id is not None else None

            # A short page means we reached the current end of the REST result.
            if len(data) < TRADE_REQUEST_LIMIT:
                break

        self.remove_old_trades()
        if len(self.seen_trade_ids) > 10000:
            self.seen_trade_ids = {
                item["trade_id"] for item in self.trades if item.get("trade_id") is not None
            }
        return {"pages": pages, "any_data": any_data}

    def remove_old_trades(self):
        cutoff = exchange_now_ms() / 1000.0 - TRADE_WINDOW_SECONDS
        while self.trades and self.trades[0]["time"] < cutoff:
            self.trades.popleft()

    def top_levels(self, side, count):
        source = self.bids if side == "bids" else self.asks
        return sorted(source.items(), key=lambda item: item[0], reverse=(side == "bids"))[:count]

    def calculate_depth(self, count):
        bid_levels = self.top_levels("bids", count)
        ask_levels = self.top_levels("asks", count)
        bid_value = sum(p * q for p, q in bid_levels)
        ask_value = sum(p * q for p, q in ask_levels)
        total = bid_value + ask_value
        return {
            "levels_used": count,
            "bid_usdt": round(bid_value, 2),
            "ask_usdt": round(ask_value, 2),
            "imbalance": round((bid_value - ask_value) / total, 6) if total > 0 else 0.0,
        }

    def find_walls(self):
        bid_walls, ask_walls = [], []
        for p, q in self.top_levels("bids", ORDERBOOK_DEPTH_LIMIT):
            value = p * q
            if value >= WALL_USD_THRESHOLD:
                bid_walls.append({"price": p, "quantity": q, "usdt": round(value, 2)})
        for p, q in self.top_levels("asks", ORDERBOOK_DEPTH_LIMIT):
            value = p * q
            if value >= WALL_USD_THRESHOLD:
                ask_walls.append({"price": p, "quantity": q, "usdt": round(value, 2)})
        return {"bid_walls": bid_walls, "ask_walls": ask_walls, "wall_threshold_usdt": WALL_USD_THRESHOLD}

    def calculate_book_reductions(self):
        bid_reduction = sum(p * max(old_q - self.bids.get(p, 0.0), 0.0) for p, old_q in self.previous_bids.items())
        ask_reduction = sum(p * max(old_q - self.asks.get(p, 0.0), 0.0) for p, old_q in self.previous_asks.items())
        return {
            "book_bid_reduction_usdt": round(bid_reduction, 2),
            "book_ask_reduction_usdt": round(ask_reduction, 2),
            "interpretation": "盘口数量减少，不能区分成交和撤单",
        }

    def calculate_trade_flow(self):
        self.remove_old_trades()
        buy_value = sum(x["quote_value"] for x in self.trades if x["side"] == "AGGRESSIVE_BUY")
        sell_value = sum(x["quote_value"] for x in self.trades if x["side"] == "AGGRESSIVE_SELL")
        total = buy_value + sell_value
        return {
            "window_seconds": TRADE_WINDOW_SECONDS,
            "aggressive_buy_usdt": round(buy_value, 2),
            "aggressive_sell_usdt": round(sell_value, 2),
            "total_usdt": round(total, 2),
            "taker_imbalance": round((buy_value - sell_value) / total, 6) if total > 0 else 0.0,
            "trade_count": len(self.trades),
            "first_trade_id": self.trades[0]["trade_id"] if self.trades else None,
            "last_trade_id": self.trades[-1]["trade_id"] if self.trades else None,
            "first_exchange_time_ms": self.trades[0]["exchange_time"] if self.trades else None,
            "last_exchange_time_ms": self.trades[-1]["exchange_time"] if self.trades else None,
        }

    def build_snapshot(self):
        self.remove_old_trades()
        depth_100 = self.calculate_depth(100)
        depth_1000 = self.calculate_depth(ORDERBOOK_DEPTH_LIMIT)
        now_mono = monotonic_seconds()
        now_ms = exchange_now_ms()
        clock_status = get_clock_status(sync_if_needed=False)
        orderbook_fetch_age = now_mono - self.last_orderbook_fetch if self.last_orderbook_fetch > 0 else None
        trades_fetch_age = now_mono - self.last_trade_fetch if self.last_trade_fetch > 0 else None
        orderbook_event_age = (
            max(0.0, (now_ms - self.last_orderbook_exchange_time_ms) / 1000.0)
            if self.last_orderbook_exchange_time_ms else None
        )
        trades_event_age = (
            max(0.0, (now_ms - self.last_trade_exchange_time_ms) / 1000.0)
            if self.last_trade_exchange_time_ms else None
        )
        ready = (
            bool(self.bids)
            and bool(self.asks)
            and self.last_orderbook_time is not None
            and self.last_trade_time is not None
            and orderbook_fetch_age is not None and orderbook_fetch_age <= MAX_ORDERBOOK_AGE
            and trades_fetch_age is not None and trades_fetch_age <= MAX_TRADES_AGE
            and orderbook_event_age is not None and orderbook_event_age <= MAX_ORDERBOOK_AGE
            and trades_event_age is not None and trades_event_age <= MAX_TRADES_AGE
            and self.orderbook_error is None
            and self.trade_error is None
            and not self.trade_gap
            and clock_status.get("healthy") is True
        )
        return {
            "version": "ORDERFLOW_REST_V2.2",
            "timestamp": utc_now(),
            "dataset_id": self.dataset_id,
            "symbol": SYMBOL,
            "transport": "REST_POLLING",
            "data_quality": "APPROXIMATE",
            "ready": ready,
            "trade_gap": self.trade_gap,
            "last_trade_id": self.last_trade_id,
            "last_trade_price": self.last_price,
            "orderbook": {
                "last_update_id": self.last_orderbook_update_id,
                "message_output_time_ms": self.last_orderbook_message_time_ms,
                "exchange_transaction_time_ms": self.last_orderbook_exchange_time_ms,
                "bid_levels": len(self.bids),
                "ask_levels": len(self.asks),
                "depth_100": depth_100,
                "depth_1000": depth_1000,
                "walls": self.find_walls(),
                "reductions": self.calculate_book_reductions(),
            },
            "trade_flow": self.calculate_trade_flow(),
            "continuity": {
                "restored_cursor": self.restored_cursor,
                "gap_sticky": self.trade_gap,
                "gap_expected_id": self.gap_expected_id,
                "gap_first_returned_id": self.gap_first_returned_id,
            },
            "freshness": {
                "orderbook_fetch_age_seconds": round(orderbook_fetch_age, 3) if orderbook_fetch_age is not None else None,
                "trades_fetch_age_seconds": round(trades_fetch_age, 3) if trades_fetch_age is not None else None,
                "orderbook_event_age_seconds": round(orderbook_event_age, 3) if orderbook_event_age is not None else None,
                "trades_event_age_seconds": round(trades_event_age, 3) if trades_event_age is not None else None,
                "max_orderbook_age": MAX_ORDERBOOK_AGE,
                "max_trades_age": MAX_TRADES_AGE,
            },
            "clock": clock_status,
            "source_times": {
                "orderbook_exchange_time_ms": self.last_orderbook_exchange_time_ms,
                "orderbook_message_time_ms": self.last_orderbook_message_time_ms,
                "last_trade_exchange_time_ms": self.last_trade_exchange_time_ms,
            },
            "errors": {"orderbook": self.orderbook_error, "trades": self.trade_error},
            "analysis_status": "READY" if ready else ("GAP_DETECTED" if self.trade_gap else "WARMING_UP"),
        }

    def save_snapshot(self, snapshot):
        ensure_directories()
        temp = OUTPUT_FILE.with_suffix(OUTPUT_FILE.suffix + ".tmp")
        with temp.open("w", encoding="utf-8") as f:
            json.dump(snapshot, f, ensure_ascii=False, indent=2)
        temp.replace(OUTPUT_FILE)


def run():
    collector = RestOrderflowCollector()
    print("=" * 72)
    print("BTC AI HUNTER V1.2.2 | REST ORDERFLOW V2.2")
    print("=" * 72)
    print("DATASET_ID          :", collector.dataset_id)
    print("TRANSPORT           : REST_POLLING")
    print("TRADE LIMIT         : 1000")
    print("FROM_ID CONTINUITY  : ENABLED + RESTART RESTORE")
    print("GAP STATUS          : STICKY WITHIN DATASET")
    print("CLOCK               : BINANCE_FAPI_SERVER_TIME_MONOTONIC_V1")
    print("REAL_ORDER_SEND     : False")
    print("=" * 72)
    last_output = 0.0
    while True:
        try:
            now = monotonic_seconds()
            if now - collector.last_orderbook_fetch >= ORDERBOOK_REFRESH_SECONDS:
                try:
                    collector.fetch_orderbook()
                except Exception as e:
                    collector.orderbook_error = type(e).__name__ + ": " + str(e)
                    collector.last_orderbook_fetch = monotonic_seconds()
            if now - collector.last_trade_fetch >= TRADES_REFRESH_SECONDS:
                try:
                    collector.fetch_recent_trades()
                except Exception as e:
                    collector.trade_error = type(e).__name__ + ": " + str(e)
                    collector.last_trade_fetch = monotonic_seconds()
            if now - last_output >= OUTPUT_REFRESH_SECONDS:
                snapshot = collector.build_snapshot()
                collector.save_snapshot(snapshot)
                flow = snapshot["trade_flow"]
                print(
                    f"[{snapshot['timestamp']}] status={snapshot['analysis_status']} "
                    f"gap={snapshot['trade_gap']} last_id={snapshot['last_trade_id']} "
                    f"price={snapshot['last_trade_price']} buy={flow['aggressive_buy_usdt']:.0f} "
                    f"sell={flow['aggressive_sell_usdt']:.0f} taker={flow['taker_imbalance']:.4f}"
                )
                last_output = now
            time.sleep(0.2)
        except KeyboardInterrupt:
            print("Orderflow stopped.")
            break
        except Exception as e:
            print("Orderflow error:", type(e).__name__, str(e))
            time.sleep(3)


if __name__ == "__main__":
    run()


