# -*- coding: utf-8 -*-

"""BTC HUNTER USD-M Futures market layer - recent-trade canonical price + exchange-clock synchronized, read only.

Adds source/event timestamps and funding-settlement metadata while preserving
all existing V1.2 market features. No direction or order logic lives here.
"""

import json
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from EvidenceHunter_config import (
    RUNTIME_DIR,
    SYMBOL,
    CONTEXT_TIMEFRAMES,
    EXECUTION_TIMEFRAMES,
    STRATEGIC_TIMEFRAME,
    SOURCE_TIME_SKEW_MAX_MS,
    PRICE_EVENT_MAX_AGE_MS,
    FUNDING_EVENT_MAX_AGE_MS,
    OPEN_INTEREST_EVENT_MAX_AGE_MS,
    ensure_directories,
)
from EvidenceHunter_archive import (
    ArchiveDataUnavailable,
    get_archive_agg_trades_between,
    get_archive_klines_range,
)
from EvidenceHunter_clock import (
    exchange_now_ms,
    exchange_utc_now,
    get_clock_status,
    local_wall_ms,
    monotonic_ms,
)

BASE_URL = "https://fapi.binance.com"
KLINE_LIMIT = 200
REQUEST_TIMEOUT = 10
FUNDING_INTERVAL_CACHE_SECONDS = 600
MARKET_FILE = RUNTIME_DIR / "market_snapshot.json"
ARCHIVE_CACHE_ROOT = RUNTIME_DIR / "binance_public_data_cache" / "futures_um"
AGGTRADES_ARCHIVE_CACHE_DIR = ARCHIVE_CACHE_ROOT / "aggTrades" / SYMBOL
KLINES_ARCHIVE_CACHE_DIR = ARCHIVE_CACHE_ROOT / "klines" / SYMBOL
LOCAL_REQUEST_WEIGHT_BUDGET_PER_MINUTE = 1000

_funding_interval_cache = {"value": None, "updated": 0.0}
_request_weight_history = deque()


def utc_now():
    return exchange_utc_now()


def epoch_ms():
    # All market/event logic uses Binance-adjusted time, not Windows wall time.
    return exchange_now_ms()


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


class BinanceHTTPError(RuntimeError):
    def __init__(self, status, url, body, retry_after=None):
        self.status = int(status)
        self.url = str(url)
        self.body = str(body)
        self.retry_after = retry_after
        super().__init__(
            f"BINANCE_HTTP_ERROR status={self.status} url={self.url} "
            f"retry_after={self.retry_after} body={self.body[:500]}"
        )


class AggTradeDataUnavailable(RuntimeError):
    pass


def _request_weight(path, params):
    params = params or {}
    if path == "/fapi/v1/aggTrades":
        return 20
    if path == "/fapi/v1/klines":
        limit = int(params.get("limit", 500))
        if limit < 100:
            return 1
        if limit < 500:
            return 2
        if limit <= 1000:
            return 5
        return 10
    return 1


def _reserve_request_weight(weight):
    weight = max(1, int(weight))
    while True:
        now = time.monotonic()
        cutoff = now - 60.0
        while _request_weight_history and _request_weight_history[0][0] <= cutoff:
            _request_weight_history.popleft()
        used = sum(w for _, w in _request_weight_history)
        if used + weight <= LOCAL_REQUEST_WEIGHT_BUDGET_PER_MINUTE:
            _request_weight_history.append((now, weight))
            return
        sleep_for = max(0.25, 60.0 - (now - _request_weight_history[0][0]) + 0.05)
        print(
            f"[RATE_LIMIT] local budget wait {sleep_for:.2f}s "
            f"(used={used}, next_weight={weight})"
        )
        time.sleep(sleep_for)


def request_json(path, params=None):
    params = params or {}
    query = urlencode(params)
    url = BASE_URL + path
    if query:
        url += "?" + query

    weight = _request_weight(path, params)

    for attempt in range(1, 6):
        _reserve_request_weight(weight)
        request = Request(
            url=url,
            headers={"User-Agent": "BTC-AI-Hunter-V1.2.3", "Accept": "application/json"},
            method="GET",
        )
        try:
            with urlopen(request, timeout=REQUEST_TIMEOUT) as response:
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as error:
            try:
                body = error.read().decode("utf-8", errors="replace")
            except Exception:
                body = ""
            retry_after = None
            try:
                if error.headers:
                    value = error.headers.get("Retry-After")
                    retry_after = float(value) if value is not None else None
            except Exception:
                retry_after = None

            if error.code == 429 and attempt < 5:
                wait = max(5.0, retry_after or 0.0)
                print(
                    f"[BINANCE_429] backing off {wait:.1f}s "
                    f"attempt={attempt}/5 url={url}"
                )
                time.sleep(wait)
                continue

            raise BinanceHTTPError(error.code, url, body, retry_after=retry_after) from error
        except URLError as error:
            if attempt < 5:
                time.sleep(min(30.0, 2.0 * attempt))
                continue
            raise RuntimeError(
                f"BINANCE_NETWORK_ERROR url={url} reason={error.reason}"
            ) from error

    raise RuntimeError("REQUEST_RETRY_EXHAUSTED")


def normalize_kline(row):
    return {
        "open_time": int(row[0]),
        "open": safe_float(row[1]),
        "high": safe_float(row[2]),
        "low": safe_float(row[3]),
        "close": safe_float(row[4]),
        "volume": safe_float(row[5]),
        "close_time": int(row[6]),
        "quote_volume": safe_float(row[7]) if len(row) > 7 else 0.0,
        "trades": int(row[8]) if len(row) > 8 else 0,
        "taker_buy_volume": safe_float(row[9]) if len(row) > 9 else 0.0,
        "taker_buy_quote": safe_float(row[10]) if len(row) > 10 else 0.0,
    }


def get_closed_klines(interval):
    rows = request_json(
        "/fapi/v1/klines",
        {"symbol": SYMBOL, "interval": interval, "limit": KLINE_LIMIT},
    )
    now_ms = epoch_ms()
    candles = []
    for row in rows:
        candle = normalize_kline(row)
        if candle["close_time"] <= now_ms:
            candles.append(candle)
    return candles


def get_klines_range(interval, start_ms, end_ms, limit=1500):
    """Completed UTC days use official Archive; current UTC day uses REST."""
    start_ms = int(start_ms)
    end_ms = int(end_ms)
    if end_ms < start_ms:
        return []

    today = datetime.fromtimestamp(epoch_ms() / 1000.0, tz=timezone.utc).date()
    start_day = datetime.fromtimestamp(start_ms / 1000.0, tz=timezone.utc).date()
    end_day = datetime.fromtimestamp(end_ms / 1000.0, tz=timezone.utc).date()

    result = []
    day = start_day
    while day <= end_day:
        day_start_ms = int(
            datetime(day.year, day.month, day.day, tzinfo=timezone.utc).timestamp() * 1000
        )
        day_end_ms = day_start_ms + 86_400_000 - 1
        seg_start = max(start_ms, day_start_ms)
        seg_end = min(end_ms, day_end_ms)

        if day < today:
            try:
                result.extend(
                    get_archive_klines_range(
                        symbol=SYMBOL,
                        interval=interval,
                        start_ms=seg_start,
                        end_ms=seg_end,
                        cache_dir=KLINES_ARCHIVE_CACHE_DIR / interval,
                    )
                )
            except ArchiveDataUnavailable:
                rows = request_json(
                    "/fapi/v1/klines",
                    {
                        "symbol": SYMBOL,
                        "interval": interval,
                        "startTime": seg_start,
                        "endTime": seg_end,
                        "limit": min(int(limit), 1000),
                    },
                )
                result.extend(normalize_kline(row) for row in rows)
        else:
            rows = request_json(
                "/fapi/v1/klines",
                {
                    "symbol": SYMBOL,
                    "interval": interval,
                    "startTime": seg_start,
                    "endTime": seg_end,
                    "limit": min(int(limit), 1000),
                },
            )
            result.extend(normalize_kline(row) for row in rows)

        day += timedelta(days=1)

    unique = {x["open_time"]: x for x in result}
    return sorted(unique.values(), key=lambda x: x["open_time"])


def _get_agg_trades_between_rest(start_ms, end_ms, max_pages=20):
    """Original REST implementation for a range Binance still allows."""
    start_ms = int(start_ms)
    end_ms = int(end_ms)
    if end_ms < start_ms:
        return []

    page = request_json(
        "/fapi/v1/aggTrades",
        {
            "symbol": SYMBOL,
            "startTime": start_ms,
            "endTime": end_ms,
            "limit": 1000,
        },
    )
    result = []
    pages = 0
    last_id = None

    while True:
        pages += 1
        for row in page or []:
            trade_time = safe_int(row.get("T"), 0) or 0
            trade_id = safe_int(row.get("a"), 0) or 0
            if start_ms <= trade_time <= end_ms:
                result.append({
                    "trade_id": trade_id,
                    "time": trade_time,
                    "price": safe_float(row.get("p")),
                    "quantity": safe_float(row.get("q")),
                    "normal_quantity": safe_float(row.get("nq", row.get("q"))),
                    "buyer_is_maker": bool(row.get("m")),
                    "source": "BINANCE_REST_AGGTRADES",
                })
            last_id = trade_id if trade_id else last_id

        if not page or len(page) < 1000 or pages >= max_pages or last_id is None:
            break

        next_page = request_json(
            "/fapi/v1/aggTrades",
            {"symbol": SYMBOL, "fromId": int(last_id) + 1, "limit": 1000},
        )
        if not next_page:
            break
        first_time = safe_int(next_page[0].get("T"), 0) or 0
        if first_time > end_ms:
            break
        page = next_page

    return result


def get_agg_trades_between(start_ms, end_ms, max_pages=20):
    """Completed UTC days use official Archive; current UTC day uses REST."""
    start_ms = int(start_ms)
    end_ms = int(end_ms)
    if end_ms < start_ms:
        return []

    today = datetime.fromtimestamp(epoch_ms() / 1000.0, tz=timezone.utc).date()
    start_day = datetime.fromtimestamp(start_ms / 1000.0, tz=timezone.utc).date()
    end_day = datetime.fromtimestamp(end_ms / 1000.0, tz=timezone.utc).date()

    result = []
    day = start_day
    while day <= end_day:
        day_start_ms = int(
            datetime(day.year, day.month, day.day, tzinfo=timezone.utc).timestamp() * 1000
        )
        day_end_ms = day_start_ms + 86_400_000 - 1
        seg_start = max(start_ms, day_start_ms)
        seg_end = min(end_ms, day_end_ms)

        if day < today:
            try:
                result.extend(
                    get_archive_agg_trades_between(
                        symbol=SYMBOL,
                        start_ms=seg_start,
                        end_ms=seg_end,
                        cache_dir=AGGTRADES_ARCHIVE_CACHE_DIR,
                        require_checksum=True,
                    )
                )
            except ArchiveDataUnavailable as error:
                raise AggTradeDataUnavailable(
                    f"ARCHIVE_AGGTRADES_UNAVAILABLE start={seg_start} "
                    f"end={seg_end} detail={error}"
                ) from error
        else:
            try:
                result.extend(
                    _get_agg_trades_between_rest(seg_start, seg_end, max_pages=max_pages)
                )
            except BinanceHTTPError as error:
                raise AggTradeDataUnavailable(
                    f"CURRENT_DAY_REST_AGGTRADES_UNAVAILABLE start={seg_start} "
                    f"end={seg_end} detail={error}"
                ) from error

        day += timedelta(days=1)

    unique = {x["trade_id"]: x for x in result}
    return sorted(unique.values(), key=lambda x: (x["time"], x["trade_id"]))


def calculate_rsi(candles, period=14):
    if len(candles) < period + 1:
        return None
    changes = [candles[i]["close"] - candles[i - 1]["close"] for i in range(1, len(candles))]
    recent = changes[-period:]
    gains = [value for value in recent if value > 0]
    losses = [abs(value) for value in recent if value < 0]
    average_gain = sum(gains) / period
    average_loss = sum(losses) / period
    if average_loss == 0:
        return 50.0 if average_gain == 0 else 100.0
    rs = average_gain / average_loss
    return round(100 - 100 / (1 + rs), 2)


def calculate_atr(candles, period=14):
    if len(candles) < period + 1:
        return None
    ranges = []
    for i in range(1, len(candles)):
        current, previous = candles[i], candles[i - 1]
        ranges.append(max(
            current["high"] - current["low"],
            abs(current["high"] - previous["close"]),
            abs(current["low"] - previous["close"]),
        ))
    return round(sum(ranges[-period:]) / period, 2)


def calculate_volume_ratio(candles, period=20):
    if len(candles) < period + 1:
        return None
    previous = [c["volume"] for c in candles[-period - 1:-1]]
    avg = sum(previous) / len(previous) if previous else 0.0
    return round(candles[-1]["volume"] / avg, 4) if avg > 0 else None


def calculate_taker_buy_ratio(candles):
    if not candles or candles[-1]["volume"] <= 0:
        return None
    return round(candles[-1]["taker_buy_volume"] / candles[-1]["volume"], 4)


def calculate_structure(candles, lookback=20):
    if len(candles) < lookback:
        return {"support": None, "resistance": None}
    recent = candles[-lookback:]
    return {
        "support": round(min(c["low"] for c in recent), 2),
        "resistance": round(max(c["high"] for c in recent), 2),
    }


def get_ticker_price_reference():
    """Auxiliary ticker reference only; never the canonical Shadow entry source."""
    request_started = epoch_ms()
    local_started = local_wall_ms()
    mono_started = monotonic_ms()
    data = request_json("/fapi/v1/ticker/price", {"symbol": SYMBOL})
    received = epoch_ms()
    local_received = local_wall_ms()
    latency_ms = max(0, monotonic_ms() - mono_started)
    event_time = safe_int(data.get("time"))
    return {
        "source": "SYMBOL_PRICE_TICKER_REFERENCE",
        "price": safe_float(data.get("price")),
        "exchange_event_time_ms": event_time,
        "collector_request_started_time_ms": request_started,
        "collector_received_time_ms": received,
        "collector_local_request_started_time_ms": local_started,
        "collector_local_received_time_ms": local_received,
        "event_age_at_receive_ms": max(0, received - event_time) if event_time else None,
        "request_latency_ms": latency_ms,
    }


def get_price_snapshot():
    """Canonical Shadow entry price from the newest real market trade.

    The observed Entry price and its event timestamp must describe the same
    exchange event. Recent Trades supplies both directly.
    """
    request_started = epoch_ms()
    local_started = local_wall_ms()
    mono_started = monotonic_ms()
    rows = request_json("/fapi/v1/trades", {"symbol": SYMBOL, "limit": 10})
    received = epoch_ms()
    local_received = local_wall_ms()
    latency_ms = max(0, monotonic_ms() - mono_started)

    valid = []
    for row in rows or []:
        event_time = safe_int(row.get("time"))
        trade_id = safe_int(row.get("id"))
        price = safe_float(row.get("price"))
        qty = safe_float(row.get("qty"))
        if event_time and event_time > 0 and trade_id is not None and price > 0:
            valid.append(
                (
                    event_time,
                    trade_id,
                    price,
                    qty,
                    bool(row.get("isBuyerMaker")),
                    bool(row.get("isRPITrade")),
                )
            )

    if not valid:
        fallback = get_ticker_price_reference()
        fallback.update({
            "source": "TICKER_FALLBACK_NO_RECENT_TRADE",
            "canonical_trade_id": None,
            "quantity": None,
            "is_buyer_maker": None,
            "is_rpi_trade": None,
        })
        return fallback

    event_time, trade_id, price, qty, is_buyer_maker, is_rpi_trade = max(
        valid, key=lambda x: (x[0], x[1])
    )
    return {
        "source": "RECENT_MARKET_TRADE",
        "price": price,
        "canonical_trade_id": trade_id,
        "quantity": qty,
        "is_buyer_maker": is_buyer_maker,
        "is_rpi_trade": is_rpi_trade,
        "exchange_event_time_ms": event_time,
        "collector_request_started_time_ms": request_started,
        "collector_received_time_ms": received,
        "collector_local_request_started_time_ms": local_started,
        "collector_local_received_time_ms": local_received,
        "event_age_at_receive_ms": max(0, received - event_time),
        "request_latency_ms": latency_ms,
    }


def get_market_price():
    return get_price_snapshot()["price"]


def _infer_funding_interval_hours():
    now = time.monotonic()
    cached = _funding_interval_cache.get("value")
    if cached is not None and now - _funding_interval_cache.get("updated", 0.0) < FUNDING_INTERVAL_CACHE_SECONDS:
        return cached

    interval = None
    source = "UNKNOWN"
    try:
        rows = request_json("/fapi/v1/fundingInfo")
        if isinstance(rows, dict):
            rows = [rows]
        for row in rows or []:
            if row.get("symbol") == SYMBOL and row.get("fundingIntervalHours") is not None:
                interval = safe_float(row.get("fundingIntervalHours"), 0.0) or None
                source = "FUNDING_INFO_ADJUSTMENT"
                break
    except Exception:
        pass

    if interval is None:
        try:
            rows = request_json("/fapi/v1/fundingRate", {"symbol": SYMBOL, "limit": 3})
            times = sorted(safe_int(r.get("fundingTime"), 0) or 0 for r in rows or [])
            times = [t for t in times if t > 0]
            if len(times) >= 2:
                deltas = [(b - a) / 3600000.0 for a, b in zip(times, times[1:]) if b > a]
                if deltas:
                    interval = deltas[-1]
                    source = "INFERRED_FROM_FUNDING_HISTORY"
        except Exception:
            pass

    value = {"hours": interval, "source": source}
    _funding_interval_cache["value"] = value
    _funding_interval_cache["updated"] = now
    return value


def get_funding():
    # Funding interval discovery can require additional REST calls on a cold
    # cache.  Do it BEFORE timing premiumIndex so the returned funding snapshot
    # itself stays close to the final snapshot time.
    interval = _infer_funding_interval_hours()
    request_started = epoch_ms()
    local_started = local_wall_ms()
    mono_started = monotonic_ms()
    data = request_json("/fapi/v1/premiumIndex", {"symbol": SYMBOL})
    received = epoch_ms()
    local_received = local_wall_ms()
    latency_ms = max(0, monotonic_ms() - mono_started)
    next_time = safe_int(data.get("nextFundingTime"))
    event_time = safe_int(data.get("time"))
    return {
        "mark_price": safe_float(data.get("markPrice")),
        "index_price": safe_float(data.get("indexPrice")),
        "estimated_settle_price": safe_float(data.get("estimatedSettlePrice")),
        "last_funding_rate": safe_float(data.get("lastFundingRate")),
        "interest_rate": safe_float(data.get("interestRate")),
        "next_funding_time": next_time,
        "funding_interval_hours": interval.get("hours"),
        "funding_interval_minutes": (
            round(float(interval.get("hours")) * 60.0, 6)
            if interval.get("hours") is not None
            else None
        ),
        "funding_interval_source": interval.get("source"),
        "time_to_funding_seconds": round((next_time - received) / 1000.0, 3) if next_time else None,
        "exchange_event_time_ms": event_time,
        "collector_request_started_time_ms": request_started,
        "collector_received_time_ms": received,
        "collector_local_request_started_time_ms": local_started,
        "collector_local_received_time_ms": local_received,
        "event_age_at_receive_ms": max(0, received - event_time) if event_time else None,
        "request_latency_ms": latency_ms,
    }


def get_open_interest_snapshot():
    request_started = epoch_ms()
    local_started = local_wall_ms()
    mono_started = monotonic_ms()
    data = request_json("/fapi/v1/openInterest", {"symbol": SYMBOL})
    received = epoch_ms()
    local_received = local_wall_ms()
    latency_ms = max(0, monotonic_ms() - mono_started)
    event_time = safe_int(data.get("time"))
    return {
        "open_interest": safe_float(data.get("openInterest")),
        "exchange_event_time_ms": event_time,
        "collector_request_started_time_ms": request_started,
        "collector_received_time_ms": received,
        "collector_local_request_started_time_ms": local_started,
        "collector_local_received_time_ms": local_received,
        "event_age_at_receive_ms": max(0, received - event_time) if event_time else None,
        "request_latency_ms": latency_ms,
    }


def get_open_interest():
    return get_open_interest_snapshot()["open_interest"]


def _source_skew_ms(event_times):
    """Legacy cross-source event-time spread; informational only."""
    values = [int(v) for v in event_times if isinstance(v, int) and v > 0]
    if len(values) < 2:
        return None
    return max(values) - min(values)


def _event_age_ms(snapshot_now_ms, event_time_ms):
    if not isinstance(event_time_ms, int) or event_time_ms <= 0:
        return None
    return max(0, int(snapshot_now_ms) - int(event_time_ms))


def build_market_snapshot():
    ensure_directories()
    build_started_mono = monotonic_ms()
    get_clock_status(sync_if_needed=True)

    # Slow context/history requests are collected first.  Fresh point-in-time
    # sources are intentionally collected LAST so the Shadow entry price is not
    # several seconds old merely because RSI/ATR klines took time to download.
    timeframes = [STRATEGIC_TIMEFRAME, *CONTEXT_TIMEFRAMES, *EXECUTION_TIMEFRAMES]
    periods = {}
    for interval in list(dict.fromkeys(timeframes)):
        candles = get_closed_klines(interval)
        periods[interval] = {
            "candle_count": len(candles),
            "rsi14": calculate_rsi(candles),
            "atr14": calculate_atr(candles),
            "volume_ratio": calculate_volume_ratio(candles),
            "taker_buy_ratio": calculate_taker_buy_ratio(candles),
            "structure": calculate_structure(candles),
            "last_closed_time": candles[-1]["close_time"] if candles else None,
            "last_closed_price": candles[-1]["close"] if candles else None,
        }

    # Warm interval metadata before the fresh premiumIndex request.
    _infer_funding_interval_hours()
    price_detail = get_price_snapshot()
    funding = get_funding()
    oi_detail = get_open_interest_snapshot()
    try:
        ticker_reference = get_ticker_price_reference()
    except Exception:
        ticker_reference = None
    price = price_detail["price"]
    open_interest = oi_detail["open_interest"]

    event_times = [
        price_detail.get("exchange_event_time_ms"),
        funding.get("exchange_event_time_ms"),
        oi_detail.get("exchange_event_time_ms"),
    ]
    event_spread = _source_skew_ms(event_times)
    snapshot_created_ms = epoch_ms()
    clock_status = get_clock_status(sync_if_needed=False)

    price_age = _event_age_ms(snapshot_created_ms, price_detail.get("exchange_event_time_ms"))
    funding_age = _event_age_ms(snapshot_created_ms, funding.get("exchange_event_time_ms"))
    oi_age = _event_age_ms(snapshot_created_ms, oi_detail.get("exchange_event_time_ms"))

    price_fresh = price_age is not None and price_age <= PRICE_EVENT_MAX_AGE_MS
    funding_fresh = funding_age is not None and funding_age <= FUNDING_EVENT_MAX_AGE_MS
    oi_fresh = oi_age is not None and oi_age <= OPEN_INTEREST_EVENT_MAX_AGE_MS
    source_freshness_valid = (
        price_fresh
        and funding_fresh
        and oi_fresh
        and price_detail.get("source") == "RECENT_MARKET_TRADE"
    )

    # Keep source_time_skew_ms for schema compatibility, but its meaning remains
    # cross-endpoint event spread and it is no longer a hard gate.  Different
    # Binance endpoints can update at different moments.  The hard gate is the
    # exchange-synchronized clock plus source-specific freshness.
    snapshot = {
        "version": "MARKET_V1.2.3",
        "timestamp": utc_now(),
        "snapshot_created_time_ms": snapshot_created_ms,
        "snapshot_local_wall_time_ms": local_wall_ms(),
        "snapshot_build_ms": max(0, monotonic_ms() - build_started_mono),
        "symbol": SYMBOL,
        "market_type": "USD_M_FUTURES",
        "price": price,
        "price_detail": price_detail,
        "ticker_price_reference": ticker_reference,
        "funding": funding,
        "open_interest": open_interest,
        "open_interest_detail": oi_detail,
        "periods": periods,
        "clock": clock_status,
        "time_consistency": {
            "exchange_event_time_ms": max([x for x in event_times if isinstance(x, int)], default=None),
            "collector_received_time_ms": snapshot_created_ms,
            "snapshot_created_time_ms": snapshot_created_ms,
            "source_time_skew_ms": event_spread,
            "source_event_spread_ms": event_spread,
            "source_time_skew_limit_ms": SOURCE_TIME_SKEW_MAX_MS,
            "source_time_skew_gate": "INFORMATIONAL_ONLY",
            "price_source": price_detail.get("source"),
            "price_trade_id": price_detail.get("canonical_trade_id"),
            "price_event_age_ms": price_age,
            "funding_event_age_ms": funding_age,
            "open_interest_event_age_ms": oi_age,
            "price_event_max_age_ms": PRICE_EVENT_MAX_AGE_MS,
            "funding_event_max_age_ms": FUNDING_EVENT_MAX_AGE_MS,
            "open_interest_event_max_age_ms": OPEN_INTEREST_EVENT_MAX_AGE_MS,
            "clock_offset_ms": clock_status.get("offset_ms"),
            "local_clock_ahead_ms": clock_status.get("local_clock_ahead_ms"),
            "clock_rtt_ms": clock_status.get("rtt_ms"),
            "clock_sync_age_seconds": clock_status.get("sync_age_seconds"),
        },
        "data_quality": {
            "price_valid": price > 0,
            "price_source_valid": price_detail.get("source") == "RECENT_MARKET_TRADE",
            "funding_valid": funding["mark_price"] > 0,
            "oi_valid": open_interest > 0,
            "periods_loaded": len(periods),
            "clock_sync_valid": clock_status.get("healthy") is True,
            "source_freshness_valid": source_freshness_valid,
            "price_fresh": price_fresh,
            "funding_fresh": funding_fresh,
            "open_interest_fresh": oi_fresh,
            # Retained for old readers; no longer means cross-source spread <=5s.
            "source_time_skew_valid": source_freshness_valid and clock_status.get("healthy") is True,
        },
    }

    temp_file = MARKET_FILE.with_suffix(MARKET_FILE.suffix + ".tmp")
    with temp_file.open("w", encoding="utf-8") as f:
        json.dump(snapshot, f, ensure_ascii=False, indent=2)
    temp_file.replace(MARKET_FILE)
    return snapshot


def print_snapshot(snapshot):
    print("=" * 72)
    print("BTC AI HUNTER V1.2.3 | MARKET SNAPSHOT")
    print("=" * 72)
    print("PRICE               :", snapshot["price"])
    print("PRICE SOURCE        :", snapshot["price_detail"].get("source"))
    print("PRICE TRADE ID      :", snapshot["price_detail"].get("canonical_trade_id"))
    print("MARK PRICE          :", snapshot["funding"]["mark_price"])
    print("FUNDING RATE        :", snapshot["funding"]["last_funding_rate"])
    print("NEXT FUNDING        :", snapshot["funding"]["next_funding_time"])
    print("FUNDING INTERVAL H  :", snapshot["funding"]["funding_interval_hours"])
    print("FUNDING INTERVAL M  :", snapshot["funding"]["funding_interval_minutes"])
    print("OPEN INTEREST       :", snapshot["open_interest"])
    print("SOURCE EVENT SPREAD :", snapshot["time_consistency"]["source_event_spread_ms"])
    print("CLOCK OFFSET MS     :", snapshot["time_consistency"]["clock_offset_ms"])
    print("LOCAL CLOCK AHEAD   :", snapshot["time_consistency"]["local_clock_ahead_ms"])
    print("SOURCE FRESHNESS    :", snapshot["data_quality"]["source_freshness_valid"])
    print("FILE                :", MARKET_FILE)
    print("REAL_ORDER_SEND     : False")
    print("=" * 72)


if __name__ == "__main__":
    try:
        print_snapshot(build_market_snapshot())
    except Exception as error:
        print("MARKET_STATUS: ERROR")
        print(type(error).__name__ + ": " + str(error))


