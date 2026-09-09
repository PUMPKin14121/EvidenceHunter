# -*- coding: utf-8 -*-

"""BTC HUNTER exchange-synchronized clock V1.

The Windows wall clock is not trusted for market/event timing.  The module
samples Binance USD-M server time (/fapi/v1/time), estimates local clock offset
with an RTT midpoint, and then advances time from a monotonic clock anchor.

This module is read-only and never submits/cancels/modifies orders.
"""

import json
import threading
import time
from datetime import datetime, timezone
from urllib.request import Request, urlopen

from EvidenceHunter_config import (
    CLOCK_MAX_RTT_MS,
    CLOCK_MAX_STALE_SECONDS,
    CLOCK_RETRY_AFTER_FAILURE_SECONDS,
    CLOCK_SYNC_INTERVAL_SECONDS,
)

TIME_URL = "https://fapi.binance.com/fapi/v1/time"
REQUEST_TIMEOUT_SECONDS = 5

_lock = threading.Lock()
_state = {
    "synced": False,
    "server_time_ms": None,
    "offset_ms": None,
    "rtt_ms": None,
    "anchor_monotonic_ms": None,
    "last_sync_monotonic_ms": None,
    "last_sync_local_wall_ms": None,
    "last_attempt_monotonic_ms": None,
    "last_error": None,
}


def local_wall_ms():
    return time.time_ns() // 1_000_000


def monotonic_ms():
    return time.monotonic_ns() // 1_000_000


def monotonic_seconds():
    return time.monotonic()


def _fetch_server_time():
    request = Request(
        TIME_URL,
        headers={"User-Agent": "BTC-AI-Hunter-V1.2.2", "Accept": "application/json"},
        method="GET",
    )
    with urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
        data = json.loads(response.read().decode("utf-8"))
    server_time = int(data["serverTime"])
    if server_time <= 0:
        raise RuntimeError("invalid Binance serverTime")
    return server_time


def _sync_age_seconds_unlocked(now_mono_ms=None):
    last = _state.get("last_sync_monotonic_ms")
    if last is None:
        return None
    now_mono_ms = monotonic_ms() if now_mono_ms is None else int(now_mono_ms)
    return max(0.0, (now_mono_ms - int(last)) / 1000.0)


def sync_exchange_clock(force=False):
    """Synchronize to Binance USD-M server time.

    A cached anchor is reused for CLOCK_SYNC_INTERVAL_SECONDS.  On refresh,
    server time is aligned to the midpoint of the local request RTT.  Market
    timestamps are then advanced with time.monotonic(), so later Windows clock
    corrections cannot jump the research clock forward/backward.
    """
    now_mono = monotonic_ms()
    with _lock:
        age = _sync_age_seconds_unlocked(now_mono)
        if (
            not force
            and _state.get("synced")
            and age is not None
            and age < CLOCK_SYNC_INTERVAL_SECONDS
        ):
            return get_clock_status(sync_if_needed=False)

        last_attempt = _state.get("last_attempt_monotonic_ms")
        if (
            not force
            and _state.get("synced")
            and last_attempt is not None
            and (now_mono - int(last_attempt)) < CLOCK_RETRY_AFTER_FAILURE_SECONDS * 1000
        ):
            return get_clock_status(sync_if_needed=False)

        _state["last_attempt_monotonic_ms"] = int(now_mono)
        wall_before = local_wall_ms()
        mono_before = monotonic_ms()
        try:
            server_time = _fetch_server_time()
            wall_after = local_wall_ms()
            mono_after = monotonic_ms()
            rtt_ms = max(0, mono_after - mono_before)

            wall_mid = (wall_before + wall_after) / 2.0
            mono_mid = (mono_before + mono_after) / 2.0
            offset_ms = float(server_time) - wall_mid

            _state.update({
                "synced": True,
                "server_time_ms": int(server_time),
                "offset_ms": round(offset_ms, 3),
                "rtt_ms": int(rtt_ms),
                "anchor_monotonic_ms": float(mono_mid),
                "last_sync_monotonic_ms": int(mono_after),
                "last_sync_local_wall_ms": int(wall_after),
                "last_error": None,
            })
        except Exception as exc:
            _state["last_error"] = f"{type(exc).__name__}: {exc}"
            age = _sync_age_seconds_unlocked(monotonic_ms())
            # A previously synchronized monotonic anchor may survive a brief
            # /time outage.  Beyond the maximum stale age we fail closed.
            if not _state.get("synced") or age is None or age > CLOCK_MAX_STALE_SECONDS:
                raise

        return get_clock_status(sync_if_needed=False)


def exchange_now_ms():
    """Return current Binance-adjusted milliseconds from a monotonic anchor."""
    status = get_clock_status(sync_if_needed=True)
    if not status.get("usable"):
        raise RuntimeError(f"EXCHANGE_CLOCK_NOT_USABLE: {status}")
    anchor = _state.get("anchor_monotonic_ms")
    server_time = _state.get("server_time_ms")
    if anchor is None or server_time is None:
        raise RuntimeError("EXCHANGE_CLOCK_NOT_INITIALIZED")
    elapsed = monotonic_ms() - float(anchor)
    return int(round(float(server_time) + elapsed))


def exchange_utc_now():
    return datetime.fromtimestamp(exchange_now_ms() / 1000.0, tz=timezone.utc).isoformat()


def exchange_minute():
    return datetime.fromtimestamp(exchange_now_ms() / 1000.0, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M")


def get_clock_status(force_sync=False, sync_if_needed=True):
    if force_sync:
        sync_exchange_clock(force=True)
    elif sync_if_needed:
        age = _sync_age_seconds_unlocked()
        if (not _state.get("synced")) or age is None or age >= CLOCK_SYNC_INTERVAL_SECONDS:
            sync_exchange_clock(force=False)

    now_mono = monotonic_ms()
    age = _sync_age_seconds_unlocked(now_mono)
    offset = _state.get("offset_ms")
    rtt = _state.get("rtt_ms")
    stale_ok = age is not None and age <= CLOCK_MAX_STALE_SECONDS
    rtt_ok = rtt is not None and rtt <= CLOCK_MAX_RTT_MS
    usable = bool(_state.get("synced") and stale_ok)
    healthy = bool(usable and rtt_ok)
    return {
        "clock_policy": "BINANCE_FAPI_SERVER_TIME_MONOTONIC_V1",
        "synced": bool(_state.get("synced")),
        "usable": usable,
        "healthy": healthy,
        "offset_ms": offset,
        # Positive means the Windows wall clock is ahead of Binance.
        "local_clock_ahead_ms": round(-float(offset), 3) if isinstance(offset, (int, float)) else None,
        "rtt_ms": rtt,
        "sync_age_seconds": round(age, 3) if age is not None else None,
        "max_rtt_ms": CLOCK_MAX_RTT_MS,
        "max_stale_seconds": CLOCK_MAX_STALE_SECONDS,
        "sync_interval_seconds": CLOCK_SYNC_INTERVAL_SECONDS,
        "retry_after_failure_seconds": CLOCK_RETRY_AFTER_FAILURE_SECONDS,
        "last_error": _state.get("last_error"),
    }

