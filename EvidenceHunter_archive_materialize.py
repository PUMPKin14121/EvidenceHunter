# -*- coding: utf-8 -*-
"""
Manual, explicit-date Binance official archive materialization tool.

PURPOSE
-------
EvidenceHunter_archive_prefetch.py only fetches archive days that are still
needed by *unlocked* shadow outcomes. Once every outcome in a dataset has
already been resolved via the live REST backfill path (as is the case for
the closed 168h dataset BTCV12P3_20260828T000323Z_75696678), that selection
logic finds nothing to do (COMPLETED_UTC_DAYS_NEEDED: 0) even on a day whose
archive genuinely has never been downloaded/verified locally.

This tool exists ONLY to let an operator explicitly force materialization
(download + official CHECKSUM verification) of ONE specific completed UTC
day, independent of the outcome-locking selection logic above.

WHAT THIS TOOL DOES NOT DO
---------------------------
- It does NOT modify EvidenceHunter_shadow_records.jsonl (the Frozen dataset).
- It does NOT modify any Discovery Contract file.
- It does NOT modify EvidenceHunter_archive.py, EvidenceHunter_archive_prefetch.py,
  or any other existing source file. It only calls their existing, already
  battle-tested public functions (ensure_archive_day / ensure_kline_archive_day),
  unmodified.
- It does NOT hand-write or fake a .VERIFIED marker. A .VERIFIED file is only
  ever written by _ensure_verified_zip() inside EvidenceHunter_archive.py, and
  only after an actual SHA256 comparison against the official .CHECKSUM
  object succeeds.
- It refuses to run against "today" (current UTC date), because Binance only
  publishes a day's daily archive after that UTC day is fully complete.

WHAT IT WRITES
---------------
Only new files under runtime/binance_public_data_cache/futures_um/ (the same
cache directory EvidenceHunter_archive_prefetch.py already uses): the official
.zip, its official .zip.CHECKSUM sidecar, and (only on verified success) a
.zip.VERIFIED stamp. For --asset agg it additionally builds the local
extracted .csv + .csv.index.json, exactly as archive_prefetch.py's own
"INDEX OK" step does (a purely local indexing convenience, not a third
remote Binance asset -- confirmed by source-code reading).

USAGE
-----
    python EvidenceHunter_archive_materialize.py --date 2026-09-04
    python EvidenceHunter_archive_materialize.py --date 2026-09-04 --asset agg
    python EvidenceHunter_archive_materialize.py --date 2026-09-04 --asset kline

NOTE ON SCOPE
-------------
The Frozen EFFORT_RESULT_DIVERGENCE_V1 Discovery Contract's own price
reconstruction (data_lineage.historical_path_source and
data_lineage.future_target_source) is defined exclusively in terms of the
"Binance public USD-M Futures aggTrades archive". Nothing in the Contract
text references klines archive. This tool still supports materializing
klines too (default --asset both) only for parity with the project's
existing archive_prefetch.py convention of fetching both per day; klines
materialization is NOT required to satisfy this Contract's own eligibility
gate. This distinction does not change any Contract text and is documented
here only as an operator note.
"""

import argparse
import sys
from datetime import date, datetime, timezone

from EvidenceHunter_archive import (
    ArchiveDataUnavailable,
    ensure_archive_day,
    ensure_kline_archive_day,
    get_archive_agg_trades_between,
)
from EvidenceHunter_config import RUNTIME_DIR, SYMBOL

ROOT = RUNTIME_DIR / "binance_public_data_cache" / "futures_um"
AGG_DIR = ROOT / "aggTrades" / SYMBOL
KLINE_DIR = ROOT / "klines" / SYMBOL / "1m"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Force-materialize (download + official CHECKSUM verify) one explicit "
                    "completed UTC day of Binance public archive data, independent of the "
                    "outcome-locking selection logic in EvidenceHunter_archive_prefetch.py."
    )
    parser.add_argument("--date", required=True, help="UTC date to materialize, format YYYY-MM-DD")
    parser.add_argument("--symbol", default=SYMBOL, help=f"Symbol (default: {SYMBOL})")
    parser.add_argument(
        "--asset",
        choices=["agg", "kline", "both"],
        default="both",
        help="Which archive type(s) to materialize (default: both). "
             "Note: the Frozen EFFORT_RESULT_DIVERGENCE_V1 Contract's own eligibility gate "
             "only requires 'agg' (aggTrades archive); 'kline' is fetched only for parity "
             "with the project's existing archive_prefetch.py convention.",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    try:
        day = datetime.strptime(args.date, "%Y-%m-%d").date()
    except ValueError:
        print(f"INVALID_DATE_FORMAT: {args.date!r} (expected YYYY-MM-DD)")
        sys.exit(2)

    today_utc = datetime.now(timezone.utc).date()
    if day >= today_utc:
        print(
            f"REFUSED: {day.isoformat()} is not a completed UTC day yet "
            f"(current UTC date is {today_utc.isoformat()}). Binance only publishes "
            f"a day's daily archive after that UTC day is fully complete."
        )
        sys.exit(1)

    symbol = args.symbol.upper()
    print(f"MATERIALIZE_DAY: {day.isoformat()}  SYMBOL: {symbol}  ASSET: {args.asset}")

    exit_code = 0

    if args.asset in ("agg", "both"):
        try:
            ap = ensure_archive_day(symbol, day, AGG_DIR, require_checksum=True)
            print("  AGG    OK:", ap)
            day_start_ms = int(
                datetime(day.year, day.month, day.day, tzinfo=timezone.utc).timestamp() * 1000
            )
            get_archive_agg_trades_between(
                symbol, day_start_ms, day_start_ms + 1000, AGG_DIR, require_checksum=True
            )
            print("  INDEX  OK  (local extracted CSV + index built; not a remote asset)")
        except ArchiveDataUnavailable as error:
            print("  AGG    FAILED:", error)
            exit_code = 1

    if args.asset in ("kline", "both"):
        try:
            kp = ensure_kline_archive_day(symbol, "1m", day, KLINE_DIR)
            print("  KLINE  OK:", kp)
        except ArchiveDataUnavailable as error:
            print("  KLINE  FAILED:", error)
            exit_code = 1

    if exit_code == 0:
        print("MATERIALIZATION_COMPLETE:", day.isoformat())
    else:
        print("MATERIALIZATION_INCOMPLETE:", day.isoformat())

    sys.exit(exit_code)


if __name__ == "__main__":
    main()


