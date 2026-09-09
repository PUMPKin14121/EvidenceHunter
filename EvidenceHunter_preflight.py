# -*- coding: utf-8 -*-

"""Pre-start safety/data-integrity checks for BTC HUNTER V1.2.3."""

import argparse
import json
import sys

import EvidenceHunter_config as cfg


def check(name, condition, detail=""):
    status = "PASS" if condition else "FAIL"
    print(f"{status:<5} {name:<36} {detail}")
    return bool(condition)


def info(name, detail=""):
    print(f"INFO  {name:<36} {detail}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--live", action="store_true", help="also perform read-only Binance market/account calls")
    args = parser.parse_args()

    cfg.ensure_directories()
    session = cfg.load_dataset_session()
    dataset_id = cfg.get_dataset_id()
    ok = True
    print("=" * 88)
    print("BTC HUNTER V1.2.3 PRE-FLIGHT — BINANCE EXCHANGE CLOCK")
    print("=" * 88)

    ok &= check("PAPER_ONLY", cfg.PAPER_ONLY is True, str(cfg.PAPER_ONLY))
    ok &= check("AUTO_TRADE disabled", cfg.AUTO_TRADE is False, str(cfg.AUTO_TRADE))
    ok &= check("AUTO_CANCEL disabled", cfg.AUTO_CANCEL is False, str(cfg.AUTO_CANCEL))
    ok &= check("Manual confirmation required", cfg.MANUAL_CONFIRMATION_REQUIRED is True)
    ok &= check("Dataset session initialized", bool(dataset_id), str(dataset_id))
    ok &= check("Schema version", cfg.SCHEMA_VERSION == "V1.2.3", cfg.SCHEMA_VERSION)
    ok &= check("Collector version", cfg.COLLECTOR_VERSION == "V1.2.3", cfg.COLLECTOR_VERSION)
    ok &= check("Outcome version", cfg.OUTCOME_VERSION == "V1.2.3", cfg.OUTCOME_VERSION)
    ok &= check("V2 analysis path", cfg.ANALYSIS_FILE_V2.parent.name == "v2", str(cfg.ANALYSIS_FILE_V2))
    ok &= check("Minimum RR configured", cfg.MIN_REWARD_RISK >= 1.5, str(cfg.MIN_REWARD_RISK))

    if session:
        ok &= check("Session schema matches", session.get("schema_version") == cfg.SCHEMA_VERSION, str(session.get("schema_version")))
        ok &= check("Session collector matches", session.get("collector_version") == cfg.COLLECTOR_VERSION, str(session.get("collector_version")))
        ok &= check("Session outcome matches", session.get("outcome_version") == cfg.OUTCOME_VERSION, str(session.get("outcome_version")))

    if args.live:
        try:
            from EvidenceHunter_clock import get_clock_status
            clock = get_clock_status(force_sync=True)
            ok &= check(
                "Binance exchange clock usable",
                clock.get("usable") is True,
                json.dumps(clock, ensure_ascii=False),
            )
            ok &= check(
                "Clock sync RTT healthy",
                clock.get("healthy") is True,
                f"rtt_ms={clock.get('rtt_ms')} limit={clock.get('max_rtt_ms')}",
            )
            info(
                "Windows clock correction",
                f"offset_ms={clock.get('offset_ms')} local_clock_ahead_ms={clock.get('local_clock_ahead_ms')}",
            )
        except Exception as exc:
            ok &= check("Binance exchange clock usable", False, f"{type(exc).__name__}: {exc}")

        try:
            from EvidenceHunter_market import build_market_snapshot
            market = build_market_snapshot()
            data_quality = market.get("data_quality", {}) or {}
            funding = market.get("funding", {}) or {}
            time_consistency = market.get("time_consistency", {}) or {}
            price_detail = market.get("price_detail", {}) or {}

            ok &= check("Live market read", data_quality.get("price_valid") is True, str(market.get("price")))
            ok &= check(
                "Funding metadata",
                funding.get("next_funding_time") is not None,
                json.dumps(funding, ensure_ascii=False),
            )
            ok &= check(
                "Funding interval minutes",
                isinstance(funding.get("funding_interval_minutes"), (int, float))
                and funding.get("funding_interval_minutes") > 0,
                str(funding.get("funding_interval_minutes")),
            )
            ok &= check(
                "Time to funding",
                isinstance(funding.get("time_to_funding_seconds"), (int, float)),
                str(funding.get("time_to_funding_seconds")),
            )
            ok &= check(
                "Price exchange timestamp",
                isinstance(price_detail.get("exchange_event_time_ms"), int)
                and price_detail.get("exchange_event_time_ms") > 0,
                str(price_detail.get("exchange_event_time_ms")),
            )
            ok &= check(
                "Canonical price source",
                price_detail.get("source") == "RECENT_MARKET_TRADE",
                f"source={price_detail.get('source')} trade_id={price_detail.get('canonical_trade_id')}",
            )
            ok &= check(
                "Market clock sync valid",
                data_quality.get("clock_sync_valid") is True,
                f"offset={time_consistency.get('clock_offset_ms')} rtt={time_consistency.get('clock_rtt_ms')}",
            )
            ok &= check(
                "Source freshness valid",
                data_quality.get("source_freshness_valid") is True,
                (
                    f"price_age={time_consistency.get('price_event_age_ms')}/"
                    f"{time_consistency.get('price_event_max_age_ms')} "
                    f"funding_age={time_consistency.get('funding_event_age_ms')}/"
                    f"{time_consistency.get('funding_event_max_age_ms')} "
                    f"oi_age={time_consistency.get('open_interest_event_age_ms')}/"
                    f"{time_consistency.get('open_interest_event_max_age_ms')}"
                ),
            )
            info(
                "Cross-source event spread",
                f"{time_consistency.get('source_event_spread_ms')} ms (informational; not a hard gate)",
            )
            ticker_ref = market.get("ticker_price_reference") or {}
            if ticker_ref:
                info(
                    "Ticker reference only",
                    (
                        f"price={ticker_ref.get('price')} "
                        f"event_age_at_receive_ms={ticker_ref.get('event_age_at_receive_ms')} "
                        f"(not canonical; not a hard gate)"
                    ),
                )
        except Exception as exc:
            ok &= check("Live market read", False, f"{type(exc).__name__}: {exc}")

        try:
            from EvidenceHunter_account import build_account_snapshot
            account = build_account_snapshot()
            ok &= check("Live account read", account.get("account", {}).get("total_margin_balance", 0) > 0, str(account.get("account")))
            position = account.get("position", {}) or {}
            ok &= check(
                "Position ambiguity field",
                "position_ambiguous" in position,
                f"side={position.get('side')} ambiguous={position.get('position_ambiguous')}",
            )
            ok &= check("Account is read-only", account.get("safety", {}).get("read_only") is True)
        except Exception as exc:
            ok &= check("Live account read", False, f"{type(exc).__name__}: {exc}")

    print("=" * 88)
    print("PRE-FLIGHT RESULT:", "PASS" if ok else "FAIL")
    print("=" * 88)
    sys.exit(0 if ok else 2)


if __name__ == "__main__":
    main()


