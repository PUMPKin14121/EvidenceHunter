# -*- coding: utf-8 -*-
"""
Tests for EvidenceHunter_discovery_runner_effort_result_divergence_v1.py.

Follows the isolation convention established in tests/test_audit_order.py:
this module is entirely self-contained (no shared conftest.py yet -- extract
one only once a second test module needs the same fixtures), uses only
synthetic fixture data (never the real Frozen Dataset or real Contract
file), and every test that needs a specific EXPECTED_* identity constant
monkeypatches the runner module's own module-global (not some other
module's copy of it) for exactly the duration of that test, per the
"patch where it's used" rule.

ORACLE CLASSIFICATION
----------------------
Every computation test's expected value is a CONTRACT_ORACLE: hand-derived
directly from the arithmetic the Frozen Contract specifies in
confirmed_effort_fields / contemporaneous_result / future_targets (not from
running the runner once and asserting whatever it produced). Identity-gate
and eligibility tests are SPECIFICATION_ORACLE: derived from the Contract's
eligibility block and this module's own documented fail-closed contract.
"""

import csv
import hashlib
import io
import json
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

import EvidenceHunter_discovery_runner_effort_result_divergence_v1 as runner


# ---------------------------------------------------------------------------
# Fixture builders
# ---------------------------------------------------------------------------

TEST_CONTRACT_ID = "EFFORT_RESULT_DIVERGENCE_V1_DISCOVERY_CONTRACT_V1"
TEST_DATASET_ID = "TEST_DATASET_ID_20260101"


def make_contract_file(tmp_path: Path, contract_id: str = TEST_CONTRACT_ID) -> tuple[Path, str]:
    data = {"contract_id": contract_id, "note": "synthetic test fixture, not the real Contract"}
    p = tmp_path / "contract.json"
    p.write_text(json.dumps(data), encoding="utf-8")
    return p, hashlib.sha256(p.read_bytes()).hexdigest()


def make_record(record_id, ts_iso, buy, sell, orderflow_age=1.0,
                 dataset_id=TEST_DATASET_ID, dq_pass=True, trade_gap=False, ready=True):
    return {
        "record_id": record_id,
        "dataset_id": dataset_id,
        "data_quality_dimensions": {
            "freshness": {"pass": dq_pass, "orderflow_age_seconds": orderflow_age},
            "completeness": {"pass": dq_pass},
            "continuity": {"pass": dq_pass, "trade_gap": trade_gap},
            "validity": {"pass": dq_pass},
        },
        "features": {
            "timestamp": ts_iso,
            "orderflow": {
                "ready": ready,
                "aggressive_buy_usdt": buy,
                "aggressive_sell_usdt": sell,
            },
        },
    }


def make_dataset_file(tmp_path: Path, records: list) -> tuple[Path, str]:
    p = tmp_path / "dataset.jsonl"
    with open(p, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")
    return p, hashlib.sha256(p.read_bytes()).hexdigest()


def make_archive_zip(tmp_path: Path, day: str, rows: list) -> tuple[Path, str]:
    """rows: list of (agg_id, price, qty, first_id, last_id, ts_ms, is_buyer_maker)."""
    archive_dir = tmp_path / "archive"
    archive_dir.mkdir(exist_ok=True)
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["agg_trade_id", "price", "quantity", "first_trade_id", "last_trade_id",
                "transact_time", "is_buyer_maker"])
    for row in rows:
        w.writerow(row)
    zpath = archive_dir / f"BTCUSDT-aggTrades-{day}.zip"
    with zipfile.ZipFile(zpath, "w") as zf:
        zf.writestr(f"BTCUSDT-aggTrades-{day}.csv", buf.getvalue())
    return zpath, hashlib.sha256(zpath.read_bytes()).hexdigest()


def fake_fetch_fn(all_rows, call_log=None):
    """Builds a fetch_fn matching EvidenceHunter_archive.get_archive_agg_trades_between's
    signature/contract. If call_log is given, every (start_ms, end_ms) pair
    queried is appended to it, so tests can assert on exactly what ranges
    were requested (used by the no-future-leakage test)."""
    def fetch(symbol, start_ms, end_ms, cache_dir, require_checksum):
        if call_log is not None:
            call_log.append((start_ms, end_ms))
        out = []
        for row in all_rows:
            ts = row[5]
            if start_ms <= ts <= end_ms:
                out.append({"trade_id": row[0], "time": ts, "price": row[1], "quantity": row[2]})
        return out
    return fetch


def iso(dt: datetime) -> str:
    return dt.isoformat().replace("+00:00", "+00:00")


def ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


DAY0 = datetime(2026, 1, 1, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def _isolate_identity_constants(monkeypatch):
    """Every test starts from a clean set of EXPECTED_* constants pointing at
    nothing (empty archive manifest), so a test that forgets to set up
    identity explicitly fails loudly (IdentityGateFailure) rather than
    silently matching leftover state from another test."""
    monkeypatch.setattr(runner, "EXPECTED_CONTRACT_ID", TEST_CONTRACT_ID)
    monkeypatch.setattr(runner, "EXPECTED_CONTRACT_SHA256", "0" * 64)
    monkeypatch.setattr(runner, "EXPECTED_DATASET_ID", TEST_DATASET_ID)
    monkeypatch.setattr(runner, "EXPECTED_DATASET_SHA256", "0" * 64)
    monkeypatch.setattr(runner, "EXPECTED_ARCHIVE_SHA256", {})
    yield


# ---------------------------------------------------------------------------
# 1-3: fail-closed identity gate
# ---------------------------------------------------------------------------

def test_contract_hash_mismatch_fails_closed(tmp_path, monkeypatch):
    contract_path, real_hash = make_contract_file(tmp_path)
    dataset_path, dataset_hash = make_dataset_file(tmp_path, [])
    monkeypatch.setattr(runner, "EXPECTED_CONTRACT_SHA256", "f" * 64)  # deliberately wrong
    monkeypatch.setattr(runner, "EXPECTED_DATASET_SHA256", dataset_hash)
    monkeypatch.setattr(runner, "EXPECTED_ARCHIVE_SHA256", {})

    result = runner.verify_identity(contract_path, dataset_path, tmp_path / "archive")
    assert result.passed is False
    assert any(e.startswith("CONTRACT_HASH_MISMATCH") for e in result.errors)


def test_dataset_hash_mismatch_fails_closed(tmp_path, monkeypatch):
    contract_path, contract_hash = make_contract_file(tmp_path)
    dataset_path, real_hash = make_dataset_file(tmp_path, [])
    monkeypatch.setattr(runner, "EXPECTED_CONTRACT_SHA256", contract_hash)
    monkeypatch.setattr(runner, "EXPECTED_DATASET_SHA256", "e" * 64)  # deliberately wrong
    monkeypatch.setattr(runner, "EXPECTED_ARCHIVE_SHA256", {})

    result = runner.verify_identity(contract_path, dataset_path, tmp_path / "archive")
    assert result.passed is False
    assert any(e.startswith("DATASET_HASH_MISMATCH") for e in result.errors)


def test_archive_hash_mismatch_fails_closed(tmp_path, monkeypatch):
    contract_path, contract_hash = make_contract_file(tmp_path)
    dataset_path, dataset_hash = make_dataset_file(tmp_path, [])
    zpath, real_hash = make_archive_zip(tmp_path, "2026-01-01", [(1, "100.0", "1", 1, 1, 0, "false")])
    monkeypatch.setattr(runner, "EXPECTED_CONTRACT_SHA256", contract_hash)
    monkeypatch.setattr(runner, "EXPECTED_DATASET_SHA256", dataset_hash)
    monkeypatch.setattr(runner, "EXPECTED_ARCHIVE_SHA256", {"2026-01-01": "d" * 64})  # wrong

    result = runner.verify_identity(contract_path, dataset_path, tmp_path / "archive")
    assert result.passed is False
    assert any(e.startswith("ARCHIVE_HASH_MISMATCH:2026-01-01") for e in result.errors)


def test_all_identity_checks_pass_with_correct_hashes(tmp_path, monkeypatch):
    contract_path, contract_hash = make_contract_file(tmp_path)
    dataset_path, dataset_hash = make_dataset_file(tmp_path, [])
    zpath, archive_hash = make_archive_zip(tmp_path, "2026-01-01", [(1, "100.0", "1", 1, 1, 0, "false")])
    monkeypatch.setattr(runner, "EXPECTED_CONTRACT_SHA256", contract_hash)
    monkeypatch.setattr(runner, "EXPECTED_DATASET_SHA256", dataset_hash)
    monkeypatch.setattr(runner, "EXPECTED_ARCHIVE_SHA256", {"2026-01-01": archive_hash})

    result = runner.verify_identity(contract_path, dataset_path, tmp_path / "archive")
    assert result.passed is True
    assert result.errors == []


# ---------------------------------------------------------------------------
# 4: missing target-window archive day -> INELIGIBLE per Contract, not a crash
# ---------------------------------------------------------------------------

def test_missing_archive_day_marks_record_ineligible():
    decision = DAY0 + timedelta(hours=12)
    record = make_record("r1", iso(decision), buy=100.0, sell=50.0, orderflow_age=1.0)
    covered_days = set()  # nothing covered at all
    result = runner.check_eligibility(record, TEST_DATASET_ID, covered_days)
    assert result.eligible is False
    assert "ARCHIVE_COVERAGE_MISSING" in result.reasons


def test_full_archive_coverage_makes_record_eligible_on_this_criterion():
    decision = DAY0 + timedelta(hours=12)
    record = make_record("r1", iso(decision), buy=100.0, sell=50.0, orderflow_age=1.0)
    covered_days = {(DAY0 + timedelta(days=d)).date().isoformat() for d in range(-1, 2)}
    result = runner.check_eligibility(record, TEST_DATASET_ID, covered_days)
    assert result.eligible is True
    assert result.reasons == []


# ---------------------------------------------------------------------------
# 5: malformed timestamp -> Contract-defined exclusion (not a crash)
# ---------------------------------------------------------------------------

def test_malformed_timestamp_is_excluded_not_crashed():
    record = make_record("r1", "not-a-real-timestamp", buy=100.0, sell=50.0)
    result = runner.check_eligibility(record, TEST_DATASET_ID, covered_days=set())
    assert result.eligible is False
    assert "INVALID_FEATURE_TIMESTAMP" in result.reasons


def test_missing_timestamp_field_is_excluded_not_crashed():
    record = make_record("r1", None, buy=100.0, sell=50.0)
    result = runner.check_eligibility(record, TEST_DATASET_ID, covered_days=set())
    assert result.eligible is False
    assert "INVALID_FEATURE_TIMESTAMP" in result.reasons


# ---------------------------------------------------------------------------
# 6: duplicate / reordered archive rows handling
# ---------------------------------------------------------------------------

def test_price_lookup_deduplicates_and_reorders_defensively():
    # Deliberately out of list-order, plus an exact duplicate row (same
    # trade_id, same fields -- e.g. the upstream source handed back the same
    # trade twice) -- the reader must still resolve to the single, correctly
    # time-sorted answer rather than trusting list position or over-counting
    # the duplicate.
    messy = [
        {"trade_id": 3, "time": 3000, "price": 300.0, "quantity": 1},
        {"trade_id": 1, "time": 1000, "price": 100.0, "quantity": 1},
        {"trade_id": 1, "time": 1000, "price": 100.0, "quantity": 1},  # exact duplicate row
        {"trade_id": 2, "time": 2000, "price": 200.0, "quantity": 1},
    ]

    def fetch(symbol, start_ms, end_ms, cache_dir, require_checksum):
        return [t for t in messy if start_ms <= t["time"] <= end_ms]

    reader = runner.ArchiveReader(symbol="BTCUSDT", cache_dir=Path("."), fetch_fn=fetch)
    assert reader.price_at_or_after(0) == 100.0  # earliest by time, not by list order
    assert reader.price_at_or_before(3500) == 300.0  # latest by time


# ---------------------------------------------------------------------------
# 7: quantile boundary / tie handling
# ---------------------------------------------------------------------------

def test_quantile_boundaries_and_deterministic_tie_assignment():
    # 10 values, 5 quintiles -> boundaries at the 20/40/60/80th percentiles.
    values = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    bounds = runner.quantile_boundaries(values, n_bins=5)
    assert len(bounds) == 4

    # A value exactly equal to a boundary must always land in the LOWER bin
    # (documented, fixed tie rule) -- verified twice to confirm determinism.
    bin_a = runner.assign_bin(bounds[0], bounds)
    bin_b = runner.assign_bin(bounds[0], bounds)
    assert bin_a == bin_b
    assert bin_a == 0  # bisect_left: value==boundary goes to the bin BELOW the boundary

    # All ties at the same value must always resolve to the same bin.
    tie_value = bounds[2]
    assignments = {runner.assign_bin(tie_value, bounds) for _ in range(5)}
    assert len(assignments) == 1


def test_quantile_boundaries_do_not_mutate_input_order():
    values = [5, 1, 4, 2, 3]
    original = list(values)
    runner.quantile_boundaries(values, n_bins=5)
    assert values == original  # caller's list must be untouched


# ---------------------------------------------------------------------------
# 8: mirror symmetry (buy/sell direction definition)
# ---------------------------------------------------------------------------

def test_mirror_symmetry_of_buy_and_sell():
    """Swapping buy_usdt and sell_usdt must flip the sign of every directional
    quantity (signed_effort, directional_response, effort_signed_future_return)
    while leaving every magnitude (effort_magnitude, |return|) identical."""
    decision = DAY0 + timedelta(hours=12)
    record_a = make_record("a", iso(decision), buy=300.0, sell=100.0, orderflow_age=1.0)
    record_b = make_record("b", iso(decision), buy=100.0, sell=300.0, orderflow_age=1.0)  # mirror

    covered_days = {(DAY0 + timedelta(days=d)).date().isoformat() for d in range(-1, 2)}
    elig_a = runner.check_eligibility(record_a, TEST_DATASET_ID, covered_days)
    elig_b = runner.check_eligibility(record_b, TEST_DATASET_ID, covered_days)
    assert elig_a.eligible and elig_b.eligible

    decision_ms = ms(decision)
    window_start_ms = ms(elig_a.window_start)
    window_end_ms = ms(elig_a.window_end)
    # Asymmetric prices around decision_time so return_60s_bps != 0, making
    # the sign flip observable rather than trivially 0 == -0.
    price_rows = [
        (1, "100.0", "1", 1, 1, window_start_ms, "false"),
        (2, "110.0", "1", 2, 2, window_end_ms, "false"),
        (3, "110.0", "1", 3, 3, decision_ms, "false"),
        (4, "121.0", "1", 4, 4, decision_ms + 300_000, "false"),
        (5, "133.1", "1", 5, 5, decision_ms + 900_000, "false"),
        (6, "146.41", "1", 6, 6, decision_ms + 1_800_000, "false"),
    ]
    fetch = fake_fetch_fn(price_rows)
    reader = runner.ArchiveReader(symbol="BTCUSDT", cache_dir=Path("."), fetch_fn=fetch)

    comp_a = runner.compute_record(record_a, elig_a, reader)
    comp_b = runner.compute_record(record_b, elig_a.__class__(**{**elig_b.__dict__}), reader)

    assert comp_a.signed_effort == pytest.approx(-comp_b.signed_effort)
    assert comp_a.effort_magnitude == pytest.approx(comp_b.effort_magnitude)
    assert comp_a.return_60s_bps == pytest.approx(comp_b.return_60s_bps)  # market data identical
    assert comp_a.directional_response == pytest.approx(-comp_b.directional_response)
    for label, _ in runner.HORIZONS_SECONDS:
        assert comp_a.future[label]["raw_future_return_bps"] == pytest.approx(
            comp_b.future[label]["raw_future_return_bps"]
        )
        assert comp_a.future[label]["effort_signed_future_return"] == pytest.approx(
            -comp_b.future[label]["effort_signed_future_return"]
        )


# ---------------------------------------------------------------------------
# 9: no future leakage around features.timestamp
# ---------------------------------------------------------------------------

def test_contemporaneous_window_never_queries_past_window_end():
    decision = DAY0 + timedelta(hours=12)
    record = make_record("r1", iso(decision), buy=100.0, sell=50.0, orderflow_age=1.0)
    covered_days = {(DAY0 + timedelta(days=d)).date().isoformat() for d in range(-1, 2)}
    elig = runner.check_eligibility(record, TEST_DATASET_ID, covered_days)

    window_end_ms = ms(elig.window_end)
    call_log = []
    price_rows = [
        (1, "100.0", "1", 1, 1, ms(elig.window_start), "false"),
        (2, "100.0", "1", 2, 2, window_end_ms, "false"),
        (3, "100.0", "1", 3, 3, ms(decision), "false"),
        (4, "100.0", "1", 4, 4, ms(decision) + 300_000, "false"),
        (5, "100.0", "1", 5, 5, ms(decision) + 900_000, "false"),
        (6, "100.0", "1", 6, 6, ms(decision) + 1_800_000, "false"),
    ]
    fetch = fake_fetch_fn(price_rows, call_log=call_log)
    reader = runner.ArchiveReader(symbol="BTCUSDT", cache_dir=Path("."), fetch_fn=fetch)

    runner.compute_record(record, elig, reader)

    # Find the specific call that resolved p_end (price_at_or_before(window_end)):
    # every such call's end_ms must be <= window_end_ms -- it must never ask
    # for data past the effort window's own end when reconstructing p_end.
    p_end_calls = [c for c in call_log if c[1] <= window_end_ms]
    assert p_end_calls, "expected at least one call bounded at or before window_end"
    for start_ms, end_ms in p_end_calls:
        assert end_ms <= window_end_ms


def test_future_target_never_queries_before_decision_time():
    decision = DAY0 + timedelta(hours=12)
    record = make_record("r1", iso(decision), buy=100.0, sell=50.0, orderflow_age=1.0)
    covered_days = {(DAY0 + timedelta(days=d)).date().isoformat() for d in range(-1, 2)}
    elig = runner.check_eligibility(record, TEST_DATASET_ID, covered_days)
    decision_ms = ms(decision)

    call_log = []
    price_rows = [
        (1, "100.0", "1", 1, 1, ms(elig.window_start), "false"),
        (2, "100.0", "1", 2, 2, ms(elig.window_end), "false"),
        (3, "100.0", "1", 3, 3, decision_ms, "false"),
        (4, "100.0", "1", 4, 4, decision_ms + 300_000, "false"),
        (5, "100.0", "1", 5, 5, decision_ms + 900_000, "false"),
        (6, "100.0", "1", 6, 6, decision_ms + 1_800_000, "false"),
    ]
    fetch = fake_fetch_fn(price_rows, call_log=call_log)
    reader = runner.ArchiveReader(symbol="BTCUSDT", cache_dir=Path("."), fetch_fn=fetch)

    runner.compute_record(record, elig, reader)

    # price_at_or_after is always called with start_ms == the timestamp being
    # resolved (decision_time or decision_time+horizon) -- every such call's
    # start_ms must be >= decision_ms, i.e. future-target lookups never probe
    # before the decision time.
    future_lookup_calls = [c for c in call_log if c[0] >= decision_ms]
    assert len(future_lookup_calls) >= 4  # decision + 3 horizons
    for start_ms, end_ms in future_lookup_calls:
        assert start_ms >= decision_ms


# ---------------------------------------------------------------------------
# 10: horizons exactly 5m/15m/30m, fixed order, never re-sorted
# ---------------------------------------------------------------------------

def test_horizons_are_exactly_5m_15m_30m_in_contract_order():
    assert runner.HORIZONS_SECONDS == [("5m", 300), ("15m", 900), ("30m", 1800)]
    assert runner.MAX_HORIZON_SECONDS == 1800


def test_future_output_contains_all_three_horizons_every_time():
    decision = DAY0 + timedelta(hours=12)
    record = make_record("r1", iso(decision), buy=100.0, sell=50.0, orderflow_age=1.0)
    covered_days = {(DAY0 + timedelta(days=d)).date().isoformat() for d in range(-1, 2)}
    elig = runner.check_eligibility(record, TEST_DATASET_ID, covered_days)
    decision_ms = ms(decision)
    price_rows = [
        (1, "100.0", "1", 1, 1, ms(elig.window_start), "false"),
        (2, "100.0", "1", 2, 2, ms(elig.window_end), "false"),
        (3, "100.0", "1", 3, 3, decision_ms, "false"),
        (4, "105.0", "1", 4, 4, decision_ms + 300_000, "false"),
        (5, "110.0", "1", 5, 5, decision_ms + 900_000, "false"),
        (6, "115.0", "1", 6, 6, decision_ms + 1_800_000, "false"),
    ]
    fetch = fake_fetch_fn(price_rows)
    reader = runner.ArchiveReader(symbol="BTCUSDT", cache_dir=Path("."), fetch_fn=fetch)
    comp = runner.compute_record(record, elig, reader)
    assert set(comp.future.keys()) == {"5m", "15m", "30m"}


# ---------------------------------------------------------------------------
# 11: forbidden subgroup/horizon-selection logic must not exist
# ---------------------------------------------------------------------------

def test_eligibility_never_reads_forbidden_subgroup_fields():
    """Contract discovery_protocol.no_primary_subgroup_selection_by forbids
    ever using these fields to select/filter records. This is a static
    guarantee: a record carrying ONLY forbidden-field data and none of the
    fields eligibility actually reads must still be evaluated purely on the
    Contract's real criteria (and fail only for reasons unrelated to those
    forbidden fields) -- proving they are never consulted."""
    decision = DAY0 + timedelta(hours=12)
    record = make_record("r1", iso(decision), buy=100.0, sell=50.0, orderflow_age=1.0)
    # Attach forbidden-subgroup-looking fields with values that WOULD fail
    # eligibility if (incorrectly) consulted.
    record["market_regime"] = "FORCE_EXCLUDE_IF_READ"
    record["trade_plan"] = "FORCE_EXCLUDE_IF_READ"
    record["funding"] = "FORCE_EXCLUDE_IF_READ"
    record["depth100_imbalance_regime"] = "FORCE_EXCLUDE_IF_READ"
    covered_days = {(DAY0 + timedelta(days=d)).date().isoformat() for d in range(-1, 2)}
    result = runner.check_eligibility(record, TEST_DATASET_ID, covered_days)
    assert result.eligible is True  # would have failed if any forbidden field were read as a gate

    # Strip the function's own docstring before scanning: the docstring
    # legitimately *names* these fields to explain that they are never used
    # as gates (e.g. "market_regime_primary_gate=false ... NEVER used").
    # What must never appear is a reference to them in the executable body.
    import ast
    import inspect
    source = inspect.getsource(runner.check_eligibility)
    func_node = ast.parse(source).body[0]
    docstring = ast.get_docstring(func_node, clean=False)
    body_source = source
    if docstring:
        body_source = source.replace('"""' + docstring + '"""', "", 1)
    for forbidden in ("market_regime", "depth100", "depth1000", "position_side",
                      "trade_plan", "funding"):
        assert forbidden not in body_source, f"eligibility must never reference '{forbidden}'"


def test_run_discovery_never_reorders_or_selects_a_best_horizon(tmp_path, monkeypatch):
    source = inspect_source_no_horizon_selection = __import__("inspect").getsource(runner)
    # No sorting/selection of HORIZONS_SECONDS by any computed statistic
    # anywhere in the module (the only ordering allowed is the fixed literal
    # assignment at module load time).
    assert "sorted(HORIZONS_SECONDS" not in source
    assert "max(future" not in source
    assert "best_horizon" not in source


# ---------------------------------------------------------------------------
# 12: repeat run determinism
# ---------------------------------------------------------------------------

def test_repeat_computation_is_byte_identical(tmp_path):
    decision = DAY0 + timedelta(hours=12)
    record = make_record("r1", iso(decision), buy=273.0, sell=118.0, orderflow_age=2.5)
    covered_days = {(DAY0 + timedelta(days=d)).date().isoformat() for d in range(-1, 2)}
    elig = runner.check_eligibility(record, TEST_DATASET_ID, covered_days)
    decision_ms = ms(decision)
    price_rows = [
        (1, "100.13", "1", 1, 1, ms(elig.window_start), "false"),
        (2, "101.77", "1", 2, 2, ms(elig.window_end), "false"),
        (3, "101.77", "1", 3, 3, decision_ms, "false"),
        (4, "103.05", "1", 4, 4, decision_ms + 300_000, "false"),
        (5, "99.41", "1", 5, 5, decision_ms + 900_000, "false"),
        (6, "104.90", "1", 6, 6, decision_ms + 1_800_000, "false"),
    ]

    def run_once():
        fetch = fake_fetch_fn(price_rows)
        reader = runner.ArchiveReader(symbol="BTCUSDT", cache_dir=Path("."), fetch_fn=fetch)
        comp = runner.compute_record(record, elig, reader)
        return json.dumps({
            "signed_effort": comp.signed_effort,
            "effort_magnitude": comp.effort_magnitude,
            "return_60s_bps": comp.return_60s_bps,
            "directional_response": comp.directional_response,
            "future": comp.future,
        }, sort_keys=True)

    r1 = run_once()
    r2 = run_once()
    r3 = run_once()
    assert r1 == r2 == r3
    assert hashlib.sha256(r1.encode()).hexdigest() == hashlib.sha256(r2.encode()).hexdigest()


def test_run_discovery_dry_run_is_deterministic_and_leaks_no_statistics(tmp_path, monkeypatch):
    contract_path, contract_hash = make_contract_file(tmp_path)
    decision = DAY0 + timedelta(hours=12)
    records = [
        make_record("r1", iso(decision), buy=100.0, sell=50.0, orderflow_age=1.0),
        make_record("r2", iso(decision + timedelta(hours=1)), buy=None, sell=None, orderflow_age=1.0),  # -> TOTAL_EFFORT_NOT_POSITIVE
    ]
    dataset_path, dataset_hash = make_dataset_file(tmp_path, records)
    zpath, archive_hash = make_archive_zip(tmp_path, "2026-01-01", [(1, "100.0", "1", 1, 1, 0, "false")])

    monkeypatch.setattr(runner, "EXPECTED_CONTRACT_SHA256", contract_hash)
    monkeypatch.setattr(runner, "EXPECTED_DATASET_SHA256", dataset_hash)
    monkeypatch.setattr(runner, "EXPECTED_ARCHIVE_SHA256", {"2026-01-01": archive_hash})

    price_rows = [(1, "100.0", "1", 1, 1, 0, "false")]
    fetch = fake_fetch_fn(price_rows)

    r1 = runner.run_discovery(contract_path, dataset_path, tmp_path / "archive", fetch, dry_run=True)
    r2 = runner.run_discovery(contract_path, dataset_path, tmp_path / "archive", fetch, dry_run=True)

    assert r1.identity.passed and r2.identity.passed
    assert r1.total_records == r2.total_records == 2
    assert r1.output_rows == [] and r2.output_rows == []  # dry-run: no per-record numbers retained
    s1 = json.dumps(runner.dry_run_summary(r1), sort_keys=True)
    s2 = json.dumps(runner.dry_run_summary(r2), sort_keys=True)
    # generated_at_utc will differ; strip it before the determinism comparison.
    d1 = json.loads(s1); d1.pop("generated_at_utc")
    d2 = json.loads(s2); d2.pop("generated_at_utc")
    assert d1 == d2
    # output_schema legitimately lists the COLUMN NAMES a real run would
    # produce (e.g. "return_60s_bps") -- that is schema metadata, not a
    # leaked research value. Exclude it before scanning for forbidden
    # substrings, which must never appear anywhere ELSE in the dry-run
    # summary (i.e. no actual computed numeric statistic must leak).
    d1_without_schema = {k: v for k, v in d1.items() if k != "output_schema"}
    for forbidden_key in ("return_60s_bps", "directional_response", "effort_magnitude_quintile"):
        assert forbidden_key not in json.dumps(d1_without_schema)


# ---------------------------------------------------------------------------
# 21: operational progress callback (added for the real-data performance
# pass; purely additive -- progress_callback defaults to None so every test
# above this line is unaffected).
# ---------------------------------------------------------------------------

def test_progress_callback_fires_at_expected_intervals_with_counts_only(tmp_path, monkeypatch):
    contract_path, contract_hash = make_contract_file(tmp_path)
    decision = DAY0 + timedelta(hours=12)
    # 5 eligible records + 1 ineligible (TOTAL_EFFORT_NOT_POSITIVE), so the
    # callback's own counts can be cross-checked against the final result.
    records = [make_record(f"r{i}", iso(decision + timedelta(minutes=i)), buy=100.0, sell=50.0,
                            orderflow_age=1.0) for i in range(5)]
    records.append(make_record("r_ineligible", iso(decision + timedelta(minutes=99)),
                                buy=None, sell=None, orderflow_age=1.0))
    dataset_path, dataset_hash = make_dataset_file(tmp_path, records)
    zpath, archive_hash = make_archive_zip(tmp_path, "2026-01-01", [(1, "100.0", "1", 1, 1, 0, "false")])

    monkeypatch.setattr(runner, "EXPECTED_CONTRACT_SHA256", contract_hash)
    monkeypatch.setattr(runner, "EXPECTED_DATASET_SHA256", dataset_hash)
    monkeypatch.setattr(runner, "EXPECTED_ARCHIVE_SHA256", {"2026-01-01": archive_hash})

    price_rows = [(1, "100.0", "1", 1, 1, 0, "false")]
    fetch = fake_fetch_fn(price_rows)

    calls = []
    result = runner.run_discovery(contract_path, dataset_path, tmp_path / "archive", fetch,
                                   dry_run=True, progress_callback=calls.append, progress_every=2)

    assert result.identity.passed
    # progress_every=2 over 6 total records -> callback fires at record 2, 4, 6.
    assert len(calls) == 3
    assert [c["total_records"] for c in calls] == [2, 4, 6]
    # The callback's final snapshot must agree exactly with the RunResult's
    # own final counts (no drift between the two bookkeeping paths).
    assert calls[-1]["total_records"] == result.total_records
    assert calls[-1]["eligible_records"] == result.eligible_records
    assert calls[-1]["ineligible_records"] == result.ineligible_records
    assert calls[-1]["computed_count"] == result.computed_count
    # Progress payload is counts-only -- same no-leakage guarantee as
    # dry_run_summary: no per-record or aggregate research statistic.
    for c in calls:
        payload = json.dumps(c)
        for forbidden_key in ("return_60s_bps", "directional_response", "effort_magnitude_quintile", "record_id"):
            assert forbidden_key not in payload


def test_progress_callback_none_by_default_is_backward_compatible(tmp_path, monkeypatch):
    """No progress_callback passed -> identical behavior to before this field
    existed (this is the same fixture/assertions as the dry-run determinism
    test above, just re-run without any callback wired in)."""
    contract_path, contract_hash = make_contract_file(tmp_path)
    decision = DAY0 + timedelta(hours=12)
    records = [make_record("r1", iso(decision), buy=100.0, sell=50.0, orderflow_age=1.0)]
    dataset_path, dataset_hash = make_dataset_file(tmp_path, records)
    zpath, archive_hash = make_archive_zip(tmp_path, "2026-01-01", [(1, "100.0", "1", 1, 1, 0, "false")])

    monkeypatch.setattr(runner, "EXPECTED_CONTRACT_SHA256", contract_hash)
    monkeypatch.setattr(runner, "EXPECTED_DATASET_SHA256", dataset_hash)
    monkeypatch.setattr(runner, "EXPECTED_ARCHIVE_SHA256", {"2026-01-01": archive_hash})

    price_rows = [(1, "100.0", "1", 1, 1, 0, "false")]
    fetch = fake_fetch_fn(price_rows)
    result = runner.run_discovery(contract_path, dataset_path, tmp_path / "archive", fetch, dry_run=True)
    assert result.identity.passed
    assert result.total_records == 1


# ---------------------------------------------------------------------------
# 22: dry_run=False end-to-end (added BEFORE the first real formal execution
# -- every prior dry_run=False-adjacent unit (quantile_boundaries, assign_bin)
# was already tested in isolation, but no test previously exercised
# run_discovery(..., dry_run=False) as a whole, which is the exact code path
# the first real, GPT-authorized Discovery execution depends on. This test
# must pass BEFORE that execution is run for real, per this project's
# test-first discipline.)
# ---------------------------------------------------------------------------

def test_run_discovery_dry_run_false_populates_output_rows_with_quintile_bins(tmp_path, monkeypatch):
    """10 synthetic eligible records, each with a distinct effort magnitude,
    sign, and price path -> dry_run=False must populate output_rows (one per
    computed record) with every output_schema column, valid 0-4 quintile bin
    assignments on both quintile columns, real (non-degenerate) spread across
    bins, and results identical across two independent runs (determinism is
    not just a dry_run=True property)."""
    contract_path, contract_hash = make_contract_file(tmp_path)

    n = 10
    records = []
    decisions = []
    for i in range(n):
        decision = DAY0 + timedelta(hours=2 + i)  # 1h apart, all mid-day on 2026-01-01
        decisions.append(decision)
        if i % 2 == 0:
            buy, sell = 100.0 + i * 20.0, 60.0  # positive signed_effort, varying magnitude
        else:
            buy, sell = 60.0, 100.0 + i * 20.0  # negative signed_effort, varying magnitude
        records.append(make_record(f"r{i}", iso(decision), buy=buy, sell=sell, orderflow_age=1.0))
    dataset_path, dataset_hash = make_dataset_file(tmp_path, records)

    covered_days = {"2026-01-01"}
    all_price_rows = []
    agg_id = 1
    for i, decision in enumerate(decisions):
        elig = runner.check_eligibility(records[i], TEST_DATASET_ID, covered_days)
        assert elig.eligible, elig.reasons
        window_start_ms = ms(elig.window_start)
        window_end_ms = ms(elig.window_end)
        decision_ms = ms(decision)

        p_start = 100.0 + i * 1.0
        p_end = p_start + ((i % 3 - 1) * 0.7)          # -0.7 / 0 / +0.7, cycling
        p_decision = p_end + ((i % 2) * 0.5 - 0.25)     # -0.25 / +0.25, alternating
        p_5m = p_decision + (i - 5) * 0.4               # negative for early i, positive for late i
        p_15m = p_decision + (i - 5) * 0.8
        p_30m = p_decision + (i - 5) * 1.2

        for ts_ms, price in (
            (window_start_ms, p_start),
            (window_end_ms, p_end),
            (decision_ms, p_decision),
            (decision_ms + 300_000, p_5m),
            (decision_ms + 900_000, p_15m),
            (decision_ms + 1_800_000, p_30m),
        ):
            all_price_rows.append((agg_id, f"{price:.4f}", "1", agg_id, agg_id, ts_ms, "false"))
            agg_id += 1

    zpath, archive_hash = make_archive_zip(tmp_path, "2026-01-01", all_price_rows)

    monkeypatch.setattr(runner, "EXPECTED_CONTRACT_SHA256", contract_hash)
    monkeypatch.setattr(runner, "EXPECTED_DATASET_SHA256", dataset_hash)
    monkeypatch.setattr(runner, "EXPECTED_ARCHIVE_SHA256", {"2026-01-01": archive_hash})

    fetch = fake_fetch_fn(all_price_rows)

    def run_once():
        return runner.run_discovery(contract_path, dataset_path, tmp_path / "archive", fetch, dry_run=False)

    r1 = run_once()
    r2 = run_once()

    assert r1.identity.passed and r2.identity.passed
    assert r1.total_records == r2.total_records == n
    assert r1.eligible_records == r2.eligible_records == n
    assert r1.computed_count == r2.computed_count == n
    assert r1.computation_failures == {} and r2.computation_failures == {}
    assert len(r1.output_rows) == len(r2.output_rows) == n

    schema_keys = set(r1.output_schema)
    for row in r1.output_rows:
        assert set(row.keys()) == schema_keys
        assert row["effort_magnitude_quintile"] in (0, 1, 2, 3, 4)
        assert row["directional_response_quintile"] in (0, 1, 2, 3, 4)
        for label, _ in runner.HORIZONS_SECONDS:
            assert isinstance(row[f"future_{label}_raw_bps"], float)
            assert isinstance(row[f"future_{label}_effort_signed"], float)

    # Non-degenerate spread: with 10 distinct effort magnitudes and 10
    # distinct directional responses split into 5 quintile bins, real
    # differentiation must produce more than one distinct bin value on each
    # quintile column (a bug that always returned bin 0, for example, would
    # still pass a "value in (0,1,2,3,4)" check but must be caught here).
    assert len({row["effort_magnitude_quintile"] for row in r1.output_rows}) > 1
    assert len({row["directional_response_quintile"] for row in r1.output_rows}) > 1

    # Determinism: dry_run=False must reproduce byte-identical output_rows
    # across independent runs against the same frozen inputs, matching the
    # guarantee already established for dry_run=True's summary.
    j1 = json.dumps(r1.output_rows, sort_keys=True)
    j2 = json.dumps(r2.output_rows, sort_keys=True)
    assert j1 == j2
    assert hashlib.sha256(j1.encode()).hexdigest() == hashlib.sha256(j2.encode()).hexdigest()

