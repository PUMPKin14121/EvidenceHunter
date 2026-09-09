# -*- coding: utf-8 -*-
"""
Tests for EvidenceHunter_discovery_result_builder_effort_result_divergence_v1.py
-- Layer 2 (5x5 grid summary) and Layer 3 (chronological greedy non-overlap
per horizon; moving-block bootstrap), per the exact parameters frozen in
EFFORT_RESULT_DIVERGENCE_V1_DISCOVERY_METHOD_SUPPLEMENT_V1.json.

Written and confirmed passing BEFORE this module is ever run against the
real Frozen Dataset, per this project's test-first discipline -- this is
specifically the gap GPT flagged (dependence_controls layers had zero test
coverage) that paused Formal Discovery Execution authorization.

ORACLE CLASSIFICATION
----------------------
- grid_summary / greedy_non_overlap_select tests: CONTRACT_ORACLE -- expected
  values are hand-computed directly from the Method Supplement's literal
  rules, not from running this module and trusting its own output.
- moving_block_bootstrap determinism/structural tests: SPECIFICATION_ORACLE
  (must hold for any correct implementation of the Supplement's spec).
- moving_block_bootstrap "insufficient replicates" trigger test:
  CHARACTERIZATION_ORACLE -- the exact valid_replicates count (121/200) for
  the fixed random_seed was observed once by directly running the function
  and is asserted here as a golden value, matching this project's
  established convention for seed-dependent-but-deterministic behavior
  (see e.g. test_archive_agg_index_fix.py's precedent for this pattern).
"""
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import EvidenceHunter_discovery_result_builder_effort_result_divergence_v1 as b  # noqa: E402

BASE = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _row(record_id, decision_time, mag_q, resp_q, raw_5m, signed_5m,
         raw_15m=0.0, signed_15m=0.0, raw_30m=0.0, signed_30m=0.0):
    return {
        "record_id": record_id,
        "decision_time_utc": decision_time.isoformat(),
        "effort_magnitude_quintile": mag_q,
        "directional_response_quintile": resp_q,
        "future_5m_raw_bps": raw_5m,
        "future_5m_effort_signed": signed_5m,
        "future_15m_raw_bps": raw_15m,
        "future_15m_effort_signed": signed_15m,
        "future_30m_raw_bps": raw_30m,
        "future_30m_effort_signed": signed_30m,
    }


# ---------------------------------------------------------------------------
# grid_summary
# ---------------------------------------------------------------------------

def test_grid_summary_hand_computed_stats_for_populated_cells():
    rows = [
        _row("a", BASE, 0, 0, raw_5m=10.0, signed_5m=-5.0),
        _row("b", BASE + timedelta(minutes=1), 0, 0, raw_5m=20.0, signed_5m=15.0),
        _row("c", BASE + timedelta(minutes=2), 0, 0, raw_5m=30.0, signed_5m=25.0),
        _row("d", BASE + timedelta(minutes=3), 1, 1, raw_5m=5.0, signed_5m=5.0),
        _row("e", BASE + timedelta(minutes=4), 1, 1, raw_5m=-5.0, signed_5m=-5.0),
    ]
    grid = b.grid_summary(rows, "5m")

    assert len(grid) == 25  # every one of the 25 cells always present

    cell00 = grid[(0, 0)]
    assert cell00["n"] == 3
    assert cell00["mean_raw_future_return_bps"] == pytest.approx(20.0)
    assert cell00["median_raw_future_return_bps"] == pytest.approx(20.0)
    assert cell00["mean_effort_signed_future_return_bps"] == pytest.approx(35.0 / 3.0)
    assert cell00["median_effort_signed_future_return_bps"] == pytest.approx(15.0)
    assert cell00["positive_effort_signed_rate"] == pytest.approx(2.0 / 3.0)

    cell11 = grid[(1, 1)]
    assert cell11["n"] == 2
    assert cell11["mean_raw_future_return_bps"] == pytest.approx(0.0)
    assert cell11["median_raw_future_return_bps"] == pytest.approx(0.0)
    assert cell11["mean_effort_signed_future_return_bps"] == pytest.approx(0.0)
    assert cell11["median_effort_signed_future_return_bps"] == pytest.approx(0.0)
    assert cell11["positive_effort_signed_rate"] == pytest.approx(0.5)


def test_grid_summary_empty_cell_reports_none_not_crash():
    rows = [_row("a", BASE, 0, 0, raw_5m=1.0, signed_5m=1.0)]
    grid = b.grid_summary(rows, "5m")
    empty_cell = grid[(4, 4)]
    assert empty_cell["n"] == 0
    assert empty_cell["mean_raw_future_return_bps"] is None
    assert empty_cell["median_raw_future_return_bps"] is None
    assert empty_cell["mean_effort_signed_future_return_bps"] is None
    assert empty_cell["median_effort_signed_future_return_bps"] is None
    assert empty_cell["positive_effort_signed_rate"] is None


def test_grid_summary_never_mutates_input_row_order():
    rows = [_row(f"r{i}", BASE + timedelta(minutes=i), 0, 0, raw_5m=float(i), signed_5m=float(i)) for i in range(5)]
    original_ids = [r["record_id"] for r in rows]
    b.grid_summary(rows, "5m")
    assert [r["record_id"] for r in rows] == original_ids


# ---------------------------------------------------------------------------
# greedy_non_overlap_select
# ---------------------------------------------------------------------------

def test_greedy_non_overlap_select_half_open_interval_boundary():
    # horizon = 300s (5m). Times (seconds from BASE): 0, 60, 299, 300, 301, 600.
    rows = [
        _row("t0", BASE, 0, 0, 0.0, 0.0),
        _row("t60", BASE + timedelta(seconds=60), 0, 0, 0.0, 0.0),
        _row("t299", BASE + timedelta(seconds=299), 0, 0, 0.0, 0.0),
        _row("t300", BASE + timedelta(seconds=300), 0, 0, 0.0, 0.0),
        _row("t301", BASE + timedelta(seconds=301), 0, 0, 0.0, 0.0),
        _row("t600", BASE + timedelta(seconds=600), 0, 0, 0.0, 0.0),
    ]
    selected = b.greedy_non_overlap_select(rows, horizon_seconds=300)
    # t0 accepted (first) -> t60, t299 rejected (< t0+300) -> t300 accepted
    # (== t0+300, half-open boundary IS eligible) -> t301 rejected (< t300+300)
    # -> t600 accepted (== t300+300).
    assert [r["record_id"] for r in selected] == ["t0", "t300", "t600"]


def test_greedy_non_overlap_select_tie_break_by_record_id_ascending():
    rows = [
        _row("b", BASE, 0, 0, 0.0, 0.0),  # deliberately listed before "a"
        _row("a", BASE, 0, 0, 0.0, 0.0),  # identical decision_time
    ]
    selected = b.greedy_non_overlap_select(rows, horizon_seconds=300)
    # Sort order must break the tie by record_id ascending -> "a" sorts
    # first and is accepted; "b" (same timestamp) fails t >= last+horizon
    # and is rejected.
    assert [r["record_id"] for r in selected] == ["a"]


def test_greedy_non_overlap_select_never_mutates_input_list():
    rows = [_row("z", BASE + timedelta(minutes=1), 0, 0, 0.0, 0.0),
            _row("a", BASE, 0, 0, 0.0, 0.0)]
    original_order = [r["record_id"] for r in rows]
    b.greedy_non_overlap_select(rows, horizon_seconds=300)
    assert [r["record_id"] for r in rows] == original_order  # caller's list/order untouched


def test_greedy_non_overlap_select_reuses_original_bin_labels():
    rows = [_row("a", BASE, 3, 2, 1.0, 1.0), _row("b", BASE + timedelta(seconds=600), 4, 0, 2.0, 2.0)]
    selected = b.greedy_non_overlap_select(rows, horizon_seconds=300)
    assert selected[0]["effort_magnitude_quintile"] == 3
    assert selected[0]["directional_response_quintile"] == 2
    assert selected[1]["effort_magnitude_quintile"] == 4
    assert selected[1]["directional_response_quintile"] == 0


# ---------------------------------------------------------------------------
# moving_block_bootstrap
# ---------------------------------------------------------------------------

def _make_rows_across_cells(n, seed_values=True):
    rows = []
    for i in range(n):
        mag_q = i % 5
        resp_q = (i // 5) % 5
        v = float(i) if seed_values else 0.0
        rows.append(_row(f"r{i:03d}", BASE + timedelta(minutes=i), mag_q, resp_q,
                          raw_5m=v, signed_5m=v, raw_15m=v * 2, signed_15m=v * 2,
                          raw_30m=v * 3, signed_30m=v * 3))
    return rows


def test_moving_block_bootstrap_is_deterministic_with_fixed_seed():
    rows = _make_rows_across_cells(60)
    r1 = b.moving_block_bootstrap(rows, block_length_rows=30, replicates=100, random_seed=20260906)
    r2 = b.moving_block_bootstrap(rows, block_length_rows=30, replicates=100, random_seed=20260906)
    assert r1 == r2


def test_moving_block_bootstrap_different_seed_can_differ():
    rows = _make_rows_across_cells(60)
    r1 = b.moving_block_bootstrap(rows, block_length_rows=30, replicates=100, random_seed=1)
    r2 = b.moving_block_bootstrap(rows, block_length_rows=30, replicates=100, random_seed=2)
    assert r1 != r2  # sanity: the seed is actually wired in, not ignored


def test_moving_block_bootstrap_structural_shape_all_cells_and_horizons_present():
    rows = _make_rows_across_cells(60)
    result = b.moving_block_bootstrap(rows, block_length_rows=30, replicates=50, random_seed=20260906)
    assert len(result) == 25
    for (i, j), per_horizon in result.items():
        assert 0 <= i < 5 and 0 <= j < 5
        assert set(per_horizon.keys()) == {"5m", "15m", "30m"}
        for h, cell_result in per_horizon.items():
            assert "valid_replicates" in cell_result
            for stat_name in ("mean_raw_future_return_bps", "mean_effort_signed_future_return_bps"):
                val = cell_result[stat_name]
                if val == "BOOTSTRAP_INSUFFICIENT_VALID_REPLICATES":
                    continue
                assert val["ci_low"] <= val["ci_high"]


def test_moving_block_bootstrap_constant_cell_collapses_ci_to_that_constant():
    """Every row lands in the SAME cell with an identical horizon value ->
    no matter how resampling shuffles rows, the mean of any non-empty subset
    of identical values is that exact value, so the CI must collapse to a
    single point at that constant -- a strong, exactly-checkable structural
    guarantee independent of the RNG's specific draws."""
    rows = [_row(f"r{i:03d}", BASE + timedelta(minutes=i), 2, 2, raw_5m=42.0, signed_5m=42.0)
            for i in range(60)]
    result = b.moving_block_bootstrap(rows, block_length_rows=30, replicates=200, random_seed=20260906)
    cell = result[(2, 2)]["5m"]
    assert cell["valid_replicates"] == 200  # every replicate must include >=1 row (only cell populated)
    assert cell["mean_raw_future_return_bps"]["ci_low"] == pytest.approx(42.0)
    assert cell["mean_raw_future_return_bps"]["ci_high"] == pytest.approx(42.0)
    assert cell["mean_effort_signed_future_return_bps"]["ci_low"] == pytest.approx(42.0)
    assert cell["mean_effort_signed_future_return_bps"]["ci_high"] == pytest.approx(42.0)
    # Every other (empty) cell must correctly report insufficient replicates,
    # not a fabricated interval.
    other_cell = result[(0, 0)]["5m"]
    assert other_cell["valid_replicates"] == 0
    assert other_cell["mean_raw_future_return_bps"] == "BOOTSTRAP_INSUFFICIENT_VALID_REPLICATES"


def test_moving_block_bootstrap_flags_insufficient_replicates_for_a_rare_cell():
    """19 rows in cell (0,0), exactly 1 row in the rare cell (4,4), row-level
    bootstrap (block_length_rows=1), 200 replicates, the Method Supplement's
    official random_seed. With such low representation the rare cell must
    fall below the 95% valid-replicate threshold (190/200) and be flagged,
    never silently given a CI computed from too few replicates. The exact
    valid_replicates=121 is a golden/characterization value observed once
    from this fixed seed+construction (see module docstring)."""
    rows = [_row(f"a{i:02d}", BASE + timedelta(minutes=i), 0, 0, raw_5m=1.0, signed_5m=1.0) for i in range(19)]
    rows.append(_row("zrare", BASE + timedelta(minutes=19), 4, 4, raw_5m=9.0, signed_5m=9.0))

    result = b.moving_block_bootstrap(rows, block_length_rows=1, replicates=200, random_seed=20260906)

    rare = result[(4, 4)]["5m"]
    assert rare["valid_replicates"] == 121  # golden value, see docstring
    assert rare["mean_raw_future_return_bps"] == "BOOTSTRAP_INSUFFICIENT_VALID_REPLICATES"
    assert rare["mean_effort_signed_future_return_bps"] == "BOOTSTRAP_INSUFFICIENT_VALID_REPLICATES"

    common = result[(0, 0)]["5m"]
    assert common["valid_replicates"] == 200
    assert common["mean_raw_future_return_bps"]["ci_low"] == pytest.approx(1.0)
    assert common["mean_raw_future_return_bps"]["ci_high"] == pytest.approx(1.0)


def test_moving_block_bootstrap_never_mutates_input_row_order():
    rows = _make_rows_across_cells(40)
    original_ids = [r["record_id"] for r in rows]
    b.moving_block_bootstrap(rows, block_length_rows=30, replicates=20, random_seed=20260906)
    assert [r["record_id"] for r in rows] == original_ids


def test_moving_block_bootstrap_empty_input_returns_all_cells_insufficient_not_crash():
    result = b.moving_block_bootstrap([], block_length_rows=30, replicates=50, random_seed=20260906)
    assert len(result) == 25
    for (i, j), per_horizon in result.items():
        for h, cell_result in per_horizon.items():
            assert cell_result["valid_replicates"] == 0
            assert cell_result["mean_raw_future_return_bps"] == "BOOTSTRAP_INSUFFICIENT_VALID_REPLICATES"

