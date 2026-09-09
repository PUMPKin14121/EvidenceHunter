# -*- coding: utf-8 -*-
"""
Discovery result builder for EFFORT_RESULT_DIVERGENCE_V1.

Implements Layer 2 (5x5 grid summary) and Layer 3 (the two dependence
controls: chronological greedy non-overlap sampling per horizon, and a
moving-block bootstrap) required by the Frozen Contract's
discovery_protocol.dependence_controls, per the exact parameters frozen in
EFFORT_RESULT_DIVERGENCE_V1_DISCOVERY_METHOD_SUPPLEMENT_V1.json.

Deliberately kept as a SEPARATE module from
EvidenceHunter_discovery_runner_effort_result_divergence_v1.py (the Runner,
already committed to the repository): this module only ever consumes the
Runner's already-computed, already-frozen `output_rows` (dry_run=False
RunResult) -- it never touches the Contract, the Dataset, or the archive
directly, and never re-runs any per-record price reconstruction. This keeps
the already-audited Runner file untouched by this changeset.

Every input row is expected to be one of the Runner's `output_rows` dicts
(see run_discovery()'s output_schema), i.e. already carries its FROZEN
full-sample effort_magnitude_quintile / directional_response_quintile bin
labels -- per the Method Supplement's bin_boundary_policy, nothing in this
module ever recomputes or reassigns a bin label.

ORACLE CLASSIFICATION
----------------------
- grid_summary, greedy_non_overlap_select: CONTRACT_ORACLE / hand-derived --
  expected values in the test suite are computed by hand from the Method
  Supplement's literal rules, not from running this code and trusting its
  output.
- moving_block_bootstrap: structural + determinism assertions are
  SPECIFICATION_ORACLE (must hold for ANY correct implementation of the
  Supplement's spec); the "insufficient replicates" trigger point for a
  specific contrived low-probability cell is CHARACTERIZATION_ORACLE (the
  exact valid-replicate count for a fixed seed is observed once and then
  asserted, matching the project's established convention for
  seed-dependent-but-deterministic behavior).
"""
from __future__ import annotations

import math
import random
from datetime import datetime, timedelta
from statistics import mean, median

QUINTILE_COUNT = 5
HORIZONS_SECONDS = [("5m", 300), ("15m", 900), ("30m", 1800)]


def _parse_iso(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _empty_cells() -> dict:
    return {(i, j): [] for i in range(QUINTILE_COUNT) for j in range(QUINTILE_COUNT)}


def grid_summary(rows: list, horizon_label: str) -> dict:
    """Per Method Supplement grid_summary_layer. Returns a dict keyed by
    (effort_magnitude_quintile, directional_response_quintile) -> stats dict
    for the given horizon_label ('5m' / '15m' / '30m'). Every one of the 25
    cells is always present in the result, even if empty (n=0, all stats
    None) -- callers must never assume a populated grid without checking n.
    """
    raw_key = f"future_{horizon_label}_raw_bps"
    signed_key = f"future_{horizon_label}_effort_signed"

    cells = _empty_cells()
    for row in rows:
        key = (row["effort_magnitude_quintile"], row["directional_response_quintile"])
        cells[key].append(row)

    result = {}
    for key, cell_rows in cells.items():
        n = len(cell_rows)
        if n == 0:
            result[key] = {
                "n": 0,
                "mean_raw_future_return_bps": None,
                "median_raw_future_return_bps": None,
                "mean_effort_signed_future_return_bps": None,
                "median_effort_signed_future_return_bps": None,
                "positive_effort_signed_rate": None,
            }
            continue
        raw_vals = [r[raw_key] for r in cell_rows]
        signed_vals = [r[signed_key] for r in cell_rows]
        positive_count = sum(1 for v in signed_vals if v > 0)
        result[key] = {
            "n": n,
            "mean_raw_future_return_bps": mean(raw_vals),
            "median_raw_future_return_bps": median(raw_vals),
            "mean_effort_signed_future_return_bps": mean(signed_vals),
            "median_effort_signed_future_return_bps": median(signed_vals),
            "positive_effort_signed_rate": positive_count / n,
        }
    return result


def greedy_non_overlap_select(rows: list, horizon_seconds: int) -> list:
    """Per Method Supplement greedy_non_overlap_layer. Sorts a COPY of `rows`
    by (decision_time_utc, record_id) ascending (never mutates the caller's
    list/order) and greedily selects rows so that no two selected rows'
    half-open [t, t+H) windows overlap. A row landing exactly on the prior
    selected window's end boundary IS eligible (half-open semantics)."""
    sorted_rows = sorted(rows, key=lambda r: (_parse_iso(r["decision_time_utc"]), r["record_id"]))
    selected = []
    last_time = None
    horizon_delta = timedelta(seconds=horizon_seconds)
    for row in sorted_rows:
        t = _parse_iso(row["decision_time_utc"])
        if last_time is None or t >= last_time + horizon_delta:
            selected.append(row)
            last_time = t
    return selected


def _percentile(sorted_vals: list, q: float) -> float:
    """Linear-interpolation percentile (same convention as the Runner's
    quantile_boundaries()). `sorted_vals` must already be sorted ascending."""
    n = len(sorted_vals)
    if n == 1:
        return sorted_vals[0]
    pos = q * (n - 1)
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return sorted_vals[lo]
    frac = pos - lo
    return sorted_vals[lo] + (sorted_vals[hi] - sorted_vals[lo]) * frac


def moving_block_bootstrap(rows: list, block_length_rows: int = 30, replicates: int = 2000,
                            confidence_level: float = 0.95, random_seed: int = 20260906,
                            min_valid_fraction: float = 0.95) -> dict:
    """Per Method Supplement moving_block_bootstrap_layer. `rows` is the FULL
    chronological eligible sample (this function sorts a copy by
    decision_time itself, so caller ordering does not matter). Returns a dict
    keyed by (effort_magnitude_quintile, directional_response_quintile) ->
    {horizon_label: {"valid_replicates": int,
                      "mean_raw_future_return_bps": {"ci_low", "ci_high",
                          "point_estimate_mean_of_replicates"} or the literal
                          string "BOOTSTRAP_INSUFFICIENT_VALID_REPLICATES",
                      "mean_effort_signed_future_return_bps": <same shape>}}.

    A replicate counts as "valid" for a cell if that replicate's resampled
    data contains at least one row landing in that cell. A cell with fewer
    than ceil(min_valid_fraction * replicates) valid replicates gets the
    literal insufficient-replicates string for both of its statistics
    instead of a CI computed from too few replicates.
    """
    horizons = [h for h, _ in HORIZONS_SECONDS]
    stats_names = ("mean_raw_future_return_bps", "mean_effort_signed_future_return_bps")

    sorted_rows = sorted(rows, key=lambda r: (_parse_iso(r["decision_time_utc"]), r["record_id"]))
    n = len(sorted_rows)

    result = {}
    if n == 0:
        for i in range(QUINTILE_COUNT):
            for j in range(QUINTILE_COUNT):
                result[(i, j)] = {h: {"valid_replicates": 0,
                                       "mean_raw_future_return_bps": "BOOTSTRAP_INSUFFICIENT_VALID_REPLICATES",
                                       "mean_effort_signed_future_return_bps": "BOOTSTRAP_INSUFFICIENT_VALID_REPLICATES"}
                                  for h in horizons}
        return result

    block_length = min(block_length_rows, n)
    max_start = n - block_length  # inclusive; N-L+1 possible block start positions

    rng = random.Random(random_seed)

    accum = {(i, j, h, s): [] for i in range(QUINTILE_COUNT) for j in range(QUINTILE_COUNT)
             for h in horizons for s in stats_names}
    valid_counts = {(i, j, h): 0 for i in range(QUINTILE_COUNT) for j in range(QUINTILE_COUNT) for h in horizons}

    for _ in range(replicates):
        resampled = []
        while len(resampled) < n:
            start = rng.randint(0, max_start)
            resampled.extend(sorted_rows[start:start + block_length])
        resampled = resampled[:n]

        cells = {}
        for row in resampled:
            key = (row["effort_magnitude_quintile"], row["directional_response_quintile"])
            cells.setdefault(key, []).append(row)

        for key, cell_rows in cells.items():
            for h in horizons:
                valid_counts[(key[0], key[1], h)] += 1
                raw_key = f"future_{h}_raw_bps"
                signed_key = f"future_{h}_effort_signed"
                accum[(key[0], key[1], h, "mean_raw_future_return_bps")].append(
                    mean(r[raw_key] for r in cell_rows))
                accum[(key[0], key[1], h, "mean_effort_signed_future_return_bps")].append(
                    mean(r[signed_key] for r in cell_rows))

    alpha = 1.0 - confidence_level
    lo_q, hi_q = alpha / 2.0, 1.0 - alpha / 2.0
    min_valid = math.ceil(min_valid_fraction * replicates)

    for i in range(QUINTILE_COUNT):
        for j in range(QUINTILE_COUNT):
            result[(i, j)] = {}
            for h in horizons:
                vcount = valid_counts[(i, j, h)]
                cell_result = {"valid_replicates": vcount}
                for s in stats_names:
                    if vcount < min_valid:
                        cell_result[s] = "BOOTSTRAP_INSUFFICIENT_VALID_REPLICATES"
                    else:
                        vals = sorted(accum[(i, j, h, s)])
                        cell_result[s] = {
                            "ci_low": _percentile(vals, lo_q),
                            "ci_high": _percentile(vals, hi_q),
                            "point_estimate_mean_of_replicates": mean(vals),
                        }
                result[(i, j)][h] = cell_result
    return result

