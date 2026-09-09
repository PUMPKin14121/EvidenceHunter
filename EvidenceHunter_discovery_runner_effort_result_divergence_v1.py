# -*- coding: utf-8 -*-
"""
Discovery Runner for EFFORT_RESULT_DIVERGENCE_V1.

Implements ONLY what is specified in the Frozen Discovery Contract
research_design_next/EFFORT_RESULT_DIVERGENCE_V1/EFFORT_RESULT_DIVERGENCE_V1_DISCOVERY_CONTRACT_V1.json
(contract_id EFFORT_RESULT_DIVERGENCE_V1_DISCOVERY_CONTRACT_V1). This module does not
reinterpret, extend, or redesign the research protocol; every computation below cites the
exact Contract field it implements in a comment.

GOVERNANCE (do not remove or weaken any of this):
  - IMPLEMENTED != VERIFIED. READY != DISCOVERY_EXECUTED. DISCOVERY_RESULT != CONFIRMATION.
  - This file may be imported and its dry-run mode exercised for schema/identity/determinism
    checks. Real execution (dry_run=False) against the Frozen Dataset requires explicit
    PROJECT_OWNER/GPT authorization recorded in the living document; this file does not
    grant that authorization by existing.
  - fail-closed identity gate: if the Contract, the Dataset, or any required archive input
    does not match its recorded hash, this module refuses to compute anything and returns/
    raises an identity-failure result instead.
  - Never modifies the Frozen Contract file, the Frozen Dataset file, or any archive input
    file. Reads them only.
  - Never selects a "best" horizon, subgroup, or threshold. Always reports all three
    pre-registered horizons (5m/15m/30m) and never conditions eligibility or binning on
    market_regime, depth100, depth1000, position_side, trade_plan, funding, or day/session
    (Contract discovery_protocol.no_primary_subgroup_selection_by).
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# Frozen identity constants. These are hardcoded on purpose: the whole point of
# the fail-closed identity gate is that it does not trust any runtime-supplied
# "this is the right contract/dataset" claim. If the Contract or Dataset is
# ever regenerated, these constants must be updated in a reviewed changeset,
# not silently overridden by a config file or CLI flag.
# ---------------------------------------------------------------------------

EXPECTED_CONTRACT_ID = "EFFORT_RESULT_DIVERGENCE_V1_DISCOVERY_CONTRACT_V1"
EXPECTED_CONTRACT_SHA256 = "15ddd4b2a7d9fb8a88711ee73940a140e3ff6b409da54a643aadd9c329de453c"

EXPECTED_DATASET_ID = "BTCV12P3_20260828T000323Z_75696678"
EXPECTED_DATASET_SHA256 = "571c497ab0e251e1e88b15bc9b6a2ca832a863b9c9d3ac69fb9ea175053de75d"

# aggTrades archive SHA256 per required UTC day, independently verified in
# ARCHIVE_INPUT_MANIFEST_EFFORT_RESULT_DIVERGENCE_V1.json (2026-09-06).
# Contract data_lineage requires ONLY the aggTrades archive (klines archive is
# not referenced anywhere in the Contract text; see living-document '2-BC').
EXPECTED_ARCHIVE_SHA256 = {
    "2026-08-28": "918af94802638af0a0d5fd826c18d827c2e09da69e8521badd9af38df90d6e27",
    "2026-08-29": "0b0d0c729e027aeaa28762f2e0eea7fd99d17722337e249e10b7cce87314de19",
    "2026-08-30": "c342e2f94b9a17b8a48205dd35e297d346bf067c2c1c0a9d90b2b38dfcd9b99e",
    "2026-08-31": "454a1448bc2d8fa54e8372a3454d8aff1505ccec981c7c9e371333697ac0cc46",
    "2026-09-01": "8ee2432189e091f8df2181a7bf894f6458cf4c24ffcb0de904e7bbb80f0245ba",
    "2026-09-02": "e0c1ebeac376aca642c091f389bc9316c2cfc006606f4d9528c66ea17e4cd6d1",
    "2026-09-03": "37f4135ac25f6cce4d82d38f577ccc9803619b7516d574a61afaa9db856254ca",
    "2026-09-04": "9ec137c60c6bfce0473dac8a183e96ffae1db90b8c3fe829cf8ca2f8b2e5ee42",
}

# Contract future_targets.horizons_seconds = [300, 900, 1800]. Order is fixed
# and must never be re-sorted "by interestingness" -- Contract horizon_rule
# forbids retroactively declaring a best horizon.
HORIZONS_SECONDS = [("5m", 300), ("15m", 900), ("30m", 1800)]
MAX_HORIZON_SECONDS = 1800

# Contract timing_contract: estimated_effort_window end/start.
EFFORT_WINDOW_SECONDS = 60

# Contract discovery_protocol.fixed_binning: quintiles, 5x5 grid.
QUINTILE_COUNT = 5


class IdentityGateFailure(RuntimeError):
    """Raised when the fail-closed identity gate does not pass. Never caught
    silently -- callers must treat this as a hard stop, not a warning."""


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _parse_iso(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


@dataclass
class IdentityCheckResult:
    passed: bool
    contract_sha256: Optional[str] = None
    dataset_sha256: Optional[str] = None
    archive_sha256: dict = field(default_factory=dict)
    errors: list = field(default_factory=list)


def verify_identity(contract_path: Path, dataset_path: Path, archive_dir: Path,
                     symbol: str = "BTCUSDT") -> IdentityCheckResult:
    """Fail-closed identity gate. Returns a result whose .passed is False if
    ANY of: contract hash, contract_id field, dataset hash, or any of the 8
    required archive-day hashes do not match the frozen expected values.
    Nothing downstream of this function may run unless .passed is True.
    """
    errors = []
    contract_sha = None
    dataset_sha = None
    archive_sha = {}

    if not contract_path.exists():
        errors.append(f"CONTRACT_FILE_MISSING:{contract_path}")
    else:
        contract_bytes = contract_path.read_bytes()
        contract_sha = _sha256_bytes(contract_bytes)
        if contract_sha != EXPECTED_CONTRACT_SHA256:
            errors.append(
                f"CONTRACT_HASH_MISMATCH:expected={EXPECTED_CONTRACT_SHA256}:actual={contract_sha}"
            )
        try:
            contract = json.loads(contract_bytes)
            if contract.get("contract_id") != EXPECTED_CONTRACT_ID:
                errors.append(
                    f"CONTRACT_ID_MISMATCH:expected={EXPECTED_CONTRACT_ID}:actual={contract.get('contract_id')}"
                )
        except Exception as exc:
            errors.append(f"CONTRACT_NOT_VALID_JSON:{exc}")

    if not dataset_path.exists():
        errors.append(f"DATASET_FILE_MISSING:{dataset_path}")
    else:
        dataset_sha = _sha256_file(dataset_path)
        if dataset_sha != EXPECTED_DATASET_SHA256:
            errors.append(
                f"DATASET_HASH_MISMATCH:expected={EXPECTED_DATASET_SHA256}:actual={dataset_sha}"
            )

    for day, expected_hash in sorted(EXPECTED_ARCHIVE_SHA256.items()):
        zip_path = archive_dir / f"{symbol.upper()}-aggTrades-{day}.zip"
        if not zip_path.exists():
            errors.append(f"ARCHIVE_FILE_MISSING:{day}")
            continue
        actual_hash = _sha256_file(zip_path)
        archive_sha[day] = actual_hash
        if actual_hash != expected_hash:
            errors.append(
                f"ARCHIVE_HASH_MISMATCH:{day}:expected={expected_hash}:actual={actual_hash}"
            )

    return IdentityCheckResult(
        passed=(len(errors) == 0),
        contract_sha256=contract_sha,
        dataset_sha256=dataset_sha,
        archive_sha256=archive_sha,
        errors=errors,
    )


# ---------------------------------------------------------------------------
# Eligibility (Contract "eligibility" block, every key implemented explicitly)
# ---------------------------------------------------------------------------

def _days_between(start: datetime, end: datetime) -> set:
    days = set()
    d = start.date()
    last = end.date()
    while d <= last:
        days.add(d.isoformat())
        d = d + timedelta(days=1)
    return days


@dataclass
class EligibilityResult:
    eligible: bool
    reasons: list
    decision_time: Optional[datetime] = None
    window_start: Optional[datetime] = None
    window_end: Optional[datetime] = None
    required_days: set = field(default_factory=set)


def check_eligibility(record: dict, dataset_id: str, covered_days: set) -> EligibilityResult:
    """Implements Contract 'eligibility' block exactly:
      require_same_dataset_id
      require_data_quality_freshness_pass
      require_data_quality_completeness_pass
      require_data_quality_continuity_pass
      require_data_quality_validity_pass
      require_trade_gap_false
      require_orderflow_ready
      require_total_effort_usdt_gt_zero
      require_valid_feature_timestamp
      require_archive_coverage_for_contemporaneous_window_and_target
    market_regime_primary_gate=false and depth_primary_gate=false mean those
    fields are NEVER used as an eligibility or selection criterion here.
    """
    reasons = []

    if record.get("dataset_id") != dataset_id:
        reasons.append("DATASET_ID_MISMATCH")

    dq = record.get("data_quality_dimensions") or {}
    if (dq.get("freshness") or {}).get("pass") is not True:
        reasons.append("FRESHNESS_FAIL")
    if (dq.get("completeness") or {}).get("pass") is not True:
        reasons.append("COMPLETENESS_FAIL")
    if (dq.get("continuity") or {}).get("pass") is not True:
        reasons.append("CONTINUITY_FAIL")
    if (dq.get("continuity") or {}).get("trade_gap") is not False:
        reasons.append("TRADE_GAP_TRUE")
    if (dq.get("validity") or {}).get("pass") is not True:
        reasons.append("VALIDITY_FAIL")

    features = record.get("features") or {}
    orderflow = features.get("orderflow") or {}
    if orderflow.get("ready") is not True:
        reasons.append("ORDERFLOW_NOT_READY")

    buy = orderflow.get("aggressive_buy_usdt")
    sell = orderflow.get("aggressive_sell_usdt")
    total_effort = (buy or 0) + (sell or 0)
    if not (total_effort > 0):
        reasons.append("TOTAL_EFFORT_NOT_POSITIVE")

    decision_time = _parse_iso(features.get("timestamp"))
    window_start = window_end = None
    required_days: set = set()
    if decision_time is None:
        reasons.append("INVALID_FEATURE_TIMESTAMP")
    else:
        age = (dq.get("freshness") or {}).get("orderflow_age_seconds")
        try:
            age = float(age)
        except Exception:
            age = 0.0
        window_end = decision_time - timedelta(seconds=age)
        window_start = window_end - timedelta(seconds=EFFORT_WINDOW_SECONDS)
        target_end = decision_time + timedelta(seconds=MAX_HORIZON_SECONDS)
        required_days = _days_between(window_start, target_end)
        if required_days - covered_days:
            reasons.append("ARCHIVE_COVERAGE_MISSING")

    return EligibilityResult(
        eligible=(len(reasons) == 0),
        reasons=reasons,
        decision_time=decision_time,
        window_start=window_start,
        window_end=window_end,
        required_days=required_days,
    )


# ---------------------------------------------------------------------------
# Price reconstruction from the Binance aggTrades archive.
# Delegates to the already-audited EvidenceHunter_archive.get_archive_agg_trades_between
# rather than re-implementing archive parsing (minimizes changeset risk).
# ---------------------------------------------------------------------------

class PriceLookupFailure(RuntimeError):
    """A specific record's price reconstruction could not be completed (e.g.
    no trade found in the required window). This is a per-record computation
    failure, tracked separately from Contract eligibility -- eligibility only
    guarantees archive DAY coverage exists, not that a trade exists in every
    specific sub-window."""


@dataclass
class ArchiveReader:
    """Thin wrapper around EvidenceHunter_archive.get_archive_agg_trades_between.
    Constructed once per run and reused for every record."""
    symbol: str
    cache_dir: Path
    fetch_fn: object  # callable(symbol, start_ms, end_ms, cache_dir, require_checksum) -> list[dict]
    max_lookahead_ms: int = 5 * 60 * 1000  # widen search window up to 5 minutes if needed

    @staticmethod
    def _clean(trades: list) -> list:
        """Defensively de-duplicate by trade_id and sort by (time, trade_id).
        Never trust an upstream source (even an already-audited one) to hand
        back perfectly sorted/deduped rows without checking -- this is what
        makes the reader robust to duplicate/reordered archive rows rather
        than merely assuming the delegate always behaves."""
        if not trades:
            return trades
        unique = {t["trade_id"]: t for t in trades}
        return sorted(unique.values(), key=lambda t: (t["time"], t["trade_id"]))

    def price_at_or_after(self, ts_ms: int) -> float:
        window = 5000
        while window <= self.max_lookahead_ms:
            trades = self._clean(self.fetch_fn(self.symbol, ts_ms, ts_ms + window, self.cache_dir, True))
            if trades:
                return float(trades[0]["price"])
            window *= 4
        raise PriceLookupFailure(f"NO_TRADE_AT_OR_AFTER:{ts_ms}")

    def price_at_or_before(self, ts_ms: int) -> float:
        window = 5000
        while window <= self.max_lookahead_ms:
            trades = self._clean(self.fetch_fn(self.symbol, ts_ms - window, ts_ms, self.cache_dir, True))
            if trades:
                return float(trades[-1]["price"])
            window *= 4
        raise PriceLookupFailure(f"NO_TRADE_AT_OR_BEFORE:{ts_ms}")


def _ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


@dataclass
class RecordComputation:
    record_id: str
    decision_time: datetime
    signed_effort: float
    effort_magnitude: float
    total_effort_usdt: float
    p_start: float
    p_end: float
    return_60s_bps: float
    directional_response: float
    future: dict  # horizon_label -> {"raw_future_return_bps": float, "effort_signed_future_return": float}


def compute_record(record: dict, eligibility: EligibilityResult, reader: ArchiveReader) -> RecordComputation:
    """Implements Contract 'confirmed_effort_fields', 'contemporaneous_result',
    and 'future_targets' blocks exactly. Raises PriceLookupFailure if any
    required trade cannot be located -- callers must treat this as a
    per-record exclusion, not a crash of the whole run."""
    orderflow = record["features"]["orderflow"]
    buy = float(orderflow["aggressive_buy_usdt"])
    sell = float(orderflow["aggressive_sell_usdt"])
    total_effort_usdt = buy + sell
    # signed_effort_formula: (buy_usdt - sell_usdt) / (buy_usdt + sell_usdt)
    signed_effort = (buy - sell) / total_effort_usdt
    effort_magnitude = abs(signed_effort)
    sign = 1.0 if signed_effort >= 0 else -1.0

    decision_time = eligibility.decision_time
    window_start_ms = _ms(eligibility.window_start)
    window_end_ms = _ms(eligibility.window_end)

    # contemporaneous_result: p_start = first trade at/after window_start,
    # p_end = last trade at/before window_end.
    p_start = reader.price_at_or_after(window_start_ms)
    p_end = reader.price_at_or_before(window_end_ms)
    return_60s_bps = 10000.0 * math.log(p_end / p_start)
    directional_response = sign * return_60s_bps

    # future_targets: origin = decision_time = features.timestamp
    decision_ms = _ms(decision_time)
    price_at_decision = reader.price_at_or_after(decision_ms)

    future = {}
    for label, seconds in HORIZONS_SECONDS:
        target_ms = decision_ms + seconds * 1000
        price_at_target = reader.price_at_or_after(target_ms)
        raw_future_return_bps = 10000.0 * math.log(price_at_target / price_at_decision)
        effort_signed_future_return = sign * raw_future_return_bps
        future[label] = {
            "raw_future_return_bps": raw_future_return_bps,
            "effort_signed_future_return": effort_signed_future_return,
        }

    return RecordComputation(
        record_id=record.get("record_id"),
        decision_time=decision_time,
        signed_effort=signed_effort,
        effort_magnitude=effort_magnitude,
        total_effort_usdt=total_effort_usdt,
        p_start=p_start,
        p_end=p_end,
        return_60s_bps=return_60s_bps,
        directional_response=directional_response,
        future=future,
    )


# ---------------------------------------------------------------------------
# Quintile binning (Contract discovery_protocol.fixed_binning), deterministic
# tie-handling: boundaries computed once from the full eligible-and-computed
# sample, ties at a boundary are assigned to the LOWER bin, consistently.
# ---------------------------------------------------------------------------

def quantile_boundaries(values: list, n_bins: int = QUINTILE_COUNT) -> list:
    """Returns n_bins-1 boundary values splitting `values` into n_bins
    equal-count groups (linear-interpolation quantile method, same convention
    as numpy's default 'linear' method), computed on a SORTED COPY so the
    caller's ordering is never mutated."""
    if not values:
        return []
    s = sorted(values)
    n = len(s)
    boundaries = []
    for k in range(1, n_bins):
        q = k / n_bins
        pos = q * (n - 1)
        lo = int(math.floor(pos))
        hi = int(math.ceil(pos))
        if lo == hi:
            boundaries.append(s[lo])
        else:
            frac = pos - lo
            boundaries.append(s[lo] + (s[hi] - s[lo]) * frac)
    return boundaries


def assign_bin(value: float, boundaries: list) -> int:
    """0-indexed bin assignment. A value exactly equal to a boundary is
    assigned to the LOWER bin (bisect_left semantics) -- fixed, deterministic,
    documented tie rule."""
    import bisect
    return bisect.bisect_left(boundaries, value)


# ---------------------------------------------------------------------------
# Top-level orchestration
# ---------------------------------------------------------------------------

@dataclass
class RunResult:
    identity: IdentityCheckResult
    total_records: int = 0
    eligible_records: int = 0
    ineligible_records: int = 0
    ineligible_reason_counts: dict = field(default_factory=dict)
    computation_failures: dict = field(default_factory=dict)  # reason -> count
    computed_count: int = 0
    output_rows: list = field(default_factory=list)  # only populated when dry_run=False
    output_schema: list = field(default_factory=list)
    generated_at_utc: str = ""


def _bump(d: dict, k: str):
    d[k] = d.get(k, 0) + 1


def run_discovery(contract_path: Path, dataset_path: Path, archive_dir: Path,
                   fetch_fn, symbol: str = "BTCUSDT", dry_run: bool = True,
                   progress_callback=None, progress_every: int = 500) -> RunResult:
    """Full Discovery Runner entrypoint.

    dry_run=True (the only mode this session will ever invoke against the
    real Frozen Dataset): runs the complete identity gate + eligibility +
    price-reconstruction pipeline, but output_rows is left EMPTY and no
    per-record numeric result (bps, effort, bin assignment) is retained
    anywhere in the returned object -- only counts and the output schema
    (column names) are populated. This is what "the dry-run must not emit
    real research statistics" means in code, not just in prose.

    dry_run=False: additionally populates output_rows with the full
    per-record computation and the 5x5 quintile grid. Requires separate,
    explicit authorization before being invoked against the real dataset;
    this function does not check for that authorization itself -- the
    authorization gate is procedural (this living document + GPT review),
    not something this code can enforce on its own.
    """
    identity = verify_identity(contract_path, dataset_path, archive_dir, symbol=symbol)
    result = RunResult(identity=identity, generated_at_utc=datetime.now(timezone.utc).isoformat())
    if not identity.passed:
        return result

    dataset_id = EXPECTED_DATASET_ID
    covered_days = set(EXPECTED_ARCHIVE_SHA256.keys())
    reader = ArchiveReader(symbol=symbol, cache_dir=archive_dir, fetch_fn=fetch_fn)

    computations = []
    with open(dataset_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except Exception:
                continue
            if record.get("dataset_id") != dataset_id:
                continue
            result.total_records += 1

            elig = check_eligibility(record, dataset_id, covered_days)
            if not elig.eligible:
                result.ineligible_records += 1
                for r in set(elig.reasons):
                    _bump(result.ineligible_reason_counts, r)
            else:
                result.eligible_records += 1
                try:
                    comp = compute_record(record, elig, reader)
                except PriceLookupFailure as exc:
                    _bump(result.computation_failures, str(exc).split(":")[0])
                else:
                    result.computed_count += 1
                    computations.append(comp)

            if progress_callback is not None and result.total_records % progress_every == 0:
                # Progress is purely operational (counts only, same fields as
                # dry_run_summary) -- never exposes a per-record or aggregate
                # research statistic, so it is safe to call in either dry_run
                # mode without weakening the "dry-run must not emit real
                # research statistics" governance rule above. Fires on every
                # Nth record REACHED (not every Nth successfully computed
                # one), so progress keeps advancing even through a run with
                # many ineligible/failed records instead of stalling.
                progress_callback(dict(
                    total_records=result.total_records,
                    eligible_records=result.eligible_records,
                    ineligible_records=result.ineligible_records,
                    computed_count=result.computed_count,
                ))

    result.output_schema = [
        "record_id", "decision_time_utc", "signed_effort", "effort_magnitude",
        "total_effort_usdt", "p_start", "p_end", "return_60s_bps",
        "directional_response", "effort_magnitude_quintile", "directional_response_quintile",
    ] + [f"future_{label}_raw_bps" for label, _ in HORIZONS_SECONDS] + \
        [f"future_{label}_effort_signed" for label, _ in HORIZONS_SECONDS]

    if not dry_run and computations:
        magnitudes = [c.effort_magnitude for c in computations]
        responses = [c.directional_response for c in computations]
        mag_bounds = quantile_boundaries(magnitudes)
        resp_bounds = quantile_boundaries(responses)
        for c in computations:
            row = {
                "record_id": c.record_id,
                "decision_time_utc": c.decision_time.isoformat(),
                "signed_effort": c.signed_effort,
                "effort_magnitude": c.effort_magnitude,
                "total_effort_usdt": c.total_effort_usdt,
                "p_start": c.p_start,
                "p_end": c.p_end,
                "return_60s_bps": c.return_60s_bps,
                "directional_response": c.directional_response,
                "effort_magnitude_quintile": assign_bin(c.effort_magnitude, mag_bounds),
                "directional_response_quintile": assign_bin(c.directional_response, resp_bounds),
            }
            for label, _ in HORIZONS_SECONDS:
                row[f"future_{label}_raw_bps"] = c.future[label]["raw_future_return_bps"]
                row[f"future_{label}_effort_signed"] = c.future[label]["effort_signed_future_return"]
            result.output_rows.append(row)

    return result


def dry_run_summary(result: RunResult) -> dict:
    """Schema/count-only summary safe to print/log/hand to GPT. Contains no
    per-record or aggregate research statistic (no bps values, no bin edges,
    no 5x5 cell contents)."""
    return {
        "identity_passed": result.identity.passed,
        "identity_errors": result.identity.errors,
        "contract_sha256": result.identity.contract_sha256,
        "dataset_sha256": result.identity.dataset_sha256,
        "archive_sha256": result.identity.archive_sha256,
        "total_records": result.total_records,
        "eligible_records": result.eligible_records,
        "ineligible_records": result.ineligible_records,
        "ineligible_reason_counts": result.ineligible_reason_counts,
        "computation_failures": result.computation_failures,
        "computed_count": result.computed_count,
        "output_schema": result.output_schema,
        "generated_at_utc": result.generated_at_utc,
    }

