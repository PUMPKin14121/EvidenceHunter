# -*- coding: utf-8 -*-
"""
FIRST FORMAL DISCOVERY EXECUTION -- EFFORT_RESULT_DIVERGENCE_V1 (real machine).

Run this from the D:\\EvidenceHunter repo root (same directory this file
lives in) with the project's own virtualenv:

    .venv\\Scripts\\python.exe run_formal_discovery_execution_real_machine.py

This is the authoritative run: RUNNER_COMMIT below must match your current
git HEAD (97ad9321efa53b450fb9b96c0cbcc5f49c3ca9ac). It runs
run_discovery(dry_run=False) against the real Frozen Contract + Frozen
Dataset + real 8-day aggTrades archive, then builds all three
Contract-required dependence_controls views exactly per
EFFORT_RESULT_DIVERGENCE_V1_DISCOVERY_METHOD_SUPPLEMENT_V1.json. This same
script was already run once in the cloud sandbox against a byte-identical
mirror of these same frozen inputs (confirmed via SHA256 on every input file
this session) purely as an engineering validation pass -- this run on your
own machine, tied to your own git commit, is the one that becomes the
official, git-tracked first formal execution artifact.

Writes exactly three artifacts into this directory and REFUSES to run if
any of them already exist (first-formal-result-is-never-overwritten,
enforced in code):
  - <EXECUTION_ID>_RAW_ROWS.jsonl      (chronological raw descriptive rows)
  - <EXECUTION_ID>_SUMMARY.json        (grid summary + 3 non-overlap views + bootstrap)
  - <EXECUTION_ID>_PROVENANCE.json     (execution/provenance metadata + all hashes/counts)

On any exception, writes <EXECUTION_ID>_FAILED_PROVENANCE.json capturing
whatever was computed plus the traceback, and re-raises -- never quietly
patch code and rerun under the same execution identity; report back
instead.

DISCOVERY_RESULT != CONFIRMATION. TRADE_PERMISSION = FALSE. This script
computes and describes only; it makes no claim about the hypothesis being
confirmed, picks no best cell/horizon, and creates no trading signal.

Please relay the ENTIRE raw console output back exactly as printed so it
can be independently checked line by line and cross-verified byte-for-byte
against the sandbox validation run's own hashes.
"""
import hashlib
import json
import sys
import time
import traceback
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

import EvidenceHunter_archive as archive  # noqa: E402
import EvidenceHunter_discovery_runner_effort_result_divergence_v1 as runner  # noqa: E402
import EvidenceHunter_discovery_result_builder_effort_result_divergence_v1 as builder  # noqa: E402

EXECUTION_ID = "EFFORT_RESULT_DIVERGENCE_V1_DISCOVERY_EXECUTION_001"
RUNNER_COMMIT = "97ad9321efa53b450fb9b96c0cbcc5f49c3ca9ac"
METHOD_SUPPLEMENT_PATH = ROOT / "research_design_next" / "EFFORT_RESULT_DIVERGENCE_V1" / "EFFORT_RESULT_DIVERGENCE_V1_DISCOVERY_METHOD_SUPPLEMENT_V1.json"
CONTRACT_PATH = ROOT / "research_design_next" / "EFFORT_RESULT_DIVERGENCE_V1" / "EFFORT_RESULT_DIVERGENCE_V1_DISCOVERY_CONTRACT_V1.json"
DATASET_PATH = ROOT / "logs" / "v2" / "EvidenceHunter_shadow_records.jsonl"
ARCHIVE_DIR = ROOT / "runtime" / "binance_public_data_cache" / "futures_um" / "aggTrades" / "BTCUSDT"
OUT_DIR = ROOT

RAW_ROWS_PATH = OUT_DIR / f"{EXECUTION_ID}_RAW_ROWS.jsonl"
SUMMARY_PATH = OUT_DIR / f"{EXECUTION_ID}_SUMMARY.json"
PROVENANCE_PATH = OUT_DIR / f"{EXECUTION_ID}_PROVENANCE.json"
FAILED_PROVENANCE_PATH = OUT_DIR / f"{EXECUTION_ID}_FAILED_PROVENANCE.json"

HORIZON_LABELS = [h for h, _ in runner.HORIZONS_SECONDS]
HORIZON_SECONDS_BY_LABEL = dict(runner.HORIZONS_SECONDS)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _stringify_cell_keys(d: dict) -> dict:
    return {f"{i}_{j}": v for (i, j), v in d.items()}


def main():
    for p in (RAW_ROWS_PATH, SUMMARY_PATH, PROVENANCE_PATH):
        if p.exists():
            raise RuntimeError(f"REFUSING_TO_OVERWRITE_EXISTING_ARTIFACT: {p}")

    for p, label in [(CONTRACT_PATH, "CONTRACT_PATH"), (METHOD_SUPPLEMENT_PATH, "METHOD_SUPPLEMENT_PATH"),
                     (DATASET_PATH, "DATASET_PATH"), (ARCHIVE_DIR, "ARCHIVE_DIR")]:
        if not p.exists():
            raise RuntimeError(f"FATAL_MISSING_INPUT: {label}={p}")

    t0 = time.time()
    print(f"[{EXECUTION_ID}] STARTING first formal Discovery execution (dry_run=False, real machine)...", flush=True)

    contract_sha256 = _sha256_file(CONTRACT_PATH)
    method_supplement_sha256 = _sha256_file(METHOD_SUPPLEMENT_PATH)
    dataset_sha256 = _sha256_file(DATASET_PATH)
    archive_manifest_sha256 = _sha256_text(json.dumps(runner.EXPECTED_ARCHIVE_SHA256, sort_keys=True))

    print(f"  contract_sha256           = {contract_sha256}", flush=True)
    print(f"  method_supplement_sha256  = {method_supplement_sha256}", flush=True)
    print(f"  dataset_sha256            = {dataset_sha256}", flush=True)
    print(f"  archive_manifest_sha256   = {archive_manifest_sha256}", flush=True)
    print(f"  runner_commit (expected)  = {RUNNER_COMMIT}", flush=True)
    print(f"  >>> Please confirm this matches `git log -1 --format=%H` on this machine <<<", flush=True)

    last = {"t": t0}

    def progress(counts):
        now = time.time()
        print(f"  [{now - t0:8.1f}s | +{now - last['t']:5.1f}s] total={counts['total_records']} "
              f"eligible={counts['eligible_records']} ineligible={counts['ineligible_records']} "
              f"computed={counts['computed_count']}", flush=True)
        last["t"] = now

    result = runner.run_discovery(
        contract_path=CONTRACT_PATH,
        dataset_path=DATASET_PATH,
        archive_dir=ARCHIVE_DIR,
        fetch_fn=archive.get_archive_agg_trades_between,
        symbol="BTCUSDT",
        dry_run=False,
        progress_callback=progress,
        progress_every=500,
    )

    if not result.identity.passed:
        raise RuntimeError(f"IDENTITY_GATE_FAILED: {result.identity.errors}")

    print(f"[{EXECUTION_ID}] run_discovery done in {time.time()-t0:.1f}s. "
          f"total={result.total_records} eligible={result.eligible_records} "
          f"computed={result.computed_count} output_rows={len(result.output_rows)}", flush=True)

    chronological_rows = sorted(result.output_rows, key=lambda r: (r["decision_time_utc"], r["record_id"]))

    grid_summary_full_sample = {h: _stringify_cell_keys(builder.grid_summary(chronological_rows, h))
                                 for h in HORIZON_LABELS}

    greedy_non_overlap = {}
    for h in HORIZON_LABELS:
        seconds = HORIZON_SECONDS_BY_LABEL[h]
        selected = builder.greedy_non_overlap_select(chronological_rows, seconds)
        greedy_non_overlap[h] = {
            "horizon_seconds": seconds,
            "selected_count": len(selected),
            "grid_summary": _stringify_cell_keys(builder.grid_summary(selected, h)),
        }
    selected_counts = {h: greedy_non_overlap[h]["selected_count"] for h in HORIZON_LABELS}
    print(f"[{EXECUTION_ID}] greedy non-overlap selected counts: {selected_counts}", flush=True)

    print(f"[{EXECUTION_ID}] running moving-block bootstrap (block=30, replicates=2000)...", flush=True)
    tb0 = time.time()
    bootstrap_raw = builder.moving_block_bootstrap(
        chronological_rows, block_length_rows=30, replicates=2000,
        confidence_level=0.95, random_seed=20260906,
    )
    bootstrap = {f"{i}_{j}": v for (i, j), v in bootstrap_raw.items()}
    print(f"[{EXECUTION_ID}] bootstrap done in {time.time()-tb0:.1f}s", flush=True)

    generated_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    with RAW_ROWS_PATH.open("w", encoding="utf-8") as f:
        for row in chronological_rows:
            f.write(json.dumps(row, sort_keys=True) + "\n")
    raw_rows_sha256 = _sha256_file(RAW_ROWS_PATH)

    summary_doc = {
        "execution_id": EXECUTION_ID,
        "generated_at_utc": generated_at,
        "grid_summary_full_sample": grid_summary_full_sample,
        "greedy_non_overlap": greedy_non_overlap,
        "moving_block_bootstrap": {
            "params": {"block_length_rows": 30, "replicates": 2000, "confidence_level": 0.95,
                       "random_seed": 20260906, "interval_type": "percentile",
                       "source_sample": "full_chronological_eligible_sample"},
            "cells": bootstrap,
        },
    }
    SUMMARY_PATH.write_text(json.dumps(summary_doc, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    summary_sha256 = _sha256_file(SUMMARY_PATH)

    provenance_doc = {
        "execution_id": EXECUTION_ID,
        "runner_commit": RUNNER_COMMIT,
        "contract_id": runner.EXPECTED_CONTRACT_ID,
        "contract_sha256": contract_sha256,
        "method_supplement_sha256": method_supplement_sha256,
        "dataset_sha256": dataset_sha256,
        "archive_manifest_sha256": archive_manifest_sha256,
        "archive_sha256_per_day": dict(sorted(runner.EXPECTED_ARCHIVE_SHA256.items())),
        "identity_passed": result.identity.passed,
        "identity_errors": result.identity.errors,
        "total_records": result.total_records,
        "eligible_records": result.eligible_records,
        "ineligible_records": result.ineligible_records,
        "ineligible_reason_counts": result.ineligible_reason_counts,
        "computed_count": result.computed_count,
        "computation_failures": result.computation_failures,
        "raw_rows_artifact_path": str(RAW_ROWS_PATH.name),
        "raw_rows_artifact_sha256": raw_rows_sha256,
        "raw_rows_count": len(chronological_rows),
        "summary_artifact_path": str(SUMMARY_PATH.name),
        "summary_artifact_sha256": summary_sha256,
        "elapsed_seconds": round(time.time() - t0, 2),
        "generated_at_utc": generated_at,
        "status": "FIRST_FORMAL_DISCOVERY_EXECUTION_SUCCEEDED",
        "semantics": {
            "DISCOVERY_RESULT_NOT_EQUAL_CONFIRMATION": True,
            "TRADE_PERMISSION": False,
        },
    }
    PROVENANCE_PATH.write_text(json.dumps(provenance_doc, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    provenance_sha256 = _sha256_file(PROVENANCE_PATH)

    print(f"\n[{EXECUTION_ID}] DONE in {time.time()-t0:.1f}s total.", flush=True)
    print(json.dumps({
        "EXECUTION_ID": EXECUTION_ID,
        "RUNNER_COMMIT": RUNNER_COMMIT,
        "CONTRACT_HASH": contract_sha256,
        "METHOD_SUPPLEMENT_HASH": method_supplement_sha256,
        "DATASET_HASH": dataset_sha256,
        "ARCHIVE_MANIFEST_HASH": archive_manifest_sha256,
        "RAW_ROWS_ARTIFACT_PATH": str(RAW_ROWS_PATH.name),
        "RAW_ROWS_SHA256": raw_rows_sha256,
        "SUMMARY_ARTIFACT_PATH": str(SUMMARY_PATH.name),
        "SUMMARY_SHA256": summary_sha256,
        "PROVENANCE_ARTIFACT_PATH": str(PROVENANCE_PATH.name),
        "PROVENANCE_SHA256": provenance_sha256,
        "TOTAL_RECORDS": result.total_records,
        "ELIGIBLE_RECORDS": result.eligible_records,
        "COMPUTED_RECORDS": result.computed_count,
        "EXCLUSION_COUNTS": {"ineligible": result.ineligible_reason_counts,
                              "computation_failures": result.computation_failures},
    }, ensure_ascii=False, indent=2), flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        tb = traceback.format_exc()
        print("FATAL ERROR -- see traceback below. Writing FAILED provenance artifact.", flush=True)
        print(tb, flush=True)
        try:
            FAILED_PROVENANCE_PATH.write_text(json.dumps({
                "execution_id": EXECUTION_ID,
                "status": "FIRST_FORMAL_DISCOVERY_EXECUTION_FAILED",
                "generated_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "traceback": tb,
            }, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            pass
        sys.exit(1)


