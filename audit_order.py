# -*- coding: utf-8 -*-

"""BTC HUNTER V1.2.3 clean-dataset audit.

Read-only audit for the current Shadow baseline. It does not modify logs and
it does not grant trade permission.
"""

import json
import sys
import hashlib
from pathlib import Path
from collections import Counter
from datetime import datetime, timezone

from EvidenceHunter_config import (
    ORDERFLOW_FILE_V2,
    OUTCOMES_FILE_V2,
    SHADOW_FILE_V2,
    get_dataset_id,
)

HORIZONS = {"5m": 300, "15m": 900, "30m": 1800}
REQUIRED_RECORD_FIELDS = (
    "record_id",
    "dataset_id",
    "schema_version",
    "feature_version",
    "collector_version",
    "outcome_version",
    "run_id",
    "timestamp",
    "exchange_event_time_ms",
    "collector_received_time_ms",
    "source_time_skew_ms",
    "source_event_spread_ms",
    "clock_offset_ms",
    "local_clock_ahead_ms",
    "clock_rtt_ms",
    "clock_sync_age_seconds",
    "clock_policy",
    "price_exchange_event_time_ms",
    "price_collector_received_time_ms",
    "observation_minute",
    "price",
    "market_regime",
    "market_regime_status",
    "signal_source",
    "research_direction",
    "data_quality_dimensions",
    "funding_interval_minutes",
    "time_to_funding_seconds",
    "trade_plan",
)


def parse_time(value):
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None


def load_jsonl(path):
    rows, bad_lines = [], 0
    if not path.exists():
        return rows, bad_lines
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            try:
                item = json.loads(line)
                if isinstance(item, dict):
                    rows.append(item)
                else:
                    bad_lines += 1
            except Exception:
                bad_lines += 1
    return rows, bad_lines


def main():
    dataset_id = get_dataset_id()
    if not dataset_id:
        print("AUDIT: FAIL - DATASET_NOT_INITIALIZED")
        return 2

    records_all, bad_record_lines = load_jsonl(SHADOW_FILE_V2)
    from EvidenceHunter_config import research_outcomes_path
    outcome_path = research_outcomes_path(dataset_id, OUTCOMES_FILE_V2)
    outcome_rows_all, bad_outcome_lines = load_jsonl(outcome_path)
    records = [r for r in records_all if r.get("dataset_id") == dataset_id]
    outcome_rows = [r for r in outcome_rows_all if r.get("dataset_id") == dataset_id]

    record_ids = [r.get("record_id") for r in records if r.get("record_id")]
    id_counts = Counter(record_ids)
    duplicate_ids = [rid for rid, count in id_counts.items() if count > 1]
    minute_counts = Counter(r.get("observation_minute") for r in records if r.get("observation_minute"))
    duplicate_minutes = [m for m, count in minute_counts.items() if count > 1]

    missing_fields = []
    wrong_dataset_rows = len(records_all) - len(records)
    for r in records:
        missing = [field for field in REQUIRED_RECORD_FIELDS if field not in r]
        if missing:
            missing_fields.append({"record_id": r.get("record_id"), "missing": missing})

    outcomes = {}
    orphan_outcomes = []
    for row in outcome_rows:
        rid = row.get("record_id")
        future = row.get("future_outcome")
        if rid and isinstance(future, dict):
            outcomes[rid] = future
            if rid not in id_counts:
                orphan_outcomes.append(rid)

    now = datetime.now(timezone.utc)
    mature_missing = []
    path_unavailable = []
    barrier_counts = {h: Counter() for h in HORIZONS}
    ambiguous = {h: 0 for h in HORIZONS}
    for r in records:
        rid = r.get("record_id")
        created = parse_time(r.get("timestamp"))
        if not rid or created is None:
            continue
        future = outcomes.get(rid, {})
        age = (now - created).total_seconds()
        for h, seconds in HORIZONS.items():
            if age < seconds + 5:
                continue
            if future.get("locked_" + h) is not True:
                mature_missing.append((rid, h, "NOT_LOCKED"))
                continue
            if future.get("path_status_" + h) != "OK":
                path_unavailable.append((rid, h, future.get("path_status_" + h)))
            result = future.get("result_" + h, "MISSING")
            barrier_counts[h][result] += 1
            if result == "AMBIGUOUS":
                ambiguous[h] += 1

    orderflow = {}
    if ORDERFLOW_FILE_V2.exists():
        try:
            orderflow = json.loads(ORDERFLOW_FILE_V2.read_text(encoding="utf-8"))
        except Exception:
            orderflow = {}

    critical = []
    if not records:
        critical.append("NO_SHADOW_RECORDS")
    if bad_record_lines:
        critical.append(f"BAD_SHADOW_JSON_LINES={bad_record_lines}")
    if bad_outcome_lines:
        critical.append(f"BAD_OUTCOME_JSON_LINES={bad_outcome_lines}")
    if duplicate_ids:
        critical.append(f"DUPLICATE_RECORD_ID={len(duplicate_ids)}")
    if duplicate_minutes:
        critical.append(f"DUPLICATE_OBSERVATION_MINUTE={len(duplicate_minutes)}")
    if missing_fields:
        critical.append(f"MISSING_REQUIRED_FIELDS={len(missing_fields)}")
    if orphan_outcomes:
        critical.append(f"ORPHAN_OUTCOMES={len(orphan_outcomes)}")
    if mature_missing:
        critical.append(f"MATURE_OUTCOME_MISSING={len(mature_missing)}")
    if path_unavailable:
        critical.append(f"PATH_UNAVAILABLE={len(path_unavailable)}")
    if orderflow.get("dataset_id") not in (None, dataset_id):
        critical.append("ORDERFLOW_DATASET_ID_MISMATCH")
    if orderflow.get("trade_gap") is True:
        critical.append("TRADE_GAP_DETECTED")

    print("=" * 80)
    print("BTC HUNTER V1.2.3 DATASET AUDIT")
    print("=" * 80)
    print("DATASET_ID                 :", dataset_id)
    print("SHADOW_RECORDS             :", len(records))
    print("OUTCOME_ROWS               :", len(outcome_rows))
    print("OLD/OTHER_DATASET_ROWS     :", wrong_dataset_rows)
    print("BAD_RECORD_LINES           :", bad_record_lines)
    print("BAD_OUTCOME_LINES          :", bad_outcome_lines)
    print("DUPLICATE_RECORD_IDS       :", len(duplicate_ids))
    print("DUPLICATE_MINUTES          :", len(duplicate_minutes))
    print("MISSING_FIELD_RECORDS      :", len(missing_fields))
    print("ORPHAN_OUTCOMES            :", len(orphan_outcomes))
    print("MATURE_OUTCOME_MISSING     :", len(mature_missing))
    print("PATH_UNAVAILABLE           :", len(path_unavailable))
    print("ORDERFLOW_TRADE_GAP        :", orderflow.get("trade_gap"))
    print("ORDERFLOW_ANALYSIS_STATUS  :", orderflow.get("analysis_status"))
    for h in HORIZONS:
        print(f"{h:>3} BARRIERS               :", dict(barrier_counts[h]))
        print(f"{h:>3} AMBIGUOUS              :", ambiguous[h])
    if wrong_dataset_rows:
        print("NOTE                       : Other dataset rows are ignored by V1.2.3 analysis.")

    if critical:
        print("-" * 80)
        print("AUDIT RESULT: FAIL")
        for item in critical:
            print(" -", item)
        return 2

    print("-" * 80)
    print("AUDIT RESULT: PASS")
    print("NOTE: PASS means baseline data-integrity checks passed; it does NOT prove EV/OOS/trade readiness.")
    return 0


def reconcile_project(registry_path, roots):
    """Bounded version/asset reconciliation. Does not certify independent review."""
    findings = []
    try:
        registry = json.loads(Path(registry_path).read_text(encoding="utf-8"))
        if registry.get("schema_version") != 1 or not registry.get("requirements") or not registry.get("assets"):
            raise ValueError("Registry needs nonempty requirements and assets")
        assets = registry["assets"]
        for requirement in registry["requirements"]:
            for asset_id in requirement["code_assets"]:
                if asset_id not in assets:
                    findings.append({"kind": "MISSING_EXPECTED_ASSET", "asset": asset_id})
            if requirement["implementation_status"] not in ("DOCUMENTED", "PLANNED", "TESTED"):
                findings.append({"kind": "UNSUPPORTED_STATUS_FOR_CURRENT_AUDIT_SCOPE", "requirement": requirement["requirement_id"]})
        for asset_id, asset in assets.items():
            root = Path(roots[asset["project"]]).resolve()
            path = (root / asset["path"]).resolve()
            if not path.is_relative_to(root):
                findings.append({"kind": "ASSET_OUTSIDE_PROJECT", "asset": asset_id})
                continue
            if not path.is_file():
                findings.append({"kind": "MISSING_EXPECTED_ASSET", "asset": asset_id})
            elif hashlib.sha256(path.read_bytes()).hexdigest() != asset["sha256"]:
                findings.append({"kind": "VERSION_MISMATCH", "asset": asset_id})
        registered = {r["requirement_id"] for r in registry["requirements"]}
        for asset_id, asset in assets.items():
            for requirement_id in asset["requirements"]:
                if requirement_id not in registered:
                    findings.append({"kind": "DEPENDENCY_MISMATCH", "asset": asset_id, "requirement": requirement_id})
        scope = registry["scope"]
    except (OSError, ValueError, KeyError, TypeError) as error:
        scope = "REGISTRY_UNAVAILABLE_OR_INVALID"
        findings.append({"kind": "INVALID_REGISTRY", "detail": str(error)})
    return {
        "scope": scope,
        "RECONCILIATION_STATUS": "FAIL" if findings else "PASS_FOR_REGISTERED_ASSET_VERSIONS_ONLY",
        "INDEPENDENT_REVIEW_STATUS": "PENDING",
        "RELEASE_READY": False,
        "findings": findings,
        "chinese_summary": "登记资产缺失或版本不一致，须修复后重新核对。" if findings else "本次登记资产的文件与哈希一致；已测试的修改仍待独立审核。此检查不代表全项目治理、运行状态或交易资格验收。",
        "not_checked": ["unregistered MASTER requirements", "live processes", "UI wiring", "all historical artifacts", "independent review authenticity"],
    }


def project_audit_cli():
    # Persisted console evidence uses UTF-8 on Windows as well as Linux.
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    root = Path(__file__).resolve().parent
    report = reconcile_project(root / "research" / "governance" / "IMPLEMENTATION_STATE_REGISTRY.json", {
        "formal": root,
        "v11r1": root.parent / "EvidenceHunter_V11R1_OUTCOME_RECORDER_CN",
    })
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 2 if report["RECONCILIATION_STATUS"] == "FAIL" else 0


if __name__ == "__main__":
    if sys.argv[1:] == ["--project-reconciliation"]:
        sys.exit(project_audit_cli())
    if sys.argv[1:]:
        raise SystemExit("Usage: audit_order.py [--project-reconciliation]")
    sys.exit(main())


