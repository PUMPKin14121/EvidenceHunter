# -*- coding: utf-8 -*-

"""BTC HUNTER Shadow Analysis V2.1 - descriptive only.

Uses non-overlap plus moving-block bootstrap. This is still NOT purged OOS,
not a model validation, and not trade EV.
"""

import json
import random
from EvidenceHunter_config import research_outcomes_path
from datetime import datetime, timezone
from statistics import mean, median

from EvidenceHunter_config import (
    ANALYSIS_FILE_V2,
    BLOCK_BOOTSTRAP_BLOCK_SIZE,
    BLOCK_BOOTSTRAP_ROUNDS,
    BLOCK_BOOTSTRAP_SEED,
    OUTCOMES_FILE_V2,
    SHADOW_FILE_V2,
    ensure_directories,
    get_dataset_id,
)

INPUT_FILE = SHADOW_FILE_V2
OUTCOMES_FILE = OUTCOMES_FILE_V2
OUTPUT_FILE = ANALYSIS_FILE_V2
HORIZONS = {"5m": 300, "15m": 900, "30m": 1800}


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def parse_time(value):
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None


def load_records(dataset_id):
    outcome_path = research_outcomes_path(dataset_id, OUTCOMES_FILE)
    if not INPUT_FILE.exists():
        return []
    records = []
    with INPUT_FILE.open("r", encoding="utf-8") as f:
        for line in f:
            try:
                row = json.loads(line)
                if not isinstance(row, dict) or row.get("dataset_id") != dataset_id:
                    continue
                dt = parse_time(row.get("timestamp"))
                if dt:
                    row["_parsed_time"] = dt
                    records.append(row)
            except Exception:
                continue

    if outcome_path.exists():
        outcomes = {}
        with outcome_path.open("r", encoding="utf-8") as f:
            for line in f:
                try:
                    item = json.loads(line)
                    if item.get("dataset_id") != dataset_id:
                        continue
                    rid, future = item.get("record_id"), item.get("future_outcome")
                    if rid and isinstance(future, dict):
                        outcomes[rid] = future
                except Exception:
                    continue
        for rec in records:
            if rec.get("record_id") in outcomes:
                rec["future_outcome"] = outcomes[rec["record_id"]]

    records.sort(key=lambda x: x["_parsed_time"])
    return records


def get_group(record):
    state = (record.get("observation") or {}).get("orderflow_state")
    return state if state in ("CONTRADICTION", "NO_CONTRADICTION") else "UNKNOWN"


def get_outcome(record, direction, horizon):
    future = record.get("future_outcome") or {}
    if future.get("locked_" + horizon) is not True:
        return None
    if future.get("path_status_" + horizon) != "OK":
        return None
    key = f"{direction}_return_{horizon}"
    try:
        value = future.get(key)
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def select_non_overlapping(records, seconds):
    selected, last_time = [], None
    for record in records:
        current = record.get("_parsed_time")
        if current is None:
            continue
        if last_time is None or (current - last_time).total_seconds() >= seconds:
            selected.append(record)
            last_time = current
    return selected


def moving_block_bootstrap_mean_ci(values, block_size=BLOCK_BOOTSTRAP_BLOCK_SIZE):
    values = [float(v) for v in values if v is not None]
    n = len(values)
    if n < max(10, block_size * 2):
        return None
    block_size = max(2, min(int(block_size), n))
    blocks = [values[i:i + block_size] for i in range(0, n - block_size + 1)]
    if not blocks:
        return None
    rng = random.Random(BLOCK_BOOTSTRAP_SEED + n + block_size)
    boot_means = []
    for _ in range(BLOCK_BOOTSTRAP_ROUNDS):
        sample = []
        while len(sample) < n:
            sample.extend(blocks[rng.randrange(len(blocks))])
        boot_means.append(mean(sample[:n]))
    boot_means.sort()
    lo = int(BLOCK_BOOTSTRAP_ROUNDS * 0.025)
    hi = min(BLOCK_BOOTSTRAP_ROUNDS - 1, int(BLOCK_BOOTSTRAP_ROUNDS * 0.975))
    return {
        "lower": round(boot_means[lo], 8),
        "upper": round(boot_means[hi], 8),
        "method": "MOVING_BLOCK_BOOTSTRAP",
        "block_size": block_size,
        "rounds": BLOCK_BOOTSTRAP_ROUNDS,
    }


def calculate_stats(values):
    values = [v for v in values if v is not None]
    if not values:
        return {
            "samples": 0,
            "average": None,
            "median": None,
            "positive_rate": None,
            "positive_count": 0,
            "negative_count": 0,
            "bootstrap_ci95": None,
        }
    positive = [v for v in values if v > 0]
    negative = [v for v in values if v < 0]
    return {
        "samples": len(values),
        "average": round(mean(values), 8),
        "median": round(median(values), 8),
        "positive_rate": round(len(positive) / len(values) * 100, 4),
        "positive_count": len(positive),
        "negative_count": len(negative),
        "bootstrap_ci95": moving_block_bootstrap_mean_ci(values),
    }


def barrier_counts(records, horizon):
    result = {"TP_FIRST": 0, "STOP_FIRST": 0, "TIMEOUT": 0, "AMBIGUOUS": 0, "NO_PLAN": 0, "OTHER": 0}
    for r in records:
        value = (r.get("future_outcome") or {}).get("result_" + horizon)
        if value in result:
            result[value] += 1
        elif value is not None:
            result["OTHER"] += 1
    return result


def analyze_group(records, horizon):
    result = {}
    for group_name in ["ALL", "CONTRADICTION", "NO_CONTRADICTION", "UNKNOWN"]:
        group_records = [r for r in records if group_name == "ALL" or get_group(r) == group_name]
        long_values = [get_outcome(r, "long", horizon) for r in group_records]
        short_values = [get_outcome(r, "short", horizon) for r in group_records]
        result[group_name] = {
            "record_count": len(group_records),
            "long": calculate_stats(long_values),
            "short": calculate_stats(short_values),
            "barrier_counts": barrier_counts(group_records, horizon),
        }
    return result


def build_analysis(records, dataset_id):
    result = {
        "version": "SHADOW_ANALYSIS_V2.1",
        "created": utc_now(),
        "dataset_id": dataset_id,
        "mode": "DESCRIPTIVE_ONLY",
        "ev_status": "NOT_CALIBRATED",
        "validation_status": "NOT_OOS",
        "input_files": {"records": str(INPUT_FILE), "outcomes": str(research_outcomes_path(dataset_id, OUTCOMES_FILE))},
        "total_records": len(records),
        "horizons": {},
        "warnings": [
            "REST订单流仍然是近似数据",
            "当前统计不是Economic EV",
            "当前不是Purged/Embargoed Walk-forward OOS",
            "Moving-block bootstrap仅用于描述性不确定性，不是正式模型验证",
            "market_regime字段当前仍是UNVALIDATED_HEURISTIC",
        ],
    }
    for horizon, seconds in HORIZONS.items():
        usable = [r for r in records if get_outcome(r, "long", horizon) is not None]
        selected = select_non_overlapping(usable, seconds)
        result["horizons"][horizon] = {
            "window_seconds": seconds,
            "raw_completed_records": len(usable),
            "non_overlapping_records": len(selected),
            "groups": analyze_group(selected, horizon),
        }
    return result


def save_analysis(result):
    ensure_directories()
    temp = OUTPUT_FILE.with_suffix(OUTPUT_FILE.suffix + ".tmp")
    with temp.open("w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    temp.replace(OUTPUT_FILE)


def main():
    dataset_id = get_dataset_id()
    if not dataset_id:
        raise RuntimeError("DATASET_NOT_INITIALIZED")
    records = load_records(dataset_id)
    result = build_analysis(records, dataset_id)
    save_analysis(result)
    print("=" * 72)
    print("BTC AI HUNTER V1.2.2 | SHADOW ANALYSIS V2.1")
    print("=" * 72)
    print("DATASET_ID          :", dataset_id)
    print("TOTAL RECORDS       :", len(records))
    print("MODE                : DESCRIPTIVE_ONLY")
    print("BOOTSTRAP           : MOVING_BLOCK_BOOTSTRAP")
    print("OOS                 : NOT YET")
    print("FILE                :", OUTPUT_FILE)


if __name__ == "__main__":
    main()

