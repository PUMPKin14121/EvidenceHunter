# -*- coding: utf-8 -*-

"""Prepare a clean BTC HUNTER dataset session safely.

Default command is status-only. Destructive cleanup requires:
    python EvidenceHunter_dataset.py prepare --yes

The prepare command archives logs/v2 and runtime/v2 into separate subfolders,
then clears those V2 working folders and creates a new dataset_session.json.
"""

import argparse
import json
import shutil
import uuid
from EvidenceHunter_config import assert_dataset_writable, get_dataset_id
from datetime import datetime, timezone
from pathlib import Path

from EvidenceHunter_config import (
    ARCHIVE_DIR,
    COLLECTOR_VERSION,
    DATASET_SESSION_FILE,
    FEATURE_VERSION,
    LOG_DIR,
    OUTCOME_VERSION,
    RUNTIME_DIR,
    SCHEMA_VERSION,
    ensure_directories,
    load_dataset_session,
)


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def stamp():
    return datetime.now().strftime("%Y-%m-%d_%H%M%S")


def copy_tree_contents(src: Path, dst: Path):
    dst.mkdir(parents=True, exist_ok=True)
    if not src.exists():
        return
    for item in src.iterdir():
        target = dst / item.name
        if item.is_dir():
            shutil.copytree(item, target, dirs_exist_ok=True)
        else:
            shutil.copy2(item, target)


def clear_directory(path: Path):
    assert_dataset_writable(None, path)
    path.mkdir(parents=True, exist_ok=True)
    for item in path.iterdir():
        if item.is_dir():
            shutil.rmtree(item)
        else:
            item.unlink()


def status():
    ensure_directories()
    session = load_dataset_session()
    log_files = [p for p in (LOG_DIR / "v2").rglob("*") if p.is_file()]
    runtime_files = [p for p in (RUNTIME_DIR / "v2").rglob("*") if p.is_file()]
    print("=" * 72)
    print("BTC HUNTER DATASET STATUS")
    print("=" * 72)
    print("SESSION:", session or "NOT_INITIALIZED")
    print("LOG_V2_FILES:", len(log_files))
    print("RUNTIME_V2_FILES:", len(runtime_files))
    try:
        assert_dataset_writable((session or {}).get("dataset_id"), LOG_DIR / "v2")
        writable = bool(session)
    except RuntimeError:
        writable = False
    print("READY_FOR_NEW_SHADOW:", writable)
    print("=" * 72)


def prepare(confirmed: bool):
    if not confirmed:
        raise SystemExit("Refusing cleanup without --yes")

    assert_dataset_writable(get_dataset_id(), LOG_DIR / "v2")

    ensure_directories()
    archive_root = ARCHIVE_DIR / f"pre_clean_restart_{stamp()}"
    copy_tree_contents(LOG_DIR / "v2", archive_root / "logs_v2")
    copy_tree_contents(RUNTIME_DIR / "v2", archive_root / "runtime_v2")

    clear_directory(LOG_DIR / "v2")
    clear_directory(RUNTIME_DIR / "v2")

    dataset_id = "BTCV12P3_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ_") + uuid.uuid4().hex[:8]
    session = {
        "dataset_id": dataset_id,
        "created_at": utc_now(),
        "schema_version": SCHEMA_VERSION,
        "feature_version": FEATURE_VERSION,
        "collector_version": COLLECTOR_VERSION,
        "outcome_version": OUTCOME_VERSION,
        "archive_before_reset": str(archive_root),
        "mode": "SHADOW_ONLY",
        "paper_only": True,
    }
    with DATASET_SESSION_FILE.open("w", encoding="utf-8") as f:
        json.dump(session, f, ensure_ascii=False, indent=2)

    print("=" * 72)
    print("NEW DATASET PREPARED")
    print("DATASET_ID:", dataset_id)
    print("ARCHIVE:", archive_root)
    print("LOGS_V2:", LOG_DIR / "v2")
    print("RUNTIME_V2:", RUNTIME_DIR / "v2")
    print("=" * 72)


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command")
    sub.add_parser("status")
    p = sub.add_parser("prepare")
    p.add_argument("--yes", action="store_true")
    args = parser.parse_args()

    if args.command == "prepare":
        prepare(args.yes)
    else:
        status()


if __name__ == "__main__":
    main()


