# -*- coding: utf-8 -*-

"""Regression coverage for EvidenceHunter / audit_order.py.

MINIMUM_INITIAL_SLICE — first pytest slice for this repository.
Implemented per GPT (INDEPENDENT_REVIEWER) review "2-AR / 13BA" in
EvidenceHunter_MASTER_LIVING_DOCUMENT.txt, PASS_WITH_REQUIRED_CHANGES,
2026-09-04.

READ THIS BEFORE ADDING MORE TESTS
-----------------------------------
1. TEST ISOLATION IS NOT OPTIONAL.
   audit_order.main() calls get_dataset_id(), which calls
   EvidenceHunter_config.load_dataset_session(), which unconditionally calls
   EvidenceHunter_config.ensure_directories() -> real mkdir(parents=True,
   exist_ok=True) on the *actual* repository's runtime/, logs/,
   research/, archive/ directories (AUDIT_TOOL_FILESYSTEM_SIDE_EFFECT
   finding, recorded in MASTER "2-AR" / CHANGELOG "2-AR/13BA"; not a
   destructive bug, but the audit tool is NOT strictly filesystem-pure).
   Every test that calls audit_order.main() MUST use the `audit_env`
   fixture below, which patches audit_order.get_dataset_id() to a fixed
   synthetic value so this call chain is never reached and no real
   production directory is ever touched.

2. WHY WE PATCH `audit_order.X`, NOT `EvidenceHunter_config.X`.
   audit_order.py does:
       from EvidenceHunter_config import (
           ORDERFLOW_FILE_V2, OUTCOMES_FILE_V2, SHADOW_FILE_V2,
           get_dataset_id,
       )
   `from module import name` binds a *new* reference inside audit_order's
   own namespace at import time; it is not a live alias back to
   EvidenceHunter_config. audit_order.main() only ever reads the bare names
   SHADOW_FILE_V2 / OUTCOMES_FILE_V2 / ORDERFLOW_FILE_V2 and calls
   get_dataset_id() — all resolved through audit_order's own module
   globals. Patching EvidenceHunter_config.SHADOW_FILE_V2 etc. would silently
   have NO effect on audit_order.main(). We therefore always
   monkeypatch.setattr(audit_order, "<name>", ...), never
   EvidenceHunter_config.<name>. This is the pytest-recommended "patch where
   it's used" rule, applied concretely to this codebase.

3. WHY WE PATCH `audit_order.datetime`, NOT `datetime.datetime`.
   audit_order.py does `from datetime import datetime, timezone` and
   inside main() calls `datetime.now(timezone.utc)`. Same reasoning as
   above: `datetime` inside audit_order is its own module-global name.
   We patch audit_order.datetime with a subclass (_FrozenDateTime) that
   overrides only .now(); parse_time() in the same module also resolves
   `datetime` via this same name and calls datetime.fromisoformat(...),
   so the frozen class must remain a real datetime subclass (inheriting
   fromisoformat, arithmetic, comparison unchanged) rather than an
   unrelated fake object.

4. IMPORT-TIME SAFETY (verified by direct source reading, not assumed).
   Both audit_order.py and EvidenceHunter_config.py contain only constant
   definitions and function/def statements at module level; neither
   module calls ensure_directories(), get_dataset_id(), or any other
   function as a side effect of being imported. `import audit_order`
   alone does not touch the filesystem. The only filesystem access
   happens inside main() (and inside load_jsonl(), which is exercised
   directly and safely against tmp_path files in the tests below).

5. NO tests/conftest.py.
   This is the first and only test module for this repository (no
   tests/, conftest.py, or pytest config existed before this commit —
   confirmed via directory listing before writing this file). Per
   "DO NOT ABSTRACT BEFORE RESPONSIBILITY IS PROVEN SHARED", the
   isolation fixture stays local to this file. Extract it to
   tests/conftest.py only once a second real test module needs it.

ORACLE CLASSIFICATION (GPT-mandated; do not delete this table when
adding tests later — extend it)
--------------------------------------------------------------------
TEST_ID: test_audit_unique_valid_records_passes
  ORACLE_CLASS  = CHARACTERIZATION_ORACLE
  ORACLE_SOURCE = audit_order.py current implementation (the combined
                  PASS/FAIL gate). No independently-cited MASTER "Frozen
                  Contract" defining this exact combination was located
                  (searched MASTER for a REQUIRED_RECORD_FIELDS schema
                  section; none found as of 2026-09-04). Recommend
                  future cross-check against any authoritative schema
                  definition if/when one is written.
  EXPECTED_BEHAVIOR = main() returns 0 ("AUDIT RESULT: PASS") for a
                  minimal set of unique, complete, immature records.

TEST_ID: test_audit_duplicate_record_id_fails
  ORACLE_CLASS  = CHARACTERIZATION_ORACLE
  ORACLE_SOURCE = audit_order.py current implementation (Counter-based
                  duplicate-id detection). Same caveat as above.
  EXPECTED_BEHAVIOR = main() returns 2, prints "DUPLICATE_RECORD_ID".

TEST_ID: test_audit_duplicate_observation_minute_fails
  ORACLE_CLASS  = CHARACTERIZATION_ORACLE
  ORACLE_SOURCE = audit_order.py current implementation.
  EXPECTED_BEHAVIOR = main() returns 2, prints
                  "DUPLICATE_OBSERVATION_MINUTE".

TEST_ID: test_audit_missing_required_field_fails
  ORACLE_CLASS  = CHARACTERIZATION_ORACLE
  ORACLE_SOURCE = audit_order.py current implementation
                  (REQUIRED_RECORD_FIELDS gate).
  EXPECTED_BEHAVIOR = main() returns 2, prints
                  "MISSING_REQUIRED_FIELDS".

TEST_ID: test_load_jsonl_malformed_lines_follow_current_defined_behavior
  ORACLE_CLASS  = CHARACTERIZATION_ORACLE
  ORACLE_SOURCE = audit_order.load_jsonl() current implementation:
                  blank lines are skipped silently; a line that is not
                  valid JSON, or is valid JSON but not a dict, increments
                  bad_lines and is excluded from the returned rows. This
                  is recorded to detect unintended drift, not asserted
                  as permanent business truth.
  EXPECTED_BEHAVIOR = load_jsonl() returns only the well-formed dict
                  rows, with bad_lines counting every other non-blank
                  line.

TEST_ID: test_load_jsonl_missing_file_returns_empty
  ORACLE_CLASS  = CHARACTERIZATION_ORACLE
  ORACLE_SOURCE = audit_order.load_jsonl() current implementation
                  (`if not path.exists(): return rows, bad_lines` with
                  both starting at empty/0).
  EXPECTED_BEHAVIOR = load_jsonl() on a nonexistent path returns
                  ([], 0).

TEST_ID: test_raw_preservation_source_file_unchanged
  ORACLE_CLASS  = CONTRACT_ORACLE
  ORACLE_SOURCE = Project-level IRRECOVERABILITY-FIRST / RAW DATA
                  PRESERVATION principle (MASTER Part 7, established
                  independently of audit_order.py's own code — this is
                  a genuine project invariant, not merely current
                  behavior).
  EXPECTED_BEHAVIOR = Running main() never modifies the bytes of the
                  source SHADOW_FILE_V2 input file, regardless of
                  PASS/FAIL outcome. audit_order.py is documented as
                  "does not modify logs"; this test holds it to that
                  claim.
"""

import json
import sys
from pathlib import Path

import pytest

# Make the repository root importable regardless of the directory pytest
# is invoked from (no tests/conftest.py or pytest.ini exists yet — see
# module docstring point 5).
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import audit_order  # noqa: E402  (import after sys.path fix-up, by design)


# ---------------------------------------------------------------------------
# Frozen clock
# ---------------------------------------------------------------------------

# Captured while audit_order.datetime is still the real stdlib class (this
# module is imported before any monkeypatch runs), so this subclass's base
# is always the genuine datetime.datetime, never a previously-frozen stand-in.
_RealDateTime = audit_order.datetime
_RealTimezone = audit_order.timezone

FIXED_NOW = _RealDateTime(2026, 9, 4, 12, 0, 0, tzinfo=_RealTimezone.utc)


class _FrozenDateTime(_RealDateTime):
    """Deterministic stand-in for audit_order.datetime.

    Overrides only .now(). Every other method (fromisoformat, arithmetic,
    comparison, astimezone, ...) is inherited unchanged, because
    audit_order.parse_time() also resolves `datetime` via this same
    module-global name and must keep working exactly as it does today.
    """

    @classmethod
    def now(cls, tz=None):
        if tz is None:
            return FIXED_NOW
        return FIXED_NOW.astimezone(tz)


# ---------------------------------------------------------------------------
# Isolation fixture (local to this module — see docstring point 5)
# ---------------------------------------------------------------------------

TEST_DATASET_ID = "TEST_DATASET_2026_09_04"


@pytest.fixture
def audit_env(tmp_path, monkeypatch):
    """Isolate audit_order.main() from the real repository filesystem.

    - Redirects SHADOW_FILE_V2 / OUTCOMES_FILE_V2 / ORDERFLOW_FILE_V2 to
      tmp_path so nothing under the real repo's runtime/ or logs/ is ever
      read or written.
    - Patches get_dataset_id() to a fixed synthetic value so the real
      get_dataset_id -> load_dataset_session -> ensure_directories chain
      is never invoked (AUDIT_TOOL_FILESYSTEM_SIDE_EFFECT finding), and
      no directory is created under the real BASE_DIR.
    - Freezes audit_order.datetime so main()'s maturity-window logic is
      deterministic regardless of wall-clock time, timezone, DST, or
      clock drift.
    """
    shadow_file = tmp_path / "EvidenceHunter_shadow_records.jsonl"
    outcomes_file = tmp_path / "EvidenceHunter_shadow_outcomes.jsonl"
    orderflow_file = tmp_path / "orderflow_snapshot.json"
    # Deliberately not created here — each test writes exactly what it needs.

    monkeypatch.setattr(audit_order, "SHADOW_FILE_V2", shadow_file)
    monkeypatch.setattr(audit_order, "OUTCOMES_FILE_V2", outcomes_file)
    monkeypatch.setattr(audit_order, "ORDERFLOW_FILE_V2", orderflow_file)
    monkeypatch.setattr(audit_order, "get_dataset_id", lambda: TEST_DATASET_ID)
    monkeypatch.setattr(audit_order, "datetime", _FrozenDateTime)

    return {
        "dataset_id": TEST_DATASET_ID,
        "shadow_file": shadow_file,
        "outcomes_file": outcomes_file,
        "orderflow_file": orderflow_file,
    }


# ---------------------------------------------------------------------------
# Record-building helper (not a shared fixture — plain function, local to
# this module)
# ---------------------------------------------------------------------------

def _valid_record(record_id, observation_minute, dataset_id=TEST_DATASET_ID,
                   timestamp=None, missing_field=None):
    """Build a dict with every REQUIRED_RECORD_FIELDS key populated.

    Values for fields the tests don't specifically care about are cheap
    placeholders; only record_id / dataset_id / observation_minute /
    timestamp are meaningful to the checks under test here.
    """
    record = {field: f"placeholder_{field}" for field in audit_order.REQUIRED_RECORD_FIELDS}
    record["record_id"] = record_id
    record["dataset_id"] = dataset_id
    record["observation_minute"] = observation_minute
    record["timestamp"] = timestamp or FIXED_NOW.isoformat()
    if missing_field is not None:
        record.pop(missing_field, None)
    return record


def _write_jsonl(path, rows):
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row))
            f.write("\n")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_audit_unique_valid_records_passes(audit_env, capsys):
    records = [
        _valid_record("rec-1", "2026-09-04T11:59"),
        _valid_record("rec-2", "2026-09-04T12:00"),
    ]
    _write_jsonl(audit_env["shadow_file"], records)

    exit_code = audit_order.main()

    out = capsys.readouterr().out
    assert exit_code == 0
    assert "AUDIT RESULT: PASS" in out


def test_audit_duplicate_record_id_fails(audit_env, capsys):
    records = [
        _valid_record("rec-dup", "2026-09-04T11:59"),
        _valid_record("rec-dup", "2026-09-04T12:00"),
    ]
    _write_jsonl(audit_env["shadow_file"], records)

    exit_code = audit_order.main()

    out = capsys.readouterr().out
    assert exit_code == 2
    assert "AUDIT RESULT: FAIL" in out
    assert "DUPLICATE_RECORD_ID" in out


def test_audit_duplicate_observation_minute_fails(audit_env, capsys):
    records = [
        _valid_record("rec-1", "2026-09-04T12:00"),
        _valid_record("rec-2", "2026-09-04T12:00"),
    ]
    _write_jsonl(audit_env["shadow_file"], records)

    exit_code = audit_order.main()

    out = capsys.readouterr().out
    assert exit_code == 2
    assert "AUDIT RESULT: FAIL" in out
    assert "DUPLICATE_OBSERVATION_MINUTE" in out


def test_audit_missing_required_field_fails(audit_env, capsys):
    records = [
        _valid_record("rec-1", "2026-09-04T12:00", missing_field="schema_version"),
    ]
    _write_jsonl(audit_env["shadow_file"], records)

    exit_code = audit_order.main()

    out = capsys.readouterr().out
    assert exit_code == 2
    assert "AUDIT RESULT: FAIL" in out
    assert "MISSING_REQUIRED_FIELDS" in out


def test_load_jsonl_malformed_lines_follow_current_defined_behavior(tmp_path):
    path = tmp_path / "mixed.jsonl"
    good_row = {"record_id": "rec-1", "dataset_id": TEST_DATASET_ID}
    with path.open("w", encoding="utf-8") as f:
        f.write(json.dumps(good_row) + "\n")
        f.write("{not valid json\n")
        f.write("\n")  # blank line: silently skipped, not counted as bad
        f.write(json.dumps([1, 2, 3]) + "\n")  # valid JSON, not a dict
        f.write("   \n")  # whitespace-only: also silently skipped

    rows, bad_lines = audit_order.load_jsonl(path)

    assert rows == [good_row]
    assert bad_lines == 2


def test_load_jsonl_missing_file_returns_empty(tmp_path):
    missing_path = tmp_path / "does_not_exist.jsonl"

    rows, bad_lines = audit_order.load_jsonl(missing_path)

    assert rows == []
    assert bad_lines == 0


def test_raw_preservation_source_file_unchanged(audit_env, capsys):
    # Deliberately exercise a FAIL path (duplicate record_id), not the
    # happy path, to confirm RAW PRESERVATION holds regardless of the
    # PASS/FAIL outcome, not only when everything is clean.
    duplicate_records = [
        _valid_record("rec-dup", "2026-09-04T11:59"),
        _valid_record("rec-dup", "2026-09-04T12:00"),
    ]
    _write_jsonl(audit_env["shadow_file"], duplicate_records)
    original_bytes = audit_env["shadow_file"].read_bytes()

    audit_order.main()
    capsys.readouterr()  # drain output; not the subject of this test

    assert audit_env["shadow_file"].read_bytes() == original_bytes





