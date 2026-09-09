"""governance_guard.py

Machine-checkable enforcement for AI_EXECUTION_CONTRACT_V1.json.

This script is deliberately small and dependency-free (Python stdlib only,
matching the style of audit_order.py in this repo). It performs two
read-only checks and never mutates the repository or touches the network:

  1. validate_output(candidate, schema)
     Rejects a structured-output object that has keys outside the
     contract's structured_output_schema, is missing a required key, or
     violates a type/enum constraint.

  2. check_change_budget(scope_declaration, change_summary, default_budgets)
     Checks a *declared* summary of a changeset's new files/dirs, scan/
     test/reconciliation run counts, and MASTER/CHANGELOG append sizes
     against the effective budgets (scope_declaration's budget_overrides
     if present, else default_budgets), and checks every new path against
     allowed_paths / forbidden_paths prefixes.

  check_change_budget does NOT walk the git repository itself and does NOT
  infer which paths are "new" on its own -- the caller supplies an explicit
  change_summary. This keeps the guard a simple, auditable mechanical
  check rather than an autonomous scanner (consistent with
  AI_EXECUTION_CONTRACT_V1.json's non_goals).

CLI usage:
  python governance_guard.py validate-output <output.json> [--contract <path>]
  python governance_guard.py check-budget --scope <scope.json> --summary <summary.json> [--contract <path>]
  python governance_guard.py --self-test

Exit codes: 0 on PASS, 2 on FAIL, 1 on usage/self-test failure.
"""

import json
import sys
from pathlib import Path

DEFAULT_CONTRACT_PATH = Path(__file__).resolve().parent / "research" / "governance" / "AI_EXECUTION_CONTRACT_V1.json"


def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_contract(path=None):
    contract_path = Path(path) if path else DEFAULT_CONTRACT_PATH
    return load_json(contract_path)


def _type_ok(value, declared_type):
    types = declared_type if isinstance(declared_type, list) else [declared_type]
    checks = {
        "string": lambda v: isinstance(v, str),
        "boolean": lambda v: isinstance(v, bool),
        "null": lambda v: v is None,
        "integer": lambda v: isinstance(v, int) and not isinstance(v, bool),
        "object": lambda v: isinstance(v, dict),
        "array": lambda v: isinstance(v, list),
    }
    return any(checks.get(t, lambda v: False)(value) for t in types)


def validate_output(candidate, schema):
    """Structural validation only: required keys, no extra keys, type/enum checks."""
    violations = []

    if not isinstance(candidate, dict):
        return {"valid": False, "violations": ["candidate is not a JSON object"]}

    properties = schema.get("properties", {})
    required = schema.get("required", [])
    additional_allowed = schema.get("additionalProperties", True)

    for key in required:
        if key not in candidate:
            violations.append(f"missing required key: {key}")

    if not additional_allowed:
        for key in candidate:
            if key not in properties:
                violations.append(f"unexpected key not in schema (additionalProperties=false): {key}")

    for key, spec in properties.items():
        if key not in candidate:
            continue
        value = candidate[key]
        declared_type = spec.get("type")
        if declared_type is not None and not _type_ok(value, declared_type):
            violations.append(f"key '{key}' has wrong type: expected {declared_type}, got {type(value).__name__}")
            continue
        enum = spec.get("enum")
        if enum is not None and value is not None and value not in enum:
            violations.append(f"key '{key}' value {value!r} not in allowed enum {enum}")

    return {"valid": len(violations) == 0, "violations": violations}


def _effective_budgets(scope_declaration, default_budgets):
    overrides = (scope_declaration or {}).get("budget_overrides", {}) or {}
    effective = dict(default_budgets)
    for k, v in overrides.items():
        if k in effective and v is not None:
            effective[k] = v
    return effective


def _path_matches_any_prefix(path, prefixes):
    normalized = path.replace("\\", "/")
    return any(normalized.startswith(p.replace("\\", "/")) for p in prefixes)


def check_change_budget(scope_declaration, change_summary, default_budgets):
    violations = []
    effective = _effective_budgets(scope_declaration, default_budgets)

    new_files = change_summary.get("new_files", [])
    new_dirs = change_summary.get("new_dirs", [])

    if len(new_files) > effective["MAX_NEW_FILES"]:
        violations.append(f"new_files count {len(new_files)} exceeds MAX_NEW_FILES {effective['MAX_NEW_FILES']}")
    if len(new_dirs) > effective["MAX_NEW_DIRS"]:
        violations.append(f"new_dirs count {len(new_dirs)} exceeds MAX_NEW_DIRS {effective['MAX_NEW_DIRS']}")

    allowed_paths = (scope_declaration or {}).get("allowed_paths", []) or []
    forbidden_paths = (scope_declaration or {}).get("forbidden_paths", []) or []

    for p in list(new_files) + list(new_dirs):
        if forbidden_paths and _path_matches_any_prefix(p, forbidden_paths):
            violations.append(f"path '{p}' matches a forbidden_paths prefix")
        elif allowed_paths and not _path_matches_any_prefix(p, allowed_paths):
            violations.append(f"path '{p}' does not match any allowed_paths prefix")

    counters = {
        "full_repo_scans": "MAX_FULL_REPO_SCANS",
        "full_test_runs": "MAX_FULL_TEST_RUNS",
        "reconciliation_runs": "MAX_RECONCILIATION_RUNS",
        "master_append_bytes": "MASTER_APPEND_BUDGET_BYTES",
        "changelog_entries": "CHANGELOG_MAX_ENTRIES",
    }
    for summary_key, budget_key in counters.items():
        value = change_summary.get(summary_key, 0)
        limit = effective.get(budget_key)
        if limit is not None and value > limit:
            violations.append(f"{summary_key} {value} exceeds {budget_key} {limit}")

    return {"result": "PASS" if not violations else "FAIL", "violations": violations, "effective_budgets": effective}


def _self_test():
    contract = {
        "default_budgets": {
            "MAX_NEW_FILES": 2, "MAX_NEW_DIRS": 0, "MAX_FULL_REPO_SCANS": 0,
            "MAX_FULL_TEST_RUNS": 1, "MAX_RECONCILIATION_RUNS": 1,
            "MASTER_APPEND_BUDGET_BYTES": 2000, "CHANGELOG_MAX_ENTRIES": 1,
        },
        "structured_output_schema": {
            "type": "object", "additionalProperties": False,
            "required": ["status", "action", "scope_status", "blocking_issue", "scope_change_requested", "next_action"],
            "properties": {
                "status": {"type": "string", "enum": ["PASS", "FAIL"]},
                "action": {"type": "string"},
                "scope_status": {"type": "string", "enum": ["IN_SCOPE", "OUT_OF_SCOPE"]},
                "blocking_issue": {"type": ["string", "null"]},
                "scope_change_requested": {"type": "boolean"},
                "next_action": {"type": "string", "enum": ["CONTINUE", "STOP", "ESCALATE_TO_OWNER"]},
            },
        },
    }

    good_output = {
        "status": "PASS", "action": "validate_execution_contract", "scope_status": "IN_SCOPE",
        "blocking_issue": None, "scope_change_requested": False, "next_action": "STOP",
    }
    bad_output_extra_key = dict(good_output, extra_field="not allowed")
    bad_output_bad_enum = dict(good_output, next_action="KEEP_GOING_FOREVER")
    bad_output_missing_key = {k: v for k, v in good_output.items() if k != "blocking_issue"}

    checks = []
    r = validate_output(good_output, contract["structured_output_schema"])
    checks.append(("good_output should be valid", r["valid"] is True))
    r = validate_output(bad_output_extra_key, contract["structured_output_schema"])
    checks.append(("extra key should be rejected", r["valid"] is False))
    r = validate_output(bad_output_bad_enum, contract["structured_output_schema"])
    checks.append(("bad enum should be rejected", r["valid"] is False))
    r = validate_output(bad_output_missing_key, contract["structured_output_schema"])
    checks.append(("missing required key should be rejected", r["valid"] is False))

    scope = {
        "changeset_id": "SELF_TEST",
        "allowed_paths": ["research/governance/", "governance_guard.py", "AI_START_HERE.md", "PROJECT_CURRENT_STATE.json"],
        "forbidden_paths": ["archive/discovery_evidence_v1_to_v2/"],
    }
    good_summary = {
        "new_files": ["research/governance/AI_EXECUTION_CONTRACT_V1.json", "governance_guard.py"],
        "new_dirs": [],
        "full_repo_scans": 0, "full_test_runs": 1, "reconciliation_runs": 1,
        "master_append_bytes": 1500, "changelog_entries": 1,
    }
    r = check_change_budget(scope, good_summary, contract["default_budgets"])
    checks.append(("in-budget summary should PASS", r["result"] == "PASS"))

    over_budget_summary = dict(good_summary, new_files=good_summary["new_files"] + ["extra_file.py", "another_extra.py"])
    r = check_change_budget(scope, over_budget_summary, contract["default_budgets"])
    checks.append(("over MAX_NEW_FILES should FAIL", r["result"] == "FAIL"))

    forbidden_summary = dict(good_summary, new_files=good_summary["new_files"] + ["archive/discovery_evidence_v1_to_v2/should_not_be_here.json"])
    r = check_change_budget(scope, forbidden_summary, contract["default_budgets"])
    checks.append(("touching forbidden_paths should FAIL", r["result"] == "FAIL"))

    passed = sum(1 for _, ok in checks if ok)
    for name, ok in checks:
        print(("PASS  " if ok else "FAIL  ") + name)
    print(f"self-test: {passed}/{len(checks)} checks passed")
    return passed == len(checks)


def main(argv):
    if argv[:1] == ["--self-test"]:
        ok = _self_test()
        return 0 if ok else 1

    if len(argv) >= 2 and argv[0] == "validate-output":
        output_path = argv[1]
        contract_path = None
        if "--contract" in argv:
            contract_path = argv[argv.index("--contract") + 1]
        contract = load_contract(contract_path)
        candidate = load_json(output_path)
        result = validate_output(candidate, contract["structured_output_schema"])
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0 if result["valid"] else 2

    if len(argv) >= 4 and argv[0] == "check-budget" and "--scope" in argv and "--summary" in argv:
        scope_path = argv[argv.index("--scope") + 1]
        summary_path = argv[argv.index("--summary") + 1]
        contract_path = None
        if "--contract" in argv:
            contract_path = argv[argv.index("--contract") + 1]
        contract = load_contract(contract_path)
        scope_declaration = load_json(scope_path)
        change_summary = load_json(summary_path)
        result = check_change_budget(scope_declaration, change_summary, contract["default_budgets"])
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0 if result["result"] == "PASS" else 2

    raise SystemExit(
        "Usage:\n"
        "  governance_guard.py validate-output <output.json> [--contract <path>]\n"
        "  governance_guard.py check-budget --scope <scope.json> --summary <summary.json> [--contract <path>]\n"
        "  governance_guard.py --self-test"
    )


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
