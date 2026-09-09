"""Fault injection for the limited governance audit; synthetic files only."""
import hashlib
import json
import pytest
from audit_order import reconcile_project


@pytest.fixture
def project(tmp_path):
    source = tmp_path / "source.py"
    source.write_text("value = 1\n", encoding="utf-8")
    registry = {"schema_version": 1, "scope": "TEST_ONLY", "requirements": [
        {"requirement_id": "R1", "implementation_status": "TESTED", "code_assets": ["A1"]}
    ], "assets": {"A1": {"project": "formal", "path": "source.py", "sha256": hashlib.sha256(source.read_bytes()).hexdigest(), "requirements": ["R1"]}}}
    return tmp_path, registry


def audit(project):
    root, registry = project
    path = root / "registry.json"
    path.write_text(json.dumps(registry), encoding="utf-8")
    return reconcile_project(path, {"formal": root})


def test_matching_assets_never_self_certifies(project):
    report = audit(project)
    assert report["RECONCILIATION_STATUS"] == "PASS_FOR_REGISTERED_ASSET_VERSIONS_ONLY"
    assert report["RELEASE_READY"] is False
    assert report["INDEPENDENT_REVIEW_STATUS"] == "PENDING"


@pytest.mark.parametrize("fault,expected", [("modify", "VERSION_MISMATCH"), ("delete", "MISSING_EXPECTED_ASSET"), ("remove_requirement", "DEPENDENCY_MISMATCH"), ("self_verify", "UNSUPPORTED_STATUS_FOR_CURRENT_AUDIT_SCOPE")])
def test_faults_are_detected(project, fault, expected):
    root, registry = project
    if fault == "modify":
        (root / "source.py").write_text("value = 2\n", encoding="utf-8")
    elif fault == "delete":
        (root / "source.py").unlink()
    elif fault == "remove_requirement":
        registry["requirements"][0]["requirement_id"] = "OTHER"
    else:
        registry["requirements"][0]["implementation_status"] = "VERIFIED"
    report = audit(project)
    assert report["RECONCILIATION_STATUS"] == "FAIL"
    assert expected in {f["kind"] for f in report["findings"]}


def test_missing_registry_is_not_success(tmp_path):
    report = reconcile_project(tmp_path / "missing.json", {"formal": tmp_path})
    assert report["RECONCILIATION_STATUS"] == "FAIL"
