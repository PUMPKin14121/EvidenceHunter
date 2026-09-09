"""Synthetic lifecycle regression checks; never run real collectors or research."""
import hashlib
import importlib.util
import json
from pathlib import Path

import pytest
import EvidenceHunter_config as cfg
import EvidenceHunter_dataset as dataset


@pytest.fixture
def frozen(tmp_path, monkeypatch):
    canonical = tmp_path / "corrected.jsonl"
    canonical.write_text("corrected\n", encoding="utf-8")
    registry = tmp_path / "lifecycle.json"
    registry.write_text(json.dumps({"schema_version": 1, "frozen_datasets": {
        "FROZEN": {"protected_paths": ["old.jsonl", "corrected.jsonl"],
                   "canonical_outcome": {"path": "corrected.jsonl", "sha256": hashlib.sha256(canonical.read_bytes()).hexdigest()}}
    }}), encoding="utf-8")
    monkeypatch.setattr(cfg, "BASE_DIR", tmp_path)
    monkeypatch.setattr(cfg, "LIFECYCLE_FILE", registry)
    return tmp_path


def test_canonical_read_and_tamper_rejection(frozen):
    assert cfg.research_outcomes_path("FROZEN", frozen / "old.jsonl") == frozen / "corrected.jsonl"
    (frozen / "corrected.jsonl").write_text("corrupt", encoding="utf-8")
    with pytest.raises(RuntimeError, match="HASH_MISMATCH"):
        cfg.research_outcomes_path("FROZEN", frozen / "old.jsonl")


@pytest.mark.parametrize("dataset_id,path", [("FROZEN", "new.jsonl"), ("NEW", "old.jsonl"), ("NEW", "corrected.jsonl"), ("NEW", ".")])
def test_frozen_id_and_protected_paths_blocked(frozen, dataset_id, path):
    with pytest.raises(RuntimeError, match="FORBIDDEN"):
        cfg.assert_dataset_writable(dataset_id, frozen / path)


def test_new_dataset_separate_path_allowed(frozen):
    cfg.assert_dataset_writable("NEW", frozen / "new.jsonl")
    assert cfg.research_outcomes_path("NEW", frozen / "new.jsonl") == frozen / "new.jsonl"


def test_missing_registry_fails_closed(frozen):
    cfg.LIFECYCLE_FILE.unlink()
    with pytest.raises(FileNotFoundError):
        cfg.assert_dataset_writable("NEW", frozen / "new.jsonl")


def test_prepare_and_clear_preserve_frozen_files(frozen, monkeypatch):
    monkeypatch.setattr(dataset, "get_dataset_id", lambda: "FROZEN")
    before = (frozen / "corrected.jsonl").read_bytes()
    with pytest.raises(RuntimeError, match="FORBIDDEN"):
        dataset.prepare(True)
    with pytest.raises(RuntimeError, match="FORBIDDEN"):
        dataset.clear_directory(frozen)
    assert (frozen / "corrected.jsonl").read_bytes() == before


def test_retired_recorder_rejects_before_runtime_write(tmp_path):
    # NOTE: this test assumes the v11r1 project lives as a sibling directory
    # of this (formal) project's parent -- i.e. both projects sit directly
    # under the same parent folder (e.g. D:\EvidenceHunter and
    # D:\EvidenceHunter_V11R1_OUTCOME_RECORDER_CN under D:\). This holds on the
    # current development machine but is not guaranteed on every checkout;
    # the existence check below turns a missing sibling into an explicit
    # skip instead of a confusing import/spec error.
    source = Path(__file__).resolve().parents[2] / "EvidenceHunter_V11R1_OUTCOME_RECORDER_CN" / "control_persistence_horizon_v1_shadow_recorder.py"
    if not source.exists():
        pytest.skip(
            "v11r1 sibling project not found at expected path "
            f"({source}); this cross-repo test only runs when both "
            "projects share a parent directory."
        )
    spec = importlib.util.spec_from_file_location("cph_guard_test", source)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    runtime = tmp_path / mod.RUNTIME_FILE
    runtime.write_text("historical runtime", encoding="utf-8")
    (tmp_path / "CONTROL_PERSISTENCE_HORIZON_V1_CP200_AUDIT.json").write_text(json.dumps({"final_conclusion": {"hypothesis_status": "COMPLETE / RETIRED_FROM_FURTHER_TUNING"}}), encoding="utf-8")
    with pytest.raises(RuntimeError, match="RETIRED_RESEARCH"):
        mod.Recorder(tmp_path)
    assert runtime.read_text(encoding="utf-8") == "historical runtime"


def test_analysis_joins_corrected_outcome_instead_of_old(frozen, monkeypatch):
    import EvidenceHunter_shadow_analysis_v2 as analysis
    records = frozen / "records.jsonl"
    records.write_text(json.dumps({"dataset_id": "FROZEN", "record_id": "r1", "timestamp": "2026-01-01T00:00:00Z"}) + "\n", encoding="utf-8")
    for name, value in [("old.jsonl", -99), ("corrected.jsonl", 42)]:
        (frozen / name).write_text(json.dumps({"dataset_id": "FROZEN", "record_id": "r1", "future_outcome": {"long_return_5m": value}}) + "\n", encoding="utf-8")
    registry = json.loads(cfg.LIFECYCLE_FILE.read_text(encoding="utf-8"))
    registry["frozen_datasets"]["FROZEN"]["canonical_outcome"]["sha256"] = hashlib.sha256((frozen / "corrected.jsonl").read_bytes()).hexdigest()
    cfg.LIFECYCLE_FILE.write_text(json.dumps(registry), encoding="utf-8")
    monkeypatch.setattr(analysis, "INPUT_FILE", records)
    monkeypatch.setattr(analysis, "OUTCOMES_FILE", frozen / "old.jsonl")
    assert analysis.load_records("FROZEN")[0]["future_outcome"]["long_return_5m"] == 42


def test_audit_reads_canonical_path(frozen, monkeypatch):
    import audit_order
    monkeypatch.setattr(audit_order, "get_dataset_id", lambda: "FROZEN")
    monkeypatch.setattr(audit_order, "SHADOW_FILE_V2", frozen / "records.jsonl")
    monkeypatch.setattr(audit_order, "OUTCOMES_FILE_V2", frozen / "old.jsonl")
    class ReadReached(Exception):
        pass
    def read(path):
        if path == frozen / "records.jsonl":
            return [], 0
        assert path == frozen / "corrected.jsonl"
        raise ReadReached
    monkeypatch.setattr(audit_order, "load_jsonl", read)
    with pytest.raises(ReadReached):
        audit_order.main()


def test_diag_reads_canonical_outcome_instead_of_old(frozen, monkeypatch):
    import EvidenceHunter_shadow_outcome_diag as diag
    monkeypatch.setattr(diag, "get_dataset_id", lambda: "FROZEN")
    monkeypatch.setattr(diag, "OUTCOMES_FILE_V2", frozen / "old.jsonl")
    monkeypatch.setattr(diag, "load_records", lambda dataset_id: [])

    class ReadReached(Exception):
        pass

    def fake_load_existing_outcomes(dataset_id, path=None):
        assert dataset_id == "FROZEN"
        assert path == frozen / "corrected.jsonl"
        raise ReadReached

    monkeypatch.setattr(diag, "load_existing_outcomes", fake_load_existing_outcomes)
    with pytest.raises(ReadReached):
        diag.main()



