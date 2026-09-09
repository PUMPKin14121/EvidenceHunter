"""EvidenceHunter_gap_ledger: append-only gap evidence, aggTrade ID gap / REST
backfill and unrecoverable-depth-gap + repair-provenance record-keeping."""
import json
import threading

import pytest

import EvidenceHunter_gap_ledger as gl


def test_open_gap_writes_required_fields(tmp_path):
    path = tmp_path / "gap_ledger.jsonl"
    gap_id = gl.open_gap(
        path, component="aggTrade", gap_start=101, gap_end=105,
        reason="AGGTRADE_ID_SEQUENCE_GAP", last_good_identifier=100,
        first_good_identifier_after=106, recoverability="RECOVERABLE_VIA_BACKFILL",
    )
    events = gl.read_events(path)
    assert len(events) == 1
    record = events[0]
    for field in gl.REQUIRED_OPEN_FIELDS:
        assert field in record
    assert record["gap_id"] == gap_id
    assert record["repair_status"] == "OPEN"
    assert record["event_type"] == "GAP_OPENED"


def test_invalid_recoverability_rejected(tmp_path):
    path = tmp_path / "gap_ledger.jsonl"
    with pytest.raises(gl.GapLedgerError):
        gl.open_gap(
            path, component="aggTrade", gap_start=1, gap_end=1,
            reason="X", last_good_identifier=0, recoverability="MAYBE",
        )


def test_append_only_update_never_rewrites_prior_lines(tmp_path):
    path = tmp_path / "gap_ledger.jsonl"
    gap_id = gl.open_gap(
        path, component="diff_depth", gap_start=1, gap_end=5,
        reason="PU_CONTINUITY_MISMATCH", last_good_identifier=1000,
        first_good_identifier_after=None,
    )
    before = path.read_bytes()
    gl.update_gap(path, gap_id, resync_status="PENDING")
    after = path.read_bytes()
    assert after.startswith(before)
    assert after != before


def test_mark_gap_repaired_produces_reconstructable_latest_state(tmp_path):
    path = tmp_path / "gap_ledger.jsonl"
    gap_id = gl.open_gap(
        path, component="aggTrade", gap_start=101, gap_end=105,
        reason="AGGTRADE_ID_SEQUENCE_GAP", last_good_identifier=100,
        first_good_identifier_after=106, recoverability="RECOVERABLE_VIA_BACKFILL",
    )
    gl.mark_gap_repaired(path, gap_id, repair_source="REST_AGGTRADES_FROM_ID")
    latest = gl.materialize_latest(path)
    assert len(latest) == 1
    assert latest[0]["repair_status"] == "REPAIRED"
    assert latest[0]["repair_source"] == "REST_AGGTRADES_FROM_ID"
    assert latest[0]["resync_status"] == "COMPLETED"
    assert latest[0]["component"] == "aggTrade"
    assert latest[0]["last_good_identifier"] == 100


def test_mark_gap_unrecoverable_records_detail_and_final_status(tmp_path):
    path = tmp_path / "gap_ledger.jsonl"
    gap_id = gl.open_gap(
        path, component="diff_depth", gap_start=2000, gap_end=None,
        reason="PU_CONTINUITY_MISMATCH", last_good_identifier=2000,
        first_good_identifier_after=None,
    )
    gl.mark_gap_unrecoverable(path, gap_id, detail="EXCEEDED_MAX_RESYNC_ATTEMPTS=5")
    latest = gl.materialize_latest(path)[0]
    assert latest["repair_status"] == "UNRECOVERABLE_CONFIRMED"
    assert latest["recoverability"] == "UNRECOVERABLE"
    assert latest["unrecoverable_detail"] == "EXCEEDED_MAX_RESYNC_ATTEMPTS=5"


def test_build_repair_provenance_tags_backfilled_records():
    tag = gl.build_repair_provenance(
        acquisition_mode="REST_BACKFILL", original_gap_id="GAP_aggTrade_abc123",
        source_provenance="GET /fapi/v1/aggTrades?fromId=",
    )
    assert tag["acquisition_mode"] == "REST_BACKFILL"
    assert tag["original_gap_id"] == "GAP_aggTrade_abc123"
    assert "repaired_at" in tag


def test_invalid_acquisition_mode_rejected():
    with pytest.raises(gl.GapLedgerError):
        gl.build_repair_provenance(acquisition_mode="LIVE_BUT_FAKED", original_gap_id="x", source_provenance="x")


def test_update_before_open_raises_on_materialize(tmp_path):
    path = tmp_path / "gap_ledger.jsonl"
    gl.update_gap(path, "GAP_NEVER_OPENED", repair_status="REPAIRED")
    with pytest.raises(gl.GapLedgerError):
        gl.materialize_latest(path)


def test_corrupt_line_raises_gap_ledger_error(tmp_path):
    path = tmp_path / "gap_ledger.jsonl"
    path.write_text("{not valid json\n", encoding="utf-8")
    with pytest.raises(gl.GapLedgerError):
        gl.read_events(path)


def test_missing_ledger_file_reads_as_empty(tmp_path):
    path = tmp_path / "does_not_exist.jsonl"
    assert gl.read_events(path) == []
    assert gl.materialize_latest(path) == []


def test_concurrent_appends_from_multiple_threads_all_survive(tmp_path):
    path = tmp_path / "gap_ledger.jsonl"

    def worker(i):
        gl.open_gap(
            path, component=f"component_{i}", gap_start=i, gap_end=i,
            reason="TEST", last_good_identifier=i, first_good_identifier_after=i + 1,
        )

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    events = gl.read_events(path)
    assert len(events) == 20
    for event in events:
        json.loads(json.dumps(event))

