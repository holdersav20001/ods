"""Phase 2 client wrappers — live round-trip tests.

EVERY wrapper call passes commit=False; the `conn` fixture rolls back, keeping
tests isolated. Committing here would poison the shared DB.
"""
import datetime
import uuid

import pytest

from control import dlq, lineage, recon, runs, stages

BD = datetime.date(2025, 1, 15)


def _start(conn, *, status=None, record_count_out=None):
    """Start a run via the client; optionally drive it to a terminal status."""
    wfid = str(uuid.uuid4())
    run_id = runs.start(
        conn,
        workflow_run_id=wfid,
        pipeline_type="ingestion",
        domain="sales",
        dataset="orders",
        business_date=BD,
        trigger_type="manual",
        commit=False,
    )
    if status:
        runs.finalise(conn, run_id, status=status,
                      record_count_out=record_count_out, commit=False)
    return run_id, wfid


# ---- runs.start -------------------------------------------------------------

def test_start_creates_running_run(conn):
    run_id, wfid = _start(conn)
    assert isinstance(run_id, str)
    uuid.UUID(run_id)  # parses as a uuid
    row = conn.execute(
        "SELECT status, trigger_type, business_date, workflow_run_id "
        "FROM cp.run_log WHERE run_id=%s", (run_id,)
    ).fetchone()
    assert row == ("running", "manual", BD, wfid)


# ---- runs.patch / finalise --------------------------------------------------

def test_patch_updates_status_and_counts(conn):
    run_id, _ = _start(conn)
    runs.patch(conn, run_id, status="running", record_count_in=42, commit=False)
    row = conn.execute(
        "SELECT status, record_count_in FROM cp.run_log WHERE run_id=%s",
        (run_id,)
    ).fetchone()
    assert row == ("running", 42)


def test_patch_can_set_error_explicitly(conn):
    run_id, _ = _start(conn)
    runs.patch(conn, run_id, error="boom", commit=False)
    err = conn.execute(
        "SELECT error FROM cp.run_log WHERE run_id=%s", (run_id,)
    ).fetchone()[0]
    assert err == "boom"


def test_finalise_stamps_terminal_status_and_finished_at(conn):
    run_id, _ = _start(conn)
    runs.finalise(conn, run_id, status="succeeded", record_count_out=10,
                  commit=False)
    status, out, finished = conn.execute(
        "SELECT status, record_count_out, finished_at "
        "FROM cp.run_log WHERE run_id=%s", (run_id,)
    ).fetchone()
    assert status == "succeeded"
    assert out == 10
    assert finished is not None


# ---- runs.latest_succeeded_run ---------------------------------------------

def test_latest_succeeded_run_none_when_no_success(conn):
    _start(conn)                       # running
    _start(conn, status="failed")      # failed
    got = runs.latest_succeeded_run(
        conn, domain="sales", dataset="orders",
        business_date=BD, pipeline_type="ingestion")
    assert got is None


def test_latest_succeeded_run_returns_newest(conn):
    first, _ = _start(conn, status="succeeded")
    second, _ = _start(conn, status="succeeded")
    got = runs.latest_succeeded_run(
        conn, domain="sales", dataset="orders",
        business_date=BD, pipeline_type="ingestion")
    # Discovery index orders by (finished_at DESC NULLS LAST, run_id DESC).
    # Both runs share finished_at (same txn now()), so the deterministic
    # tiebreaker is the greater run_id. Assert the documented contract rather
    # than insertion order.
    assert got == max(first, second)


# ---- lineage.write_link -----------------------------------------------------

def test_write_link_creates_link_and_edges(conn):
    run_id, _ = _start(conn)
    edges = [
        {"edge_type": "raw_to_curated", "record_count": 5},
        {"edge_type": "raw_to_curated", "record_count": 3},
    ]
    link_id = lineage.write_link(
        conn, consumer_run_id=run_id, edge_type="raw_to_curated",
        target_ref={"content_hash": "abc"}, record_count=8, edges=edges,
        commit=False)
    assert isinstance(link_id, str)
    n_edges = conn.execute(
        "SELECT count(*) FROM cp.lineage_edge WHERE lineage_link_id=%s",
        (link_id,)).fetchone()[0]
    assert n_edges == 2


def test_write_link_empty_edges_raises(conn):
    run_id, _ = _start(conn)
    with pytest.raises(Exception):
        lineage.write_link(
            conn, consumer_run_id=run_id, edge_type="raw_to_curated",
            target_ref={"content_hash": "x"}, record_count=0, edges=[],
            commit=False)


def test_write_link_idempotent_on_content_hash(conn):
    run_id, _ = _start(conn)
    edges = [{"edge_type": "raw_to_curated", "record_count": 1}]
    first = lineage.write_link(
        conn, consumer_run_id=run_id, edge_type="raw_to_curated",
        target_ref={"content_hash": "dup"}, record_count=1, edges=edges,
        commit=False)
    second = lineage.write_link(
        conn, consumer_run_id=run_id, edge_type="raw_to_curated",
        target_ref={"content_hash": "dup"}, record_count=1, edges=edges,
        commit=False)
    assert first == second
    n_edges = conn.execute(
        "SELECT count(*) FROM cp.lineage_edge WHERE lineage_link_id=%s",
        (first,)).fetchone()[0]
    assert n_edges == 1


# ---- lineage.write_link_then_rows ------------------------------------------

def test_write_link_then_rows_stamps_target_rows(conn):
    run_id, wfid = _start(conn)
    rows = [{"id": 1}, {"id": 2}, {"id": 3}]
    edges = [{"edge_type": "curated_to_canonical", "record_count": 3}]
    link_id = lineage.write_link_then_rows(
        conn, consumer_run_id=run_id, edge_type="curated_to_canonical",
        target_ref={"content_hash": "rows1"}, record_count=3,
        edges=edges, rows=rows, commit=False)
    cnt, wf = conn.execute(
        "SELECT count(*), max(_ods_workflow_run_id) FROM ods.orders "
        "WHERE _ods_lineage_link_id=%s", (link_id,)).fetchone()
    assert cnt == len(rows)
    assert wf == wfid


# ---- stages.stage_scope -----------------------------------------------------

def test_stage_scope_succeeds_with_counts(conn):
    run_id, _ = _start(conn)
    with stages.stage_scope(conn, run_id, "curate", commit=False) as st:
        st.record_in = 10
        st.record_out = 9
        st.metrics = {"dropped": 1}
    status, rin, rout, finished = conn.execute(
        "SELECT status, record_count_in, record_count_out, finished_at "
        "FROM cp.run_stage_log WHERE stage_log_id=%s", (st.stage_log_id,)
    ).fetchone()
    assert status == "succeeded"
    assert (rin, rout) == (10, 9)
    assert finished is not None


def test_stage_scope_records_failure_and_reraises(conn):
    run_id, _ = _start(conn)
    captured = {}
    with pytest.raises(ValueError):
        with stages.stage_scope(conn, run_id, "curate", commit=False) as st:
            captured["id"] = st.stage_log_id
            raise ValueError("kaboom")
    status = conn.execute(
        "SELECT status FROM cp.run_stage_log WHERE stage_log_id=%s",
        (captured["id"],)).fetchone()[0]
    assert status == "failed"


# ---- recon.write_check ------------------------------------------------------

def test_write_check_records_discrepancy_and_status(conn):
    run_id, _ = _start(conn)
    recon.write_check(
        conn, run_id=run_id, check_type="row_count",
        source_count=100, accounted_count=97,
        metrics={"note": "3 quarantined"}, commit=False)
    disc, status = conn.execute(
        "SELECT discrepancy, status FROM cp.reconciliation_log "
        "WHERE run_id=%s", (run_id,)).fetchone()
    assert disc == 3
    assert status == "breach"


# ---- dlq.quarantine / replay ------------------------------------------------

def test_quarantine_writes_dlq_and_lineage_link(conn):
    run_id, _ = _start(conn)
    dlq_id = dlq.quarantine(
        conn, run_id=run_id, stage="curate", reason="bad_schema",
        source_ref={"file": "x.csv"}, payload_ref="s3://dlq/x",
        record_count=2, commit=False)
    assert isinstance(dlq_id, str)
    assert conn.execute(
        "SELECT 1 FROM cp.dlq WHERE dlq_id=%s", (dlq_id,)).fetchone() is not None
    link = conn.execute(
        "SELECT 1 FROM cp.lineage_link "
        "WHERE consumer_run_id=%s AND edge_type='quarantine'", (run_id,)
    ).fetchone()
    assert link is not None


def test_replay_mints_new_run_linked_to_original(conn):
    original, orig_wfid = _start(conn, status="failed")
    new_wfid, new_run = dlq.replay(
        conn, original_run_id=original, pipeline_type="ingestion",
        domain="sales", dataset="orders", business_date=BD, commit=False)
    assert new_wfid != orig_wfid
    trigger, replay_of = conn.execute(
        "SELECT trigger_type, replay_of_run_id FROM cp.run_log WHERE run_id=%s",
        (new_run,)).fetchone()
    assert trigger == "replay"
    assert str(replay_of) == original
