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


def _file(conn):
    """A real registered raw file, to anchor a well-formed raw_to_curated edge
    (012 raw_edge_requires_source_file)."""
    return runs.register_file(
        conn, s3_raw_path=f"s3://raw/{uuid.uuid4()}.csv",
        file_md5=uuid.uuid4().hex, business_date=BD,
        domain="sales", dataset="orders", commit=False)


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
    # Discovery orders by (finished_at DESC NULLS LAST, run_id DESC). Since the
    # 007 fix, finished_at is stamped with clock_timestamp(), which ADVANCES
    # within a transaction — so the SECOND run finalised has a strictly newer
    # finished_at and discovery deterministically returns it (even in-txn),
    # regardless of the random run_id ordering. (Pre-007 this had to fall back
    # to max(run_id) because both shared the txn-fixed now().)
    assert got == second


# ---- runs.succeeded_runs ----------------------------------------------------

def test_succeeded_runs_returns_all_for_key_newest_first(conn):
    # 3 succeeded ingestion runs for the (sales/orders/BD) key ...
    r1, _ = _start(conn, status="succeeded")
    r2, _ = _start(conn, status="succeeded")
    r3, _ = _start(conn, status="succeeded")
    # ... force a strict finished_at ordering so newest-first is deterministic.
    for offset, rid in ((1, r1), (2, r2), (3, r3)):
        conn.execute(
            "UPDATE cp.run_log SET finished_at = now() + (%s||' hour')::interval "
            "WHERE run_id=%s", (offset, rid))
    # 1 FAILED run for the same key (must be excluded) ...
    _start(conn, status="failed")
    # ... and 1 SUCCEEDED run for a DIFFERENT dataset (must be excluded).
    other = runs.start(
        conn, workflow_run_id=str(uuid.uuid4()), pipeline_type="ingestion",
        domain="sales", dataset="returns", business_date=BD,
        trigger_type="manual", commit=False)
    runs.finalise(conn, other, status="succeeded", commit=False)

    got = runs.succeeded_runs(
        conn, domain="sales", dataset="orders",
        business_date=BD, pipeline_type="ingestion")
    # exactly the 3, newest-first (r3 has the latest finished_at), no failed/other
    assert got == [r3, r2, r1]
    assert other not in got


# ---- lineage.write_link -----------------------------------------------------

def test_write_link_creates_link_and_edges(conn):
    run_id, _ = _start(conn)
    edges = [
        {"edge_type": "raw_to_curated", "source_file_id": str(_file(conn)),
         "record_count": 5},
        {"edge_type": "raw_to_curated", "source_file_id": str(_file(conn)),
         "record_count": 3},
    ]
    link_id = lineage.write_link(
        conn, consumer_run_id=run_id, edge_type="raw_to_curated",
        target_ref={"path": "s3://curated/abc", "content_hash": "abc",
                    "version": 1}, record_count=8, edges=edges,
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
            target_ref={"path": "s3://c/x", "content_hash": "x", "version": 1},
            record_count=0, edges=[],
            commit=False)


def test_write_link_idempotent_on_content_hash(conn):
    run_id, _ = _start(conn)
    edges = [{"edge_type": "raw_to_curated",
              "source_file_id": str(_file(conn)), "record_count": 1}]
    tref = {"path": "s3://curated/dup", "content_hash": "dup", "version": 1}
    first = lineage.write_link(
        conn, consumer_run_id=run_id, edge_type="raw_to_curated",
        target_ref=tref, record_count=1, edges=edges,
        commit=False)
    second = lineage.write_link(
        conn, consumer_run_id=run_id, edge_type="raw_to_curated",
        target_ref=tref, record_count=1, edges=edges,
        commit=False)
    assert first == second
    n_edges = conn.execute(
        "SELECT count(*) FROM cp.lineage_edge WHERE lineage_link_id=%s",
        (first,)).fetchone()[0]
    assert n_edges == 1


# ---- lineage.write_link_then_rows ------------------------------------------

def test_write_link_then_rows_stamps_target_rows(conn):
    run_id, wfid = _start(conn)
    # A run-to-run edge (curated_to_canonical) must name its exact upstream
    # output link (009 CHECK). Mint a minimal upstream raw_to_curated link.
    up_link = lineage.write_link(
        conn, consumer_run_id=run_id, edge_type="raw_to_curated",
        target_ref={"path": "s3://curated/up", "content_hash": "up1",
                    "version": 1},
        record_count=3,
        edges=[{"edge_type": "raw_to_curated",
                "source_file_id": str(_file(conn)), "record_count": 3}],
        commit=False)
    rows = [{"id": 1}, {"id": 2}, {"id": 3}]
    edges = [{"upstream_lineage_link_id": up_link,
              "edge_type": "curated_to_canonical", "record_count": 3}]
    link_id = lineage.write_link_then_rows(
        conn, consumer_run_id=run_id, edge_type="curated_to_canonical",
        target_ref={"path": "s3://canon/rows1", "content_hash": "rows1",
                    "version": 1}, record_count=3,
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


# ---- runs.register_file -----------------------------------------------------

def test_register_file_round_trip(conn):
    md5 = "md5-" + uuid.uuid4().hex
    file_id = runs.register_file(
        conn, s3_raw_path="s3://raw/orders/f.csv", file_md5=md5,
        business_date=BD, domain="sales", dataset="orders", commit=False)
    assert isinstance(file_id, str)
    path, fmd5, bd, dom, ds = conn.execute(
        "SELECT s3_raw_path, file_md5, business_date, domain, dataset "
        "FROM cp.file_catalogue WHERE file_id=%s", (file_id,)).fetchone()
    assert (path, fmd5, bd, dom, ds) == (
        "s3://raw/orders/f.csv", md5, BD, "sales", "orders")


def test_register_file_idempotent_on_md5_business_date(conn):
    md5 = "md5-" + uuid.uuid4().hex
    first = runs.register_file(
        conn, s3_raw_path="s3://raw/orders/f.csv", file_md5=md5,
        business_date=BD, domain="sales", dataset="orders", commit=False)
    second = runs.register_file(
        conn, s3_raw_path="s3://raw/orders/f.csv", file_md5=md5,
        business_date=BD, domain="sales", dataset="orders", commit=False)
    assert first == second
