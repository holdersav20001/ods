import json
from uuid import uuid4

import pytest
import psycopg

EXPECTED_TABLES = {
    "edge_type", "dataset_config", "file_catalogue", "run_log",
    "run_stage_log", "lineage_link", "lineage_edge",
    "reconciliation_log", "dlq",
}

BD = "2026-05-29"


def test_all_cp_tables_exist(conn):
    rows = conn.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema='cp'"
    ).fetchall()
    present = {r[0] for r in rows}
    assert EXPECTED_TABLES <= present, EXPECTED_TABLES - present


# ---- helpers ----------------------------------------------------------------

def _start_run(conn, workflow_run_id=None, status=None):
    """Create a run via cp.start_run; optionally bump it to a terminal status."""
    wfid = workflow_run_id or str(uuid4())
    run_id = conn.execute(
        "SELECT cp.start_run(%s,'ingestion','sales','orders',%s,'manual')",
        (wfid, BD),
    ).fetchone()[0]
    if status:
        conn.execute(
            "SELECT cp.patch_run(%s, %s)",
            (run_id, json.dumps({"status": status})),
        )
    return run_id, wfid


# ---- start_run --------------------------------------------------------------

def test_start_run_creates_running_row(conn):
    run_id, wfid = _start_run(conn)
    assert run_id is not None
    row = conn.execute(
        "SELECT workflow_run_id, status, trigger_type, business_date, "
        "pipeline_type, domain, dataset FROM cp.run_log WHERE run_id=%s",
        (run_id,),
    ).fetchone()
    assert row is not None
    assert row[0] == wfid
    assert row[1] == "running"
    assert row[2] == "manual"
    assert str(row[3]) == BD
    assert (row[4], row[5], row[6]) == ("ingestion", "sales", "orders")


# ---- register_file ----------------------------------------------------------

def test_register_file_idempotent(conn):
    md5 = "md5-" + uuid4().hex
    fid1 = conn.execute(
        "SELECT cp.register_file(%s,'s3://bucket/raw/a.csv',%s,%s,'sales','orders')",
        (str(uuid4()), md5, BD),
    ).fetchone()[0]
    fid2 = conn.execute(
        "SELECT cp.register_file(%s,'s3://bucket/raw/b.csv',%s,%s,'sales','orders')",
        (str(uuid4()), md5, BD),
    ).fetchone()[0]
    assert fid1 == fid2
    cnt = conn.execute(
        "SELECT count(*) FROM cp.file_catalogue WHERE file_md5=%s AND business_date=%s",
        (md5, BD),
    ).fetchone()[0]
    assert cnt == 1


# ---- write_lineage_link -----------------------------------------------------

def test_write_lineage_link_empty_edges_raises(conn):
    run_id, _ = _start_run(conn)
    with pytest.raises(psycopg.errors.RaiseException):
        conn.execute(
            "SELECT cp.write_lineage_link(%s,'raw_to_curated',%s,%s,%s)",
            (run_id, json.dumps({"content_hash": "h1"}), 10, json.dumps([])),
        )


def test_write_lineage_link_creates_link_and_edges(conn):
    run_id, _ = _start_run(conn)
    up_run, _ = _start_run(conn)
    edges = [
        {"upstream_run_id": str(up_run), "edge_type": "raw_to_curated",
         "source_ref": {"k": 1}, "record_count": 5},
        {"upstream_run_id": str(up_run), "edge_type": "raw_to_curated",
         "source_ref": {"k": 2}, "record_count": 5, "input_slot": 1},
    ]
    link = conn.execute(
        "SELECT cp.write_lineage_link(%s,'raw_to_curated',%s,%s,%s)",
        (run_id, json.dumps({"content_hash": "hh"}), 10, json.dumps(edges)),
    ).fetchone()[0]
    assert link is not None
    n = conn.execute(
        "SELECT count(*) FROM cp.lineage_edge WHERE lineage_link_id=%s", (link,)
    ).fetchone()[0]
    assert n == 2


def test_write_lineage_link_idempotent(conn):
    run_id, _ = _start_run(conn)
    edges = [{"edge_type": "raw_to_curated", "source_ref": {"k": 1}, "record_count": 5}]
    tref = json.dumps({"content_hash": "dup"})
    link1 = conn.execute(
        "SELECT cp.write_lineage_link(%s,'raw_to_curated',%s,%s,%s)",
        (run_id, tref, 10, json.dumps(edges)),
    ).fetchone()[0]
    link2 = conn.execute(
        "SELECT cp.write_lineage_link(%s,'raw_to_curated',%s,%s,%s)",
        (run_id, tref, 10, json.dumps(edges)),
    ).fetchone()[0]
    assert link1 == link2
    n = conn.execute(
        "SELECT count(*) FROM cp.lineage_edge WHERE lineage_link_id=%s", (link1,)
    ).fetchone()[0]
    assert n == 1  # edges not duplicated


# ---- write_link_then_rows ---------------------------------------------------

def test_write_link_then_rows_stamps_rows(conn):
    run_id, wfid = _start_run(conn)
    edges = [{"edge_type": "raw_to_curated", "source_ref": {"k": 1}, "record_count": 2}]
    rows = [{"order_id": 1, "amt": 10}, {"order_id": 2, "amt": 20}]
    link = conn.execute(
        "SELECT cp.write_link_then_rows(%s,'raw_to_curated',%s,%s,%s,%s)",
        (run_id, json.dumps({"content_hash": "rows1"}), 2,
         json.dumps(edges), json.dumps(rows)),
    ).fetchone()[0]
    got = conn.execute(
        "SELECT count(*), count(*) FILTER (WHERE _ods_lineage_link_id=%s "
        "AND _ods_workflow_run_id=%s) FROM ods.orders WHERE _ods_lineage_link_id=%s",
        (link, wfid, link),
    ).fetchone()
    assert got[0] == 2
    assert got[1] == 2


# ---- write_reconciliation_check ---------------------------------------------

@pytest.mark.parametrize(
    "src,acc,disc,status",
    [(100, 100, 0, "ok"), (100, 90, 10, "breach"), (90, 100, -10, "double_count")],
)
def test_write_reconciliation_check(conn, src, acc, disc, status):
    run_id, _ = _start_run(conn)
    conn.execute(
        "SELECT cp.write_reconciliation_check(%s,'row_count',%s,%s)",
        (run_id, src, acc),
    )
    row = conn.execute(
        "SELECT discrepancy, status FROM cp.reconciliation_log WHERE run_id=%s",
        (run_id,),
    ).fetchone()
    assert row[0] == disc
    assert row[1] == status


# ---- quarantine -------------------------------------------------------------

def test_quarantine_creates_dlq_and_lineage(conn):
    run_id, _ = _start_run(conn)
    dlq_id = conn.execute(
        "SELECT cp.quarantine(%s,'curate','bad rows',%s,%s,%s)",
        (run_id, json.dumps({"src": "x"}), "s3://dlq/payload.json", 3),
    ).fetchone()[0]
    assert dlq_id is not None
    dlq_cnt = conn.execute(
        "SELECT count(*) FROM cp.dlq WHERE dlq_id=%s", (dlq_id,)
    ).fetchone()[0]
    assert dlq_cnt == 1
    link = conn.execute(
        "SELECT lineage_link_id FROM cp.lineage_link "
        "WHERE consumer_run_id=%s AND edge_type='quarantine'",
        (run_id,),
    ).fetchone()
    assert link is not None
    edge_cnt = conn.execute(
        "SELECT count(*) FROM cp.lineage_edge "
        "WHERE lineage_link_id=%s AND edge_type='quarantine'",
        (link[0],),
    ).fetchone()[0]
    assert edge_cnt == 1


# ---- latest_succeeded_run ---------------------------------------------------

def test_latest_succeeded_run(conn):
    # no succeeded runs yet -> null
    assert conn.execute(
        "SELECT cp.latest_succeeded_run('sales','orders',%s,'ingestion')", (BD,)
    ).fetchone()[0] is None

    _start_run(conn, status="running")
    _start_run(conn, status="failed")
    older, _ = _start_run(conn, status="succeeded")
    newer, _ = _start_run(conn, status="succeeded")
    # ensure newer has a strictly later finished_at
    conn.execute(
        "UPDATE cp.run_log SET finished_at = now() + interval '1 hour' WHERE run_id=%s",
        (newer,),
    )
    got = conn.execute(
        "SELECT cp.latest_succeeded_run('sales','orders',%s,'ingestion')", (BD,)
    ).fetchone()[0]
    assert got == newer


# ---- start_stage / finish_stage / patch_run --------------------------------

def test_start_finish_stage_roundtrip(conn):
    run_id, _ = _start_run(conn)
    sid = conn.execute(
        "SELECT cp.start_stage(%s,'curate',1)", (run_id,)
    ).fetchone()[0]
    assert sid is not None
    st = conn.execute(
        "SELECT status FROM cp.run_stage_log WHERE stage_log_id=%s", (sid,)
    ).fetchone()[0]
    assert st == "running"
    conn.execute(
        "SELECT cp.finish_stage(%s,'succeeded',%s,%s,%s)",
        (sid, 100, 95, json.dumps({"dropped": 5})),
    )
    row = conn.execute(
        "SELECT status, record_count_in, record_count_out, metrics, finished_at "
        "FROM cp.run_stage_log WHERE stage_log_id=%s",
        (sid,),
    ).fetchone()
    assert row[0] == "succeeded"
    assert row[1] == 100
    assert row[2] == 95
    assert row[3] == {"dropped": 5}
    assert row[4] is not None


def test_patch_run_whitelist_and_terminal(conn):
    run_id, _ = _start_run(conn)
    # non-whitelisted key ignored; whitelisted applied
    conn.execute(
        "SELECT cp.patch_run(%s,%s)",
        (run_id, json.dumps({"record_count_in": 42, "domain": "HACK"})),
    )
    row = conn.execute(
        "SELECT record_count_in, domain, finished_at FROM cp.run_log WHERE run_id=%s",
        (run_id,),
    ).fetchone()
    assert row[0] == 42
    assert row[1] == "sales"   # not overwritten
    assert row[2] is None      # not terminal yet

    conn.execute(
        "SELECT cp.patch_run(%s,%s)",
        (run_id, json.dumps({"status": "succeeded", "record_count_out": 40})),
    )
    row = conn.execute(
        "SELECT status, record_count_out, finished_at FROM cp.run_log WHERE run_id=%s",
        (run_id,),
    ).fetchone()
    assert row[0] == "succeeded"
    assert row[1] == 40
    assert row[2] is not None   # terminal sets finished_at
