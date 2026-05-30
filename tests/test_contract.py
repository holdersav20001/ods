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
        "SELECT cp.register_file('s3://bucket/raw/a.csv',%s,%s,'sales','orders')",
        (md5, BD),
    ).fetchone()[0]
    fid2 = conn.execute(
        "SELECT cp.register_file('s3://bucket/raw/b.csv',%s,%s,'sales','orders')",
        (md5, BD),
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


def test_write_link_then_rows_unknown_run_raises(conn):
    bogus = str(uuid4())  # no run_log row -> v_dataset lookup yields NULL
    edges = [{"edge_type": "raw_to_curated", "source_ref": {"k": 1}, "record_count": 1}]
    rows = [{"order_id": 1}]
    with pytest.raises(psycopg.errors.RaiseException, match="no run_log row"):
        conn.execute(
            "SELECT cp.write_link_then_rows(%s,'raw_to_curated',%s,%s,%s,%s)",
            (bogus, json.dumps({"content_hash": "bogus1"}), 1,
             json.dumps(edges), json.dumps(rows)),
        )


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


def test_quarantine_distinct_failures_dont_collapse(conn):
    # Two quarantine events in the SAME run with the SAME payload_ref must NOT
    # collapse to one lineage_link (content_hash must be discriminated by dlq_id).
    run_id, _ = _start_run(conn)
    ref = "s3://dlq/same.json"
    d1 = conn.execute(
        "SELECT cp.quarantine(%s,'curate','bad A',%s,%s,%s)",
        (run_id, json.dumps({"src": "a"}), ref, 2),
    ).fetchone()[0]
    d2 = conn.execute(
        "SELECT cp.quarantine(%s,'curate','bad B',%s,%s,%s)",
        (run_id, json.dumps({"src": "b"}), ref, 3),
    ).fetchone()[0]
    assert d1 != d2
    dlq_cnt = conn.execute(
        "SELECT count(*) FROM cp.dlq WHERE run_id=%s", (run_id,)
    ).fetchone()[0]
    assert dlq_cnt == 2
    link_cnt = conn.execute(
        "SELECT count(*) FROM cp.lineage_link "
        "WHERE consumer_run_id=%s AND edge_type='quarantine'",
        (run_id,),
    ).fetchone()[0]
    assert link_cnt == 2, "distinct quarantine events collapsed to one link"
    # each link has exactly one edge
    edge_cnt = conn.execute(
        "SELECT count(*) FROM cp.lineage_edge e JOIN cp.lineage_link l "
        "ON l.lineage_link_id=e.lineage_link_id "
        "WHERE l.consumer_run_id=%s AND l.edge_type='quarantine'",
        (run_id,),
    ).fetchone()[0]
    assert edge_cnt == 2


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


# ---- succeeded_runs ---------------------------------------------------------

def test_succeeded_runs_returns_all_succeeded_newest_first(conn):
    # no succeeded runs yet -> empty set
    assert conn.execute(
        "SELECT array_agg(r) FROM cp.succeeded_runs('sales','orders',%s,'ingestion') r",
        (BD,)
    ).fetchone()[0] is None

    _start_run(conn, status="failed")           # excluded: failed
    a, _ = _start_run(conn, status="succeeded")
    b, _ = _start_run(conn, status="succeeded")
    # b strictly newer than a so ordering is deterministic
    conn.execute(
        "UPDATE cp.run_log SET finished_at = now() + interval '1 hour' WHERE run_id=%s",
        (b,),
    )
    got = [r[0] for r in conn.execute(
        "SELECT cp.succeeded_runs('sales','orders',%s,'ingestion')", (BD,)
    ).fetchall()]
    assert got == [b, a]   # newest-first, failed excluded


# ---- run_output_link --------------------------------------------------------

def test_run_output_link_output_identity_contract(conn):
    """cp.run_output_link contract (migration 010 / audit F1): OUTPUT-IDENTITY
    discovery, no random pick.
      * no output of that edge_type      -> RAISES (not NULL).
      * exactly one output               -> that link.
      * multiple outputs, no target_path -> RAISES (ambiguous).
      * target_path given                -> the EXACT matching output.
    """
    import psycopg
    run_id, _ = _start_run(conn)

    # No link of that edge_type -> RAISE (was: returned NULL).
    conn.execute("SAVEPOINT c_none")
    with pytest.raises(psycopg.errors.RaiseException, match="no raw_to_curated"):
        conn.execute("SELECT cp.run_output_link(%s,'raw_to_curated')", (run_id,)).fetchone()
    conn.execute("ROLLBACK TO SAVEPOINT c_none")

    edges = [{"edge_type": "raw_to_curated", "source_ref": {"k": 1}, "record_count": 1}]
    l1 = conn.execute(
        "SELECT cp.write_lineage_link(%s,'raw_to_curated',%s,%s,%s)",
        (run_id, json.dumps({"path": "p1", "content_hash": "h1"}), 1, json.dumps(edges)),
    ).fetchone()[0]
    # Exactly one output -> returns that link (no target_path needed).
    got = conn.execute(
        "SELECT cp.run_output_link(%s,'raw_to_curated')", (run_id,)
    ).fetchone()[0]
    assert str(got) == str(l1)

    # Same run, a DIFFERENT output (distinct path) -> two links.
    l2 = conn.execute(
        "SELECT cp.write_lineage_link(%s,'raw_to_curated',%s,%s,%s)",
        (run_id, json.dumps({"path": "p2", "content_hash": "h2"}), 1, json.dumps(edges)),
    ).fetchone()[0]
    # Ambiguous (no target_path) -> RAISES instead of arbitrarily picking.
    conn.execute("SAVEPOINT c_ambig")
    with pytest.raises(psycopg.errors.RaiseException, match="ambiguous"):
        conn.execute("SELECT cp.run_output_link(%s,'raw_to_curated')", (run_id,)).fetchone()
    conn.execute("ROLLBACK TO SAVEPOINT c_ambig")
    # Each output addressable by its EXACT path.
    assert str(conn.execute(
        "SELECT cp.run_output_link(%s,'raw_to_curated','p1')", (run_id,)
    ).fetchone()[0]) == str(l1)
    assert str(conn.execute(
        "SELECT cp.run_output_link(%s,'raw_to_curated','p2')", (run_id,)
    ).fetchone()[0]) == str(l2)

    # A different edge_type the run never produced -> RAISES.
    conn.execute("SAVEPOINT c_other")
    with pytest.raises(psycopg.errors.RaiseException, match="no canonical_to_sink"):
        conn.execute("SELECT cp.run_output_link(%s,'canonical_to_sink')", (run_id,)).fetchone()
    conn.execute("ROLLBACK TO SAVEPOINT c_other")


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


# ---- P1 EXIT GATE: exhaustiveness + column-drift guards ---------------------

def test_every_cp_function_is_asserted(conn):
    fns = {r[0] for r in conn.execute(
        "SELECT proname FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace "
        "WHERE n.nspname='cp' AND p.prokind='f'").fetchall()}
    # ASSERTED is maintained by hand as each fn gets a round-trip test.
    ASSERTED = {
        "start_run", "patch_run", "register_file", "start_stage", "finish_stage",
        "write_lineage_link", "write_link_then_rows", "write_reconciliation_check",
        "quarantine", "latest_succeeded_run", "succeeded_runs", "run_output_link",
        # F7: graph-derived sink recon — asserted in tests/test_graph_recon.py.
        "reconcile_sink",
    }
    missing = fns - ASSERTED
    assert not missing, f"cp functions with no contract assertion: {missing}"


def _columns(conn, table):
    return {r[0] for r in conn.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema='cp' AND table_name=%s", (table,)).fetchall()}


def test_start_run_returns_run_log_row_with_loadbearing_columns(conn):
    run_id, _ = _start_run(conn)
    # returned run_id resolves to a cp.run_log row
    assert conn.execute(
        "SELECT 1 FROM cp.run_log WHERE run_id=%s", (run_id,)
    ).fetchone() is not None
    required = {
        "run_id", "workflow_run_id", "trigger_type", "replay_of_run_id",
        "pipeline_type", "domain", "dataset", "business_date", "file_id",
        "status", "record_count_in", "record_count_out", "error",
        "started_at", "finished_at",
    }
    cols = _columns(conn, "run_log")
    assert required <= cols, f"run_log missing load-bearing columns: {required - cols}"


def test_write_lineage_link_tables_have_loadbearing_columns(conn):
    link_required = {
        "lineage_link_id", "consumer_run_id", "edge_type", "sink_type",
        "target_ref", "transform_version", "record_count", "created_at",
    }
    edge_required = {
        "lineage_edge_id", "lineage_link_id", "upstream_run_id", "source_file_id",
        "input_slot", "edge_type", "source_ref", "record_count",
    }
    link_cols = _columns(conn, "lineage_link")
    edge_cols = _columns(conn, "lineage_edge")
    assert link_required <= link_cols, f"lineage_link missing: {link_required - link_cols}"
    assert edge_required <= edge_cols, f"lineage_edge missing: {edge_required - edge_cols}"
