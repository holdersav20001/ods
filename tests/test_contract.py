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


def _file(conn):
    """A real registered raw file to anchor a well-formed raw_to_curated edge
    (012 raw_edge_requires_source_file)."""
    return conn.execute(
        "SELECT cp.register_file(%s,%s,%s,'sales','orders')",
        (f"s3://raw/{uuid4()}.csv", uuid4().hex, BD),
    ).fetchone()[0]


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
         "source_file_id": str(_file(conn)),
         "source_ref": {"k": 1}, "record_count": 5},
        {"upstream_run_id": str(up_run), "edge_type": "raw_to_curated",
         "source_file_id": str(_file(conn)),
         "source_ref": {"k": 2}, "record_count": 5, "input_slot": 1},
    ]
    link = conn.execute(
        "SELECT cp.write_lineage_link(%s,'raw_to_curated',%s,%s,%s)",
        (run_id, json.dumps({"path": "s3://c/hh", "content_hash": "hh",
                             "version": 1}), 10, json.dumps(edges)),
    ).fetchone()[0]
    assert link is not None
    n = conn.execute(
        "SELECT count(*) FROM cp.lineage_edge WHERE lineage_link_id=%s", (link,)
    ).fetchone()[0]
    assert n == 2


def test_write_lineage_link_idempotent(conn):
    run_id, _ = _start_run(conn)
    edges = [{"edge_type": "raw_to_curated", "source_file_id": str(_file(conn)),
              "source_ref": {"k": 1}, "record_count": 5}]
    tref = json.dumps({"path": "s3://c/dup", "content_hash": "dup", "version": 1})
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
    edges = [{"edge_type": "raw_to_curated", "source_file_id": str(_file(conn)),
              "source_ref": {"k": 1}, "record_count": 2}]
    rows = [{"order_id": 1, "amt": 10}, {"order_id": 2, "amt": 20}]
    link = conn.execute(
        "SELECT cp.write_link_then_rows(%s,'raw_to_curated',%s,%s,%s,%s)",
        (run_id, json.dumps({"path": "s3://c/rows1", "content_hash": "rows1",
                             "version": 1}), 2,
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
    """SCOPE (A3 audit 2026-06-03): function-contract test for the status math of
    cp.write_reconciliation_check on caller-supplied (source, accounted) numbers —
    NOT a real-loss detector (it never queries actual stamped rows). The
    DB-derived, falsifiable recon contract is covered separately by
    test_contract.py::test_reconcile_sink_link_per_output_roundtrip and the
    cross-hop reconcile_workflow tests."""
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


# ---- resolve_dlq (023) ------------------------------------------------------

def test_resolve_dlq_roundtrip(conn):
    """cp.resolve_dlq (migration 023): flips status + resolution refs and leaves
    failed_payload/reason untouched. Full lifecycle coverage in
    tests/test_dlq_lifecycle.py."""
    run_id, _ = _start_run(conn)
    dlq_id = conn.execute(
        "SELECT cp.quarantine(%s,'validate','bad',%s,%s,%s,%s)",
        (run_id, json.dumps({}), "s3://dlq/c.json", 1, json.dumps({"x": 1})),
    ).fetchone()[0]
    conn.execute("SELECT cp.resolve_dlq(%s,'resolved',%s)", (dlq_id, run_id))
    row = conn.execute(
        "SELECT status, failed_payload, resolved_by_run_id FROM cp.dlq WHERE dlq_id=%s",
        (dlq_id,)
    ).fetchone()
    assert row[0] == "resolved"
    assert row[1] == {"x": 1}                 # preserved
    assert str(row[2]) == str(run_id)


def test_resolve_dlq_terminal_requires_traceable_ref(conn):
    """P2c (migration 029): a TERMINAL resolution ('resolved'/'replayed') with NO
    effective resolved_by_run_id AND NO resolved_by_output_link_id is untraceable
    and must RAISE. With a ref it succeeds. 'rejected' stays lenient. The fix never
    touches failed_payload/reason."""
    run_id, _ = _start_run(conn)
    dlq_id = conn.execute(
        "SELECT cp.quarantine(%s,'validate','bad',%s,%s,%s,%s)",
        (run_id, json.dumps({}), "s3://dlq/p2c.json", 1, json.dumps({"x": 1})),
    ).fetchone()[0]

    # resolved with NO refs -> RAISES (untraceable terminal resolution).
    conn.execute("SAVEPOINT c_noref")
    with pytest.raises(psycopg.errors.RaiseException, match="traceable"):
        conn.execute("SELECT cp.resolve_dlq(%s,'resolved')", (dlq_id,))
    conn.execute("ROLLBACK TO SAVEPOINT c_noref")

    # replayed with NO refs -> also RAISES (the other terminal state).
    conn.execute("SAVEPOINT c_noref2")
    with pytest.raises(psycopg.errors.RaiseException, match="traceable"):
        conn.execute("SELECT cp.resolve_dlq(%s,'replayed')", (dlq_id,))
    conn.execute("ROLLBACK TO SAVEPOINT c_noref2")

    # 'rejected' stays lenient (no ref required) and preserves history.
    conn.execute("SELECT cp.resolve_dlq(%s,'rejected')", (dlq_id,))
    rej = conn.execute(
        "SELECT status, failed_payload FROM cp.dlq WHERE dlq_id=%s", (dlq_id,)
    ).fetchone()
    assert rej == ("rejected", {"x": 1})

    # With a ref, a terminal resolution succeeds.
    conn.execute("SELECT cp.resolve_dlq(%s,'resolved',%s)", (dlq_id, run_id))
    ok = conn.execute(
        "SELECT status, failed_payload, resolved_by_run_id FROM cp.dlq WHERE dlq_id=%s",
        (dlq_id,)
    ).fetchone()
    assert ok[0] == "resolved"
    assert ok[1] == {"x": 1}                  # history never touched
    assert str(ok[2]) == str(run_id)

    # An effective ref ALREADY on the row (from a prior corrected step) lets a
    # later terminal resolution with no NEW ref succeed (uses the existing ref).
    run2, _ = _start_run(conn)
    dlq2 = conn.execute(
        "SELECT cp.quarantine(%s,'validate','bad2',%s,%s,%s,%s)",
        (run2, json.dumps({}), "s3://dlq/p2c2.json", 1, json.dumps({"y": 2})),
    ).fetchone()[0]
    # set a ref via a (lenient) corrected step first
    conn.execute("SELECT cp.resolve_dlq(%s,'corrected',%s)", (dlq2, run2))
    # now resolve with NO new ref -> the existing ref makes it traceable -> ok.
    conn.execute("SELECT cp.resolve_dlq(%s,'resolved')", (dlq2,))
    assert conn.execute(
        "SELECT status FROM cp.dlq WHERE dlq_id=%s", (dlq2,)
    ).fetchone()[0] == "resolved"


# ---- get_schema_contract (024) ----------------------------------------------

def test_get_schema_contract_roundtrip(conn):
    """cp.get_schema_contract (migration 024): exact-version fetch and
    latest-when-null. Validation logic lives in control/schema.py
    (tests/test_schema_contract.py)."""
    dom, ds, layer = "insurance", "claim", "silver"
    conn.execute(
        "INSERT INTO cp.schema_contract (domain,dataset,layer,schema_version,"
        "required_columns,nullable_columns,business_key) VALUES "
        "(%s,%s,%s,'claim.v1',%s,%s,%s),(%s,%s,%s,'claim.v2',%s,%s,%s)",
        (dom, ds, layer, json.dumps(["policy_id"]), json.dumps([]),
         json.dumps(["policy_id"]),
         dom, ds, layer, json.dumps(["policy_id", "claim_id"]), json.dumps([]),
         json.dumps(["policy_id", "claim_id"])),
    )
    # exact version
    row = conn.execute(
        "SELECT (c).schema_version, (c).required_columns "
        "FROM cp.get_schema_contract(%s,%s,%s,'claim.v1') c", (dom, ds, layer)
    ).fetchone()
    assert row[0] == "claim.v1"
    assert row[1] == ["policy_id"]
    # latest when version null -> v2 (DESC by schema_version)
    latest = conn.execute(
        "SELECT (c).schema_version FROM cp.get_schema_contract(%s,%s,%s) c",
        (dom, ds, layer)
    ).fetchone()[0]
    assert latest == "claim.v2"
    # no match -> composite row of NULLs (PK null)
    none_row = conn.execute(
        "SELECT (c).schema_contract_id FROM cp.get_schema_contract('x','y','z') c"
    ).fetchone()[0]
    assert none_row is None


def test_get_schema_contract_latest_is_numeric_not_text(conn):
    """P3 (migration 029): 'latest' must be the highest NUMERIC semver suffix, not
    text order. Insert claim.v9 and claim.v10 — text DESC wrongly picks v9
    ('claim.v9' > 'claim.v10' lexically); the fix must pick v10."""
    dom, ds, layer = "insurance", "claim_p3", "silver"
    conn.execute(
        "INSERT INTO cp.schema_contract (domain,dataset,layer,schema_version) "
        "VALUES (%s,%s,%s,'claim.v9'),(%s,%s,%s,'claim.v10')",
        (dom, ds, layer, dom, ds, layer),
    )
    latest = conn.execute(
        "SELECT (c).schema_version FROM cp.get_schema_contract(%s,%s,%s) c",
        (dom, ds, layer),
    ).fetchone()[0]
    assert latest == "claim.v10", f"latest picked {latest}, expected claim.v10"
    # exact-version path still honours the request verbatim.
    exact = conn.execute(
        "SELECT (c).schema_version FROM cp.get_schema_contract(%s,%s,%s,'claim.v9') c",
        (dom, ds, layer),
    ).fetchone()[0]
    assert exact == "claim.v9"


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

    edges = [{"edge_type": "raw_to_curated", "source_file_id": str(_file(conn)),
              "source_ref": {"k": 1}, "record_count": 1}]
    l1 = conn.execute(
        "SELECT cp.write_lineage_link(%s,'raw_to_curated',%s,%s,%s)",
        (run_id, json.dumps({"path": "p1", "content_hash": "h1", "version": 1}),
         1, json.dumps(edges)),
    ).fetchone()[0]
    # Exactly one output -> returns that link (no target_path needed).
    got = conn.execute(
        "SELECT cp.run_output_link(%s,'raw_to_curated')", (run_id,)
    ).fetchone()[0]
    assert str(got) == str(l1)

    # Same run, a DIFFERENT output (distinct path) -> two links.
    l2 = conn.execute(
        "SELECT cp.write_lineage_link(%s,'raw_to_curated',%s,%s,%s)",
        (run_id, json.dumps({"path": "p2", "content_hash": "h2", "version": 1}),
         1, json.dumps(edges)),
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


def test_run_output_link_content_hash_exact_selector(conn):
    """cp.run_output_link content_hash disambiguator (migration 015 / THEME B):
    two links at the SAME path with different content_hash. The path branch is
    EXACT-or-RAISE: path alone over >1 link RAISES 'ambiguous — pass
    p_content_hash'; content_hash (with or without path) resolves the EXACT one;
    a content_hash that matches nothing RAISES."""
    run_id, _ = _start_run(conn)
    edges = [{"edge_type": "raw_to_curated", "source_file_id": str(_file(conn)),
              "source_ref": {"k": 1}, "record_count": 1}]
    path = "s3://c/same"
    l_old = conn.execute(
        "SELECT cp.write_lineage_link(%s,'raw_to_curated',%s,%s,%s)",
        (run_id, json.dumps({"path": path, "content_hash": "ho", "version": 1}),
         1, json.dumps(edges))).fetchone()[0]
    l_new = conn.execute(
        "SELECT cp.write_lineage_link(%s,'raw_to_curated',%s,%s,%s)",
        (run_id, json.dumps({"path": path, "content_hash": "hn", "version": 2}),
         1, json.dumps(edges))).fetchone()[0]
    assert l_old != l_new

    # path alone over two links -> RAISE (no silent pick).
    conn.execute("SAVEPOINT c_pa")
    with pytest.raises(psycopg.errors.RaiseException, match="ambiguous"):
        conn.execute("SELECT cp.run_output_link(%s,'raw_to_curated',%s)",
                     (run_id, path)).fetchone()
    conn.execute("ROLLBACK TO SAVEPOINT c_pa")

    # path + content_hash -> EXACT.
    assert str(conn.execute(
        "SELECT cp.run_output_link(%s,'raw_to_curated',%s,'ho')", (run_id, path)
    ).fetchone()[0]) == str(l_old)
    # content_hash alone (path NULL) -> EXACT.
    assert str(conn.execute(
        "SELECT cp.run_output_link(%s,'raw_to_curated',NULL,'hn')", (run_id,)
    ).fetchone()[0]) == str(l_new)
    # content_hash matching nothing -> RAISE.
    conn.execute("SAVEPOINT c_none_ch")
    with pytest.raises(psycopg.errors.RaiseException, match="no raw_to_curated"):
        conn.execute("SELECT cp.run_output_link(%s,'raw_to_curated',NULL,'nope')",
                     (run_id,)).fetchone()
    conn.execute("ROLLBACK TO SAVEPOINT c_none_ch")


# ---- reconcile_sink_link (per-output) ---------------------------------------

def test_reconcile_sink_link_per_output_roundtrip(conn):
    """cp.reconcile_sink_link (migration 015 / THEME E): accounted is the count
    of ods.<dataset> rows stamped with THIS link only; status satisfies the 014
    recon_internally_consistent CHECK."""
    run_id, _ = _start_run(conn)
    up_run, _ = _start_run(conn)
    # a real upstream link to anchor the canonical_to_sink edge (edge_must_anchor
    # requires source_file_id or upstream_lineage_link_id).
    up_link = conn.execute(
        "SELECT cp.write_lineage_link(%s,'raw_to_curated',%s,%s,%s)",
        (up_run, json.dumps({"path": "s3://cur/sl", "content_hash": "slup",
                             "version": 1}), 3,
         json.dumps([{"edge_type": "raw_to_curated",
                      "source_file_id": str(_file(conn)),
                      "source_ref": {}, "record_count": 3}]))).fetchone()[0]
    edges = [{"edge_type": "canonical_to_sink", "upstream_run_id": str(up_run),
              "upstream_lineage_link_id": str(up_link),
              "source_ref": {"k": 1}, "record_count": 3}]
    rows = [{"order_id": i} for i in range(3)]
    link = conn.execute(
        "SELECT cp.write_link_then_rows(%s,'canonical_to_sink',%s,%s,%s,%s,'postgres')",
        (run_id, json.dumps({"path": "pg://orders", "content_hash": "sl1",
                             "version": 1}), 3,
         json.dumps(edges), json.dumps(rows))).fetchone()[0]
    # exact match -> ok
    conn.execute("SELECT cp.reconcile_sink_link(%s,%s)", (link, 3))
    row = conn.execute(
        "SELECT source_count, accounted_count, discrepancy, status, check_type, "
        "run_id, metrics FROM cp.reconciliation_log "
        "WHERE check_type='sink_link' AND metrics->>'lineage_link_id'=%s",
        (str(link),)).fetchone()
    assert row[0] == 3 and row[1] == 3 and row[2] == 0 and row[3] == "ok"
    assert row[4] == "sink_link"
    assert str(row[5]) == str(run_id)  # run_id = the link's consumer_run_id
    assert row[6]["graph_derived"] is True
    # over-claimed source -> breach (3 source, only this link's 3 accounted... )
    conn.execute("SELECT cp.reconcile_sink_link(%s,%s)", (link, 5))
    breach = conn.execute(
        "SELECT discrepancy, status FROM cp.reconciliation_log "
        "WHERE check_type='sink_link' AND metrics->>'lineage_link_id'=%s "
        "ORDER BY recon_id DESC LIMIT 1", (str(link),)).fetchone()
    assert breach == (2, "breach")


# ---- reconcile_workflow (cross-hop) -----------------------------------------

def test_reconcile_workflow_cross_hop_roundtrip(conn):
    """cp.reconcile_workflow (migration 015 / THEME F): raw_in (SUM raw_to_curated
    edge counts over the workflow's runs) vs accounted (sink rows + dlq).
    Balanced single-hop workflow reconciles ok and satisfies the 014 CHECK."""
    wf = str(uuid4())
    fid = _file(conn)
    ir = conn.execute(
        "INSERT INTO cp.run_log (workflow_run_id,pipeline_type,domain,dataset,"
        "business_date,trigger_type,status,file_id) "
        "VALUES (%s,'ingestion','sales','orders',%s,'manual','succeeded',%s) "
        "RETURNING run_id", (wf, BD, fid)).fetchone()[0]
    raw_edges = [{"edge_type": "raw_to_curated", "source_file_id": str(fid),
                  "source_ref": {}, "record_count": 4}]
    raw_link = conn.execute(
        "SELECT cp.write_lineage_link(%s,'raw_to_curated',%s,%s,%s)",
        (ir, json.dumps({"path": "s3://c/wf", "content_hash": "wfc", "version": 1}),
         4, json.dumps(raw_edges))).fetchone()[0]
    cr = conn.execute(
        "INSERT INTO cp.run_log (workflow_run_id,pipeline_type,domain,dataset,"
        "business_date,trigger_type,status) "
        "VALUES (%s,'canonicalize','sales','orders',%s,'manual','succeeded') "
        "RETURNING run_id", (wf, BD)).fetchone()[0]
    canon = conn.execute(
        "SELECT cp.write_lineage_link(%s,'curated_to_canonical',%s,%s,%s)",
        (cr, json.dumps({"path": "s3://can/wf", "content_hash": "wfcan",
                         "version": 1}), 4,
         json.dumps([{"edge_type": "curated_to_canonical",
                      "upstream_run_id": str(ir),
                      "upstream_lineage_link_id": str(raw_link),
                      "source_ref": {}, "record_count": 4}]))).fetchone()[0]
    sr = conn.execute(
        "INSERT INTO cp.run_log (workflow_run_id,pipeline_type,domain,dataset,"
        "business_date,trigger_type,status) "
        "VALUES (%s,'sink','sales','orders',%s,'manual','succeeded') RETURNING run_id",
        (wf, BD)).fetchone()[0]
    conn.execute(
        "SELECT cp.write_link_then_rows(%s,'canonical_to_sink',%s,%s,%s,%s,'postgres')",
        (sr, json.dumps({"path": "pg://wf", "content_hash": "wfs", "version": 1}), 4,
         json.dumps([{"edge_type": "canonical_to_sink", "upstream_run_id": str(cr),
                      "upstream_lineage_link_id": str(canon),
                      "source_ref": {}, "record_count": 4}]),
         json.dumps([{"order_id": i} for i in range(4)])))

    conn.execute("SELECT cp.reconcile_workflow(%s)", (wf,))
    row = conn.execute(
        "SELECT source_count, accounted_count, discrepancy, status, metrics "
        "FROM cp.reconciliation_log WHERE check_type='workflow' "
        "AND metrics->>'workflow_run_id'=%s", (wf,)).fetchone()
    assert row[0] == 4 and row[1] == 4 and row[2] == 0 and row[3] == "ok"
    assert row[4]["raw_in"] == 4 and row[4]["sink_out"] == 4 and row[4]["dlq_out"] == 0


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
        # P10-C (015): per-OUTPUT sink recon (THEME E) + cross-hop workflow recon
        # (THEME F). Round-trips below; flipped probes in test_team_r1/r3.
        "reconcile_sink_link", "reconcile_workflow",
        # P10-B (014): BEFORE INSERT trigger fn enforcing edge_type == link type
        # at the table — asserted in tests/test_team_r3.py (test_FIXED_* probes)
        # and tests/test_team_r4.py.
        "trg_edge_type_matches_link",
            # P10-D (016): target-visibility active-slice activation primitive —
            # round-trip + invariants in tests/test_target_visibility.py.
        "activate_target_visibility",
            # 022: dashboard/developer read APIs — round-trips and exception
        # handling asserted in tests/test_dashboard_developer_functions.py.
        "dashboard_workflows", "dashboard_workflow_detail",
        "dashboard_output_trace", "developer_diagnostics",
        # 023: DLQ lifecycle resolution — round-trip below
        # (test_resolve_dlq_*). cp.quarantine re-declared in 023 (same name).
        "resolve_dlq",
        # 024: schema-validation contract fetch — round-trip below
        # (test_get_schema_contract_*).
        "get_schema_contract",
        # 027: optional support/developer lookup helpers — round-trips in
        # tests/test_diagnostics.py (test_dashboard_*_round_trip). The
        # strengthened developer_diagnostics keeps the same signature (already
        # asserted above) and is anomaly-covered in tests/test_diagnostics.py.
        "dashboard_file_usage", "dashboard_target_row_trace",
        "dashboard_airflow_lookup",
        # 029: downstream-impact view (every output derived from a raw file via
        # provenance) — round-trip in tests/test_diagnostics.py
        # (test_p2b_dashboard_file_impact_returns_downstream).
        "dashboard_file_impact",
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


def test_start_run_populates_orchestrator_columns(conn):
    """cp.start_run p_orchestrator round-trip (migration 020): the 8 orchestrator
    columns exist on cp.run_log and are populated from the JSONB argument."""
    cols = _columns(conn, "run_log")
    orch_cols = {
        "orchestrator_type", "orchestrator_dag_id", "orchestrator_run_id",
        "orchestrator_task_id", "orchestrator_try_number",
        "orchestrator_map_index", "orchestrator_url", "orchestrator_payload",
    }
    assert orch_cols <= cols, f"run_log missing orchestrator columns: {orch_cols - cols}"
    orch = {"type": "airflow", "dag_id": "d", "run_id": "r", "task_id": "t",
            "try_number": 2, "map_index": -1, "url": "u",
            "payload": {"k": "v"}}
    run_id = conn.execute(
        "SELECT cp.start_run(%s,'ingestion','sales','orders',%s,'airflow',NULL,NULL,%s)",
        (str(uuid4()), BD, json.dumps(orch)),
    ).fetchone()[0]
    row = conn.execute(
        "SELECT orchestrator_type, orchestrator_dag_id, orchestrator_run_id, "
        "orchestrator_task_id, orchestrator_try_number, orchestrator_map_index, "
        "orchestrator_url, orchestrator_payload FROM cp.run_log WHERE run_id=%s",
        (run_id,)).fetchone()
    assert row[:7] == ("airflow", "d", "r", "t", 2, -1, "u")
    assert row[7] == orch  # whole object preserved


def test_detail_to_aggregate_edge_type_registered(conn):
    """Migration 021: detail_to_aggregate is a registered is_provenance edge_type."""
    row = conn.execute(
        "SELECT is_provenance FROM cp.edge_type WHERE edge_type='detail_to_aggregate'"
    ).fetchone()
    assert row is not None and row[0] is True


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
