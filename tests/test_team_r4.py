"""R4 data-integrity sweep — nonsense values the DB ACCEPTS (lineage-review team).

Mission: the DB is meant to be the AUTHORITY. Anything the schema accepts via a
direct INSERT/UPDATE that would corrupt lineage or reconciliation is a finding,
because the sanctioned cp.* functions are not the only writers we must trust.

Each probe ASSERTS the CURRENT behaviour (almost always ACCEPT = the gap) so the
suite stays green today and FLIPS RED the moment a CHECK/trigger closes the gap.
Gaps are marked CONFIRMED_GAP_* in comments; the proposed DB constraint is noted
inline next to each.

Audit only — no schema is dropped or altered here. Probes use the rolled-back
`conn` fixture and per-probe SAVEPOINTs so one test can fire several inserts.

Empirically verified against ods_cp at schema 001-012 (2026-05-30):
the ONLY cp.* CHECK constraints are
  lineage_link.sink_type_iff_sink, lineage_link.target_ref_contract,
  lineage_edge.{raw_edge_requires_source_file, edge_must_anchor,
               upstream_link_required_for_run_edges}.
There is NO CHECK on any count column, status, trigger_type, business_date,
attempt, input_slot, or any recon discrepancy/status consistency.
"""
import json
import uuid

import psycopg
import pytest

BD = "2026-05-30"


# --------------------------------------------------------------------------- #
# helpers — minimal real rows to hang corruption off of
# --------------------------------------------------------------------------- #
def _accepts(conn, sql, params=()):
    """Return True iff the statement is ACCEPTED by the DB (savepoint-isolated)."""
    conn.execute("SAVEPOINT p9r4")
    try:
        conn.execute(sql, params)
        conn.execute("RELEASE SAVEPOINT p9r4")
        return True
    except psycopg.Error:
        conn.execute("ROLLBACK TO SAVEPOINT p9r4")
        return False


def _start_run(conn, *, status="running", trigger_type="airflow",
               business_date=BD):
    return conn.execute(
        "INSERT INTO cp.run_log "
        "(workflow_run_id, pipeline_type, domain, dataset, business_date, "
        " trigger_type, status) "
        "VALUES (%s,'ingestion','sales','orders',%s,%s,%s) RETURNING run_id",
        (str(uuid.uuid4()), business_date, trigger_type, status),
    ).fetchone()[0]


def _register_file(conn):
    return conn.execute(
        "INSERT INTO cp.file_catalogue "
        "(s3_raw_path, file_md5, business_date, domain, dataset) "
        "VALUES (%s,%s,%s,'sales','orders') RETURNING file_id",
        (f"s3://raw/{uuid.uuid4()}.csv", uuid.uuid4().hex, BD),
    ).fetchone()[0]


def _good_target(tag):
    # passes target_ref_contract: non-empty path + content_hash + version key
    return json.dumps({"path": f"s3://c/{tag}.parquet",
                       "content_hash": f"hash-{tag}", "version": "1"})


def _make_link(conn, run_id, tag, *, record_count=10):
    return conn.execute(
        "INSERT INTO cp.lineage_link "
        "(consumer_run_id, edge_type, target_ref, record_count) "
        "VALUES (%s,'raw_to_curated',%s::jsonb,%s) RETURNING lineage_link_id",
        (run_id, _good_target(tag), record_count),
    ).fetchone()[0]


# =========================================================================== #
# ATTACK 1 — NEGATIVE record_count on every count column.
#   A negative count silently corrupts SUM-based recon and merge totals: a
#   single -1 row can mask N missing rows in a SUM, turning a real breach into
#   a false 'ok'. None of these columns has a CHECK (>= 0).
#   PROPOSED: ADD CONSTRAINT ... CHECK (<col> >= 0) on each (NULL still allowed
#   where the column is nullable, via `<col> IS NULL OR <col> >= 0`).
# =========================================================================== #
def test_r4_negative_record_count_lineage_link(conn):
    run = _start_run(conn)
    # distinct content_hash so the unique target index is not what rejects us
    sql = ("INSERT INTO cp.lineage_link "
           "(consumer_run_id, edge_type, target_ref, record_count) "
           "VALUES (%s,'raw_to_curated',%s::jsonb,-1)")
    assert _accepts(conn, sql, (run, _good_target("neg-link"))), \
        "expected -1 to be accepted (the gap)"
    # CONFIRMED_GAP_1a: lineage_link.record_count accepts -1.
    # PROPOSED: CHECK (record_count >= 0)


def test_r4_negative_record_count_lineage_edge(conn):
    run = _start_run(conn)
    fil = _register_file(conn)
    link = _make_link(conn, run, "edge-neg")
    sql = ("INSERT INTO cp.lineage_edge "
           "(lineage_link_id, source_file_id, edge_type, record_count) "
           "VALUES (%s,%s,'raw_to_curated',-1)")
    assert _accepts(conn, sql, (link, fil)), "expected -1 accepted (the gap)"
    # CONFIRMED_GAP_1b: lineage_edge.record_count accepts -1.
    # PROPOSED: CHECK (record_count >= 0)


def test_r4_negative_counts_run_log(conn):
    run = _start_run(conn)
    assert _accepts(conn, "UPDATE cp.run_log SET record_count_in=-5 WHERE run_id=%s", (run,))
    assert _accepts(conn, "UPDATE cp.run_log SET record_count_out=-5 WHERE run_id=%s", (run,))
    # CONFIRMED_GAP_1c/1d: run_log.record_count_in / record_count_out accept -5.
    # PROPOSED: CHECK (record_count_in IS NULL OR record_count_in >= 0) and same for _out


def test_r4_negative_counts_run_stage_log(conn):
    run = _start_run(conn)
    sid = conn.execute(
        "INSERT INTO cp.run_stage_log (run_id, stage, status) "
        "VALUES (%s,'curate','running') RETURNING stage_log_id", (run,)
    ).fetchone()[0]
    assert _accepts(conn, "UPDATE cp.run_stage_log SET record_count_in=-5 WHERE stage_log_id=%s", (sid,))
    assert _accepts(conn, "UPDATE cp.run_stage_log SET record_count_out=-5 WHERE stage_log_id=%s", (sid,))
    # CONFIRMED_GAP_1e/1f: run_stage_log.record_count_in / _out accept -5.
    # PROPOSED: CHECK (record_count_in IS NULL OR record_count_in >= 0) and same for _out


def test_r4_negative_count_dlq(conn):
    run = _start_run(conn)
    sql = ("INSERT INTO cp.dlq (run_id, stage, reason, record_count) "
           "VALUES (%s,'curate','bad',-1)")
    assert _accepts(conn, sql, (run,))
    # CONFIRMED_GAP_1g: dlq.record_count accepts -1.
    # PROPOSED: CHECK (record_count >= 0)


def test_r4_negative_counts_reconciliation_log(conn):
    run = _start_run(conn)
    base = ("INSERT INTO cp.reconciliation_log "
            "(run_id, check_type, source_count, accounted_count, discrepancy, status) ")
    assert _accepts(conn, base + "VALUES (%s,'arith',-1,0,-1,'breach')", (run,))
    assert _accepts(conn, base + "VALUES (%s,'arith',0,-1,1,'breach')", (run,))
    # CONFIRMED_GAP_1h/1i: reconciliation_log.source_count / accounted_count accept -1.
    # PROPOSED: CHECK (source_count >= 0), CHECK (accounted_count >= 0)
    # NOTE: discrepancy MAY legitimately be negative (over-accounting), so a
    #       blanket discrepancy >= 0 is NOT proposed; see GAP 5 for its real bug.


# =========================================================================== #
# ATTACK 2 — target_ref version null / empty.
#   target_ref_contract only does `target_ref ? 'version'` (key EXISTS). The
#   version is meant to defeat overwrite-mutability (decision #4); a null or
#   empty version is as useless as no version — two different outputs can both
#   carry version "" / null and be treated as the same identity.
#   CONTROL: a MISSING version key is correctly REJECTED (constraint works).
#   PROPOSED: tighten to coalesce(target_ref->>'version','') <> ''
# =========================================================================== #
def test_r4_target_ref_version_null_empty(conn):
    run = _start_run(conn)
    ins = ("INSERT INTO cp.lineage_link "
           "(consumer_run_id, edge_type, target_ref, record_count) "
           "VALUES (%s,'raw_to_curated',%s::jsonb,1)")
    vnull = json.dumps({"path": "p", "content_hash": "vnull", "version": None})
    vempty = json.dumps({"path": "p", "content_hash": "vempty", "version": ""})
    vmiss = json.dumps({"path": "p", "content_hash": "vmiss"})
    assert _accepts(conn, ins, (run, vnull)), "version=null should currently pass (gap)"
    assert _accepts(conn, ins, (run, vempty)), "version='' should currently pass (gap)"
    # CONFIRMED_GAP_2: {"version": null} and {"version": ""} pass target_ref_contract.
    # control: missing key is rejected -> proves the constraint is only `? 'version'`.
    assert not _accepts(conn, ins, (run, vmiss)), "missing version key must be rejected"


# =========================================================================== #
# ATTACK 3 — free-TEXT status / trigger_type (no enum / CHECK / lookup FK).
#   Recon and orchestration branch on these strings; a typo'd or garbage status
#   is silently stored and can defeat status-based filters (e.g. a row that is
#   neither 'ok' nor 'breach' is invisible to both halves of a recon report).
#   PROPOSED: CHECK status IN (...) per table, or an FK to a status lookup;
#             CHECK trigger_type IN ('airflow','manual','replay','dlq_drain').
# =========================================================================== #
def test_r4_freetext_status_columns(conn):
    run = _start_run(conn)
    sid = conn.execute(
        "INSERT INTO cp.run_stage_log (run_id, stage, status) "
        "VALUES (%s,'curate','running') RETURNING stage_log_id", (run,)
    ).fetchone()[0]
    assert _accepts(conn, "UPDATE cp.run_log SET status='banana' WHERE run_id=%s", (run,))
    assert _accepts(conn, "UPDATE cp.run_stage_log SET status='banana' WHERE stage_log_id=%s", (sid,))
    assert _accepts(conn,
        "INSERT INTO cp.reconciliation_log "
        "(run_id, check_type, source_count, accounted_count, discrepancy, status) "
        "VALUES (%s,'arith',5,5,0,'banana')", (run,))
    # CONFIRMED_GAP_3a/3b/3c: run_log.status, run_stage_log.status,
    #   reconciliation_log.status accept 'banana'. PROPOSED: CHECK status IN (...)


def test_r4_freetext_trigger_type(conn):
    run = _start_run(conn)
    assert _accepts(conn, "UPDATE cp.run_log SET trigger_type='banana' WHERE run_id=%s", (run,))
    # CONFIRMED_GAP_3d: run_log.trigger_type accepts 'banana' (spec enumerates
    #   airflow|manual|replay|dlq_drain).
    # PROPOSED: CHECK (trigger_type IN ('airflow','manual','replay','dlq_drain'))


# =========================================================================== #
# ATTACK 4 — business_date sanity, attempt, input_slot.
# =========================================================================== #
def test_r4_business_date_far_future(conn):
    # SUSPECTED (severity LOW): a far-future business_date is accepted. Whether
    # this is a bug depends on whether future-dated runs are ever legitimate
    # (back/forward-fill). Flagged, not asserted as a hard corruption.
    assert _accepts(conn,
        "INSERT INTO cp.run_log "
        "(workflow_run_id, pipeline_type, domain, dataset, business_date, "
        " trigger_type, status) "
        "VALUES (%s,'ingestion','sales','orders','2999-12-31','airflow','running')",
        (str(uuid.uuid4()),))
    # SUSPECTED_GAP_4a: business_date='2999-12-31' accepted. PROPOSED (optional):
    #   CHECK (business_date <= current_date + interval '1 day') — only if forward
    #   fill is disallowed; likely function-only, not a hard constraint.


def test_r4_attempt_non_positive(conn):
    run = _start_run(conn)
    ins = ("INSERT INTO cp.run_stage_log (run_id, stage, attempt, status) "
           "VALUES (%s,'curate',%s,'running')")
    assert _accepts(conn, ins, (run, 0)),  "attempt=0 accepted (gap)"
    assert _accepts(conn, ins, (run, -3)), "attempt=-3 accepted (gap)"
    # CONFIRMED_GAP_4b: run_stage_log.attempt accepts 0 and -3 (spec: 1-based retry).
    # PROPOSED: CHECK (attempt >= 1)


def test_r4_input_slot_negative(conn):
    run = _start_run(conn)
    fil = _register_file(conn)
    link = _make_link(conn, run, "slot")
    assert _accepts(conn,
        "INSERT INTO cp.lineage_edge "
        "(lineage_link_id, source_file_id, input_slot, edge_type, record_count) "
        "VALUES (%s,%s,-1,'raw_to_curated',1)", (link, fil))
    # CONFIRMED_GAP_4c: lineage_edge.input_slot accepts -1 (merge slot binding is
    #   0-based; a negative slot can never bind to a declared input).
    # PROPOSED: CHECK (input_slot >= 0)


# =========================================================================== #
# ATTACK 5 — reconciliation_log status / discrepancy INCONSISTENCY.
#   Nothing ties status to discrepancy, nor discrepancy to source-accounted.
#   A row can claim status='ok' while discrepancy=500 (a real breach reported as
#   clean), or carry a discrepancy that does not equal source_count-accounted_count
#   (the arithmetic the recon layer is supposed to certify). Either makes the
#   recon table lie. This is the highest-severity authority gap: the recon table
#   is the system's own truth check, and it can self-contradict.
# =========================================================================== #
def test_r4_recon_status_discrepancy_inconsistent(conn):
    run = _start_run(conn)
    base = ("INSERT INTO cp.reconciliation_log "
            "(run_id, check_type, source_count, accounted_count, discrepancy, status) ")
    # status='ok' but a 500-row discrepancy
    assert _accepts(conn, base + "VALUES (%s,'arith',100,100,500,'ok')", (run,))
    # CONFIRMED_GAP_5a: status='ok' with discrepancy=500 accepted.
    # discrepancy disagrees with source-accounted (10-3=7, stored 0)
    assert _accepts(conn, base + "VALUES (%s,'arith',10,3,0,'ok')", (run,))
    # CONFIRMED_GAP_5b: discrepancy != source_count - accounted_count accepted.
    # PROPOSED:
    #   CHECK (discrepancy = source_count - accounted_count)       -- arithmetic truth
    #   CHECK ((status='ok') = (discrepancy = 0))                  -- status derives from disc
    #   (or derive both via a trigger / generated column so callers cannot lie).


# =========================================================================== #
# ATTACK 6 — link.record_count vs SUM(edge.record_count) — the core merge
#   invariant. Nothing enforces that a link's declared total equals the sum of
#   its edges' counts, so a merge link can claim 10 while its edges sum to 999.
#   Lineage SUMs (merge totals, graph recon) then silently disagree with the
#   link header.
#   PROPOSED: a deferred constraint trigger on lineage_edge re-checking
#     SUM(edge.record_count) per link == link.record_count at COMMIT
#     (cannot be a row CHECK — it is cross-row/cross-table). Acceptable as
#     function-only ONLY if every writer goes through cp.write_lineage_link AND
#     that function enforces it; a direct INSERT bypasses it today.
# =========================================================================== #
def test_r4_link_count_vs_edge_sum_unenforced(conn):
    run = _start_run(conn)
    fil = _register_file(conn)
    link = _make_link(conn, run, "summismatch", record_count=10)
    assert _accepts(conn,
        "INSERT INTO cp.lineage_edge "
        "(lineage_link_id, source_file_id, edge_type, record_count) "
        "VALUES (%s,%s,'raw_to_curated',999)", (link, fil))
    declared, edge_sum = conn.execute(
        "SELECT l.record_count, COALESCE(SUM(e.record_count),0) "
        "FROM cp.lineage_link l LEFT JOIN cp.lineage_edge e "
        "  ON e.lineage_link_id = l.lineage_link_id "
        "WHERE l.lineage_link_id=%s GROUP BY l.record_count", (link,)
    ).fetchone()
    # CONFIRMED_GAP_6: link header (10) and edge sum (999) silently disagree.
    assert declared != edge_sum, "link.record_count != SUM(edges) is accepted (gap)"
