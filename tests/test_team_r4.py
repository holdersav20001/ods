"""R4 data-integrity sweep — nonsense values the DB ACCEPTS (lineage-review team).

Mission: the DB is meant to be the AUTHORITY. Anything the schema accepts via a
direct INSERT/UPDATE that would corrupt lineage or reconciliation is a finding,
because the sanctioned cp.* functions are not the only writers we must trust.

Each probe originally ASSERTED the CURRENT behaviour (ACCEPT = the gap). As of
P10-B (migration 014_integrity.sql) the constraints are LANDED, so each probe is
FLIPPED to assert the nonsense value is now REJECTED. Gaps are marked
GAP_* CLOSED in comments; the closing DB constraint is noted inline next to each.

Probes use the rolled-back `conn` fixture and per-probe SAVEPOINTs so one test can
fire several inserts. The schema is never mutated by the tests themselves.

As of schema 001-014 (2026-05-30) the cp.* integrity constraints added in 014 are:
  * non-negative counts on lineage_link, lineage_edge, dlq, run_log,
    run_stage_log, reconciliation_log (D1);
  * lineage_link.target_ref_contract requiring NON-EMPTY version (D2);
  * status_enum on run_log / run_stage_log / reconciliation_log (D3);
  * run_log.trigger_type_enum (D4);
  * run_stage_log.attempt_positive, lineage_edge.input_slot_non_negative (D5);
  * reconciliation_log.recon_internally_consistent (D6 — recon cannot lie);
  * a BEFORE INSERT trigger edge_type_matches_link on lineage_edge (Theme C).
The cross-row SUM(edges)==link.record_count invariant (GAP 6) is DEFERRED to
P10-C; its probe below still asserts the gap.
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
    assert not _accepts(conn, sql, (run, _good_target("neg-link"))), \
        "FIXED (014): record_count >= 0 must REJECT -1"
    # GAP_1a CLOSED: lineage_link.record_count CHECK (record_count >= 0).


def test_r4_negative_record_count_lineage_edge(conn):
    run = _start_run(conn)
    fil = _register_file(conn)
    link = _make_link(conn, run, "edge-neg")
    sql = ("INSERT INTO cp.lineage_edge "
           "(lineage_link_id, source_file_id, edge_type, record_count) "
           "VALUES (%s,%s,'raw_to_curated',-1)")
    assert not _accepts(conn, sql, (link, fil)), \
        "FIXED (014): lineage_edge.record_count >= 0 must REJECT -1"
    # GAP_1b CLOSED: lineage_edge.record_count CHECK (record_count >= 0).


def test_r4_negative_counts_run_log(conn):
    run = _start_run(conn)
    assert not _accepts(conn, "UPDATE cp.run_log SET record_count_in=-5 WHERE run_id=%s", (run,))
    assert not _accepts(conn, "UPDATE cp.run_log SET record_count_out=-5 WHERE run_id=%s", (run,))
    # GAP_1c/1d CLOSED: run_log.record_count_in/_out CHECK (NULL OR >= 0) REJECT -5.


def test_r4_negative_counts_run_stage_log(conn):
    run = _start_run(conn)
    sid = conn.execute(
        "INSERT INTO cp.run_stage_log (run_id, stage, status) "
        "VALUES (%s,'curate','running') RETURNING stage_log_id", (run,)
    ).fetchone()[0]
    assert not _accepts(conn, "UPDATE cp.run_stage_log SET record_count_in=-5 WHERE stage_log_id=%s", (sid,))
    assert not _accepts(conn, "UPDATE cp.run_stage_log SET record_count_out=-5 WHERE stage_log_id=%s", (sid,))
    # GAP_1e/1f CLOSED: run_stage_log.record_count_in/_out CHECK (NULL OR >= 0) REJECT -5.


def test_r4_negative_count_dlq(conn):
    run = _start_run(conn)
    sql = ("INSERT INTO cp.dlq (run_id, stage, reason, record_count) "
           "VALUES (%s,'curate','bad',-1)")
    assert not _accepts(conn, sql, (run,))
    # GAP_1g CLOSED: dlq.record_count CHECK (record_count >= 0) REJECTS -1.


def test_r4_negative_counts_reconciliation_log(conn):
    run = _start_run(conn)
    base = ("INSERT INTO cp.reconciliation_log "
            "(run_id, check_type, source_count, accounted_count, discrepancy, status) ")
    # rows are otherwise D6-consistent (discrepancy/status match) so the ONLY
    # reason for rejection is the negative count column under test.
    #   row1: source=-1,accounted=0 -> disc=-1 -> status 'double_count' (D6 ok),
    #         rejected by source_count >= 0.
    #   row2: source=0,accounted=-1 -> disc=1  -> status 'breach' (D6 ok),
    #         rejected by accounted_count >= 0.
    assert not _accepts(conn, base + "VALUES (%s,'arith',-1,0,-1,'double_count')", (run,))
    assert not _accepts(conn, base + "VALUES (%s,'arith',0,-1,1,'breach')", (run,))
    # GAP_1h/1i CLOSED: reconciliation_log source_count/accounted_count CHECK (>= 0).
    # NOTE: discrepancy MAY legitimately be negative (over-accounting / double_count),
    #       so no blanket discrepancy >= 0; D6 governs its internal consistency.


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
    assert not _accepts(conn, ins, (run, vnull)), \
        "FIXED (014): version=null must be REJECTED (non-empty version required)"
    assert not _accepts(conn, ins, (run, vempty)), \
        "FIXED (014): version='' must be REJECTED (non-empty version required)"
    # GAP_2 CLOSED: target_ref_contract now requires coalesce(version,'') <> ''.
    # control: a missing version key is also rejected (was already, still is).
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
    assert not _accepts(conn, "UPDATE cp.run_log SET status='banana' WHERE run_id=%s", (run,))
    assert not _accepts(conn, "UPDATE cp.run_stage_log SET status='banana' WHERE stage_log_id=%s", (sid,))
    # recon row is internally consistent (disc=0) so it ONLY fails on status_enum.
    assert not _accepts(conn,
        "INSERT INTO cp.reconciliation_log "
        "(run_id, check_type, source_count, accounted_count, discrepancy, status) "
        "VALUES (%s,'arith',5,5,0,'banana')", (run,))
    # GAP_3a/3b/3c CLOSED: run_log/run_stage_log/reconciliation_log status_enum
    #   CHECK rejects 'banana'.


def test_r4_freetext_trigger_type(conn):
    run = _start_run(conn)
    assert not _accepts(conn, "UPDATE cp.run_log SET trigger_type='banana' WHERE run_id=%s", (run,))
    # GAP_3d CLOSED: run_log.trigger_type_enum CHECK rejects 'banana'
    #   (only airflow|manual|replay|dlq_drain).


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
    assert not _accepts(conn, ins, (run, 0)),  "FIXED (014): attempt=0 must be REJECTED"
    assert not _accepts(conn, ins, (run, -3)), "FIXED (014): attempt=-3 must be REJECTED"
    # GAP_4b CLOSED: run_stage_log.attempt_positive CHECK (attempt >= 1).


def test_r4_input_slot_negative(conn):
    run = _start_run(conn)
    fil = _register_file(conn)
    link = _make_link(conn, run, "slot")
    assert not _accepts(conn,
        "INSERT INTO cp.lineage_edge "
        "(lineage_link_id, source_file_id, input_slot, edge_type, record_count) "
        "VALUES (%s,%s,-1,'raw_to_curated',1)", (link, fil))
    # GAP_4c CLOSED: lineage_edge.input_slot_non_negative CHECK (input_slot >= 0).
    # (edge_type matches the 'raw_to_curated' link, so the C trigger passes; the
    #  only rejection is the input_slot CHECK.)


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
    # status='ok' but a 500-row discrepancy (the headline lie) — now impossible.
    assert not _accepts(conn, base + "VALUES (%s,'arith',100,100,500,'ok')", (run,))
    # GAP_5a CLOSED: status='ok' with discrepancy=500 REJECTED.
    # discrepancy disagrees with source-accounted (10-3=7, stored 0) — now impossible.
    assert not _accepts(conn, base + "VALUES (%s,'arith',10,3,0,'ok')", (run,))
    # GAP_5b CLOSED: discrepancy != source_count - accounted_count REJECTED.
    # control: a fully self-consistent row IS still accepted (the constraint only
    # forbids LIES, not honest recon rows).
    assert _accepts(conn, base + "VALUES (%s,'arith',10,3,7,'breach')", (run,))


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
