"""P7b — graph-derived sink reconciliation (audit F7).

The point of these tests is to prove recon can now genuinely FAIL on REAL row
loss. The old cp.write_reconciliation_check compares two CALLER-supplied numbers
(source_count, accounted_count) — every existing recon test feeds
source == good+dlq by construction, so it is unfalsifiable. cp.reconcile_sink
derives `accounted` from the ACTUAL ods.<dataset> rows committed for the run, so
if rows were lost the breach is real, not arithmetic.

Isolation: the rollback `conn` fixture. Rows written via write_link_then_rows are
visible within the SAME transaction/connection, so reconcile_sink counts them; the
rollback at teardown discards everything. Namespaced graph_recon_*.
"""
import uuid

import pytest

from control import lineage, recon, runs


def _sink_run_with_rows(conn, *, dataset="orders", n, dom="graph_recon_dom",
                        bd="2026-05-30", sink_type="postgres"):
    """Start a sink run and write exactly `n` real ods.<dataset> rows through the
    sanctioned primitive (write_link_then_rows). Returns (run_id, link_id).

    An upstream canonical link is NOT needed: the canonical_to_sink edge names a
    curated/canonical upstream by link, but reconcile_sink only counts the SINK's
    own rows, so we wire a self-contained sink whose edge names a real upstream
    curated link to satisfy the run-edge CHECK."""
    wf = str(uuid.uuid4())
    # An upstream raw_to_curated link (file-anchored, so it needs NO
    # upstream_lineage_link_id) for the canonical_to_sink edge to name — this
    # satisfies the run-edge CHECK on canonical_to_sink.
    up_run = runs.start(conn, workflow_run_id=wf, pipeline_type="ingestion",
                        domain=dom, dataset=dataset, business_date=bd,
                        trigger_type="manual", commit=False)
    up_file = runs.register_file(
        conn, s3_raw_path=f"s3://raw/{uuid.uuid4()}.csv",
        file_md5=uuid.uuid4().hex, business_date=bd,
        domain=dom, dataset=dataset, commit=False)
    up_link = lineage.write_link(
        conn, consumer_run_id=up_run, edge_type="raw_to_curated",
        target_ref={"path": f"s3://cur/{uuid.uuid4().hex}",
                    "content_hash": uuid.uuid4().hex, "version": 1},
        record_count=n,
        edges=[{"edge_type": "raw_to_curated", "source_file_id": str(up_file),
                "source_ref": {}, "record_count": n}], commit=False)
    sink_run = runs.start(conn, workflow_run_id=wf, pipeline_type="sink",
                          domain=dom, dataset=dataset, business_date=bd,
                          trigger_type="manual", commit=False)
    rows = [{"k": i} for i in range(n)]
    link_id = lineage.write_link_then_rows(
        conn, consumer_run_id=sink_run, edge_type="canonical_to_sink",
        target_ref={"path": f"{sink_type}://{uuid.uuid4().hex}",
                    "content_hash": uuid.uuid4().hex, "version": 1},
        record_count=n,
        edges=[{"upstream_run_id": up_run, "upstream_lineage_link_id": up_link,
                "edge_type": "canonical_to_sink", "source_ref": {},
                "record_count": n}],
        rows=rows, sink_type=sink_type, commit=False)
    return sink_run, link_id, dataset


def _recon_row(conn, run_id):
    return conn.execute(
        "SELECT source_count, accounted_count, discrepancy, status, metrics "
        "FROM cp.reconciliation_log WHERE run_id=%s AND check_type='sink_graph'",
        (run_id,)).fetchone()


# =========================================================================== #
# TEST 1 — BALANCED (happy path): derived accounted == source -> 'ok'.
# =========================================================================== #
def test_graph_recon_balanced_ok(conn):
    run_id, _link, _ds = _sink_run_with_rows(conn, n=10)
    recon.reconcile_sink(conn, run_id=run_id, source_count=10, commit=False)
    src, acc, disc, status, metrics = _recon_row(conn, run_id)
    assert (src, acc, disc, status) == (10, 10, 0, "ok")
    assert metrics["graph_derived"] is True
    assert metrics["derived_accounted"] == 10


# =========================================================================== #
# TEST 2 (HEADLINE) — FORCED REAL ROW LOSS -> 'breach'.
#   We write 10 real rows, then DELETE 3 from ods.<dataset> (a genuine loss in
#   the DB), then reconcile against source_count=10. accounted is DERIVED from
#   the 7 surviving rows -> discrepancy 3 -> BREACH. The old arithmetic recon
#   (which trusts a supplied accounted=10) could NEVER catch this; the
#   graph-derived count CAN, because it reads the real DB state.
# =========================================================================== #
def test_graph_recon_forced_real_loss_breaches(conn):
    run_id, link_id, dataset = _sink_run_with_rows(conn, n=10)

    # Precondition: 10 real rows exist for this run's sink link.
    before = conn.execute(
        f"SELECT count(*) FROM ods.{dataset} WHERE _ods_lineage_link_id=%s",
        (link_id,)).fetchone()[0]
    assert before == 10

    # GENUINE LOSS: delete 3 real target rows (simulating a sink that wrote
    # fewer rows than its source claimed / rows lost downstream).
    conn.execute(
        f"DELETE FROM ods.{dataset} WHERE row_id IN "
        f"(SELECT row_id FROM ods.{dataset} WHERE _ods_lineage_link_id=%s "
        f"ORDER BY row_id LIMIT 3)", (link_id,))
    after = conn.execute(
        f"SELECT count(*) FROM ods.{dataset} WHERE _ods_lineage_link_id=%s",
        (link_id,)).fetchone()[0]
    assert after == 7, "precondition: 3 real rows were actually deleted"

    # Reconcile: source still believed 10, but only 7 real rows remain.
    recon.reconcile_sink(conn, run_id=run_id, source_count=10, commit=False)
    src, acc, disc, status, metrics = _recon_row(conn, run_id)

    # THE PROOF: accounted is the DERIVED count (7), not the supplied 10.
    assert src == 10
    assert acc == 7, "accounted must be DERIVED from real rows, not supplied"
    assert metrics["derived_accounted"] == 7
    assert disc == 3
    assert status == "breach", (
        "graph-derived recon FAILED to catch a real 3-row loss — the whole "
        "point of F7 is that it CAN, where arithmetic recon cannot")
    print(f"\n[F7 FORCED-LOSS] source=10 derived_accounted={acc} "
          f"discrepancy={disc} status={status} "
          f"(graph_derived={metrics['graph_derived']}) — REAL loss caught")


# =========================================================================== #
# TEST 3 — DOUBLE COUNT: more real rows than source -> 'double_count'.
# =========================================================================== #
def test_graph_recon_double_count(conn):
    run_id, link_id, dataset = _sink_run_with_rows(conn, n=10)
    # Insert 2 EXTRA real rows under the same sink link (over-write).
    conn.execute(
        f"INSERT INTO ods.{dataset} (payload, _ods_workflow_run_id, "
        f"_ods_lineage_link_id) SELECT '{{}}'::jsonb, NULL, %s "
        f"FROM generate_series(1,2)", (link_id,))
    total = conn.execute(
        f"SELECT count(*) FROM ods.{dataset} WHERE _ods_lineage_link_id=%s",
        (link_id,)).fetchone()[0]
    assert total == 12
    recon.reconcile_sink(conn, run_id=run_id, source_count=10, commit=False)
    src, acc, disc, status, metrics = _recon_row(conn, run_id)
    assert (src, acc, disc, status) == (10, 12, -2, "double_count")
    assert metrics["derived_accounted"] == 12


# =========================================================================== #
# TEST 4 — accounted is INDEPENDENT of any caller-supplied accounted number.
#   reconcile_sink's signature has NO accounted param; the only number a caller
#   passes is source_count. Two reconciles of the SAME run with WILDLY different
#   source_counts both derive the SAME accounted from the DB.
# =========================================================================== #
def test_graph_recon_accounted_independent_of_caller(conn):
    run_id, _link, _ds = _sink_run_with_rows(conn, n=8)
    # Reconcile with an absurd source_count: accounted is still DERIVED (8).
    recon.reconcile_sink(conn, run_id=run_id, source_count=999, commit=False)
    rows = conn.execute(
        "SELECT source_count, accounted_count, status FROM cp.reconciliation_log "
        "WHERE run_id=%s AND check_type='sink_graph'", (run_id,)).fetchall()
    # The most recent (and only-by-source=999) row: accounted derived == 8.
    src999 = [r for r in rows if r[0] == 999]
    assert src999, "expected a recon row for source_count=999"
    assert src999[0][1] == 8, ("accounted must be DB-derived (8) regardless of "
                               "the caller's source_count")
    assert src999[0][2] == "breach"  # 999 - 8 > 0


# =========================================================================== #
# TEST 5 — fan-out: a run with TWO canonical_to_sink links counts BOTH links'
#   rows under that run (scope is consumer_run_id + edge_type). Here we force
#   one run to own two sink links to prove the scope sums correctly.
# =========================================================================== #
def test_graph_recon_fanout_counts_all_run_sink_rows(conn):
    dom, ds, bd = "graph_recon_fan", "orders", "2026-05-30"
    wf = str(uuid.uuid4())
    up_run = runs.start(conn, workflow_run_id=wf, pipeline_type="ingestion",
                        domain=dom, dataset=ds, business_date=bd,
                        trigger_type="manual", commit=False)
    fan_file = runs.register_file(
        conn, s3_raw_path=f"s3://raw/{uuid.uuid4()}.csv",
        file_md5=uuid.uuid4().hex, business_date=bd,
        domain=dom, dataset=ds, commit=False)
    cur = lineage.write_link(
        conn, consumer_run_id=up_run, edge_type="raw_to_curated",
        target_ref={"path": "s3://cur/fan", "content_hash": "fan",
                    "version": 1},
        record_count=10,
        edges=[{"edge_type": "raw_to_curated", "source_file_id": str(fan_file),
                "source_ref": {}, "record_count": 10}], commit=False)
    sink_run = runs.start(conn, workflow_run_id=wf, pipeline_type="sink",
                          domain=dom, dataset=ds, business_date=bd,
                          trigger_type="manual", commit=False)
    # TWO sink links of ONE run (same canonical bytes, two sink_type/path).
    for st in ("postgres", "kafka"):
        lineage.write_link_then_rows(
            conn, consumer_run_id=sink_run, edge_type="canonical_to_sink",
            target_ref={"path": f"{st}://fan", "content_hash": "fanhash",
                        "version": 1},
            record_count=5,
            edges=[{"upstream_run_id": up_run, "upstream_lineage_link_id": cur,
                    "edge_type": "canonical_to_sink", "source_ref": {},
                    "record_count": 5}],
            rows=[{"k": i} for i in range(5)], sink_type=st, commit=False)
    # Both links' 5+5 rows belong to this run -> derived accounted == 10.
    recon.reconcile_sink(conn, run_id=sink_run, source_count=10, commit=False)
    src, acc, disc, status, _ = _recon_row(conn, sink_run)
    assert (src, acc, disc, status) == (10, 10, 0, "ok"), (
        "fan-out: both sink links' rows must be counted under the run")
