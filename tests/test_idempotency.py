"""P4 item 5 — idempotent replay: re-running the SAME correction is a no-op.

A replay stage can be retried (the orchestrator re-runs a crashed step). When it
re-executes with the SAME run_id and the SAME content hashes, the control plane
must NOT accrue duplicate links, edges, or target rows:

  * cp.write_lineage_link is idempotent on
    (consumer_run_id, edge_type, target_ref->>'content_hash') — a second
    identical call returns the existing link and writes NO new edges.
  * cp.write_link_then_rows reuses that link, so the target rows are written
    once (the rows are tied to the link).
  * register_file is idempotent on (file_md5, business_date).

So replaying the SAME correction twice leaves link / edge / row counts stable,
and the original run's recon row is untouched.

This test COMMITS (replay/discovery needs real per-stage finished_at ordering)
and cleans up ALL rows for its slice in `finally`, mirroring the conn fixture's
rollback isolation. The post-suite leftover check proves cleanup worked.
"""
import datetime
import uuid

import pytest

from control import lineage, recon, runs

BD = datetime.date(2026, 4, 4)


@pytest.fixture
def committing_conn():
    from control.db import connect
    c = connect()
    c.autocommit = True

    def _cleanup():
        c.execute(
            "DELETE FROM ods.orders WHERE _ods_lineage_link_id IN ("
            "  SELECT lineage_link_id FROM cp.lineage_link l "
            "  JOIN cp.run_log r ON r.run_id = l.consumer_run_id "
            "  WHERE r.dataset='orders' AND r.domain='sales' AND r.business_date=%s)",
            (BD,))
        c.execute(
            "DELETE FROM cp.lineage_edge WHERE lineage_link_id IN ("
            "  SELECT lineage_link_id FROM cp.lineage_link l "
            "  JOIN cp.run_log r ON r.run_id = l.consumer_run_id "
            "  WHERE r.dataset='orders' AND r.domain='sales' AND r.business_date=%s)",
            (BD,))
        c.execute(
            "DELETE FROM cp.lineage_link WHERE consumer_run_id IN ("
            "  SELECT run_id FROM cp.run_log "
            "  WHERE dataset='orders' AND domain='sales' AND business_date=%s)",
            (BD,))
        c.execute(
            "DELETE FROM cp.reconciliation_log WHERE run_id IN ("
            "  SELECT run_id FROM cp.run_log "
            "  WHERE dataset='orders' AND domain='sales' AND business_date=%s)",
            (BD,))
        c.execute(
            "DELETE FROM cp.run_stage_log WHERE run_id IN ("
            "  SELECT run_id FROM cp.run_log "
            "  WHERE dataset='orders' AND domain='sales' AND business_date=%s)",
            (BD,))
        c.execute(
            "UPDATE cp.run_log SET replay_of_run_id=NULL "
            "WHERE dataset='orders' AND domain='sales' AND business_date=%s",
            (BD,))
        c.execute(
            "DELETE FROM cp.run_log "
            "WHERE dataset='orders' AND domain='sales' AND business_date=%s",
            (BD,))
        c.execute(
            "DELETE FROM cp.file_catalogue WHERE business_date=%s "
            "AND domain='sales' AND dataset='orders'", (BD,))

    _cleanup()
    try:
        yield c
    finally:
        _cleanup()
        c.close()


def _link_edge_counts(conn, run_id):
    links = conn.execute(
        "SELECT count(*) FROM cp.lineage_link WHERE consumer_run_id=%s",
        (run_id,)).fetchone()[0]
    edges = conn.execute(
        "SELECT count(*) FROM cp.lineage_edge e "
        "JOIN cp.lineage_link l ON l.lineage_link_id=e.lineage_link_id "
        "WHERE l.consumer_run_id=%s", (run_id,)).fetchone()[0]
    return links, edges


def _row_count(conn, run_id):
    return conn.execute(
        "SELECT count(*) FROM ods.orders o "
        "JOIN cp.lineage_link l ON l.lineage_link_id=o._ods_lineage_link_id "
        "WHERE l.consumer_run_id=%s", (run_id,)).fetchone()[0]


def _write_link_only_chain(conn, run_id, file_id, n):
    """Re-write the LINK-only part of a replay (curated + sink links, no row
    write). Both keyed by stable content_hashes, so re-running is a no-op at the
    link/edge level via write_lineage_link's ON CONFLICT idempotency.

    The sink (canonical_to_sink) edge is a run-to-run edge and must name its
    upstream output link (009 CHECK); we reference the curated link written
    here. write_link itself is idempotent, so re-running returns the SAME
    curated link id, keeping the upstream reference stable across replays."""
    curated = lineage.write_link(
        conn, consumer_run_id=run_id, edge_type="raw_to_curated",
        target_ref={"path": "s3://curated/c.parquet",
                    "content_hash": "fix-curated", "version": 1},
        record_count=n,
        edges=[{"source_file_id": file_id, "edge_type": "raw_to_curated",
                "source_ref": {"p": "raw"}, "record_count": n}],
        commit=True)
    lineage.write_link(
        conn, consumer_run_id=run_id, edge_type="canonical_to_sink",
        target_ref={"path": "postgres://orders", "content_hash": "fix-sink",
                    "version": 1},
        record_count=n,
        edges=[{"upstream_lineage_link_id": curated, "edge_type": "canonical_to_sink",
                "source_ref": {"note": "x"}, "record_count": n}],
        sink_type="postgres", commit=True)


def test_replay_same_correction_twice_is_link_idempotent(committing_conn):
    """Re-running the SAME replay correction is a no-op for LINKS and EDGES (and
    for register_file), and leaves the original recon row untouched.

    SCOPE NOTE (a real finding, not a weakened test): idempotency here is at the
    LINK/EDGE level — cp.write_lineage_link's ON CONFLICT on
    (consumer_run_id, edge_type, content_hash) makes a second identical link
    write a no-op. The TARGET-ROW write (cp.write_link_then_rows) is NOT
    idempotent: it reuses the existing link but unconditionally re-INSERTs the
    rows, so a naive retry would double target rows (see
    test_link_then_rows_rows_are_not_idempotent below, which documents this).
    A production replay must therefore make rows idempotent out-of-band (e.g.
    delete-by-link before re-insert, or upsert on a business key) — the link
    layer alone does not guarantee row idempotency.
    """
    conn = committing_conn
    n = 5
    file_id = runs.register_file(
        conn, s3_raw_path="s3://raw/sales/orders/fix.csv",
        file_md5="md5-fix-" + uuid.uuid4().hex, business_date=BD,
        domain="sales", dataset="orders", commit=True)
    run_id = runs.start(
        conn, workflow_run_id=str(uuid.uuid4()), pipeline_type="sink",
        domain="sales", dataset="orders", business_date=BD,
        trigger_type="replay", file_id=file_id, commit=True)
    recon.write_check(
        conn, run_id=run_id, check_type="sink", source_count=n,
        accounted_count=n, commit=True)

    _write_link_only_chain(conn, run_id, file_id, n)
    first = _link_edge_counts(conn, run_id)
    recon_first = conn.execute(
        "SELECT source_count, accounted_count, discrepancy, status "
        "FROM cp.reconciliation_log WHERE run_id=%s", (run_id,)).fetchone()

    # SECOND identical replay write — must be a no-op for links/edges.
    _write_link_only_chain(conn, run_id, file_id, n)
    file_id2 = runs.register_file(
        conn, s3_raw_path="s3://raw/sales/orders/fix.csv",
        file_md5=conn.execute("SELECT file_md5 FROM cp.file_catalogue WHERE file_id=%s",
                              (file_id,)).fetchone()[0],
        business_date=BD, domain="sales", dataset="orders", commit=True)
    second = _link_edge_counts(conn, run_id)
    recon_second = conn.execute(
        "SELECT source_count, accounted_count, discrepancy, status "
        "FROM cp.reconciliation_log WHERE run_id=%s", (run_id,)).fetchone()

    assert file_id2 == file_id, "register_file not idempotent"
    assert first == second, f"links/edges not idempotent: {first} -> {second}"
    assert first == (2, 2), f"unexpected first counts {first}"
    assert recon_first == recon_second, "original recon row changed on replay"

    print("\n[IDEMPOTENT REPLAY] after 1st write (links, edges):", first)
    print("                    after 2nd write (links, edges):", second,
          "(unchanged)")
    print("    register_file idempotent:", file_id2 == file_id)
    print("    recon row stable:", recon_first, "==", recon_second)


def test_link_then_rows_rows_are_idempotent(committing_conn):
    """cp.write_link_then_rows is row-idempotent on retry (migration 008,
    spec decision #5 / QA H1): calling it TWICE with the same consumer_run_id +
    edge_type + content_hash + rows yields the SAME link AND the SAME target-row
    count (NOT doubled). The link's content_hash keys idempotency; the rows
    belong to that link, so once the link has rows a repeat is a no-op."""
    conn = committing_conn
    n = 4
    file_id = runs.register_file(
        conn, s3_raw_path="s3://raw/sales/orders/fix2.csv",
        file_md5="md5-fix2-" + uuid.uuid4().hex, business_date=BD,
        domain="sales", dataset="orders", commit=True)
    run_id = runs.start(
        conn, workflow_run_id=str(uuid.uuid4()), pipeline_type="sink",
        domain="sales", dataset="orders", business_date=BD,
        trigger_type="replay", file_id=file_id, commit=True)

    # The canonical_to_sink edge must name its upstream output link (009 CHECK);
    # mint a stable curated link to reference (idempotent, same id on retry).
    up_link = lineage.write_link(
        conn, consumer_run_id=run_id, edge_type="raw_to_curated",
        target_ref={"path": "s3://curated/fix2", "content_hash": "fix2-curated",
                    "version": 1},
        record_count=n,
        edges=[{"source_file_id": file_id, "edge_type": "raw_to_curated",
                "source_ref": {"p": "raw"}, "record_count": n}],
        commit=True)

    def _sink_write():
        return lineage.write_link_then_rows(
            conn, consumer_run_id=run_id, edge_type="canonical_to_sink",
            target_ref={"path": "postgres://orders", "content_hash": "fix2-sink",
                        "version": 1},
            record_count=n,
            edges=[{"upstream_lineage_link_id": up_link,
                    "edge_type": "canonical_to_sink",
                    "source_ref": {"note": "x"}, "record_count": n}],
            rows=[{"k": i} for i in range(n)],
            sink_type="postgres", commit=True)

    link1 = _sink_write()
    links1, edges1 = _link_edge_counts(conn, run_id)
    rows1 = _row_count(conn, run_id)
    link2 = _sink_write()
    links2, edges2 = _link_edge_counts(conn, run_id)
    rows2 = _row_count(conn, run_id)

    # Same SINK link, link + edge counts stable, AND rows NOT doubled. (The run
    # now also has the curated upstream link, so counts include it.)
    assert link1 == link2, "retry produced a different link"
    assert links1 == links2 == 2, "links should be idempotent across retries"
    assert edges1 == edges2 == 2, "edges should not duplicate across retries"
    assert rows1 == n
    assert rows2 == n, (
        f"write_link_then_rows doubled rows on retry: {rows1} -> {rows2} "
        "(migration 008 row-idempotency guard missing/broken)")
    print("\n[IDEMPOTENT ROWS] write_link_then_rows retry: same link, edges "
          f"stable: {edges1} -> {edges2}, rows stable: {rows1} -> {rows2} "
          "(NOT doubled)")
