"""P4 item 6 — concurrency: two writers under ONE workflow_run_id.

Two SEPARATE connections (real concurrent transactions), each writing a
lineage_link with a DIFFERENT content_hash for runs sharing one
workflow_run_id, must BOTH succeed: no lost write, no duplicate, no deadlock.
The links are independent rows (different consumer_run_id + content_hash) so
they touch disjoint key space — the idempotency unique index does not collide.

We open both transactions, interleave the two writes BEFORE either commits
(so the transactions genuinely overlap), then commit both. Exactly 2 links must
persist for the workflow. Everything is cleaned up in `finally`.
"""
import datetime
import uuid

import pytest

from control import lineage, runs
from control.db import connect

BD = datetime.date(2026, 5, 5)


def _start(conn, wfid):
    return runs.start(
        conn, workflow_run_id=wfid, pipeline_type="ingestion",
        domain="sales", dataset="orders", business_date=BD,
        trigger_type="manual", commit=True)


def test_two_concurrent_writers_one_workflow():
    wfid = str(uuid.uuid4())
    c_setup = connect()
    c_setup.autocommit = True
    c1 = connect()
    c2 = connect()
    c1.autocommit = False
    c2.autocommit = False

    def _cleanup():
        c_setup.execute(
            "DELETE FROM cp.lineage_edge WHERE lineage_link_id IN ("
            "  SELECT lineage_link_id FROM cp.lineage_link l "
            "  JOIN cp.run_log r ON r.run_id=l.consumer_run_id "
            "  WHERE r.workflow_run_id=%s)", (wfid,))
        c_setup.execute(
            "DELETE FROM cp.lineage_link WHERE consumer_run_id IN ("
            "  SELECT run_id FROM cp.run_log WHERE workflow_run_id=%s)", (wfid,))
        c_setup.execute(
            "DELETE FROM cp.run_stage_log WHERE run_id IN ("
            "  SELECT run_id FROM cp.run_log WHERE workflow_run_id=%s)", (wfid,))
        c_setup.execute(
            "DELETE FROM cp.run_log WHERE workflow_run_id=%s", (wfid,))

    try:
        # Two runs that share ONE workflow_run_id (committed so both conns see them).
        run1 = _start(c_setup, wfid)
        run2 = _start(c_setup, wfid)

        # Interleave: both transactions open + write before either commits.
        link1 = lineage.write_link(
            c1, consumer_run_id=run1, edge_type="raw_to_curated",
            target_ref={"path": "s3://c/1", "content_hash": "concurrent-1"},
            record_count=1,
            edges=[{"edge_type": "raw_to_curated", "source_ref": {"w": 1},
                    "record_count": 1}],
            commit=False)
        link2 = lineage.write_link(
            c2, consumer_run_id=run2, edge_type="raw_to_curated",
            target_ref={"path": "s3://c/2", "content_hash": "concurrent-2"},
            record_count=1,
            edges=[{"edge_type": "raw_to_curated", "source_ref": {"w": 2},
                    "record_count": 1}],
            commit=False)

        # Commit both (no deadlock — disjoint rows).
        c1.commit()
        c2.commit()

        assert link1 != link2

        # Exactly 2 links exist for this workflow_run_id — no lost/dup writes.
        n = c_setup.execute(
            "SELECT count(*) FROM cp.lineage_link l "
            "JOIN cp.run_log r ON r.run_id=l.consumer_run_id "
            "WHERE r.workflow_run_id=%s", (wfid,)).fetchone()[0]
        assert n == 2, f"expected exactly 2 concurrent links, got {n}"

        hashes = {row[0] for row in c_setup.execute(
            "SELECT l.target_ref->>'content_hash' FROM cp.lineage_link l "
            "JOIN cp.run_log r ON r.run_id=l.consumer_run_id "
            "WHERE r.workflow_run_id=%s", (wfid,)).fetchall()}
        assert hashes == {"concurrent-1", "concurrent-2"}

        print("\n[CONCURRENCY] 2 writers, 1 workflow_run_id -> links:", n,
              "hashes:", sorted(hashes), "(both persisted, no deadlock)")
    finally:
        try:
            c1.rollback()
        except Exception:
            pass
        try:
            c2.rollback()
        except Exception:
            pass
        c1.close()
        c2.close()
        _cleanup()
        c_setup.close()
