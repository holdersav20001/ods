"""Concurrency contract for ``ods_pipeline.stages.finish``.

Reproduces the lost-update / duplicate-insert race documented in B3:

* one ``stages.start`` row is opened for ``(run_id, stage, attempt)``;
* two threads, on two distinct ``psycopg2`` connections, then race to call
  ``stages.finish`` for the same key.

Correct behaviour after the fix:

* Exactly **one** thread atomically locks the open row and UPDATEs it terminal
  (via ``SELECT ... FOR UPDATE SKIP LOCKED``).
* The other thread sees no claimable open row and INSERTs a fresh terminal row.
* Total row count for ``(run_id, stage, attempt)`` = **exactly 2**, both
  terminal, none left with ``ended_at IS NULL``.

Without the fix, both threads can race the unlocked subquery, both UPDATE the
same row (one wins, one no-ops with rowcount=0), and the no-op then ALSO
INSERTs — producing 3 rows instead of 2 — OR one of them double-updates and
no INSERT happens — producing 1 row.  The test detects either drift.
"""
from __future__ import annotations

import os
import sys
import threading
import uuid

import psycopg2
import pytest

HERE = os.path.dirname(__file__)
sys.path.insert(0, os.path.abspath(os.path.join(HERE, "..", "..")))

from ods_pipeline import runs, stages  # noqa: E402


def _connect():
    return psycopg2.connect(
        host=os.environ.get("TEST_PG_HOST", "127.0.0.1"),
        port=int(os.environ.get("TEST_PG_PORT", "5440")),
        dbname=os.environ.get("TEST_PG_DB", "ods_dev"),
        user=os.environ.get("TEST_PG_USER", "ods"),
        password=os.environ.get("TEST_PG_PASSWORD", "ods"),
    )


@pytest.fixture
def race_run(pg_conn):
    """Create a run + an open stage row, yield (run_id, stage, attempt)."""
    rid = str(uuid.uuid4())
    stage = "raw_read"
    attempt = 1
    runs.start(
        pg_conn, run_id=rid, pipeline_type="s3_batch",
        domain="insurance", dataset="policies",
        business_date="2026-04-28", file_id=None, config_version_id=1,
    )
    stages.start(pg_conn, run_id=rid, stage=stage, attempt_number=attempt,
                 input_ref="s3://raw/x.csv")
    yield rid, stage, attempt
    try:
        pg_conn.rollback()
    except Exception:
        pass
    with pg_conn.cursor() as cur:
        cur.execute("DELETE FROM pipeline.run_stage_log WHERE run_id=%s", (rid,))
        cur.execute("DELETE FROM pipeline.run_log WHERE run_id=%s", (rid,))
    pg_conn.commit()


def test_concurrent_finish_produces_exactly_one_update_and_one_append(race_run):
    rid, stage, attempt = race_run

    barrier = threading.Barrier(2)
    errors: list[BaseException] = []

    def worker(label: str):
        try:
            conn = _connect()
            try:
                # Synchronise the two threads as close to FOR UPDATE as possible.
                barrier.wait(timeout=5)
                stages.finish(
                    conn,
                    run_id=rid,
                    stage=stage,
                    attempt_number=attempt,
                    status="succeeded",
                    record_count_out=10,
                    metrics={"worker": label},
                )
            finally:
                conn.close()
        except BaseException as exc:  # noqa: BLE001 — propagate via list
            errors.append(exc)

    t1 = threading.Thread(target=worker, args=("a",), daemon=True)
    t2 = threading.Thread(target=worker, args=("b",), daemon=True)
    t1.start()
    t2.start()
    t1.join(timeout=10)
    t2.join(timeout=10)

    assert not errors, f"workers raised: {errors}"
    assert not t1.is_alive() and not t2.is_alive(), "worker thread hung"

    with _connect() as audit:
        with audit.cursor() as cur:
            cur.execute(
                """
                SELECT count(*),
                       count(*) FILTER (WHERE ended_at IS NULL),
                       count(*) FILTER (WHERE status='succeeded'),
                       count(DISTINCT id)
                  FROM pipeline.run_stage_log
                 WHERE run_id=%s AND stage=%s AND attempt_number=%s
                """,
                (rid, stage, attempt),
            )
            total, open_count, terminal_count, distinct_ids = cur.fetchone()

    assert total == 2, (
        f"expected exactly 2 rows for (run, stage, attempt) — got {total}. "
        "Either the FOR UPDATE SKIP LOCKED claim leaked (>2) or one writer "
        "silently no-op'd (1)."
    )
    assert open_count == 0, (
        f"expected zero open rows after both finish() — got {open_count}"
    )
    assert terminal_count == 2, (
        f"expected both rows terminal — got {terminal_count} succeeded"
    )
    assert distinct_ids == 2
