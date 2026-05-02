"""Atomicity contract for ``ods_pipeline.messages.record_result`` (B4).

Acceptance per plan §Task 3:

* ``record_result`` accepts an externally-managed connection and **does not
  commit** internally.
* Caller wraps the entire flow in a single transaction and commits once at
  the end.
* On a simulated mid-flow exception (raise after ``stages.write`` but before
  ``reconciliation.write_check``), the database holds **neither** stage rows
  for that run **nor** any reconciliation_log row — i.e. the whole flow
  rolls back as one unit.
* Stage idempotency: re-invoking ``stages.start`` for the same
  ``(run_id, stage, attempt_number)`` is a no-op (constraint from migration
  19, partial unique index on ``event_type='stage_started'``).
"""
from __future__ import annotations

import os
import sys
import uuid

import pytest

HERE = os.path.dirname(__file__)
sys.path.insert(0, os.path.abspath(os.path.join(HERE, "..", "..")))

from ods_pipeline import messages, reconciliation, runs, stages  # noqa: E402


@pytest.fixture
def isolated_run(pg_conn):
    rid = str(uuid.uuid4())
    yield rid
    try:
        pg_conn.rollback()
    except Exception:
        pass
    with pg_conn.cursor() as cur:
        cur.execute("DELETE FROM pipeline.reconciliation_log WHERE run_id=%s", (rid,))
        cur.execute("DELETE FROM pipeline.run_stage_log WHERE run_id=%s", (rid,))
        cur.execute("DELETE FROM pipeline.run_log WHERE run_id=%s", (rid,))
    pg_conn.commit()


def _stage_count(conn, rid):
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM pipeline.run_stage_log WHERE run_id=%s", (rid,))
        return cur.fetchone()[0]


def _recon_count(conn, rid):
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM pipeline.reconciliation_log WHERE run_id=%s", (rid,))
        return cur.fetchone()[0]


def test_record_result_rolls_back_when_recon_raises(pg_conn, isolated_run, monkeypatch):
    """Mid-flow exception leaves no partial rows.

    Sequence: caller opens tx, calls messages.start_run (writes run_log +
    message_receive started row), calls record_result which proceeds through
    finish + several writes, then write_check raises before recon row is
    written.  The caller's `with conn:` block rolls back; the entire flow
    must be invisible.
    """
    rid = isolated_run

    # Start the run + open the receive stage in its own committed tx, so we
    # have a baseline for "rows written by record_result only".
    messages.start_run(
        pg_conn,
        run_id=rid,
        domain="insurance",
        dataset="claims_event",
        source_application="claims-api",
        correlation={"source_batch_id": "batch-1"},
        expected_count=10,
    )
    pg_conn.commit()
    baseline_stages = _stage_count(pg_conn, rid)
    assert baseline_stages == 1, "start_run should have written one stage_started row"
    assert _recon_count(pg_conn, rid) == 0

    boom = RuntimeError("simulated reconciliation failure mid-flow")

    def explode(*args, **kwargs):
        raise boom

    monkeypatch.setattr(reconciliation, "write_check", explode)

    with pytest.raises(RuntimeError, match="simulated reconciliation failure"):
        with pg_conn:  # commits on success, rollbacks on exception
            messages.record_result(
                pg_conn,
                run_id=rid,
                domain="insurance",
                dataset="claims_event",
                source_count=10,
                published_count=10,
            )

    # The connection's tx is rolled back. record_result wrote stage rows
    # before write_check raised; those writes must NOT be visible.
    pg_conn.rollback()  # release any leftover idle-in-tx state
    assert _stage_count(pg_conn, rid) == baseline_stages, (
        "stage rows written inside record_result before the recon failure "
        "must not be visible after rollback"
    )
    assert _recon_count(pg_conn, rid) == 0


def test_record_result_commits_atomically_on_success(pg_conn, isolated_run):
    """Happy path: caller `with conn:` commits the whole flow at end."""
    rid = isolated_run
    messages.start_run(
        pg_conn,
        run_id=rid,
        domain="insurance",
        dataset="claims_event",
        source_application="claims-api",
        correlation={"source_batch_id": "batch-2"},
        expected_count=5,
    )
    pg_conn.commit()

    with pg_conn:
        status = messages.record_result(
            pg_conn,
            run_id=rid,
            domain="insurance",
            dataset="claims_event",
            source_count=5,
            published_count=5,
        )
    assert status == "succeeded"
    assert _stage_count(pg_conn, rid) >= 3  # finish + validate + recon
    assert _recon_count(pg_conn, rid) == 1


def test_runs_update_respects_commit_false(pg_conn, isolated_run):
    """`commit=False` defers to caller; nothing visible until caller commits."""
    rid = isolated_run
    runs.start(
        pg_conn, run_id=rid, pipeline_type="message_api",
        domain="insurance", dataset="claims_event",
        business_date="2026-04-28", file_id=None, config_version_id=1,
    )
    pg_conn.commit()

    runs.update(pg_conn, rid, status="failed", commit=False)
    pg_conn.rollback()
    with pg_conn.cursor() as cur:
        cur.execute("SELECT status FROM pipeline.run_log WHERE run_id=%s", (rid,))
        (status,) = cur.fetchone()
    assert status == "running", "commit=False + caller rollback must not persist"


def test_stages_start_idempotent_under_migration_19(pg_conn, isolated_run):
    """Migration 19 partial unique index — duplicate stage_started is a no-op.

    Only takes effect once `stages.start` is updated to use ON CONFLICT
    DO NOTHING (Step 3.5 narrowed scope).  Without that, the second call
    raises UniqueViolation; with it, the call is a silent no-op and the
    table still has exactly one stage_started row for the attempt.
    """
    rid = isolated_run
    runs.start(
        pg_conn, run_id=rid, pipeline_type="s3_batch",
        domain="insurance", dataset="policies",
        business_date="2026-04-28", file_id=None, config_version_id=1,
    )
    stages.start(pg_conn, run_id=rid, stage="raw_read", attempt_number=1,
                 input_ref="s3://raw/x.csv")
    # Second start for the same (run, stage, attempt) — should be a no-op.
    stages.start(pg_conn, run_id=rid, stage="raw_read", attempt_number=1,
                 input_ref="s3://raw/x.csv")
    with pg_conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM pipeline.run_stage_log "
            "WHERE run_id=%s AND stage='raw_read' AND attempt_number=1 "
            "  AND event_type='stage_started'",
            (rid,),
        )
        (count,) = cur.fetchone()
    assert count == 1, f"expected 1 stage_started row after duplicate start, got {count}"
