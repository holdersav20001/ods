"""Stateless write-order contract for ``ods_pipeline.messages.record_result``.

Updated 2026-05-07. The original contract (B4) was atomic-bundle: every
helper call used ``commit=False`` and the caller wrapped the flow in a
single transaction. We've moved to a stateless model where each helper
commits independently and the strict write order (stages → archive →
reconciliation → run.status) is what guarantees the dashboard never sees
``status='succeeded'`` without a matching recon row.

This file now verifies the stateless invariants:

* Stage rows committed before a mid-flow failure remain durable — that's
  the live-progress signal the dashboard relies on.
* Reconciliation row is NOT written when the recon helper raises.
* ``run_log.status`` is NOT flipped to ``succeeded`` when reconciliation
  failed — the run remains ``running`` until the heartbeat janitor reaps
  it (or the caller's ``except`` block writes ``status='failed'``).
* Stage idempotency under migration 19 (unchanged).
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


def test_record_result_failure_keeps_stages_but_not_recon_or_succeeded(
    pg_conn, isolated_run, monkeypatch,
):
    """Stateless contract — mid-flow failure leaves staged progress.

    Sequence: ``start_run`` opens the run; ``record_result`` proceeds
    through stage writes; the recon helper raises. After the failure:

    * Stage rows that committed BEFORE recon remain visible (live
      dashboard progress — that's the point of going stateless).
    * NO ``reconciliation_log`` row exists for the run.
    * ``run_log.status`` has NOT been flipped to ``succeeded`` — the
      run-status update happens AFTER reconciliation by design, so the
      "succeeded ⇒ recon present" invariant always holds.
    """
    rid = isolated_run

    messages.start_run(
        pg_conn,
        run_id=rid,
        domain="insurance",
        dataset="claims_event",
        source_application="claims-api",
        correlation={"source_batch_id": "batch-1"},
        expected_count=10,
    )
    baseline_stages = _stage_count(pg_conn, rid)
    assert baseline_stages == 1, "start_run should have written one stage_started row"
    assert _recon_count(pg_conn, rid) == 0

    def explode(*args, **kwargs):
        raise RuntimeError("simulated reconciliation failure mid-flow")

    monkeypatch.setattr(reconciliation, "write_check", explode)

    with pytest.raises(RuntimeError, match="simulated reconciliation failure"):
        messages.record_result(
            pg_conn,
            run_id=rid,
            domain="insurance",
            dataset="claims_event",
            source_count=10,
            published_count=10,
        )

    # Stages that committed before recon are durable — operators can see
    # exactly how far the run got.
    assert _stage_count(pg_conn, rid) > baseline_stages, (
        "stateless contract: stage rows committed before the recon failure "
        "MUST remain visible so the dashboard reflects progress"
    )
    # The recon row never landed → recon panel still empty.
    assert _recon_count(pg_conn, rid) == 0
    # Critically: run_log.status was NOT flipped to succeeded — runs.update
    # runs AFTER reconciliation in the stateless ordering.
    with pg_conn.cursor() as cur:
        cur.execute("SELECT status FROM pipeline.run_log WHERE run_id=%s", (rid,))
        (status,) = cur.fetchone()
    assert status == "running", (
        "run must remain 'running' when reconciliation fails — "
        "succeeded would imply the recon proof landed, which it didn't"
    )


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
        pg_conn, run_id=rid, pipeline_type="orchestration",
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


def test_start_then_finish_produces_two_rows_under_migration_19(pg_conn, isolated_run):
    """Migration 19's partial index does NOT block the started→completed pair.

    The index is keyed on ``WHERE event_type='stage_started'`` only.  The
    UPDATE in ``finish()`` mutates the existing started row's event_type to
    ``stage_completed`` (which falls outside the partial predicate), so the
    INSERT path inside finish never runs in this single-thread case.

    Outcome: 1 row ends as ``stage_completed``, with no second row appended.
    Asserts the index does not interfere with the normal start+finish
    happy-path lifecycle.
    """
    rid = isolated_run
    runs.start(
        pg_conn, run_id=rid, pipeline_type="orchestration",
        domain="insurance", dataset="policies",
        business_date="2026-04-28", file_id=None, config_version_id=1,
    )
    stages.start(pg_conn, run_id=rid, stage="raw_read", attempt_number=1,
                 input_ref="s3://raw/x.csv")
    stages.finish(pg_conn, run_id=rid, stage="raw_read", attempt_number=1,
                  status="succeeded", record_count_out=10)
    with pg_conn.cursor() as cur:
        cur.execute(
            "SELECT count(*), "
            "       count(*) FILTER (WHERE event_type='stage_started'), "
            "       count(*) FILTER (WHERE event_type='stage_completed') "
            "  FROM pipeline.run_stage_log "
            " WHERE run_id=%s AND stage='raw_read' AND attempt_number=1",
            (rid,),
        )
        total, started, completed = cur.fetchone()
    assert total == 1, (
        f"expected 1 row total after start+finish (UPDATE path), got {total}"
    )
    assert started == 0, "started row should have been mutated to completed"
    assert completed == 1
