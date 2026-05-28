"""pipeline.run_stage_log operations.

Stateless control-plane contract
--------------------------------

Every helper here defaults to ``commit=True`` — each stage row is its own
durable checkpoint. Long pipelines therefore expose live progress on the
operator dashboard and survive worker death without leaving an open
transaction holding row locks.

The trade-off is that the application code must explicitly write a
``stage_failed`` row in its exception handler. The :func:`stage_scope`
context manager below packages that discipline so callers can't forget:

    >>> with stage_scope(conn, run_id=run_id, stage=Stage.MESSAGE_RECEIVE,
    ...                  record_count_in=1):
    ...     do_work()                    # success → stage_completed
    ...                                  # raise   → stage_failed

If even the failure write fails (DB flapping, connection dead) the
context manager swallows the secondary error so the original work
exception still propagates; a heartbeat-staleness janitor closes any
``status='running'`` rows it leaves behind.
"""
from __future__ import annotations

from contextlib import contextmanager

import ods_ingestion_control as control
from ods_pipeline.models import StageEvent


def write(
    conn,
    *,
    run_id: str,
    stage: str,
    status: str,
    event_type: str | None = None,
    attempt_number: int = 1,
    input_ref: str | None = None,
    output_ref: str | None = None,
    record_count_in: int | None = None,
    record_count_out: int | None = None,
    metrics: dict | None = None,
    error: str | None = None,
    airflow_dag_id: str | None = None,
    airflow_run_id: str | None = None,
    spark_app_id: str | None = None,
    commit: bool = True,
) -> None:
    """Append one row to ``pipeline.run_stage_log``.

    ``started_at`` is always set to ``NOW()``.
    ``ended_at`` is set to ``NOW()`` only for terminal events
    (completed / failed / skipped / warned).  For ``stage_started`` /
    ``status='running'`` it is left NULL so the open interval is
    queryable.

    Idempotency (B4, migration 19): inserting a ``stage_started`` row that
    duplicates an existing open attempt is a no-op (partial unique index
    ``run_stage_log_started_unique``).  Terminal events remain append-only.

    ``commit``: when True (default), the helper commits its own transaction.
    When False, the caller owns the surrounding tx (used by atomic
    ``record_result`` flow).
    """
    control.write_stage_event(
        conn,
        run_id=run_id,
        stage=stage,
        status=status,
        event_type=event_type,
        attempt_number=attempt_number,
        input_ref=input_ref,
        output_ref=output_ref,
        record_count_in=record_count_in,
        record_count_out=record_count_out,
        metrics=metrics,
        error=error,
        airflow_dag_id=airflow_dag_id,
        airflow_run_id=airflow_run_id,
        spark_app_id=spark_app_id,
        commit=commit,
    )


def next_attempt_number(conn, *, run_id: str, stage: str) -> int:
    """Return the next free ``attempt_number`` for ``(run_id, stage)``.

    Stateless control-plane retries leave durable stage rows from the
    failed attempts; the unique index on ``(run_id, stage, event_type,
    attempt_number)`` requires the new attempt to use ``MAX+1`` rather
    than always 1.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COALESCE(MAX(attempt_number), 0) + 1
              FROM pipeline.run_stage_log
             WHERE run_id=%s AND stage=%s
            """,
            (run_id, stage),
        )
        row = cur.fetchone()
        return int(row[0]) if row and row[0] is not None else 1


def start(
    conn,
    *,
    run_id: str,
    stage: str,
    attempt_number: int | None = None,
    **kwargs,
) -> int:
    """Open a stage attempt with ``status='running'`` and ``ended_at=NULL``.

    When ``attempt_number`` is omitted, :func:`next_attempt_number` is
    consulted so retries land on a fresh row instead of colliding on the
    unique index. Returns the attempt number used so the caller can pass
    it to a later :func:`finish` for the same attempt.
    """
    return control.start_stage(
        conn,
        run_id=run_id,
        stage=stage,
        attempt_number=attempt_number,
        input_ref=kwargs.get("input_ref"),
        record_count_in=kwargs.get("record_count_in"),
        metrics=kwargs.get("metrics"),
        airflow_dag_id=kwargs.get("airflow_dag_id"),
        airflow_run_id=kwargs.get("airflow_run_id"),
        spark_app_id=kwargs.get("spark_app_id"),
    )


def finish(
    conn,
    *,
    run_id: str,
    stage: str,
    status: str,
    event_type: str | None = None,
    attempt_number: int = 1,
    input_ref: str | None = None,
    output_ref: str | None = None,
    record_count_in: int | None = None,
    record_count_out: int | None = None,
    metrics: dict | None = None,
    error: str | None = None,
    airflow_dag_id: str | None = None,
    airflow_run_id: str | None = None,
    spark_app_id: str | None = None,
    commit: bool = True,
) -> None:
    """Close the latest open stage row, falling back to append if none exists.

    Concurrency contract (B3): the open-row claim uses
    ``SELECT ... FOR UPDATE SKIP LOCKED LIMIT 1`` and the subsequent
    UPDATE-or-INSERT runs in the same transaction (single ``conn.commit()``
    at the end).  Two concurrent ``finish`` calls for the same
    ``(run_id, stage, attempt_number)`` therefore resolve as: the lock
    holder UPDATEs the open row, the loser sees no claimable row and
    INSERTs a fresh terminal row — never a silent no-op or double-update.
    """
    control.finish_stage(
        conn,
        run_id=run_id,
        stage=stage,
        status=status,
        event_type=event_type,
        attempt_number=attempt_number,
        input_ref=input_ref,
        output_ref=output_ref,
        record_count_in=record_count_in,
        record_count_out=record_count_out,
        metrics=metrics,
        error=error,
        airflow_dag_id=airflow_dag_id,
        airflow_run_id=airflow_run_id,
        spark_app_id=spark_app_id,
        commit=commit,
    )


@contextmanager
def stage_scope(
    conn,
    *,
    run_id: str,
    stage: str,
    record_count_in: int | None = None,
    input_ref: str | None = None,
    metrics: dict | None = None,
    airflow_dag_id: str | None = None,
    airflow_run_id: str | None = None,
    spark_app_id: str | None = None,
    truncate_error: int = 500,
):
    """Context manager that opens a stage on entry and closes it on exit.

    Stateless-control-plane contract:

    * On entry: writes ``stage_started`` (with auto-incremented
      ``attempt_number``) and commits. Live on the dashboard immediately.
    * On clean exit: writes ``stage_completed``. The caller may yield a
      result dict via ``ctx.set_result(...)`` — its keys merge into the
      finish call so ``output_ref``, ``record_count_out``, etc. land on
      the same row.
    * On ``s.skip(reason)``: clean exit, but writes ``stage_skipped``
      instead of ``stage_completed``. Use when a stage legitimately ran
      but had nothing to do (api_pull saw no changes, file_pipeline saw
      an empty file). ``reason`` is folded into the row's ``metrics`` so
      the dashboard can surface why.
    * On exception: writes ``stage_failed`` with the exception message
      (truncated to ``truncate_error`` chars) and re-raises the original.
      If the failure write itself fails, swallow that secondary error —
      the heartbeat janitor will close the row.

    Usage:

        with stage_scope(conn, run_id=rid, stage=Stage.MESSAGE_RECEIVE) as s:
            count = do_work()
            if count == 0:
                s.skip("no_changes")
            else:
                s.set_result(record_count_out=count, output_ref="s3://...")
    """
    attempt = start(
        conn,
        run_id=run_id,
        stage=stage,
        record_count_in=record_count_in,
        input_ref=input_ref,
        metrics=metrics,
        airflow_dag_id=airflow_dag_id,
        airflow_run_id=airflow_run_id,
        spark_app_id=spark_app_id,
    )
    box: dict = {
        "record_count_out": None,
        "output_ref": None,
        "metrics": None,
    }
    state: dict = {
        "outcome": "succeeded",   # succeeded | skipped | warned
        "reason": None,           # narrative for skipped / warned
    }

    class _Scope:
        attempt_number = attempt

        def set_result(self, **fields) -> None:
            for key, value in fields.items():
                if key not in box:
                    raise KeyError(
                        f"stage_scope only accepts {sorted(box)}; got {key!r}"
                    )
                box[key] = value

        def skip(self, reason: str | None = None) -> None:
            """Mark the stage as skipped on clean exit.

            Idempotent: a second ``skip()`` overwrites the reason. A
            subsequent exception still wins (skip is for clean exits
            only).
            """
            state["outcome"] = "skipped"
            state["reason"] = reason

        def warn(self, reason: str | None = None) -> None:
            """Mark the stage as warned on clean exit.

            Use when work completed but produced soft-failure signals
            (DQ rules fired, partial DLQ writes, schema drift below the
            blocking threshold). Distinct from ``skip`` (which means
            "ran but had nothing to do") and ``raise`` (which means
            "failed hard"). ``reason`` lands on the stage row's
            ``error`` column so the dashboard can surface it next to
            other failure signals.
            """
            state["outcome"] = "warned"
            state["reason"] = reason

    try:
        yield _Scope()
    except BaseException as exc:
        # BaseException catches SystemExit / KeyboardInterrupt — those
        # also leave the stage row stuck if we don't close it.
        try:
            finish(
                conn,
                run_id=run_id,
                stage=stage,
                status="failed",
                event_type=StageEvent.FAILED,
                attempt_number=attempt,
                record_count_in=record_count_in,
                record_count_out=box["record_count_out"],
                output_ref=box["output_ref"],
                metrics=box["metrics"],
                error=str(exc)[:truncate_error] if str(exc) else type(exc).__name__,
                airflow_dag_id=airflow_dag_id,
                airflow_run_id=airflow_run_id,
                spark_app_id=spark_app_id,
            )
        except Exception:
            # Last-ditch — heartbeat janitor catches any orphaned row.
            try:
                conn.rollback()
            except Exception:
                pass
        raise
    else:
        # Map clean-exit outcomes to status / event_type. ``skip`` /
        # ``warn`` reasons fold into the appropriate column (metrics
        # for skip — non-failure signal; error for warn — failure
        # signal that wasn't blocking).
        finish_metrics = box["metrics"]
        finish_error: str | None = None
        outcome = state["outcome"]
        if outcome == "skipped":
            if state["reason"] is not None:
                finish_metrics = dict(finish_metrics or {})
                finish_metrics.setdefault("skip_reason", state["reason"])
            finish_status = "skipped"
            finish_event = StageEvent.SKIPPED
        elif outcome == "warned":
            finish_status = "warned"
            finish_event = StageEvent.WARNED
            if state["reason"] is not None:
                finish_error = state["reason"]
        else:
            finish_status = "succeeded"
            finish_event = StageEvent.COMPLETED
        finish(
            conn,
            run_id=run_id,
            stage=stage,
            status=finish_status,
            event_type=finish_event,
            attempt_number=attempt,
            record_count_in=record_count_in,
            record_count_out=box["record_count_out"],
            output_ref=box["output_ref"],
            metrics=finish_metrics,
            error=finish_error,
            airflow_dag_id=airflow_dag_id,
            airflow_run_id=airflow_run_id,
            spark_app_id=spark_app_id,
        )
