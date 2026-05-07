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

import json
from contextlib import contextmanager

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
    is_open = (
        event_type == StageEvent.STARTED
        or (event_type is None and status == "running")
    )
    ended_at_sql = "NULL" if is_open else "NOW()"
    # Only the started-event path participates in the partial unique index
    # added by migration 19. Terminal events are append-only.
    on_conflict_sql = (
        "ON CONFLICT (run_id, stage, attempt_number) WHERE event_type = 'stage_started' DO NOTHING"
        if is_open
        else ""
    )

    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO pipeline.run_stage_log
                    (run_id, stage, status, event_type, attempt_number,
                     started_at, ended_at,
                     input_ref, output_ref,
                     record_count_in, record_count_out,
                     metrics, error,
                     airflow_dag_id, airflow_run_id, spark_app_id)
                VALUES (%s,%s,%s,%s,%s, NOW(), {ended_at_sql},
                        %s,%s, %s,%s, %s,%s, %s,%s,%s)
                {on_conflict_sql}
                """,
                (
                    run_id, stage, status, event_type, attempt_number,
                    input_ref, output_ref,
                    record_count_in, record_count_out,
                    json.dumps(metrics) if metrics else None, error,
                    airflow_dag_id, airflow_run_id, spark_app_id,
                ),
            )
        if commit:
            conn.commit()
    except Exception:
        if commit:
            conn.rollback()
        raise


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
    if attempt_number is None:
        attempt_number = next_attempt_number(conn, run_id=run_id, stage=stage)
    write(
        conn,
        run_id=run_id,
        stage=stage,
        status="running",
        event_type=StageEvent.STARTED,
        attempt_number=attempt_number,
        **kwargs,
    )
    return attempt_number


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
    terminal_event = event_type or (
        StageEvent.FAILED if status == "failed" else StageEvent.COMPLETED
    )
    metrics_json = json.dumps(metrics) if metrics else None
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id
                  FROM pipeline.run_stage_log
                 WHERE run_id=%s
                   AND stage=%s
                   AND attempt_number=%s
                   AND status='running'
                   AND ended_at IS NULL
                 ORDER BY started_at DESC, id DESC
                 LIMIT 1
                 FOR UPDATE SKIP LOCKED
                """,
                (run_id, stage, attempt_number),
            )
            row = cur.fetchone()
            if row is not None:
                cur.execute(
                    """
                    UPDATE pipeline.run_stage_log
                       SET status=%s,
                           event_type=%s,
                           ended_at=NOW(),
                           input_ref=COALESCE(%s, input_ref),
                           output_ref=COALESCE(%s, output_ref),
                           record_count_in=COALESCE(%s, record_count_in),
                           record_count_out=COALESCE(%s, record_count_out),
                           metrics=COALESCE(%s, metrics),
                           error=COALESCE(%s, error),
                           airflow_dag_id=COALESCE(%s, airflow_dag_id),
                           airflow_run_id=COALESCE(%s, airflow_run_id),
                           spark_app_id=COALESCE(%s, spark_app_id)
                     WHERE id=%s
                    """,
                    (
                        status, terminal_event,
                        input_ref, output_ref,
                        record_count_in, record_count_out,
                        metrics_json, error,
                        airflow_dag_id, airflow_run_id, spark_app_id,
                        row[0],
                    ),
                )
            else:
                # Inline append kept inside the same tx so finish() is atomic.
                # Mirrors the column set in write(); ended_at=NOW() because the
                # event is terminal.
                cur.execute(
                    """
                    INSERT INTO pipeline.run_stage_log
                        (run_id, stage, status, event_type, attempt_number,
                         started_at, ended_at,
                         input_ref, output_ref,
                         record_count_in, record_count_out,
                         metrics, error,
                         airflow_dag_id, airflow_run_id, spark_app_id)
                    VALUES (%s,%s,%s,%s,%s, NOW(), NOW(),
                            %s,%s, %s,%s, %s,%s, %s,%s,%s)
                    """,
                    (
                        run_id, stage, status, terminal_event, attempt_number,
                        input_ref, output_ref,
                        record_count_in, record_count_out,
                        metrics_json, error,
                        airflow_dag_id, airflow_run_id, spark_app_id,
                    ),
                )
        if commit:
            conn.commit()
    except Exception:
        if commit:
            conn.rollback()
        raise


@contextmanager
def stage_scope(
    conn,
    *,
    run_id: str,
    stage: str,
    record_count_in: int | None = None,
    input_ref: str | None = None,
    metrics: dict | None = None,
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
    )
    box: dict = {
        "record_count_out": None,
        "output_ref": None,
        "metrics": None,
    }
    state: dict = {
        "skipped": False,
        "skip_reason": None,
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
            state["skipped"] = True
            state["skip_reason"] = reason

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
            )
        except Exception:
            # Last-ditch — heartbeat janitor catches any orphaned row.
            try:
                conn.rollback()
            except Exception:
                pass
        raise
    else:
        # Merge skip_reason into metrics so the dashboard can render WHY
        # a skip happened without inventing a new column.
        finish_metrics = box["metrics"]
        if state["skipped"] and state["skip_reason"] is not None:
            finish_metrics = dict(finish_metrics or {})
            finish_metrics.setdefault("skip_reason", state["skip_reason"])
        finish(
            conn,
            run_id=run_id,
            stage=stage,
            status="skipped" if state["skipped"] else "succeeded",
            event_type=StageEvent.SKIPPED if state["skipped"] else StageEvent.COMPLETED,
            attempt_number=attempt,
            record_count_in=record_count_in,
            record_count_out=box["record_count_out"],
            output_ref=box["output_ref"],
            metrics=finish_metrics,
        )
