"""pipeline.run_stage_log operations."""
from __future__ import annotations

import json


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
) -> None:
    """Append one row to ``pipeline.run_stage_log``.

    Started/running rows keep ``ended_at`` null. Terminal rows set both
    timestamps at insert time because this table is an append-only event log.
    """
    terminal_event = event_type in {
        "stage_completed",
        "stage_failed",
        "stage_skipped",
        "stage_warned",
    }
    terminal_status = status in {"succeeded", "failed", "skipped", "partial", "warned"}
    has_ended = terminal_event or (event_type is None and terminal_status)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline.run_stage_log
                    (run_id, stage, status, event_type, attempt_number,
                     started_at, ended_at,
                     input_ref, output_ref,
                     record_count_in, record_count_out,
                     metrics, error,
                     airflow_dag_id, airflow_run_id, spark_app_id)
                VALUES (%s,%s,%s,%s,%s, NOW(), CASE WHEN %s THEN NOW() ELSE NULL END,
                        %s,%s, %s,%s, %s,%s, %s,%s,%s)
                """,
                (
                    run_id, stage, status, event_type, attempt_number,
                    has_ended,
                    input_ref, output_ref,
                    record_count_in, record_count_out,
                    json.dumps(metrics) if metrics else None, error,
                    airflow_dag_id, airflow_run_id, spark_app_id,
                ),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
