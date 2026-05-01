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

    ``started_at`` and ``ended_at`` are both set to ``NOW()``.
    """
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
                VALUES (%s,%s,%s,%s,%s, NOW(), NOW(),
                        %s,%s, %s,%s, %s,%s, %s,%s,%s)
                """,
                (
                    run_id, stage, status, event_type, attempt_number,
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
