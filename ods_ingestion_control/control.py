"""Thin Python wrapper around ``pipeline.control_*`` database functions.

The intent is deliberately narrow: application code gets a small, typed Python
surface, while Postgres owns the control-table state transitions.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from psycopg2.extras import Json

JsonLike = Mapping[str, Any] | Sequence[Any] | str | int | float | bool


def _json(value: JsonLike | None) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return Json(value)


def _call(conn: Any, sql: str, params: tuple[Any, ...], *, commit: bool) -> Any:
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone() if hasattr(cur, "fetchone") else None
        if commit:
            conn.commit()
    except Exception:
        if commit:
            conn.rollback()
        raise
    return row[0] if row else None


def start_run(
    conn: Any,
    *,
    run_id: str,
    pipeline_type: str,
    domain: str,
    dataset: str,
    business_date: str | None = None,
    file_id: str | None = None,
    kafka_topic: str | None = None,
    config_version_id: int | None = None,
    schema_version_id: int | None = None,
    orchestrators: JsonLike | None = None,
    runtime_context: JsonLike | None = None,
    commit: bool = True,
) -> str:
    """Create or validate a ``pipeline.run_log`` row."""
    return _call(
        conn,
        """
        SELECT pipeline.control_start_run(
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
        )
        """,
        (
            run_id,
            pipeline_type,
            domain,
            dataset,
            business_date,
            file_id,
            kafka_topic,
            config_version_id,
            schema_version_id,
            _json(orchestrators),
            _json(runtime_context),
        ),
        commit=commit,
    )


def update_run(
    conn: Any,
    *,
    run_id: str,
    status: str | None = None,
    record_count_source: int | None = None,
    record_count_dq_pass: int | None = None,
    record_count_dq_fail: int | None = None,
    record_count_published: int | None = None,
    kafka_topic: str | None = None,
    kafka_offset_start: int | None = None,
    kafka_offset_end: int | None = None,
    error_summary: str | None = None,
    runtime_context: JsonLike | None = None,
    commit: bool = True,
) -> str:
    """Update allowed ``pipeline.run_log`` fields."""
    return _call(
        conn,
        """
        SELECT pipeline.control_update_run(
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
        )
        """,
        (
            run_id,
            status,
            record_count_source,
            record_count_dq_pass,
            record_count_dq_fail,
            record_count_published,
            kafka_topic,
            kafka_offset_start,
            kafka_offset_end,
            error_summary,
            _json(runtime_context),
        ),
        commit=commit,
    )


def patch_run(
    conn: Any,
    *,
    run_id: str,
    fields: Mapping[str, Any],
    commit: bool = True,
) -> str:
    """Patch arbitrary whitelisted ``pipeline.run_log`` fields.

    Unlike ``update_run``, this preserves the distinction between omitted fields
    and fields explicitly set to ``None``.
    """
    return _call(
        conn,
        "SELECT pipeline.control_patch_run(%s,%s)",
        (run_id, Json(dict(fields))),
        commit=commit,
    )


def register_file(
    conn: Any,
    *,
    domain: str,
    dataset: str,
    business_date: str,
    file_md5: str,
    s3_raw_path: str,
    file_id: str | None = None,
    sftp_path: str | None = None,
    s3_curated_path: str | None = None,
    file_size_bytes: int | None = None,
    source_row_count: int | None = None,
    state: str = "ingesting",
    last_run_id: str | None = None,
    commit: bool = True,
) -> str:
    """Insert or update a ``pipeline.file_catalogue`` row."""
    return _call(
        conn,
        """
        SELECT pipeline.control_register_file(
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
        )
        """,
        (
            domain,
            dataset,
            business_date,
            file_md5,
            s3_raw_path,
            file_id,
            sftp_path,
            s3_curated_path,
            file_size_bytes,
            source_row_count,
            state,
            last_run_id,
        ),
        commit=commit,
    )


def update_file_catalogue(
    conn: Any,
    *,
    file_id: str | None = None,
    s3_raw_path: str | None = None,
    state: str | None = None,
    s3_curated_path: str | None = None,
    source_row_count: int | None = None,
    last_run_id: str | None = None,
    commit: bool = True,
) -> str:
    """Update mutable file catalogue state by ``file_id`` or ``s3_raw_path``."""
    return _call(
        conn,
        """
        SELECT pipeline.control_update_file_catalogue(
            %s,%s,%s,%s,%s,%s
        )
        """,
        (
            file_id,
            s3_raw_path,
            state,
            s3_curated_path,
            source_row_count,
            last_run_id,
        ),
        commit=commit,
    )


def set_file_state(
    conn: Any,
    *,
    s3_path: str,
    run_id: str,
    status: str,
    record_count: int | None = None,
    error_reason: str | None = None,
    commit: bool = True,
) -> str:
    """Upsert ``pipeline.file_processing_attempt`` for an S3 path."""
    return _call(
        conn,
        "SELECT pipeline.control_set_file_state(%s,%s,%s,%s,%s)",
        (s3_path, run_id, status, record_count, error_reason),
        commit=commit,
    )


def start_stage(
    conn: Any,
    *,
    run_id: str,
    stage: str,
    attempt_number: int | None = None,
    input_ref: str | None = None,
    record_count_in: int | None = None,
    metrics: JsonLike | None = None,
    airflow_dag_id: str | None = None,
    airflow_run_id: str | None = None,
    spark_app_id: str | None = None,
    commit: bool = True,
) -> int:
    """Open a stage row and return the attempt number used."""
    return _call(
        conn,
        """
        SELECT pipeline.control_start_stage(
            %s,%s,%s,%s,%s,%s,%s,%s,%s
        )
        """,
        (
            run_id,
            stage,
            attempt_number,
            input_ref,
            record_count_in,
            _json(metrics),
            airflow_dag_id,
            airflow_run_id,
            spark_app_id,
        ),
        commit=commit,
    )


def write_stage_event(
    conn: Any,
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
    metrics: JsonLike | None = None,
    error: str | None = None,
    airflow_dag_id: str | None = None,
    airflow_run_id: str | None = None,
    spark_app_id: str | None = None,
    commit: bool = True,
) -> int:
    """Append one ``pipeline.run_stage_log`` event row."""
    return _call(
        conn,
        """
        SELECT pipeline.control_write_stage_event(
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
        )
        """,
        (
            run_id,
            stage,
            status,
            event_type,
            attempt_number,
            input_ref,
            output_ref,
            record_count_in,
            record_count_out,
            _json(metrics),
            error,
            airflow_dag_id,
            airflow_run_id,
            spark_app_id,
        ),
        commit=commit,
    )


def finish_stage(
    conn: Any,
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
    metrics: JsonLike | None = None,
    error: str | None = None,
    airflow_dag_id: str | None = None,
    airflow_run_id: str | None = None,
    spark_app_id: str | None = None,
    commit: bool = True,
) -> int:
    """Close or append a terminal ``pipeline.run_stage_log`` row."""
    return _call(
        conn,
        """
        SELECT pipeline.control_finish_stage(
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
        )
        """,
        (
            run_id,
            stage,
            status,
            event_type,
            attempt_number,
            input_ref,
            output_ref,
            record_count_in,
            record_count_out,
            _json(metrics),
            error,
            airflow_dag_id,
            airflow_run_id,
            spark_app_id,
        ),
        commit=commit,
    )


def write_lineage_edge(
    conn: Any,
    *,
    consumer_run_id: str,
    edge_type: str,
    upstream_run_id: str | None = None,
    source_file_id: str | None = None,
    source_ref: str | None = None,
    target_ref: str | None = None,
    record_count: int | None = None,
    commit: bool = True,
) -> int:
    """Insert one ``pipeline.lineage_edge`` row."""
    return _call(
        conn,
        """
        SELECT pipeline.control_write_lineage_edge(
            %s,%s,%s,%s,%s,%s,%s
        )
        """,
        (
            consumer_run_id,
            edge_type,
            upstream_run_id,
            source_file_id,
            source_ref,
            target_ref,
            record_count,
        ),
        commit=commit,
    )


def write_reconciliation_check(
    conn: Any,
    *,
    check_type: str,
    run_id: str,
    domain: str,
    dataset: str,
    business_date: str,
    source_count: int | None = None,
    kafka_count: int | None = None,
    postgres_count: int | None = None,
    status: str = "ok",
    detail: str | None = None,
    window_start: Any = None,
    window_end: Any = None,
    commit: bool = True,
) -> int:
    """Insert one ``pipeline.reconciliation_log`` row."""
    return _call(
        conn,
        """
        SELECT pipeline.control_write_reconciliation_check(
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
        )
        """,
        (
            check_type,
            run_id,
            domain,
            dataset,
            business_date,
            source_count,
            kafka_count,
            postgres_count,
            status,
            detail,
            window_start,
            window_end,
        ),
        commit=commit,
    )


def record_run_event(
    conn: Any,
    *,
    run_id: str,
    event_type: str,
    domain: str,
    dataset: str,
    business_date: str,
    status: str,
    pipeline_type: str | None = None,
    record_count_source: int | None = None,
    record_count_dq_pass: int | None = None,
    record_count_dq_fail: int | None = None,
    record_count_published: int | None = None,
    kafka_topic: str | None = None,
    kafka_offset_end: int | None = None,
    error_summary: str | None = None,
    occurred_at: Any = None,
    file_id: str | None = None,
    s3_raw_path: str | None = None,
    s3_curated_path: str | None = None,
    file_md5: str | None = None,
    kafka_offset_start: int | None = None,
    stages: JsonLike | None = None,
    commit: bool = True,
) -> int:
    """Insert one queryable ``pipeline.run_events`` row."""
    return _call(
        conn,
        """
        SELECT pipeline.control_record_run_event(
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
        )
        """,
        (
            run_id,
            event_type,
            domain,
            dataset,
            business_date,
            status,
            pipeline_type,
            record_count_source,
            record_count_dq_pass,
            record_count_dq_fail,
            record_count_published,
            kafka_topic,
            kafka_offset_end,
            error_summary,
            occurred_at,
            file_id,
            s3_raw_path,
            s3_curated_path,
            file_md5,
            kafka_offset_start,
            _json(stages),
        ),
        commit=commit,
    )
