"""pipeline.lineage_edge operations."""
from __future__ import annotations

import ods_ingestion_control as control


def write_edge(
    conn,
    *,
    consumer_run_id: str,
    edge_type: str,
    upstream_run_id: str | None = None,
    source_file_id: str | None = None,
    source_ref: str | None = None,
    target_ref: str | None = None,
    record_count: int | None = None,
) -> None:
    """Insert one row into ``pipeline.lineage_edge``.

    Either ``upstream_run_id`` (the run that produced the data being consumed)
    or ``source_file_id`` (the file being consumed) — or both — must be
    supplied. This is *data lineage*. The orchestration parent (the DAG/route
    run that scheduled the work) belongs in ``run_log.orchestrators``, not
    here.

    Common *edge_type* values (use these constants in callers):
      * ``"raw_to_curated"``   — S3 raw → S3 curated (written by ingestion job)
      * ``"curated_to_kafka"`` — S3 curated → Kafka topic (written by publish job)
      * ``"curated_to_postgres"`` — S3 curated → Postgres table
    """
    if upstream_run_id is None and source_file_id is None:
        raise ValueError(
            "write_edge requires at least one of "
            "upstream_run_id or source_file_id"
        )
    control.write_lineage_edge(
        conn,
        consumer_run_id=consumer_run_id,
        edge_type=edge_type,
        upstream_run_id=upstream_run_id,
        source_file_id=source_file_id,
        source_ref=source_ref,
        target_ref=target_ref,
        record_count=record_count,
    )
