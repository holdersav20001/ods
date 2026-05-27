"""pipeline.lineage_edge operations."""
from __future__ import annotations

import ods_ingestion_control as control


def write_edge(
    conn,
    *,
    child_run_id: str,
    edge_type: str,
    parent_run_id: str | None = None,
    parent_file_id: str | None = None,
    source_ref: str | None = None,
    target_ref: str | None = None,
    record_count: int | None = None,
) -> None:
    """Insert one row into ``pipeline.lineage_edge``.

    Either ``parent_run_id`` or ``parent_file_id`` (or both) should be supplied.

    Common *edge_type* values (use these constants in callers):
      * ``"raw_to_curated"``   — S3 raw → S3 curated (written by ingestion job)
      * ``"curated_to_kafka"`` — S3 curated → Kafka topic (written by publish job)
      * ``"curated_to_postgres"`` — S3 curated → Postgres table
    """
    if parent_run_id is None and parent_file_id is None:
        raise ValueError(
            "write_edge requires at least one of parent_run_id or parent_file_id"
        )
    control.write_lineage_edge(
        conn,
        child_run_id=child_run_id,
        edge_type=edge_type,
        parent_run_id=parent_run_id,
        parent_file_id=parent_file_id,
        source_ref=source_ref,
        target_ref=target_ref,
        record_count=record_count,
    )
