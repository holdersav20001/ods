"""pipeline.lineage_edge operations."""
from __future__ import annotations


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
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline.lineage_edge
                    (child_run_id, parent_run_id, parent_file_id,
                     edge_type, source_ref, target_ref, record_count)
                VALUES (%s,%s,%s, %s,%s,%s,%s)
                """,
                (
                    child_run_id, parent_run_id, parent_file_id,
                    edge_type, source_ref, target_ref, record_count,
                ),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
