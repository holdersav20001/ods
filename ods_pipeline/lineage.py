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


import json
import uuid
from typing import Iterable, Mapping


def write_link(
    conn,
    *,
    consumer_run_id: str,
    edge_type: str,
    target_ref: str | None,
    record_count: int | None,
    contributions: Iterable[Mapping],
) -> str:
    """Atomically record one consumer write event + N source contributions.

    Inserts:
      * 1 row into ``pipeline.lineage_link`` (the bundle / write event)
      * N rows into ``pipeline.lineage_edge``, all sharing ``lineage_link_id``

    All inserts share a single transaction. Returns the new
    ``lineage_link_id`` so the caller can stamp it on every target row via
    ``_ods_lineage_link_id``.

    ``contributions`` items support keys:
      * ``upstream_run_id``  (str / UUID; optional — None for raw-file sources)
      * ``source_file_id``   (str / UUID; optional — None when there is no
                             registered source file)
      * ``source_ref``       (str; optional)
      * ``slot_name``        (str; optional — role tag for multi-source writes,
                              e.g. ``'core'``, ``'enrichment'``)
      * ``record_count``     (int; optional)
      * ``edge_type``        (str; optional — defaults to the link's
                              ``edge_type``)

    Single-source caller passes one contribution. Multi-source caller passes
    N. Either way the contract is identical — no branch logic.

    Use this helper for every consumer write on file-batch routes once
    migration 36 is applied. Old call sites that still use
    ``lineage.write_edge`` continue to work; their rows carry
    ``lineage_link_id = NULL``.
    """
    link_id = str(uuid.uuid4())
    payload = []
    for c in contributions:
        if not isinstance(c, Mapping):
            raise TypeError(
                "write_link: each contribution must be a Mapping; got "
                f"{type(c).__name__}"
            )
        payload.append({
            "upstream_run_id": str(c.get("upstream_run_id"))
                                if c.get("upstream_run_id") else "",
            "source_file_id":  str(c.get("source_file_id"))
                                if c.get("source_file_id") else "",
            "source_ref":      c.get("source_ref"),
            "slot_name":       c.get("slot_name"),
            "record_count":    str(c.get("record_count"))
                                if c.get("record_count") is not None else "",
            "edge_type":       c.get("edge_type"),
        })

    with conn.cursor() as cur:
        cur.execute(
            "SELECT pipeline.control_write_lineage_link("
            "  %s::uuid, %s::uuid, %s, %s, %s, %s::jsonb"
            ")",
            (link_id, consumer_run_id, edge_type, target_ref,
             record_count, json.dumps(payload)),
        )
    return link_id
