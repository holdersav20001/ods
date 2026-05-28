"""Lineage dashboard API.

Read-only FastAPI service over pipeline.* control tables. Powers the React
Flow visualisation in ../web. No writes — purely a query layer.

Endpoints
---------
GET /api/health                          → liveness
GET /api/lineage/links                   → recent lineage_link bundles
GET /api/lineage/trace/{lineage_link_id} → full target→raw chain as a graph

Run locally::

    uvicorn ops.lineage_dashboard.api.main:app --reload --port 8765
"""
from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Any

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="ODS Lineage Dashboard API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


def _dsn() -> dict:
    return dict(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", "5440")),
        dbname=os.environ.get("POSTGRES_DB", "ods_dev"),
        user=os.environ.get("POSTGRES_USER", "ods"),
        password=os.environ.get("POSTGRES_PASSWORD", "ods"),
    )


@contextmanager
def _conn():
    c = psycopg2.connect(**_dsn())
    try:
        yield c
    finally:
        c.close()


@app.get("/api/health")
def health() -> dict[str, str]:
    with _conn() as c, c.cursor() as cur:
        cur.execute("SELECT 1")
        cur.fetchone()
    return {"status": "ok"}


@app.get("/api/lineage/links")
def list_links(limit: int = Query(50, ge=1, le=500)) -> list[dict[str, Any]]:
    """Return recent lineage_link bundles with consumer run metadata."""
    sql = """
        SELECT ll.lineage_link_id::text,
               ll.consumer_run_id::text,
               ll.edge_type,
               ll.target_ref,
               ll.record_count,
               ll.created_at,
               r.pipeline_type,
               r.domain,
               r.dataset,
               r.business_date,
               r.status
          FROM pipeline.lineage_link ll
     LEFT JOIN pipeline.run_log r ON r.run_id = ll.consumer_run_id
      ORDER BY ll.created_at DESC
         LIMIT %s
    """
    with _conn() as c, c.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, (limit,))
        rows = cur.fetchall()
    return [dict(r) for r in rows]


@app.get("/api/lineage/trace/{lineage_link_id}")
def trace(lineage_link_id: str) -> dict[str, Any]:
    """Return a graph (nodes + edges) for one lineage_link.

    Graph shape (consumed by React Flow):
      file_catalogue rows           → node kind='raw_file'
      ingestion / stage run_log     → node kind='upstream_run'
      lineage_link bundle           → node kind='write_event'
      consumer run_log              → node kind='consumer_run'
      target table                  → node kind='target'

    Edges:
      raw_file        → upstream_run     (kind='produced')
      upstream_run    → write_event      (kind='contributed', carries slot_name)
      write_event     → consumer_run     (kind='emitted_by')
      write_event     → target           (kind='wrote_to')
    """
    with _conn() as c, c.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT lineage_link_id::text, consumer_run_id::text,
                   edge_type, target_ref, record_count, created_at
              FROM pipeline.lineage_link
             WHERE lineage_link_id = %s::uuid
            """,
            (lineage_link_id,),
        )
        link = cur.fetchone()
        if link is None:
            raise HTTPException(404, f"lineage_link {lineage_link_id} not found")

        cur.execute(
            """
            SELECT run_id::text, pipeline_type, domain, dataset,
                   business_date, status, started_at, ended_at,
                   record_count_source, record_count_target,
                   file_id::text
              FROM pipeline.run_log
             WHERE run_id = %s::uuid
            """,
            (link["consumer_run_id"],),
        )
        consumer_run = cur.fetchone()

        cur.execute(
            """
            SELECT le.upstream_run_id::text,
                   le.source_file_id::text,
                   le.source_ref,
                   le.slot_name,
                   le.edge_type,
                   le.record_count,
                   ur.pipeline_type AS upstream_pipeline_type,
                   ur.domain        AS upstream_domain,
                   ur.dataset       AS upstream_dataset,
                   ur.business_date AS upstream_business_date,
                   ur.status        AS upstream_status,
                   fc.s3_raw_path,
                   fc.s3_curated_path,
                   fc.file_size_bytes,
                   fc.source_row_count
              FROM pipeline.lineage_edge le
         LEFT JOIN pipeline.run_log r2  ON r2.run_id  = le.consumer_run_id
         LEFT JOIN pipeline.run_log ur  ON ur.run_id  = le.upstream_run_id
         LEFT JOIN pipeline.file_catalogue fc ON fc.file_id = le.source_file_id
             WHERE le.lineage_link_id = %s::uuid
             ORDER BY le.slot_name NULLS LAST
            """,
            (lineage_link_id,),
        )
        contributions = [dict(r) for r in cur.fetchall()]

    nodes: list[dict[str, Any]] = []
    edges: list[dict[str, Any]] = []

    def _add_node(node_id: str, kind: str, label: str, **data):
        nodes.append({"id": node_id, "kind": kind, "label": label, "data": data})

    def _add_edge(src: str, tgt: str, kind: str, **data):
        edges.append({"id": f"{src}->{tgt}:{kind}",
                      "source": src, "target": tgt, "kind": kind, "data": data})

    write_event_id = f"link:{link['lineage_link_id']}"
    _add_node(write_event_id, "write_event",
              label=f"write_event\n{link['edge_type']}",
              record_count=link["record_count"],
              created_at=str(link["created_at"]))

    if consumer_run:
        cr_id = f"run:{consumer_run['run_id']}"
        _add_node(cr_id, "consumer_run",
                  label=f"{consumer_run['pipeline_type']}\n"
                        f"{consumer_run['domain']}/{consumer_run['dataset']}",
                  **{k: (str(v) if v is not None else None)
                     for k, v in dict(consumer_run).items()})
        _add_edge(write_event_id, cr_id, "emitted_by")

    target_id = f"target:{link['target_ref']}"
    _add_node(target_id, "target",
              label=link["target_ref"] or "(no target)",
              record_count=link["record_count"])
    _add_edge(write_event_id, target_id, "wrote_to",
              record_count=link["record_count"])

    for contrib in contributions:
        slot = contrib["slot_name"]
        if contrib["upstream_run_id"]:
            ur_id = f"run:{contrib['upstream_run_id']}"
            _add_node(ur_id, "upstream_run",
                      label=f"{contrib['upstream_pipeline_type'] or 'run'}\n"
                            f"{contrib['upstream_domain'] or ''}/"
                            f"{contrib['upstream_dataset'] or ''}",
                      pipeline_type=contrib["upstream_pipeline_type"],
                      status=contrib["upstream_status"],
                      business_date=str(contrib["upstream_business_date"])
                                     if contrib["upstream_business_date"] else None)
            _add_edge(ur_id, write_event_id, "contributed",
                      slot_name=slot, record_count=contrib["record_count"])

        rf_id = None
        if contrib["source_file_id"]:
            rf_id = f"file:{contrib['source_file_id']}"
            _add_node(rf_id, "raw_file",
                      label=contrib["s3_raw_path"] or contrib["source_ref"] or "raw",
                      s3_raw_path=contrib["s3_raw_path"],
                      s3_curated_path=contrib["s3_curated_path"],
                      file_size_bytes=contrib["file_size_bytes"],
                      source_row_count=contrib["source_row_count"])
        elif contrib["source_ref"]:
            # Raw source with no registered file_id — still surface the
            # source_ref (often the S3 path) as a raw_file node so the
            # chain back to the data origin is never broken.
            rf_id = f"src:{contrib['source_ref']}"
            _add_node(rf_id, "raw_file",
                      label=contrib["source_ref"],
                      s3_raw_path=contrib["source_ref"])

        if rf_id:
            tgt_for_file = (f"run:{contrib['upstream_run_id']}"
                            if contrib["upstream_run_id"] else write_event_id)
            _add_edge(rf_id, tgt_for_file, "produced",
                      slot_name=slot, source_ref=contrib["source_ref"])

    return {
        "lineage_link": dict(link) | {"created_at": str(link["created_at"])},
        "nodes": nodes,
        "edges": edges,
    }
