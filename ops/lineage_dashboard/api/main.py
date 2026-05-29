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


@app.get("/api/run/{run_id}")
def get_run(run_id: str) -> dict[str, Any]:
    """Full run_log row + run_stage_log timeline + lineage handles."""
    with _conn() as c, c.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            -- Properties of the SOURCE FILE (s3_raw_path / s3_curated_path)
            -- live on the file_catalogue node, not on the run. Joining them
            -- here just duplicated the same paths onto every run that
            -- touched the file and made the side panel noisy.
            SELECT r.run_id::text, r.pipeline_type, r.domain, r.dataset,
                   r.business_date, r.status, r.started_at, r.ended_at,
                   r.record_count_source, r.record_count_target,
                   r.record_count_dq_pass, r.record_count_dq_fail,
                   r.error_summary, r.config_version_id, r.schema_version_id,
                   r.orchestrators, r.runtime_context,
                   r.file_id::text,
                   r.kafka_topic,
                   dc.schema_id            AS dataset_schema_id,
                   dc.schema_version       AS dataset_schema_version,
                   dc.transform_yaml_path  AS dataset_transform_yaml_path,
                   dc.is_canonical         AS dataset_is_canonical
              FROM pipeline.run_log r
         LEFT JOIN pipeline.dataset_config dc
                ON dc.domain  = r.domain
               AND dc.dataset = r.dataset
               AND dc.active  = TRUE
             WHERE r.run_id = %s::uuid
            """,
            (run_id,),
        )
        run = cur.fetchone()
        if run is None:
            raise HTTPException(404, f"run {run_id} not found")

        cur.execute(
            """
            SELECT stage, status, event_type, attempt_number,
                   started_at, ended_at,
                   input_ref, output_ref,
                   record_count_in, record_count_out,
                   error
              FROM pipeline.run_stage_log
             WHERE run_id = %s::uuid
             ORDER BY started_at, id
            """,
            (run_id,),
        )
        stages = [dict(r) for r in cur.fetchall()]

        # Links this run participated in — as consumer AND as upstream.
        cur.execute(
            """
            SELECT lineage_link_id::text AS lineage_link_id, 'consumer' AS role,
                   edge_type, target_ref, record_count, created_at
              FROM pipeline.lineage_link
             WHERE consumer_run_id = %s::uuid
            UNION ALL
            SELECT DISTINCT le.lineage_link_id::text, 'upstream' AS role,
                   le.edge_type, le.target_ref, le.record_count, NULL::timestamp
              FROM pipeline.lineage_edge le
             WHERE le.upstream_run_id = %s::uuid
               AND le.lineage_link_id IS NOT NULL
             ORDER BY created_at DESC NULLS LAST
            """,
            (run_id, run_id),
        )
        links = [dict(r) for r in cur.fetchall()]

    def _stringify(row):
        return {k: (str(v) if v is not None else None) for k, v in row.items()}

    return {
        "run": _stringify(dict(run)),
        "stages": [_stringify(s) for s in stages],
        "links": [_stringify(l) for l in links],
    }


_DATASETS_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "datasets")
)
_ALLOWED_YAML_KINDS = {
    "contract", "dataset", "delivery", "quality",
    "reconciliation", "source", "transform",
}


@app.get("/api/yaml/dataset/{domain}/{dataset}/{kind}")
def get_dataset_yaml(domain: str, dataset: str, kind: str) -> dict[str, str]:
    """Return the raw YAML for one config file under datasets/<d>/<ds>/.

    Path-sandboxed: only the whitelisted kinds and only paths that resolve
    inside ``datasets/`` are served.
    """
    if kind not in _ALLOWED_YAML_KINDS:
        raise HTTPException(400, f"kind {kind!r} not allowed")
    # Sanitise — refuse traversal.
    for part in (domain, dataset):
        if not part or "/" in part or "\\" in part or part.startswith("."):
            raise HTTPException(400, "invalid path component")
    target = os.path.abspath(
        os.path.join(_DATASETS_ROOT, domain, dataset, f"{kind}.yaml")
    )
    if not target.startswith(_DATASETS_ROOT + os.sep):
        raise HTTPException(400, "path escapes datasets/")
    if not os.path.exists(target):
        raise HTTPException(404, f"{domain}/{dataset}/{kind}.yaml not found")
    with open(target, "r", encoding="utf-8") as f:
        content = f.read()
    return {
        "path": os.path.relpath(target, os.path.dirname(_DATASETS_ROOT)),
        "kind": kind,
        "content": content,
    }


def _artefact_kind(uri: str | None) -> str:
    """Classify a source_ref / target_ref URI into a dashboard node kind.

    The classifier is intentionally simple and convention-based — it
    keeps the dashboard honest without the API needing extra schema. See
    docs/dev-guides/lineage-link-and-autonomous-tasks.md for the rules.
    """
    s = (uri or "").lower()
    if not s:
        return "raw_file"
    if s.startswith("jdbc:") or s.startswith("postgresql://") or s.startswith("postgres://"):
        return "target_db"
    if "/canonical/" in s or s.startswith("canonical://"):
        return "canonical_file"
    if "/curated/" in s or "ods-curated" in s:
        return "curated_file"
    if "/raw/"     in s or "ods-raw"     in s or s.endswith(".csv") or s.endswith(".jsonl"):
        return "raw_file"
    # Fall-through: a postgres staging table source_ref like
    # 'postgres://pipeline.slot_staging_core' or unrecognised URIs.
    if s.startswith("postgres://") or "slot_staging" in s:
        return "staging_table"
    return "raw_file"


@app.get("/api/file/{file_id}")
def get_file(file_id: str) -> dict[str, Any]:
    """file_catalogue row + every run_log entry that processed this file."""
    with _conn() as c, c.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT file_id::text, domain, dataset, business_date,
                   s3_raw_path, s3_curated_path, sftp_path,
                   file_md5, file_size_bytes, source_row_count,
                   state, last_run_id::text, created_at, updated_at
              FROM pipeline.file_catalogue
             WHERE file_id = %s::uuid
            """,
            (file_id,),
        )
        file_row = cur.fetchone()
        if file_row is None:
            raise HTTPException(404, f"file {file_id} not found")

        cur.execute(
            """
            SELECT run_id::text, pipeline_type, domain, dataset,
                   business_date, status, started_at, ended_at,
                   record_count_source, record_count_target
              FROM pipeline.run_log
             WHERE file_id = %s::uuid
             ORDER BY started_at
            """,
            (file_id,),
        )
        runs = [dict(r) for r in cur.fetchall()]

    def _stringify(row):
        return {k: (str(v) if v is not None else None) for k, v in row.items()}
    return {"file": _stringify(dict(file_row)),
            "runs": [_stringify(r) for r in runs]}


@app.get("/api/target")
def get_target(ref: str = Query(..., description="target_ref")) -> dict[str, Any]:
    """Recent lineage_link writes that landed at this target_ref."""
    with _conn() as c, c.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT ll.lineage_link_id::text,
                   ll.consumer_run_id::text,
                   ll.edge_type, ll.record_count, ll.created_at,
                   r.pipeline_type, r.domain, r.dataset,
                   r.business_date, r.status
              FROM pipeline.lineage_link ll
         LEFT JOIN pipeline.run_log r ON r.run_id = ll.consumer_run_id
             WHERE ll.target_ref = %s
             ORDER BY ll.created_at DESC
             LIMIT 50
            """,
            (ref,),
        )
        links = [dict(r) for r in cur.fetchall()]
    def _stringify(row):
        return {k: (str(v) if v is not None else None) for k, v in row.items()}
    return {"target_ref": ref, "links": [_stringify(l) for l in links]}


@app.get("/api/lineage/link/{lineage_link_id}/edges")
def get_link_edges(lineage_link_id: str) -> dict[str, Any]:
    """All lineage_edge rows in this bundle (with run + file metadata)."""
    with _conn() as c, c.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT le.input_slot, le.edge_type, le.source_ref, le.target_ref,
                   le.record_count,
                   le.upstream_run_id::text,
                   le.source_file_id::text,
                   ur.pipeline_type AS upstream_pipeline_type,
                   ur.status        AS upstream_status,
                   fc.s3_raw_path
              FROM pipeline.lineage_edge le
         LEFT JOIN pipeline.run_log ur       ON ur.run_id = le.upstream_run_id
         LEFT JOIN pipeline.file_catalogue fc ON fc.file_id = le.source_file_id
             WHERE le.lineage_link_id = %s::uuid
             ORDER BY le.input_slot NULLS LAST
            """,
            (lineage_link_id,),
        )
        edges = [dict(r) for r in cur.fetchall()]
    def _stringify(row):
        return {k: (str(v) if v is not None else None) for k, v in row.items()}
    return {"lineage_link_id": lineage_link_id,
            "edges": [_stringify(e) for e in edges]}


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
      upstream_run    → write_event      (kind='contributed', carries input_slot)
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
                   le.input_slot,
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
             ORDER BY le.input_slot NULLS LAST
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
        slot = contrib["input_slot"]
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
                      input_slot=slot, record_count=contrib["record_count"])

        rf_id = None
        if contrib["source_file_id"]:
            # An entry in file_catalogue — always represents the RAW file
            # the data originated from (file_catalogue tracks raw arrivals).
            rf_id = f"file:{contrib['source_file_id']}"
            _add_node(rf_id, "raw_file",
                      label=contrib["s3_raw_path"] or contrib["source_ref"] or "raw",
                      s3_raw_path=contrib["s3_raw_path"],
                      s3_curated_path=contrib["s3_curated_path"],
                      file_size_bytes=contrib["file_size_bytes"],
                      source_row_count=contrib["source_row_count"])
        elif contrib["source_ref"]:
            # No file_catalogue row — classify the source_ref by URI shape
            # so the dashboard reflects the actual layer (raw vs curated
            # parquet vs canonical parquet vs JDBC source).
            kind = _artefact_kind(contrib["source_ref"])
            rf_id = f"src:{contrib['source_ref']}"
            _add_node(rf_id, kind,
                      label=contrib["source_ref"],
                      uri=contrib["source_ref"])

        if rf_id:
            tgt_for_file = (f"run:{contrib['upstream_run_id']}"
                            if contrib["upstream_run_id"] else write_event_id)
            _add_edge(rf_id, tgt_for_file, "produced",
                      input_slot=slot, source_ref=contrib["source_ref"])

    return {
        "lineage_link": dict(link) | {"created_at": str(link["created_at"])},
        "nodes": nodes,
        "edges": edges,
    }
