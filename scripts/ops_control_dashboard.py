#!/usr/bin/env python3
"""
ODS Operations Control Dashboard.

This FastAPI app is intentionally separate from the existing dashboards. It uses
the durable process tables as its primary source of truth:

    run_log, run_stage_log, file_catalogue, reconciliation_log, lineage_edge

The run_events table is shown only as diagnostics because event publication is
best-effort and should not be treated as authoritative run state.

Usage:
    pip install fastapi uvicorn psycopg2-binary
    uvicorn scripts.ops_control_dashboard:app --port 8910 --reload
    Open http://localhost:8910
"""
from __future__ import annotations

import json
import os
import uuid
from contextlib import closing
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse

app = FastAPI(title="ODS Operations Control Dashboard")

PG_HOST = os.environ.get("POSTGRES_HOST", "localhost")
PG_PORT = int(os.environ.get("POSTGRES_PORT", "5440"))
PG_DB = os.environ.get("POSTGRES_DB", "ods_dev")
PG_USER = os.environ.get("POSTGRES_USER", "ods")
PG_PASS = os.environ.get("POSTGRES_PASSWORD", "ods")
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SR_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")
RUN_EVENTS_TOPIC = os.environ.get("RUN_EVENTS_TOPIC", "ods.pipeline.run-events")


def _conn():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASS,
        cursor_factory=psycopg2.extras.RealDictCursor,
    )


def _q(conn, sql: str, params: tuple | list = ()) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return [dict(row) for row in cur.fetchall()]


def _one(conn, sql: str, params: tuple | list = ()) -> dict:
    rows = _q(conn, sql, params)
    return rows[0] if rows else {}


def _serialize(obj: Any):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, timedelta):
        return str(obj)
    if isinstance(obj, uuid.UUID):
        return str(obj)
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def _json(data: Any) -> JSONResponse:
    return JSONResponse(content=json.loads(json.dumps(data, default=_serialize)))


def _table_exists(conn, table_name: str) -> bool:
    row = _one(
        conn,
        """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'pipeline'
              AND table_name = %s
        ) AS exists
        """,
        (table_name,),
    )
    return bool(row.get("exists"))


def _columns(conn, table_name: str) -> set[str]:
    return {
        row["column_name"]
        for row in _q(
            conn,
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'pipeline'
              AND table_name = %s
            """,
            (table_name,),
        )
    }


def _stage_event_expr(stage_cols: set[str]) -> str:
    if "event_type" in stage_cols:
        return "rsl.event_type"
    return """
        CASE
            WHEN rsl.status = 'running' THEN 'stage_started'
            WHEN rsl.status = 'failed' THEN 'stage_failed'
            WHEN rsl.status = 'skipped' THEN 'stage_skipped'
            WHEN rsl.status IN ('dq_warned', 'warned', 'partial') THEN 'stage_warned'
            ELSE 'stage_completed'
        END
    """


def _optional_stage_select(stage_cols: set[str]) -> str:
    pieces = [
        "rsl.id",
        "rsl.run_id",
        "rsl.stage",
        "rsl.status",
        f"{_stage_event_expr(stage_cols)} AS event_type",
        "rsl.input_ref",
        "rsl.output_ref",
        "rsl.record_count_in",
        "rsl.record_count_out",
        "rsl.started_at",
        "rsl.ended_at",
        "rsl.metrics",
        "rsl.error",
    ]
    for col in ("attempt_number", "airflow_dag_id", "airflow_run_id", "spark_app_id"):
        if col in stage_cols:
            pieces.append(f"rsl.{col}")
        else:
            pieces.append(f"NULL::text AS {col}")
    return ", ".join(pieces)


@app.get("/api/overview")
async def overview():
    try:
        with closing(_conn()) as conn:
            data = {
                "run_status_today": _q(
                    conn,
                    """
                    SELECT status, COUNT(*) AS count
                    FROM pipeline.run_log
                    WHERE started_at >= CURRENT_DATE
                    GROUP BY status
                    ORDER BY status
                    """,
                ),
                "file_state": _q(
                    conn,
                    """
                    SELECT state, COUNT(*) AS count
                    FROM pipeline.file_catalogue
                    GROUP BY state
                    ORDER BY state
                    """,
                ),
                "recent_failures": _q(
                    conn,
                    """
                    SELECT run_id, pipeline_type, domain, dataset, business_date,
                           status, started_at, ended_at, error_summary
                    FROM pipeline.run_log
                    WHERE status IN ('failed', 'partial')
                    ORDER BY COALESCE(ended_at, started_at) DESC
                    LIMIT 20
                    """,
                ),
                "stuck_runs": _q(
                    conn,
                    """
                    SELECT run_id, pipeline_type, domain, dataset, business_date,
                           started_at, now() - started_at AS age, error_summary
                    FROM pipeline.run_log
                    WHERE status = 'running'
                      AND started_at < now() - interval '30 minutes'
                    ORDER BY started_at
                    LIMIT 20
                    """,
                ),
                "recon_failures": _q(
                    conn,
                    """
                    SELECT created_at, check_type, run_id, domain, dataset,
                           business_date, status, source_count, kafka_count,
                           postgres_count, discrepancy_count, discrepancy_pct, detail
                    FROM pipeline.reconciliation_log
                    WHERE status NOT IN ('ok', 'succeeded', 'passed')
                    ORDER BY created_at DESC
                    LIMIT 20
                    """,
                ),
                "latest_runs": _q(
                    conn,
                    """
                    SELECT run_id, pipeline_type, domain, dataset, business_date,
                           file_id, status, started_at, ended_at,
                           record_count_source, record_count_dq_pass,
                           record_count_dq_fail, record_count_published,
                           kafka_topic, kafka_offset_start, kafka_offset_end,
                           error_summary
                    FROM pipeline.run_log
                    ORDER BY started_at DESC
                    LIMIT 50
                    """,
                ),
            }
            if _table_exists(conn, "run_events"):
                data["event_diagnostics"] = _q(
                    conn,
                    """
                    SELECT status, COUNT(*) AS count
                    FROM pipeline.run_events
                    WHERE occurred_at >= CURRENT_DATE
                    GROUP BY status
                    ORDER BY status
                    """,
                )
            else:
                data["event_diagnostics"] = []
            return _json(data)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc))


@app.get("/api/runs")
async def runs(
    domain: str = "",
    dataset: str = "",
    status: str = "",
    pipeline_type: str = "",
    business_date: str = "",
    file_id: str = "",
    run_id: str = "",
    limit: int = Query(default=100, ge=1, le=1000),
):
    try:
        filters: list[str] = []
        params: list[Any] = []
        if domain:
            filters.append("domain = %s")
            params.append(domain)
        if dataset:
            filters.append("dataset = %s")
            params.append(dataset)
        if status:
            filters.append("status = %s")
            params.append(status)
        if pipeline_type:
            filters.append("pipeline_type = %s")
            params.append(pipeline_type)
        if business_date:
            filters.append("business_date = %s")
            params.append(business_date)
        if file_id:
            filters.append("file_id::text ILIKE %s")
            params.append(f"%{file_id}%")
        if run_id:
            filters.append("run_id::text ILIKE %s")
            params.append(f"%{run_id}%")
        where = "WHERE " + " AND ".join(filters) if filters else ""
        params.append(limit)
        with closing(_conn()) as conn:
            rows = _q(
                conn,
                f"""
                SELECT run_id, pipeline_type, domain, dataset, business_date,
                       file_id, status, started_at, ended_at,
                       record_count_source, record_count_dq_pass,
                       record_count_dq_fail, record_count_published,
                       kafka_topic, kafka_offset_start, kafka_offset_end,
                       error_summary
                FROM pipeline.run_log
                {where}
                ORDER BY started_at DESC
                LIMIT %s
                """,
                params,
            )
            return _json(rows)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc))


@app.get("/api/run/{run_id}")
async def run_detail(run_id: str):
    try:
        run_uuid = str(uuid.UUID(run_id))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="run_id must be a UUID") from exc

    try:
        with closing(_conn()) as conn:
            stage_cols = _columns(conn, "run_stage_log")
            run = _one(
                conn,
                """
                SELECT *
                FROM pipeline.run_log
                WHERE run_id = %s
                """,
                (run_uuid,),
            )
            if not run:
                raise HTTPException(status_code=404, detail="run not found")

            stages = _q(
                conn,
                f"""
                SELECT {_optional_stage_select(stage_cols)}
                FROM pipeline.run_stage_log rsl
                WHERE rsl.run_id = %s
                ORDER BY rsl.started_at, rsl.id
                """,
                (run_uuid,),
            )
            recon = _q(
                conn,
                """
                SELECT *
                FROM pipeline.reconciliation_log
                WHERE run_id = %s
                ORDER BY created_at
                """,
                (run_uuid,),
            )
            edges = []
            if _table_exists(conn, "lineage_edge"):
                edges = _q(
                    conn,
                    """
                    SELECT *
                    FROM pipeline.lineage_edge
                    WHERE child_run_id = %s OR parent_run_id = %s
                    ORDER BY created_at
                    """,
                    (run_uuid, run_uuid),
                )
            return _json({"run": run, "stages": stages, "reconciliation": recon, "edges": edges})
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc))


@app.get("/api/file/{file_id}/lineage")
async def file_lineage(file_id: str):
    try:
        file_uuid = str(uuid.UUID(file_id))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="file_id must be a UUID") from exc

    try:
        with closing(_conn()) as conn:
            stage_cols = _columns(conn, "run_stage_log")
            file_row = _one(
                conn,
                "SELECT * FROM pipeline.file_catalogue WHERE file_id = %s",
                (file_uuid,),
            )
            if not file_row:
                raise HTTPException(status_code=404, detail="file not found")

            direct_runs = _q(
                conn,
                """
                SELECT *
                FROM pipeline.run_log
                WHERE file_id = %s
                ORDER BY started_at
                """,
                (file_uuid,),
            )
            edge_runs: list[dict] = []
            edges: list[dict] = []
            if _table_exists(conn, "lineage_edge"):
                edges = _q(
                    conn,
                    """
                    SELECT le.*, child.pipeline_type AS child_pipeline_type,
                           child.domain AS child_domain, child.dataset AS child_dataset,
                           child.business_date AS child_business_date,
                           parent.pipeline_type AS parent_pipeline_type
                    FROM pipeline.lineage_edge le
                    JOIN pipeline.run_log child ON child.run_id = le.child_run_id
                    LEFT JOIN pipeline.run_log parent ON parent.run_id = le.parent_run_id
                    WHERE le.parent_file_id = %s
                       OR child.file_id = %s
                    ORDER BY le.created_at
                    """,
                    (file_uuid, file_uuid),
                )
                edge_run_ids = sorted({str(e["child_run_id"]) for e in edges if e.get("child_run_id")})
                if edge_run_ids:
                    placeholders = ",".join(["%s"] * len(edge_run_ids))
                    edge_runs = _q(
                        conn,
                        f"""
                        SELECT *
                        FROM pipeline.run_log
                        WHERE run_id IN ({placeholders})
                        ORDER BY started_at
                        """,
                        edge_run_ids,
                    )

            runs_by_id: dict[str, dict] = {}
            for row in direct_runs + edge_runs:
                runs_by_id[str(row["run_id"])] = row
            involved_runs = sorted(
                runs_by_id.values(),
                key=lambda r: (r.get("started_at") is None, r.get("started_at") or datetime.min),
            )

            all_run_ids = sorted(runs_by_id)
            stages: list[dict] = []
            if all_run_ids:
                placeholders = ",".join(["%s"] * len(all_run_ids))
                stages = _q(
                    conn,
                    f"""
                    SELECT {_optional_stage_select(stage_cols)}
                    FROM pipeline.run_stage_log rsl
                    WHERE rsl.run_id IN ({placeholders})
                    ORDER BY rsl.started_at, rsl.id
                    """,
                    all_run_ids,
                )

            return _json(
                {
                    "file": file_row,
                    "direct_runs": direct_runs,
                    "lineage_runs": edge_runs,
                    "involved_runs": involved_runs,
                    "stages": stages,
                    "edges": edges,
                }
            )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc))


@app.get("/api/api-pull")
async def api_pull_dashboard(
    domain: str = "",
    dataset: str = "",
    source_application: str = "",
    status: str = "",
    business_date: str = "",
    run_id: str = "",
    file_id: str = "",
    limit: int = Query(default=50, ge=1, le=500),
):
    """Authoritative API pull operating view.

    Uses durable pipeline tables. ``run_events`` is deliberately not used
    here because it is a best-effort diagnostic stream.
    """
    try:
        with closing(_conn()) as conn:
            if not _table_exists(conn, "api_pull_watermark"):
                return _json(
                    {
                        "available": False,
                        "reason": "pipeline.api_pull_watermark is not available",
                        "summary": {},
                        "datasets": [],
                        "watermarks": [],
                        "latest_runs": [],
                        "archives": [],
                        "archive_reconciliation": [],
                        "failed_pulls": [],
                        "replay_candidates": [],
                    }
                )

            cfg_params: list[Any] = []
            cfg_filters = ["dc.source_type = 'api_pull'"]
            if domain:
                cfg_filters.append("dc.domain = %s")
                cfg_params.append(domain)
            if dataset:
                cfg_filters.append("dc.dataset = %s")
                cfg_params.append(dataset)
            if source_application:
                cfg_filters.append("COALESCE(dc.source_config->>'application', dc.domain || '.' || dc.dataset) = %s")
                cfg_params.append(source_application)
            cfg_where = "WHERE " + " AND ".join(cfg_filters)

            datasets = _q(
                conn,
                f"""
                SELECT dc.domain, dc.dataset, dc.active, dc.raw_format,
                       dc.target_topic, dc.canonical_topic, dc.postgres_target_table,
                       dc.config_version_id,
                       COALESCE(dc.source_config->>'application', dc.domain || '.' || dc.dataset)
                           AS source_application,
                       dc.source_config->'cursor'->>'style' AS cursor_style,
                       dc.source_config->'auth'->>'type' AS auth_type,
                       dc.source_config->'auth'->>'secret_ref' AS secret_ref,
                       dc.source_config->>'url' AS source_url
                FROM pipeline.dataset_config dc
                {cfg_where}
                ORDER BY dc.domain, dc.dataset
                """,
                cfg_params,
            )

            wm_params: list[Any] = []
            wm_filters: list[str] = []
            if domain:
                wm_filters.append("w.domain = %s")
                wm_params.append(domain)
            if dataset:
                wm_filters.append("w.dataset = %s")
                wm_params.append(dataset)
            if source_application:
                wm_filters.append("w.source_application = %s")
                wm_params.append(source_application)
            wm_where = "WHERE " + " AND ".join(wm_filters) if wm_filters else ""
            watermarks = _q(
                conn,
                f"""
                SELECT w.domain, w.dataset, w.source_application, w.cursor_type,
                       w.committed_cursor_value, w.pending_cursor_value,
                       w.pending_run_id, w.last_successful_run_id, w.locked_at,
                       w.updated_at, now() - w.updated_at AS pull_lag
                FROM pipeline.api_pull_watermark w
                {wm_where}
                ORDER BY w.updated_at DESC
                LIMIT %s
                """,
                wm_params + [limit],
            )

            run_params: list[Any] = []
            run_filters = ["r.pipeline_type = 'api_pull'"]
            if domain:
                run_filters.append("r.domain = %s")
                run_params.append(domain)
            if dataset:
                run_filters.append("r.dataset = %s")
                run_params.append(dataset)
            if status:
                run_filters.append("r.status = %s")
                run_params.append(status)
            if business_date:
                run_filters.append("r.business_date = %s")
                run_params.append(business_date)
            if run_id:
                run_filters.append("r.run_id::text ILIKE %s")
                run_params.append(f"%{run_id}%")
            run_where = "WHERE " + " AND ".join(run_filters)
            latest_runs = _q(
                conn,
                f"""
                SELECT r.run_id, r.pipeline_type, r.domain, r.dataset,
                       r.business_date, r.status, r.started_at, r.ended_at,
                       r.record_count_source, r.record_count_published,
                       r.error_summary,
                       w.source_application, w.committed_cursor_value,
                       w.pending_cursor_value, w.locked_at
                FROM pipeline.run_log r
                LEFT JOIN pipeline.api_pull_watermark w
                  ON w.domain = r.domain AND w.dataset = r.dataset
                {run_where}
                ORDER BY r.started_at DESC
                LIMIT %s
                """,
                run_params + [limit],
            )

            archive_params: list[Any] = []
            archive_filters = ["r.pipeline_type = 'api_pull'"]
            if domain:
                archive_filters.append("r.domain = %s")
                archive_params.append(domain)
            if dataset:
                archive_filters.append("r.dataset = %s")
                archive_params.append(dataset)
            if business_date:
                archive_filters.append("r.business_date = %s")
                archive_params.append(business_date)
            if run_id:
                archive_filters.append("r.run_id::text ILIKE %s")
                archive_params.append(f"%{run_id}%")
            if file_id:
                archive_filters.append("fc.file_id::text ILIKE %s")
                archive_params.append(f"%{file_id}%")
            archive_where = "WHERE " + " AND ".join(archive_filters)
            archives = _q(
                conn,
                f"""
                SELECT r.run_id, r.domain, r.dataset, r.business_date,
                       r.status AS run_status, r.started_at,
                       rsl.output_ref AS archive_uri,
                       rsl.record_count_out AS archived_count,
                       fc.file_id, fc.state AS file_state, fc.source_row_count,
                       fc.file_md5, fc.file_size_bytes, fc.state_updated_at,
                       le.edge_type
                FROM pipeline.run_log r
                LEFT JOIN pipeline.run_stage_log rsl
                  ON rsl.run_id = r.run_id AND rsl.stage = 'message_archive'
                LEFT JOIN pipeline.lineage_edge le
                  ON le.child_run_id = r.run_id AND le.edge_type = 'api_to_archive'
                LEFT JOIN pipeline.file_catalogue fc
                  ON fc.file_id = le.parent_file_id
                {archive_where}
                ORDER BY r.started_at DESC
                LIMIT %s
                """,
                archive_params + [limit],
            )

            recon_params: list[Any] = []
            recon_filters = ["check_type = 'api_pull_archive_count'"]
            if domain:
                recon_filters.append("domain = %s")
                recon_params.append(domain)
            if dataset:
                recon_filters.append("dataset = %s")
                recon_params.append(dataset)
            if status:
                recon_filters.append("status = %s")
                recon_params.append(status)
            if business_date:
                recon_filters.append("business_date = %s")
                recon_params.append(business_date)
            if run_id:
                recon_filters.append("run_id::text ILIKE %s")
                recon_params.append(f"%{run_id}%")
            recon_where = "WHERE " + " AND ".join(recon_filters)
            archive_reconciliation = _q(
                conn,
                f"""
                SELECT created_at, check_type, run_id, domain, dataset,
                       business_date, status, source_count, kafka_count,
                       postgres_count, discrepancy_count, discrepancy_pct, detail
                FROM pipeline.reconciliation_log
                {recon_where}
                ORDER BY created_at DESC
                LIMIT %s
                """,
                recon_params + [limit],
            )

            failed_pulls = _q(
                conn,
                f"""
                SELECT r.run_id, r.domain, r.dataset, r.business_date,
                       r.status, r.started_at, r.ended_at, r.error_summary,
                       w.source_application, w.pending_cursor_value,
                       w.committed_cursor_value
                FROM pipeline.run_log r
                LEFT JOIN pipeline.api_pull_watermark w
                  ON w.domain = r.domain AND w.dataset = r.dataset
                {run_where}
                  AND r.status IN ('failed', 'partial')
                ORDER BY COALESCE(r.ended_at, r.started_at) DESC
                LIMIT %s
                """,
                run_params + [limit],
            )

            replay_params: list[Any] = []
            replay_filters = ["api.pipeline_type = 'api_pull'"]
            if domain:
                replay_filters.append("api.domain = %s")
                replay_params.append(domain)
            if dataset:
                replay_filters.append("api.dataset = %s")
                replay_params.append(dataset)
            if business_date:
                replay_filters.append("api.business_date = %s")
                replay_params.append(business_date)
            replay_where = "WHERE " + " AND ".join(replay_filters)
            replay_candidates = _q(
                conn,
                f"""
                SELECT api.run_id AS api_pull_run_id,
                       api.domain, api.dataset, api.business_date,
                       api.status AS api_status,
                       downstream.run_id AS downstream_run_id,
                       downstream.status AS downstream_status,
                       fc.file_id, fc.s3_raw_path, fc.state AS file_state,
                       w.source_application, w.pending_cursor_value,
                       w.committed_cursor_value,
                       COALESCE(downstream.error_summary, api.error_summary) AS error_summary
                FROM pipeline.run_log api
                LEFT JOIN pipeline.lineage_edge le
                  ON le.child_run_id = api.run_id AND le.edge_type = 'api_to_archive'
                LEFT JOIN pipeline.file_catalogue fc
                  ON fc.file_id = le.parent_file_id
                LEFT JOIN pipeline.run_log downstream
                  ON downstream.file_id = fc.file_id
                 AND downstream.pipeline_type = 's3_batch'
                 AND downstream.status IN ('failed', 'partial')
                LEFT JOIN pipeline.api_pull_watermark w
                  ON w.domain = api.domain AND w.dataset = api.dataset
                {replay_where}
                  AND (api.status IN ('failed', 'partial') OR downstream.run_id IS NOT NULL)
                ORDER BY COALESCE(downstream.ended_at, api.ended_at, api.started_at) DESC
                LIMIT %s
                """,
                replay_params + [limit],
            )

            summary_rows = _q(
                conn,
                """
                SELECT
                    COUNT(*) FILTER (WHERE pipeline_type='api_pull') AS total_runs,
                    COUNT(*) FILTER (WHERE pipeline_type='api_pull' AND status='succeeded') AS succeeded_runs,
                    COUNT(*) FILTER (WHERE pipeline_type='api_pull' AND status IN ('failed','partial')) AS failed_runs,
                    COUNT(*) FILTER (WHERE pipeline_type='api_pull' AND status='running') AS running_runs
                FROM pipeline.run_log
                WHERE started_at >= CURRENT_DATE
                """,
            )
            pending_count = _one(
                conn,
                """
                SELECT COUNT(*) AS pending_count,
                       COUNT(*) FILTER (WHERE locked_at IS NOT NULL) AS locked_count
                FROM pipeline.api_pull_watermark
                """,
            )
            summary = {**(summary_rows[0] if summary_rows else {}), **pending_count}

            return _json(
                {
                    "available": True,
                    "summary": summary,
                    "datasets": datasets,
                    "watermarks": watermarks,
                    "latest_runs": latest_runs,
                    "archives": archives,
                    "archive_reconciliation": archive_reconciliation,
                    "failed_pulls": failed_pulls,
                    "replay_candidates": replay_candidates,
                }
            )
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc))


def _direct_postgres_sink_lag_disabled() -> dict:
    return {"available": False, "reason": "kafka_unavailable"}


def _kafka_sink_lag(topic: str, dataset: str, end_offsets: dict) -> dict:
    """Reuse the AdminClient query inline if confluent_kafka is importable.

    Mirrors ``airflow.dags.dag_api_pull._direct_kafka_sink_status`` so the
    dashboard does not depend on Airflow imports.
    """
    if not topic or not end_offsets:
        return {"available": False, "reason": "no_published_offsets"}
    try:
        from confluent_kafka import Consumer, TopicPartition
    except Exception:
        return _direct_postgres_sink_lag_disabled()

    sink_name = f"jdbc-sink-{dataset}".replace("_", "-")
    group_id = f"connect-{sink_name}"
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": group_id,
        "enable.auto.commit": False,
        "session.timeout.ms": 6000,
    })
    try:
        tps = [TopicPartition(topic, int(p)) for p in end_offsets.keys()]
        committed = consumer.committed(tps, timeout=10)
    except Exception as exc:
        try:
            consumer.close()
        except Exception:
            pass
        return {"available": False, "reason": f"kafka_error: {exc}"}
    finally:
        try:
            consumer.close()
        except Exception:
            pass

    consumed = {int(tp.partition): int(tp.offset) for tp in committed if tp.offset >= 0}
    partitions = []
    caught_up = True
    total_lag = 0
    for partition, target in end_offsets.items():
        committed_offset = consumed.get(int(partition), -1)
        target_int = int(target)
        lag = max(0, target_int - committed_offset) if committed_offset >= 0 else None
        if committed_offset < target_int:
            caught_up = False
        if lag is not None:
            total_lag += lag
        partitions.append({
            "partition": int(partition),
            "committed_offset": committed_offset,
            "target_offset": target_int,
            "lag": lag,
        })
    return {
        "available": True,
        "consumer_group": group_id,
        "topic": topic,
        "caught_up": caught_up,
        "total_lag": total_lag,
        "partitions": partitions,
    }


@app.get("/api/direct-postgres")
async def direct_postgres_dashboard(
    domain: str = "",
    dataset: str = "",
    business_date: str = "",
    limit: int = Query(default=50, ge=1, le=500),
):
    """Operator view for ``dataset_config.delivery='direct_postgres'``.

    Reuses the same durable tables as the file_pipeline dashboard.
    """
    try:
        with closing(_conn()) as conn:
            cfg_filters = ["dc.delivery = 'direct_postgres'"]
            cfg_params: list[Any] = []
            if domain:
                cfg_filters.append("dc.domain = %s")
                cfg_params.append(domain)
            if dataset:
                cfg_filters.append("dc.dataset = %s")
                cfg_params.append(dataset)
            cfg_where = "WHERE " + " AND ".join(cfg_filters)
            datasets = _q(
                conn,
                f"""
                SELECT dc.domain, dc.dataset, dc.delivery, dc.active,
                       dc.target_topic, dc.postgres_target_table,
                       dc.source_type, dc.raw_format
                FROM pipeline.dataset_config dc
                {cfg_where}
                ORDER BY dc.domain, dc.dataset
                """,
                cfg_params,
            )

            run_filters = ["r.pipeline_type = 'direct_postgres'"]
            run_params: list[Any] = []
            if domain:
                run_filters.append("r.domain = %s")
                run_params.append(domain)
            if dataset:
                run_filters.append("r.dataset = %s")
                run_params.append(dataset)
            if business_date:
                run_filters.append("r.business_date = %s")
                run_params.append(business_date)
            run_where = "WHERE " + " AND ".join(run_filters)
            latest_runs = _q(
                conn,
                f"""
                SELECT r.run_id, r.pipeline_type, r.domain, r.dataset,
                       r.business_date, r.file_id, r.status, r.started_at,
                       r.ended_at, r.record_count_source,
                       r.record_count_published, r.error_summary
                FROM pipeline.run_log r
                {run_where}
                ORDER BY r.started_at DESC
                LIMIT %s
                """,
                run_params + [limit],
            )

            recon_filters = ["check_type = 'direct_postgres_count'"]
            recon_params: list[Any] = []
            if domain:
                recon_filters.append("domain = %s")
                recon_params.append(domain)
            if dataset:
                recon_filters.append("dataset = %s")
                recon_params.append(dataset)
            if business_date:
                recon_filters.append("business_date = %s")
                recon_params.append(business_date)
            recon_where = "WHERE " + " AND ".join(recon_filters)
            reconciliations = _q(
                conn,
                f"""
                SELECT created_at, check_type, run_id, domain, dataset,
                       business_date, status, source_count, postgres_count,
                       discrepancy_count, discrepancy_pct, detail
                FROM pipeline.reconciliation_log
                {recon_where}
                ORDER BY created_at DESC
                LIMIT %s
                """,
                recon_params + [limit],
            )

            curated_files = _q(
                conn,
                f"""
                SELECT DISTINCT ON (r.run_id)
                       r.run_id, r.domain, r.dataset, r.business_date,
                       r.status, r.started_at,
                       fc.file_id, fc.s3_curated_path, fc.s3_staging_parquet_path,
                       fc.state AS file_state, fc.source_row_count,
                       fc.state_updated_at,
                       rsl.output_ref AS curated_parquet_path
                FROM pipeline.run_log r
                LEFT JOIN pipeline.file_catalogue fc ON fc.file_id = r.file_id
                LEFT JOIN pipeline.run_stage_log rsl
                  ON rsl.run_id = r.run_id AND rsl.stage = 'curated_write'
                {run_where}
                ORDER BY r.run_id, r.started_at DESC
                LIMIT %s
                """,
                run_params + [limit],
            )

            failed_recent = _q(
                conn,
                f"""
                SELECT r.run_id, r.domain, r.dataset, r.business_date,
                       r.status, r.started_at, r.ended_at, r.error_summary,
                       r.file_id
                FROM pipeline.run_log r
                {run_where}
                  AND r.status IN ('failed', 'partial')
                  AND COALESCE(r.ended_at, r.started_at) >= now() - interval '24 hours'
                ORDER BY COALESCE(r.ended_at, r.started_at) DESC
                LIMIT %s
                """,
                run_params + [limit],
            )

            summary_rows = _q(
                conn,
                """
                SELECT
                    COUNT(*) FILTER (WHERE pipeline_type='direct_postgres') AS total_runs,
                    COUNT(*) FILTER (WHERE pipeline_type='direct_postgres' AND status='succeeded') AS succeeded_runs,
                    COUNT(*) FILTER (WHERE pipeline_type='direct_postgres' AND status IN ('failed','partial')) AS failed_runs,
                    COUNT(*) FILTER (WHERE pipeline_type='direct_postgres' AND status='running') AS running_runs
                FROM pipeline.run_log
                WHERE started_at >= CURRENT_DATE
                """,
            )
            summary = summary_rows[0] if summary_rows else {}

            return _json({
                "available": True,
                "delivery": "direct_postgres",
                "summary": summary,
                "datasets": datasets,
                "latest_runs": latest_runs,
                "reconciliations": reconciliations,
                "curated_files": curated_files,
                "failed_recent": failed_recent,
            })
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc))


@app.get("/api/direct-kafka")
async def direct_kafka_dashboard(
    domain: str = "",
    dataset: str = "",
    business_date: str = "",
    include_sink_lag: bool = True,
    limit: int = Query(default=50, ge=1, le=500),
):
    """Operator view for ``dataset_config.delivery='direct_kafka'``.

    Joins ``run_log`` (pipeline_type='api_pull') against
    ``dataset_config.delivery='direct_kafka'`` and surfaces the JDBC sink
    consumer-group lag relative to the latest ``kafka_publish`` stage's
    ``offset_end_by_partition`` metric.
    """
    try:
        with closing(_conn()) as conn:
            cfg_filters = ["dc.delivery = 'direct_kafka'"]
            cfg_params: list[Any] = []
            if domain:
                cfg_filters.append("dc.domain = %s")
                cfg_params.append(domain)
            if dataset:
                cfg_filters.append("dc.dataset = %s")
                cfg_params.append(dataset)
            cfg_where = "WHERE " + " AND ".join(cfg_filters)
            datasets = _q(
                conn,
                f"""
                SELECT dc.domain, dc.dataset, dc.delivery, dc.active,
                       dc.target_topic, dc.source_type, dc.raw_format,
                       dc.source_config->>'application' AS source_application
                FROM pipeline.dataset_config dc
                {cfg_where}
                ORDER BY dc.domain, dc.dataset
                """,
                cfg_params,
            )

            run_filters = [
                "r.pipeline_type = 'api_pull'",
                "EXISTS (SELECT 1 FROM pipeline.dataset_config dc "
                " WHERE dc.domain = r.domain AND dc.dataset = r.dataset "
                "   AND dc.delivery = 'direct_kafka')",
            ]
            run_params: list[Any] = []
            if domain:
                run_filters.append("r.domain = %s")
                run_params.append(domain)
            if dataset:
                run_filters.append("r.dataset = %s")
                run_params.append(dataset)
            if business_date:
                run_filters.append("r.business_date = %s")
                run_params.append(business_date)
            run_where = "WHERE " + " AND ".join(run_filters)
            latest_runs = _q(
                conn,
                f"""
                SELECT r.run_id, r.domain, r.dataset, r.business_date,
                       r.status, r.started_at, r.ended_at,
                       r.record_count_source, r.record_count_published,
                       r.kafka_topic, r.kafka_offset_start, r.kafka_offset_end,
                       r.error_summary
                FROM pipeline.run_log r
                {run_where}
                ORDER BY r.started_at DESC
                LIMIT %s
                """,
                run_params + [limit],
            )

            wm_filters: list[str] = []
            wm_params: list[Any] = []
            wm_filters.append(
                "EXISTS (SELECT 1 FROM pipeline.dataset_config dc "
                " WHERE dc.domain = w.domain AND dc.dataset = w.dataset "
                "   AND dc.delivery = 'direct_kafka')"
            )
            if domain:
                wm_filters.append("w.domain = %s")
                wm_params.append(domain)
            if dataset:
                wm_filters.append("w.dataset = %s")
                wm_params.append(dataset)
            wm_where = "WHERE " + " AND ".join(wm_filters)
            watermarks = _q(
                conn,
                f"""
                SELECT w.domain, w.dataset, w.source_application, w.cursor_type,
                       w.committed_cursor_value, w.pending_cursor_value,
                       w.pending_run_id, w.last_successful_run_id, w.locked_at,
                       w.updated_at, now() - w.updated_at AS pull_lag
                FROM pipeline.api_pull_watermark w
                {wm_where}
                ORDER BY w.updated_at DESC
                LIMIT %s
                """,
                wm_params + [limit],
            )

            recon_filters = ["check_type = 'api_pull_publish_count'"]
            recon_params: list[Any] = []
            if domain:
                recon_filters.append("domain = %s")
                recon_params.append(domain)
            if dataset:
                recon_filters.append("dataset = %s")
                recon_params.append(dataset)
            if business_date:
                recon_filters.append("business_date = %s")
                recon_params.append(business_date)
            recon_where = "WHERE " + " AND ".join(recon_filters)
            reconciliations = _q(
                conn,
                f"""
                SELECT created_at, check_type, run_id, domain, dataset,
                       business_date, status, source_count, kafka_count,
                       postgres_count, discrepancy_count, discrepancy_pct, detail
                FROM pipeline.reconciliation_log
                {recon_where}
                ORDER BY created_at DESC
                LIMIT %s
                """,
                recon_params + [limit],
            )

            sink_lag: list[dict] = []
            if include_sink_lag:
                publish_rows = _q(
                    conn,
                    f"""
                    SELECT DISTINCT ON (r.domain, r.dataset)
                           r.domain, r.dataset, r.kafka_topic,
                           rsl.metrics
                    FROM pipeline.run_log r
                    JOIN pipeline.run_stage_log rsl
                      ON rsl.run_id = r.run_id AND rsl.stage = 'kafka_publish'
                    {run_where}
                    ORDER BY r.domain, r.dataset, r.started_at DESC
                    LIMIT %s
                    """,
                    run_params + [limit],
                )
                for row in publish_rows:
                    metrics = row.get("metrics") or {}
                    if isinstance(metrics, str):
                        try:
                            metrics = json.loads(metrics)
                        except Exception:
                            metrics = {}
                    end_offsets = (metrics or {}).get("offset_end_by_partition") or {}
                    topic = row.get("kafka_topic") or ""
                    lag = _kafka_sink_lag(topic, row["dataset"], end_offsets)
                    lag.update({
                        "domain": row["domain"],
                        "dataset": row["dataset"],
                    })
                    sink_lag.append(lag)

            summary_rows = _q(
                conn,
                """
                SELECT
                    COUNT(*) FILTER (WHERE r.pipeline_type='api_pull') AS total_runs,
                    COUNT(*) FILTER (WHERE r.pipeline_type='api_pull' AND r.status='succeeded') AS succeeded_runs,
                    COUNT(*) FILTER (WHERE r.pipeline_type='api_pull' AND r.status IN ('failed','partial')) AS failed_runs,
                    COUNT(*) FILTER (WHERE r.pipeline_type='api_pull' AND r.status='running') AS running_runs
                FROM pipeline.run_log r
                WHERE r.started_at >= CURRENT_DATE
                  AND EXISTS (
                      SELECT 1 FROM pipeline.dataset_config dc
                       WHERE dc.domain = r.domain AND dc.dataset = r.dataset
                         AND dc.delivery = 'direct_kafka'
                  )
                """,
            )
            summary = summary_rows[0] if summary_rows else {}

            return _json({
                "available": True,
                "delivery": "direct_kafka",
                "summary": summary,
                "datasets": datasets,
                "latest_runs": latest_runs,
                "watermarks": watermarks,
                "reconciliations": reconciliations,
                "sink_lag": sink_lag,
            })
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc))


@app.get("/api/event-diagnostics")
async def event_diagnostics(limit: int = Query(default=100, ge=1, le=1000)):
    try:
        with closing(_conn()) as conn:
            if not _table_exists(conn, "run_events"):
                return _json({"available": False, "events": [], "by_status": []})
            rows = _q(
                conn,
                """
                SELECT *
                FROM pipeline.run_events
                ORDER BY occurred_at DESC
                LIMIT %s
                """,
                (limit,),
            )
            by_status = _q(
                conn,
                """
                SELECT status, COUNT(*) AS count
                FROM pipeline.run_events
                WHERE occurred_at >= CURRENT_DATE
                GROUP BY status
                ORDER BY status
                """,
            )
            return _json({"available": True, "events": rows, "by_status": by_status})
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc))


@app.get("/api/topic-run-events")
async def topic_run_events(
    topic: str = RUN_EVENTS_TOPIC,
    limit: int = Query(default=100, ge=1, le=1000),
    domain: str = "",
    dataset: str = "",
    status: str = "",
    event_type: str = "",
    pipeline_type: str = "",
    run_id: str = "",
    file_id: str = "",
):
    """Tail recent Avro messages from the run-events Kafka topic."""
    try:
        from confluent_kafka import Consumer, TopicPartition
        from confluent_kafka.admin import AdminClient
        from confluent_kafka.schema_registry import SchemaRegistryClient
        from confluent_kafka.schema_registry.avro import AvroDeserializer
        from confluent_kafka.serialization import MessageField, SerializationContext
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Kafka dependencies unavailable: {exc}") from exc

    try:
        admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
        metadata = admin.list_topics(topic, timeout=5)
        if topic not in metadata.topics:
            return _json({"topic": topic, "partitions": [], "messages": [], "error": "topic not found"})

        partitions = sorted(metadata.topics[topic].partitions.keys())
        if not partitions:
            return _json({"topic": topic, "partitions": [], "messages": []})

        probe = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "_ods_ops_topic_probe"})
        bounds = {}
        window = min(max(limit * 5, 100), 5000)
        for partition in partitions:
            low, high = probe.get_watermark_offsets(TopicPartition(topic, partition), timeout=5)
            start = max(low, high - window)
            bounds[partition] = {"low": low, "high": high, "start": start}
        probe.close()

        sr = SchemaRegistryClient({"url": SR_URL})
        deserializer = AvroDeserializer(sr)
        consumer = Consumer(
            {
                "bootstrap.servers": BOOTSTRAP,
                "group.id": "_ods_ops_topic_reader",
                "enable.auto.commit": "false",
                "auto.offset.reset": "earliest",
            }
        )
        consumer.assign([TopicPartition(topic, p, bounds[p]["start"]) for p in partitions])

        reached = {p: bounds[p]["start"] >= bounds[p]["high"] for p in partitions}
        messages: list[dict] = []
        empty_polls = 0

        while not all(reached.values()) and empty_polls < 3:
            msg = consumer.poll(1.0)
            if msg is None:
                empty_polls += 1
                continue
            empty_polls = 0
            if msg.error():
                continue
            partition = msg.partition()
            if msg.offset() >= bounds[partition]["high"] - 1:
                reached[partition] = True
            try:
                value = deserializer(
                    msg.value(),
                    SerializationContext(topic, MessageField.VALUE),
                )
            except Exception:
                value = {"decode_error": True, "raw_size": len(msg.value() or b"")}
            if not value:
                continue
            row = dict(value)
            row["_topic"] = topic
            row["_partition"] = partition
            row["_offset"] = msg.offset()
            if domain and row.get("domain") != domain:
                continue
            if dataset and row.get("dataset") != dataset:
                continue
            if status and row.get("status") != status:
                continue
            if event_type and row.get("event_type") != event_type:
                continue
            if pipeline_type and row.get("pipeline_type") != pipeline_type:
                continue
            if run_id and run_id.lower() not in str(row.get("run_id", "")).lower():
                continue
            if file_id and file_id.lower() not in str(row.get("file_id", "")).lower():
                continue
            messages.append(row)

        consumer.close()
        messages.sort(key=lambda r: (r.get("occurred_at") or "", r.get("_partition", 0), r.get("_offset", 0)), reverse=True)

        partition_summary = [
            {
                "partition": partition,
                "low": bounds[partition]["low"],
                "high": bounds[partition]["high"],
                "messages": max(0, bounds[partition]["high"] - bounds[partition]["low"]),
            }
            for partition in partitions
        ]
        return _json(
            {
                "topic": topic,
                "bootstrap": BOOTSTRAP,
                "partitions": partition_summary,
                "messages": messages[:limit],
            }
        )
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc))


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    return HTML


HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>ODS Operations Control</title>
  <style>
    :root {
      --bg: #f7f8fa;
      --panel: #ffffff;
      --ink: #18212f;
      --muted: #64748b;
      --line: #dbe1ea;
      --blue: #225ea8;
      --green: #0f8a5f;
      --red: #b42318;
      --amber: #9a6700;
      --violet: #6d3fb0;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background: var(--bg);
      color: var(--ink);
      font-family: Arial, Helvetica, sans-serif;
      font-size: 14px;
    }
    header {
      background: #142033;
      color: white;
      padding: 18px 28px;
      display: flex;
      justify-content: space-between;
      gap: 24px;
      align-items: center;
      flex-wrap: wrap;
    }
    h1 { margin: 0; font-size: 22px; font-weight: 700; }
    h2 { margin: 0 0 12px; font-size: 13px; text-transform: uppercase; color: var(--muted); letter-spacing: .04em; }
    button, input, select {
      font: inherit;
      border: 1px solid var(--line);
      background: white;
      color: var(--ink);
      border-radius: 6px;
      padding: 8px 10px;
    }
    button { cursor: pointer; }
    button.primary { background: var(--blue); color: white; border-color: var(--blue); }
    main { padding: 22px 28px 34px; max-width: 1800px; margin: 0 auto; }
    .tabs { display: flex; gap: 8px; flex-wrap: wrap; margin-bottom: 16px; }
    .tab { border-color: var(--line); }
    .tab.active { background: var(--blue); color: white; border-color: var(--blue); }
    .panel { display: none; }
    .panel.active { display: block; }
    .grid { display: grid; gap: 14px; }
    .grid.cards { grid-template-columns: repeat(auto-fit, minmax(190px, 1fr)); margin-bottom: 14px; }
    .grid.two { grid-template-columns: repeat(auto-fit, minmax(480px, 1fr)); }
    .card {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 16px;
      min-width: 0;
    }
    .metric { font-size: 28px; font-weight: 700; line-height: 1.15; }
    .label { color: var(--muted); font-size: 12px; margin-top: 2px; }
    .toolbar { display: flex; gap: 8px; flex-wrap: wrap; align-items: center; margin-bottom: 12px; }
    .table-wrap { overflow-x: auto; }
    table { width: 100%; border-collapse: collapse; }
    th {
      text-align: left;
      font-size: 11px;
      text-transform: uppercase;
      color: var(--muted);
      letter-spacing: .04em;
      border-bottom: 1px solid var(--line);
      padding: 8px 8px;
      white-space: nowrap;
    }
    td { border-bottom: 1px solid #eef2f6; padding: 9px 8px; vertical-align: top; }
    tr:hover td { background: #f9fbfd; }
    .mono { font-family: Consolas, "Liberation Mono", monospace; font-size: 12px; }
    .right { text-align: right; }
    .muted { color: var(--muted); }
    .truncate { max-width: 360px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .badge { display: inline-block; border-radius: 999px; padding: 2px 8px; font-size: 12px; font-weight: 700; }
    .succeeded, .completed, .ok, .passed, .sunk, .curated { background: #dcfce7; color: var(--green); }
    .failed { background: #fee4e2; color: var(--red); }
    .partial, .warned, .dq_warned { background: #fef0c7; color: var(--amber); }
    .running, .processing, .ingesting { background: #dbeafe; color: var(--blue); }
    .skipped { background: #e2e8f0; color: #475569; }
    .edge { color: var(--violet); font-weight: 700; }
    .empty { padding: 28px; text-align: center; color: var(--muted); }
    .link { color: var(--blue); text-decoration: none; font-weight: 700; }
    .link:hover { text-decoration: underline; }
    .details { display: grid; grid-template-columns: 160px 1fr; gap: 8px 12px; }
    .details div:nth-child(odd) { color: var(--muted); }
    #toast { position: fixed; right: 20px; bottom: 20px; background: #142033; color: white; padding: 12px 14px; border-radius: 8px; display: none; max-width: 520px; }
  </style>
</head>
<body>
  <header>
    <div>
      <h1>ODS Operations Control</h1>
      <div class="muted">Authoritative views from run_log, run_stage_log, file_catalogue, reconciliation_log, and lineage_edge</div>
    </div>
    <div class="toolbar">
      <button class="primary" onclick="refreshActive()">Refresh</button>
      <span id="last-refresh" class="muted"></span>
    </div>
  </header>
  <main>
    <div class="tabs">
      <button class="tab active" onclick="showTab('overview')">Overview</button>
      <button class="tab" onclick="showTab('runs')">Runs</button>
      <button class="tab" onclick="showTab('api-pull')">API Pull</button>
      <button class="tab" onclick="showTab('direct-postgres')">Direct Postgres</button>
      <button class="tab" onclick="showTab('direct-kafka')">Direct Kafka</button>
      <button class="tab" onclick="showTab('run-detail')">Run Detail</button>
      <button class="tab" onclick="showTab('file-lineage')">File Lineage</button>
      <button class="tab" onclick="showTab('events')">Event Diagnostics</button>
      <button class="tab" onclick="showTab('topic-events')">Topic Messages</button>
    </div>

    <section id="tab-overview" class="panel active">
      <div id="overview-cards" class="grid cards"></div>
      <div class="grid two">
        <div class="card"><h2>Recent Failed Or Partial Runs</h2><div id="recent-failures"></div></div>
        <div class="card"><h2>Stuck Running Runs</h2><div id="stuck-runs"></div></div>
        <div class="card"><h2>Reconciliation Failures</h2><div id="recon-failures"></div></div>
        <div class="card"><h2>Latest Runs</h2><div id="latest-runs"></div></div>
      </div>
    </section>

    <section id="tab-runs" class="panel">
      <div class="card">
        <h2>Run Search</h2>
        <div class="toolbar">
          <input id="runs-domain" placeholder="domain" />
          <input id="runs-dataset" placeholder="dataset" />
          <input id="runs-pipeline-type" placeholder="pipeline type" />
          <input id="runs-business-date" type="date" />
          <select id="runs-status">
            <option value="">all statuses</option>
            <option value="running">running</option>
            <option value="succeeded">succeeded</option>
            <option value="failed">failed</option>
            <option value="partial">partial</option>
          </select>
          <input id="runs-file-id" placeholder="file id fragment" />
          <input id="runs-id" placeholder="run id fragment" />
          <input id="runs-limit" type="number" min="1" max="1000" value="100" style="width: 90px" />
          <button onclick="loadRuns()">Search</button>
          <button onclick="clearRunFilters()">Clear</button>
        </div>
        <div id="runs-results"></div>
      </div>
    </section>

    <section id="tab-api-pull" class="panel">
      <div class="card">
        <h2>API Pull Control</h2>
        <div class="toolbar">
          <input id="api-domain" placeholder="domain" />
          <input id="api-dataset" placeholder="dataset" />
          <input id="api-source-application" placeholder="source application" />
          <select id="api-status">
            <option value="">all statuses</option>
            <option value="running">running</option>
            <option value="succeeded">succeeded</option>
            <option value="failed">failed</option>
            <option value="partial">partial</option>
          </select>
          <input id="api-business-date" type="date" />
          <input id="api-run-id" placeholder="run id fragment" />
          <input id="api-file-id" placeholder="file id fragment" />
          <input id="api-limit" type="number" min="1" max="500" value="50" style="width: 90px" />
          <button onclick="loadApiPull()">Search</button>
          <button onclick="clearApiPullFilters()">Clear</button>
        </div>
      </div>
      <div id="api-pull-content" style="margin-top:14px"></div>
    </section>

    <section id="tab-direct-postgres" class="panel">
      <div class="card">
        <h2>Direct Postgres Delivery</h2>
        <div class="muted" style="margin-bottom: 12px">Datasets with <code>dataset_config.delivery='direct_postgres'</code>. Reads from <code>run_log</code> (pipeline_type='direct_postgres'), <code>reconciliation_log</code> (check_type='direct_postgres_count'), <code>run_stage_log</code> + <code>file_catalogue</code>.</div>
        <div class="toolbar">
          <input id="dp-domain" placeholder="domain" />
          <input id="dp-dataset" placeholder="dataset" />
          <input id="dp-business-date" type="date" />
          <input id="dp-limit" type="number" min="1" max="500" value="50" style="width: 90px" />
          <button onclick="loadDirectPostgres()">Search</button>
          <button onclick="clearDirectPostgresFilters()">Clear</button>
        </div>
      </div>
      <div id="direct-postgres-content" style="margin-top:14px"></div>
    </section>

    <section id="tab-direct-kafka" class="panel">
      <div class="card">
        <h2>Direct Kafka Delivery</h2>
        <div class="muted" style="margin-bottom: 12px">Datasets with <code>dataset_config.delivery='direct_kafka'</code>. Sink lag compares the JDBC sink consumer-group's committed offsets against the latest <code>kafka_publish</code> stage's <code>offset_end_by_partition</code>.</div>
        <div class="toolbar">
          <input id="dk-domain" placeholder="domain" />
          <input id="dk-dataset" placeholder="dataset" />
          <input id="dk-business-date" type="date" />
          <label class="muted" style="display:flex; align-items:center; gap:6px;">
            <input id="dk-include-sink-lag" type="checkbox" checked /> include sink lag
          </label>
          <input id="dk-limit" type="number" min="1" max="500" value="50" style="width: 90px" />
          <button onclick="loadDirectKafka()">Search</button>
          <button onclick="clearDirectKafkaFilters()">Clear</button>
        </div>
      </div>
      <div id="direct-kafka-content" style="margin-top:14px"></div>
    </section>

    <section id="tab-run-detail" class="panel">
      <div class="card">
        <h2>Run Detail</h2>
        <div class="toolbar">
          <input id="detail-run-id" placeholder="run id" style="min-width: 360px" />
          <button onclick="loadRunDetail()">Load Run</button>
        </div>
        <div id="run-detail-content"></div>
      </div>
    </section>

    <section id="tab-file-lineage" class="panel">
      <div class="card">
        <h2>File Lineage</h2>
        <div class="toolbar">
          <input id="lineage-file-id" placeholder="file id" style="min-width: 360px" />
          <button onclick="loadFileLineage()">Load File</button>
        </div>
        <div id="file-lineage-content"></div>
      </div>
    </section>

    <section id="tab-events" class="panel">
      <div class="card">
        <h2>Run Event Diagnostics</h2>
        <div class="muted" style="margin-bottom: 12px">This tab checks the best-effort event mirror. It is intentionally not the source of truth for run health.</div>
        <div id="event-diagnostics"></div>
      </div>
    </section>

    <section id="tab-topic-events" class="panel">
      <div class="card">
        <h2>Kafka Topic Messages</h2>
        <div class="muted" style="margin-bottom: 12px">Tails recent Avro messages from the run-events topic. This is useful for checking what actually reached Kafka.</div>
        <div class="toolbar">
          <input id="topic-name" value="ods.pipeline.run-events" style="min-width: 260px" />
          <input id="topic-domain" placeholder="domain" />
          <input id="topic-dataset" placeholder="dataset" />
          <select id="topic-status">
            <option value="">all statuses</option>
            <option value="running">running</option>
            <option value="succeeded">succeeded</option>
            <option value="failed">failed</option>
            <option value="partial">partial</option>
          </select>
          <input id="topic-event-type" placeholder="event type" />
          <input id="topic-pipeline-type" placeholder="pipeline type" />
          <input id="topic-run-id" placeholder="run id fragment" />
          <input id="topic-file-id" placeholder="file id fragment" />
          <input id="topic-limit" type="number" min="1" max="1000" value="100" style="width: 90px" />
          <button onclick="loadTopicMessages()">Load Messages</button>
          <button onclick="clearTopicFilters()">Clear</button>
        </div>
        <div id="topic-messages"></div>
      </div>
    </section>
  </main>
  <div id="toast"></div>

<script>
let activeTab = 'overview';

function esc(v) {
  if (v === null || v === undefined) return '';
  return String(v).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
}
function fmt(v) { return v === null || v === undefined || v === '' ? '-' : Number(v).toLocaleString(); }
function dt(v) { return v ? new Date(v).toLocaleString('en-GB', {dateStyle:'short', timeStyle:'medium'}) : '-'; }
function text(v) { return v === null || v === undefined || v === '' ? '-' : esc(v); }
function badge(v) { const s = String(v || 'unknown'); return `<span class="badge ${esc(s)}">${esc(s)}</span>`; }
function shortId(v) { return v ? `<span title="${esc(v)}">${esc(String(v).slice(0, 8))}</span>` : '-'; }
function toast(msg) {
  const el = document.getElementById('toast');
  el.textContent = msg;
  el.style.display = 'block';
  setTimeout(() => el.style.display = 'none', 5000);
}
async function getJson(url) {
  const res = await fetch(url);
  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    throw new Error(body.detail || res.statusText);
  }
  return res.json();
}
function showTab(name) {
  activeTab = name;
  document.querySelectorAll('.tab').forEach(btn => btn.classList.remove('active'));
  document.querySelectorAll('.panel').forEach(panel => panel.classList.remove('active'));
  document.querySelector(`button[onclick="showTab('${name}')"]`).classList.add('active');
  document.getElementById('tab-' + name).classList.add('active');
  refreshActive();
}
function refreshStamp() {
  document.getElementById('last-refresh').textContent = 'Last refreshed ' + new Date().toLocaleTimeString('en-GB');
}
async function refreshActive() {
  try {
    if (activeTab === 'overview') await loadOverview();
    if (activeTab === 'runs') await loadRuns();
    if (activeTab === 'api-pull') await loadApiPull();
    if (activeTab === 'direct-postgres') await loadDirectPostgres();
    if (activeTab === 'direct-kafka') await loadDirectKafka();
    if (activeTab === 'events') await loadEventDiagnostics();
    if (activeTab === 'topic-events') await loadTopicMessages();
    refreshStamp();
  } catch (err) {
    toast(err.message);
  }
}

function table(rows, cols, empty = 'No rows found') {
  if (!rows || rows.length === 0) return `<div class="empty">${empty}</div>`;
  return `<div class="table-wrap"><table><thead><tr>${cols.map(c => `<th class="${c.right ? 'right' : ''}">${esc(c.label)}</th>`).join('')}</tr></thead><tbody>` +
    rows.map(row => `<tr>${cols.map(c => {
      const raw = typeof c.value === 'function' ? c.value(row) : row[c.key];
      const value = c.html ? raw : text(raw);
      return `<td class="${c.right ? 'right ' : ''}${c.mono ? 'mono ' : ''}${c.truncate ? 'truncate ' : ''}">${value}</td>`;
    }).join('')}</tr>`).join('') + '</tbody></table></div>';
}

async function loadOverview() {
  const data = await getJson('/api/overview');
  const statusCounts = Object.fromEntries((data.run_status_today || []).map(r => [r.status, r.count]));
  const fileCounts = Object.fromEntries((data.file_state || []).map(r => [r.state, r.count]));
  const events = Object.fromEntries((data.event_diagnostics || []).map(r => [r.status, r.count]));
  document.getElementById('overview-cards').innerHTML = [
    ['Runs Succeeded Today', statusCounts.succeeded || 0, 'succeeded'],
    ['Runs Failed Today', statusCounts.failed || 0, 'failed'],
    ['Runs Partial Today', statusCounts.partial || 0, 'partial'],
    ['Runs Running', statusCounts.running || 0, 'running'],
    ['Files Failed', fileCounts.failed || 0, 'failed'],
    ['Files Processing', (fileCounts.processing || 0) + (fileCounts.ingesting || 0), 'running'],
    ['Event Failures Today', events.failed || 0, 'failed'],
  ].map(([label, value, cls]) => `<div class="card"><div class="metric ${cls}">${fmt(value)}</div><div class="label">${esc(label)}</div></div>`).join('');

  const runCols = [
    {label:'Run', html:true, value:r => `<a class="link mono" href="#" onclick="openRun('${esc(r.run_id)}')">${shortId(r.run_id)}</a>`},
    {label:'Type', key:'pipeline_type'},
    {label:'Domain / Dataset', value:r => `${r.domain} / ${r.dataset}`},
    {label:'Date', key:'business_date'},
    {label:'Status', html:true, value:r => badge(r.status)},
    {label:'Started', html:true, value:r => dt(r.started_at)},
    {label:'Error', key:'error_summary', truncate:true},
  ];
  document.getElementById('recent-failures').innerHTML = table(data.recent_failures, runCols, 'No failed or partial runs.');
  document.getElementById('stuck-runs').innerHTML = table(data.stuck_runs, runCols, 'No stuck running runs.');
  document.getElementById('recon-failures').innerHTML = table(data.recon_failures, [
    {label:'Created', html:true, value:r => dt(r.created_at)},
    {label:'Check', key:'check_type'},
    {label:'Domain / Dataset', value:r => `${r.domain} / ${r.dataset}`},
    {label:'Status', html:true, value:r => badge(r.status)},
    {label:'Source', key:'source_count', right:true},
    {label:'Kafka', key:'kafka_count', right:true},
    {label:'Postgres', key:'postgres_count', right:true},
    {label:'Diff', key:'discrepancy_count', right:true},
    {label:'Detail', key:'detail', truncate:true},
  ], 'No reconciliation failures.');
  document.getElementById('latest-runs').innerHTML = table(data.latest_runs, runCols, 'No runs found.');
}

async function loadRuns() {
  const params = new URLSearchParams();
  for (const [id, key] of [
    ['runs-domain','domain'],
    ['runs-dataset','dataset'],
    ['runs-pipeline-type','pipeline_type'],
    ['runs-business-date','business_date'],
    ['runs-status','status'],
    ['runs-file-id','file_id'],
    ['runs-id','run_id'],
    ['runs-limit','limit'],
  ]) {
    const v = document.getElementById(id).value.trim();
    if (v) params.set(key, v);
  }
  const rows = await getJson('/api/runs?' + params.toString());
  document.getElementById('runs-results').innerHTML = table(rows, [
    {label:'Run', html:true, value:r => `<a class="link mono" href="#" onclick="openRun('${esc(r.run_id)}')">${shortId(r.run_id)}</a>`},
    {label:'Type', key:'pipeline_type'},
    {label:'Domain / Dataset', value:r => `${r.domain} / ${r.dataset}`},
    {label:'Date', key:'business_date'},
    {label:'File', html:true, value:r => r.file_id ? `<a class="link mono" href="#" onclick="openFile('${esc(r.file_id)}')">${shortId(r.file_id)}</a>` : '-'},
    {label:'Status', html:true, value:r => badge(r.status)},
    {label:'Source', key:'record_count_source', right:true},
    {label:'DQ Pass', key:'record_count_dq_pass', right:true},
    {label:'DQ Fail', key:'record_count_dq_fail', right:true},
    {label:'Published', key:'record_count_published', right:true},
    {label:'Started', html:true, value:r => dt(r.started_at)},
    {label:'Error', key:'error_summary', truncate:true},
  ]);
}
function clearRunFilters() {
  for (const id of ['runs-domain','runs-dataset','runs-pipeline-type','runs-business-date','runs-status','runs-file-id','runs-id']) {
    document.getElementById(id).value = '';
  }
  document.getElementById('runs-limit').value = '100';
  loadRuns();
}

async function loadApiPull() {
  const params = new URLSearchParams();
  for (const [id, key] of [
    ['api-domain','domain'],
    ['api-dataset','dataset'],
    ['api-source-application','source_application'],
    ['api-status','status'],
    ['api-business-date','business_date'],
    ['api-run-id','run_id'],
    ['api-file-id','file_id'],
    ['api-limit','limit'],
  ]) {
    const v = document.getElementById(id).value.trim();
    if (v) params.set(key, v);
  }
  const data = await getJson('/api/api-pull?' + params.toString());
  if (!data.available) {
    document.getElementById('api-pull-content').innerHTML =
      `<div class="card"><div class="empty">${esc(data.reason || 'API Pull tables are not available.')}</div></div>`;
    return;
  }
  const s = data.summary || {};
  const cards = `<div class="grid cards">
    <div class="card"><div class="metric succeeded">${fmt(s.succeeded_runs || 0)}</div><div class="label">api pulls succeeded today</div></div>
    <div class="card"><div class="metric failed">${fmt(s.failed_runs || 0)}</div><div class="label">api pulls failed/partial today</div></div>
    <div class="card"><div class="metric running">${fmt(s.running_runs || 0)}</div><div class="label">api pulls running today</div></div>
    <div class="card"><div class="metric partial">${fmt(s.pending_count || 0)}</div><div class="label">pending cursor rows</div></div>
    <div class="card"><div class="metric running">${fmt(s.locked_count || 0)}</div><div class="label">locked cursor rows</div></div>
  </div>`;

  const datasetTable = table(data.datasets || [], [
    {label:'Domain / Dataset', value:r => `${r.domain} / ${r.dataset}`},
    {label:'Source App', key:'source_application'},
    {label:'Active', key:'active'},
    {label:'Format', key:'raw_format'},
    {label:'Cursor', key:'cursor_style'},
    {label:'Auth', key:'auth_type'},
    {label:'Secret Ref', key:'secret_ref'},
    {label:'Raw Topic', key:'target_topic', truncate:true},
    {label:'Canonical Topic', key:'canonical_topic', truncate:true},
    {label:'Target Table', key:'postgres_target_table', truncate:true},
  ], 'No active API pull datasets matched the filters.');

  const watermarkTable = table(data.watermarks || [], [
    {label:'Domain / Dataset', value:r => `${r.domain} / ${r.dataset}`},
    {label:'Source App', key:'source_application'},
    {label:'Cursor Type', key:'cursor_type'},
    {label:'Committed Cursor', key:'committed_cursor_value', truncate:true},
    {label:'Pending Cursor', key:'pending_cursor_value', truncate:true},
    {label:'Pending Run', html:true, value:r => r.pending_run_id ? `<a class="link mono" href="#" onclick="openRun('${esc(r.pending_run_id)}')">${shortId(r.pending_run_id)}</a>` : '-'},
    {label:'Last Success', html:true, value:r => r.last_successful_run_id ? `<a class="link mono" href="#" onclick="openRun('${esc(r.last_successful_run_id)}')">${shortId(r.last_successful_run_id)}</a>` : '-'},
    {label:'Locked At', html:true, value:r => dt(r.locked_at)},
    {label:'Updated', html:true, value:r => dt(r.updated_at)},
    {label:'Lag', key:'pull_lag', truncate:true},
  ], 'No watermark rows found.');

  const runTable = table(data.latest_runs || [], [
    {label:'Run', html:true, value:r => `<a class="link mono" href="#" onclick="openRun('${esc(r.run_id)}')">${shortId(r.run_id)}</a>`},
    {label:'Domain / Dataset', value:r => `${r.domain} / ${r.dataset}`},
    {label:'Source App', key:'source_application'},
    {label:'Date', key:'business_date'},
    {label:'Status', html:true, value:r => badge(r.status)},
    {label:'Fetched', key:'record_count_source', right:true},
    {label:'Published', key:'record_count_published', right:true},
    {label:'Started', html:true, value:r => dt(r.started_at)},
    {label:'Ended', html:true, value:r => dt(r.ended_at)},
    {label:'Error', key:'error_summary', truncate:true},
  ], 'No API pull runs found.');

  const archiveTable = table(data.archives || [], [
    {label:'Run', html:true, value:r => r.run_id ? `<a class="link mono" href="#" onclick="openRun('${esc(r.run_id)}')">${shortId(r.run_id)}</a>` : '-'},
    {label:'Domain / Dataset', value:r => `${r.domain} / ${r.dataset}`},
    {label:'Date', key:'business_date'},
    {label:'Run Status', html:true, value:r => badge(r.run_status)},
    {label:'File', html:true, value:r => r.file_id ? `<a class="link mono" href="#" onclick="openFile('${esc(r.file_id)}')">${shortId(r.file_id)}</a>` : '-'},
    {label:'File State', html:true, value:r => badge(r.file_state)},
    {label:'Archived', key:'archived_count', right:true},
    {label:'Rows', key:'source_row_count', right:true},
    {label:'Archive URI', key:'archive_uri', truncate:true},
    {label:'MD5', key:'file_md5', truncate:true},
  ], 'No archive batches found.');

  const recon = reconTable(data.archive_reconciliation || []);
  const failures = table(data.failed_pulls || [], [
    {label:'Run', html:true, value:r => `<a class="link mono" href="#" onclick="openRun('${esc(r.run_id)}')">${shortId(r.run_id)}</a>`},
    {label:'Domain / Dataset', value:r => `${r.domain} / ${r.dataset}`},
    {label:'Source App', key:'source_application'},
    {label:'Status', html:true, value:r => badge(r.status)},
    {label:'Pending Cursor', key:'pending_cursor_value', truncate:true},
    {label:'Committed Cursor', key:'committed_cursor_value', truncate:true},
    {label:'Started', html:true, value:r => dt(r.started_at)},
    {label:'Error', key:'error_summary', truncate:true},
  ], 'No failed or partial API pulls matched the filters.');

  const replay = table(data.replay_candidates || [], [
    {label:'API Run', html:true, value:r => `<a class="link mono" href="#" onclick="openRun('${esc(r.api_pull_run_id)}')">${shortId(r.api_pull_run_id)}</a>`},
    {label:'Downstream', html:true, value:r => r.downstream_run_id ? `<a class="link mono" href="#" onclick="openRun('${esc(r.downstream_run_id)}')">${shortId(r.downstream_run_id)}</a>` : '-'},
    {label:'Domain / Dataset', value:r => `${r.domain} / ${r.dataset}`},
    {label:'File', html:true, value:r => r.file_id ? `<a class="link mono" href="#" onclick="openFile('${esc(r.file_id)}')">${shortId(r.file_id)}</a>` : '-'},
    {label:'API Status', html:true, value:r => badge(r.api_status)},
    {label:'Downstream Status', html:true, value:r => badge(r.downstream_status)},
    {label:'File State', html:true, value:r => badge(r.file_state)},
    {label:'Pending Cursor', key:'pending_cursor_value', truncate:true},
    {label:'Error', key:'error_summary', truncate:true},
  ], 'No replay candidates found.');

  document.getElementById('api-pull-content').innerHTML =
    cards +
    `<div class="grid two">
      <div class="card"><h2>Active API Pull Datasets</h2>${datasetTable}</div>
      <div class="card"><h2>Watermarks And Cursors</h2>${watermarkTable}</div>
      <div class="card"><h2>Latest API Pull Runs</h2>${runTable}</div>
      <div class="card"><h2>Archive Batches</h2>${archiveTable}</div>
      <div class="card"><h2>Fetched Vs Archived Reconciliation</h2>${recon}</div>
      <div class="card"><h2>Failed API Pulls</h2>${failures}</div>
      <div class="card"><h2>Replay Candidates</h2>${replay}</div>
    </div>`;
}
function clearApiPullFilters() {
  for (const id of ['api-domain','api-dataset','api-source-application','api-status','api-business-date','api-run-id','api-file-id']) {
    document.getElementById(id).value = '';
  }
  document.getElementById('api-limit').value = '50';
  loadApiPull();
}

function openRun(runId) {
  showTab('run-detail');
  document.getElementById('detail-run-id').value = runId;
  loadRunDetail();
}
function openFile(fileId) {
  showTab('file-lineage');
  document.getElementById('lineage-file-id').value = fileId;
  loadFileLineage();
}
function details(obj, fields) {
  return `<div class="details">` + fields.map(([label, key, render]) =>
    `<div>${esc(label)}</div><div>${render ? render(obj[key], obj) : text(obj[key])}</div>`).join('') + `</div>`;
}
async function loadRunDetail() {
  const runId = document.getElementById('detail-run-id').value.trim();
  if (!runId) return toast('Enter a run_id');
  const data = await getJson('/api/run/' + encodeURIComponent(runId));
  const run = data.run;
  document.getElementById('run-detail-content').innerHTML =
    `<div class="grid two">
      <div class="card">${details(run, [
        ['Run ID','run_id', v => `<span class="mono">${esc(v)}</span>`],
        ['Pipeline Type','pipeline_type'],
        ['Domain','domain'],
        ['Dataset','dataset'],
        ['Business Date','business_date'],
        ['File ID','file_id', v => v ? `<a class="link mono" href="#" onclick="openFile('${esc(v)}')">${esc(v)}</a>` : '-'],
        ['Status','status', v => badge(v)],
        ['Started','started_at', dt],
        ['Ended','ended_at', dt],
        ['Error','error_summary'],
      ])}</div>
      <div class="card">${details(run, [
        ['Source Count','record_count_source'],
        ['DQ Pass','record_count_dq_pass'],
        ['DQ Fail','record_count_dq_fail'],
        ['Published','record_count_published'],
        ['Kafka Topic','kafka_topic'],
        ['Offset Start','kafka_offset_start'],
        ['Offset End','kafka_offset_end'],
        ['Config Version','config_version_id'],
        ['Schema Version','schema_version_id'],
      ])}</div>
    </div>
    <div class="card" style="margin-top:14px"><h2>Stage Timeline</h2>${stageTable(data.stages)}</div>
    <div class="grid two" style="margin-top:14px">
      <div class="card"><h2>Reconciliation</h2>${reconTable(data.reconciliation)}</div>
      <div class="card"><h2>Lineage Edges</h2>${edgeTable(data.edges)}</div>
    </div>`;
}
function stageTable(rows) {
  return table(rows, [
    {label:'Stage', key:'stage'},
    {label:'Event', key:'event_type'},
    {label:'Status', html:true, value:r => badge(r.status)},
    {label:'In', key:'record_count_in', right:true},
    {label:'Out', key:'record_count_out', right:true},
    {label:'Input', key:'input_ref', truncate:true},
    {label:'Output', key:'output_ref', truncate:true},
    {label:'Started', html:true, value:r => dt(r.started_at)},
    {label:'Ended', html:true, value:r => dt(r.ended_at)},
    {label:'Error', key:'error', truncate:true},
  ], 'No stage rows recorded.');
}
function reconTable(rows) {
  return table(rows, [
    {label:'Check', key:'check_type'},
    {label:'Status', html:true, value:r => badge(r.status)},
    {label:'Source', key:'source_count', right:true},
    {label:'Kafka', key:'kafka_count', right:true},
    {label:'Postgres', key:'postgres_count', right:true},
    {label:'Diff', key:'discrepancy_count', right:true},
    {label:'Created', html:true, value:r => dt(r.created_at)},
    {label:'Detail', key:'detail', truncate:true},
  ], 'No reconciliation rows.');
}
function edgeTable(rows) {
  return table(rows, [
    {label:'Edge Type', html:true, value:r => `<span class="edge">${esc(r.edge_type)}</span>`},
    {label:'Parent File', html:true, value:r => r.parent_file_id ? `<a class="link mono" href="#" onclick="openFile('${esc(r.parent_file_id)}')">${shortId(r.parent_file_id)}</a>` : '-'},
    {label:'Parent Run', html:true, value:r => r.parent_run_id ? `<a class="link mono" href="#" onclick="openRun('${esc(r.parent_run_id)}')">${shortId(r.parent_run_id)}</a>` : '-'},
    {label:'Child Run', html:true, value:r => r.child_run_id ? `<a class="link mono" href="#" onclick="openRun('${esc(r.child_run_id)}')">${shortId(r.child_run_id)}</a>` : '-'},
    {label:'Source', key:'source_ref', truncate:true},
    {label:'Target', key:'target_ref', truncate:true},
    {label:'Records', key:'record_count', right:true},
    {label:'Created', html:true, value:r => dt(r.created_at)},
  ], 'No lineage edges recorded.');
}
async function loadFileLineage() {
  const fileId = document.getElementById('lineage-file-id').value.trim();
  if (!fileId) return toast('Enter a file_id');
  const data = await getJson('/api/file/' + encodeURIComponent(fileId) + '/lineage');
  const file = data.file;
  const allRuns = data.involved_runs || Array.from(
    new Map([...(data.direct_runs || []), ...(data.lineage_runs || [])].map(r => [r.run_id, r])).values()
  );
  document.getElementById('file-lineage-content').innerHTML =
    `<div class="grid two">
      <div class="card"><h2>File</h2>${details(file, [
        ['File ID','file_id', v => `<span class="mono">${esc(v)}</span>`],
        ['Domain','domain'],
        ['Dataset','dataset'],
        ['Business Date','business_date'],
        ['State','state', v => badge(v)],
        ['Raw Path','s3_raw_path'],
        ['Curated Path','s3_curated_path'],
        ['MD5','file_md5'],
        ['First Seen','first_seen_at', dt],
        ['Updated','state_updated_at', dt],
      ])}</div>
      <div class="card"><h2>Runs Involving File</h2>${table(allRuns, [
        {label:'Run', html:true, value:r => `<a class="link mono" href="#" onclick="openRun('${esc(r.run_id)}')">${shortId(r.run_id)}</a>`},
        {label:'Type', key:'pipeline_type'},
        {label:'Dataset', key:'dataset'},
        {label:'Status', html:true, value:r => badge(r.status)},
        {label:'Started', html:true, value:r => dt(r.started_at)},
      ], 'No runs found.')}</div>
    </div>
    <div class="card" style="margin-top:14px"><h2>Lineage Edges In Both Directions</h2>${edgeTable(data.edges)}</div>
    <div class="card" style="margin-top:14px"><h2>Stages From Related Runs</h2>${stageTable(data.stages)}</div>`;
}
async function loadEventDiagnostics() {
  const data = await getJson('/api/event-diagnostics');
  if (!data.available) {
    document.getElementById('event-diagnostics').innerHTML = '<div class="empty">pipeline.run_events is not available.</div>';
    return;
  }
  document.getElementById('event-diagnostics').innerHTML =
    `<div class="grid cards">${(data.by_status || []).map(r => `<div class="card"><div class="metric">${fmt(r.count)}</div><div class="label">${esc(r.status)} events today</div></div>`).join('')}</div>` +
    table(data.events, [
      {label:'Event', key:'event_type'},
      {label:'Run', html:true, value:r => r.run_id ? `<a class="link mono" href="#" onclick="openRun('${esc(r.run_id)}')">${shortId(r.run_id)}</a>` : '-'},
      {label:'Pipeline', key:'pipeline_type'},
      {label:'Domain / Dataset', value:r => `${r.domain} / ${r.dataset}`},
      {label:'Status', html:true, value:r => badge(r.status)},
      {label:'Published', key:'record_count_published', right:true},
      {label:'Occurred', html:true, value:r => dt(r.occurred_at)},
      {label:'Error', key:'error_summary', truncate:true},
    ], 'No event rows.');
}
async function loadTopicMessages() {
  const params = new URLSearchParams();
  const fields = [
    ['topic-name', 'topic'],
    ['topic-domain', 'domain'],
    ['topic-dataset', 'dataset'],
    ['topic-status', 'status'],
    ['topic-event-type', 'event_type'],
    ['topic-pipeline-type', 'pipeline_type'],
    ['topic-run-id', 'run_id'],
    ['topic-file-id', 'file_id'],
    ['topic-limit', 'limit'],
  ];
  for (const [id, key] of fields) {
    const value = document.getElementById(id).value.trim();
    if (value) params.set(key, value);
  }
  const data = await getJson('/api/topic-run-events?' + params.toString());
  const summary = `<div class="grid cards">
    <div class="card"><div class="metric">${fmt((data.messages || []).length)}</div><div class="label">messages shown</div></div>
    <div class="card"><div class="metric">${fmt((data.partitions || []).length)}</div><div class="label">partitions</div></div>
    <div class="card"><div class="metric">${fmt((data.partitions || []).reduce((a, p) => a + Number(p.messages || 0), 0))}</div><div class="label">messages retained</div></div>
  </div>`;
  const partitionTable = table(data.partitions || [], [
    {label:'Partition', key:'partition', right:true},
    {label:'Low', key:'low', right:true},
    {label:'High', key:'high', right:true},
    {label:'Messages', key:'messages', right:true},
  ], 'No partitions found.');
  const messageTable = table(data.messages || [], [
    {label:'Event', key:'event_type'},
    {label:'Pipeline', key:'pipeline_type'},
    {label:'Domain / Dataset', value:r => `${r.domain || '-'} / ${r.dataset || '-'}`},
    {label:'Business Date', key:'business_date'},
    {label:'Status', html:true, value:r => badge(r.status)},
    {label:'Source', key:'record_count_source', right:true},
    {label:'DQ Pass', key:'record_count_dq_pass', right:true},
    {label:'DQ Fail', key:'record_count_dq_fail', right:true},
    {label:'Published', key:'record_count_published', right:true},
    {label:'Topic', key:'kafka_topic', truncate:true},
    {label:'Run', html:true, value:r => r.run_id ? `<a class="link mono" href="#" onclick="openRun('${esc(r.run_id)}')">${shortId(r.run_id)}</a>` : '-'},
    {label:'File', html:true, value:r => r.file_id ? `<a class="link mono" href="#" onclick="openFile('${esc(r.file_id)}')">${shortId(r.file_id)}</a>` : '-'},
    {label:'Partition', key:'_partition', right:true},
    {label:'Offset', key:'_offset', right:true},
    {label:'Occurred', html:true, value:r => dt(r.occurred_at)},
    {label:'Error', key:'error_summary', truncate:true},
  ], 'No topic messages matched the filters.');
  document.getElementById('topic-messages').innerHTML =
    summary +
    `<div class="card" style="margin-top:14px"><h2>Topic: ${esc(data.topic || '')}</h2>${partitionTable}</div>` +
    `<div class="card" style="margin-top:14px"><h2>Messages</h2>${messageTable}</div>`;
}
function clearTopicFilters() {
  for (const id of ['topic-domain','topic-dataset','topic-status','topic-event-type','topic-pipeline-type','topic-run-id','topic-file-id']) {
    document.getElementById(id).value = '';
  }
  document.getElementById('topic-name').value = 'ods.pipeline.run-events';
  document.getElementById('topic-limit').value = '100';
  loadTopicMessages();
}
async function loadDirectPostgres() {
  const params = new URLSearchParams();
  for (const [id, key] of [
    ['dp-domain','domain'],
    ['dp-dataset','dataset'],
    ['dp-business-date','business_date'],
    ['dp-limit','limit'],
  ]) {
    const v = document.getElementById(id).value.trim();
    if (v) params.set(key, v);
  }
  const data = await getJson('/api/direct-postgres?' + params.toString());
  const s = data.summary || {};
  const cards = `<div class="grid cards">
    <div class="card"><div class="metric succeeded">${fmt(s.succeeded_runs || 0)}</div><div class="label">direct_postgres succeeded today</div></div>
    <div class="card"><div class="metric failed">${fmt(s.failed_runs || 0)}</div><div class="label">direct_postgres failed/partial today</div></div>
    <div class="card"><div class="metric running">${fmt(s.running_runs || 0)}</div><div class="label">direct_postgres running today</div></div>
    <div class="card"><div class="metric">${fmt((data.datasets || []).length)}</div><div class="label">configured datasets</div></div>
  </div>`;

  const datasetTable = table(data.datasets || [], [
    {label:'Domain / Dataset', value:r => `${r.domain} / ${r.dataset}`},
    {label:'Active', key:'active'},
    {label:'Source Type', key:'source_type'},
    {label:'Format', key:'raw_format'},
    {label:'Postgres Target', key:'postgres_target_table'},
    {label:'Topic', key:'target_topic', truncate:true},
  ], 'No direct_postgres datasets configured.');

  const runsTable = table(data.latest_runs || [], [
    {label:'Run', html:true, value:r => `<a class="link mono" href="#" onclick="openRun('${esc(r.run_id)}')">${shortId(r.run_id)}</a>`},
    {label:'Domain / Dataset', value:r => `${r.domain} / ${r.dataset}`},
    {label:'Date', html:true, value:r => dt(r.business_date)},
    {label:'Status', html:true, value:r => badge(r.status)},
    {label:'Source', key:'record_count_source', right:true},
    {label:'Published', key:'record_count_published', right:true},
    {label:'Started', html:true, value:r => dt(r.started_at)},
    {label:'Ended', html:true, value:r => dt(r.ended_at)},
    {label:'Error', key:'error_summary', truncate:true},
  ], 'No direct_postgres runs found.');

  const reconTable = table(data.reconciliations || [], [
    {label:'When', html:true, value:r => dt(r.created_at)},
    {label:'Run', html:true, value:r => r.run_id ? `<a class="link mono" href="#" onclick="openRun('${esc(r.run_id)}')">${shortId(r.run_id)}</a>` : '-'},
    {label:'Domain / Dataset', value:r => `${r.domain} / ${r.dataset}`},
    {label:'Status', html:true, value:r => badge(r.status)},
    {label:'Source', key:'source_count', right:true},
    {label:'Postgres', key:'postgres_count', right:true},
    {label:'Diff', key:'discrepancy_count', right:true},
    {label:'Detail', key:'detail', truncate:true},
  ], 'No direct_postgres_count reconciliation rows.');

  const curatedTable = table(data.curated_files || [], [
    {label:'Run', html:true, value:r => `<a class="link mono" href="#" onclick="openRun('${esc(r.run_id)}')">${shortId(r.run_id)}</a>`},
    {label:'Domain / Dataset', value:r => `${r.domain} / ${r.dataset}`},
    {label:'File', html:true, value:r => r.file_id ? `<a class="link mono" href="#" onclick="openFile('${esc(r.file_id)}')">${shortId(r.file_id)}</a>` : '-'},
    {label:'Curated Path', key:'curated_parquet_path', truncate:true},
    {label:'Curated S3', key:'s3_curated_path', truncate:true},
    {label:'File State', html:true, value:r => badge(r.file_state)},
    {label:'Rows', key:'source_row_count', right:true},
  ], 'No curated parquet rows.');

  const failedTable = table(data.failed_recent || [], [
    {label:'Run', html:true, value:r => `<a class="link mono" href="#" onclick="openRun('${esc(r.run_id)}')">${shortId(r.run_id)}</a>`},
    {label:'Domain / Dataset', value:r => `${r.domain} / ${r.dataset}`},
    {label:'Status', html:true, value:r => badge(r.status)},
    {label:'Started', html:true, value:r => dt(r.started_at)},
    {label:'Ended', html:true, value:r => dt(r.ended_at)},
    {label:'Error', key:'error_summary', truncate:true},
  ], 'No failed direct_postgres runs in the last 24h.');

  document.getElementById('direct-postgres-content').innerHTML =
    cards +
    `<div class="card" style="margin-top:14px"><h2>Datasets</h2>${datasetTable}</div>` +
    `<div class="card" style="margin-top:14px"><h2>Latest Runs</h2>${runsTable}</div>` +
    `<div class="card" style="margin-top:14px"><h2>Source vs Postgres Reconciliation</h2>${reconTable}</div>` +
    `<div class="card" style="margin-top:14px"><h2>Latest Curated Parquet</h2>${curatedTable}</div>` +
    `<div class="card" style="margin-top:14px"><h2>Failed Runs (last 24h)</h2>${failedTable}</div>`;
}
function clearDirectPostgresFilters() {
  for (const id of ['dp-domain','dp-dataset','dp-business-date']) {
    document.getElementById(id).value = '';
  }
  document.getElementById('dp-limit').value = '50';
  loadDirectPostgres();
}

async function loadDirectKafka() {
  const params = new URLSearchParams();
  for (const [id, key] of [
    ['dk-domain','domain'],
    ['dk-dataset','dataset'],
    ['dk-business-date','business_date'],
    ['dk-limit','limit'],
  ]) {
    const v = document.getElementById(id).value.trim();
    if (v) params.set(key, v);
  }
  const includeLag = document.getElementById('dk-include-sink-lag').checked;
  params.set('include_sink_lag', includeLag ? 'true' : 'false');
  const data = await getJson('/api/direct-kafka?' + params.toString());
  const s = data.summary || {};
  const cards = `<div class="grid cards">
    <div class="card"><div class="metric succeeded">${fmt(s.succeeded_runs || 0)}</div><div class="label">direct_kafka pulls succeeded today</div></div>
    <div class="card"><div class="metric failed">${fmt(s.failed_runs || 0)}</div><div class="label">direct_kafka pulls failed/partial today</div></div>
    <div class="card"><div class="metric running">${fmt(s.running_runs || 0)}</div><div class="label">direct_kafka pulls running today</div></div>
    <div class="card"><div class="metric">${fmt((data.datasets || []).length)}</div><div class="label">configured datasets</div></div>
  </div>`;

  const datasetTable = table(data.datasets || [], [
    {label:'Domain / Dataset', value:r => `${r.domain} / ${r.dataset}`},
    {label:'Active', key:'active'},
    {label:'Source App', key:'source_application'},
    {label:'Source Type', key:'source_type'},
    {label:'Topic', key:'target_topic', truncate:true},
  ], 'No direct_kafka datasets configured.');

  const runsTable = table(data.latest_runs || [], [
    {label:'Run', html:true, value:r => `<a class="link mono" href="#" onclick="openRun('${esc(r.run_id)}')">${shortId(r.run_id)}</a>`},
    {label:'Domain / Dataset', value:r => `${r.domain} / ${r.dataset}`},
    {label:'Status', html:true, value:r => badge(r.status)},
    {label:'Started', html:true, value:r => dt(r.started_at)},
    {label:'Source', key:'record_count_source', right:true},
    {label:'Published', key:'record_count_published', right:true},
    {label:'Topic', key:'kafka_topic', truncate:true},
    {label:'Offset End', key:'kafka_offset_end', right:true},
    {label:'Error', key:'error_summary', truncate:true},
  ], 'No direct_kafka pull runs found.');

  const wmTable = table(data.watermarks || [], [
    {label:'Domain / Dataset', value:r => `${r.domain} / ${r.dataset}`},
    {label:'Source App', key:'source_application'},
    {label:'Cursor Type', key:'cursor_type'},
    {label:'Committed', key:'committed_cursor_value', truncate:true},
    {label:'Pending', key:'pending_cursor_value', truncate:true},
    {label:'Pending Run', html:true, value:r => r.pending_run_id ? `<a class="link mono" href="#" onclick="openRun('${esc(r.pending_run_id)}')">${shortId(r.pending_run_id)}</a>` : '-'},
    {label:'Pull Lag', key:'pull_lag'},
    {label:'Updated', html:true, value:r => dt(r.updated_at)},
  ], 'No direct_kafka watermark rows.');

  const lagRows = data.sink_lag || [];
  const lagTable = table(lagRows, [
    {label:'Domain / Dataset', value:r => `${r.domain} / ${r.dataset}`},
    {label:'Available', key:'available'},
    {label:'Topic', key:'topic', truncate:true},
    {label:'Group', key:'consumer_group', truncate:true},
    {label:'Caught Up', key:'caught_up'},
    {label:'Total Lag', key:'total_lag', right:true},
    {label:'Reason', key:'reason', truncate:true},
  ], 'No sink lag rows (Kafka may be unavailable).');

  const reconTable = table(data.reconciliations || [], [
    {label:'When', html:true, value:r => dt(r.created_at)},
    {label:'Run', html:true, value:r => r.run_id ? `<a class="link mono" href="#" onclick="openRun('${esc(r.run_id)}')">${shortId(r.run_id)}</a>` : '-'},
    {label:'Domain / Dataset', value:r => `${r.domain} / ${r.dataset}`},
    {label:'Status', html:true, value:r => badge(r.status)},
    {label:'Source', key:'source_count', right:true},
    {label:'Kafka', key:'kafka_count', right:true},
    {label:'Diff', key:'discrepancy_count', right:true},
    {label:'Detail', key:'detail', truncate:true},
  ], 'No api_pull_publish_count reconciliation rows.');

  document.getElementById('direct-kafka-content').innerHTML =
    cards +
    `<div class="card" style="margin-top:14px"><h2>Datasets</h2>${datasetTable}</div>` +
    `<div class="card" style="margin-top:14px"><h2>Latest API Pull Runs</h2>${runsTable}</div>` +
    `<div class="card" style="margin-top:14px"><h2>Watermarks &amp; Pull Lag</h2>${wmTable}</div>` +
    `<div class="card" style="margin-top:14px"><h2>JDBC Sink Lag</h2>${lagTable}</div>` +
    `<div class="card" style="margin-top:14px"><h2>Publish-Count Reconciliation</h2>${reconTable}</div>`;
}
function clearDirectKafkaFilters() {
  for (const id of ['dk-domain','dk-dataset','dk-business-date']) {
    document.getElementById(id).value = '';
  }
  document.getElementById('dk-include-sink-lag').checked = true;
  document.getElementById('dk-limit').value = '50';
  loadDirectKafka();
}

refreshActive();
setInterval(refreshActive, 30000);
</script>
</body>
</html>
"""


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("scripts.ops_control_dashboard:app", host="0.0.0.0", port=8910, reload=True)
