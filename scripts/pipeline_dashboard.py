#!/usr/bin/env python3
"""
ODS Pipeline Dashboard — run events, Kafka topic health, consumer lag, connector status.

Usage:
    pip install fastapi uvicorn psycopg2-binary confluent-kafka requests
    uvicorn scripts.pipeline_dashboard:app --port 8900 --reload
    Open: http://localhost:8900
"""
from __future__ import annotations

import json
import os
import uuid as _uuid
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import psycopg2
import psycopg2.extras
import requests
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse

app = FastAPI(title="ODS Pipeline Dashboard")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
PG_HOST     = os.environ.get("POSTGRES_HOST", "localhost")
PG_PORT     = int(os.environ.get("POSTGRES_PORT", "5440"))
PG_DB       = os.environ.get("POSTGRES_DB", "ods_dev")
PG_USER     = os.environ.get("POSTGRES_USER", "ods")
PG_PASS     = os.environ.get("POSTGRES_PASSWORD", "ods")
BOOTSTRAP   = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CONNECT_URL = os.environ.get("KAFKA_CONNECT_URL", "http://localhost:8083")
SR_URL      = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")
ODS_TOPIC_PREFIX = "ods."

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _conn():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASS,
        cursor_factory=psycopg2.extras.RealDictCursor,
    )

def _q(conn, sql, params=()):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return [dict(r) for r in cur.fetchall()]

def _ser(obj):
    if isinstance(obj, (datetime, date)): return obj.isoformat()
    if isinstance(obj, _uuid.UUID):       return str(obj)
    if isinstance(obj, Decimal):          return float(obj)
    raise TypeError(type(obj))

def _json(data: Any) -> JSONResponse:
    return JSONResponse(content=json.loads(json.dumps(data, default=_ser)))

# ---------------------------------------------------------------------------
# Kafka helpers
# ---------------------------------------------------------------------------

def _kafka_topics() -> list[dict]:
    try:
        from confluent_kafka import Consumer, TopicPartition
        from confluent_kafka.admin import AdminClient

        admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
        meta = admin.list_topics(timeout=5)
        topics = [t for t in meta.topics if t.startswith(ODS_TOPIC_PREFIX)]

        # Get end offsets per topic
        consumer = Consumer({
            "bootstrap.servers": BOOTSTRAP,
            "group.id": "_ods_dashboard_probe",
            "auto.offset.reset": "latest",
        })

        result = []
        for topic in sorted(topics):
            partitions = list(meta.topics[topic].partitions.keys())
            tps_lo = [TopicPartition(topic, p, 0) for p in partitions]
            tps_hi = [TopicPartition(topic, p) for p in partitions]
            lo = consumer.offsets_for_times(tps_lo) if False else None  # skip TSEARCH
            hi_tps = consumer.get_watermark_offsets(TopicPartition(topic, 0), 3)
            total = 0
            partition_info = []
            for p in partitions:
                try:
                    lo_off, hi_off = consumer.get_watermark_offsets(
                        TopicPartition(topic, p), 3
                    )
                    msgs = max(0, hi_off - lo_off)
                    total += msgs
                    partition_info.append({"partition": p, "low": lo_off, "high": hi_off, "messages": msgs})
                except Exception:
                    partition_info.append({"partition": p, "low": 0, "high": 0, "messages": 0})
            result.append({
                "topic": topic,
                "partitions": len(partitions),
                "total_messages": total,
                "partition_detail": partition_info,
            })
        consumer.close()
        # Only return topics that have messages or are actively used by connectors
        try:
            active_topics = set()
            conn_resp = requests.get(f"{CONNECT_URL}/connectors?expand=info", timeout=5)
            if conn_resp.ok:
                for name, detail in conn_resp.json().items():
                    t = detail.get("info", {}).get("config", {}).get("topics", "")
                    for tp in t.split(","):
                        active_topics.add(tp.strip())
        except Exception:
            active_topics = set()
        return [r for r in result if r["total_messages"] > 0 or r["topic"] in active_topics]
    except Exception as e:
        return [{"error": str(e)}]


def _consumer_lag() -> list[dict]:
    try:
        from confluent_kafka import Consumer, TopicPartition
        from confluent_kafka.admin import AdminClient

        admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
        groups_result = admin.list_consumer_groups(request_timeout=10).result()
        groups = [g.group_id for g in groups_result.valid if not g.group_id.startswith("_")]

        meta = admin.list_topics(timeout=5)
        ods_topics = {t for t in meta.topics if t.startswith(ODS_TOPIC_PREFIX)}

        probe = Consumer({
            "bootstrap.servers": BOOTSTRAP,
            "group.id": "_ods_dashboard_probe2",
        })

        rows = []
        for group in sorted(groups):
            try:
                desc = admin.describe_consumer_groups([group], request_timeout=10)
                cg = desc[group].result()
                committed_tps = {}
                for member in cg.members:
                    for tp in (member.assignment.topic_partitions if member.assignment else []):
                        if tp.topic in ods_topics:
                            committed_tps[(tp.topic, tp.partition)] = tp

                if not committed_tps:
                    continue

                # Get committed offsets
                c2 = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": group})
                tp_list = [TopicPartition(t, p) for (t, p) in committed_tps]
                committed = c2.committed(tp_list, 5)
                c2.close()

                for tp in committed:
                    try:
                        lo, hi = probe.get_watermark_offsets(
                            TopicPartition(tp.topic, tp.partition), 3
                        )
                        committed_off = tp.offset if tp.offset >= 0 else lo
                        lag = max(0, hi - committed_off)
                        rows.append({
                            "group": group,
                            "topic": tp.topic,
                            "partition": tp.partition,
                            "committed_offset": committed_off,
                            "end_offset": hi,
                            "lag": lag,
                        })
                    except Exception:
                        pass
            except Exception:
                pass

        probe.close()
        return sorted(rows, key=lambda r: (-r["lag"], r["group"]))
    except Exception as e:
        return [{"error": str(e)}]


def _connectors() -> list[dict]:
    try:
        resp = requests.get(f"{CONNECT_URL}/connectors?expand=status&expand=info", timeout=5)
        data = resp.json()
        result = []
        for name, detail in data.items():
            status = detail.get("status", {})
            connector_status = status.get("connector", {}).get("state", "UNKNOWN")
            tasks = status.get("tasks", [])
            task_states = [t.get("state", "UNKNOWN") for t in tasks]
            cfg = detail.get("info", {}).get("config", {})
            result.append({
                "name": name,
                "state": connector_status,
                "tasks": task_states,
                "topics": cfg.get("topics", ""),
                "table": cfg.get("table.name.format", ""),
                "insert_mode": cfg.get("insert.mode", ""),
            })
        return sorted(result, key=lambda r: r["name"])
    except Exception as e:
        return [{"error": str(e)}]


def _recent_events(limit: int = 100) -> list[dict]:
    try:
        conn = _conn()
        rows = _q(conn, """
            SELECT id, run_id, event_type, pipeline_type, domain, dataset,
                   business_date, status, record_count_source, record_count_dq_pass,
                   record_count_dq_fail, record_count_target,
                   kafka_topic, kafka_offset_end, error_summary, occurred_at,
                   file_id, s3_raw_path, s3_curated_path, file_md5, kafka_offset_start
            FROM pipeline.run_events
            ORDER BY occurred_at DESC
            LIMIT %s
        """, (limit,))
        conn.close()
        return rows
    except Exception as e:
        return [{"error": str(e)}]


def _stage_log(limit: int = 300, domain: str = "", dataset: str = "",
               event_type: str = "", run_id: str = "") -> list[dict]:
    try:
        conn = _conn()
        filters, params = [], []
        if domain:     filters.append("rl.domain = %s");    params.append(domain)
        if dataset:    filters.append("rl.dataset = %s");   params.append(dataset)
        if event_type: filters.append("rsl.event_type = %s"); params.append(event_type)
        if run_id:     filters.append("rsl.run_id::text ILIKE %s"); params.append(f"%{run_id}%")
        where = ("WHERE " + " AND ".join(filters)) if filters else ""
        params.append(limit)
        rows = _q(conn, f"""
            SELECT rsl.id, rsl.run_id, rsl.stage, rsl.status, rsl.event_type,
                   rsl.attempt_number, rsl.input_ref, rsl.output_ref,
                   rsl.record_count_in, rsl.record_count_out,
                   rsl.started_at, rsl.ended_at,
                   rsl.airflow_dag_id, rsl.airflow_run_id, rsl.spark_app_id,
                   rsl.error, rl.domain, rl.dataset, rl.pipeline_type
            FROM pipeline.run_stage_log rsl
            JOIN pipeline.run_log rl ON rl.run_id = rsl.run_id
            {where}
            ORDER BY rsl.started_at DESC
            LIMIT %s
        """, params)
        conn.close()
        return rows
    except Exception as e:
        return [{"error": str(e)}]


def _lineage_edges(limit: int = 200, domain: str = "", dataset: str = "") -> list[dict]:
    try:
        conn = _conn()
        filters, params = [], []
        if domain:  filters.append("rl.domain = %s");  params.append(domain)
        if dataset: filters.append("rl.dataset = %s"); params.append(dataset)
        where = ("WHERE " + " AND ".join(filters)) if filters else ""
        params.append(limit)
        rows = _q(conn, f"""
            SELECT le.lineage_edge_id, le.consumer_run_id, le.upstream_run_id,
                   le.source_file_id, le.edge_type,
                   le.source_ref, le.target_ref, le.record_count, le.created_at,
                   rl.domain, rl.dataset, rl.pipeline_type
            FROM pipeline.lineage_edge le
            JOIN pipeline.run_log rl ON rl.run_id = le.consumer_run_id
            {where}
            ORDER BY le.created_at DESC
            LIMIT %s
        """, params)
        conn.close()
        return rows
    except Exception as e:
        return [{"error": str(e)}]


def _event_stats() -> dict:
    try:
        conn = _conn()
        total = _q(conn, "SELECT COUNT(*) AS n FROM pipeline.run_events")[0]["n"]
        today = _q(conn, "SELECT COUNT(*) AS n FROM pipeline.run_events WHERE occurred_at >= CURRENT_DATE")[0]["n"]
        failed = _q(conn, "SELECT COUNT(*) AS n FROM pipeline.run_events WHERE status='failed' AND occurred_at >= CURRENT_DATE")[0]["n"]
        domains = _q(conn, "SELECT DISTINCT domain FROM pipeline.run_events ORDER BY domain")
        conn.close()
        return {"total": total, "today": today, "failed_today": failed,
                "domains": [r["domain"] for r in domains]}
    except Exception as e:
        return {"error": str(e)}

# ---------------------------------------------------------------------------
# API endpoints
# ---------------------------------------------------------------------------

@app.get("/api/events")
async def api_events(limit: int = 100):
    return _json(_recent_events(limit))

@app.get("/api/run-events")
async def api_run_events(
    limit: int = 500,
    domain: str = "",
    status: str = "",
    pipeline_type: str = "",
    event_type: str = "",
):
    """Read all messages directly from ods.pipeline.run-events Kafka topic (Avro)."""
    try:
        from confluent_kafka import Consumer, TopicPartition
        from confluent_kafka.schema_registry import SchemaRegistryClient
        from confluent_kafka.schema_registry.avro import AvroDeserializer
        from confluent_kafka.serialization import MessageField, SerializationContext

        TOPIC = "ods.pipeline.run-events"
        sr = SchemaRegistryClient({"url": SR_URL})
        deserializer = AvroDeserializer(sr)

        # Find high-water mark across all partitions
        admin_client = __import__("confluent_kafka").admin.AdminClient({"bootstrap.servers": BOOTSTRAP})
        meta = admin_client.list_topics(timeout=5)
        if TOPIC not in meta.topics:
            return _json([])
        partitions = list(meta.topics[TOPIC].partitions.keys())

        probe = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "_ods_dash_reader"})
        assignment = [TopicPartition(TOPIC, p) for p in partitions]
        lo_hi = {p: probe.get_watermark_offsets(TopicPartition(TOPIC, p), 5) for p in partitions}
        probe.close()

        total_msgs = sum(hi - lo for lo, hi in lo_hi.values())
        if total_msgs == 0:
            return _json([])

        # Consume from beginning up to high-water mark
        c = Consumer({
            "bootstrap.servers": BOOTSTRAP,
            "group.id": "_ods_dash_reader_consume",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": "false",
        })
        c.assign([TopicPartition(TOPIC, p, lo_hi[p][0]) for p in partitions])

        rows = []
        end_offsets = {p: lo_hi[p][1] for p in partitions}
        reached = {p: lo_hi[p][0] >= lo_hi[p][1] for p in partitions}

        while not all(reached.values()) and len(rows) < limit:
            msg = c.poll(2.0)
            if msg is None:
                break
            if msg.error():
                continue
            p = msg.partition()
            if msg.offset() >= end_offsets[p] - 1:
                reached[p] = True
            try:
                val = deserializer(msg.value(), SerializationContext(TOPIC, MessageField.VALUE))
                if val:
                    rows.append(val)
            except Exception:
                pass
        c.close()

        # Apply filters
        if domain:        rows = [r for r in rows if r.get("domain") == domain]
        if status:        rows = [r for r in rows if r.get("status") == status]
        if pipeline_type: rows = [r for r in rows if r.get("pipeline_type") == pipeline_type]
        if event_type:    rows = [r for r in rows if r.get("event_type") == event_type]

        # Sort newest first
        rows.sort(key=lambda r: r.get("occurred_at", ""), reverse=True)
        return _json(rows[:limit])
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/api/topics")
async def api_topics():
    return _json(_kafka_topics())

@app.get("/api/consumers")
async def api_consumers():
    return _json(_consumer_lag())

@app.get("/api/connectors")
async def api_connectors():
    return _json(_connectors())

@app.get("/api/stats")
async def api_stats():
    return _json(_event_stats())

@app.get("/api/stage-log")
async def api_stage_log(limit: int = 300, domain: str = "", dataset: str = "",
                        event_type: str = "", run_id: str = ""):
    return _json(_stage_log(limit, domain, dataset, event_type, run_id))

@app.get("/api/lineage-edges")
async def api_lineage_edges(limit: int = 200, domain: str = "", dataset: str = ""):
    return _json(_lineage_edges(limit, domain, dataset))

# ---------------------------------------------------------------------------
# Dashboard HTML
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def dashboard():
    return HTML

# ---------------------------------------------------------------------------
# HTML template
# ---------------------------------------------------------------------------

HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1.0"/>
  <title>ODS Pipeline Dashboard</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');
    * { font-family: 'Inter', system-ui, sans-serif; }
    code, .mono { font-family: 'JetBrains Mono', monospace; }
    .gradient-header { background: linear-gradient(135deg, #0f172a 0%, #1e3a5f 50%, #0f172a 100%); }
    .card { background: white; border-radius: 16px; box-shadow: 0 1px 3px rgba(0,0,0,0.06), 0 4px 12px rgba(0,0,0,0.04); border: 1px solid #f1f5f9; }
    .tab-btn { transition: all 0.15s; cursor: pointer; }
    .tab-btn.active { background: #4f46e5; color: white; }
    .tab-btn:not(.active):hover { background: #f1f5f9; }
    .tab-panel { display: none; }
    .tab-panel.active { display: block; }
    .badge-running   { background:#d1fae5; color:#065f46; }
    .badge-failed    { background:#fee2e2; color:#991b1b; }
    .badge-paused    { background:#fef3c7; color:#92400e; }
    .badge-unassigned{ background:#e2e8f0; color:#475569; }
    .badge-succeeded { background:#d1fae5; color:#065f46; }
    .lag-high { background:#fee2e2; color:#991b1b; }
    .lag-med  { background:#fef3c7; color:#92400e; }
    .lag-ok   { background:#d1fae5; color:#065f46; }
    tr:hover td { background: #f8fafc; }
    .skeleton { background: linear-gradient(90deg, #f1f5f9 25%, #e2e8f0 50%, #f1f5f9 75%); background-size: 200% 100%; animation: shimmer 1.4s infinite; border-radius: 6px; }
    @keyframes shimmer { 0% { background-position: 200% 0; } 100% { background-position: -200% 0; } }
    .fade-in { animation: fadeIn 0.4s ease forwards; }
    @keyframes fadeIn { from { opacity:0; transform:translateY(6px); } to { opacity:1; transform:translateY(0); } }
    .pulse-dot { width:8px; height:8px; border-radius:50%; display:inline-block; }
    .pulse-green { background:#10b981; box-shadow:0 0 0 3px #d1fae5; }
    .pulse-red   { background:#ef4444; box-shadow:0 0 0 3px #fee2e2; }
    .pulse-amber { background:#f59e0b; box-shadow:0 0 0 3px #fef3c7; animation: pulse 1.5s infinite; }
    @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.4} }
    .refresh-spin { animation: spin 1s linear infinite; }
    @keyframes spin { to { transform: rotate(360deg); } }
  </style>
</head>
<body class="bg-slate-50 min-h-screen">

<!-- Header -->
<div class="gradient-header text-white px-8 py-6 shadow-lg">
  <div class="max-w-screen-2xl mx-auto flex items-center justify-between flex-wrap gap-4">
    <div>
      <div class="text-blue-300 text-xs font-semibold tracking-widest uppercase mb-1">ODS Platform</div>
      <h1 class="text-2xl font-bold">Pipeline Dashboard</h1>
      <p class="text-slate-400 text-sm mt-0.5">Run events · Kafka topics · Consumer lag · Connectors</p>
    </div>
    <div class="flex items-center gap-4">
      <div id="stats-bar" class="flex gap-6 text-sm"></div>
      <button onclick="refreshAll()" id="refresh-btn"
        class="bg-white/10 hover:bg-white/20 text-white px-4 py-2 rounded-lg text-sm font-medium flex items-center gap-2 transition">
        <span id="refresh-icon">↻</span> Refresh
      </button>
    </div>
  </div>
</div>

<!-- Tabs -->
<div class="max-w-screen-2xl mx-auto px-6 pt-6">
  <div class="flex gap-2 mb-6 flex-wrap">
    <button class="tab-btn active px-5 py-2 rounded-lg text-sm font-semibold" onclick="switchTab('events')">📋 Run Events</button>
    <button class="tab-btn px-5 py-2 rounded-lg text-sm font-semibold" onclick="switchTab('topic-events')">📨 Topic Messages</button>
    <button class="tab-btn px-5 py-2 rounded-lg text-sm font-semibold" onclick="switchTab('stage-log')">🔬 Stage Log</button>
    <button class="tab-btn px-5 py-2 rounded-lg text-sm font-semibold" onclick="switchTab('lineage-edges')">🔗 Lineage Edges</button>
    <button class="tab-btn px-5 py-2 rounded-lg text-sm font-semibold" onclick="switchTab('topics')">📡 Kafka Topics</button>
    <button class="tab-btn px-5 py-2 rounded-lg text-sm font-semibold" onclick="switchTab('consumers')">⚡ Consumer Lag</button>
    <button class="tab-btn px-5 py-2 rounded-lg text-sm font-semibold" onclick="switchTab('connectors')">🔌 Connectors</button>
  </div>

  <!-- Events tab -->
  <div id="tab-events" class="tab-panel active">
    <div class="card p-6 fade-in">
      <div class="flex items-center justify-between mb-4 flex-wrap gap-3">
        <h2 class="text-xs font-semibold text-slate-400 uppercase tracking-widest">Pipeline Run Events</h2>
        <div class="flex gap-2">
          <input id="events-filter" type="text" placeholder="Filter domain / dataset / status…"
            class="border border-slate-200 rounded-lg px-3 py-1.5 text-sm w-64 focus:outline-none focus:ring-2 focus:ring-indigo-300"
            oninput="filterEvents()"/>
        </div>
      </div>
      <div class="overflow-x-auto">
        <table class="w-full text-sm" id="events-table">
          <thead>
            <tr class="text-xs text-slate-400 uppercase tracking-wider border-b-2 border-slate-100">
              <th class="text-left pb-3 pr-4">Type</th>
              <th class="text-left pb-3 pr-4">Domain / Dataset</th>
              <th class="text-left pb-3 pr-4">Business Date</th>
              <th class="text-left pb-3 pr-4">Status</th>
              <th class="text-right pb-3 pr-4">Source</th>
              <th class="text-right pb-3 pr-4">DQ Pass</th>
              <th class="text-right pb-3 pr-4">DQ Fail</th>
              <th class="text-right pb-3 pr-4">Published</th>
              <th class="text-left pb-3 pr-4">Kafka Topic</th>
              <th class="text-right pb-3 pr-4">Offset End</th>
              <th class="text-left pb-3 pr-4">File ID</th>
              <th class="text-left pb-3">Occurred</th>
            </tr>
          </thead>
          <tbody id="events-body">
            <tr><td colspan="12" class="py-8 text-center text-slate-300 text-sm">Loading…</td></tr>
          </tbody>
        </table>
      </div>
    </div>
  </div>

  <!-- Topic Messages tab (ods.pipeline.run-events) -->
  <div id="tab-topic-events" class="tab-panel">
    <div class="card p-6 fade-in">
      <div class="flex items-center justify-between mb-4 flex-wrap gap-3">
        <h2 class="text-xs font-semibold text-slate-400 uppercase tracking-widest">ods.pipeline.run-events — All Messages</h2>
        <div class="flex gap-2 flex-wrap">
          <select id="te-domain" class="border border-slate-200 rounded-lg px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-indigo-300" onchange="loadTopicEvents()">
            <option value="">All domains</option>
          </select>
          <select id="te-pipeline" class="border border-slate-200 rounded-lg px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-indigo-300" onchange="loadTopicEvents()">
            <option value="">All pipeline types</option>
            <option value="ingestion">ingestion</option>
            <option value="publish">publish</option>
          </select>
          <select id="te-status" class="border border-slate-200 rounded-lg px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-indigo-300" onchange="loadTopicEvents()">
            <option value="">All statuses</option>
            <option value="succeeded">succeeded</option>
            <option value="failed">failed</option>
            <option value="partial">partial</option>
          </select>
          <input id="te-limit" type="number" value="500" min="10" max="5000" step="10"
            class="border border-slate-200 rounded-lg px-3 py-1.5 text-sm w-24 focus:outline-none focus:ring-2 focus:ring-indigo-300"
            onchange="loadTopicEvents()" title="Max rows"/>
        </div>
      </div>
      <div class="overflow-x-auto">
        <table class="w-full text-sm" id="te-table">
          <thead>
            <tr class="text-xs text-slate-400 uppercase tracking-wider border-b-2 border-slate-100">
              <th class="text-left pb-3 pr-4">Event Type</th>
              <th class="text-left pb-3 pr-4">Pipeline</th>
              <th class="text-left pb-3 pr-4">Domain / Dataset</th>
              <th class="text-left pb-3 pr-4">Business Date</th>
              <th class="text-left pb-3 pr-4">Status</th>
              <th class="text-right pb-3 pr-4">Source</th>
              <th class="text-right pb-3 pr-4">DQ Pass</th>
              <th class="text-right pb-3 pr-4">DQ Fail</th>
              <th class="text-right pb-3 pr-4">Published</th>
              <th class="text-left pb-3 pr-4">Kafka Topic</th>
              <th class="text-right pb-3 pr-4">Offset</th>
              <th class="text-left pb-3 pr-4">File ID</th>
              <th class="text-left pb-3 pr-4">Error</th>
              <th class="text-left pb-3">Occurred</th>
            </tr>
          </thead>
          <tbody id="te-body">
            <tr><td colspan="14" class="py-8 text-center text-slate-300 text-sm">Loading…</td></tr>
          </tbody>
        </table>
      </div>
      <div id="te-count" class="mt-3 text-xs text-slate-400 text-right"></div>
    </div>
  </div>

  <!-- Stage Log tab -->
  <div id="tab-stage-log" class="tab-panel">
    <div class="card p-6 fade-in">
      <div class="flex items-center justify-between mb-4 flex-wrap gap-3">
        <h2 class="text-xs font-semibold text-slate-400 uppercase tracking-widest">Pipeline Stage Log</h2>
        <div class="flex gap-2 flex-wrap">
          <select id="sl-event-type" class="border border-slate-200 rounded-lg px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-indigo-300" onchange="loadStageLog()">
            <option value="">All event types</option>
            <option value="stage_completed">stage_completed</option>
            <option value="stage_failed">stage_failed</option>
            <option value="stage_started">stage_started</option>
            <option value="stage_warned">stage_warned</option>
            <option value="stage_skipped">stage_skipped</option>
          </select>
          <input id="sl-run-id" type="text" placeholder="Run ID fragment…"
            class="border border-slate-200 rounded-lg px-3 py-1.5 text-sm w-52 focus:outline-none focus:ring-2 focus:ring-indigo-300"
            oninput="loadStageLog()"/>
          <input id="sl-limit" type="number" value="300" min="10" max="2000" step="50"
            class="border border-slate-200 rounded-lg px-3 py-1.5 text-sm w-24 focus:outline-none focus:ring-2 focus:ring-indigo-300"
            onchange="loadStageLog()" title="Max rows"/>
        </div>
      </div>
      <div class="overflow-x-auto">
        <table class="w-full text-sm" id="sl-table">
          <thead>
            <tr class="text-xs text-slate-400 uppercase tracking-wider border-b-2 border-slate-100">
              <th class="text-left pb-3 pr-4">Domain / Dataset</th>
              <th class="text-left pb-3 pr-4">Stage</th>
              <th class="text-left pb-3 pr-4">Event Type</th>
              <th class="text-left pb-3 pr-4">Status</th>
              <th class="text-right pb-3 pr-4">In</th>
              <th class="text-right pb-3 pr-4">Out</th>
              <th class="text-right pb-3 pr-4">Duration</th>
              <th class="text-left pb-3 pr-4">Airflow</th>
              <th class="text-left pb-3 pr-4">Spark App</th>
              <th class="text-left pb-3 pr-4">Run ID</th>
              <th class="text-left pb-3">Started</th>
            </tr>
          </thead>
          <tbody id="sl-body">
            <tr><td colspan="11" class="py-8 text-center text-slate-300 text-sm">Loading…</td></tr>
          </tbody>
        </table>
      </div>
      <div id="sl-count" class="mt-3 text-xs text-slate-400 text-right"></div>
    </div>
  </div>

  <!-- Lineage Edges tab -->
  <div id="tab-lineage-edges" class="tab-panel">
    <div class="card p-6 fade-in">
      <div class="flex items-center justify-between mb-4 flex-wrap gap-3">
        <h2 class="text-xs font-semibold text-slate-400 uppercase tracking-widest">Lineage Edges</h2>
        <input id="le-limit" type="number" value="200" min="10" max="2000" step="50"
          class="border border-slate-200 rounded-lg px-3 py-1.5 text-sm w-24 focus:outline-none focus:ring-2 focus:ring-indigo-300"
          onchange="loadLineageEdges()" title="Max rows"/>
      </div>
      <div class="overflow-x-auto">
        <table class="w-full text-sm" id="le-table">
          <thead>
            <tr class="text-xs text-slate-400 uppercase tracking-wider border-b-2 border-slate-100">
              <th class="text-left pb-3 pr-4">Domain / Dataset</th>
              <th class="text-left pb-3 pr-4">Edge Type</th>
              <th class="text-left pb-3 pr-4">Source Ref</th>
              <th class="text-left pb-3 pr-4">Target Ref</th>
              <th class="text-right pb-3 pr-4">Records</th>
              <th class="text-left pb-3 pr-4">File ID</th>
              <th class="text-left pb-3">Created</th>
            </tr>
          </thead>
          <tbody id="le-body">
            <tr><td colspan="7" class="py-8 text-center text-slate-300 text-sm">Loading…</td></tr>
          </tbody>
        </table>
      </div>
      <div id="le-count" class="mt-3 text-xs text-slate-400 text-right"></div>
    </div>
  </div>

  <!-- Topics tab -->
  <div id="tab-topics" class="tab-panel">
    <div class="card p-6 fade-in">
      <h2 class="text-xs font-semibold text-slate-400 uppercase tracking-widest mb-5">Kafka Topics (ods.*)</h2>
      <div id="topics-content">
        <div class="skeleton h-12 w-full mb-2"></div>
        <div class="skeleton h-12 w-full mb-2"></div>
      </div>
    </div>
  </div>

  <!-- Consumers tab -->
  <div id="tab-consumers" class="tab-panel">
    <div class="card p-6 fade-in">
      <h2 class="text-xs font-semibold text-slate-400 uppercase tracking-widest mb-5">Consumer Group Lag</h2>
      <div id="consumers-content">
        <div class="skeleton h-12 w-full mb-2"></div>
        <div class="skeleton h-12 w-full mb-2"></div>
      </div>
    </div>
  </div>

  <!-- Connectors tab -->
  <div id="tab-connectors" class="tab-panel">
    <div class="card p-6 fade-in">
      <h2 class="text-xs font-semibold text-slate-400 uppercase tracking-widest mb-5">Kafka Connect Connectors</h2>
      <div id="connectors-content">
        <div class="skeleton h-12 w-full mb-2"></div>
        <div class="skeleton h-12 w-full mb-2"></div>
      </div>
    </div>
  </div>

  <div class="text-xs text-slate-300 text-right mt-4 pb-6" id="last-refresh"></div>
</div>

<script>
let _eventsData = [];
let _activeTab = 'events';

function fmt(n) { return n === null || n === undefined ? '—' : Number(n).toLocaleString(); }
function fmtDt(s) { if(!s) return '—'; const d=new Date(s); return d.toLocaleString('en-GB',{dateStyle:'short',timeStyle:'medium'}); }
function truncate(s,n=40){ if(!s) return '—'; return s.length<=n ? s : '…'+s.slice(-(n-1)); }

function statusBadge(s) {
  const m = {succeeded:'badge-succeeded',failed:'badge-failed',running:'badge-running',paused:'badge-paused'};
  const cls = m[s] || 'badge-unassigned';
  return `<span class="inline-flex px-2 py-0.5 rounded-full text-xs font-semibold ${cls}">${s||'—'}</span>`;
}

function lagBadge(lag) {
  const n = Number(lag);
  const cls = n > 1000 ? 'lag-high' : n > 100 ? 'lag-med' : 'lag-ok';
  return `<span class="inline-flex px-2 py-0.5 rounded-full text-xs font-semibold ${cls}">${fmt(n)}</span>`;
}

function pipelineIcon(t) {
  return t === 'ingestion' ? '⚙️' : t === 'publish' ? '📡' : '▶';
}

// ── Events ──
async function loadEvents() {
  const r = await fetch('/api/events?limit=200');
  _eventsData = await r.json();
  renderEvents(_eventsData);
}

// ── Topic Messages (ods.pipeline.run-events) ──
async function loadTopicEvents() {
  const domain   = document.getElementById('te-domain').value;
  const pipeline = document.getElementById('te-pipeline').value;
  const status   = document.getElementById('te-status').value;
  const limit    = document.getElementById('te-limit').value || 500;
  const params = new URLSearchParams({limit});
  if (domain)   params.set('domain', domain);
  if (pipeline) params.set('pipeline_type', pipeline);
  if (status)   params.set('status', status);
  const r = await fetch('/api/run-events?' + params);
  const data = await r.json();
  const tbody = document.getElementById('te-body');
  if (!data.length || data[0]?.error) {
    tbody.innerHTML = `<tr><td colspan="14" class="py-8 text-center text-slate-300 text-sm">${data[0]?.error||'No events found.'}</td></tr>`;
    return;
  }
  tbody.innerHTML = data.map(e => `
    <tr class="border-b border-slate-50 hover:bg-slate-50 transition-colors">
      <td class="py-2 pr-4 text-xs font-mono text-slate-500">${e.event_type||'—'}</td>
      <td class="py-2 pr-4">${pipelineIcon(e.pipeline_type)} <span class="text-xs text-slate-500">${e.pipeline_type||'—'}</span></td>
      <td class="py-2 pr-4 font-medium">${e.domain||'—'}<span class="text-slate-400"> / ${e.dataset||'—'}</span></td>
      <td class="py-2 pr-4 text-xs text-slate-500">${e.business_date||'—'}</td>
      <td class="py-2 pr-4">${statusBadge(e.status)}</td>
      <td class="py-2 pr-4 text-right mono text-xs">${fmt(e.record_count_source)}</td>
      <td class="py-2 pr-4 text-right mono text-xs text-green-600">${fmt(e.record_count_dq_pass)}</td>
      <td class="py-2 pr-4 text-right mono text-xs ${e.record_count_dq_fail>0?'text-red-500':''}">${fmt(e.record_count_dq_fail)}</td>
      <td class="py-2 pr-4 text-right mono text-xs">${fmt(e.record_count_target)}</td>
      <td class="py-2 pr-4 text-xs text-slate-400">${e.kafka_topic||'—'}</td>
      <td class="py-2 pr-4 text-right mono text-xs text-slate-400">${fmt(e.kafka_offset_start||e.kafka_offset_end)}</td>
      <td class="py-2 pr-4 mono text-xs">${e.file_id ? `<a href="http://localhost:8888/lineage/${e.file_id}" target="_blank" class="text-indigo-500 hover:text-indigo-700" title="${e.file_id}">${e.file_id.slice(0,8)}…</a>` : '—'}</td>
      <td class="py-2 pr-4 text-xs text-red-400 max-w-xs truncate" title="${e.error_summary||''}">${truncate(e.error_summary,50)}</td>
      <td class="py-2 text-xs text-slate-400 whitespace-nowrap">${fmtDt(e.occurred_at)}</td>
    </tr>`).join('');
  document.getElementById('te-count').textContent = `Showing ${data.length} events`;
}

function filterEvents() {
  const q = document.getElementById('events-filter').value.toLowerCase();
  const filtered = q ? _eventsData.filter(e =>
    (e.domain||'').toLowerCase().includes(q) ||
    (e.dataset||'').toLowerCase().includes(q) ||
    (e.status||'').toLowerCase().includes(q) ||
    (e.event_type||'').toLowerCase().includes(q)
  ) : _eventsData;
  renderEvents(filtered);
}

function renderEvents(data) {
  if (!data.length || data[0]?.error) {
    document.getElementById('events-body').innerHTML =
      `<tr><td colspan="12" class="py-8 text-center text-slate-300">${data[0]?.error || 'No events yet — run the pipeline first.'}</td></tr>`;
    return;
  }
  document.getElementById('events-body').innerHTML = data.map(e => `
    <tr class="border-b border-slate-50 hover:bg-slate-50 transition-colors align-middle">
      <td class="py-3 pr-4">${pipelineIcon(e.pipeline_type)} <span class="text-xs text-slate-500">${e.pipeline_type||e.event_type||'—'}</span></td>
      <td class="py-3 pr-4 font-medium text-slate-800">${e.domain} <span class="text-slate-400">/</span> ${e.dataset}</td>
      <td class="py-3 pr-4 mono text-xs text-slate-500">${e.business_date||'—'}</td>
      <td class="py-3 pr-4">${statusBadge(e.status)}</td>
      <td class="py-3 pr-4 text-right text-slate-600">${fmt(e.record_count_source)}</td>
      <td class="py-3 pr-4 text-right text-emerald-600 font-medium">${fmt(e.record_count_dq_pass)}</td>
      <td class="py-3 pr-4 text-right text-red-500">${fmt(e.record_count_dq_fail)}</td>
      <td class="py-3 pr-4 text-right text-indigo-600 font-semibold">${fmt(e.record_count_target)}</td>
      <td class="py-3 pr-4 mono text-xs text-slate-400">${truncate(e.kafka_topic,35)||'—'}</td>
      <td class="py-3 pr-4 text-right mono text-xs text-slate-400">${fmt(e.kafka_offset_end)}</td>
      <td class="py-3 pr-4 mono text-xs">${e.file_id ? `<a href="http://localhost:8888/lineage/${e.file_id}" target="_blank" class="text-indigo-500 hover:text-indigo-700" title="${e.file_id}">${e.file_id.slice(0,8)}…</a>` : '—'}</td>
      <td class="py-3 text-xs text-slate-400 whitespace-nowrap">${fmtDt(e.occurred_at)}</td>
    </tr>
  `).join('');
}

// ── Stage Log ──
async function loadStageLog() {
  const eventType = document.getElementById('sl-event-type').value;
  const runId     = document.getElementById('sl-run-id').value;
  const limit     = document.getElementById('sl-limit').value || 300;
  const params = new URLSearchParams({limit});
  if (eventType) params.set('event_type', eventType);
  if (runId)     params.set('run_id', runId);
  const r = await fetch('/api/stage-log?' + params);
  const data = await r.json();
  const tbody = document.getElementById('sl-body');
  if (!data.length || data[0]?.error) {
    tbody.innerHTML = `<tr><td colspan="11" class="py-8 text-center text-slate-300 text-sm">${data[0]?.error||'No stage rows found.'}</td></tr>`;
    document.getElementById('sl-count').textContent = '';
    return;
  }
  const stageIcon = {
    raw_read:'📥', schema_validate:'🧬', dq_check:'🔬',
    curated_write:'📦', curated_read:'📂', kafka_publish:'📤',
    recon_t0:'⚖️', sink_pg_wait:'🐘', sink_s3_wait:'☁️', finalise:'✅',
  };
  const etBadge = t => {
    if (!t) return '—';
    const m = {
      stage_completed:'bg-emerald-50 text-emerald-700',
      stage_failed:'bg-red-50 text-red-700',
      stage_started:'bg-amber-50 text-amber-700',
      stage_skipped:'bg-slate-100 text-slate-500',
      stage_warned:'bg-orange-50 text-orange-700',
    };
    return `<span class="inline-flex px-2 py-0.5 rounded-full text-xs font-semibold ${m[t]||'bg-slate-100 text-slate-500'}">${t.replace('stage_','')}</span>`;
  };
  const fmtDur = (a, b) => {
    if (!a || !b) return '—';
    const ms = new Date(b) - new Date(a);
    return ms < 1000 ? ms+'ms' : (ms/1000).toFixed(1)+'s';
  };
  tbody.innerHTML = data.map(s => `
    <tr class="border-b border-slate-50 hover:bg-slate-50 transition-colors text-xs">
      <td class="py-2 pr-4 font-medium">${s.domain||'—'} <span class="text-slate-400">/</span> ${s.dataset||'—'}</td>
      <td class="py-2 pr-4 whitespace-nowrap">${stageIcon[s.stage]||'◆'} ${s.stage}</td>
      <td class="py-2 pr-4">${etBadge(s.event_type)}</td>
      <td class="py-2 pr-4">${statusBadge(s.status)}</td>
      <td class="py-2 pr-4 text-right mono">${fmt(s.record_count_in)}</td>
      <td class="py-2 pr-4 text-right mono text-emerald-600">${fmt(s.record_count_out)}</td>
      <td class="py-2 pr-4 text-right mono text-slate-400">${fmtDur(s.started_at, s.ended_at)}</td>
      <td class="py-2 pr-4">${s.airflow_dag_id
        ? `<a href="/dags/${s.airflow_dag_id}/dagRuns/${encodeURIComponent(s.airflow_run_id||'')}" target="_blank" class="text-sky-500 hover:text-sky-700">${s.airflow_dag_id}</a>`
        : '—'}</td>
      <td class="py-2 pr-4 mono text-slate-400" title="${s.spark_app_id||''}">${s.spark_app_id ? s.spark_app_id.slice(0,18)+'…' : '—'}</td>
      <td class="py-2 pr-4 mono text-slate-400" title="${s.run_id}">${String(s.run_id).slice(0,8)}…</td>
      <td class="py-2 text-slate-400 whitespace-nowrap">${fmtDt(s.started_at)}</td>
    </tr>`).join('');
  document.getElementById('sl-count').textContent = `Showing ${data.length} stage rows`;
}

// ── Lineage Edges ──
async function loadLineageEdges() {
  const limit = document.getElementById('le-limit').value || 200;
  const r = await fetch(`/api/lineage-edges?limit=${limit}`);
  const data = await r.json();
  const tbody = document.getElementById('le-body');
  if (!data.length || data[0]?.error) {
    tbody.innerHTML = `<tr><td colspan="7" class="py-8 text-center text-slate-300 text-sm">${data[0]?.error||'No lineage edges found.'}</td></tr>`;
    document.getElementById('le-count').textContent = '';
    return;
  }
  const edgeColor = {
    raw_to_curated:'text-indigo-600', curated_to_kafka:'text-purple-600', curated_to_postgres:'text-teal-600',
  };
  const edgeIcon = { raw_to_curated:'📥→📦', curated_to_kafka:'📦→📡', curated_to_postgres:'📦→🐘' };
  tbody.innerHTML = data.map(e => `
    <tr class="border-b border-slate-50 hover:bg-slate-50 text-xs">
      <td class="py-2 pr-4 font-medium">${e.domain||'—'} <span class="text-slate-400">/</span> ${e.dataset||'—'}</td>
      <td class="py-2 pr-4 font-semibold ${edgeColor[e.edge_type]||''}">${edgeIcon[e.edge_type]||'→'} ${e.edge_type}</td>
      <td class="py-2 pr-4 mono text-slate-400">${truncate(e.source_ref,45)}</td>
      <td class="py-2 pr-4 mono text-slate-400">${truncate(e.target_ref,45)}</td>
      <td class="py-2 pr-4 text-right font-semibold text-slate-700">${fmt(e.record_count)}</td>
      <td class="py-2 pr-4 mono">${e.source_file_id
        ? `<a href="http://localhost:8888/lineage/${e.source_file_id}" target="_blank" class="text-indigo-500 hover:text-indigo-700" title="${e.source_file_id}">${e.source_file_id.slice(0,8)}…</a>`
        : '—'}</td>
      <td class="py-2 text-slate-400 whitespace-nowrap">${fmtDt(e.created_at)}</td>
    </tr>`).join('');
  document.getElementById('le-count').textContent = `Showing ${data.length} edges`;
}

// ── Topics ──
async function loadTopics() {
  const r = await fetch('/api/topics');
  const data = await r.json();
  if (!data.length || data[0]?.error) {
    document.getElementById('topics-content').innerHTML = `<p class="text-slate-400 text-sm">${data[0]?.error||'No topics found.'}</p>`;
    return;
  }
  document.getElementById('topics-content').innerHTML = `
    <table class="w-full text-sm">
      <thead><tr class="text-xs text-slate-400 uppercase tracking-wider border-b-2 border-slate-100">
        <th class="text-left pb-3 pr-6">Topic</th>
        <th class="text-right pb-3 pr-6">Partitions</th>
        <th class="text-right pb-3 pr-6">Total Messages</th>
        <th class="text-left pb-3">Partition Detail</th>
      </tr></thead>
      <tbody>${data.map(t => `
        <tr class="border-b border-slate-50 hover:bg-slate-50 align-top">
          <td class="py-3 pr-6 mono text-xs font-medium text-slate-700">${t.topic}</td>
          <td class="py-3 pr-6 text-right text-slate-600">${t.partitions}</td>
          <td class="py-3 pr-6 text-right font-bold text-indigo-700">${fmt(t.total_messages)}</td>
          <td class="py-3 text-xs text-slate-400">${(t.partition_detail||[]).map(p=>
            `<span class="mr-3">p${p.partition}: <span class="text-slate-600 font-medium">${fmt(p.messages)}</span> msgs (${fmt(p.low)}…${fmt(p.high)})</span>`
          ).join('')}</td>
        </tr>`).join('')}
      </tbody>
    </table>`;
}

// ── Consumer Lag ──
async function loadConsumers() {
  const r = await fetch('/api/consumers');
  const data = await r.json();
  if (!data.length || data[0]?.error) {
    document.getElementById('consumers-content').innerHTML = `<p class="text-slate-400 text-sm">${data[0]?.error||'No consumer groups found.'}</p>`;
    return;
  }
  document.getElementById('consumers-content').innerHTML = `
    <table class="w-full text-sm">
      <thead><tr class="text-xs text-slate-400 uppercase tracking-wider border-b-2 border-slate-100">
        <th class="text-left pb-3 pr-6">Consumer Group</th>
        <th class="text-left pb-3 pr-6">Topic</th>
        <th class="text-right pb-3 pr-4">Partition</th>
        <th class="text-right pb-3 pr-4">Committed</th>
        <th class="text-right pb-3 pr-4">End Offset</th>
        <th class="text-right pb-3">Lag</th>
      </tr></thead>
      <tbody>${data.map(r => `
        <tr class="border-b border-slate-50 hover:bg-slate-50">
          <td class="py-3 pr-6 mono text-xs text-slate-600">${r.group||'—'}</td>
          <td class="py-3 pr-6 mono text-xs text-slate-500">${r.topic||'—'}</td>
          <td class="py-3 pr-4 text-right text-slate-500">${r.partition}</td>
          <td class="py-3 pr-4 text-right mono text-xs text-slate-400">${fmt(r.committed_offset)}</td>
          <td class="py-3 pr-4 text-right mono text-xs text-slate-400">${fmt(r.end_offset)}</td>
          <td class="py-3 text-right">${lagBadge(r.lag)}</td>
        </tr>`).join('')}
      </tbody>
    </table>`;
}

// ── Connectors ──
async function loadConnectors() {
  const r = await fetch('/api/connectors');
  const data = await r.json();
  if (!data.length || data[0]?.error) {
    document.getElementById('connectors-content').innerHTML = `<p class="text-slate-400 text-sm">${data[0]?.error||'No connectors found.'}</p>`;
    return;
  }
  document.getElementById('connectors-content').innerHTML = `
    <table class="w-full text-sm">
      <thead><tr class="text-xs text-slate-400 uppercase tracking-wider border-b-2 border-slate-100">
        <th class="text-left pb-3 pr-6">Connector</th>
        <th class="text-left pb-3 pr-4">State</th>
        <th class="text-left pb-3 pr-4">Tasks</th>
        <th class="text-left pb-3 pr-6">Source Topic</th>
        <th class="text-left pb-3 pr-6">Target Table</th>
        <th class="text-left pb-3">Insert Mode</th>
      </tr></thead>
      <tbody>${data.map(c => {
        const allOk = c.tasks && c.tasks.every(t => t==='RUNNING');
        const taskHtml = (c.tasks||[]).map((t,i)=>`<span class="mr-1 ${t==='RUNNING'?'text-emerald-600':'text-red-500'} font-medium">T${i}:${t}</span>`).join('');
        return `
        <tr class="border-b border-slate-50 hover:bg-slate-50">
          <td class="py-3 pr-6 font-medium text-slate-700 mono text-xs">${c.name}</td>
          <td class="py-3 pr-4">${statusBadge(c.state?.toLowerCase())}</td>
          <td class="py-3 pr-4 text-xs">${taskHtml||'—'}</td>
          <td class="py-3 pr-6 mono text-xs text-slate-500">${c.topics||'—'}</td>
          <td class="py-3 pr-6 mono text-xs text-slate-500">${c.table||'—'}</td>
          <td class="py-3 mono text-xs text-indigo-500 font-medium">${c.insert_mode||'—'}</td>
        </tr>`;
      }).join('')}
      </tbody>
    </table>`;
}

// ── Stats bar ──
async function loadStats() {
  const r = await fetch('/api/stats');
  const s = await r.json();
  if (s.error) return;
  document.getElementById('stats-bar').innerHTML = `
    <div class="text-center"><div class="text-xl font-bold">${fmt(s.today)}</div><div class="text-blue-300 text-xs">Events today</div></div>
    <div class="text-center"><div class="text-xl font-bold ${s.failed_today>0?'text-red-400':''}">${fmt(s.failed_today)}</div><div class="text-blue-300 text-xs">Failed today</div></div>
    <div class="text-center"><div class="text-xl font-bold">${fmt(s.total)}</div><div class="text-blue-300 text-xs">Total events</div></div>
  `;
}

// ── Tab switching ──
function switchTab(name) {
  _activeTab = name;
  document.querySelectorAll('.tab-btn').forEach((b,i) => {
    const tabs = ['events','topic-events','stage-log','lineage-edges','topics','consumers','connectors'];
    b.classList.toggle('active', tabs[i]===name);
  });
  document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
  document.getElementById('tab-'+name).classList.add('active');
  if      (name==='topic-events')   loadTopicEvents();
  else if (name==='stage-log')      loadStageLog();
  else if (name==='lineage-edges')  loadLineageEdges();
  else if (name==='topics')         loadTopics();
  else if (name==='consumers')      loadConsumers();
  else if (name==='connectors')     loadConnectors();
}

// ── Refresh ──
async function refreshAll() {
  const icon = document.getElementById('refresh-icon');
  icon.classList.add('refresh-spin');
  await Promise.all([loadStats(), loadEvents()]);
  if      (_activeTab==='topic-events')  await loadTopicEvents();
  else if (_activeTab==='stage-log')     await loadStageLog();
  else if (_activeTab==='lineage-edges') await loadLineageEdges();
  else if (_activeTab==='topics')        await loadTopics();
  else if (_activeTab==='consumers')     await loadConsumers();
  else if (_activeTab==='connectors')    await loadConnectors();
  // Populate domain dropdown from stats
  try {
    const sr = await fetch('/api/stats'); const ss = await sr.json();
    const sel = document.getElementById('te-domain');
    if (ss.domains && sel.options.length <= 1) {
      ss.domains.forEach(d => { const o=document.createElement('option'); o.value=d; o.text=d; sel.appendChild(o); });
    }
  } catch(e) {}
  icon.classList.remove('refresh-spin');
  document.getElementById('last-refresh').textContent =
    'Last refreshed: ' + new Date().toLocaleTimeString('en-GB');
}

// ── Auto-refresh every 30s ──
setInterval(refreshAll, 30000);

// ── Init ──
refreshAll();
</script>
</body>
</html>
"""


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("scripts.pipeline_dashboard:app", host="0.0.0.0", port=8900, reload=True)
