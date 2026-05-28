#!/usr/bin/env python3
"""
ODS Data Lineage Viewer

Usage:
    pip install fastapi uvicorn psycopg2-binary
    uvicorn scripts.lineage_viewer:app --port 8888 --reload
    Open: http://localhost:8888/lineage/69c53df0-3698-4a4d-9fbd-7702256349b4
"""
from __future__ import annotations

import os
import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse

app = FastAPI(title="ODS Lineage Viewer")


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _conn():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", "5440")),
        dbname=os.environ.get("POSTGRES_DB", "ods_dev"),
        user=os.environ.get("POSTGRES_USER", "ods"),
        password=os.environ.get("POSTGRES_PASSWORD", "ods"),
        cursor_factory=psycopg2.extras.RealDictCursor,
    )


def _q(conn, sql: str, params=()) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    return [dict(r) for r in rows]


def _scalar(conn, sql: str, params=()) -> Any:
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
    return list(row)[0] if row else None


def _serialize(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, uuid.UUID):
        return str(obj)
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Not serializable: {type(obj)}")


# ---------------------------------------------------------------------------
# API endpoint
# ---------------------------------------------------------------------------

@app.get("/api/lineage/{file_id}")
async def lineage_api(file_id: str):
    try:
        conn = _conn()
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"DB connection failed: {e}")

    try:
        fc = _q(conn, "SELECT * FROM pipeline.file_catalogue WHERE file_id = %s", (file_id,))
        if not fc:
            raise HTTPException(status_code=404, detail=f"file_id {file_id!r} not found")
        fc = fc[0]

        runs = _q(conn,
            "SELECT * FROM pipeline.run_log WHERE file_id = %s ORDER BY started_at",
            (file_id,))

        run_ids = [str(r["run_id"]) for r in runs]
        stages: list[dict] = []
        if run_ids:
            placeholders = ",".join(["%s"] * len(run_ids))
            stages = _q(conn,
                f"SELECT rsl.*, rl.pipeline_type FROM pipeline.run_stage_log rsl "
                f"JOIN pipeline.run_log rl ON rl.run_id = rsl.run_id "
                f"WHERE rsl.run_id IN ({placeholders}) ORDER BY rsl.started_at",
                run_ids)

        # Lineage edges for all runs of this file
        edges: list[dict] = []
        if run_ids:
            placeholders = ",".join(["%s"] * len(run_ids))
            edges = _q(conn,
                f"SELECT * FROM pipeline.lineage_edge "
                f"WHERE consumer_run_id IN ({placeholders}) ORDER BY created_at",
                run_ids)

        policy_count = _scalar(conn,
            "SELECT COUNT(*) FROM ods.insurance_policy WHERE _ods_file_id = %s", (file_id,)) or 0
        history_count = _scalar(conn,
            "SELECT COUNT(*) FROM ods.insurance_policy_history WHERE _ods_file_id = %s", (file_id,)) or 0

        import json
        return JSONResponse(content=json.loads(json.dumps({
            "file": fc,
            "runs": runs,
            "stages": stages,
            "edges": edges,
            "affected": {
                "insurance_policy": int(policy_count),
                "insurance_policy_history": int(history_count),
            },
        }, default=_serialize)))
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# HTML page
# ---------------------------------------------------------------------------

@app.get("/lineage/{file_id}", response_class=HTMLResponse)
async def lineage_page(file_id: str):
    return HTML.replace("__FILE_ID__", file_id)


# ---------------------------------------------------------------------------
# HTML template
# ---------------------------------------------------------------------------

HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>ODS Lineage &mdash; __FILE_ID__</title>
  <script src="https://cdn.tailwindcss.com" defer></script>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');
    * { font-family: 'Inter', system-ui, sans-serif; }
    code, .mono { font-family: 'JetBrains Mono', monospace; }

    .gradient-header {
      background: linear-gradient(135deg, #4f46e5 0%, #7c3aed 50%, #a855f7 100%);
    }

    .pipeline-node {
      position: relative;
      transition: transform 0.2s, box-shadow 0.2s;
    }
    .pipeline-node:hover {
      transform: translateY(-3px);
      box-shadow: 0 12px 24px -4px rgba(79,70,229,0.18);
    }
    .pipeline-node.has-arrow::after {
      content: '';
      position: absolute;
      top: 50%;
      right: -28px;
      transform: translateY(-50%);
      width: 24px;
      height: 2px;
      background: #c7d2fe;
      z-index: 10;
    }
    .pipeline-node.has-arrow::before {
      content: '▶';
      position: absolute;
      top: 50%;
      right: -20px;
      transform: translateY(-50%) translateX(4px);
      font-size: 10px;
      color: #818cf8;
      z-index: 11;
    }
    .fork-container {
      display: flex;
      flex-direction: column;
      gap: 10px;
      position: relative;
    }
    .fork-container::before {
      content: '';
      position: absolute;
      left: -20px;
      top: 50%;
      transform: translateY(-50%);
      width: 2px;
      height: calc(100% - 20px);
      background: #c7d2fe;
    }
    .fork-branch {
      position: relative;
    }
    .fork-branch::before {
      content: '▶';
      position: absolute;
      left: -16px;
      top: 50%;
      transform: translateY(-50%);
      font-size: 10px;
      color: #818cf8;
    }

    .status-dot {
      width: 8px; height: 8px; border-radius: 50%;
      display: inline-block; margin-right: 5px;
    }
    .dot-success { background: #10b981; box-shadow: 0 0 0 3px #d1fae5; }
    .dot-failed  { background: #ef4444; box-shadow: 0 0 0 3px #fee2e2; }
    .dot-running { background: #f59e0b; box-shadow: 0 0 0 3px #fef3c7; animation: pulse 1.5s infinite; }
    .dot-skipped { background: #94a3b8; box-shadow: 0 0 0 3px #e2e8f0; }

    @keyframes pulse {
      0%, 100% { opacity: 1; }
      50% { opacity: 0.4; }
    }

    .badge {
      display: inline-flex; align-items: center; padding: 2px 10px;
      border-radius: 9999px; font-size: 12px; font-weight: 600;
    }
    .badge-success { background: #d1fae5; color: #065f46; }
    .badge-failed  { background: #fee2e2; color: #991b1b; }
    .badge-running { background: #fef3c7; color: #92400e; }
    .badge-superseded { background: #ede9fe; color: #4c1d95; }
    .badge-curated { background: #dbeafe; color: #1e40af; }
    .badge-published { background: #d1fae5; color: #065f46; }

    .card {
      background: white;
      border-radius: 16px;
      box-shadow: 0 1px 3px rgba(0,0,0,0.06), 0 4px 12px rgba(0,0,0,0.04);
      border: 1px solid #f1f5f9;
    }

    .run-row:hover { background: #f8fafc; }

    .progress-bar-bg { background: #e2e8f0; border-radius: 9999px; height: 8px; overflow: hidden; }
    .progress-bar-fill { border-radius: 9999px; height: 100%; transition: width 1s ease; }

    .skeleton { background: linear-gradient(90deg, #f1f5f9 25%, #e2e8f0 50%, #f1f5f9 75%);
      background-size: 200% 100%; animation: shimmer 1.4s infinite; border-radius: 6px; }
    @keyframes shimmer { 0% { background-position: 200% 0; } 100% { background-position: -200% 0; } }

    .fade-in { animation: fadeIn 0.5s ease forwards; }
    @keyframes fadeIn { from { opacity: 0; transform: translateY(8px); } to { opacity: 1; transform: translateY(0); } }

    .node-icon { font-size: 28px; line-height: 1; }
    .count-num { font-size: 22px; font-weight: 700; color: #1e293b; font-variant-numeric: tabular-nums; }
  </style>
</head>
<body class="bg-slate-50 min-h-screen">

<!-- Header -->
<div class="gradient-header text-white px-8 py-7 shadow-lg">
  <div class="max-w-screen-xl mx-auto">
    <div class="flex items-start justify-between flex-wrap gap-4">
      <div>
        <div class="flex items-center gap-3 mb-1">
          <span class="text-indigo-200 text-sm font-medium tracking-widest uppercase">ODS Data Lineage</span>
          <span id="state-badge" class="badge"></span>
        </div>
        <h1 class="text-2xl font-bold tracking-tight" id="title">Loading&hellip;</h1>
        <p class="mono text-indigo-200 text-xs mt-1 break-all" id="file-id-display">__FILE_ID__</p>
      </div>
      <div class="text-right">
        <p class="text-indigo-200 text-xs uppercase tracking-widest">Business Date</p>
        <p class="text-2xl font-bold" id="business-date">&mdash;</p>
      </div>
    </div>
  </div>
</div>

<div class="max-w-screen-xl mx-auto px-6 py-8 space-y-6">

  <!-- Pipeline Flow -->
  <div class="card p-6 fade-in">
    <h2 class="text-xs font-semibold text-slate-400 uppercase tracking-widest mb-6">Data Journey</h2>
    <div id="pipeline-flow" class="flex items-center gap-8 overflow-x-auto pb-2">
      <!-- populated by JS -->
      <div class="skeleton h-32 w-36 flex-shrink-0"></div>
      <div class="skeleton h-32 w-36 flex-shrink-0"></div>
      <div class="skeleton h-32 w-36 flex-shrink-0"></div>
      <div class="skeleton h-32 w-36 flex-shrink-0"></div>
    </div>
  </div>

  <!-- Middle row: File meta + DQ + Affected rows -->
  <div class="grid grid-cols-1 md:grid-cols-3 gap-6">

    <!-- File Metadata -->
    <div class="card p-6 fade-in" style="animation-delay:0.1s">
      <h2 class="text-xs font-semibold text-slate-400 uppercase tracking-widest mb-4">File Details</h2>
      <div id="file-meta" class="space-y-3 text-sm">
        <div class="skeleton h-4 w-full"></div>
        <div class="skeleton h-4 w-3/4"></div>
        <div class="skeleton h-4 w-2/3"></div>
      </div>
    </div>

    <!-- DQ Results -->
    <div class="card p-6 fade-in" style="animation-delay:0.15s">
      <h2 class="text-xs font-semibold text-slate-400 uppercase tracking-widest mb-4">Data Quality</h2>
      <div id="dq-section" class="space-y-3">
        <div class="skeleton h-4 w-full"></div>
        <div class="skeleton h-4 w-full"></div>
      </div>
    </div>

    <!-- Affected Rows -->
    <div class="card p-6 fade-in" style="animation-delay:0.2s">
      <h2 class="text-xs font-semibold text-slate-400 uppercase tracking-widest mb-4">Rows in Postgres</h2>
      <div id="affected-section" class="space-y-4">
        <div class="skeleton h-12 w-full"></div>
        <div class="skeleton h-12 w-full"></div>
      </div>
    </div>
  </div>

  <!-- Pipeline Execution -->
  <div class="card p-6 fade-in" style="animation-delay:0.25s">
    <h2 class="text-xs font-semibold text-slate-400 uppercase tracking-widest mb-5">Pipeline Execution</h2>
    <div id="execution-section">
      <div class="skeleton h-14 w-full mb-2"></div>
      <div class="skeleton h-10 w-5/6 ml-8 mb-1"></div>
      <div class="skeleton h-10 w-5/6 ml-8 mb-4"></div>
      <div class="skeleton h-14 w-full mb-2"></div>
      <div class="skeleton h-10 w-5/6 ml-8 mb-1"></div>
    </div>
  </div>

  <!-- Lineage Edges -->
  <div class="card p-6 fade-in" style="animation-delay:0.3s">
    <h2 class="text-xs font-semibold text-slate-400 uppercase tracking-widest mb-5">Lineage Edges</h2>
    <div id="edges-section">
      <div class="skeleton h-10 w-full mb-2"></div>
      <div class="skeleton h-10 w-full mb-2"></div>
    </div>
  </div>

</div>

<!-- Error toast -->
<div id="error-toast" class="hidden fixed bottom-6 right-6 bg-red-600 text-white px-5 py-3 rounded-xl shadow-lg text-sm font-medium max-w-sm"></div>

<script>
const FILE_ID = '__FILE_ID__';

function fmt(n) {
  if (n === null || n === undefined) return '—';
  return Number(n).toLocaleString();
}
function fmtBytes(b) {
  if (!b) return '—';
  if (b < 1024) return b + ' B';
  if (b < 1048576) return (b/1024).toFixed(1) + ' KB';
  return (b/1048576).toFixed(1) + ' MB';
}
function fmtDt(s) {
  if (!s) return '—';
  const d = new Date(s);
  return d.toLocaleString('en-GB', { dateStyle: 'short', timeStyle: 'medium' });
}
function fmtDuration(start, end) {
  if (!start || !end) return '—';
  const ms = new Date(end) - new Date(start);
  if (ms < 1000) return ms + 'ms';
  return (ms/1000).toFixed(1) + 's';
}
function truncate(s, n=42) {
  if (!s) return '—';
  if (s.length <= n) return s;
  return '…' + s.slice(-(n-1));
}
function stateBadgeClass(state) {
  const m = { succeeded:'badge-success', published:'badge-published',
    curated:'badge-curated', failed:'badge-failed',
    running:'badge-running', superseded:'badge-superseded' };
  return m[state] || 'badge-curated';
}
function statusDot(s) {
  const m = { succeeded:'dot-success', failed:'dot-failed',
    running:'dot-running', skipped:'dot-skipped', partial:'dot-running' };
  return `<span class="status-dot ${m[s]||'dot-skipped'}"></span>`;
}

async function load() {
  try {
    const res = await fetch(`/api/lineage/${FILE_ID}`);
    if (!res.ok) {
      const err = await res.json();
      throw new Error(err.detail || res.statusText);
    }
    const data = await res.json();
    render(data);
  } catch(e) {
    showError(e.message);
  }
}

function render(data) {
  const { file, runs, stages, edges, affected } = data;

  // Header
  const stateEl = document.getElementById('state-badge');
  stateEl.textContent = (file.state || '').toUpperCase();
  stateEl.className = 'badge ' + stateBadgeClass(file.state);
  document.getElementById('title').textContent = `${file.domain} / ${file.dataset}`;
  document.getElementById('business-date').textContent = file.business_date || '—';

  // Pipeline flow
  const ingestRun = runs.find(r => r.pipeline_type === 'ingestion');
  const publishRun = runs.find(r => r.pipeline_type === 'publish');

  const terminal = new Set(['succeeded','failed','partial','skipped']);
  const terminalKeys = new Set(stages.filter(s => terminal.has(s.status)).map(s => s.run_id+'|'+s.stage));
  const dedupedStages = stages.filter(s => !(s.status === 'running' && terminalKeys.has(s.run_id+'|'+s.stage)));
  const pubDqStage = dedupedStages.find(s => s.pipeline_type === 'publish' && s.stage === 'dq_check') || null;

  const linearNodes = [
    {
      icon: '🗄️',
      label: 'S3 Raw',
      sub: truncate(file.s3_raw_path, 34),
      count: ingestRun ? fmt(ingestRun.record_count_source) : '—',
      countLabel: 'source rows',
      status: file.s3_raw_path ? 'succeeded' : 'skipped',
      extra: fmtBytes(file.file_size_bytes),
    },
    {
      icon: '⚙️',
      label: 'Ingestion',
      sub: ingestRun ? fmtDt(ingestRun.started_at) : 'not run',
      count: ingestRun ? fmt(ingestRun.record_count_dq_pass) : '—',
      countLabel: 'passed DQ',
      status: ingestRun ? ingestRun.status : 'skipped',
      extra: ingestRun && ingestRun.record_count_source
        ? Math.round((ingestRun.record_count_dq_pass||0) / ingestRun.record_count_source * 100) + '% pass rate'
        : (ingestRun ? fmtDuration(ingestRun.started_at, ingestRun.ended_at) : ''),
    },
    {
      icon: '📦',
      label: 'S3 Curated',
      sub: truncate(file.s3_curated_path, 34),
      count: ingestRun ? fmt(ingestRun.record_count_dq_pass) : '—',
      countLabel: 'parquet rows',
      status: file.s3_curated_path ? 'succeeded' : 'skipped',
      extra: 'Parquet',
    },
    {
      icon: '🔬',
      label: 'DQ Gate',
      sub: pubDqStage ? truncate(pubDqStage.input_ref, 34) : (publishRun ? 'curated parquet' : 'not run'),
      count: pubDqStage ? fmt(pubDqStage.record_count_out) : (publishRun ? fmt(publishRun.record_count_dq_pass) : '—'),
      countLabel: 'rows passed',
      status: pubDqStage ? (pubDqStage.status === 'dq_warned' ? 'partial' : pubDqStage.status) : (publishRun ? publishRun.status : 'skipped'),
      extra: pubDqStage ? `${fmt((pubDqStage.record_count_in||0)-(pubDqStage.record_count_out||0))} failed` : '',
    },
    {
      icon: '📡',
      label: 'Kafka',
      sub: publishRun ? (publishRun.kafka_topic || '—') : 'not published',
      count: publishRun ? fmt(publishRun.record_count_target) : '—',
      countLabel: 'messages',
      status: publishRun ? publishRun.status : 'skipped',
      extra: publishRun && publishRun.kafka_offset_start != null
        ? `offset ${fmt(publishRun.kafka_offset_start)}–${fmt(publishRun.kafka_offset_end)}`
        : '',
    },
  ];

  const forkNodes = [
    {
      icon: '🐘',
      label: 'insurance_policy',
      sub: 'UPSERT · current state',
      count: fmt(affected.insurance_policy),
      countLabel: 'rows this file',
      status: publishRun ? publishRun.status : 'skipped',
      color: 'indigo',
    },
    {
      icon: '📋',
      label: 'insurance_policy_history',
      sub: 'INSERT · append only',
      count: fmt(affected.insurance_policy_history),
      countLabel: 'rows this file',
      status: publishRun ? publishRun.status : 'skipped',
      color: 'purple',
    },
  ];

  const dotClass = { succeeded:'dot-success',failed:'dot-failed',running:'dot-running',skipped:'dot-skipped' };

  const linearHtml = linearNodes.map(n => `
    <div class="pipeline-node has-arrow card flex-shrink-0 w-40 p-4 flex flex-col items-center text-center gap-1 mr-4">
      <div class="node-icon mb-1">${n.icon}</div>
      <div class="flex items-center gap-1 mb-1">
        <span class="status-dot ${dotClass[n.status]||'dot-skipped'}"></span>
        <span class="text-xs font-semibold text-slate-600">${n.label}</span>
      </div>
      <div class="count-num">${n.count}</div>
      <div class="text-xs text-slate-400">${n.countLabel}</div>
      ${n.extra ? `<div class="text-xs text-indigo-500 font-medium mt-1">${n.extra}</div>` : ''}
      <div class="text-xs text-slate-300 mt-1 break-all leading-tight" title="${n.sub||''}">${n.sub||''}</div>
    </div>
  `).join('');

  const colorMap = { indigo: 'bg-indigo-50 border-indigo-200', purple: 'bg-purple-50 border-purple-200' };
  const countColor = { indigo: 'text-indigo-700', purple: 'text-purple-700' };
  const labelColor = { indigo: 'text-indigo-500', purple: 'text-purple-500' };

  const forkHtml = `
    <div class="fork-container flex-shrink-0 ml-4">
      ${forkNodes.map(n => `
        <div class="fork-branch">
          <div class="pipeline-node card w-48 p-4 flex flex-col gap-1 border ${colorMap[n.color]}">
            <div class="flex items-center gap-2 mb-1">
              <span class="status-dot ${dotClass[n.status]||'dot-skipped'}"></span>
              <span class="text-xs font-semibold text-slate-600">${n.icon} ${n.label}</span>
            </div>
            <div class="flex items-end justify-between">
              <div>
                <div class="count-num ${countColor[n.color]}">${n.count}</div>
                <div class="text-xs text-slate-400">${n.countLabel}</div>
              </div>
            </div>
            <div class="text-xs ${labelColor[n.color]} font-medium">${n.sub}</div>
          </div>
        </div>
      `).join('')}
    </div>
  `;

  document.getElementById('pipeline-flow').innerHTML = linearHtml + forkHtml;

  // File metadata
  const md5Short = file.file_md5 ? file.file_md5.slice(0,8)+'…'+file.file_md5.slice(-6) : '—';
  document.getElementById('file-meta').innerHTML = `
    <div class="flex justify-between"><span class="text-slate-500">File ID</span><span class="mono text-xs text-slate-700 truncate max-w-[160px]" title="${file.file_id}">${file.file_id ? file.file_id.slice(0,14)+'…' : '—'}</span></div>
    <div class="flex justify-between"><span class="text-slate-500">MD5</span><span class="mono text-xs text-slate-700">${md5Short}</span></div>
    <div class="flex justify-between"><span class="text-slate-500">Size</span><span class="font-medium text-slate-700">${fmtBytes(file.file_size_bytes)}</span></div>
    <div class="flex justify-between"><span class="text-slate-500">State</span><span class="badge ${stateBadgeClass(file.state)} text-xs">${file.state||'—'}</span></div>
    <div class="flex justify-between"><span class="text-slate-500">Updated</span><span class="text-slate-700 text-xs">${fmtDt(file.state_updated_at)}</span></div>
    <div class="flex justify-between"><span class="text-slate-500">Runs</span><span class="font-semibold text-slate-700">${runs.length}</span></div>
  `;

  // DQ
  const src = ingestRun ? (ingestRun.record_count_source||0) : 0;
  const pass = ingestRun ? (ingestRun.record_count_dq_pass||0) : 0;
  const fail = ingestRun ? (ingestRun.record_count_dq_fail||0) : 0;
  const pct = src > 0 ? Math.round(pass/src*100) : 0;
  const barColor = pct >= 95 ? '#10b981' : pct >= 80 ? '#f59e0b' : '#ef4444';
  document.getElementById('dq-section').innerHTML = `
    <div class="flex justify-between items-center mb-2">
      <span class="text-2xl font-bold" style="color:${barColor}">${pct}%</span>
      <span class="text-xs text-slate-400">pass rate</span>
    </div>
    <div class="progress-bar-bg mb-3">
      <div class="progress-bar-fill" style="width:${pct}%;background:${barColor}"></div>
    </div>
    <div class="grid grid-cols-3 gap-2 text-center">
      <div class="bg-slate-50 rounded-lg p-2">
        <div class="text-sm font-bold text-slate-700">${fmt(src)}</div>
        <div class="text-xs text-slate-400">Source</div>
      </div>
      <div class="bg-emerald-50 rounded-lg p-2">
        <div class="text-sm font-bold text-emerald-700">${fmt(pass)}</div>
        <div class="text-xs text-emerald-500">Pass</div>
      </div>
      <div class="bg-red-50 rounded-lg p-2">
        <div class="text-sm font-bold text-red-600">${fmt(fail)}</div>
        <div class="text-xs text-red-400">Fail</div>
      </div>
    </div>
  `;

  // Affected rows
  document.getElementById('affected-section').innerHTML = `
    <div class="flex items-center justify-between bg-indigo-50 rounded-xl p-4">
      <div>
        <div class="text-xs font-semibold text-indigo-400 uppercase tracking-wide">insurance_policy</div>
        <div class="text-xs text-slate-400 mt-0.5">Current state (upsert)</div>
      </div>
      <div class="text-3xl font-bold text-indigo-700">${fmt(affected.insurance_policy)}</div>
    </div>
    <div class="flex items-center justify-between bg-purple-50 rounded-xl p-4">
      <div>
        <div class="text-xs font-semibold text-purple-400 uppercase tracking-wide">insurance_policy_history</div>
        <div class="text-xs text-slate-400 mt-0.5">Full audit trail (append)</div>
      </div>
      <div class="text-3xl font-bold text-purple-700">${fmt(affected.insurance_policy_history)}</div>
    </div>
  `;

  // Combined Pipeline Execution (runs + nested stages)
  const pipelineIcon = { ingestion:'⚙️', publish:'📡', merge:'🔀' };
  const stageIcon = {
    raw_read:'📥', schema_validate:'🧬', dq_check:'🔬',
    curated_write:'📦', curated_read:'📂',
    kafka_publish:'📤', recon_t0:'⚖️',
    sink_pg_wait:'🐘', sink_s3_wait:'☁️', finalise:'✅',
  };
  const eventTypeBadge = t => {
    if (!t) return '';
    const m = {
      stage_completed: 'bg-emerald-50 text-emerald-700',
      stage_failed:    'bg-red-50 text-red-700',
      stage_started:   'bg-amber-50 text-amber-700',
      stage_skipped:   'bg-slate-100 text-slate-500',
      stage_warned:    'bg-orange-50 text-orange-700',
    };
    return `<span class="inline-flex px-2 py-0.5 rounded-full text-xs font-semibold ${m[t]||'bg-slate-100 text-slate-500'}">${t.replace('stage_','')}</span>`;
  };
  const correlationLinks = s => {
    let html = '';
    if (s.airflow_dag_id && s.airflow_run_id) {
      html += `<a href="/dags/${s.airflow_dag_id}/dagRuns/${encodeURIComponent(s.airflow_run_id)}" target="_blank"
               title="Open in Airflow" class="text-sky-500 hover:text-sky-700 text-xs mr-2">✈ Airflow</a>`;
    }
    if (s.spark_app_id) {
      html += `<span class="mono text-xs text-slate-400" title="Spark app id: ${s.spark_app_id}">⚡ ${s.spark_app_id.slice(0,20)}</span>`;
    }
    return html || '—';
  };

  if (runs.length === 0) {
    document.getElementById('execution-section').innerHTML = `<p class="text-sm text-slate-400 text-center py-6">No pipeline runs recorded.</p>`;
  } else {
    const html = runs.map((r, ri) => {
      const runStages = dedupedStages.filter(s => s.run_id === r.run_id);
      const stageRows = runStages.map(s => `
        <tr class="bg-slate-50 border-b border-slate-100 align-top text-xs">
          <td class="py-2 pl-12 pr-4 text-slate-400 w-6">└</td>
          <td class="py-2 pr-4 font-medium text-slate-600 whitespace-nowrap">${stageIcon[s.stage]||'◆'} ${s.stage}</td>
          <td class="py-2 pr-4">${eventTypeBadge(s.event_type)}</td>
          <td class="py-2 pr-4">${statusDot(s.status)}<span class="badge ${stateBadgeClass(s.status)} text-xs">${s.status}</span></td>
          <td class="py-2 pr-4 mono text-slate-400 break-all leading-relaxed">${s.input_ref||'—'}</td>
          <td class="py-2 pr-4 mono text-slate-400 break-all leading-relaxed">${s.output_ref||'—'}</td>
          <td class="py-2 pr-4 text-right text-slate-500">${fmt(s.record_count_in)}</td>
          <td class="py-2 pr-4 text-right text-emerald-600 font-medium">${fmt(s.record_count_out)}</td>
          <td class="py-2 pr-4 text-right text-slate-400 whitespace-nowrap">${fmtDuration(s.started_at, s.ended_at)}</td>
          <td class="py-2 text-slate-400">${correlationLinks(s)}</td>
        </tr>
      `).join('');

      return `
        <tr class="run-row border-b-2 border-slate-100 align-middle text-sm ${ri > 0 ? 'border-t-4 border-t-slate-50' : ''}">
          <td class="py-4 pr-4 w-8 text-lg">${pipelineIcon[r.pipeline_type]||'▶'}</td>
          <td class="py-4 pr-6 font-semibold text-slate-800 whitespace-nowrap capitalize">${r.pipeline_type}</td>
          <td class="py-4 pr-4">${statusDot(r.status)}<span class="badge ${stateBadgeClass(r.status)} text-xs">${r.status}</span></td>
          <td class="py-4 pr-6 mono text-xs text-slate-400 whitespace-nowrap">${r.run_id}</td>
          <td class="py-4 pr-6 text-slate-500 whitespace-nowrap">${fmtDt(r.started_at)}</td>
          <td class="py-4 pr-6 text-right text-slate-500 whitespace-nowrap">${fmtDuration(r.started_at, r.ended_at)}</td>
          <td class="py-4 pr-4 text-right"><span class="text-xs text-slate-400">src </span><span class="font-semibold">${fmt(r.record_count_source)}</span></td>
          <td class="py-4 text-right"><span class="text-xs text-slate-400">pub </span><span class="font-semibold text-indigo-700">${fmt(r.record_count_target)}</span></td>
        </tr>
        ${stageRows}
      `;
    }).join('');

    document.getElementById('execution-section').innerHTML = `
      <div class="overflow-x-auto">
      <table class="w-full">
        <thead>
          <tr class="text-xs text-slate-400 uppercase tracking-wider border-b-2 border-slate-100">
            <th class="pb-3 pr-4 w-8"></th>
            <th class="text-left pb-3 pr-6 w-28">Run type</th>
            <th class="text-left pb-3 pr-4 w-28">Status</th>
            <th class="text-left pb-3 pr-6">Run ID</th>
            <th class="text-left pb-3 pr-6 w-44">Started</th>
            <th class="text-right pb-3 pr-6 w-20">Duration</th>
            <th class="text-right pb-3 pr-4 w-24">Source</th>
            <th class="text-right pb-3 w-24">Published</th>
            <th class="text-left pb-3 pl-4 text-xs normal-case" colspan="3">Stage · Event Type · Status · In/Out · Duration · Correlation</th>
          </tr>
        </thead>
        <tbody>${html}</tbody>
      </table>
      </div>
    `;
  }

  // Lineage Edges
  const edgeIcon = { raw_to_curated:'📥→📦', curated_to_kafka:'📦→📡', curated_to_postgres:'📦→🐘' };
  if (!edges || edges.length === 0) {
    document.getElementById('edges-section').innerHTML =
      `<p class="text-sm text-slate-400 text-center py-4">No lineage edges recorded.</p>`;
  } else {
    document.getElementById('edges-section').innerHTML = `
      <div class="overflow-x-auto">
      <table class="w-full text-sm">
        <thead><tr class="text-xs text-slate-400 uppercase tracking-wider border-b-2 border-slate-100">
          <th class="text-left pb-3 pr-4">Edge Type</th>
          <th class="text-left pb-3 pr-4">Source Ref</th>
          <th class="text-left pb-3 pr-4">Target Ref</th>
          <th class="text-right pb-3 pr-4">Record Count</th>
          <th class="text-left pb-3">Created</th>
        </tr></thead>
        <tbody>${edges.map(e => `
          <tr class="border-b border-slate-50 hover:bg-slate-50 text-xs">
            <td class="py-2 pr-4 font-semibold text-indigo-600">${edgeIcon[e.edge_type]||'→'} ${e.edge_type}</td>
            <td class="py-2 pr-4 mono text-slate-400 break-all">${truncate(e.source_ref,50)}</td>
            <td class="py-2 pr-4 mono text-slate-400 break-all">${truncate(e.target_ref,50)}</td>
            <td class="py-2 pr-4 text-right font-medium text-slate-700">${fmt(e.record_count)}</td>
            <td class="py-2 text-slate-400 whitespace-nowrap">${fmtDt(e.created_at)}</td>
          </tr>`).join('')}
        </tbody>
      </table>
      </div>`;
  }
}

function showError(msg) {
  const t = document.getElementById('error-toast');
  t.textContent = '⚠️ ' + msg;
  t.classList.remove('hidden');
  document.getElementById('title').textContent = 'Not Found';
  ['pipeline-flow','file-meta','dq-section','affected-section','runs-section','stages-section'].forEach(id => {
    const el = document.getElementById(id);
    if (el) el.innerHTML = `<p class="text-sm text-slate-400 italic">No data</p>`;
  });
  document.getElementById('state-badge').textContent = 'NOT FOUND';
  document.getElementById('state-badge').className = 'badge badge-failed';
}

load();
</script>
</body>
</html>
"""


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("scripts.lineage_viewer:app", host="0.0.0.0", port=8888, reload=True)
