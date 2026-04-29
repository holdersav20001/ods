"""Capture pipeline evidence to docs/evidence/.

Runs against the live local stack (postgres on localhost:5440) and writes:
- lineage.txt        — file_catalogue + run_log + run_stage_log chain
- reconciliation.txt — reconciliation_log rows (T0 + T2)
- observability.txt  — health snapshot per service + grafana datasource probe
- dr.txt             — DR test result summary

Run after E2E tests pass.
"""
from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import psycopg2
import requests


OUT = Path("docs/evidence")
OUT.mkdir(parents=True, exist_ok=True)


def _pg():
    return psycopg2.connect(
        host="localhost", port=5440, dbname="ods_dev", user="ods", password="ods"
    )


def _section(title: str) -> str:
    return f"\n{'=' * 78}\n{title}\n{'=' * 78}\n"


def _query(conn, sql: str) -> str:
    with conn.cursor() as cur:
        cur.execute(sql)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    if not rows:
        return "(no rows)\n"
    width = [max(len(str(c)), max((len(str(r[i])) for r in rows), default=0)) for i, c in enumerate(cols)]
    out = " | ".join(f"{c:<{width[i]}}" for i, c in enumerate(cols)) + "\n"
    out += "-+-".join("-" * w for w in width) + "\n"
    for r in rows:
        out += " | ".join(f"{str(r[i]):<{width[i]}}" for i in range(len(cols))) + "\n"
    return out


def lineage(conn) -> str:
    out = _section("LINEAGE — file_catalogue (raw drop)")
    out += _query(conn, """
        SELECT file_id::text, domain, dataset, business_date, state,
               substring(s3_raw_path,1,60) AS s3_raw_path, file_md5,
               last_run_id::text
          FROM pipeline.file_catalogue
         ORDER BY first_seen_at DESC LIMIT 10
    """)
    out += _section("LINEAGE — run_log (per-file run header)")
    out += _query(conn, """
        SELECT run_id::text, domain, dataset, business_date, status,
               record_count_source, record_count_dq_pass,
               record_count_published, kafka_offset_start, kafka_offset_end
          FROM pipeline.run_log
         ORDER BY started_at DESC LIMIT 10
    """)
    out += _section("LINEAGE — run_stage_log (per-stage breakdown)")
    out += _query(conn, """
        SELECT run_id::text, stage, status,
               substring(input_ref,1,40)  AS input,
               substring(output_ref,1,40) AS output,
               started_at, ended_at
          FROM pipeline.run_stage_log
         ORDER BY started_at DESC LIMIT 30
    """)
    out += _section("LINEAGE — joined chain (file → run → stages → target)")
    out += _query(conn, """
        SELECT f.file_md5, f.state AS file_state,
               r.run_id::text, r.status AS run_status,
               s.stage, s.status AS stage_status
          FROM pipeline.file_catalogue f
          LEFT JOIN pipeline.run_log r        ON r.file_id = f.file_id
          LEFT JOIN pipeline.run_stage_log s  ON s.run_id  = r.run_id
         ORDER BY r.started_at DESC, s.started_at NULLS LAST
         LIMIT 30
    """)
    return out


def reconciliation(conn) -> str:
    out = _section("RECONCILIATION — reconciliation_log")
    out += _query(conn, """
        SELECT created_at, check_type, domain, dataset, business_date,
               source_count, kafka_count, postgres_count,
               discrepancy_count, discrepancy_pct, status
          FROM pipeline.reconciliation_log
         ORDER BY created_at DESC LIMIT 30
    """)
    return out


def observability() -> str:
    out = _section("OBSERVABILITY — docker compose ps")
    cp = subprocess.run(
        ["docker", "compose", "ps", "--format", "{{.Service}}\t{{.Status}}"],
        capture_output=True, text=True,
    )
    out += cp.stdout + "\n"

    out += _section("OBSERVABILITY — kafka-connect connector states")
    try:
        names = requests.get("http://localhost:8083/connectors", timeout=5).json()
        for name in names:
            s = requests.get(f"http://localhost:8083/connectors/{name}/status", timeout=5).json()
            out += f"{name}: connector={s['connector']['state']}; "
            out += "; ".join(f"task{t['id']}={t['state']}" for t in s.get("tasks", []))
            out += "\n"
    except Exception as e:
        out += f"connect probe error: {e}\n"

    out += _section("OBSERVABILITY — schema registry subjects")
    try:
        subs = requests.get("http://localhost:8081/subjects", timeout=5).json()
        out += json.dumps(subs, indent=2) + "\n"
    except Exception as e:
        out += f"sr probe error: {e}\n"

    out += _section("OBSERVABILITY — grafana health + datasource")
    try:
        h = requests.get("http://localhost:3000/api/health", timeout=5).json()
        out += f"grafana health: {json.dumps(h)}\n"
        ds = requests.get(
            "http://localhost:3000/api/datasources",
            auth=("admin", "admin"), timeout=5,
        ).json()
        out += "datasources: " + json.dumps([{"name": d["name"], "type": d["type"]} for d in ds]) + "\n"
        dashboards = requests.get(
            "http://localhost:3000/api/search?type=dash-db",
            auth=("admin", "admin"), timeout=5,
        ).json()
        out += "dashboards: " + json.dumps([d["title"] for d in dashboards]) + "\n"
    except Exception as e:
        out += f"grafana probe error: {e}\n"
    return out


def dr_summary(conn) -> str:
    out = _section("DR — failed runs preserved (audit trail)")
    out += _query(conn, """
        SELECT run_id::text, domain, dataset, business_date, status,
               substring(error_summary,1,80) AS error_summary, started_at
          FROM pipeline.run_log
         WHERE status = 'failed'
         ORDER BY started_at DESC LIMIT 10
    """)
    out += _section("DR — recoveries (succeeded runs after failures, same bd)")
    out += _query(conn, """
        SELECT business_date,
               sum(case when status='failed'    then 1 else 0 end) AS failed_runs,
               sum(case when status='succeeded' then 1 else 0 end) AS succeeded_runs
          FROM pipeline.run_log
         WHERE domain='insurance' AND dataset='policies'
         GROUP BY business_date
         ORDER BY business_date DESC LIMIT 10
    """)
    return out


if __name__ == "__main__":
    conn = _pg()
    try:
        (OUT / "lineage.txt").write_text(lineage(conn), encoding="utf-8")
        (OUT / "reconciliation.txt").write_text(reconciliation(conn), encoding="utf-8")
        (OUT / "observability.txt").write_text(observability(), encoding="utf-8")
        (OUT / "dr.txt").write_text(dr_summary(conn), encoding="utf-8")
    finally:
        conn.close()
    print(f"Evidence written to {OUT}")
