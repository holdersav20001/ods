"""Replay / rerun subcommand for python -m ods_pipeline.ops.

Two operations:
  - replay --file-id <uuid>   restart all runs that touched a file
  - rerun  --run-id  <uuid>   restart one specific run

Both operations:
  1. Look up the original run + dataset metadata from run_log
  2. Allocate a new run_id, INSERT a fresh run_log row (status='running')
  3. Write a lineage_edge with edge_type='replay' linking new -> original
  4. Trigger the appropriate Airflow DAG via REST (or print intended action
     in --dry-run)
  5. Old run + its evidence are NEVER mutated
"""
from __future__ import annotations

import argparse
import json
import sys
import uuid
from typing import Any


def register(parser: argparse.ArgumentParser) -> None:
    sub = parser.add_subparsers(dest="runs_cmd", required=True)
    p_replay = sub.add_parser("replay", help="restart runs for a file")
    p_replay.add_argument("--file-id", required=True)
    p_replay.add_argument("--dry-run", action="store_true")

    p_rerun = sub.add_parser("rerun", help="restart a specific run")
    p_rerun.add_argument("--run-id", required=True)
    p_rerun.add_argument("--dry-run", action="store_true")


def dispatch(args) -> int:  # pragma: no cover — thin glue
    pg = _connect_pg()
    airflow = _make_airflow_client()
    ops = _RunsOps(pg_conn=pg, airflow=airflow)
    if args.runs_cmd == "replay":
        out = ops.replay_file(args.file_id, dry_run=args.dry_run)
    elif args.runs_cmd == "rerun":
        out = ops.rerun(args.run_id, dry_run=args.dry_run)
    else:
        print(f"unknown subcommand {args.runs_cmd!r}", file=sys.stderr)
        return 2
    print(json.dumps(out, indent=2, default=str))
    return 0


class _RunsOps:
    """Stateless wrapper. Postgres + Airflow client injected for testability."""

    def __init__(self, *, pg_conn, airflow):
        self._pg = pg_conn
        self._airflow = airflow

    # ---------------------------------------------------------------
    # rerun: keyed by run_id
    # ---------------------------------------------------------------
    def rerun(self, run_id: str, *, dry_run: bool = False) -> dict[str, Any]:
        original = self._lookup_run(run_id)
        if original is None:
            raise LookupError(f"run_id not found: {run_id}")
        return self._launch_replay(original, source="rerun", dry_run=dry_run)

    # ---------------------------------------------------------------
    # replay: keyed by file_id (one or more original runs)
    # ---------------------------------------------------------------
    def replay_file(self, file_id: str, *, dry_run: bool = False) -> dict[str, Any]:
        originals = self._lookup_runs_by_file(file_id)
        if not originals:
            raise LookupError(f"no runs found for file_id={file_id}")
        results = [
            self._launch_replay(o, source="replay", dry_run=dry_run)
            for o in originals
        ]
        return {"file_id": file_id, "replays": results}

    # ---------------------------------------------------------------
    # internals
    # ---------------------------------------------------------------
    def _lookup_run(self, run_id: str):
        with self._pg.cursor() as cur:
            cur.execute(
                """
                SELECT run_id::text, pipeline_type, domain, dataset,
                       business_date::text, file_id::text, kafka_topic
                  FROM pipeline.run_log WHERE run_id = %s
                """,
                (run_id,),
            )
            row = cur.fetchone()
        if not row:
            return None
        keys = ("run_id", "pipeline_type", "domain", "dataset",
                "business_date", "file_id", "kafka_topic")
        return dict(zip(keys, row))

    def _lookup_runs_by_file(self, file_id: str) -> list[dict]:
        with self._pg.cursor() as cur:
            cur.execute(
                """
                SELECT run_id::text, pipeline_type, domain, dataset,
                       business_date::text, file_id::text, kafka_topic
                  FROM pipeline.run_log WHERE file_id = %s::uuid
                  ORDER BY started_at DESC
                """,
                (file_id,),
            )
            rows = cur.fetchall()
        keys = ("run_id", "pipeline_type", "domain", "dataset",
                "business_date", "file_id", "kafka_topic")
        return [dict(zip(keys, r)) for r in rows]

    def _launch_replay(self, original, *, source: str,
                       dry_run: bool) -> dict[str, Any]:
        new_run_id = str(uuid.uuid4())
        result = {
            "source": source,
            "new_run_id": new_run_id,
            "original_run_id": original["run_id"],
            "pipeline_type": original["pipeline_type"],
            "domain": original["domain"],
            "dataset": original["dataset"],
            "business_date": original["business_date"],
            "file_id": original["file_id"],
            "dry_run": dry_run,
        }
        if dry_run:
            result["status"] = "dry-run"
            return result
        # Real flow: open new run, link lineage, trigger Airflow.
        from ods_pipeline import lineage, runs
        runs.start(self._pg, run_id=new_run_id,
                   pipeline_type=original["pipeline_type"],
                   domain=original["domain"],
                   dataset=original["dataset"],
                   business_date=original["business_date"],
                   file_id=original["file_id"],
                   kafka_topic=original["kafka_topic"],
                   parents=[{"replay_of": original["run_id"]}])
        lineage.write_edge(self._pg, child_run_id=new_run_id,
                           parent_run_id=original["run_id"],
                           edge_type="replay",
                           record_count=None)
        dag_id = _dag_for(original["pipeline_type"])
        self._airflow.trigger_dag(
            dag_id=dag_id,
            conf={"run_id": new_run_id,
                  "file_id": original["file_id"],
                  "domain": original["domain"],
                  "dataset": original["dataset"],
                  "business_date": original["business_date"]},
        )
        result["status"] = "triggered"
        result["dag_id"] = dag_id
        return result


def _dag_for(pipeline_type: str) -> str:
    return {
        "file":          "dag_ingest",
        "message_api":   "dag_event_api",
        "dlq_replay":    "dag_dlq_replay",
    }.get(pipeline_type, "dag_ingest")


# ---------------------------------------------------------------------------
# CLI helpers (production-only paths)
# ---------------------------------------------------------------------------

def _connect_pg():  # pragma: no cover
    import os
    import psycopg2
    return psycopg2.connect(
        host=os.environ.get("PG_HOST", "127.0.0.1"),
        port=int(os.environ.get("PG_PORT", "5440")),
        dbname=os.environ.get("PG_DB", "ods_dev"),
        user=os.environ.get("PG_USER", "ods"),
        password=os.environ.get("PG_PASSWORD", "ods"),
    )


def _make_airflow_client():  # pragma: no cover
    import os
    import requests

    base = os.environ.get("AIRFLOW_BASE_URL", "http://localhost:8080")
    auth = (
        os.environ.get("AIRFLOW_USER", "airflow"),
        os.environ.get("AIRFLOW_PASSWORD", "airflow"),
    )

    class _AirflowClient:
        def trigger_dag(self, *, dag_id: str, conf: dict[str, Any]) -> dict:
            resp = requests.post(
                f"{base}/api/v1/dags/{dag_id}/dagRuns",
                json={"conf": conf},
                auth=auth,
                timeout=30,
            )
            resp.raise_for_status()
            return resp.json()

    return _AirflowClient()
