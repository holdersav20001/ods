"""Heartbeat-staleness janitor for ``pipeline.run_log``.

Closes orphan runs that died mid-flight without writing a terminal status
— k8s pod evicted, OOM killer, SIGKILL, network partition between worker
and Postgres after the last commit. The stateless control-plane contract
relies on every code path writing its own success / failure row; this
DAG covers the gap when no code path runs at all.

Definition of "stuck"
---------------------

A run is considered abandoned when ALL of the following hold:

* ``run_log.status = 'running'``
* ``run_log.started_at < NOW() - heartbeat_grace`` (default 5 min)
* No row in ``run_stage_log`` for that ``run_id`` had any activity
  (``started_at`` or ``ended_at``) inside the last ``heartbeat_grace``.
  Activity covers ``stage_heartbeat`` rows from
  :func:`airflow.dags.common.long_running_docker.make_long_running_docker_operator`
  AND ordinary stage transitions written by short-lived ingestions.

The grace window is intentionally **not** keyed on wall-clock job
duration — long-running Glue jobs may legitimately run for hours, and
the long-running operator emits a ``stage_heartbeat`` row every 30s
to keep that activity timestamp fresh.

Action
------

When a run matches the predicate, this DAG:

1. Inserts a terminal ``run_stage_log`` row marking the kill, so the
   stage timeline shows when and why the janitor acted.
2. Updates ``run_log`` to ``status='failed'``,
   ``error_summary='janitor_no_heartbeat'``, ``ended_at=NOW()``.
3. Logs the run_id so operators searching the dashboard for "why did
   this fail?" can find this DAG run via the lineage viewer.

The two writes share one transaction so the dashboard never sees a
status='failed' run with no matching stage row.
"""
from __future__ import annotations

import logging
import os
from datetime import timedelta

import psycopg2

from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago

log = logging.getLogger(__name__)

# Configurable via Airflow Variable / env if operators want to tune. Keep
# the default conservative — five minutes is comfortably longer than the
# 30s long-running heartbeat cadence and the 60s api_pull poll interval.
_GRACE_MINUTES_DEFAULT = int(os.environ.get("RUN_JANITOR_GRACE_MINUTES", "5"))


def _connect_pg():
    return psycopg2.connect(
        os.environ.get(
            "PIPELINE_PG_DSN",
            "host=postgres port=5432 dbname=ods_dev user=ods password=ods",
        ),
        connect_timeout=5,
    )


def reap_orphan_runs(
    conn,
    *,
    grace_minutes: int = _GRACE_MINUTES_DEFAULT,
) -> list[str]:
    """Flip every stuck-running run to failed and write a kill stage row.

    Returns the list of ``run_id`` values closed in this pass so the
    Airflow log surfaces them — operators chasing a missed alert can
    grep ``run_janitor`` and find which runs the platform reaped.
    """
    interval = f"{int(grace_minutes)} minutes"
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT r.run_id
                  FROM pipeline.run_log r
                 WHERE r.status = 'running'
                   AND r.started_at < NOW() - INTERVAL %s
                   AND NOT EXISTS (
                         SELECT 1
                           FROM pipeline.run_stage_log s
                          WHERE s.run_id = r.run_id
                            AND GREATEST(
                                  s.started_at,
                                  COALESCE(s.ended_at, s.started_at)
                                ) > NOW() - INTERVAL %s
                       )
                """,
                (interval, interval),
            )
            stuck = [row[0] for row in cur.fetchall()]
            if not stuck:
                return []

            # One stage row per killed run + one run_log update per row.
            # Both writes share this transaction so the dashboard never
            # observes a failed run without a matching kill marker.
            for run_id in stuck:
                cur.execute(
                    """
                    INSERT INTO pipeline.run_stage_log
                        (run_id, stage, status, event_type, attempt_number,
                         started_at, ended_at, error, metrics)
                    VALUES (%s, 'finalise', 'failed', 'stage_failed', 1,
                            NOW(), NOW(),
                            %s, %s::jsonb)
                    """,
                    (
                        run_id,
                        f"janitor_no_heartbeat: no stage activity for >{grace_minutes} min",
                        '{"reaped_by": "dag_run_janitor"}',
                    ),
                )
            cur.execute(
                """
                UPDATE pipeline.run_log
                   SET status = 'failed',
                       error_summary = 'janitor_no_heartbeat',
                       ended_at = NOW()
                 WHERE run_id = ANY(%s)
                """,
                (stuck,),
            )
    return stuck


with DAG(
    dag_id="dag_run_janitor",
    description=(
        "Heartbeat-staleness janitor — closes orphan runs left in "
        "status='running' when their worker process died without writing "
        "a terminal status. Runs every minute; cheap single-SQL pass."
    ),
    schedule="* * * * *",        # every minute
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,           # never let two reaper runs overlap
    tags=["control-plane", "janitor", "stateless"],
    default_args={
        "retries": 0,            # if Postgres is down, the next minute will retry
        "execution_timeout": timedelta(seconds=30),
    },
) as dag:

    @task
    def reap() -> list[str]:
        conn = _connect_pg()
        try:
            killed = reap_orphan_runs(conn)
        finally:
            conn.close()
        if killed:
            log.warning(
                "run_janitor.reaped count=%d run_ids=%s",
                len(killed),
                killed,
            )
        else:
            log.info("run_janitor.no_orphans")
        return killed

    reap()
