"""dag_recon_t2 — hourly cross-plane reconciliation.

For each (domain, dataset, business_date) where a run completed in the lookback
window, compare:
  - source_count   = run_log.record_count_source
  - kafka_count    = run_log.kafka_offset_end - run_log.kafka_offset_start
  - postgres_count = SELECT count(*) FROM <postgres_target_table>
                       WHERE business_date = bd

Writes one reconciliation_log row per run with check_type='t2_full' and
status in ('passed','warning','failed') based on tolerance from dataset_config.
"""
from __future__ import annotations

import os

import pendulum
import psycopg2
from airflow import DAG
from airflow.decorators import task

PG_DSN = os.environ.get(
    "PIPELINE_PG_DSN",
    "host=postgres port=5432 dbname=ods_dev user=ods password=ods",
)
LOOKBACK_HOURS = int(os.environ.get("RECON_LOOKBACK_HOURS", "24"))


@task
def reconcile() -> None:
    conn = psycopg2.connect(PG_DSN)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT r.run_id, r.domain, r.dataset, r.business_date,
                       r.record_count_source,
                       COALESCE(r.kafka_offset_end - r.kafka_offset_start, 0) AS kafka_delta,
                       d.postgres_target_table,
                       COALESCE(d.recon_tolerance_records, 0)  AS tol_rec,
                       COALESCE(d.recon_tolerance_pct, 0)      AS tol_pct
                  FROM pipeline.run_log r
                  JOIN pipeline.dataset_config d
                       ON d.domain = r.domain AND d.dataset = r.dataset
                 WHERE r.status = 'succeeded'
                   AND r.started_at > NOW() - (%s || ' hours')::interval
                """,
                (str(LOOKBACK_HOURS),),
            )
            runs = cur.fetchall()

        for run_id, domain, dataset, bd, src, kafka, target_tbl, tol_rec, tol_pct in runs:
            schema, _, table = target_tbl.partition(".") if target_tbl else ("ods", ".", "unknown")
            with conn.cursor() as cur:
                cur.execute(
                    f'SELECT count(*) FROM {schema}."{table}" '
                    f'WHERE _ods_run_id = %s AND _ods_business_date = %s',
                    (str(run_id), str(bd)),
                )
                pg_count = cur.fetchone()[0]

            discrepancy = abs(int(src or 0) - int(pg_count))
            pct = (discrepancy / max(int(src or 0), 1)) * 100
            status = "passed"
            if discrepancy > tol_rec or pct > float(tol_pct):
                status = "failed"

            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO pipeline.reconciliation_log (
                        check_type, run_id, domain, dataset, business_date,
                        source_count, kafka_count, postgres_count,
                        discrepancy_count, discrepancy_pct, status, detail
                    ) VALUES ('t2_full', %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    (
                        run_id, domain, dataset, bd,
                        src, kafka, pg_count,
                        discrepancy, round(pct, 4), status,
                        f"src={src} kafka={kafka} pg={pg_count}",
                    ),
                )
            conn.commit()
    finally:
        conn.close()


with DAG(
    dag_id="dag_recon_t2",
    start_date=pendulum.datetime(2026, 4, 28, tz="UTC"),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["ods", "reconciliation"],
):
    reconcile()
