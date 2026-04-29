"""dag_ingest — orchestrate per-file ingestion: init_run -> stage_ingest -> stage_publish ->
wait_sinks -> finalise. Triggered by dag_drop_to_raw with `conf` carrying file_id, domain,
dataset, business_date.
"""
from __future__ import annotations

import os
import uuid

import pendulum
import psycopg2
from airflow import DAG
from airflow.decorators import task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from common.connect_admin import wait_until_offset_consumed
from common.run_log import insert_run_header, update_run_header, write_stage


PG_DSN = os.environ.get(
    "PIPELINE_PG_DSN",
    "host=postgres port=5432 dbname=ods_dev user=ods password=ods",
)


@task
def init_run(conf: dict) -> dict:
    run_id = str(uuid.uuid4())
    conn = psycopg2.connect(PG_DSN)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT config_version_id FROM pipeline.dataset_config
                 WHERE domain=%s AND dataset=%s
                """,
                (conf["domain"], conf["dataset"]),
            )
            row = cur.fetchone()
            if not row:
                raise RuntimeError(
                    f"dataset_config missing for {conf['domain']}/{conf['dataset']}"
                )
            config_version_id = row[0]
        insert_run_header(
            conn,
            run_id=run_id,
            pipeline_type="s3_batch",
            domain=conf["domain"],
            dataset=conf["dataset"],
            business_date=conf["business_date"],
            file_id=conf["file_id"],
            config_version_id=config_version_id,
        )
    finally:
        conn.close()
    return {**conf, "run_id": run_id, "config_version_id": config_version_id}


@task
def wait_sinks(ctx: dict) -> dict:
    conn = psycopg2.connect(PG_DSN)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT kafka_topic, kafka_offset_end FROM pipeline.run_log WHERE run_id=%s",
                (ctx["run_id"],),
            )
            row = cur.fetchone()
        if not row or row[0] is None or row[1] is None:
            raise RuntimeError(
                f"run_log row for {ctx['run_id']} missing kafka_topic/offset_end"
            )
        topic, target = row

        ok_jdbc = wait_until_offset_consumed("jdbc-sink-policies", topic, target)
        ok_s3 = wait_until_offset_consumed("s3-sink-policies", topic, target)

        write_stage(
            conn,
            run_id=ctx["run_id"],
            stage="sink_pg",
            status="succeeded" if ok_jdbc else "failed",
            output_ref=f"kafka://{topic}#consumed",
            error=None if ok_jdbc else "jdbc sink did not advance",
        )
        write_stage(
            conn,
            run_id=ctx["run_id"],
            stage="sink_s3",
            status="succeeded" if ok_s3 else "failed",
            output_ref=f"kafka://{topic}#consumed",
            error=None if ok_s3 else "s3 sink did not advance",
        )

        if not (ok_jdbc and ok_s3):
            update_run_header(conn, ctx["run_id"], status="partial")
            raise RuntimeError("sink wait failed")
    finally:
        conn.close()
    return ctx


@task
def finalise(ctx: dict) -> None:
    conn = psycopg2.connect(PG_DSN)
    try:
        update_run_header(conn, ctx["run_id"], status="succeeded")
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE pipeline.file_catalogue
                   SET state='sunk', state_updated_at=NOW(), last_run_id=%s
                 WHERE file_id=%s
                """,
                (ctx["run_id"], ctx["file_id"]),
            )
        conn.commit()
    finally:
        conn.close()


with DAG(
    dag_id="dag_ingest",
    start_date=pendulum.datetime(2026, 4, 28, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["ods"],
    params={
        "file_id": None,
        "domain": None,
        "dataset": None,
        "business_date": None,
    },
):
    ctx = init_run("{{ dag_run.conf }}")

    ingest = SparkSubmitOperator(
        task_id="stage_ingest",
        application="/opt/glue/jobs/ods_ingestion.py",
        application_args=[
            "--run-id", '{{ ti.xcom_pull(task_ids="init_run")["run_id"] }}',
            "--file-id", '{{ dag_run.conf["file_id"] }}',
            "--domain", '{{ dag_run.conf["domain"] }}',
            "--dataset", '{{ dag_run.conf["dataset"] }}',
            "--business-date", '{{ dag_run.conf["business_date"] }}',
        ],
        conn_id="spark_default",
    )

    publish = SparkSubmitOperator(
        task_id="stage_publish",
        application="/opt/glue/jobs/ods_s3_publish.py",
        application_args=[
            "--run-id", '{{ ti.xcom_pull(task_ids="init_run")["run_id"] }}',
            "--domain", '{{ dag_run.conf["domain"] }}',
            "--dataset", '{{ dag_run.conf["dataset"] }}',
            "--business-date", '{{ dag_run.conf["business_date"] }}',
        ],
        conn_id="spark_default",
    )

    waited = wait_sinks(ctx)
    fin = finalise(waited)

    ctx >> ingest >> publish >> waited >> fin
