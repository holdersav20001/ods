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
from airflow.operators.python import get_current_context
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

from common.connect_admin import wait_until_offset_consumed
from common.run_log import insert_run_header, update_run_header, write_stage
from common.run_event_producer import produce_run_event


PG_DSN = os.environ.get(
    "PIPELINE_PG_DSN",
    "host=postgres port=5432 dbname=ods_dev user=ods password=ods",
)
GLUE_JOBS_PATH = os.environ.get(
    "GLUE_JOBS_PATH",
    "/c/Users/Holde/development/aviva ODS/glue/jobs",
)
GLUE_IMAGE = os.environ.get("GLUE_IMAGE", "ods-glue:local")
GLUE_ENV = {
    "AWS_ACCESS_KEY_ID": "test",
    "AWS_SECRET_ACCESS_KEY": "test",
    "AWS_DEFAULT_REGION": "eu-west-1",
    "LOCALSTACK_ENDPOINT": "http://localstack:4566",
    "KAFKA_BOOTSTRAP_SERVERS": "broker:29092",
    "SCHEMA_REGISTRY_URL": "http://schema-registry:8081",
    "POSTGRES_HOST": "postgres",
    "POSTGRES_DB": "ods_dev",
    "POSTGRES_USER": "ods",
    "POSTGRES_PASSWORD": "ods",
    "ENV": "local",
}


@task
def init_run() -> dict:
    ctx = get_current_context()
    dag_run = ctx["dag_run"]
    conf = dict(dag_run.conf or {})
    required = ("file_id", "domain", "dataset", "business_date")
    missing = [k for k in required if not conf.get(k)]
    if missing:
        raise RuntimeError(f"dag_run.conf missing keys: {missing}")
    run_id = str(uuid.uuid4())
    conn = psycopg2.connect(PG_DSN)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT s3_raw_path, s3_curated_path
                  FROM pipeline.file_catalogue
                 WHERE file_id=%s
                """,
                (conf["file_id"],),
            )
            row = cur.fetchone()
            if not row:
                raise RuntimeError(f"file_catalogue missing for file_id={conf['file_id']}")
            s3_raw_path, s3_curated_path = row

            cur.execute(
                """
                SELECT config_version_id, s3_curated_path
                  FROM pipeline.dataset_config
                 WHERE domain=%s AND dataset=%s
                """,
                (conf["domain"], conf["dataset"]),
            )
            cfg = cur.fetchone()
            if not cfg:
                raise RuntimeError(
                    f"dataset_config missing for {conf['domain']}/{conf['dataset']}"
                )
            config_version_id, dataset_curated_root = cfg
        if not s3_curated_path:
            s3_curated_path = (
                f"{dataset_curated_root.rstrip('/')}/date={conf['business_date']}/"
            )
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

    produce_run_event(
        "run_started",
        run_id=run_id,
        domain=conf["domain"],
        dataset=conf["dataset"],
        business_date=conf["business_date"],
        status="running",
    )

    return {
        **conf,
        "run_id": run_id,
        "config_version_id": config_version_id,
        "s3_raw_path": s3_raw_path,
        "s3_curated_path": s3_curated_path,
        "airflow_dag_id": dag_run.dag_id,
        "airflow_run_id": dag_run.run_id,
    }


@task
def wait_sinks(ctx: dict) -> dict:
    conn = psycopg2.connect(PG_DSN)
    try:
        try:
            return _wait_sinks_inner(conn, ctx)
        except Exception as exc:
            # Any unexpected error in the sink-wait path means we cannot
            # confirm the sinks landed; force the run to 'partial' so the
            # audit trail never silently retains the publish-job's
            # 'succeeded' status.
            try:
                update_run_header(conn, ctx["run_id"], status="partial",
                                  error_summary=f"wait_sinks aborted: {exc}")
                write_stage(
                    conn,
                    run_id=ctx["run_id"],
                    stage="sink_pg_wait",
                    status="failed",
                    event_type="stage_failed",
                    output_ref=None,
                    error=f"wait_sinks aborted: {exc}",
                    airflow_dag_id=ctx.get("airflow_dag_id"),
                    airflow_run_id=ctx.get("airflow_run_id"),
                )
            except Exception:
                pass
            try:
                produce_run_event(
                    "run_partial",
                    run_id=ctx["run_id"],
                    domain=ctx["domain"],
                    dataset=ctx["dataset"],
                    business_date=ctx["business_date"],
                    status="partial",
                )
            except Exception:
                pass
            raise
    finally:
        conn.close()


def _wait_sinks_inner(conn, ctx: dict) -> dict:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT kafka_topic, kafka_offset_end FROM pipeline.run_log WHERE run_id=%s",
            (ctx["run_id"],),
        )
        row = cur.fetchone()
    if not row or row[0] is None or row[1] is None:
        update_run_header(conn, ctx["run_id"], status="partial")
        write_stage(
            conn,
            run_id=ctx["run_id"],
            stage="sink_pg_wait",
            status="failed",
            event_type="stage_failed",
            output_ref=None,
            error="kafka_topic/offset_end missing — publish stage did not run",
            airflow_dag_id=ctx.get("airflow_dag_id"),
            airflow_run_id=ctx.get("airflow_run_id"),
        )
        raise RuntimeError(
            f"run_log row for {ctx['run_id']} missing kafka_topic/offset_end"
        )
    topic, target = row

    ok_jdbc = wait_until_offset_consumed("jdbc-sink-policies", topic, target)
    ok_s3 = wait_until_offset_consumed("s3-sink-policies", topic, target)

    write_stage(
        conn,
        run_id=ctx["run_id"],
        stage="sink_pg_wait",
        status="succeeded" if ok_jdbc else "failed",
        event_type="stage_completed" if ok_jdbc else "stage_failed",
        output_ref=f"kafka://{topic}#consumed",
        error=None if ok_jdbc else "jdbc sink did not advance",
        airflow_dag_id=ctx.get("airflow_dag_id"),
        airflow_run_id=ctx.get("airflow_run_id"),
    )
    write_stage(
        conn,
        run_id=ctx["run_id"],
        stage="sink_s3_wait",
        status="succeeded" if ok_s3 else "failed",
        event_type="stage_completed" if ok_s3 else "stage_failed",
        output_ref=f"kafka://{topic}#consumed",
        error=None if ok_s3 else "s3 sink did not advance",
        airflow_dag_id=ctx.get("airflow_dag_id"),
        airflow_run_id=ctx.get("airflow_run_id"),
    )

    if not (ok_jdbc and ok_s3):
        update_run_header(conn, ctx["run_id"], status="partial")
        raise RuntimeError("sink wait failed")
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

    produce_run_event(
        "run_succeeded",
        run_id=ctx["run_id"],
        domain=ctx["domain"],
        dataset=ctx["dataset"],
        business_date=ctx["business_date"],
        status="succeeded",
    )


with DAG(
    dag_id="dag_ingest",
    start_date=pendulum.datetime(2026, 4, 28, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["ods"],
):
    ctx = init_run()

    ingest = DockerOperator(
        task_id="stage_ingest",
        image=GLUE_IMAGE,
        network_mode="ods-network",
        auto_remove=True,
        mount_tmp_dir=False,
        command=(
            "spark-submit "
            "--py-files /home/glue_user/workspace/jobs/utils.py,"
            "/home/glue_user/workspace/jobs/dq.py "
            "/home/glue_user/workspace/jobs/ods_ingestion.py "
            "--run_id {{ ti.xcom_pull(task_ids='init_run')['run_id'] }} "
            "--domain {{ ti.xcom_pull(task_ids='init_run')['domain'] }} "
            "--dataset {{ ti.xcom_pull(task_ids='init_run')['dataset'] }} "
            "--s3_input_path {{ ti.xcom_pull(task_ids='init_run')['s3_raw_path'] }} "
            "--file_id {{ ti.xcom_pull(task_ids='init_run')['file_id'] }} "
            "--airflow_dag_id {{ dag.dag_id }} "
            "--airflow_run_id {{ run_id }}"
        ),
        environment=GLUE_ENV,
        mounts=[Mount(source=GLUE_JOBS_PATH, target="/home/glue_user/workspace/jobs", type="bind")],
    )

    publish = DockerOperator(
        task_id="stage_publish",
        image=GLUE_IMAGE,
        network_mode="ods-network",
        auto_remove=True,
        mount_tmp_dir=False,
        command=(
            "spark-submit "
            "--py-files /home/glue_user/workspace/jobs/utils.py,"
            "/home/glue_user/workspace/jobs/dq.py "
            "/home/glue_user/workspace/jobs/ods_s3_publish.py "
            "--run_id {{ ti.xcom_pull(task_ids='init_run')['run_id'] }} "
            "--domain {{ ti.xcom_pull(task_ids='init_run')['domain'] }} "
            "--dataset {{ ti.xcom_pull(task_ids='init_run')['dataset'] }} "
            "--s3_input_path {{ ti.xcom_pull(task_ids='init_run')['s3_curated_path'] }} "
            "--file_id {{ ti.xcom_pull(task_ids='init_run')['file_id'] }} "
            "--airflow_dag_id {{ dag.dag_id }} "
            "--airflow_run_id {{ run_id }}"
        ),
        environment=GLUE_ENV,
        mounts=[Mount(source=GLUE_JOBS_PATH, target="/home/glue_user/workspace/jobs", type="bind")],
    )

    waited = wait_sinks(ctx)
    fin = finalise(waited)

    ctx >> ingest >> publish >> waited >> fin
