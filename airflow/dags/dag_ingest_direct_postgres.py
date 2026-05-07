"""dag_ingest_direct_postgres — file pattern, Postgres-only sink.

Companion to ``dag_ingest`` for ``dataset_config.delivery='direct_postgres'``
datasets. Skips Kafka publish, canonicalize, and the JDBC Connect
sink. Just:

    init_run (parent + ingestion + direct_postgres child runs)
      -> stage_ingest         (Glue ods_ingestion: CSV/JSONL → Parquet)
      -> stage_postgres_write (Glue ods_postgres_write: Parquet → PG)
      -> finalise             (mark parent succeeded, advance file_catalogue)

Triggered by ``dag_drop_to_raw`` for any ``source_type='s3_batch'``
dataset whose ``dataset_config.delivery='direct_postgres'``.
"""
from __future__ import annotations

import os
import sys
import uuid

import pendulum
import psycopg2
from common.long_running_docker import make_long_running_docker_operator  # R8

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.trigger_rule import TriggerRule
from docker.types import Mount

_DAG_DIR = os.path.dirname(__file__)
for _root in (
    os.path.abspath(os.path.join(_DAG_DIR, "..")),
    os.path.abspath(os.path.join(_DAG_DIR, "..", "..")),
):
    if _root not in sys.path:
        sys.path.insert(0, _root)

import ods_pipeline

PG_DSN = os.environ.get(
    "PIPELINE_PG_DSN",
    "host=postgres port=5432 dbname=ods_dev user=ods password=ods",
)
GLUE_JOBS_PATH = os.environ.get(
    "GLUE_JOBS_PATH",
    "/c/Users/Holde/development/aviva ODS/glue/jobs",
)
ODS_PIPELINE_PATH = os.environ.get(
    "ODS_PIPELINE_PATH",
    "/c/Users/Holde/development/aviva ODS/ods_pipeline",
)
PATTERNS_PATH = os.environ.get(
    "PATTERNS_PATH",
    "/c/Users/Holde/development/aviva ODS/patterns",
)
GLUE_IMAGE = os.environ.get("GLUE_IMAGE", "ods-glue:local")
# Postgres JDBC driver is baked into ods-glue:local (see glue/Dockerfile).
GLUE_ENV = {
    "AWS_ACCESS_KEY_ID": "test",
    "AWS_SECRET_ACCESS_KEY": "test",
    "AWS_DEFAULT_REGION": "eu-west-1",
    "LOCALSTACK_ENDPOINT": "http://localstack:4566",
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

    parent_run_id = conf.get("parent_run_id") or str(uuid.uuid4())
    ingest_run_id = conf.get("ingest_run_id") or str(uuid.uuid4())
    pg_write_run_id = conf.get("pg_write_run_id") or str(uuid.uuid4())

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
                raise RuntimeError(
                    f"file_catalogue missing for file_id={conf['file_id']}"
                )
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

        ods_pipeline.runs.start(
            conn,
            run_id=parent_run_id,
            pipeline_type="s3_batch",
            domain=conf["domain"],
            dataset=conf["dataset"],
            business_date=conf["business_date"],
            file_id=conf["file_id"],
            config_version_id=config_version_id,
        )
        ods_pipeline.runs.start(
            conn,
            run_id=ingest_run_id,
            pipeline_type="ingestion",
            domain=conf["domain"],
            dataset=conf["dataset"],
            business_date=conf["business_date"],
            file_id=conf["file_id"],
            config_version_id=config_version_id,
            parents=[{"run_id": parent_run_id, "edge_type": "orchestrates"}],
        )
        ods_pipeline.runs.start(
            conn,
            run_id=pg_write_run_id,
            pipeline_type="direct_postgres",
            domain=conf["domain"],
            dataset=conf["dataset"],
            business_date=conf["business_date"],
            file_id=conf["file_id"],
            config_version_id=config_version_id,
            parents=[{"run_id": parent_run_id, "edge_type": "orchestrates"}],
        )

        ods_pipeline.lineage.write_edge(
            conn,
            child_run_id=ingest_run_id,
            parent_run_id=parent_run_id,
            parent_file_id=conf["file_id"],
            edge_type="raw_to_curated",
        )
    finally:
        conn.close()

    ods_pipeline.events.produce(
        "run_started",
        run_id=parent_run_id,
        domain=conf["domain"],
        dataset=conf["dataset"],
        business_date=conf["business_date"],
        status="running",
    )

    return {
        **conf,
        "run_id": parent_run_id,
        "parent_run_id": parent_run_id,
        "ingest_run_id": ingest_run_id,
        "pg_write_run_id": pg_write_run_id,
        "s3_raw_path": s3_raw_path,
        "s3_curated_path": s3_curated_path,
        "airflow_dag_id": dag_run.dag_id,
        "airflow_run_id": dag_run.run_id,
    }


@task(trigger_rule=TriggerRule.ALL_DONE)
def finalise(ctx: dict) -> None:
    conn = psycopg2.connect(PG_DSN)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT run_id::text, status FROM pipeline.run_log "
                "WHERE run_id::text = ANY(%s)",
                ([ctx["run_id"], ctx["ingest_run_id"], ctx["pg_write_run_id"]],),
            )
            statuses = {rid: status for rid, status in cur.fetchall()}

        ingest_status = statuses.get(ctx["ingest_run_id"], "running")
        pg_write_status = statuses.get(ctx["pg_write_run_id"], "running")

        if ingest_status != "succeeded":
            ods_pipeline.runs.update(
                conn, ctx["run_id"],
                status="failed",
                error_summary=f"ingestion child ended {ingest_status}",
            )
            final_status = "failed"
        elif pg_write_status != "succeeded":
            ods_pipeline.runs.update(
                conn, ctx["run_id"],
                status="failed",
                error_summary=f"direct_postgres child ended {pg_write_status}",
            )
            final_status = "failed"
        else:
            ods_pipeline.runs.update(conn, ctx["run_id"], status="succeeded")
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE pipeline.file_catalogue "
                    "SET state='sunk', state_updated_at=NOW(), last_run_id=%s "
                    "WHERE file_id=%s",
                    (ctx["pg_write_run_id"], ctx["file_id"]),
                )
            conn.commit()
            final_status = "succeeded"

        if final_status == "failed":
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE pipeline.file_catalogue "
                    "SET state='failed', state_updated_at=NOW(), last_run_id=%s "
                    "WHERE file_id=%s",
                    (ctx["run_id"], ctx["file_id"]),
                )
            conn.commit()
    finally:
        conn.close()

    ods_pipeline.events.produce(
        "run_succeeded" if final_status == "succeeded" else f"run_{final_status}",
        run_id=ctx["run_id"],
        domain=ctx["domain"],
        dataset=ctx["dataset"],
        business_date=ctx["business_date"],
        status=final_status,
    )


with DAG(
    dag_id="dag_ingest_direct_postgres",
    start_date=pendulum.datetime(2026, 5, 5, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["ods", "direct_postgres"],
):
    ctx = init_run()

    _glue_mounts = [
        Mount(source=GLUE_JOBS_PATH, target="/home/glue_user/workspace/jobs", type="bind"),
        Mount(source=ODS_PIPELINE_PATH, target="/home/glue_user/ods_pipeline", type="bind"),
        Mount(source=PATTERNS_PATH, target="/home/glue_user/patterns", type="bind"),
    ]

    ingest = DockerOperator(
        task_id="stage_ingest",
        image=GLUE_IMAGE,
        network_mode="ods-network",
        auto_remove=True,
        mount_tmp_dir=False,
        command=(
            "spark-submit "
            "--py-files /home/glue_user/workspace/jobs/utils.py,"
            "/home/glue_user/workspace/jobs/utils_bootstrap.py,"
            "/home/glue_user/workspace/jobs/utils_data.py,"
            "/home/glue_user/workspace/jobs/utils_config.py,"
            "/home/glue_user/workspace/jobs/utils_state.py,"
            "/home/glue_user/workspace/jobs/utils_runs.py,"
            "/home/glue_user/workspace/jobs/utils_jobs.py,"
            "/home/glue_user/workspace/jobs/dq.py "
            "/home/glue_user/workspace/jobs/ods_ingestion.py "
            "--run_id {{ ti.xcom_pull(task_ids='init_run')['ingest_run_id'] }} "
            "--domain {{ ti.xcom_pull(task_ids='init_run')['domain'] }} "
            "--dataset {{ ti.xcom_pull(task_ids='init_run')['dataset'] }} "
            "--s3_input_path {{ ti.xcom_pull(task_ids='init_run')['s3_raw_path'] }} "
            "--file_id {{ ti.xcom_pull(task_ids='init_run')['file_id'] }} "
            "--parent_run_id {{ ti.xcom_pull(task_ids='init_run')['parent_run_id'] }} "
            "--airflow_dag_id {{ dag.dag_id }} "
            "--airflow_run_id {{ run_id }}"
        ),
        environment=GLUE_ENV,
        mounts=_glue_mounts,
    )

    # R8: bulk Parquet -> Postgres writes for historic backfills run for
    # hours; swap in the long-running wrapper for heartbeat + force-stop.
    pg_write = make_long_running_docker_operator(
        task_id="stage_postgres_write",
        image=GLUE_IMAGE,
        network_mode="ods-network",
        auto_remove=True,
        mount_tmp_dir=False,
        command=(
            "spark-submit "
            "--py-files /home/glue_user/workspace/jobs/utils.py,"
            "/home/glue_user/workspace/jobs/utils_bootstrap.py,"
            "/home/glue_user/workspace/jobs/utils_data.py,"
            "/home/glue_user/workspace/jobs/utils_config.py,"
            "/home/glue_user/workspace/jobs/utils_state.py,"
            "/home/glue_user/workspace/jobs/utils_runs.py,"
            "/home/glue_user/workspace/jobs/utils_jobs.py,"
            "/home/glue_user/workspace/jobs/dq.py "
            "/home/glue_user/workspace/jobs/ods_postgres_write.py "
            "--run_id {{ ti.xcom_pull(task_ids='init_run')['pg_write_run_id'] }} "
            "--domain {{ ti.xcom_pull(task_ids='init_run')['domain'] }} "
            "--dataset {{ ti.xcom_pull(task_ids='init_run')['dataset'] }} "
            "--s3_input_path {{ ti.xcom_pull(task_ids='init_run')['s3_curated_path'] }} "
            "--file_id {{ ti.xcom_pull(task_ids='init_run')['file_id'] }} "
            "--parent_run_id {{ ti.xcom_pull(task_ids='init_run')['parent_run_id'] }} "
            "--airflow_dag_id {{ dag.dag_id }} "
            "--airflow_run_id {{ run_id }}"
        ),
        environment=GLUE_ENV,
        mounts=_glue_mounts,
        heartbeat_seconds=30,
        soft_timeout_minutes=240,
        poll_interval_seconds=10,
        run_id_xcom_task="init_run",
        run_id_xcom_key="pg_write_run_id",
    )

    fin = finalise(ctx)
    ctx >> ingest >> pg_write >> fin
    [ingest, pg_write] >> fin
