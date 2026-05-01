"""dag_multi_file — stage a slot file then trigger merge if all slots ready.

Triggered per-file by dag_drop_to_raw with conf:
    file_id, domain, dataset (slot dataset e.g. policies_core), business_date
"""
from __future__ import annotations

import os
import sys
import uuid

import pendulum
import psycopg2
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

# Add repo root to sys.path so ods_pipeline package is importable
_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

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
GLUE_IMAGE = os.environ.get("GLUE_IMAGE", "ods-glue:local")
GLUE_ENV = {
    "AWS_ACCESS_KEY_ID":      "test",
    "AWS_SECRET_ACCESS_KEY":  "test",
    "AWS_DEFAULT_REGION":     "eu-west-1",
    "LOCALSTACK_ENDPOINT":    "http://localstack:4566",
    "KAFKA_BOOTSTRAP_SERVERS":"broker:29092",
    "SCHEMA_REGISTRY_URL":    "http://schema-registry:8081",
    "POSTGRES_HOST":          "postgres",
    "POSTGRES_DB":            "ods_dev",
    "POSTGRES_USER":          "ods",
    "POSTGRES_PASSWORD":      "ods",
    "ENV":                    "local",
}


def _deterministic_merge_run_id(domain: str, dataset: str, business_date: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_OID, f"{domain}/{dataset}/{business_date}"))


@task
def init_run() -> dict:
    ctx = get_current_context()
    conf = dict(ctx["dag_run"].conf or {})
    for key in ("file_id", "domain", "dataset", "business_date"):
        if not conf.get(key):
            raise RuntimeError(f"dag_run.conf missing key: {key}")

    run_id = str(uuid.uuid4())
    conn = psycopg2.connect(PG_DSN)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT s3_raw_path FROM pipeline.file_catalogue WHERE file_id=%s",
                (conf["file_id"],),
            )
            row = cur.fetchone()
            if not row:
                raise RuntimeError(f"file_catalogue missing for file_id={conf['file_id']}")
            s3_raw_path = row[0]

            cur.execute(
                "SELECT slot_name, merge_dataset FROM pipeline.dataset_config "
                "WHERE domain=%s AND dataset=%s AND active=TRUE",
                (conf["domain"], conf["dataset"]),
            )
            cfg = cur.fetchone()
            if not cfg or not cfg[0]:
                raise RuntimeError(
                    f"{conf['domain']}/{conf['dataset']} is not a slot dataset"
                )
            slot_name, merge_dataset = cfg

        ods_pipeline.runs.start(
            conn,
            run_id=run_id,
            pipeline_type="stage",
            domain=conf["domain"],
            dataset=conf["dataset"],
            business_date=conf["business_date"],
            file_id=conf["file_id"],
        )
    finally:
        conn.close()

    return {
        **conf,
        "run_id":         run_id,
        "s3_raw_path":    s3_raw_path,
        "slot_name":      slot_name,
        "merge_dataset":  merge_dataset,
    }


@task
def check_all_slots_ready(ctx: dict) -> dict:
    conn = psycopg2.connect(PG_DSN)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT slot_name, dataset FROM pipeline.dataset_config "
                "WHERE domain=%s AND merge_dataset=%s AND active=TRUE",
                (ctx["domain"], ctx["merge_dataset"]),
            )
            required = {r[0]: r[1] for r in cur.fetchall()}

        ready: dict[str, str] = {}
        with conn.cursor() as cur:
            for slot_name, slot_dataset in required.items():
                cur.execute(
                    """
                    SELECT run_id::text FROM pipeline.run_log
                    WHERE domain=%s AND dataset=%s
                      AND business_date=%s AND status='succeeded'
                      AND pipeline_type='stage'
                    ORDER BY started_at DESC LIMIT 1
                    """,
                    (ctx["domain"], slot_dataset, ctx["business_date"]),
                )
                row = cur.fetchone()
                if row:
                    ready[slot_name] = row[0]
    finally:
        conn.close()

    all_ready = len(ready) == len(required)
    return {**ctx, "all_slots_ready": all_ready, "ready_slots": ready}


@task.branch
def route_merge(ctx: dict) -> str:
    return "prepare_merge" if ctx["all_slots_ready"] else "skip_merge"


@task
def skip_merge(ctx: dict) -> None:
    print(
        f"Slot '{ctx['slot_name']}' staged for {ctx['business_date']}. "
        f"Waiting for remaining slots before merge."
    )


@task
def prepare_merge(ctx: dict) -> dict:
    merge_run_id = _deterministic_merge_run_id(
        ctx["domain"], ctx["merge_dataset"], ctx["business_date"]
    )
    return {**ctx, "merge_run_id": merge_run_id}


@task
def finalise(ctx: dict) -> None:
    conn = psycopg2.connect(PG_DSN)
    try:
        ods_pipeline.runs.update(conn, ctx["run_id"], status="succeeded")
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE pipeline.file_catalogue "
                "SET state='staged', state_updated_at=NOW(), last_run_id=%s "
                "WHERE file_id=%s",
                (ctx["run_id"], ctx["file_id"]),
            )
        conn.commit()
    finally:
        conn.close()


with DAG(
    dag_id="dag_multi_file",
    start_date=pendulum.datetime(2026, 4, 28, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["ods", "multi-file"],
):
    ctx = init_run()

    stage = DockerOperator(
        task_id="stage_ingest",
        image=GLUE_IMAGE,
        network_mode="ods-network",
        auto_remove=True,
        mount_tmp_dir=False,
        command=(
            "spark-submit "
            "--py-files /home/glue_user/workspace/jobs/utils.py,"
            "/home/glue_user/workspace/jobs/dq.py "
            "/home/glue_user/workspace/jobs/ods_stage.py "
            "--run_id {{ ti.xcom_pull(task_ids='init_run')['run_id'] }} "
            "--domain {{ ti.xcom_pull(task_ids='init_run')['domain'] }} "
            "--dataset {{ ti.xcom_pull(task_ids='init_run')['dataset'] }} "
            "--s3_input_path {{ ti.xcom_pull(task_ids='init_run')['s3_raw_path'] }}"
        ),
        environment=GLUE_ENV,
        mounts=[
            Mount(source=GLUE_JOBS_PATH,
                  target="/home/glue_user/workspace/jobs", type="bind"),
            Mount(source=ODS_PIPELINE_PATH,
                  target="/home/glue_user/ods_pipeline", type="bind"),
        ],
    )

    slots_ready = check_all_slots_ready(ctx)
    branch = route_merge(slots_ready)
    skip = skip_merge(slots_ready)
    prep = prepare_merge(slots_ready)

    merge = DockerOperator(
        task_id="run_merge",
        image=GLUE_IMAGE,
        network_mode="ods-network",
        auto_remove=True,
        mount_tmp_dir=False,
        command=(
            "spark-submit "
            "--py-files /home/glue_user/workspace/jobs/utils.py,"
            "/home/glue_user/workspace/jobs/dq.py "
            "/home/glue_user/workspace/jobs/ods_merge.py "
            "--merge_run_id {{ ti.xcom_pull(task_ids='prepare_merge')['merge_run_id'] }} "
            "--domain {{ ti.xcom_pull(task_ids='init_run')['domain'] }} "
            "--dataset {{ ti.xcom_pull(task_ids='init_run')['merge_dataset'] }} "
            "--business_date {{ ti.xcom_pull(task_ids='init_run')['business_date'] }}"
        ),
        environment=GLUE_ENV,
        mounts=[
            Mount(source=GLUE_JOBS_PATH,
                  target="/home/glue_user/workspace/jobs", type="bind"),
            Mount(source=ODS_PIPELINE_PATH,
                  target="/home/glue_user/ods_pipeline", type="bind"),
        ],
    )

    fin = finalise(slots_ready)

    ctx >> stage >> slots_ready >> branch
    branch >> skip
    branch >> prep >> merge >> fin
