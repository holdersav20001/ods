"""dag_ingest — orchestrate per-file ingestion: init_run -> stage_ingest -> stage_publish ->
wait_sinks -> finalise. Triggered by dag_drop_to_raw with `conf` carrying file_id, domain,
dataset, business_date.
"""
from __future__ import annotations

import os
import sys
import uuid
import json

import pendulum
import psycopg2
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import get_current_context
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.trigger_rule import TriggerRule
from docker.types import Mount

# Add repo root to sys.path so ods_pipeline package is importable
_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

import ods_pipeline

from common.connect_admin import wait_until_offset_consumed


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
    parent_run_id = str(uuid.uuid4())
    ingest_run_id = str(uuid.uuid4())
    publish_run_id = str(uuid.uuid4())
    canonicalize_run_id = str(uuid.uuid4())
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
                SELECT config_version_id, s3_curated_path,
                       is_canonical, canonical_topic, transform_yaml_path
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
            (config_version_id, dataset_curated_root, is_canonical,
             canonical_topic, transform_yaml_path) = cfg
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
        "publish_run_id": publish_run_id,
        "canonicalize_run_id": canonicalize_run_id,
        "config_version_id": config_version_id,
        "s3_raw_path": s3_raw_path,
        "s3_curated_path": s3_curated_path,
        "is_canonical": bool(is_canonical),
        "canonical_topic": canonical_topic,
        "transform_yaml_path": transform_yaml_path,
        "airflow_dag_id": dag_run.dag_id,
        "airflow_run_id": dag_run.run_id,
    }


@task
def wait_sinks(ctx: dict) -> dict:
    publish_run_id = ctx["publish_run_id"]
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
                ods_pipeline.runs.update(conn, publish_run_id, status="partial",
                                         error_summary=f"wait_sinks aborted: {exc}")
                ods_pipeline.runs.update(conn, ctx["run_id"], status="partial",
                                         error_summary=f"wait_sinks aborted: {exc}")
                ods_pipeline.stages.write(
                    conn,
                    run_id=publish_run_id,
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
                ods_pipeline.events.produce(
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
    publish_run_id = ctx.get("sink_run_id") or ctx["publish_run_id"]
    with conn.cursor() as cur:
        cur.execute(
            "SELECT kafka_topic, kafka_offset_end FROM pipeline.run_log WHERE run_id=%s",
            (publish_run_id,),
        )
        row = cur.fetchone()
    if not row or row[0] is None or row[1] is None:
        ods_pipeline.runs.update(conn, publish_run_id, status="partial")
        ods_pipeline.runs.update(conn, ctx["run_id"], status="partial")
        ods_pipeline.stages.write(
            conn,
            run_id=publish_run_id,
            stage="sink_pg_wait",
            status="failed",
            event_type="stage_failed",
            output_ref=None,
            error="kafka_topic/offset_end missing — publish stage did not run",
            airflow_dag_id=ctx.get("airflow_dag_id"),
            airflow_run_id=ctx.get("airflow_run_id"),
        )
        raise RuntimeError(
            f"run_log row for {publish_run_id} missing kafka_topic/offset_end"
        )
    topic, target = row

    jdbc_connector = (
        "jdbc-sink-policies"
        if ctx["domain"] == "insurance" and ctx["dataset"] == "policies"
        else f"jdbc-sink-{ctx['domain']}-{ctx['dataset']}".replace("_", "-")
    )
    ok_jdbc = wait_until_offset_consumed(jdbc_connector, topic, target)
    ok_s3 = True
    if ctx["domain"] == "insurance" and ctx["dataset"] == "policies":
        ok_s3 = wait_until_offset_consumed("s3-sink-policies", topic, target)

    ods_pipeline.stages.write(
        conn,
        run_id=publish_run_id,
        stage="sink_pg_wait",
        status="succeeded" if ok_jdbc else "failed",
        event_type="stage_completed" if ok_jdbc else "stage_failed",
        output_ref=f"kafka://{topic}#consumed",
        error=None if ok_jdbc else "jdbc sink did not advance",
        airflow_dag_id=ctx.get("airflow_dag_id"),
        airflow_run_id=ctx.get("airflow_run_id"),
    )
    ods_pipeline.stages.write(
        conn,
        run_id=publish_run_id,
        stage="sink_s3_wait",
        status="succeeded" if ok_s3 else "failed",
        event_type="stage_completed" if ok_s3 else "stage_failed",
        output_ref=f"kafka://{topic}#consumed",
        error=None if ok_s3 else "s3 sink did not advance",
        airflow_dag_id=ctx.get("airflow_dag_id"),
        airflow_run_id=ctx.get("airflow_run_id"),
    )

    if not (ok_jdbc and ok_s3):
        ods_pipeline.runs.update(conn, publish_run_id, status="partial")
        ods_pipeline.runs.update(conn, ctx["run_id"], status="partial")
        raise RuntimeError("sink wait failed")
    return ctx


@task.branch
def route_canonicalize(ctx: dict) -> str:
    return "skip_canonicalize" if ctx.get("is_canonical", True) else "stage_canonicalize"


@task
def prepare_canonicalize(ctx: dict) -> dict:
    if ctx.get("is_canonical", True):
        return ctx

    conn = psycopg2.connect(PG_DSN)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT kafka_topic
                  FROM pipeline.run_log
                 WHERE run_id=%s
                """,
                (ctx["publish_run_id"],),
            )
            topic_row = cur.fetchone()
            cur.execute(
                """
                SELECT metrics
                  FROM pipeline.run_stage_log
                 WHERE run_id=%s AND stage='kafka_publish'
                   AND event_type='stage_completed'
                 ORDER BY id DESC
                 LIMIT 1
                """,
                (ctx["publish_run_id"],),
            )
            metrics_row = cur.fetchone()
    finally:
        conn.close()

    if not topic_row or not topic_row[0]:
        raise RuntimeError("publish run did not record raw kafka topic")
    if not metrics_row or not metrics_row[0]:
        raise RuntimeError("publish stage did not record offset metrics")
    metrics = metrics_row[0]
    starts = metrics.get("offset_start_by_partition") or {"0": metrics["offset_start"]}
    ends = metrics.get("offset_end_by_partition") or {"0": metrics["offset_end"]}
    ranges = {
        str(partition): {
            "start": int(starts.get(str(partition), starts.get(partition, 0))),
            "end": int(end),
        }
        for partition, end in ends.items()
    }
    return {
        **ctx,
        "raw_topic": topic_row[0],
        "offset_ranges": json.dumps(ranges, sort_keys=True),
    }


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def select_sink_run(ctx: dict) -> dict:
    if ctx.get("is_canonical", True):
        return {**ctx, "sink_run_id": ctx["publish_run_id"]}
    return {**ctx, "sink_run_id": ctx["canonicalize_run_id"]}


@task
def finalise(ctx: dict) -> None:
    conn = psycopg2.connect(PG_DSN)
    try:
        ods_pipeline.runs.update(conn, ctx["run_id"], status="succeeded")
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

    ods_pipeline.events.produce(
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
        mounts=[
            Mount(source=GLUE_JOBS_PATH, target="/home/glue_user/workspace/jobs", type="bind"),
            Mount(source=ODS_PIPELINE_PATH, target="/home/glue_user/ods_pipeline", type="bind"),
        ],
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
            "--run_id {{ ti.xcom_pull(task_ids='init_run')['publish_run_id'] }} "
            "--domain {{ ti.xcom_pull(task_ids='init_run')['domain'] }} "
            "--dataset {{ ti.xcom_pull(task_ids='init_run')['dataset'] }} "
            "--s3_input_path {{ ti.xcom_pull(task_ids='init_run')['s3_curated_path'] }} "
            "--file_id {{ ti.xcom_pull(task_ids='init_run')['file_id'] }} "
            "--parent_run_id {{ ti.xcom_pull(task_ids='init_run')['parent_run_id'] }} "
            "--airflow_dag_id {{ dag.dag_id }} "
            "--airflow_run_id {{ run_id }}"
        ),
        environment=GLUE_ENV,
        mounts=[
            Mount(source=GLUE_JOBS_PATH, target="/home/glue_user/workspace/jobs", type="bind"),
            Mount(source=ODS_PIPELINE_PATH, target="/home/glue_user/ods_pipeline", type="bind"),
        ],
    )

    prepared = prepare_canonicalize(ctx)
    branch = route_canonicalize(prepared)
    skip_canonicalize = EmptyOperator(task_id="skip_canonicalize")

    canonicalize = DockerOperator(
        task_id="stage_canonicalize",
        image=GLUE_IMAGE,
        network_mode="ods-network",
        auto_remove=True,
        mount_tmp_dir=False,
        command=(
            "spark-submit "
            "--py-files /home/glue_user/workspace/jobs/utils.py,"
            "/home/glue_user/workspace/jobs/dq.py,"
            "/home/glue_user/workspace/jobs/canonicalize.py "
            "/home/glue_user/workspace/jobs/ods_canonicalize.py "
            "--run_id {{ ti.xcom_pull(task_ids='prepare_canonicalize')['canonicalize_run_id'] }} "
            "--domain {{ ti.xcom_pull(task_ids='prepare_canonicalize')['domain'] }} "
            "--dataset {{ ti.xcom_pull(task_ids='prepare_canonicalize')['dataset'] }} "
            "--raw_topic {{ ti.xcom_pull(task_ids='prepare_canonicalize')['raw_topic'] }} "
            "--canonical_topic {{ ti.xcom_pull(task_ids='prepare_canonicalize')['canonical_topic'] }} "
            "--transform_yaml_path {{ ti.xcom_pull(task_ids='prepare_canonicalize')['transform_yaml_path'] }} "
            "--offset_ranges '{{ ti.xcom_pull(task_ids='prepare_canonicalize')['offset_ranges'] }}' "
            "--file_id {{ ti.xcom_pull(task_ids='prepare_canonicalize')['file_id'] }} "
            "--parent_run_id {{ ti.xcom_pull(task_ids='prepare_canonicalize')['publish_run_id'] }} "
            "--business_date {{ ti.xcom_pull(task_ids='prepare_canonicalize')['business_date'] }}"
        ),
        environment=GLUE_ENV,
        mounts=[
            Mount(source=GLUE_JOBS_PATH, target="/home/glue_user/workspace/jobs", type="bind"),
            Mount(source=ODS_PIPELINE_PATH, target="/home/glue_user/ods_pipeline", type="bind"),
            Mount(source=PATTERNS_PATH, target="/home/glue_user/patterns", type="bind"),
        ],
    )

    selected = select_sink_run(prepared)
    waited = wait_sinks(selected)
    fin = finalise(waited)

    ctx >> ingest >> publish >> prepared >> branch
    branch >> skip_canonicalize >> selected
    branch >> canonicalize >> selected
    selected >> waited >> fin
