"""dag_ingest - orchestrate per-file ingestion.

Flow: init_run -> stage_ingest -> stage_publish -> optional stage_canonicalize
-> wait_sinks -> finalise. Triggered by dag_drop_to_raw with ``conf`` carrying
file_id, domain, dataset, and business_date.
"""
from __future__ import annotations

import json
import os
import sys
import uuid

import pendulum
import psycopg2
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import get_current_context
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.trigger_rule import TriggerRule
from docker.types import Mount

# Add likely roots so ods_pipeline is importable both locally and in Airflow.
_DAG_DIR = os.path.dirname(__file__)
for _root in (
    os.path.abspath(os.path.join(_DAG_DIR, "..")),
    os.path.abspath(os.path.join(_DAG_DIR, "..", "..")),
):
    if _root not in sys.path:
        sys.path.insert(0, _root)

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
                SELECT config_version_id, s3_curated_path, target_topic,
                       COALESCE(is_canonical, TRUE), canonical_topic,
                       transform_yaml_path
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
            (
                config_version_id,
                dataset_curated_root,
                target_topic,
                is_canonical,
                canonical_topic,
                transform_yaml_path,
            ) = cfg

        if not s3_curated_path:
            s3_curated_path = (
                f"{dataset_curated_root.rstrip('/')}/date={conf['business_date']}/"
            )
        is_canonical = bool(is_canonical)

        # parents is a generic linkage list. Callers may declare:
        #   - replay_of_run_id          : retry/replay edge to a prior run
        #   - triggered_by_run_id +
        #     triggered_by_edge_type    : explicit "this dag_ingest run was
        #                                 launched by THAT control-plane run".
        # The triggered_by edge keeps dag_ingest source-pattern agnostic
        # while letting upstream DAGs (dag_api_pull today) prove which
        # downstream execution to observe — preventing replay-on-the-same
        # file_id from racing the wrong run state into a watermark commit.
        parent_links: list[dict] = []
        if conf.get("replay_of_run_id"):
            parent_links.append({
                "run_id": conf["replay_of_run_id"],
                "edge_type": "replay",
                "replay_request_id": conf.get("replay_request_id"),
            })
        if conf.get("triggered_by_run_id"):
            parent_links.append({
                "run_id": conf["triggered_by_run_id"],
                "edge_type": conf.get("triggered_by_edge_type", "triggered_by"),
            })
        ods_pipeline.runs.start(
            conn,
            run_id=parent_run_id,
            pipeline_type="s3_batch",
            domain=conf["domain"],
            dataset=conf["dataset"],
            business_date=conf["business_date"],
            file_id=conf["file_id"],
            config_version_id=config_version_id,
            parents=parent_links or None,
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
            run_id=publish_run_id,
            pipeline_type="publish",
            domain=conf["domain"],
            dataset=conf["dataset"],
            business_date=conf["business_date"],
            file_id=conf["file_id"],
            kafka_topic=target_topic,
            config_version_id=config_version_id,
            parents=[{"run_id": parent_run_id, "edge_type": "orchestrates"}],
        )
        if not is_canonical:
            if not canonical_topic or not transform_yaml_path:
                raise RuntimeError(
                    "non-canonical dataset requires canonical_topic and transform_yaml_path"
                )
            ods_pipeline.runs.start(
                conn,
                run_id=canonicalize_run_id,
                pipeline_type="canonicalize",
                domain=conf["domain"],
                dataset=conf["dataset"],
                business_date=conf["business_date"],
                file_id=conf["file_id"],
                kafka_topic=canonical_topic,
                config_version_id=config_version_id,
                parents=[{"run_id": publish_run_id, "edge_type": "raw_to_canonical"}],
            )

        ods_pipeline.lineage.write_edge(
            conn,
            child_run_id=ingest_run_id,
            parent_run_id=parent_run_id,
            parent_file_id=conf["file_id"],
            edge_type="raw_to_curated",
        )
        ods_pipeline.lineage.write_edge(
            conn,
            child_run_id=publish_run_id,
            parent_run_id=parent_run_id,
            parent_file_id=conf["file_id"],
            edge_type="curated_to_kafka",
        )
        if conf.get("replay_of_run_id"):
            ods_pipeline.lineage.write_edge(
                conn,
                child_run_id=parent_run_id,
                parent_run_id=conf["replay_of_run_id"],
                parent_file_id=conf["file_id"],
                edge_type="replay",
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
        "target_topic": target_topic,
        "s3_raw_path": s3_raw_path,
        "s3_curated_path": s3_curated_path,
        "is_canonical": is_canonical,
        "canonical_topic": canonical_topic,
        "transform_yaml_path": transform_yaml_path,
        "airflow_dag_id": dag_run.dag_id,
        "airflow_run_id": dag_run.run_id,
    }


@task
def wait_sinks(ctx: dict) -> dict:
    sink_run_id = ctx.get("sink_run_id") or ctx["publish_run_id"]
    conn = psycopg2.connect(PG_DSN)
    try:
        try:
            return _wait_sinks_inner(conn, ctx)
        except Exception as exc:
            try:
                ods_pipeline.runs.update(
                    conn,
                    sink_run_id,
                    status="partial",
                    error_summary=f"wait_sinks aborted: {exc}",
                )
                ods_pipeline.runs.update(
                    conn,
                    ctx["run_id"],
                    status="partial",
                    error_summary=f"wait_sinks aborted: {exc}",
                )
                ods_pipeline.stages.write(
                    conn,
                    run_id=sink_run_id,
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


def _sink_target_offsets_by_partition(conn, run_id: str) -> dict[int, int] | None:
    """Read partition end offsets captured by publish/canonicalize stages."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT metrics
              FROM pipeline.run_stage_log
             WHERE run_id=%s
               AND stage IN ('kafka_publish', 'recon_t1')
               AND event_type IN ('stage_completed', 'stage_warned')
             ORDER BY ended_at DESC NULLS LAST, id DESC
             LIMIT 5
            """,
            (run_id,),
        )
        rows = cur.fetchall()

    for (metrics,) in rows:
        if not metrics:
            continue
        if isinstance(metrics, str):
            metrics = json.loads(metrics)
        raw_offsets = (
            metrics.get("canonical_offset_end")
            or metrics.get("produced_offset_end_by_partition")
            or metrics.get("offset_end_by_partition")
        )
        if raw_offsets:
            return {int(partition): int(offset) for partition, offset in raw_offsets.items()}
    return None


def _wait_sinks_inner(conn, ctx: dict) -> dict:
    sink_run_id = ctx.get("sink_run_id") or ctx["publish_run_id"]
    with conn.cursor() as cur:
        cur.execute(
            "SELECT kafka_topic, kafka_offset_end FROM pipeline.run_log WHERE run_id=%s",
            (sink_run_id,),
        )
        row = cur.fetchone()
    if not row or row[0] is None or row[1] is None:
        ods_pipeline.runs.update(conn, sink_run_id, status="partial")
        ods_pipeline.runs.update(conn, ctx["run_id"], status="partial")
        ods_pipeline.stages.write(
            conn,
            run_id=sink_run_id,
            stage="sink_pg_wait",
            status="failed",
            event_type="stage_failed",
            output_ref=None,
            error="kafka_topic/offset_end missing - publish/canonicalize stage did not run",
            airflow_dag_id=ctx.get("airflow_dag_id"),
            airflow_run_id=ctx.get("airflow_run_id"),
        )
        raise RuntimeError(f"run_log row for {sink_run_id} missing kafka_topic/offset_end")
    topic, target = row
    target_offsets_by_partition = _sink_target_offsets_by_partition(conn, sink_run_id)

    jdbc_connector = (
        "jdbc-sink-policies"
        if ctx["domain"] == "insurance" and ctx["dataset"] == "policies"
        else f"jdbc-sink-{ctx['domain']}-{ctx['dataset']}".replace("_", "-")
    )
    ok_jdbc = wait_until_offset_consumed(
        jdbc_connector,
        topic,
        target,
        target_offsets_by_partition=target_offsets_by_partition,
    )
    ok_s3 = True
    if ctx["domain"] == "insurance" and ctx["dataset"] == "policies":
        ok_s3 = wait_until_offset_consumed(
            "s3-sink-policies",
            topic,
            target,
            target_offsets_by_partition=target_offsets_by_partition,
        )

    ods_pipeline.stages.write(
        conn,
        run_id=sink_run_id,
        stage="sink_pg_wait",
        status="succeeded" if ok_jdbc else "failed",
        event_type="stage_completed" if ok_jdbc else "stage_failed",
        output_ref=f"kafka://{topic}#consumed",
        metrics={"target_offsets_by_partition": target_offsets_by_partition},
        error=None if ok_jdbc else "jdbc sink did not advance",
        airflow_dag_id=ctx.get("airflow_dag_id"),
        airflow_run_id=ctx.get("airflow_run_id"),
    )
    ods_pipeline.stages.write(
        conn,
        run_id=sink_run_id,
        stage="sink_s3_wait",
        status="succeeded" if ok_s3 else "failed",
        event_type="stage_completed" if ok_s3 else "stage_failed",
        output_ref=f"kafka://{topic}#consumed",
        metrics={"target_offsets_by_partition": target_offsets_by_partition},
        error=None if ok_s3 else "s3 sink did not advance",
        airflow_dag_id=ctx.get("airflow_dag_id"),
        airflow_run_id=ctx.get("airflow_run_id"),
    )

    if not (ok_jdbc and ok_s3):
        ods_pipeline.runs.update(conn, sink_run_id, status="partial")
        ods_pipeline.runs.update(conn, ctx["run_id"], status="partial")
        raise RuntimeError("sink wait failed")
    return ctx


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
    starts = (
        metrics.get("produced_offset_start_by_partition")
        or metrics.get("offset_start_by_partition")
        or {"0": metrics["offset_start"]}
    )
    ends = (
        metrics.get("produced_offset_end_by_partition")
        or metrics.get("offset_end_by_partition")
        or {"0": metrics["offset_end"]}
    )
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


@task.branch
def route_canonicalize(ctx: dict) -> str:
    return "skip_canonicalize" if ctx.get("is_canonical", True) else "stage_canonicalize"


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def select_sink_run(ctx: dict) -> dict:
    if ctx.get("is_canonical", True):
        return {**ctx, "sink_run_id": ctx["publish_run_id"]}
    return {**ctx, "sink_run_id": ctx["canonicalize_run_id"]}


def _run_statuses(conn, run_ids: list[str]) -> dict[str, str]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT run_id::text, status FROM pipeline.run_log WHERE run_id::text = ANY(%s)",
            (run_ids,),
        )
        return {rid: status for rid, status in cur.fetchall()}


def _close_child_if_running(conn, run_id: str, *, status: str, reason: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE pipeline.run_log
               SET status=%s,
                   error_summary=COALESCE(error_summary, %s),
                   ended_at=COALESCE(ended_at, NOW())
             WHERE run_id=%s
               AND status='running'
            """,
            (status, reason, run_id),
        )
    conn.commit()


@task(trigger_rule=TriggerRule.ALL_DONE)
def finalise(ctx: dict) -> None:
    run_ids = [ctx["run_id"], ctx["ingest_run_id"], ctx["publish_run_id"]]
    if not ctx.get("is_canonical", True):
        run_ids.append(ctx["canonicalize_run_id"])

    conn = psycopg2.connect(PG_DSN)
    try:
        statuses = _run_statuses(conn, run_ids)
        parent_status = statuses.get(ctx["run_id"], "running")
        ingest_status = statuses.get(ctx["ingest_run_id"], "running")
        publish_status = statuses.get(ctx["publish_run_id"], "running")
        canonicalize_status = statuses.get(ctx.get("canonicalize_run_id"), "skipped")
        sink_run_id = (
            ctx["publish_run_id"]
            if ctx.get("is_canonical", True)
            else ctx["canonicalize_run_id"]
        )

        if ingest_status != "succeeded":
            _close_child_if_running(
                conn,
                ctx["publish_run_id"],
                status="failed",
                reason="Publish skipped because ingestion did not succeed.",
            )
            if not ctx.get("is_canonical", True):
                _close_child_if_running(
                    conn,
                    ctx["canonicalize_run_id"],
                    status="failed",
                    reason="Canonicalize skipped because ingestion did not succeed.",
                )
            ods_pipeline.runs.update(
                conn,
                ctx["run_id"],
                status="failed",
                error_summary=f"ingestion child ended {ingest_status}",
            )
            final_status = "failed"
        elif publish_status != "succeeded":
            _close_child_if_running(
                conn,
                ctx["publish_run_id"],
                status="failed",
                reason="Publish did not complete successfully.",
            )
            if not ctx.get("is_canonical", True):
                _close_child_if_running(
                    conn,
                    ctx["canonicalize_run_id"],
                    status="failed",
                    reason="Canonicalize skipped because publish did not succeed.",
                )
            ods_pipeline.runs.update(
                conn,
                ctx["run_id"],
                status="failed",
                error_summary=f"publish child ended {publish_status}",
            )
            final_status = "failed"
        elif not ctx.get("is_canonical", True) and canonicalize_status != "succeeded":
            _close_child_if_running(
                conn,
                ctx["canonicalize_run_id"],
                status="failed",
                reason="Canonicalize did not complete successfully.",
            )
            ods_pipeline.runs.update(
                conn,
                ctx["run_id"],
                status="failed",
                error_summary=f"canonicalize child ended {canonicalize_status}",
            )
            final_status = "failed"
        elif parent_status == "partial":
            final_status = "partial"
        elif parent_status in ("failed", "succeeded"):
            final_status = parent_status
        else:
            ods_pipeline.runs.update(conn, ctx["run_id"], status="succeeded")
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE pipeline.file_catalogue
                       SET state='sunk', state_updated_at=NOW(), last_run_id=%s
                     WHERE file_id=%s
                    """,
                    (sink_run_id, ctx["file_id"]),
                )
            conn.commit()
            final_status = "succeeded"

        if final_status == "failed":
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE pipeline.file_catalogue
                       SET state='failed', state_updated_at=NOW(), last_run_id=%s
                     WHERE file_id=%s
                    """,
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
    dag_id="dag_ingest",
    start_date=pendulum.datetime(2026, 4, 28, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["ods"],
):
    ctx = init_run()

    _glue_mounts = [
        Mount(source=GLUE_JOBS_PATH, target="/home/glue_user/workspace/jobs", type="bind"),
        Mount(source=ODS_PIPELINE_PATH, target="/home/glue_user/ods_pipeline", type="bind"),
    ]
    _canonicalize_mounts = _glue_mounts + [
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
        mounts=_glue_mounts,
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
        mounts=_canonicalize_mounts,
    )

    selected = select_sink_run(prepared)
    waited = wait_sinks(selected)
    fin = finalise(ctx)

    ctx >> ingest >> publish >> prepared >> branch
    branch >> skip_canonicalize >> selected
    branch >> canonicalize >> selected
    selected >> waited
    [ingest, publish, canonicalize, skip_canonicalize, selected, waited] >> fin
