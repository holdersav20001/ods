"""dag_publish.py

Triggered when Parquet lands in ods-curated-{ENV}.
For local dev, trigger manually with params: s3_path, domain, dataset.

Task sequence:
    check_idempotency → load_config → set_processing
    → trigger_glue_publish → set_completed → emit_audit_event
"""

import json
import os
from datetime import datetime, timezone

import boto3
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.docker.operators.docker import DockerOperator

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _pg_conn():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        dbname=os.environ.get("POSTGRES_DB", "ods_dev"),
        user=os.environ.get("POSTGRES_USER", "ods"),
        password=os.environ.get("POSTGRES_PASSWORD", "ods"),
    )


def _s3():
    kwargs = dict(
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
        region_name=os.environ.get("AWS_DEFAULT_REGION", "eu-west-1"),
    )
    endpoint = os.environ.get("LOCALSTACK_ENDPOINT")
    if endpoint:
        kwargs["endpoint_url"] = endpoint
    return boto3.client("s3", **kwargs)


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------

def _check_idempotency(**context):
    """Return False (short-circuit) if the file has already been completed."""
    s3_path = context["params"]["s3_path"]

    sql = """
        SELECT status
        FROM   pipeline.file_state
        WHERE  s3_path = %s
    """
    conn = _pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (s3_path,))
            row = cur.fetchone()
    finally:
        conn.close()

    if row and row[0] == "completed":
        return False
    return True


def _load_config(**context):
    """Load dataset config and push config_version to XCom."""
    domain = context["params"]["domain"]
    dataset = context["params"]["dataset"]

    sql = """
        SELECT config_version
        FROM   pipeline.dataset_config
        WHERE  domain = %s
          AND  dataset = %s
    """
    conn = _pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (domain, dataset))
            row = cur.fetchone()
    finally:
        conn.close()

    if not row:
        raise ValueError(f"No dataset_config found for domain={domain} dataset={dataset}")

    config_version = row[0]
    context["ti"].xcom_push(key="config_version", value=config_version)
    return config_version


def _set_processing(**context):
    """Upsert file_state to processing."""
    s3_path = context["params"]["s3_path"]

    upsert_sql = """
        INSERT INTO pipeline.file_state (s3_path, status)
        VALUES (%s, 'processing')
        ON CONFLICT (s3_path)
        DO UPDATE SET status = 'processing'
    """
    conn = _pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(upsert_sql, (s3_path,))
        conn.commit()
    finally:
        conn.close()


def _set_completed(**context):
    """Update file_state to completed."""
    s3_path = context["params"]["s3_path"]

    sql = """
        UPDATE pipeline.file_state
        SET    status = 'completed'
        WHERE  s3_path = %s
    """
    conn = _pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (s3_path,))
        conn.commit()
    finally:
        conn.close()


def _emit_audit_event(**context):
    """Produce a publish_completed event to the ods.pipeline.audit Kafka topic."""
    from confluent_kafka import Producer

    run_id = context["run_id"]
    s3_path = context["params"]["s3_path"]
    ts = datetime.now(timezone.utc).isoformat()

    payload = json.dumps(
        {
            "event": "publish_completed",
            "run_id": run_id,
            "s3_path": s3_path,
            "ts": ts,
        }
    )

    kafka_bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "broker:29092")
    producer = Producer({"bootstrap.servers": kafka_bootstrap})
    producer.produce("ods.pipeline.audit", value=payload.encode("utf-8"))
    producer.flush(timeout=10)


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

_GLUE_JOBS_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../glue/jobs")
)

with DAG(
    dag_id="dag_publish",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    params={"s3_path": "", "domain": "insurance", "dataset": "policies"},
) as dag:

    check_idempotency = ShortCircuitOperator(
        task_id="check_idempotency",
        python_callable=_check_idempotency,
    )

    load_config = PythonOperator(
        task_id="load_config",
        python_callable=_load_config,
    )

    set_processing = PythonOperator(
        task_id="set_processing",
        python_callable=_set_processing,
    )

    trigger_glue_publish = DockerOperator(
        task_id="trigger_glue_publish",
        image="ods-glue:local",
        network_mode="ods-network",
        command=(
            "spark-submit "
            "--py-files /home/glue_user/workspace/jobs/utils.py,"
            "/home/glue_user/workspace/jobs/dq.py "
            "/home/glue_user/workspace/jobs/ods_s3_publish.py "
            "--run_id {{ run_id }} "
            "--domain {{ params.domain }} "
            "--dataset {{ params.dataset }} "
            "--s3_input_path {{ params.s3_path }}"
        ),
        environment={
            "AWS_ACCESS_KEY_ID": os.environ.get("AWS_ACCESS_KEY_ID", "test"),
            "AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
            "AWS_DEFAULT_REGION": os.environ.get("AWS_DEFAULT_REGION", "eu-west-1"),
            "LOCALSTACK_ENDPOINT": os.environ.get("LOCALSTACK_ENDPOINT", "http://localstack:4566"),
            "POSTGRES_HOST": os.environ.get("POSTGRES_HOST", "postgres"),
            "POSTGRES_DB": os.environ.get("POSTGRES_DB", "ods_dev"),
            "POSTGRES_USER": os.environ.get("POSTGRES_USER", "ods"),
            "POSTGRES_PASSWORD": os.environ.get("POSTGRES_PASSWORD", "ods"),
            "KAFKA_BOOTSTRAP_SERVERS": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "broker:29092"),
            "ENV": os.environ.get("ENV", "local"),
        },
        volumes=[f"{_GLUE_JOBS_PATH}:/home/glue_user/workspace/jobs"],
        auto_remove=True,
        mount_tmp_dir=False,
    )

    set_completed = PythonOperator(
        task_id="set_completed",
        python_callable=_set_completed,
    )

    emit_audit_event = PythonOperator(
        task_id="emit_audit_event",
        python_callable=_emit_audit_event,
    )

    # Task dependencies
    check_idempotency >> load_config >> set_processing >> trigger_glue_publish >> set_completed >> emit_audit_event
