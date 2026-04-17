"""dag2_etl_trigger.py

Triggered when a file lands in ods-raw-{ENV}.
For local dev, trigger manually and supply params.s3_key.

Task sequence:
    check_file_catalogue → check_idempotency → verify_checksum
    → trigger_glue_ingestion → update_file_state
"""

import hashlib
import os
from datetime import datetime

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

def _check_file_catalogue(**context):
    """Return False (short-circuit) if filename does not match any active pattern."""
    s3_key = context["params"]["s3_key"]
    filename = s3_key.split("/")[-1]

    sql = """
        SELECT COUNT(*)
        FROM   pipeline.file_catalogue  fc
        JOIN   pipeline.dataset_config  dc ON dc.id = fc.dataset_config_id
        WHERE  dc.active = TRUE
          AND  %s LIKE REPLACE(fc.name_pattern, '*', '%%')
    """
    conn = _pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (filename,))
            count = cur.fetchone()[0]
    finally:
        conn.close()

    return count > 0


def _check_idempotency(**context):
    """Return False (short-circuit) if the file has already been transferred."""
    s3_key = context["params"]["s3_key"]
    env = os.environ.get("ENV", "local")
    bucket = f"ods-raw-{env}"
    s3_path = f"s3://{bucket}/{s3_key}"

    sql = """
        SELECT status
        FROM   pipeline.ingestion_file_state
        WHERE  s3_path = %s
    """
    conn = _pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (s3_path,))
            row = cur.fetchone()
    finally:
        conn.close()

    if row and row[0] == "transferred":
        return False
    return True


def _verify_checksum(**context):
    """Download object, compute MD5, compare with metadata; upsert file state."""
    s3_key = context["params"]["s3_key"]
    env = os.environ.get("ENV", "local")
    bucket = f"ods-raw-{env}"
    s3_path = f"s3://{bucket}/{s3_key}"

    client = _s3()
    obj = client.get_object(Bucket=bucket, Key=s3_key)
    body = obj["Body"].read()
    computed_md5 = hashlib.md5(body).hexdigest()

    # Compare with metadata md5 if present — raise on mismatch
    metadata = obj.get("Metadata", {})
    expected_md5 = metadata.get("md5")
    if expected_md5 and expected_md5 != computed_md5:
        raise ValueError(
            f"Checksum mismatch for {s3_path}: "
            f"expected={expected_md5}, got={computed_md5}"
        )

    upsert_sql = """
        INSERT INTO pipeline.ingestion_file_state (s3_path, status, checksum_md5)
        VALUES (%s, 'detected', %s)
        ON CONFLICT (s3_path)
        DO UPDATE SET status = 'detected', checksum_md5 = EXCLUDED.checksum_md5
    """
    conn = _pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(upsert_sql, (s3_path, computed_md5))
        conn.commit()
    finally:
        conn.close()


def _update_file_state(**context):
    """Mark the file as transferred."""
    s3_key = context["params"]["s3_key"]
    env = os.environ.get("ENV", "local")
    bucket = f"ods-raw-{env}"
    s3_path = f"s3://{bucket}/{s3_key}"

    sql = """
        UPDATE pipeline.ingestion_file_state
        SET    status = 'transferred'
        WHERE  s3_path = %s
    """
    conn = _pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (s3_path,))
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

_GLUE_JOBS_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../glue/jobs")
)

with DAG(
    dag_id="dag2_etl_trigger",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    params={"s3_key": ""},
) as dag:

    check_file_catalogue = ShortCircuitOperator(
        task_id="check_file_catalogue",
        python_callable=_check_file_catalogue,
    )

    check_idempotency = ShortCircuitOperator(
        task_id="check_idempotency",
        python_callable=_check_idempotency,
    )

    verify_checksum = PythonOperator(
        task_id="verify_checksum",
        python_callable=_verify_checksum,
    )

    trigger_glue_ingestion = DockerOperator(
        task_id="trigger_glue_ingestion",
        image="ods-glue:local",
        network_mode="ods-network",
        command=(
            "spark-submit "
            "--py-files /home/glue_user/workspace/jobs/utils.py,"
            "/home/glue_user/workspace/jobs/dq.py "
            "/home/glue_user/workspace/jobs/ods_ingestion.py "
            "--run_id {{ run_id }} --domain insurance --dataset policies "
            "--s3_input_path s3://ods-raw-local/{{ params.s3_key }}"
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
            "ENV": os.environ.get("ENV", "local"),
        },
        volumes=[f"{_GLUE_JOBS_PATH}:/home/glue_user/workspace/jobs"],
        auto_remove=True,
        mount_tmp_dir=False,
    )

    update_file_state = PythonOperator(
        task_id="update_file_state",
        python_callable=_update_file_state,
    )

    # Task dependencies
    check_file_catalogue >> check_idempotency >> verify_checksum >> trigger_glue_ingestion >> update_file_state
