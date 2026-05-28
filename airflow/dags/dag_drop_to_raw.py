"""dag_drop_to_raw — scan SFTP, hash files, register new ones in pipeline.file_catalogue,
upload to S3 raw, and trigger the appropriate downstream DAG per file based on the
``delivery`` column in pipeline.dataset_config.

Routing is SQL-driven: two scan tasks each query dataset_config with a delivery filter
(``file_pipeline`` -> dag_ingest, ``direct_postgres`` -> dag_ingest_direct_postgres) and
emit only files for that route. Idempotent via the (domain, dataset, s3_raw_path)
uniqueness check on pipeline.file_catalogue.
"""
from __future__ import annotations

import hashlib
import os
import re
import sys
import uuid

import boto3
import paramiko
import pendulum
import psycopg2

# Add likely roots so ods_pipeline is importable both during full DAG parsing
# and when Airflow LocalExecutor loads only this DAG file by subdir.
_DAG_DIR = os.path.dirname(__file__)
for _root in (
    os.path.abspath(os.path.join(_DAG_DIR, "..")),
    os.path.abspath(os.path.join(_DAG_DIR, "..", "..")),
):
    if _root not in sys.path:
        sys.path.insert(0, _root)

import ods_pipeline
from airflow import DAG
from airflow.decorators import task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

PG_DSN = os.environ.get(
    "PIPELINE_PG_DSN",
    "host=postgres port=5432 dbname=ods_dev user=ods password=ods",
)
SFTP_HOST = os.environ.get("SFTP_HOST", "sftp")
SFTP_PORT = int(os.environ.get("SFTP_PORT", "22"))
SFTP_USER = os.environ.get("SFTP_USER", "ods")
SFTP_PASS = os.environ.get("SFTP_PASS", "odspass")
S3_RAW_BUCKET = os.environ.get("S3_RAW_BUCKET", "ods-raw-local")
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://localstack:4566")


def _sftp():
    t = paramiko.Transport((SFTP_HOST, SFTP_PORT))
    t.connect(username=SFTP_USER, password=SFTP_PASS)
    return paramiko.SFTPClient.from_transport(t), t


def _s3():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
        region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
    )


def _datasets_for_delivery(conn, delivery: str):
    """Return active s3_batch datasets whose ``delivery`` column matches.

    The COALESCE keeps backward-compat for rows where delivery is NULL — those
    default to ``file_pipeline``.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT domain, dataset, filename_pattern
              FROM pipeline.dataset_config
             WHERE active = TRUE
               AND source_type = 's3_batch'
               AND COALESCE(delivery, 'file_pipeline') = %s
            """,
            (delivery,),
        )
        return cur.fetchall()


def _extract_business_date(match: re.Match) -> str:
    """Pull YYYYMMDD from regex match — prefer named group 'bd', fallback to group(1)."""
    try:
        bd = match.group("bd")
    except IndexError:
        bd = match.group(1)
    return f"{bd[:4]}-{bd[4:6]}-{bd[6:8]}"


def _scan_and_register(delivery: str) -> list[dict]:
    """Per-route scanner: list SFTP /upload, match against datasets filtered by
    ``delivery``, hash + upload to S3, register new rows in file_catalogue, and
    return the list of new file conf dicts to feed into TriggerDagRunOperator.

    Idempotency: a SELECT on (domain, dataset, s3_raw_path) gates the INSERT, so
    re-running this task over the same SFTP file list does not produce duplicate
    file_catalogue rows.
    """
    conn = psycopg2.connect(PG_DSN)
    sftp, transport = _sftp()
    s3 = _s3()
    new_files: list[dict] = []
    try:
        try:
            sftp.chdir("upload")
            files = sftp.listdir()
        except IOError:
            files = []

        datasets = _datasets_for_delivery(conn, delivery)
        for filename in files:
            for domain, dataset, pattern in datasets:
                m = re.match(pattern, filename)
                if not m:
                    continue
                with sftp.file(filename, "r") as fh:
                    body = fh.read()
                md5 = hashlib.md5(body).hexdigest()
                bd_iso = _extract_business_date(m)
                s3_key = f"{domain}/{dataset}/{bd_iso}/{filename}"
                s3_raw_path = f"s3://{S3_RAW_BUCKET}/{s3_key}"

                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT file_id FROM pipeline.file_catalogue
                         WHERE domain=%s AND dataset=%s AND s3_raw_path=%s
                        """,
                        (domain, dataset, s3_raw_path),
                    )
                    if cur.fetchone():
                        break  # idempotent: already registered

                    file_id = str(uuid.uuid4())
                    s3.put_object(Bucket=S3_RAW_BUCKET, Key=s3_key, Body=body)
                    ods_pipeline.files.upsert(
                        conn,
                        file_id=file_id,
                        domain=domain,
                        dataset=dataset,
                        business_date=bd_iso,
                        sftp_path=f"/upload/{filename}",
                        s3_raw_path=s3_raw_path,
                        file_size_bytes=len(body),
                        file_md5=md5,
                        state="received",
                    )
                new_files.append(
                    {
                        "file_id": file_id,
                        "domain": domain,
                        "dataset": dataset,
                        "business_date": bd_iso,
                    }
                )
                break
    finally:
        try:
            sftp.close()
        finally:
            transport.close()
        conn.close()
    return new_files


@task
def scan_and_register_for_dag_ingest() -> list[dict]:
    """Scan SFTP for files matching datasets with delivery='file_pipeline'."""
    return _scan_and_register("file_pipeline")


@task
def scan_and_register_for_direct_postgres() -> list[dict]:
    """Scan SFTP for files matching datasets with delivery='direct_postgres'."""
    return _scan_and_register("direct_postgres")


with DAG(
    dag_id="dag_drop_to_raw",
    start_date=pendulum.datetime(2026, 4, 28, tz="UTC"),
    schedule="*/1 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["ods"],
):
    kafka_files = scan_and_register_for_dag_ingest()
    direct_pg_files = scan_and_register_for_direct_postgres()

    TriggerDagRunOperator.partial(
        task_id="trigger_ingest",
        trigger_dag_id="dag_ingest",
    ).expand(conf=kafka_files)

    TriggerDagRunOperator.partial(
        task_id="trigger_ingest_direct_postgres",
        trigger_dag_id="dag_ingest_direct_postgres",
    ).expand(conf=direct_pg_files)
