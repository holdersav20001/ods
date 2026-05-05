"""dag_drop_to_raw — scan SFTP, hash files, register new ones in pipeline.file_catalogue,
upload to S3 raw, and trigger dag_ingest per new file. Idempotent via (domain, dataset, file_md5)
unique constraint.
"""
from __future__ import annotations

import hashlib
import os
import re
import uuid

import boto3
import paramiko
import pendulum
import psycopg2
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


def _datasets(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT domain, dataset, filename_pattern,
                   COALESCE(delivery, 'file_pipeline')
              FROM pipeline.dataset_config
             WHERE active = TRUE AND source_type = 's3_batch'
            """
        )
        return cur.fetchall()


def _extract_business_date(match: re.Match) -> str:
    """Pull YYYYMMDD from regex match — prefer named group 'bd', fallback to group(1)."""
    try:
        bd = match.group("bd")
    except IndexError:
        bd = match.group(1)
    return f"{bd[:4]}-{bd[4:6]}-{bd[6:8]}"


@task
def scan_and_register():
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

        datasets = _datasets(conn)
        for filename in files:
            for domain, dataset, pattern, delivery in datasets:
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
                    cur.execute(
                        """
                        INSERT INTO pipeline.file_catalogue
                            (file_id, domain, dataset, business_date, sftp_path, s3_raw_path,
                             file_size_bytes, file_md5, state)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'received')
                        """,
                        (
                            file_id,
                            domain,
                            dataset,
                            bd_iso,
                            f"/upload/{filename}",
                            s3_raw_path,
                            len(body),
                            md5,
                        ),
                    )
                conn.commit()
                new_files.append(
                    {
                        "file_id": file_id,
                        "domain": domain,
                        "dataset": dataset,
                        "business_date": bd_iso,
                        "delivery": delivery,
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
def split_by_delivery(files: list[dict]) -> dict:
    """Partition new files by ``delivery`` so each TriggerDagRunOperator
    routes to the right downstream DAG."""
    by_delivery = {"file_pipeline": [], "direct_postgres": []}
    for f in files or []:
        d = f.get("delivery", "file_pipeline")
        by_delivery.setdefault(d, []).append(f)
    return by_delivery


@task
def for_kafka(routes: dict) -> list[dict]:
    return routes.get("file_pipeline", [])


@task
def for_direct_pg(routes: dict) -> list[dict]:
    return routes.get("direct_postgres", [])


with DAG(
    dag_id="dag_drop_to_raw",
    start_date=pendulum.datetime(2026, 4, 28, tz="UTC"),
    schedule="*/1 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["ods"],
):
    files = scan_and_register()
    routes = split_by_delivery(files)
    TriggerDagRunOperator.partial(
        task_id="trigger_ingest",
        trigger_dag_id="dag_ingest",
    ).expand(conf=for_kafka(routes))
    TriggerDagRunOperator.partial(
        task_id="trigger_ingest_direct_postgres",
        trigger_dag_id="dag_ingest_direct_postgres",
    ).expand(conf=for_direct_pg(routes))
