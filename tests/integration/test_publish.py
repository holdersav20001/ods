import os
import subprocess
import uuid
import time

import boto3
import psycopg2
import pytest
from confluent_kafka import Consumer, KafkaError

S3_ENDPOINT = "http://localhost:4566"
CURATED_BUCKET = "ods-curated-local"
NETWORK = "ods-network"
KAFKA_BROKERS = "localhost:9092"
TOPIC = "ods.insurance.policies"


@pytest.fixture
def s3():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="eu-west-1",
    )


@pytest.fixture
def pg():
    conn = psycopg2.connect(
        host=os.environ.get("TEST_PG_HOST", "127.0.0.1"),
        port=int(os.environ.get("TEST_PG_PORT", "5440")),
        dbname=os.environ.get("TEST_PG_DB", "ods_dev"),
        user=os.environ.get("TEST_PG_USER", "ods"),
        password=os.environ.get("TEST_PG_PASSWORD", "ods"),
    )
    yield conn
    conn.close()


def run_ingestion_job(run_id, s3_path, domain="insurance", dataset="policies"):
    cmd = [
        "docker", "run", "--rm", "--network", NETWORK,
        "-e", "AWS_DEFAULT_REGION=eu-west-1",
        "-e", "AWS_ACCESS_KEY_ID=test",
        "-e", "AWS_SECRET_ACCESS_KEY=test",
        "-e", "LOCALSTACK_ENDPOINT=http://localstack:4566",
        "-e", "POSTGRES_HOST=postgres",
        "-e", "POSTGRES_DB=ods_dev",
        "-e", "POSTGRES_USER=ods",
        "-e", "POSTGRES_PASSWORD=ods",
        "-e", "SCHEMA_REGISTRY_URL=http://schema-registry:8081",
        "-e", "ENV=local",
        "-v", f"{os.getcwd()}/glue/jobs:/home/glue_user/workspace/jobs",
        "ods-glue:local", "spark-submit",
        "--py-files",
        "/home/glue_user/workspace/jobs/utils.py,/home/glue_user/workspace/jobs/dq.py",
        "/home/glue_user/workspace/jobs/ods_ingestion.py",
        "--run_id", run_id,
        "--domain", domain,
        "--dataset", dataset,
        "--s3_input_path", s3_path,
    ]
    return subprocess.run(cmd, capture_output=True, text=True, timeout=300)


def run_publish_job(run_id, s3_path, domain="insurance", dataset="policies"):
    cmd = [
        "docker", "run", "--rm", "--network", NETWORK,
        "-e", "AWS_DEFAULT_REGION=eu-west-1",
        "-e", "AWS_ACCESS_KEY_ID=test",
        "-e", "AWS_SECRET_ACCESS_KEY=test",
        "-e", "LOCALSTACK_ENDPOINT=http://localstack:4566",
        "-e", "KAFKA_BOOTSTRAP_SERVERS=broker:29092",
        "-e", "SCHEMA_REGISTRY_URL=http://schema-registry:8081",
        "-e", "POSTGRES_HOST=postgres",
        "-e", "POSTGRES_DB=ods_dev",
        "-e", "POSTGRES_USER=ods",
        "-e", "POSTGRES_PASSWORD=ods",
        "-e", "ENV=local",
        "-v", f"{os.getcwd()}/glue/jobs:/home/glue_user/workspace/jobs",
        "ods-glue:local", "spark-submit",
        "--py-files",
        "/home/glue_user/workspace/jobs/utils.py,/home/glue_user/workspace/jobs/dq.py",
        "/home/glue_user/workspace/jobs/ods_s3_publish.py",
        "--run_id", run_id,
        "--domain", domain,
        "--dataset", dataset,
        "--s3_input_path", s3_path,
    ]
    return subprocess.run(cmd, capture_output=True, text=True, timeout=300)


def consume_messages(topic, timeout=15.0):
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKERS,
        "group.id": f"test-{uuid.uuid4()}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([topic])
    messages = []
    deadline = time.time() + timeout
    while time.time() < deadline:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            break
        messages.append(msg.value())
    consumer.close()
    return messages


GOOD_CSV = (
    "policy_id,policyholder_name,premium_amount,start_date,end_date,agent_code,postcode\n"
    "POL-100,Alice,1200.00,2026-01-01,2027-01-01,AG001,EC1A1BB\n"
    "POL-101,Bob,950.50,2026-02-01,2027-02-01,AG002,WC2N5DU\n"
)


def test_publish_happy_path(s3, pg):
    ingest_run_id = str(uuid.uuid4())
    pub_run_id = str(uuid.uuid4())
    raw_key = "insurance/policies/date=20260501/policies_20260501.csv"
    s3.put_object(Bucket="ods-raw-local", Key=raw_key, Body=GOOD_CSV.encode())

    # Run ingestion first to produce curated parquet
    r = run_ingestion_job(ingest_run_id, f"s3://ods-raw-local/{raw_key}")
    assert r.returncode == 0, r.stderr

    curated_path = "s3://ods-curated-local/insurance/policies/date=2026-05-01/"
    result = run_publish_job(pub_run_id, curated_path)
    assert result.returncode == 0, result.stderr

    # ── Kafka: at least 2 messages consumed ─────────────────────────────
    msgs = consume_messages(TOPIC)
    assert len(msgs) >= 2

    cur = pg.cursor()

    # ── run_log: record_count_published + offset fields populated ───────
    cur.execute(
        """
        SELECT record_count_published, kafka_offset_start, kafka_offset_end,
               kafka_topic, status
        FROM pipeline.run_log
        WHERE run_id = %s
        """,
        (pub_run_id,),
    )
    row = cur.fetchone()
    assert row is not None, "No row in pipeline.run_log for pub_run_id"
    record_count_published, offset_start, offset_end, kafka_topic, status = row
    assert record_count_published == 2, f"expected 2, got {record_count_published}"
    assert offset_start is not None, "kafka_offset_start should be populated"
    assert offset_end is not None, "kafka_offset_end should be populated"
    assert offset_end > offset_start, "offset_end should be > offset_start after producing"
    assert status == "succeeded"

    # ── run_stage_log: publish stage exists ─────────────────────────────
    cur.execute(
        """
        SELECT status, record_count_out
        FROM pipeline.run_stage_log
        WHERE run_id = %s AND stage = 'publish'
        """,
        (pub_run_id,),
    )
    stage_row = cur.fetchone()
    assert stage_row is not None, "No 'publish' stage row in pipeline.run_stage_log"
    assert stage_row[0] == "succeeded"
    assert stage_row[1] == 2

    # ── reconciliation_log: t0_publish_count row with status=ok ─────────
    cur.execute(
        """
        SELECT status, source_count, kafka_count, discrepancy_count
        FROM pipeline.reconciliation_log
        WHERE run_id = %s AND check_type = 't0_publish_count'
        """,
        (pub_run_id,),
    )
    recon_row = cur.fetchone()
    assert recon_row is not None, "No t0_publish_count recon row found"
    recon_status, source_count, kafka_count, discrepancy = recon_row
    assert recon_status == "ok", f"T0 check status: {recon_status}"
    assert source_count == 2
    assert kafka_count == 2
    assert discrepancy == 0

    # ── file_state: completed ────────────────────────────────────────────
    cur.execute(
        "SELECT status FROM pipeline.file_state WHERE run_id = %s",
        (pub_run_id,),
    )
    assert cur.fetchone()[0] == "completed"
