"""Integration smoke: raw risk topic -> canonical risk topic via ods_canonicalize."""
from __future__ import annotations

import json
import os
import subprocess
import time
import uuid
from pathlib import Path

import psycopg2
import pytest
import requests
from confluent_kafka import Consumer, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer


CONNECT_SR = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")
CONNECT_URL = os.environ.get("CONNECT_URL", "http://localhost:8083")
RAW_TOPIC = "ods.insurance.risk"
CANONICAL_TOPIC = "ods.insurance.risk-canonical"
ROOT = Path(__file__).resolve().parents[2]


def _register(subject: str, path: Path) -> None:
    payload = {"schemaType": "AVRO", "schema": json.dumps(json.loads(path.read_text()))}
    resp = requests.post(
        f"{CONNECT_SR}/subjects/{subject}/versions",
        json=payload,
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        timeout=10,
    )
    assert resp.status_code in (200, 201, 409), resp.text


def _recreate_topic(topic: str) -> None:
    subprocess.run(
        ["docker", "exec", "avivaods-broker-1", "kafka-topics",
         "--bootstrap-server", "localhost:9092", "--delete", "--topic", topic],
        capture_output=True,
    )
    describe_cmd = [
        "docker", "exec", "avivaods-broker-1", "kafka-topics",
        "--bootstrap-server", "localhost:9092", "--describe", "--topic", topic,
    ]
    for _ in range(30):
        probe = subprocess.run(describe_cmd, capture_output=True)
        if probe.returncode != 0:
            break
        time.sleep(1)
    create_cmd = [
        "docker", "exec", "avivaods-broker-1", "kafka-topics",
        "--bootstrap-server", "localhost:9092", "--create", "--topic", topic,
        "--partitions", "1", "--replication-factor", "1",
    ]
    for _ in range(10):
        result = subprocess.run(create_cmd, capture_output=True)
        if result.returncode == 0:
            return
        if b"already exists" not in result.stderr + result.stdout:
            result.check_returncode()
        time.sleep(1)
    result.check_returncode()


@pytest.fixture
def pg():
    conn = psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", "5440")),
        dbname="ods_dev",
        user="ods",
        password="ods",
    )
    yield conn
    conn.close()


def _produce_raw(run_id: str, file_id: str) -> None:
    schema = (ROOT / "schemas" / "insurance" / "risk_raw.avsc").read_text()
    sr = SchemaRegistryClient({"url": CONNECT_SR})
    value_serializer = AvroSerializer(sr, schema)
    key_serializer = StringSerializer("utf_8")
    producer = Producer({"bootstrap.servers": "localhost:9092"})
    rows = [
        {"RskID": "R1", "PolNo": "P1", "ExposureAmt": 123.45, "AsOfDt": "20260501"},
        {"RskID": "R2", "PolNo": "P2", "ExposureAmt": 200.00, "AsOfDt": "20260501"},
        {"RskID": "R3", "PolNo": "P3", "ExposureAmt": 300.00, "AsOfDt": None},
    ]
    for row in rows:
        row.update({
            "_ods_business_date": "2026-05-01",
            "_ods_run_id": run_id,
            "_ods_file_id": file_id,
            "_ods_domain": "insurance",
            "_ods_dataset": "risk",
            "_ods_source_application": "risk-app",
            "_ods_ingested_at": "2026-05-01T10:00:00Z",
        })
        producer.produce(
            RAW_TOPIC,
            key=key_serializer(row["RskID"]),
            value=value_serializer(row, SerializationContext(RAW_TOPIC, MessageField.VALUE)),
        )
    producer.flush()


def _canonical_count() -> int:
    consumer = Consumer({
        "bootstrap.servers": "localhost:9092",
        "group.id": f"test-canonical-{uuid.uuid4()}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([CANONICAL_TOPIC])
    count = 0
    deadline = time.time() + 10
    try:
        while time.time() < deadline:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                break
            count += 1
    finally:
        consumer.close()
    return count


def _provision_risk_sink() -> None:
    _delete_risk_sink()
    config = json.loads((ROOT / "docker" / "connect-config" / "jdbc-sink-risk.json").read_text())
    resp = requests.post(
        f"{CONNECT_URL}/connectors",
        json=config,
        headers={"Content-Type": "application/json"},
        timeout=10,
    )
    assert resp.status_code in (200, 201, 409), resp.text


def _delete_risk_sink() -> None:
    requests.delete(f"{CONNECT_URL}/connectors/jdbc-sink-risk", timeout=10)


def _wait_risk_rows(pg, run_id: str, timeout: int = 90) -> list[tuple]:
    deadline = time.time() + timeout
    rows: list[tuple] = []
    while time.time() < deadline:
        pg.rollback()
        with pg.cursor() as cur:
            cur.execute(
                """
                SELECT risk_id, policy_id, exposure_amount::text, as_of_date::text,
                       _ods_canonicalize_run_id
                  FROM ods.insurance_risk
                 WHERE _ods_canonicalize_run_id=%s
                 ORDER BY risk_id
                """,
                (run_id,),
            )
            rows = cur.fetchall()
        if len(rows) == 2:
            return rows
        time.sleep(2)
    return rows


def test_risk_canonicalize_job_writes_t1_recon(pg):
    _register("ods.insurance.risk-value", ROOT / "schemas" / "insurance" / "risk_raw.avsc")
    _register(
        "ods.insurance.risk-canonical-value",
        ROOT / "schemas" / "insurance" / "risk_canonical.avsc",
    )
    _delete_risk_sink()
    _recreate_topic(RAW_TOPIC)
    _recreate_topic(CANONICAL_TOPIC)
    _provision_risk_sink()

    run_id = str(uuid.uuid4())
    raw_run_id = str(uuid.uuid4())
    file_id = str(uuid.uuid4())
    _produce_raw(raw_run_id, file_id)

    with pg.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline.file_catalogue
                (file_id, domain, dataset, business_date, s3_raw_path, file_md5, state)
            VALUES (%s, 'insurance', 'risk', '2026-05-01',
                    %s, '00000000000000000000000000000000', 'curated')
            ON CONFLICT DO NOTHING
            """,
            (file_id, f"s3://ods-raw-local/insurance/risk/date=20260501/risk_{file_id}.csv"),
        )
        cur.execute(
            """
            INSERT INTO pipeline.run_log
                (run_id, pipeline_type, domain, dataset, business_date,
                 file_id, status, kafka_topic, kafka_offset_start, kafka_offset_end)
            VALUES (%s, 'publish', 'insurance', 'risk', '2026-05-01',
                    %s, 'succeeded', %s, 0, 3)
            ON CONFLICT DO NOTHING
            """,
            (raw_run_id, file_id, RAW_TOPIC),
        )
        cur.execute(
            "DELETE FROM ods.insurance_risk WHERE risk_id IN ('R1', 'R2', 'R3')"
        )
    pg.commit()

    cmd = [
        "docker", "run", "--rm", "--network", "ods-network",
        "-e", "AWS_ACCESS_KEY_ID=test",
        "-e", "AWS_SECRET_ACCESS_KEY=test",
        "-e", "AWS_DEFAULT_REGION=eu-west-1",
        "-e", "LOCALSTACK_ENDPOINT=http://localstack:4566",
        "-e", "KAFKA_BOOTSTRAP_SERVERS=broker:29092",
        "-e", "SCHEMA_REGISTRY_URL=http://schema-registry:8081",
        "-e", "POSTGRES_HOST=postgres",
        "-e", "POSTGRES_DB=ods_dev",
        "-e", "POSTGRES_USER=ods",
        "-e", "POSTGRES_PASSWORD=ods",
        "-e", "ENV=local",
        "-v", f"{ROOT / 'glue' / 'jobs'}:/home/glue_user/workspace/jobs",
        "-v", f"{ROOT / 'ods_pipeline'}:/home/glue_user/ods_pipeline",
        "-v", f"{ROOT / 'patterns'}:/home/glue_user/patterns",
        "ods-glue:local",
        "spark-submit",
        "--py-files",
        "/home/glue_user/workspace/jobs/utils.py,"
        "/home/glue_user/workspace/jobs/utils_bootstrap.py,"
        "/home/glue_user/workspace/jobs/utils_data.py,"
        "/home/glue_user/workspace/jobs/utils_config.py,"
        "/home/glue_user/workspace/jobs/utils_state.py,"
        "/home/glue_user/workspace/jobs/utils_runs.py,"
        "/home/glue_user/workspace/jobs/utils_jobs.py,"
        "/home/glue_user/workspace/jobs/dq.py,"
        "/home/glue_user/workspace/jobs/canonicalize.py",
        "/home/glue_user/workspace/jobs/ods_canonicalize.py",
        "--run_id", run_id,
        "--domain", "insurance",
        "--dataset", "risk",
        "--raw_topic", RAW_TOPIC,
        "--canonical_topic", CANONICAL_TOPIC,
        "--transform_yaml_path", "/home/glue_user/patterns/insurance/risk.yaml",
        "--offset_ranges", '{"0":{"start":0,"end":3}}',
        "--file_id", file_id,
        "--parent_run_id", raw_run_id,
        "--business_date", "2026-05-01",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
    assert result.returncode == 0, result.stderr
    assert _canonical_count() == 2

    with pg.cursor() as cur:
        cur.execute(
            """
            SELECT status, source_count, kafka_count, discrepancy_count
              FROM pipeline.reconciliation_log
             WHERE run_id=%s AND check_type='t1_canonicalize_count'
            """,
            (run_id,),
        )
        row = cur.fetchone()
    assert row == ("ok", 2, 2, 0)

    rows = _wait_risk_rows(pg, run_id)
    assert rows == [
        ("R1", "P1", "123.45", "2026-05-01", run_id),
        ("R2", "P2", "200.00", "2026-05-01", run_id),
    ]
