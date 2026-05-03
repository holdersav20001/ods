"""Event API integration tests against local Postgres, LocalStack, and Kafka."""
from __future__ import annotations

import json
import os
import time
import uuid

import boto3
import psycopg2
import pytest
from confluent_kafka import Consumer, Producer

fastapi = pytest.importorskip("fastapi")
from fastapi.testclient import TestClient

from services.event_api.main import build_app


S3_ENDPOINT = os.environ.get("LOCALSTACK_ENDPOINT", "http://localhost:4566")
KAFKA_BROKERS = os.environ.get("KAFKA_BROKERS", "localhost:9092")
EVENT_TOPIC = "ods.insurance.events"
ARCHIVE_BUCKET = "ods-event-api-test"


@pytest.fixture
def pg_conn():
    conn = psycopg2.connect(
        host=os.environ.get("TEST_PG_HOST", "127.0.0.1"),
        port=int(os.environ.get("TEST_PG_PORT", "5440")),
        dbname=os.environ.get("TEST_PG_DB", "ods_dev"),
        user=os.environ.get("TEST_PG_USER", "ods"),
        password=os.environ.get("TEST_PG_PASSWORD", "ods"),
    )
    yield conn
    conn.close()


@pytest.fixture
def s3_client():
    client = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="eu-west-1",
    )
    try:
        client.create_bucket(
            Bucket=ARCHIVE_BUCKET,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-1"},
        )
    except Exception:
        pass
    return client


def _producer():
    return Producer({"bootstrap.servers": KAFKA_BROKERS})


def _consume_event(event_id: str, timeout_s: float = 15.0) -> dict | None:
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKERS,
        "group.id": f"event-api-test-{uuid.uuid4()}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([EVENT_TOPIC])
    deadline = time.time() + timeout_s
    try:
        while time.time() < deadline:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                continue
            payload = json.loads(msg.value().decode("utf-8"))
            if payload.get("_ods_source_event_id") == event_id:
                return payload
    finally:
        consumer.close()
    return None


def test_event_api_archives_publishes_and_closes_run(pg_conn, s3_client):
    event_id = f"evt-live-{uuid.uuid4()}"
    business_date = "2026-05-03"

    app = build_app(
        pg_factory=lambda: pg_conn,
        s3_client=s3_client,
        producer_factory=_producer,
        archive_bucket=ARCHIVE_BUCKET,
    )
    client = TestClient(app)

    response = client.post("/events", json={
        "event_id": event_id,
        "business_date": business_date,
        "payload": {"policy_id": "P-LIVE", "premium": 101.25},
    })

    assert response.status_code == 200, response.text
    body = response.json()
    run_id = body["run_id"]
    archive_key = body["archive_uri"].removeprefix(f"s3://{ARCHIVE_BUCKET}/")

    archived = s3_client.get_object(Bucket=ARCHIVE_BUCKET, Key=archive_key)
    assert json.loads(archived["Body"].read().decode("utf-8"))["policy_id"] == "P-LIVE"

    kafka_payload = _consume_event(event_id)
    assert kafka_payload is not None
    assert kafka_payload["_ods_run_id"] == run_id
    assert kafka_payload["_ods_domain"] == "insurance"
    assert kafka_payload["_ods_dataset"] == "event_demo"

    with pg_conn.cursor() as cur:
        cur.execute(
            """
            SELECT status, pipeline_type, record_count_source, record_count_published
              FROM pipeline.run_log
             WHERE run_id = %s::uuid
            """,
            (run_id,),
        )
        assert cur.fetchone() == ("succeeded", "message_api", 1, 1)
        cur.execute(
            """
            SELECT stage, status
              FROM pipeline.run_stage_log
             WHERE run_id = %s::uuid
             ORDER BY stage
            """,
            (run_id,),
        )
        stages = dict(cur.fetchall())
        assert stages["message_receive"] == "succeeded"
        assert stages["message_archive"] == "succeeded"
        assert stages["recon_message"] == "succeeded"
        cur.execute(
            """
            SELECT status, source_count, kafka_count
              FROM pipeline.reconciliation_log
             WHERE run_id = %s::uuid
               AND check_type = 'message_batch_count'
            """,
            (run_id,),
        )
        assert cur.fetchone() == ("ok", 1, 1)
