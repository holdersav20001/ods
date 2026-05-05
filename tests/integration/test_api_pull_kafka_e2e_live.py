"""Live E2E for the direct-Kafka api_pull pattern.

Stub API → ods_pipeline.ingest.api_pull_kafka.run_once → real Kafka
broker (Avro via real Schema Registry) → real Kafka Connect JDBC sink
→ real Postgres rows. Verifies the full happy path AND watermark
promotion via the Connect consumer-group offset oracle.

Skipped when docker / required containers are not running.
"""
from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import sys
import threading
import time
import uuid

import pytest
import requests
import uvicorn
from fastapi import FastAPI, Header, HTTPException, Query, Response

# yaml_loader needs sys.path on airflow/dags so common is importable.
sys.path.insert(0, os.path.abspath(os.path.join(
    os.path.dirname(__file__), "..", "..", "airflow", "dags",
)))
from common import yaml_loader  # type: ignore  # noqa: E402

from ods_pipeline.ingest.api_pull import WatermarkStore  # noqa: E402
from ods_pipeline.ingest.api_pull_kafka import run_once as run_once_direct  # noqa: E402


REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BROKERS", "localhost:9092")
CONNECT_URL = os.environ.get("CONNECT_URL", "http://localhost:8083")

DOMAIN = "insurance"
DATASET = "api_pull_lowlat_demo"
SOURCE_APPLICATION = "lowlat_api"
TOPIC = "ods.insurance.api_pull_lowlat_demo"
SUBJECT = "ods.insurance.api_pull_lowlat_demo-value"
SINK_NAME = "jdbc-sink-api-pull-lowlat-demo"
GROUP_ID = f"connect-{SINK_NAME}"
TARGET_TABLE = "ods.insurance_api_pull_lowlat_demo"
TOKEN = "lowlat-bearer-token"
TOKEN_ENV = "API_PULL_LOWLAT_DEMO_TOKEN"
YAML_PATH = os.path.join(REPO_ROOT, "patterns", "insurance", "api_pull_lowlat_demo.yaml")
SCHEMA_PATH = os.path.join(REPO_ROOT, "schemas", "insurance", "api_pull_lowlat_demo.avsc")
CONNECTOR_CONFIG_PATH = os.path.join(
    REPO_ROOT, "docker", "connect-config", "jdbc-sink-api-pull-lowlat-demo.json",
)


# ---------------------------------------------------------------------------
# Skip guards
# ---------------------------------------------------------------------------


def _docker_available() -> bool:
    return shutil.which("docker") is not None


def _container_running(name: str) -> bool:
    try:
        out = subprocess.run(
            ["docker", "ps", "--filter", f"name={name}",
             "--format", "{{.Names}}"],
            capture_output=True, text=True, timeout=10, check=False,
        )
        return name in out.stdout.splitlines()
    except Exception:
        return False


def _confluent_kafka_available() -> bool:
    try:
        import confluent_kafka  # noqa: F401
        return True
    except Exception:
        return False


@pytest.fixture(scope="module", autouse=True)
def _ensure_environment():
    if not _docker_available():
        pytest.skip("docker not available")
    if not _confluent_kafka_available():
        pytest.skip("confluent_kafka not installed in test env")
    for c in (
        "avivaods-postgres-1",
        "avivaods-broker-1",
        "avivaods-schema-registry-1",
        "avivaods-kafka-connect-1",
    ):
        if not _container_running(c):
            pytest.skip(f"required container {c} not running")


# ---------------------------------------------------------------------------
# Schema + connector setup
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def schema_registered():
    with open(SCHEMA_PATH) as f:
        schema_str = json.dumps(json.load(f))
    resp = requests.post(
        f"{SCHEMA_REGISTRY_URL}/subjects/{SUBJECT}/versions",
        json={"schema": schema_str, "schemaType": "AVRO"},
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        timeout=10,
    )
    resp.raise_for_status()
    yield schema_str


@pytest.fixture(scope="module")
def jdbc_sink_registered(schema_registered):
    with open(CONNECTOR_CONFIG_PATH) as f:
        cfg = json.load(f)
    requests.delete(f"{CONNECT_URL}/connectors/{SINK_NAME}", timeout=10)
    # Brief wait for delete to settle in Connect cluster state.
    time.sleep(1.0)
    resp = requests.post(
        f"{CONNECT_URL}/connectors",
        json=cfg,
        headers={"Content-Type": "application/json"},
        timeout=15,
    )
    resp.raise_for_status()
    # Wait for the connector to be RUNNING before tests produce.
    deadline = time.time() + 30
    while time.time() < deadline:
        status = requests.get(
            f"{CONNECT_URL}/connectors/{SINK_NAME}/status", timeout=5,
        ).json()
        if status.get("connector", {}).get("state") == "RUNNING":
            break
        time.sleep(1)
    yield SINK_NAME


# ---------------------------------------------------------------------------
# Stub source
# ---------------------------------------------------------------------------


def _free_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _build_stub_app() -> FastAPI:
    app = FastAPI()

    @app.get("/items")
    def items(
        updated_since: str = Query("2026-01-01T00:00:00Z"),
        authorization: str = Header(default=""),
    ) -> Response:
        if authorization != f"Bearer {TOKEN}":
            raise HTTPException(status_code=401, detail="bad token")
        records = [
            {"request_id": "lowlat-r1", "amount": 11.1,
             "updated_at": "2026-04-02T00:00:00Z"},
            {"request_id": "lowlat-r2", "amount": 22.2,
             "updated_at": "2026-04-03T00:00:00Z"},
            {"request_id": "lowlat-r3", "amount": 33.3,
             "updated_at": "2026-04-04T00:00:00Z"},
        ]
        kept = [r for r in records if r["updated_at"] > updated_since]
        return Response(
            content=json.dumps(kept), media_type="application/json",
        )

    return app


@pytest.fixture(scope="module")
def stub_url():
    app = _build_stub_app()
    port = _free_port()
    config = uvicorn.Config(app, host="127.0.0.1", port=port,
                            log_level="warning")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    url = f"http://127.0.0.1:{port}/items"
    deadline = time.time() + 5
    while time.time() < deadline:
        try:
            requests.get(url, timeout=1)
            break
        except requests.exceptions.RequestException:
            time.sleep(0.05)
    else:
        pytest.fail("uvicorn stub did not bind in 5s")
    yield url
    server.should_exit = True
    thread.join(timeout=2)


@pytest.fixture(autouse=True)
def set_token():
    os.environ[TOKEN_ENV] = TOKEN
    yield
    os.environ.pop(TOKEN_ENV, None)


# ---------------------------------------------------------------------------
# Postgres / config
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def dataset_config_synced(pg_conn):
    yaml_loader.sync_to_db(YAML_PATH, pg_conn)
    yield
    pg_conn.rollback()
    with pg_conn.cursor() as cur:
        cur.execute(
            "DELETE FROM pipeline.dataset_config WHERE domain=%s AND dataset=%s",
            (DOMAIN, DATASET),
        )
    pg_conn.commit()


@pytest.fixture
def control_plane_clean(pg_conn):
    def _wipe():
        pg_conn.rollback()
        with pg_conn.cursor() as cur:
            cur.execute(
                "DELETE FROM pipeline.api_pull_watermark "
                "WHERE domain=%s AND dataset=%s AND source_application=%s",
                (DOMAIN, DATASET, SOURCE_APPLICATION),
            )
            cur.execute(
                f"DELETE FROM {TARGET_TABLE} WHERE _ods_dataset=%s OR _ods_domain=%s",
                (DATASET, DOMAIN),
            )
        pg_conn.commit()
    _wipe()
    yield
    _wipe()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _consumer_group_offsets(topic: str, group: str) -> dict[int, int]:
    """Read the committed offsets of ``group`` for ``topic`` using a
    short-lived Consumer. Avoids ``ConsumerGroupTopicPartitions`` which
    is not exported in older confluent-kafka-python."""
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": group,
        "enable.auto.commit": False,
        "session.timeout.ms": 6000,
    })
    try:
        md = consumer.list_topics(topic=topic, timeout=5)
        if topic not in md.topics or md.topics[topic].error is not None:
            return {}
        partitions = list(md.topics[topic].partitions.keys())
        tps = [TopicPartition(topic, p) for p in partitions]
        committed = consumer.committed(tps, timeout=10)
    finally:
        consumer.close()
    return {
        int(tp.partition): int(tp.offset)
        for tp in committed if tp.offset >= 0
    }


# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------


def test_e2e_direct_kafka_publish_and_jdbc_sink(
    stub_url, schema_registered, jdbc_sink_registered, pg_conn,
    dataset_config_synced, control_plane_clean,
):
    """Full happy path: stub → produce → sink → Postgres → cursor commit."""
    run_id = str(uuid.uuid4())
    business_date = "2026-05-05"

    cfg = {
        "domain": DOMAIN,
        "dataset": DATASET,
        "schema_id": SUBJECT,
        "schema_version": 1,
        "target_topic": TOPIC,
        "source": {
            "application": SOURCE_APPLICATION,
            "url": stub_url,
            "auth": {"type": "bearer", "secret_ref": TOKEN_ENV},
            "cursor": {
                "style": "since_timestamp",
                "request_param": "updated_since",
                "response_field": "updated_at",
                "initial": "2026-01-01T00:00:00Z",
            },
            "page": {"style": "none"},
            "timeout_seconds": 10,
            "retries": 0,
        },
    }

    result = run_once_direct(
        dataset_config=cfg,
        kafka_bootstrap=KAFKA_BOOTSTRAP,
        schema_registry_url=SCHEMA_REGISTRY_URL,
        schema_str=schema_registered,
        committed_cursor_value=None,
        run_id=run_id,
        business_date=business_date,
    )

    assert result.no_changes is False
    assert result.record_count == 3
    assert result.new_cursor_value == "2026-04-04T00:00:00Z"
    assert result.offset_end_by_partition  # must have at least one partition

    # Wait for the JDBC sink consumer group to catch up to the produced
    # end offsets, then assert Postgres rows landed.
    deadline = time.time() + 120
    consumed = {}
    while time.time() < deadline:
        consumed = _consumer_group_offsets(TOPIC, GROUP_ID)
        ok = consumed and all(
            consumed.get(int(p), -1) >= int(target)
            for p, target in result.offset_end_by_partition.items()
        )
        if ok:
            break
        time.sleep(2)
    else:
        pytest.fail(
            f"jdbc sink did not consume offsets in 60s. "
            f"consumed={consumed} target={result.offset_end_by_partition}"
        )

    pg_conn.rollback()
    with pg_conn.cursor() as cur:
        cur.execute(
            f"SELECT request_id, amount FROM {TARGET_TABLE} "
            f"WHERE _ods_run_id=%s ORDER BY request_id",
            (run_id,),
        )
        rows = cur.fetchall()
    assert len(rows) == 3, rows
    assert {r[0] for r in rows} == {"lowlat-r1", "lowlat-r2", "lowlat-r3"}
    assert {r[1] for r in rows} == {11.1, 22.2, 33.3}

    # Watermark promote: this run's offsets are sunk → simulate the
    # finalise_watermark sensor's work and prove committed advances.
    store = WatermarkStore(pg_conn)
    store.read(domain=DOMAIN, dataset=DATASET,
               source_application=SOURCE_APPLICATION,
               cursor_type="since_timestamp")
    store.try_lock(domain=DOMAIN, dataset=DATASET,
                   source_application=SOURCE_APPLICATION, run_id=run_id)
    store.record_pending(domain=DOMAIN, dataset=DATASET,
                         source_application=SOURCE_APPLICATION, run_id=run_id,
                         new_cursor_value=result.new_cursor_value)
    store.unlock(domain=DOMAIN, dataset=DATASET,
                 source_application=SOURCE_APPLICATION)
    promoted = store.promote(domain=DOMAIN, dataset=DATASET,
                             source_application=SOURCE_APPLICATION,
                             run_id=run_id)
    assert promoted is True
    row = store.read(domain=DOMAIN, dataset=DATASET,
                     source_application=SOURCE_APPLICATION,
                     cursor_type="since_timestamp")
    assert row.committed_cursor_value == "2026-04-04T00:00:00Z"
    assert row.last_successful_run_id == run_id
