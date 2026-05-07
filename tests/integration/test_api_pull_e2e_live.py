"""End-to-end live test for the API pull pattern.

Drives the canonical API-pull demo path via real components: a host
FastAPI stub source, real LocalStack S3, real Postgres, real Kafka,
Schema Registry, Kafka Connect, and the real Glue Spark image invoked
through ``docker run``.

Coverage:

  Stub API → poll_and_archive → S3 JSONL archive → file_catalogue
  registration → dag_ingest's stage_ingest equivalent (Glue jsonl read,
  schema validate, DQ, curated parquet write) → run_log + run_stage_log
  + lineage_edge inserts → watermark promotion via the explicit
  triggered_by_api_pull linkage.

What it intentionally does NOT cover (kept in
docs/api-pull-backlog.md item 1):

  * Kafka publish stage (ods_s3_publish.py) — same code path as the
    file pattern; covered by tests/integration/test_run_events.py.
  * Canonicalize stage and JDBC sink wait — these require Avro schema
    registration and a JDBC connector for the api_pull dataset; they
    are deferred to the next slice once the connector is provisioned.

Note: the list above is kept for historical context. The current test
does cover Kafka publish and JDBC sink for api_pull_demo. Canonicalize is
not part of this specific dataset because it is configured as canonical
(`is_canonical=true`).

Skipped automatically when docker / the Glue image are not available.
"""
from __future__ import annotations

import gzip
import io
import json
import os
import shutil
import socket
import subprocess
import sys
import threading
import time
import uuid

import boto3
import pytest
import requests
import uvicorn
from fastapi import FastAPI, Header, HTTPException, Query, Response

# Allow the test to invoke yaml_loader without Airflow installed; the
# loader only depends on PyYAML and psycopg2.
sys.path.insert(0, os.path.abspath(os.path.join(
    os.path.dirname(__file__), "..", "..", "airflow", "dags",
)))
from common import yaml_loader  # type: ignore  # noqa: E402

from ods_pipeline.ingest.api_pull import (  # noqa: E402
    TRIGGERED_BY_API_PULL_EDGE,
    WatermarkStore,
    derive_dag_ingest_parent_run_id,
    ingest_status_for_api_pull_run,
    poll_and_archive,
)


REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
LOCALSTACK_ENDPOINT = os.environ.get("LOCALSTACK_ENDPOINT", "http://localhost:4566")
SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")
CONNECT_URL = os.environ.get("CONNECT_URL", "http://localhost:8083")
RAW_BUCKET = "ods-raw-local"
CURATED_BUCKET = "ods-curated-local"
NETWORK = "ods-network"

DOMAIN = "insurance"
DATASET = "api_pull_demo"
SOURCE_APPLICATION = "demo_api_e2e"
RISK_DATASET = "api_pull_risk"
RISK_SOURCE_APPLICATION = "demo_api_risk"
TOKEN_ENV = "API_PULL_E2E_TOKEN"
TOKEN = "e2e-bearer-token"
YAML_PATH = os.path.join(
    REPO_ROOT, "patterns", "insurance", "api_pull_demo.yaml",
)
RISK_YAML_PATH = os.path.join(
    REPO_ROOT, "patterns", "insurance", "api_pull_risk.yaml",
)
API_PULL_SCHEMA_PATH = os.path.join(
    REPO_ROOT, "schemas", "insurance", "api_pull_demo.avsc",
)
API_PULL_RISK_RAW_SCHEMA_PATH = os.path.join(
    REPO_ROOT, "schemas", "insurance", "api_pull_risk_raw.avsc",
)
API_PULL_RISK_CANONICAL_SCHEMA_PATH = os.path.join(
    REPO_ROOT, "schemas", "insurance", "api_pull_risk_canonical.avsc",
)
API_PULL_CONNECTOR_PATH = os.path.join(
    REPO_ROOT, "docker", "connect-config", "jdbc-sink-api-pull-demo.json",
)
API_PULL_RISK_CONNECTOR_PATH = os.path.join(
    REPO_ROOT, "docker", "connect-config", "jdbc-sink-api-pull-risk.json",
)
API_PULL_SINK_MIGRATION = os.path.join(
    REPO_ROOT, "db", "migrations", "25_api_pull_demo_sink.sql",
)
API_PULL_RISK_SINK_MIGRATION = os.path.join(
    REPO_ROOT, "db", "migrations", "26_api_pull_risk_sink.sql",
)


# ---------------------------------------------------------------------------
# Skip-if-environment-not-ready guards
# ---------------------------------------------------------------------------


def _docker_available() -> bool:
    return shutil.which("docker") is not None


def _glue_image_present() -> bool:
    try:
        out = subprocess.run(
            ["docker", "image", "inspect", "ods-glue:local"],
            capture_output=True, text=True, timeout=10, check=False,
        )
        return out.returncode == 0
    except Exception:
        return False


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


@pytest.fixture(scope="module", autouse=True)
def _ensure_environment():
    if not _docker_available():
        pytest.skip("docker not available")
    if not _glue_image_present():
        pytest.skip("ods-glue:local image not built")
    if not _container_running("avivaods-postgres-1"):
        pytest.skip("postgres container not running")
    if not _container_running("avivaods-localstack-1"):
        pytest.skip("localstack container not running")
    if not _container_running("avivaods-broker-1"):
        pytest.skip("kafka broker container not running")
    if not _container_running("avivaods-schema-registry-1"):
        pytest.skip("schema registry container not running")
    if not _container_running("avivaods-kafka-connect-1"):
        pytest.skip("kafka connect container not running")


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
            {"request_id": "req-001",
             "updated_at": "2026-04-02T00:00:00Z",
             "amount": 120.5},
            {"request_id": "req-002",
             "updated_at": "2026-04-03T00:00:00Z",
             "amount": 99.0},
            {"request_id": "req-003",
             "updated_at": "2026-04-04T00:00:00Z",
             "amount": 250.0},
        ]
        kept = [r for r in records if r["updated_at"] > updated_since]
        return Response(
            content=json.dumps(kept), media_type="application/json",
        )

    @app.get("/risk-items")
    def risk_items(
        updated_since: str = Query("2026-01-01T00:00:00Z"),
        authorization: str = Header(default=""),
    ) -> Response:
        if authorization != f"Bearer {TOKEN}":
            raise HTTPException(status_code=401, detail="bad token")
        records = [
            {
                "RskID": "API-R1",
                "PolNo": "POL-API-1",
                "ExposureAmt": 120.5,
                "AsOfDt": "20260502",
                "updated_at": "2026-04-02T00:00:00Z",
            },
            {
                "RskID": "API-R2",
                "PolNo": "POL-API-2",
                "ExposureAmt": 99.0,
                "AsOfDt": "20260502",
                "updated_at": "2026-04-03T00:00:00Z",
            },
            {
                "RskID": "API-R3",
                "PolNo": "POL-API-3",
                "ExposureAmt": 250.0,
                "AsOfDt": None,
                "updated_at": "2026-04-04T00:00:00Z",
            },
        ]
        kept = [r for r in records if r["updated_at"] > updated_since]
        return Response(content=json.dumps(kept), media_type="application/json")

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


# ---------------------------------------------------------------------------
# Postgres + S3 fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=LOCALSTACK_ENDPOINT,
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
        region_name=os.environ.get("AWS_DEFAULT_REGION", "eu-west-1"),
    )


@pytest.fixture(scope="module", autouse=True)
def _ensure_buckets(s3_client):
    for bucket in (RAW_BUCKET, CURATED_BUCKET):
        try:
            s3_client.head_bucket(Bucket=bucket)
        except Exception:
            s3_client.create_bucket(Bucket=bucket)


@pytest.fixture
def set_token():
    os.environ[TOKEN_ENV] = TOKEN
    yield
    os.environ.pop(TOKEN_ENV, None)


@pytest.fixture(scope="module")
def dataset_config_synced(pg_conn):
    """Sync patterns/insurance/api_pull_demo.yaml into pipeline.dataset_config
    via the production yaml_loader. The DAG and Glue ingestion both
    resolve config through this row."""
    with open(API_PULL_SINK_MIGRATION, encoding="utf-8") as migration:
        with pg_conn.cursor() as cur:
            cur.execute(migration.read())
    with open(API_PULL_RISK_SINK_MIGRATION, encoding="utf-8") as migration:
        with pg_conn.cursor() as cur:
            cur.execute(migration.read())
    pg_conn.commit()
    yaml_loader.sync_to_db(YAML_PATH, pg_conn)
    yaml_loader.sync_to_db(RISK_YAML_PATH, pg_conn)
    yield
    pg_conn.rollback()
    with pg_conn.cursor() as cur:
        cur.execute(
            "DELETE FROM pipeline.dataset_config "
            "WHERE domain=%s AND dataset IN (%s, %s)",
            (DOMAIN, DATASET, RISK_DATASET),
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
                "DELETE FROM pipeline.run_stage_log "
                "WHERE run_id IN (SELECT run_id FROM pipeline.run_log "
                "                  WHERE domain=%s AND dataset IN (%s, %s))",
                (DOMAIN, DATASET, RISK_DATASET),
            )
            cur.execute(
                "DELETE FROM pipeline.run_kafka_offsets "
                "WHERE run_id IN (SELECT run_id FROM pipeline.run_log "
                "                  WHERE domain=%s AND dataset IN (%s, %s))",
                (DOMAIN, DATASET, RISK_DATASET),
            )
            cur.execute(
                "DELETE FROM pipeline.lineage_edge "
                "WHERE child_run_id IN (SELECT run_id FROM pipeline.run_log "
                "                        WHERE domain=%s AND dataset IN (%s, %s))",
                (DOMAIN, DATASET, RISK_DATASET),
            )
            cur.execute(
                "DELETE FROM pipeline.reconciliation_log "
                "WHERE domain=%s AND dataset IN (%s, %s)",
                (DOMAIN, DATASET, RISK_DATASET),
            )
            cur.execute(
                "DELETE FROM pipeline.run_log "
                "WHERE domain=%s AND dataset IN (%s, %s)",
                (DOMAIN, DATASET, RISK_DATASET),
            )
            cur.execute(
                "DELETE FROM pipeline.file_catalogue "
                "WHERE domain=%s AND dataset IN (%s, %s)",
                (DOMAIN, DATASET, RISK_DATASET),
            )
            cur.execute(
                "DELETE FROM pipeline.file_state "
                "WHERE s3_path LIKE %s OR s3_path LIKE %s "
                "   OR s3_path LIKE %s OR s3_path LIKE %s",
                (
                    f"s3://{RAW_BUCKET}/api_pull/{DOMAIN}/{DATASET}/%",
                    f"s3://{CURATED_BUCKET}/{DOMAIN}/{DATASET}/%",
                    f"s3://{RAW_BUCKET}/api_pull/{DOMAIN}/{RISK_DATASET}/%",
                    f"s3://{CURATED_BUCKET}/{DOMAIN}/{RISK_DATASET}/%",
                ),
            )
            cur.execute(
                "DELETE FROM ods.insurance_api_pull_demo "
                "WHERE request_id IN ('req-001', 'req-002', 'req-003') "
                "   OR _ods_dataset=%s",
                (DATASET,),
            )
            cur.execute(
                "DELETE FROM ods.insurance_api_pull_risk "
                "WHERE risk_id LIKE 'API-R%%' OR _ods_dataset=%s",
                (RISK_DATASET,),
            )
            cur.execute(
                "DELETE FROM pipeline.api_pull_watermark "
                "WHERE domain=%s AND dataset=%s AND source_application=%s",
                (DOMAIN, RISK_DATASET, RISK_SOURCE_APPLICATION),
            )
        pg_conn.commit()

    _wipe()
    yield
    _wipe()


# ---------------------------------------------------------------------------
# Glue subprocess
# ---------------------------------------------------------------------------


def _glue_env_args() -> list[str]:
    base = [
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
        "-e", "ODS_SOURCE_APPLICATION=demo_api_e2e",
    ]
    base += [
        "-v", f"{REPO_ROOT}/glue/jobs:/home/glue_user/workspace/jobs",
        "-v", f"{REPO_ROOT}/ods_pipeline:/home/glue_user/ods_pipeline",
    ]
    return base


def _run_glue_jsonl_ingest(*, run_id, file_id, s3_input_path,
                          parent_run_id, domain=DOMAIN,
                          dataset=DATASET) -> subprocess.CompletedProcess:
    cmd = [
        "docker", "run", "--rm", "--network", NETWORK,
        *_glue_env_args(),
        "ods-glue:local", "spark-submit",
        "--py-files",
        "/home/glue_user/workspace/jobs/utils.py,"
        "/home/glue_user/workspace/jobs/utils_bootstrap.py,"
        "/home/glue_user/workspace/jobs/utils_data.py,"
        "/home/glue_user/workspace/jobs/utils_config.py,"
        "/home/glue_user/workspace/jobs/utils_state.py,"
        "/home/glue_user/workspace/jobs/utils_runs.py,"
        "/home/glue_user/workspace/jobs/utils_jobs.py,"
        "/home/glue_user/workspace/jobs/dq.py",
        "/home/glue_user/workspace/jobs/ods_ingestion.py",
        "--run_id", run_id,
        "--domain", domain,
        "--dataset", dataset,
        "--s3_input_path", s3_input_path,
        "--file_id", file_id,
        "--parent_run_id", parent_run_id,
    ]
    return subprocess.run(cmd, capture_output=True, text=True, timeout=420)


def _run_glue_publish(*, run_id, file_id, s3_input_path,
                     parent_run_id, domain=DOMAIN,
                     dataset=DATASET) -> subprocess.CompletedProcess:
    cmd = [
        "docker", "run", "--rm", "--network", NETWORK,
        *_glue_env_args(),
        "-e", "KAFKA_BOOTSTRAP_SERVERS=broker:29092",
        "ods-glue:local", "spark-submit",
        "--py-files",
        "/home/glue_user/workspace/jobs/utils.py,"
        "/home/glue_user/workspace/jobs/utils_bootstrap.py,"
        "/home/glue_user/workspace/jobs/utils_data.py,"
        "/home/glue_user/workspace/jobs/utils_config.py,"
        "/home/glue_user/workspace/jobs/utils_state.py,"
        "/home/glue_user/workspace/jobs/utils_runs.py,"
        "/home/glue_user/workspace/jobs/utils_jobs.py,"
        "/home/glue_user/workspace/jobs/dq.py",
        "/home/glue_user/workspace/jobs/ods_s3_publish.py",
        "--run_id", run_id,
        "--domain", domain,
        "--dataset", dataset,
        "--s3_input_path", s3_input_path,
        "--file_id", file_id,
        "--parent_run_id", parent_run_id,
    ]
    return subprocess.run(cmd, capture_output=True, text=True, timeout=420)


def _run_glue_canonicalize(
    *,
    run_id,
    file_id,
    parent_run_id,
    offset_ranges: dict[int, dict[str, int]],
    business_date: str,
) -> subprocess.CompletedProcess:
    cmd = [
        "docker", "run", "--rm", "--network", NETWORK,
        *_glue_env_args(),
        "-e", "KAFKA_BOOTSTRAP_SERVERS=broker:29092",
        "-v", f"{REPO_ROOT}/patterns:/home/glue_user/patterns",
        "ods-glue:local", "spark-submit",
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
        "--domain", DOMAIN,
        "--dataset", RISK_DATASET,
        "--raw_topic", "ods.insurance.api_pull_risk",
        "--canonical_topic", "ods.insurance.api_pull_risk-canonical",
        "--transform_yaml_path", "/home/glue_user/patterns/insurance/api_pull_risk.yaml",
        "--offset_ranges", json.dumps(offset_ranges),
        "--file_id", file_id,
        "--parent_run_id", parent_run_id,
        "--business_date", business_date,
    ]
    return subprocess.run(cmd, capture_output=True, text=True, timeout=420)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _decode_archive(s3_client, bucket: str, key: str) -> list[dict]:
    body = s3_client.get_object(Bucket=bucket, Key=key)["Body"].read()
    with gzip.GzipFile(fileobj=io.BytesIO(body), mode="rb") as gz:
        return [json.loads(line) for line in gz.read().splitlines() if line]


def _open_runs(conn, *, pipeline_type: str) -> list[tuple]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT run_id::text, status FROM pipeline.run_log "
            "WHERE domain=%s AND dataset=%s AND pipeline_type=%s "
            "ORDER BY started_at",
            (DOMAIN, DATASET, pipeline_type),
        )
        return cur.fetchall()


def _stage_statuses(conn, run_id: str) -> dict[str, str]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT stage, status FROM pipeline.run_stage_log "
            "WHERE run_id=%s ORDER BY id DESC",
            (run_id,),
        )
        rows = cur.fetchall()
    out: dict[str, str] = {}
    for stage, status in rows:
        out.setdefault(stage, status)
    return out


def _register_schema(subject: str, schema_path: str) -> None:
    with open(schema_path, encoding="utf-8") as schema_file:
        schema = json.dumps(json.load(schema_file))
    resp = requests.post(
        f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions",
        json={"schemaType": "AVRO", "schema": schema},
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        timeout=10,
    )
    assert resp.status_code in (200, 201, 409), resp.text


def _register_api_pull_schema() -> None:
    _register_schema("ods.insurance.api_pull_demo-value", API_PULL_SCHEMA_PATH)


def _provision_connector(name: str, connector_path: str) -> None:
    requests.delete(f"{CONNECT_URL}/connectors/{name}", timeout=10)
    with open(connector_path, encoding="utf-8") as connector_file:
        config = json.load(connector_file)
    resp = requests.post(
        f"{CONNECT_URL}/connectors",
        json=config,
        headers={"Content-Type": "application/json"},
        timeout=10,
    )
    assert resp.status_code in (200, 201, 409), resp.text


def _provision_api_pull_sink() -> None:
    _provision_connector("jdbc-sink-api-pull-demo", API_PULL_CONNECTOR_PATH)


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
        combined = (result.stderr + result.stdout).decode("utf-8", errors="ignore").lower()
        if "already exists" in combined:
            return
        if "topicexists" not in combined:
            result.check_returncode()
        time.sleep(1)
    result.check_returncode()


def _wait_api_pull_sink_rows(conn, file_id: str, timeout: int = 120) -> list[tuple]:
    deadline = time.time() + timeout
    rows: list[tuple] = []
    while time.time() < deadline:
        conn.rollback()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT request_id, payload, _ods_run_id, _ods_file_id
                  FROM ods.insurance_api_pull_demo
                 WHERE _ods_file_id=%s
                 ORDER BY request_id
                """,
                (str(file_id),),
            )
            rows = cur.fetchall()
        if len(rows) == 3:
            return rows
        time.sleep(2)
    return rows


def _wait_api_pull_risk_rows(conn, canonicalize_run_id: str, timeout: int = 120) -> list[tuple]:
    deadline = time.time() + timeout
    rows: list[tuple] = []
    while time.time() < deadline:
        conn.rollback()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT risk_id, policy_id, exposure_amount::text, as_of_date::text,
                       _ods_canonicalize_run_id
                  FROM ods.insurance_api_pull_risk
                 WHERE _ods_canonicalize_run_id=%s
                 ORDER BY risk_id
                """,
                (canonicalize_run_id,),
            )
            rows = cur.fetchall()
        if len(rows) == 2:
            return rows
        time.sleep(2)
    return rows


def _offset_ranges_for_run(conn, run_id: str) -> dict[int, dict[str, int]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT partition, offset_start, offset_end
              FROM pipeline.run_kafka_offsets
             WHERE run_id=%s AND stage='kafka_publish'
             ORDER BY partition
            """,
            (run_id,),
        )
        rows = cur.fetchall()
    return {
        int(partition): {"start": int(start), "end": int(end)}
        for partition, start, end in rows
    }


def _run_t2_reconcile() -> None:
    old_dsn = os.environ.get("PIPELINE_PG_DSN")
    os.environ["PIPELINE_PG_DSN"] = (
        "host=127.0.0.1 port=5440 dbname=ods_dev user=ods password=ods"
    )
    try:
        import importlib.util

        module_path = os.path.join(REPO_ROOT, "airflow", "dags", "dag_recon_t2.py")
        spec = importlib.util.spec_from_file_location("dag_recon_t2_live", module_path)
        assert spec and spec.loader
        dag_recon_t2 = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(dag_recon_t2)
        dag_recon_t2.reconcile.func()
    finally:
        if old_dsn is None:
            os.environ.pop("PIPELINE_PG_DSN", None)
        else:
            os.environ["PIPELINE_PG_DSN"] = old_dsn


# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------


def test_e2e_stub_to_curated_parquet_with_watermark_promotion(
    stub_url, s3_client, pg_conn, set_token,
    dataset_config_synced, control_plane_clean,
):
    """Full happy path through Glue JSONL ingestion + watermark promote."""
    _register_api_pull_schema()
    _provision_api_pull_sink()

    api_pull_run_id = str(uuid.uuid4())
    business_date = "2026-05-02"

    # ------------------------------------------------------------------
    # 1) Poll the stub source and archive to S3 JSONL.
    # ------------------------------------------------------------------
    archive = poll_and_archive(
        dataset_config={
            "domain": DOMAIN,
            "dataset": DATASET,
            "schema_id": "ods.insurance.api_pull_demo-value",
            "schema_version": 1,
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
        },
        s3_client=s3_client,
        archive_bucket=RAW_BUCKET,
        committed_cursor_value=None,
        run_id=api_pull_run_id,
        business_date=business_date,
    )
    assert archive.no_changes is False
    assert archive.record_count == 3
    assert archive.new_cursor_value == "2026-04-04T00:00:00Z"

    archive_lines = _decode_archive(s3_client, RAW_BUCKET, archive.s3_key)
    assert {ln["payload"]["request_id"] for ln in archive_lines} == {
        "req-001", "req-002", "req-003",
    }

    # ------------------------------------------------------------------
    # 2) Register the archive in file_catalogue and stage api_pull
    #    control-plane rows (run_log, lineage, recon, pending watermark).
    # ------------------------------------------------------------------
    import ods_pipeline

    ods_pipeline.runs.start(
        pg_conn,
        run_id=api_pull_run_id,
        pipeline_type="api_pull",
        domain=DOMAIN,
        dataset=DATASET,
        business_date=business_date,
        kafka_topic=None,
    )
    file_id = ods_pipeline.files.upsert(
        pg_conn,
        domain=DOMAIN,
        dataset=DATASET,
        business_date=business_date,
        file_md5=archive.file_md5,
        s3_raw_path=archive.s3_uri,
        file_size_bytes=archive.file_size_bytes,
        source_row_count=archive.record_count,
        state="received",
        last_run_id=api_pull_run_id,
    )
    ods_pipeline.lineage.write_edge(
        pg_conn,
        child_run_id=api_pull_run_id,
        parent_file_id=file_id,
        edge_type="api_to_archive",
        source_ref=stub_url,
        target_ref=archive.s3_uri,
        record_count=archive.record_count,
    )
    ods_pipeline.reconciliation.write_check(
        pg_conn,
        check_type="api_pull_archive_count",
        run_id=api_pull_run_id,
        domain=DOMAIN,
        dataset=DATASET,
        business_date=business_date,
        source_count=archive.record_count,
        kafka_count=None,
        postgres_count=None,
        status="ok",
        detail=json.dumps({"fetched_count": archive.record_count}),
    )

    store = WatermarkStore(pg_conn)
    store.read(domain=DOMAIN, dataset=DATASET,
               source_application=SOURCE_APPLICATION,
               cursor_type="since_timestamp")
    store.try_lock(domain=DOMAIN, dataset=DATASET,
                   source_application=SOURCE_APPLICATION,
                   run_id=api_pull_run_id)
    store.record_pending(domain=DOMAIN, dataset=DATASET,
                         source_application=SOURCE_APPLICATION,
                         run_id=api_pull_run_id,
                         new_cursor_value=archive.new_cursor_value)
    store.unlock(domain=DOMAIN, dataset=DATASET,
                 source_application=SOURCE_APPLICATION)

    # ------------------------------------------------------------------
    # 3) Drive Glue JSONL ingestion (the dag_ingest stage_ingest
    #    equivalent) via docker run. This exercises the new raw_format
    #    branch in glue/jobs/ods_ingestion.py end-to-end.
    # ------------------------------------------------------------------
    # Use the same deterministic parent_run_id dag_api_pull.poll_one
    # would have pre-minted so the linkage helper exercises the
    # exact-PK match.
    parent_run_id = derive_dag_ingest_parent_run_id(api_pull_run_id)
    ingest_run_id = str(uuid.uuid4())

    # Stand in for dag_ingest.init_run: create the s3_batch parent run
    # carrying the triggered_by_api_pull edge so the linkage helper can
    # resolve it after Glue completes.
    ods_pipeline.runs.start(
        pg_conn,
        run_id=parent_run_id,
        pipeline_type="s3_batch",
        domain=DOMAIN,
        dataset=DATASET,
        business_date=business_date,
        file_id=file_id,
        parents=[{
            "run_id": api_pull_run_id,
            "edge_type": TRIGGERED_BY_API_PULL_EDGE,
        }],
    )

    glue_result = _run_glue_jsonl_ingest(
        run_id=ingest_run_id,
        file_id=file_id,
        s3_input_path=archive.s3_uri,
        parent_run_id=parent_run_id,
    )
    assert glue_result.returncode == 0, (
        "glue ingestion failed:\n"
        f"STDOUT:\n{glue_result.stdout[-4000:]}\n"
        f"STDERR:\n{glue_result.stderr[-4000:]}"
    )

    # ------------------------------------------------------------------
    # 4) Verify control-plane state landed correctly.
    # ------------------------------------------------------------------
    pg_conn.rollback()
    api_pull_runs = _open_runs(pg_conn, pipeline_type="api_pull")
    assert any(rid == api_pull_run_id for rid, _ in api_pull_runs)

    ingest_runs = _open_runs(pg_conn, pipeline_type="ingestion")
    assert any(rid == ingest_run_id and status == "succeeded"
               for rid, status in ingest_runs), (
        f"ingestion run {ingest_run_id} did not succeed: {ingest_runs}"
    )

    stages = _stage_statuses(pg_conn, ingest_run_id)
    assert stages.get("raw_read") == "succeeded", stages
    assert stages.get("curated_write") == "succeeded", stages
    # DQ check happened against an empty hard_blocks rules — succeeded or
    # warned both acceptable.
    assert stages.get("dq_check") in {"succeeded", "warned"}, stages

    # File catalogue should have advanced to 'curated' with the curated path set.
    with pg_conn.cursor() as cur:
        cur.execute(
            "SELECT state, s3_curated_path FROM pipeline.file_catalogue "
            "WHERE file_id=%s",
            (file_id,),
        )
        state, curated_path = cur.fetchone()
    assert state == "curated", (state, curated_path)
    assert curated_path and curated_path.startswith("s3://ods-curated-local/"), curated_path

    # ------------------------------------------------------------------
    # 4b) Publish curated Parquet to Kafka and verify the JDBC sink lands
    #     the same three records in Postgres.
    # ------------------------------------------------------------------
    publish_run_id = str(uuid.uuid4())
    publish_result = _run_glue_publish(
        run_id=publish_run_id,
        file_id=file_id,
        s3_input_path=curated_path,
        parent_run_id=parent_run_id,
    )
    assert publish_result.returncode == 0, (
        "glue publish failed:\n"
        f"STDOUT:\n{publish_result.stdout[-4000:]}\n"
        f"STDERR:\n{publish_result.stderr[-4000:]}"
    )

    sink_rows = _wait_api_pull_sink_rows(pg_conn, file_id)
    assert [(row[0], row[2], row[3]) for row in sink_rows] == [
        ("req-001", ingest_run_id, str(file_id)),
        ("req-002", ingest_run_id, str(file_id)),
        ("req-003", ingest_run_id, str(file_id)),
    ]
    decoded_payloads = [json.loads(row[1]) for row in sink_rows]
    assert {payload["amount"] for payload in decoded_payloads} == {99.0, 120.5, 250.0}

    with pg_conn.cursor() as cur:
        cur.execute(
            """
            SELECT status, record_count_published, kafka_topic
              FROM pipeline.run_log
             WHERE run_id=%s
            """,
            (publish_run_id,),
        )
        publish_log = cur.fetchone()
        cur.execute(
            """
            SELECT status, source_count, kafka_count, discrepancy_count
              FROM pipeline.reconciliation_log
             WHERE run_id=%s AND check_type='t0_publish_count'
            """,
            (publish_run_id,),
        )
        publish_recon = cur.fetchone()
        cur.execute(
            """
            SELECT COUNT(*), COALESCE(SUM(record_count), 0)
              FROM pipeline.run_kafka_offsets
             WHERE run_id=%s
            """,
            (publish_run_id,),
        )
        offset_summary = cur.fetchone()
    assert publish_log == ("succeeded", 3, "ods.insurance.api_pull_demo")
    assert publish_recon == ("ok", 3, 3, 0)
    assert offset_summary[0] >= 1
    assert offset_summary[1] == 3

    # Lineage api_to_archive (written by us), raw_to_curated (written by
    # ods_ingestion), and curated_to_kafka (written by publish) must exist.
    with pg_conn.cursor() as cur:
        cur.execute(
            "SELECT edge_type FROM pipeline.lineage_edge "
            "WHERE parent_file_id=%s",
            (file_id,),
        )
        edges = {row[0] for row in cur.fetchall()}
    assert "api_to_archive" in edges, edges
    assert "raw_to_curated" in edges, edges
    assert "curated_to_kafka" in edges, edges

    # Reconciliation row from poll.
    with pg_conn.cursor() as cur:
        cur.execute(
            "SELECT status, source_count FROM pipeline.reconciliation_log "
            "WHERE run_id=%s AND check_type='api_pull_archive_count'",
            (api_pull_run_id,),
        )
        recon = cur.fetchone()
    assert recon == ("ok", archive.record_count), recon

    # ------------------------------------------------------------------
    # 5) Linkage + watermark promotion: the s3_batch run carrying the
    #    triggered_by_api_pull edge succeeded → promote.
    # ------------------------------------------------------------------
    ods_pipeline.runs.update(pg_conn, parent_run_id, status="succeeded")

    assert ingest_status_for_api_pull_run(
        pg_conn, api_pull_run_id,
        expected_parent_run_id=parent_run_id,
    ) == "succeeded"

    promoted = store.promote(
        domain=DOMAIN, dataset=DATASET,
        source_application=SOURCE_APPLICATION,
        run_id=api_pull_run_id,
    )
    assert promoted is True

    row = store.read(
        domain=DOMAIN, dataset=DATASET,
        source_application=SOURCE_APPLICATION,
        cursor_type="since_timestamp",
    )
    assert row.committed_cursor_value == archive.new_cursor_value
    assert row.pending_cursor_value is None
    assert row.last_successful_run_id == api_pull_run_id

    # ------------------------------------------------------------------
    # 6) Replay safety: a SECOND poll using the now-committed cursor
    #    must observe no_changes (stub returns 0 records past
    #    2026-04-04T00:00:00Z) and not advance the cursor.
    # ------------------------------------------------------------------
    second_run_id = str(uuid.uuid4())
    second_archive = poll_and_archive(
        dataset_config={
            "domain": DOMAIN,
            "dataset": DATASET,
            "schema_id": "ods.insurance.api_pull_demo-value",
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
        },
        s3_client=s3_client,
        archive_bucket=RAW_BUCKET,
        committed_cursor_value=row.committed_cursor_value,
        run_id=second_run_id,
        business_date=business_date,
    )
    assert second_archive.no_changes is True
    assert second_archive.record_count == 0


def test_e2e_api_pull_noncanonical_to_canonical_jdbc_with_t1_t2_recon(
    stub_url, s3_client, pg_conn, set_token,
    dataset_config_synced, control_plane_clean,
):
    """API Pull non-canonical source -> raw Kafka -> canonical Kafka -> JDBC."""
    _register_schema("ods.insurance.api_pull_risk-value", API_PULL_RISK_RAW_SCHEMA_PATH)
    _register_schema(
        "ods.insurance.api_pull_risk-canonical-value",
        API_PULL_RISK_CANONICAL_SCHEMA_PATH,
    )
    _recreate_topic("ods.insurance.api_pull_risk")
    _recreate_topic("ods.insurance.api_pull_risk-canonical")
    _provision_connector(
        "jdbc-sink-insurance-api-pull-risk",
        API_PULL_RISK_CONNECTOR_PATH,
    )

    import ods_pipeline

    api_pull_run_id = str(uuid.uuid4())
    business_date = "2026-05-02"
    risk_url = stub_url.replace("/items", "/risk-items")

    archive = poll_and_archive(
        dataset_config={
            "domain": DOMAIN,
            "dataset": RISK_DATASET,
            "schema_id": "ods.insurance.api_pull_risk-value",
            "schema_version": 1,
            "source": {
                "application": RISK_SOURCE_APPLICATION,
                "url": risk_url,
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
        },
        s3_client=s3_client,
        archive_bucket=RAW_BUCKET,
        committed_cursor_value=None,
        run_id=api_pull_run_id,
        business_date=business_date,
    )
    assert archive.record_count == 3

    ods_pipeline.runs.start(
        pg_conn,
        run_id=api_pull_run_id,
        pipeline_type="api_pull",
        domain=DOMAIN,
        dataset=RISK_DATASET,
        business_date=business_date,
    )
    file_id = ods_pipeline.files.upsert(
        pg_conn,
        domain=DOMAIN,
        dataset=RISK_DATASET,
        business_date=business_date,
        file_md5=archive.file_md5,
        s3_raw_path=archive.s3_uri,
        file_size_bytes=archive.file_size_bytes,
        source_row_count=archive.record_count,
        state="received",
        last_run_id=api_pull_run_id,
    )
    ods_pipeline.lineage.write_edge(
        pg_conn,
        child_run_id=api_pull_run_id,
        parent_file_id=file_id,
        edge_type="api_to_archive",
        source_ref=risk_url,
        target_ref=archive.s3_uri,
        record_count=archive.record_count,
    )
    ods_pipeline.reconciliation.write_check(
        pg_conn,
        check_type="api_pull_archive_count",
        run_id=api_pull_run_id,
        domain=DOMAIN,
        dataset=RISK_DATASET,
        business_date=business_date,
        source_count=archive.record_count,
        status="ok",
    )

    parent_run_id = derive_dag_ingest_parent_run_id(api_pull_run_id)
    ingest_run_id = str(uuid.uuid4())
    ods_pipeline.runs.start(
        pg_conn,
        run_id=parent_run_id,
        pipeline_type="s3_batch",
        domain=DOMAIN,
        dataset=RISK_DATASET,
        business_date=business_date,
        file_id=file_id,
        parents=[{
            "run_id": api_pull_run_id,
            "edge_type": TRIGGERED_BY_API_PULL_EDGE,
        }],
    )

    ingest_result = _run_glue_jsonl_ingest(
        run_id=ingest_run_id,
        file_id=file_id,
        s3_input_path=archive.s3_uri,
        parent_run_id=parent_run_id,
        dataset=RISK_DATASET,
    )
    assert ingest_result.returncode == 0, (
        "glue jsonl ingestion failed:\n"
        f"STDOUT:\n{ingest_result.stdout[-4000:]}\n"
        f"STDERR:\n{ingest_result.stderr[-4000:]}"
    )

    with pg_conn.cursor() as cur:
        cur.execute(
            "SELECT s3_curated_path FROM pipeline.file_catalogue WHERE file_id=%s",
            (file_id,),
        )
        curated_path = cur.fetchone()[0]
    assert curated_path

    publish_run_id = str(uuid.uuid4())
    publish_result = _run_glue_publish(
        run_id=publish_run_id,
        file_id=file_id,
        s3_input_path=curated_path,
        parent_run_id=parent_run_id,
        dataset=RISK_DATASET,
    )
    assert publish_result.returncode == 0, (
        "glue raw publish failed:\n"
        f"STDOUT:\n{publish_result.stdout[-4000:]}\n"
        f"STDERR:\n{publish_result.stderr[-4000:]}"
    )

    pg_conn.rollback()
    offset_ranges = _offset_ranges_for_run(pg_conn, publish_run_id)
    assert offset_ranges

    canonicalize_run_id = str(uuid.uuid4())
    canonicalize_result = _run_glue_canonicalize(
        run_id=canonicalize_run_id,
        file_id=file_id,
        parent_run_id=publish_run_id,
        offset_ranges=offset_ranges,
        business_date=business_date,
    )
    assert canonicalize_result.returncode == 0, (
        "glue canonicalize failed:\n"
        f"STDOUT:\n{canonicalize_result.stdout[-4000:]}\n"
        f"STDERR:\n{canonicalize_result.stderr[-4000:]}"
    )

    rows = _wait_api_pull_risk_rows(pg_conn, canonicalize_run_id)
    assert rows == [
        ("API-R1", "POL-API-1", "120.5", "2026-05-02", canonicalize_run_id),
        ("API-R2", "POL-API-2", "99", "2026-05-02", canonicalize_run_id),
    ]

    with pg_conn.cursor() as cur:
        cur.execute(
            """
            SELECT status, source_count, kafka_count, discrepancy_count
              FROM pipeline.reconciliation_log
             WHERE run_id=%s AND check_type='t1_canonicalize_count'
            """,
            (canonicalize_run_id,),
        )
        t1 = cur.fetchone()
    assert t1 == ("ok", 2, 2, 0)

    _run_t2_reconcile()
    pg_conn.rollback()
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            SELECT status, source_count, postgres_count, discrepancy_count
              FROM pipeline.reconciliation_log
             WHERE run_id=%s AND check_type='t2_append_file_count'
             ORDER BY created_at DESC LIMIT 1
            """,
            (canonicalize_run_id,),
        )
        t2 = cur.fetchone()
    assert t2 == ("passed", 2, 2, 0)

    ods_pipeline.runs.update(pg_conn, parent_run_id, status="succeeded")
    assert ingest_status_for_api_pull_run(
        pg_conn,
        api_pull_run_id,
        expected_parent_run_id=parent_run_id,
    ) == "succeeded"
