"""End-to-end live test for the API pull pattern.

Drives the full pipeline up to (and including) Glue JSONL ingestion via
real components: a host FastAPI stub source, real LocalStack S3, real
Postgres, and the real Glue Spark image invoked through ``docker run``.

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
RAW_BUCKET = "ods-raw-local"
CURATED_BUCKET = "ods-curated-local"
NETWORK = "ods-network"

DOMAIN = "insurance"
DATASET = "api_pull_demo"
SOURCE_APPLICATION = "demo_api_e2e"
TOKEN_ENV = "API_PULL_E2E_TOKEN"
TOKEN = "e2e-bearer-token"
YAML_PATH = os.path.join(
    REPO_ROOT, "patterns", "insurance", "api_pull_demo.yaml",
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
    yaml_loader.sync_to_db(YAML_PATH, pg_conn)
    yield
    pg_conn.rollback()
    with pg_conn.cursor() as cur:
        cur.execute(
            "DELETE FROM pipeline.dataset_config "
            "WHERE domain=%s AND dataset=%s",
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
                "DELETE FROM pipeline.run_stage_log "
                "WHERE run_id IN (SELECT run_id FROM pipeline.run_log "
                "                  WHERE domain=%s AND dataset=%s)",
                (DOMAIN, DATASET),
            )
            cur.execute(
                "DELETE FROM pipeline.lineage_edge "
                "WHERE child_run_id IN (SELECT run_id FROM pipeline.run_log "
                "                        WHERE domain=%s AND dataset=%s)",
                (DOMAIN, DATASET),
            )
            cur.execute(
                "DELETE FROM pipeline.reconciliation_log "
                "WHERE domain=%s AND dataset=%s",
                (DOMAIN, DATASET),
            )
            cur.execute(
                "DELETE FROM pipeline.run_log "
                "WHERE domain=%s AND dataset=%s",
                (DOMAIN, DATASET),
            )
            cur.execute(
                "DELETE FROM pipeline.file_catalogue "
                "WHERE domain=%s AND dataset=%s",
                (DOMAIN, DATASET),
            )
            cur.execute(
                "DELETE FROM pipeline.file_state "
                "WHERE s3_path LIKE %s",
                (f"s3://{RAW_BUCKET}/api_pull/{DOMAIN}/{DATASET}/%",),
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
                          parent_run_id) -> subprocess.CompletedProcess:
    cmd = [
        "docker", "run", "--rm", "--network", NETWORK,
        *_glue_env_args(),
        "ods-glue:local", "spark-submit",
        "--py-files",
        "/home/glue_user/workspace/jobs/utils.py,"
        "/home/glue_user/workspace/jobs/dq.py",
        "/home/glue_user/workspace/jobs/ods_ingestion.py",
        "--run_id", run_id,
        "--domain", DOMAIN,
        "--dataset", DATASET,
        "--s3_input_path", s3_input_path,
        "--file_id", file_id,
        "--parent_run_id", parent_run_id,
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


# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------


def test_e2e_stub_to_curated_parquet_with_watermark_promotion(
    stub_url, s3_client, pg_conn, set_token,
    dataset_config_synced, control_plane_clean,
):
    """Full happy path through Glue JSONL ingestion + watermark promote."""
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

    # Lineage api_to_archive (written by us) AND raw_to_curated
    # (written by ods_ingestion) must both exist.
    with pg_conn.cursor() as cur:
        cur.execute(
            "SELECT edge_type FROM pipeline.lineage_edge "
            "WHERE parent_file_id=%s",
            (file_id,),
        )
        edges = {row[0] for row in cur.fetchall()}
    assert "api_to_archive" in edges, edges
    assert "raw_to_curated" in edges, edges

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
