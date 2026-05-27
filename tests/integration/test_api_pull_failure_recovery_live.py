"""Failure / recovery integration tests for the api_pull control-plane.

Each scenario uses a real local FastAPI source plus the real Postgres
api_pull_watermark table so we exercise the SQL contracts, not just
Python-level mocks.

Scenarios covered (matches docs/api-pull-backlog.md item 3 + design
"Failure And Recovery"):

  * Source 5xx after retries -> no archive, no pending cursor, run fails.
  * Source returns empty page -> no_changes, no pending cursor.
  * Archive ok + downstream dag_ingest fails -> committed cursor unchanged
    after clear_pending; pending re-issued on next poll.
  * Archive ok + downstream dag_ingest succeeds -> promote moves
    pending -> committed and records last_successful_run_id.
"""
from __future__ import annotations

import json
import os
import socket
import threading
import time
import uuid
from unittest.mock import MagicMock

import boto3
import pytest
import requests
import uvicorn
from fastapi import FastAPI, Header, HTTPException, Query, Response

from ods_pipeline.ingest.api_pull import (
    WatermarkStore,
    ingest_status_for_api_pull_run,
    poll_and_archive,
)


LOCALSTACK_ENDPOINT = os.environ.get("LOCALSTACK_ENDPOINT", "http://localhost:4566")
TEST_BUCKET = "ods-raw-local"
TOKEN = "failure-recovery-token"
TOKEN_ENV = "API_PULL_FAILURE_RECOVERY_TOKEN"

DOMAIN = "insurance"
DATASET = "api_pull_failure_recovery_test"
SOURCE_APPLICATION = "demo_api_failure_recovery"


# ---------------------------------------------------------------------------
# Stub source — toggleable behaviour per request via query param
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
        mode: str = Query("ok"),
        updated_since: str = Query("2026-01-01T00:00:00Z"),
        authorization: str = Header(default=""),
    ) -> Response:
        if authorization != f"Bearer {TOKEN}":
            raise HTTPException(status_code=401, detail="bad token")
        if mode == "fail":
            raise HTTPException(status_code=500, detail="boom")
        if mode == "empty":
            return Response(content="[]", media_type="application/json")
        records = [
            {"id": 1, "request_id": "r1", "updated_at": "2026-04-02T00:00:00Z"},
            {"id": 2, "request_id": "r2", "updated_at": "2026-04-03T00:00:00Z"},
        ]
        return Response(
            content=json.dumps(records),
            media_type="application/json",
        )

    return app


@pytest.fixture(scope="module")
def stub_url():
    app = _build_stub_app()
    port = _free_port()
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
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


@pytest.fixture(scope="module")
def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=LOCALSTACK_ENDPOINT,
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
        region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
    )


@pytest.fixture(scope="module", autouse=True)
def ensure_bucket(s3_client):
    try:
        s3_client.head_bucket(Bucket=TEST_BUCKET)
    except Exception:
        s3_client.create_bucket(Bucket=TEST_BUCKET)


@pytest.fixture(autouse=True)
def set_token():
    os.environ[TOKEN_ENV] = TOKEN
    yield
    os.environ.pop(TOKEN_ENV, None)


def _wipe(pg_conn):
    """Delete all api_pull_watermark / run_log / file_catalogue rows for
    this test's domain+dataset. Must clear file_catalogue last to satisfy
    the run_log → file_catalogue FK; clears it on either side of the test
    so prior leakage cannot pollute new test state."""
    pg_conn.rollback()
    with pg_conn.cursor() as cur:
        cur.execute(
            "DELETE FROM pipeline.api_pull_watermark "
            "WHERE domain=%s AND dataset=%s AND source_application=%s",
            (DOMAIN, DATASET, SOURCE_APPLICATION),
        )
        cur.execute(
            "DELETE FROM pipeline.run_log WHERE domain=%s AND dataset=%s",
            (DOMAIN, DATASET),
        )
        cur.execute(
            "DELETE FROM pipeline.file_catalogue WHERE domain=%s AND dataset=%s",
            (DOMAIN, DATASET),
        )
    pg_conn.commit()


@pytest.fixture
def watermark_clean(pg_conn):
    _wipe(pg_conn)
    yield
    _wipe(pg_conn)


def _insert_file_catalogue(conn, *, file_id, run_id):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline.file_catalogue
                (file_id, domain, dataset, business_date, file_md5,
                 s3_raw_path, state, last_run_id)
            VALUES (%s,%s,%s,%s,%s,%s,'received',%s)
            ON CONFLICT DO NOTHING
            """,
            (file_id, DOMAIN, DATASET, "2026-05-02",
             "0" * 32, f"s3://test/{file_id}.jsonl.gz", run_id),
        )
    conn.commit()


def _dataset_config(stub_url: str, mode: str = "ok") -> dict:
    return {
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
            "timeout_seconds": 5,
            "retries": 0,
        },
    }


def _insert_run_log(conn, *, run_id, file_id, orchestrators, status):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline.run_log
                (run_id, pipeline_type, domain, dataset, business_date,
                 file_id, status, orchestrators)
            VALUES (%s, 's3_batch', %s, %s, %s, %s, %s, %s::jsonb)
            ON CONFLICT (run_id) DO NOTHING
            """,
            (run_id, DOMAIN, DATASET, "2026-05-02",
             file_id, status, json.dumps(orchestrators)),
        )
    conn.commit()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_source_5xx_no_archive_no_pending(stub_url, s3_client, pg_conn,
                                          watermark_clean):
    """API 5xx after retries -> poll raises, no S3 put, no pending cursor."""
    store = WatermarkStore(pg_conn)
    store.read(domain=DOMAIN, dataset=DATASET,
               source_application=SOURCE_APPLICATION,
               cursor_type="since_timestamp")

    cfg = _dataset_config(stub_url)
    cfg["source"]["url"] = f"{stub_url}?mode=fail"
    s3_spy = MagicMock(wraps=s3_client)
    api_pull_run_id = str(uuid.uuid4())

    with pytest.raises(Exception):
        poll_and_archive(
            dataset_config=cfg,
            s3_client=s3_spy,
            archive_bucket=TEST_BUCKET,
            committed_cursor_value=None,
            run_id=api_pull_run_id,
            business_date="2026-05-02",
        )

    s3_spy.put_object.assert_not_called()

    row = store.read(domain=DOMAIN, dataset=DATASET,
                     source_application=SOURCE_APPLICATION,
                     cursor_type="since_timestamp")
    assert row.pending_cursor_value is None
    assert row.committed_cursor_value is None


def test_empty_response_no_archive_no_pending(stub_url, s3_client, pg_conn,
                                              watermark_clean):
    """Empty page -> no_changes, no S3 put, no pending cursor."""
    store = WatermarkStore(pg_conn)
    store.read(domain=DOMAIN, dataset=DATASET,
               source_application=SOURCE_APPLICATION,
               cursor_type="since_timestamp")

    cfg = _dataset_config(stub_url)
    cfg["source"]["url"] = f"{stub_url}?mode=empty"
    s3_spy = MagicMock(wraps=s3_client)

    archive = poll_and_archive(
        dataset_config=cfg,
        s3_client=s3_spy,
        archive_bucket=TEST_BUCKET,
        committed_cursor_value="2026-04-01T00:00:00Z",
        run_id=str(uuid.uuid4()),
        business_date="2026-05-02",
    )

    assert archive.no_changes is True
    assert archive.record_count == 0
    assert archive.new_cursor_value is None
    s3_spy.put_object.assert_not_called()

    row = store.read(domain=DOMAIN, dataset=DATASET,
                     source_application=SOURCE_APPLICATION,
                     cursor_type="since_timestamp")
    assert row.pending_cursor_value is None
    # committed left whatever it was (None — first poll).
    assert row.committed_cursor_value is None


def test_archive_ok_downstream_failed_clears_pending(stub_url, s3_client,
                                                    pg_conn, watermark_clean):
    """Archive succeeds, downstream dag_ingest reports 'failed' -> sensor
    calls clear_pending; committed cursor must NOT advance.
    """
    store = WatermarkStore(pg_conn)
    store.read(domain=DOMAIN, dataset=DATASET,
               source_application=SOURCE_APPLICATION,
               cursor_type="since_timestamp")

    cfg = _dataset_config(stub_url)
    api_pull_run_id = str(uuid.uuid4())
    file_id = str(uuid.uuid4())
    _insert_file_catalogue(pg_conn, file_id=file_id, run_id=api_pull_run_id)

    archive = poll_and_archive(
        dataset_config=cfg,
        s3_client=s3_client,
        archive_bucket=TEST_BUCKET,
        committed_cursor_value=None,
        run_id=api_pull_run_id,
        business_date="2026-05-02",
    )
    assert archive.record_count == 2

    store.try_lock(domain=DOMAIN, dataset=DATASET,
                   source_application=SOURCE_APPLICATION,
                   run_id=api_pull_run_id)
    store.record_pending(domain=DOMAIN, dataset=DATASET,
                         source_application=SOURCE_APPLICATION,
                         run_id=api_pull_run_id,
                         new_cursor_value=archive.new_cursor_value)
    store.unlock(domain=DOMAIN, dataset=DATASET,
                 source_application=SOURCE_APPLICATION)

    # Simulate dag_ingest reporting 'failed' for THIS api_pull run.
    _insert_run_log(
        pg_conn,
        run_id=str(uuid.uuid4()),
        file_id=file_id,
        orchestrators=[{"run_id": api_pull_run_id, "edge_type": "triggered_by_api_pull"}],
        status="failed",
    )

    assert ingest_status_for_api_pull_run(pg_conn, api_pull_run_id) == "failed"

    # Sensor's recovery action.
    store.clear_pending(domain=DOMAIN, dataset=DATASET,
                        source_application=SOURCE_APPLICATION,
                        run_id=api_pull_run_id)

    row = store.read(domain=DOMAIN, dataset=DATASET,
                     source_application=SOURCE_APPLICATION,
                     cursor_type="since_timestamp")
    assert row.pending_cursor_value is None
    assert row.committed_cursor_value is None
    assert row.last_successful_run_id is None


def test_archive_ok_downstream_succeeded_promotes(stub_url, s3_client,
                                                  pg_conn, watermark_clean):
    """Archive succeeds, downstream dag_ingest 'succeeded' under THIS
    api_pull run -> promote moves pending -> committed.
    """
    store = WatermarkStore(pg_conn)
    store.read(domain=DOMAIN, dataset=DATASET,
               source_application=SOURCE_APPLICATION,
               cursor_type="since_timestamp")

    cfg = _dataset_config(stub_url)
    api_pull_run_id = str(uuid.uuid4())
    file_id = str(uuid.uuid4())
    _insert_file_catalogue(pg_conn, file_id=file_id, run_id=api_pull_run_id)

    archive = poll_and_archive(
        dataset_config=cfg,
        s3_client=s3_client,
        archive_bucket=TEST_BUCKET,
        committed_cursor_value=None,
        run_id=api_pull_run_id,
        business_date="2026-05-02",
    )
    assert archive.record_count == 2

    store.try_lock(domain=DOMAIN, dataset=DATASET,
                   source_application=SOURCE_APPLICATION,
                   run_id=api_pull_run_id)
    store.record_pending(domain=DOMAIN, dataset=DATASET,
                         source_application=SOURCE_APPLICATION,
                         run_id=api_pull_run_id,
                         new_cursor_value=archive.new_cursor_value)
    store.unlock(domain=DOMAIN, dataset=DATASET,
                 source_application=SOURCE_APPLICATION)

    _insert_run_log(
        pg_conn,
        run_id=str(uuid.uuid4()),
        file_id=file_id,
        orchestrators=[{"run_id": api_pull_run_id, "edge_type": "triggered_by_api_pull"}],
        status="succeeded",
    )

    assert ingest_status_for_api_pull_run(pg_conn, api_pull_run_id) == "succeeded"

    promoted = store.promote(domain=DOMAIN, dataset=DATASET,
                             source_application=SOURCE_APPLICATION,
                             run_id=api_pull_run_id)
    assert promoted is True

    row = store.read(domain=DOMAIN, dataset=DATASET,
                     source_application=SOURCE_APPLICATION,
                     cursor_type="since_timestamp")
    assert row.committed_cursor_value == archive.new_cursor_value
    assert row.pending_cursor_value is None
    assert row.last_successful_run_id == api_pull_run_id
