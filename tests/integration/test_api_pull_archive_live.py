"""Live archive integration test: in-process FastAPI stub source -> real
LocalStack S3 -> the api_pull poller writes a gzipped JSONL archive.

This proves the bearer-auth path, the since_timestamp cursor, the
link-header paging branch and the S3 archive shape end-to-end without
running the full DAG.
"""
from __future__ import annotations

import gzip
import io
import json
import os
import threading
import time
import uuid

import boto3
import pytest
import requests
import uvicorn
from fastapi import FastAPI, Header, HTTPException, Query, Request, Response

from ods_pipeline.ingest.api_pull import poll_and_archive


LOCALSTACK_ENDPOINT = os.environ.get("LOCALSTACK_ENDPOINT", "http://localhost:4566")
TEST_BUCKET = "ods-raw-local"
TOKEN = "test-bearer-token"
TOKEN_ENV = "API_PULL_DEMO_TOKEN_TEST"


# ---------------------------------------------------------------------------
# In-process FastAPI stub source.
# ---------------------------------------------------------------------------


def _build_stub_app() -> FastAPI:
    app = FastAPI()

    @app.get("/items")
    def items(
        request: Request,
        updated_since: str = Query("2026-01-01T00:00:00Z"),
        page: int = Query(1),
        authorization: str = Header(default=""),
    ) -> Response:
        if authorization != f"Bearer {TOKEN}":
            raise HTTPException(status_code=401, detail="bad token")
        all_records = [
            {"id": 1, "request_id": "r1", "updated_at": "2026-04-02T00:00:00Z"},
            {"id": 2, "request_id": "r2", "updated_at": "2026-04-03T00:00:00Z"},
            {"id": 3, "request_id": "r3", "updated_at": "2026-04-04T00:00:00Z"},
        ]
        # Filter by since_timestamp.
        filtered = [r for r in all_records if r["updated_at"] > updated_since]
        # Two-page split: 2 records on page 1, 1 record on page 2.
        page_size = 2
        total_pages = max(1, -(-len(filtered) // page_size))
        start = (page - 1) * page_size
        chunk = filtered[start:start + page_size]
        headers = {}
        if page < total_pages:
            base = str(request.url).split("?")[0]
            headers["Link"] = (
                f'<{base}?updated_since={updated_since}&page={page + 1}>; rel="next"'
            )
        return Response(
            content=json.dumps(chunk),
            media_type="application/json",
            headers=headers,
        )

    return app


def _free_port() -> int:
    import socket
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


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


def _decode(s3_client, bucket: str, key: str) -> list[dict]:
    body = s3_client.get_object(Bucket=bucket, Key=key)["Body"].read()
    with gzip.GzipFile(fileobj=io.BytesIO(body), mode="rb") as gz:
        return [json.loads(line) for line in gz.read().splitlines() if line]


def test_live_archive_walks_pages_with_bearer(stub_url, s3_client):
    run_id = str(uuid.uuid4())
    business_date = "2026-05-02"

    archive = poll_and_archive(
        dataset_config={
            "domain": "insurance",
            "dataset": "api_pull_demo_test",
            "schema_id": "ods.insurance.api_pull_demo-value",
            "schema_version": 1,
            "source": {
                "application": "demo_api_test",
                "url": stub_url,
                "auth": {"type": "bearer", "secret_ref": TOKEN_ENV},
                "cursor": {
                    "style": "since_timestamp",
                    "request_param": "updated_since",
                    "response_field": "updated_at",
                    "initial": "2026-01-01T00:00:00Z",
                },
                "page": {"style": "link_header"},
                "timeout_seconds": 5,
                "retries": 0,
            },
        },
        s3_client=s3_client,
        archive_bucket=TEST_BUCKET,
        committed_cursor_value=None,
        run_id=run_id,
        business_date=business_date,
    )

    assert archive.no_changes is False
    assert archive.record_count == 3
    assert archive.page_count == 2
    assert archive.new_cursor_value == "2026-04-04T00:00:00Z"

    lines = _decode(s3_client, archive.s3_bucket, archive.s3_key)
    assert len(lines) == 3
    assert {ln["payload"]["request_id"] for ln in lines} == {"r1", "r2", "r3"}
    for line in lines:
        assert line["_ods_run_id"] == run_id
        assert line["_ods_source_application"] == "demo_api_test"
        assert line["_ods_business_date"] == business_date
        assert line["_ods_archive_s3_uri"] == archive.s3_uri


def test_live_archive_rejects_when_token_missing(stub_url, s3_client):
    os.environ.pop(TOKEN_ENV, None)
    with pytest.raises(ValueError, match="not set in environment"):
        poll_and_archive(
            dataset_config={
                "domain": "insurance",
                "dataset": "api_pull_demo_test",
                "schema_id": "ods.insurance.api_pull_demo-value",
                "source": {
                    "application": "demo_api_test",
                    "url": stub_url,
                    "auth": {"type": "bearer", "secret_ref": TOKEN_ENV},
                    "cursor": {"style": "since_timestamp",
                               "request_param": "updated_since",
                               "response_field": "updated_at",
                               "initial": "2026-01-01T00:00:00Z"},
                    "page": {"style": "link_header"},
                    "timeout_seconds": 5,
                    "retries": 0,
                },
            },
            s3_client=s3_client,
            archive_bucket=TEST_BUCKET,
            committed_cursor_value=None,
            run_id=str(uuid.uuid4()),
            business_date="2026-05-02",
        )


def test_live_archive_no_changes_when_cursor_at_max(stub_url, s3_client):
    archive = poll_and_archive(
        dataset_config={
            "domain": "insurance",
            "dataset": "api_pull_demo_test",
            "schema_id": "ods.insurance.api_pull_demo-value",
            "source": {
                "application": "demo_api_test",
                "url": stub_url,
                "auth": {"type": "bearer", "secret_ref": TOKEN_ENV},
                "cursor": {"style": "since_timestamp",
                           "request_param": "updated_since",
                           "response_field": "updated_at",
                           "initial": "2026-01-01T00:00:00Z"},
                "page": {"style": "link_header"},
                "timeout_seconds": 5,
                "retries": 0,
            },
        },
        s3_client=s3_client,
        archive_bucket=TEST_BUCKET,
        committed_cursor_value="2026-04-04T00:00:00Z",
        run_id=str(uuid.uuid4()),
        business_date="2026-05-02",
    )
    assert archive.no_changes is True
    assert archive.record_count == 0
    assert archive.new_cursor_value is None
