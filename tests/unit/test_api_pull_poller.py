"""End-to-end unit tests for poll_and_archive with a fake HTTP session."""
from __future__ import annotations

import gzip
import io
import json
from unittest.mock import MagicMock

import pytest

from ods_pipeline.ingest.api_pull import poll_and_archive


class _FakeResponse:
    def __init__(self, body, headers=None, status_code=200):
        self._body = body
        self.headers = headers or {}
        self.status_code = status_code
        self.content = (
            json.dumps(body).encode("utf-8") if body is not None else b""
        )

    def json(self):
        return self._body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class _FakeSession:
    """Minimal Session stand-in that returns scripted responses by URL."""

    def __init__(self, script):
        self._script = list(script)
        self.calls: list[tuple[str, dict]] = []

    def get(self, url, params=None):
        self.calls.append((url, dict(params or {})))
        if not self._script:
            raise AssertionError(f"no scripted response for {url}?{params}")
        return self._script.pop(0)


def _decode(body: bytes) -> list[dict]:
    with gzip.GzipFile(fileobj=io.BytesIO(body), mode="rb") as gz:
        return [json.loads(line) for line in gz.read().splitlines() if line]


def _dataset_config():
    return {
        "domain": "insurance",
        "dataset": "api_pull_demo",
        "schema_id": "ods.insurance.api_pull_demo-value",
        "schema_version": 1,
        "source": {
            "application": "demo_api",
            "url": "https://api.example/items",
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
    }


def test_single_page_archive_with_cursor_advance():
    session = _FakeSession([
        _FakeResponse(
            [
                {"id": 1, "updated_at": "2026-04-02T00:00:00Z"},
                {"id": 2, "updated_at": "2026-04-03T00:00:00Z"},
            ],
        ),
    ])
    s3 = MagicMock()
    archive = poll_and_archive(
        dataset_config=_dataset_config(),
        s3_client=s3,
        archive_bucket="ods-raw-test",
        committed_cursor_value="2026-04-01T00:00:00Z",
        run_id="00000000-0000-0000-0000-000000000010",
        business_date="2026-05-02",
        session=session,
    )

    assert archive.record_count == 2
    assert archive.page_count == 1
    assert archive.old_cursor_value == "2026-04-01T00:00:00Z"
    assert archive.new_cursor_value == "2026-04-03T00:00:00Z"
    assert archive.no_changes is False

    assert session.calls == [
        ("https://api.example/items",
         {"updated_since": "2026-04-01T00:00:00Z"}),
    ]
    s3.put_object.assert_called_once()
    body = s3.put_object.call_args.kwargs["Body"]
    lines = _decode(body)
    assert {line["payload"]["id"] for line in lines} == {1, 2}


def test_link_header_paging_walks_all_pages():
    session = _FakeSession([
        _FakeResponse(
            [{"id": 1, "updated_at": "2026-04-02T00:00:00Z"}],
            headers={"Link": '<https://api.example/items?page=2>; rel="next"'},
        ),
        _FakeResponse(
            [{"id": 2, "updated_at": "2026-04-04T00:00:00Z"}],
            headers={"Link": '<https://api.example/items?page=3>; rel="next"'},
        ),
        _FakeResponse(
            [{"id": 3, "updated_at": "2026-04-03T00:00:00Z"}],
            headers={},
        ),
    ])
    s3 = MagicMock()
    archive = poll_and_archive(
        dataset_config=_dataset_config(),
        s3_client=s3,
        archive_bucket="ods-raw-test",
        committed_cursor_value="2026-04-01T00:00:00Z",
        run_id="00000000-0000-0000-0000-000000000020",
        business_date="2026-05-02",
        session=session,
    )

    assert archive.record_count == 3
    assert archive.page_count == 3
    assert archive.new_cursor_value == "2026-04-04T00:00:00Z"
    urls = [call[0] for call in session.calls]
    assert urls == [
        "https://api.example/items",
        "https://api.example/items?page=2",
        "https://api.example/items?page=3",
    ]


def test_empty_response_marks_no_changes_and_skips_s3():
    session = _FakeSession([_FakeResponse([])])
    s3 = MagicMock()
    archive = poll_and_archive(
        dataset_config=_dataset_config(),
        s3_client=s3,
        archive_bucket="ods-raw-test",
        committed_cursor_value="2026-04-01T00:00:00Z",
        run_id="00000000-0000-0000-0000-000000000030",
        business_date="2026-05-02",
        session=session,
    )
    assert archive.no_changes is True
    assert archive.record_count == 0
    assert archive.new_cursor_value is None
    s3.put_object.assert_not_called()


def test_envelope_response_with_items_key_unwraps():
    session = _FakeSession([
        _FakeResponse({"items": [{"id": 7, "updated_at": "2026-04-05T00:00:00Z"}]}),
    ])
    s3 = MagicMock()
    archive = poll_and_archive(
        dataset_config=_dataset_config(),
        s3_client=s3,
        archive_bucket="ods-raw-test",
        committed_cursor_value=None,
        run_id="00000000-0000-0000-0000-000000000040",
        business_date="2026-05-02",
        session=session,
    )
    assert archive.record_count == 1
    assert archive.new_cursor_value == "2026-04-05T00:00:00Z"


def test_http_error_propagates_and_skips_archive():
    session = _FakeSession([_FakeResponse(None, status_code=500)])
    s3 = MagicMock()
    with pytest.raises(RuntimeError, match="HTTP 500"):
        poll_and_archive(
            dataset_config=_dataset_config(),
            s3_client=s3,
            archive_bucket="ods-raw-test",
            committed_cursor_value=None,
            run_id="00000000-0000-0000-0000-000000000050",
            business_date="2026-05-02",
            session=session,
        )
    s3.put_object.assert_not_called()
