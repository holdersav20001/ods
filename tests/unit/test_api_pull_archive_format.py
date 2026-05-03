"""Gzipped JSONL archive writer tests for the api_pull poller."""
from __future__ import annotations

import gzip
import io
import json
from unittest.mock import MagicMock

from ods_pipeline.ingest.api_pull.archive import write_jsonl_archive


def _decode(body: bytes) -> list[dict]:
    with gzip.GzipFile(fileobj=io.BytesIO(body), mode="rb") as gz:
        return [json.loads(line) for line in gz.read().splitlines() if line]


def _write(s3_client, records):
    return write_jsonl_archive(
        s3_client=s3_client,
        bucket="ods-raw-test",
        records=records,
        domain="insurance",
        dataset="api_pull_demo",
        business_date="2026-05-02",
        run_id="00000000-0000-0000-0000-000000000001",
        source_application="demo_api",
        source_request_id="00000000-0000-0000-0000-0000000000aa",
        cursor_value="2026-04-01T00:00:00Z",
        schema_id="ods.insurance.api_pull_demo-value",
        schema_version=1,
        page_count=2,
        old_cursor_value="2026-04-01T00:00:00Z",
        new_cursor_value="2026-05-01T00:00:00Z",
    )


def test_one_line_per_record_with_metadata_envelope():
    s3 = MagicMock()
    archive = _write(s3, records=[{"id": 1}, {"id": 2}, {"id": 3}])

    s3.put_object.assert_called_once()
    kwargs = s3.put_object.call_args.kwargs
    assert kwargs["Bucket"] == "ods-raw-test"
    assert kwargs["ContentEncoding"] == "gzip"
    assert kwargs["ContentType"] == "application/x-jsonlines"
    assert kwargs["Key"].startswith("api_pull/insurance/api_pull_demo/date=2026-05-02/")

    lines = _decode(kwargs["Body"])
    assert len(lines) == 3
    for i, line in enumerate(lines, start=1):
        assert line["payload"]["id"] == i
        assert line["_ods_run_id"] == "00000000-0000-0000-0000-000000000001"
        assert line["_ods_source_request_id"] == "00000000-0000-0000-0000-0000000000aa"
        assert line["_ods_source_application"] == "demo_api"
        assert line["_ods_domain"] == "insurance"
        assert line["_ods_dataset"] == "api_pull_demo"
        assert line["_ods_business_date"] == "2026-05-02"
        assert line["_ods_source_cursor"] == "2026-04-01T00:00:00Z"
        assert line["_ods_archive_s3_uri"] == archive.s3_uri

    assert archive.record_count == 3
    assert archive.no_changes is False
    assert archive.page_count == 2
    assert archive.new_cursor_value == "2026-05-01T00:00:00Z"
    assert archive.file_md5
    assert archive.file_size_bytes == len(kwargs["Body"])


def test_md5_is_stable_for_same_content():
    s3 = MagicMock()
    a = _write(s3, [{"id": 1}, {"id": 2}])
    s3 = MagicMock()
    b = _write(s3, [{"id": 1}, {"id": 2}])
    assert a.file_md5 == b.file_md5
    assert a.file_size_bytes == b.file_size_bytes


def test_empty_records_no_changes_skips_s3_put():
    s3 = MagicMock()
    archive = _write(s3, records=[])
    s3.put_object.assert_not_called()
    assert archive.no_changes is True
    assert archive.record_count == 0
    assert archive.file_md5 == ""
    assert archive.file_size_bytes == 0
    # s3_uri is still computed so dashboards can reference where it WOULD land
    assert archive.s3_uri.startswith("s3://ods-raw-test/api_pull/")
