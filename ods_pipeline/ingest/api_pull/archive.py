"""Gzipped JSONL archive writer for api_pull.

Each fetched record is wrapped in the standard ODS metadata envelope
(_ods_run_id, _ods_source_request_id, _ods_source_application, ...)
before being written to S3 as one line of gzipped JSONL. The downstream
Glue ingestion job reads JSONL via spark.read.json and applies the same
schema validation and DQ pipeline as the file pattern.

Identity: the archive is keyed by (domain, dataset, business_date,
run_id) — one immutable object per poll. Replay is safe because the
file_catalogue upsert is keyed on s3_raw_path.
"""
from __future__ import annotations

import gzip
import hashlib
import io
import json
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from ods_pipeline import metadata as _metadata


@dataclass
class ArchivedBatch:
    """Outcome of a single api_pull poll.

    ``no_changes=True`` means the source returned 0 records (or 304); the
    DAG should mark the run skipped, not trigger dag_ingest, and leave
    the watermark unchanged.
    """

    domain: str
    dataset: str
    business_date: str
    source_application: str
    s3_uri: str
    s3_bucket: str
    s3_key: str
    file_md5: str
    file_size_bytes: int
    record_count: int
    page_count: int
    old_cursor_value: str | None
    new_cursor_value: str | None
    source_request_id: str
    no_changes: bool = False


def _envelope(
    *,
    record: Mapping[str, Any],
    run_id: str,
    source_application: str,
    domain: str,
    dataset: str,
    business_date: str,
    source_request_id: str,
    cursor_value: str | None,
    archive_uri: str,
    schema_id: str,
    schema_version: int | str,
) -> dict[str, Any]:
    """One JSONL line: ODS metadata envelope wrapping the source record."""
    meta = _metadata.message_metadata(
        run_id=run_id,
        domain=domain,
        dataset=dataset,
        source_application=source_application,
        source_request_id=source_request_id,
    )
    envelope = _metadata.archive_envelope(
        payload=dict(record),
        metadata=meta,
        schema_id=schema_id,
        schema_version=schema_version,
        archive_s3_uri=archive_uri,
    )
    envelope["_ods_business_date"] = business_date
    envelope["_ods_source_cursor"] = cursor_value
    return envelope


def _gzip_jsonl(lines: Sequence[Mapping[str, Any]]) -> bytes:
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb", mtime=0) as gz:
        for line in lines:
            gz.write(json.dumps(line, default=str, sort_keys=True).encode("utf-8"))
            gz.write(b"\n")
    return buffer.getvalue()


def write_jsonl_archive(
    *,
    s3_client,
    bucket: str,
    records: Sequence[Mapping[str, Any]],
    domain: str,
    dataset: str,
    business_date: str,
    run_id: str,
    source_application: str,
    source_request_id: str,
    cursor_value: str | None,
    schema_id: str,
    schema_version: int | str = 1,
    page_count: int = 1,
    old_cursor_value: str | None = None,
    new_cursor_value: str | None = None,
) -> ArchivedBatch:
    """Wrap ``records`` in ODS envelopes, gzip as JSONL, and put to S3.

    Returns ArchivedBatch with file_md5, size and S3 location so the caller
    can register the archive in pipeline.file_catalogue and write lineage.
    """
    s3_key = (
        f"api_pull/{domain}/{dataset}/"
        f"date={business_date}/run_id={run_id}.jsonl.gz"
    )
    s3_uri = f"s3://{bucket}/{s3_key}"

    if not records:
        return ArchivedBatch(
            domain=domain,
            dataset=dataset,
            business_date=business_date,
            source_application=source_application,
            s3_uri=s3_uri,
            s3_bucket=bucket,
            s3_key=s3_key,
            file_md5="",
            file_size_bytes=0,
            record_count=0,
            page_count=page_count,
            old_cursor_value=old_cursor_value,
            new_cursor_value=new_cursor_value,
            source_request_id=source_request_id,
            no_changes=True,
        )

    enveloped = [
        _envelope(
            record=record,
            run_id=run_id,
            source_application=source_application,
            domain=domain,
            dataset=dataset,
            business_date=business_date,
            source_request_id=source_request_id,
            cursor_value=cursor_value,
            archive_uri=s3_uri,
            schema_id=schema_id,
            schema_version=schema_version,
        )
        for record in records
    ]
    body = _gzip_jsonl(enveloped)
    file_md5 = hashlib.md5(body).hexdigest()
    s3_client.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=body,
        ContentType="application/x-jsonlines",
        ContentEncoding="gzip",
    )
    return ArchivedBatch(
        domain=domain,
        dataset=dataset,
        business_date=business_date,
        source_application=source_application,
        s3_uri=s3_uri,
        s3_bucket=bucket,
        s3_key=s3_key,
        file_md5=file_md5,
        file_size_bytes=len(body),
        record_count=len(records),
        page_count=page_count,
        old_cursor_value=old_cursor_value,
        new_cursor_value=new_cursor_value,
        source_request_id=source_request_id,
        no_changes=False,
    )
