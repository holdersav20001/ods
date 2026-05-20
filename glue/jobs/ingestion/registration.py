"""File catalogue registration — head_object, MD5, upsert.

The S3 raw object's MD5 is the dedup key for ``pipeline.file_catalogue``.
We prefer the multipart-aware S3 ``ETag`` when it exists (it equals the
object MD5 for non-multipart uploads), and fall back to streaming the
body when the ETag is multipart-style (`<hex>-<n>`) or absent.

Returned tuple: ``(file_id, md5_hex, size_bytes)``. The caller writes
``file_id`` onto the run row and stage rows so lineage queries from the
viewer all resolve.
"""
from __future__ import annotations

import hashlib
import os
from typing import Any

import boto3

import ods_pipeline


def _s3_client() -> Any:
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("LOCALSTACK_ENDPOINT")
        or os.environ.get("S3_ENDPOINT"),
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
        region_name=os.environ.get("AWS_DEFAULT_REGION", "eu-west-1"),
    )


def _split_s3_uri(s3_uri: str) -> tuple[str, str]:
    bucket, key = s3_uri.replace("s3://", "").split("/", 1)
    return bucket, key


def head_md5(s3_uri: str, *, s3_client: Any | None = None) -> tuple[str, int]:
    """Return ``(md5_hex, size_bytes)`` for the S3 object at ``s3_uri``.

    Uses ETag when single-part (32 hex chars, no dash); streams + hashes
    otherwise. Injectable ``s3_client`` for tests.
    """
    client = s3_client or _s3_client()
    bucket, key = _split_s3_uri(s3_uri)
    head = client.head_object(Bucket=bucket, Key=key)
    etag = head.get("ETag", "").strip('"')
    size = head.get("ContentLength", 0)
    if etag and "-" not in etag and len(etag) == 32:
        return etag, size
    obj = client.get_object(Bucket=bucket, Key=key)
    md5 = hashlib.md5(obj["Body"].read()).hexdigest()
    return md5, size


def register(
    conn,
    *,
    run_id: str,
    domain: str,
    dataset: str,
    business_date: str,
    s3_input_path: str,
    file_id: str | None,
    s3_client: Any | None = None,
) -> tuple[str, str, int]:
    """Register the input file in ``pipeline.file_catalogue`` and return
    ``(file_id, md5_hex, size_bytes)``.

    If ``file_id`` is supplied (the DAG already registered the row),
    update its ``state`` to ``ingesting``. Otherwise upsert a fresh
    catalogue row keyed by MD5.
    """
    md5, size = head_md5(s3_input_path, s3_client=s3_client)

    if file_id:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE pipeline.file_catalogue
                   SET state='ingesting',
                       last_run_id=%s,
                       state_updated_at=NOW()
                 WHERE file_id=%s
                """,
                (run_id, file_id),
            )
        conn.commit()
        return file_id, md5, size

    new_file_id = ods_pipeline.files.upsert(
        conn,
        domain=domain,
        dataset=dataset,
        business_date=business_date,
        file_md5=md5,
        s3_raw_path=s3_input_path,
        file_size_bytes=size,
        state="ingesting",
        last_run_id=run_id,
    )
    return new_file_id, md5, size


def already_completed(conn, s3_input_path: str) -> bool:
    """Idempotency short-circuit: was this S3 object already curated?"""
    return ods_pipeline.files.get_state(conn, s3_input_path) == "completed"


def mark_curated(conn, *, file_id: str, curated_uri: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE pipeline.file_catalogue
               SET s3_curated_path=%s,
                   state='curated',
                   state_updated_at=NOW()
             WHERE file_id=%s
            """,
            (curated_uri, file_id),
        )
    conn.commit()


def mark_failed(
    conn,
    *,
    s3_input_path: str,
    run_id: str,
    reason: str,
    source_row_count: int | None = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE pipeline.file_catalogue
               SET state='failed',
                   source_row_count=COALESCE(%s, source_row_count),
                   last_run_id=%s,
                   state_updated_at=NOW()
             WHERE s3_raw_path=%s
            """,
            (source_row_count, run_id, s3_input_path),
        )
    conn.commit()
    ods_pipeline.files.set_state(
        conn, s3_input_path, run_id, "failed", error_reason=reason,
    )


def mark_completed(
    conn,
    *,
    s3_input_path: str,
    run_id: str,
    record_count: int,
    source_row_count: int | None = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE pipeline.file_catalogue
               SET source_row_count=COALESCE(%s, source_row_count),
                   last_run_id=%s,
                   state_updated_at=NOW()
             WHERE s3_raw_path=%s
            """,
            (source_row_count, run_id, s3_input_path),
        )
    conn.commit()
    ods_pipeline.files.set_state(
        conn, s3_input_path, run_id, "completed", record_count=record_count,
    )
