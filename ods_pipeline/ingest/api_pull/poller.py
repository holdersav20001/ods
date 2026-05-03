"""api_pull HTTP poller.

Single public entry-point: :func:`poll_and_archive`. Builds an HTTP
session with the configured auth, walks pages via the configured cursor
strategy, accumulates all records into one logical batch, and writes
that batch to S3 as one immutable gzipped JSONL archive. Watermark
read/record_pending lifecycle is owned by the caller (dag_api_pull) so
locking and the post-trigger promote/clear_pending hooks live in one
place.
"""
from __future__ import annotations

import uuid
from typing import Any, Mapping, Sequence

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from ods_pipeline.ingest.api_pull.archive import ArchivedBatch, write_jsonl_archive
from ods_pipeline.ingest.api_pull.auth import AuthProvider, build_auth
from ods_pipeline.ingest.api_pull.cursors import Cursor, CursorRequest, build_cursor


def _build_session(
    *,
    auth: AuthProvider,
    timeout_seconds: float,
    retries: int,
) -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=retries,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET", "HEAD"}),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers["Accept"] = "application/json"
    session.request = _with_default_timeout(session.request, timeout_seconds)  # type: ignore[assignment]
    auth.apply(session)
    return session


def _with_default_timeout(method, timeout_seconds: float):
    """Wrap Session.request so callers always get a default timeout."""

    def wrapper(*args, **kwargs):
        kwargs.setdefault("timeout", timeout_seconds)
        return method(*args, **kwargs)

    return wrapper


def _records_from_body(body: Any) -> Sequence[Mapping[str, Any]]:
    """Accept ``[{...}]`` or ``{"items": [...]}`` / ``{"data": [...]}``.

    Avoids hard-coding a wire shape; api_pull integrations vary. Anything
    else is treated as zero records — the caller decides how to surface it.
    """
    if isinstance(body, list):
        return body
    if isinstance(body, dict):
        for key in ("items", "data", "records", "results"):
            value = body.get(key)
            if isinstance(value, list):
                return value
    return []


def poll_and_archive(
    *,
    dataset_config: Mapping[str, Any],
    s3_client,
    archive_bucket: str,
    committed_cursor_value: str | None,
    run_id: str,
    business_date: str,
    session: requests.Session | None = None,
    cursor: Cursor | None = None,
    env: Mapping[str, str] | None = None,
) -> ArchivedBatch:
    """Run one HTTP poll for a dataset and archive results to S3.

    Wiring:
      - ``dataset_config`` carries domain, dataset, schema_id and the
        ``source`` block (URL, auth, cursor, page, retries, timeout).
      - ``committed_cursor_value`` is the watermark to issue this poll
        from. ``None`` falls back to the cursor's configured ``initial``.
      - ``s3_client``, ``session`` and ``cursor`` are injectable so tests
        can drive the function without real network or S3.

    Returns ArchivedBatch. ``no_changes=True`` means the source had
    nothing new — the DAG should skip dag_ingest and leave the watermark
    unchanged.
    """
    domain = str(dataset_config["domain"])
    dataset = str(dataset_config["dataset"])
    source = dict(dataset_config.get("source") or {})
    source_application = str(source.get("application", f"{domain}.{dataset}"))
    schema_id = str(dataset_config.get("schema_id", f"{domain}.{dataset}"))
    schema_version = dataset_config.get("schema_version", 1)
    timeout_seconds = float(source.get("timeout_seconds", 30))
    retries = int(source.get("retries", 3))

    if session is None:
        auth = build_auth(source.get("auth"), env=env)
        session = _build_session(
            auth=auth,
            timeout_seconds=timeout_seconds,
            retries=retries,
        )

    if cursor is None:
        cursor = build_cursor(source, committed_value=committed_cursor_value)

    source_request_id = str(uuid.uuid4())
    all_records: list[Mapping[str, Any]] = []
    request: CursorRequest | None = cursor.initial_request()
    page_count = 0

    while request is not None:
        response = session.get(request.url, params=request.params or None)
        page_count += 1
        if response.status_code == 304:
            # Etag/no-change short-circuit. Slice 1 doesn't issue
            # If-None-Match yet, but we still honour 304 if a source emits it.
            break
        response.raise_for_status()
        body = response.json() if response.content else None
        records = _records_from_body(body)
        all_records.extend(records)
        request = cursor.next_request(response.headers, body)

    new_cursor_value = cursor.advance(all_records)

    return write_jsonl_archive(
        s3_client=s3_client,
        bucket=archive_bucket,
        records=all_records,
        domain=domain,
        dataset=dataset,
        business_date=business_date,
        run_id=run_id,
        source_application=source_application,
        source_request_id=source_request_id,
        cursor_value=committed_cursor_value,
        schema_id=schema_id,
        schema_version=schema_version,
        page_count=page_count,
        old_cursor_value=committed_cursor_value,
        new_cursor_value=new_cursor_value,
    )
