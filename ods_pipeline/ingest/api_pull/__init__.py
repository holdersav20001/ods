"""API pull poller — fetch external HTTP API on a schedule and archive
records as gzipped JSONL on S3.

Public surface kept small on purpose; dag_api_pull only needs:

    poll_and_archive(...)         the one-shot batch poller
    ArchivedBatch                 the typed return value
    AuthProvider, BearerAuth      auth Protocol + bearer impl
    Cursor, build_cursor          cursor Protocol + factory
    WatermarkStore                pending/committed cursor store

Each strategy lives in its own module so adding etag/offset/full_replace
later is one new file, not a refactor.
"""
from __future__ import annotations

from ods_pipeline.ingest.api_pull.archive import ArchivedBatch, write_jsonl_archive
from ods_pipeline.ingest.api_pull.auth import AuthProvider, BearerAuth, build_auth
from ods_pipeline.ingest.api_pull.cursors import Cursor, build_cursor
from ods_pipeline.ingest.api_pull.linkage import (
    TRIGGERED_BY_API_PULL_EDGE,
    ingest_status_for_api_pull_run,
)
from ods_pipeline.ingest.api_pull.poller import poll_and_archive
from ods_pipeline.ingest.api_pull.watermark import WatermarkStore

__all__ = [
    "ArchivedBatch",
    "AuthProvider",
    "BearerAuth",
    "Cursor",
    "TRIGGERED_BY_API_PULL_EDGE",
    "WatermarkStore",
    "build_auth",
    "build_cursor",
    "ingest_status_for_api_pull_run",
    "poll_and_archive",
    "write_jsonl_archive",
]
