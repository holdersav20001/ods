"""DLQ helpers shared by file, canonicalize, and message/API flows."""
from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from ods_pipeline import metadata


def s3_prefix(
    *,
    env: str,
    domain: str,
    dataset: str,
    stage: str,
    run_id: str,
    business_date: str | None = None,
) -> str:
    """Return the standard S3 prefix for failed records/events."""
    date_part = business_date or "unknown"
    return (
        f"s3://ods-dlq-{env}/{domain}/{dataset}/{stage}/"
        f"date={date_part}/run_id={run_id}/"
    )


def envelope(
    *,
    payload: Mapping[str, Any],
    run_id: str,
    domain: str,
    dataset: str,
    source_application: str,
    error_type: str,
    error_message: str,
    stage: str,
    source_metadata: Mapping[str, Any] | None = None,
    source_message_id: str | None = None,
    source_event_id: str | None = None,
    source_request_id: str | None = None,
    source_batch_id: str | None = None,
    file_id: str | None = None,
    business_date: str | None = None,
    replayable: bool = True,
) -> dict[str, Any]:
    """Build a JSON/JSONL-friendly DLQ envelope."""
    source_metadata = dict(source_metadata or {})
    correlation = {
        "_ods_source_message_id": source_message_id or source_metadata.get("_ods_source_message_id"),
        "_ods_source_event_id": source_event_id or source_metadata.get("_ods_source_event_id"),
        "_ods_source_request_id": source_request_id or source_metadata.get("_ods_source_request_id"),
        "_ods_source_batch_id": source_batch_id or source_metadata.get("_ods_source_batch_id"),
    }
    result = {
        **{k: v for k, v in correlation.items() if v},
        "_ods_file_id": file_id or source_metadata.get("_ods_file_id"),
        "_ods_run_id": run_id,
        "_ods_domain": domain,
        "_ods_dataset": dataset,
        "_ods_business_date": business_date or source_metadata.get("_ods_business_date"),
        "_ods_source_application": source_application,
        "_ods_failed_stage": stage,
        "_ods_error_type": error_type,
        "_ods_error_message": error_message,
        "_ods_replayable": replayable,
        "payload": dict(payload),
    }
    if not result.get("_ods_file_id"):
        metadata.require_message_correlation(result, context="DLQ envelope")
    metadata.require_fields(
        result,
        (
            "_ods_run_id",
            "_ods_domain",
            "_ods_dataset",
            "_ods_source_application",
            "_ods_failed_stage",
            "_ods_error_type",
            "_ods_error_message",
            "payload",
        ),
        context="DLQ envelope",
    )
    return {key: value for key, value in result.items() if value is not None}


def replay_request(
    *,
    dlq_uri: str,
    run_id: str,
    domain: str,
    dataset: str,
    target_stage: str,
    reason: str,
) -> dict[str, Any]:
    """Return a small structured object operators can pass to replay tooling."""
    return {
        "dlq_uri": dlq_uri,
        "source_run_id": run_id,
        "domain": domain,
        "dataset": dataset,
        "target_stage": target_stage,
        "reason": reason,
    }
