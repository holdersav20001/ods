"""ODS metadata contracts for file, message, canonical, and archive records."""
from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Mapping


FILE_RECORD_FIELDS: tuple[str, ...] = (
    "_ods_file_id",
    "_ods_run_id",
    "_ods_domain",
    "_ods_dataset",
    "_ods_business_date",
    "_ods_source_application",
    "_ods_ingested_at",
)

MESSAGE_CORRELATION_FIELDS: tuple[str, ...] = (
    "_ods_source_message_id",
    "_ods_source_event_id",
    "_ods_source_request_id",
    "_ods_source_batch_id",
)

MESSAGE_RECORD_FIELDS: tuple[str, ...] = (
    *MESSAGE_CORRELATION_FIELDS,
    "_ods_run_id",
    "_ods_domain",
    "_ods_dataset",
    "_ods_source_application",
    "_ods_ingested_at",
)

CANONICAL_FILE_RECORD_FIELDS: tuple[str, ...] = (
    "_ods_file_id",
    "_ods_raw_run_id",
    "_ods_canonicalize_run_id",
    "_ods_domain",
    "_ods_dataset",
    "_ods_business_date",
    "_ods_source_application",
    "_ods_ingested_at",
)

CANONICAL_MESSAGE_RECORD_FIELDS: tuple[str, ...] = (
    *MESSAGE_CORRELATION_FIELDS,
    "_ods_raw_run_id",
    "_ods_canonicalize_run_id",
    "_ods_domain",
    "_ods_dataset",
    "_ods_source_application",
    "_ods_ingested_at",
)

HISTORY_TABLE_FIELDS: tuple[str, ...] = (
    "_ods_run_id",
    "_ods_business_date",
    "_ods_ingested_at",
)

FILE_HISTORY_TABLE_FIELDS: tuple[str, ...] = (
    *HISTORY_TABLE_FIELDS,
    "_ods_file_id",
)

MESSAGE_HISTORY_TABLE_FIELDS: tuple[str, ...] = (
    *HISTORY_TABLE_FIELDS,
    *MESSAGE_CORRELATION_FIELDS,
)

ARCHIVE_ENVELOPE_FIELDS: tuple[str, ...] = (
    *MESSAGE_CORRELATION_FIELDS,
    "_ods_source_application",
    "_ods_domain",
    "_ods_dataset",
    "_ods_ingested_at",
    "_ods_schema_id",
    "_ods_schema_version",
    "_ods_run_id",
    "payload",
)


def utc_now_iso() -> str:
    """Return an ISO-8601 UTC timestamp for ODS metadata fields."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def normalise_value(value: Any) -> Any:
    """Normalise common Python values into JSON/Avro friendly metadata values."""
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    return value


def missing_fields(record: Mapping[str, Any], required_fields: tuple[str, ...]) -> list[str]:
    """Return required fields that are absent, ``None``, or blank strings."""
    missing: list[str] = []
    for field in required_fields:
        value = record.get(field)
        if value is None or value == "":
            missing.append(field)
    return missing


def require_fields(
    record: Mapping[str, Any],
    required_fields: tuple[str, ...],
    *,
    context: str = "record",
) -> None:
    """Raise ``ValueError`` if *record* misses any required ODS metadata fields."""
    missing = missing_fields(record, required_fields)
    if missing:
        raise ValueError(f"{context} missing required ODS metadata fields: {missing}")


def require_message_correlation(record: Mapping[str, Any], *, context: str = "message") -> None:
    """Require at least one stable source correlation key for message/API records."""
    if not any(record.get(field) for field in MESSAGE_CORRELATION_FIELDS):
        raise ValueError(
            f"{context} must include at least one source correlation key: "
            f"{list(MESSAGE_CORRELATION_FIELDS)}"
        )


def file_metadata(
    *,
    file_id: str,
    run_id: str,
    domain: str,
    dataset: str,
    business_date: str | date,
    source_application: str,
    ingested_at: str | datetime | None = None,
) -> dict[str, Any]:
    """Build the standard metadata block for file based records."""
    metadata = {
        "_ods_file_id": file_id,
        "_ods_run_id": run_id,
        "_ods_domain": domain,
        "_ods_dataset": dataset,
        "_ods_business_date": normalise_value(business_date),
        "_ods_source_application": source_application,
        "_ods_ingested_at": normalise_value(ingested_at) if ingested_at else utc_now_iso(),
    }
    require_fields(metadata, FILE_RECORD_FIELDS, context="file metadata")
    return metadata


def message_metadata(
    *,
    run_id: str,
    domain: str,
    dataset: str,
    source_application: str,
    source_message_id: str | None = None,
    source_event_id: str | None = None,
    source_request_id: str | None = None,
    source_batch_id: str | None = None,
    ingested_at: str | datetime | None = None,
) -> dict[str, Any]:
    """Build the standard metadata block for message/API records."""
    metadata = {
        "_ods_source_message_id": source_message_id,
        "_ods_source_event_id": source_event_id,
        "_ods_source_request_id": source_request_id,
        "_ods_source_batch_id": source_batch_id,
        "_ods_run_id": run_id,
        "_ods_domain": domain,
        "_ods_dataset": dataset,
        "_ods_source_application": source_application,
        "_ods_ingested_at": normalise_value(ingested_at) if ingested_at else utc_now_iso(),
    }
    require_fields(
        metadata,
        ("_ods_run_id", "_ods_domain", "_ods_dataset", "_ods_source_application", "_ods_ingested_at"),
        context="message metadata",
    )
    require_message_correlation(metadata)
    return metadata


def canonical_metadata(
    source_metadata: Mapping[str, Any],
    *,
    canonicalize_run_id: str,
    raw_run_id: str | None = None,
) -> dict[str, Any]:
    """Preserve source correlation metadata and add canonicalization run metadata."""
    metadata = {
        key: source_metadata.get(key)
        for key in (
            "_ods_file_id",
            *MESSAGE_CORRELATION_FIELDS,
            "_ods_domain",
            "_ods_dataset",
            "_ods_business_date",
            "_ods_source_application",
            "_ods_ingested_at",
        )
        if source_metadata.get(key) is not None
    }
    metadata["_ods_raw_run_id"] = raw_run_id or source_metadata.get("_ods_run_id")
    metadata["_ods_canonicalize_run_id"] = canonicalize_run_id
    if metadata.get("_ods_file_id"):
        require_fields(metadata, CANONICAL_FILE_RECORD_FIELDS, context="canonical file metadata")
    else:
        require_message_correlation(metadata, context="canonical message metadata")
        require_fields(
            metadata,
            (
                "_ods_raw_run_id",
                "_ods_canonicalize_run_id",
                "_ods_domain",
                "_ods_dataset",
                "_ods_source_application",
                "_ods_ingested_at",
            ),
            context="canonical message metadata",
        )
    return metadata


def archive_envelope(
    *,
    payload: Mapping[str, Any],
    metadata: Mapping[str, Any],
    schema_id: str,
    schema_version: int | str,
    archive_s3_uri: str | None = None,
) -> dict[str, Any]:
    """Build a JSONL-friendly S3 archive envelope for complex events."""
    envelope = {
        key: metadata.get(key)
        for key in (
            *MESSAGE_CORRELATION_FIELDS,
            "_ods_source_application",
            "_ods_domain",
            "_ods_dataset",
            "_ods_ingested_at",
            "_ods_run_id",
        )
        if metadata.get(key) is not None
    }
    envelope["_ods_schema_id"] = schema_id
    envelope["_ods_schema_version"] = schema_version
    if archive_s3_uri is not None:
        envelope["_ods_archive_s3_uri"] = archive_s3_uri
    envelope["payload"] = dict(payload)
    require_message_correlation(envelope, context="archive envelope")
    require_fields(
        envelope,
        (
            "_ods_source_application",
            "_ods_domain",
            "_ods_dataset",
            "_ods_ingested_at",
            "_ods_schema_id",
            "_ods_schema_version",
            "_ods_run_id",
            "payload",
        ),
        context="archive envelope",
    )
    return envelope
