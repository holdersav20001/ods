from datetime import date, datetime, timezone

import pytest

from ods_pipeline import metadata


def test_file_metadata_contains_required_contract_fields():
    result = metadata.file_metadata(
        file_id="file-1",
        run_id="run-1",
        domain="insurance",
        dataset="policies",
        business_date=date(2026, 5, 1),
        source_application="sftp",
        ingested_at=datetime(2026, 5, 1, 10, 0, tzinfo=timezone.utc),
    )

    assert set(metadata.FILE_RECORD_FIELDS).issubset(result)
    assert result["_ods_business_date"] == "2026-05-01"
    assert result["_ods_ingested_at"] == "2026-05-01T10:00:00Z"


def test_message_metadata_requires_a_source_correlation_key():
    with pytest.raises(ValueError, match="source correlation key"):
        metadata.message_metadata(
            run_id="run-1",
            domain="insurance",
            dataset="risk_event",
            source_application="claims-api",
        )


def test_message_metadata_accepts_batch_correlation_key():
    result = metadata.message_metadata(
        run_id="run-1",
        domain="insurance",
        dataset="risk_event",
        source_application="claims-api",
        source_batch_id="batch-1",
    )

    assert result["_ods_source_batch_id"] == "batch-1"
    assert result["_ods_run_id"] == "run-1"


def test_canonical_metadata_preserves_file_identity_and_adds_canonical_run():
    source = metadata.file_metadata(
        file_id="file-1",
        run_id="raw-run",
        domain="insurance",
        dataset="risk",
        business_date="2026-05-01",
        source_application="sftp",
        ingested_at="2026-05-01T10:00:00Z",
    )

    result = metadata.canonical_metadata(
        source,
        canonicalize_run_id="canonical-run",
    )

    assert result["_ods_file_id"] == "file-1"
    assert result["_ods_raw_run_id"] == "raw-run"
    assert result["_ods_canonicalize_run_id"] == "canonical-run"


def test_canonical_metadata_preserves_message_identity():
    source = metadata.message_metadata(
        run_id="raw-run",
        domain="insurance",
        dataset="risk_event",
        source_application="claims-api",
        source_message_id="msg-1",
        ingested_at="2026-05-01T10:00:00Z",
    )

    result = metadata.canonical_metadata(
        source,
        canonicalize_run_id="canonical-run",
    )

    assert result["_ods_source_message_id"] == "msg-1"
    assert result["_ods_raw_run_id"] == "raw-run"
    assert result["_ods_canonicalize_run_id"] == "canonical-run"


def test_archive_envelope_contains_payload_and_schema_metadata():
    source = metadata.message_metadata(
        run_id="run-1",
        domain="insurance",
        dataset="claims_event",
        source_application="claims-api",
        source_message_id="msg-1",
        ingested_at="2026-05-01T10:00:00Z",
    )

    envelope = metadata.archive_envelope(
        payload={"claimId": "C1"},
        metadata=source,
        schema_id="ods.insurance.claim-event",
        schema_version=3,
        archive_s3_uri="s3://ods-event-archive/domain=insurance/file.jsonl",
    )

    assert envelope["_ods_source_message_id"] == "msg-1"
    assert envelope["_ods_schema_version"] == 3
    assert envelope["payload"] == {"claimId": "C1"}
