import pytest

from ods_pipeline import dlq


def test_dlq_prefix_uses_standard_partition_layout():
    assert dlq.s3_prefix(
        env="local",
        domain="insurance",
        dataset="risk",
        stage="canonicalize",
        business_date="2026-05-01",
        run_id="run-1",
    ) == "s3://ods-dlq-local/insurance/risk/canonicalize/date=2026-05-01/run_id=run-1/"


def test_file_dlq_envelope_does_not_need_message_correlation():
    result = dlq.envelope(
        payload={"risk_id": "R1"},
        run_id="run-1",
        domain="insurance",
        dataset="risk",
        source_application="risk-app",
        file_id="file-1",
        error_type="validation",
        error_message="risk_id missing",
        stage="canonical_transform",
    )

    assert result["_ods_file_id"] == "file-1"
    assert result["_ods_replayable"] is True
    assert result["payload"] == {"risk_id": "R1"}


def test_message_dlq_envelope_requires_correlation():
    with pytest.raises(ValueError, match="source correlation key"):
        dlq.envelope(
            payload={"claim_id": "C1"},
            run_id="run-1",
            domain="insurance",
            dataset="claims_event",
            source_application="claims-api",
            error_type="validation",
            error_message="claim_id invalid",
            stage="message_validate",
        )


def test_replay_request_is_structured():
    result = dlq.replay_request(
        dlq_uri="s3://ods-dlq-local/insurance/risk/",
        run_id="run-1",
        domain="insurance",
        dataset="risk",
        target_stage="canonicalize",
        reason="mapping fixed",
    )

    assert result["source_run_id"] == "run-1"
    assert result["target_stage"] == "canonicalize"
