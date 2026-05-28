import json

import pytest

from ods_pipeline import messages


def test_start_run_requires_message_correlation():
    with pytest.raises(ValueError, match="source correlation key"):
        messages.start_run(
            object(),
            run_id="run-1",
            domain="insurance",
            dataset="claims_event",
            source_application="claims-api",
            correlation={},
        )


def test_start_run_writes_run_and_receive_stage(monkeypatch):
    calls = []

    monkeypatch.setattr(messages.runs, "start", lambda *args, **kwargs: calls.append(("run", kwargs)))
    monkeypatch.setattr(messages.stages, "start", lambda *args, **kwargs: calls.append(("stage", kwargs)))

    meta = messages.start_run(
        object(),
        run_id="run-1",
        domain="insurance",
        dataset="claims_event",
        source_application="claims-api",
        correlation={"source_batch_id": "batch-1"},
        expected_count=3,
        kafka_topic="ods.insurance.claims-event",
    )

    assert meta == {"_ods_source_batch_id": "batch-1"}
    assert calls[0][0] == "run"
    assert calls[0][1]["pipeline_type"] == "message_api"
    assert calls[1][0] == "stage"
    assert calls[1][1]["stage"] == "message_receive"
    assert calls[1][1]["record_count_in"] == 3


def test_record_result_accounts_for_validation_dlq_and_archive(monkeypatch):
    calls = []

    monkeypatch.setattr(messages.stages, "finish", lambda *args, **kwargs: calls.append(("finish", kwargs)))
    monkeypatch.setattr(messages.stages, "write", lambda *args, **kwargs: calls.append(("stage", kwargs)))
    monkeypatch.setattr(messages.reconciliation, "write_check", lambda *args, **kwargs: calls.append(("recon", kwargs)))
    monkeypatch.setattr(messages.runs, "update", lambda *args, **kwargs: calls.append(("run_update", args, kwargs)))

    status = messages.record_result(
        object(),
        run_id="run-1",
        domain="insurance",
        dataset="claims_event",
        business_date=None,
        source_count=10,
        validation_fail_count=2,
        dlq_count=1,
        published_count=7,
        archive_count=10,
        kafka_topic="ods.insurance.claims-canonical",
        dlq_ref="s3://ods-dlq-local/insurance/claims_event/",
    )

    assert status == "succeeded"
    recon = [call for call in calls if call[0] == "recon"][0][1]
    assert recon["check_type"] == "message_batch_count"
    assert recon["source_count"] == 7
    assert recon["accounted_count"] == 7
    detail = json.loads(recon["detail"])
    assert detail["validation_fail_count"] == 2
    assert detail["dlq_count"] == 1
    assert detail["archive_count"] == 10


def test_record_result_fails_when_archive_count_does_not_match(monkeypatch):
    monkeypatch.setattr(messages.stages, "finish", lambda *args, **kwargs: None)
    monkeypatch.setattr(messages.stages, "write", lambda *args, **kwargs: None)
    monkeypatch.setattr(messages.reconciliation, "write_check", lambda *args, **kwargs: None)
    updates = []
    monkeypatch.setattr(messages.runs, "update", lambda *args, **kwargs: updates.append(kwargs))

    status = messages.record_result(
        object(),
        run_id="run-1",
        domain="insurance",
        dataset="claims_event",
        source_count=10,
        published_count=10,
        archive_count=9,
    )

    assert status == "failed"
    assert updates[0]["status"] == "failed"
