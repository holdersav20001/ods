"""DLQ envelope, key layout, retry, and replay-request contract tests.

These pin the operator-visible contract of ``ods_pipeline.dlq``:

  - envelope shape (correlation fields, error metadata, replayable flag);
  - S3 prefix layout (so replay tooling can list partitions);
  - DlqWriter put_object retry + final-failure semantics;
  - replay_request dict shape (what the replay CLI consumes).

Mocks the S3 client + sleep so retries don't slow the suite. No
network or Postgres.
"""
from __future__ import annotations

import time
from unittest.mock import MagicMock

import pytest

from ods_pipeline.dlq import (
    DlqWriter,
    envelope,
    replay_request,
    s3_prefix,
)


# ---------------------------------------------------------------------------
# s3_prefix
# ---------------------------------------------------------------------------


def test_s3_prefix_includes_every_partition_axis():
    prefix = s3_prefix(
        env="local",
        domain="insurance",
        dataset="policies",
        stage="schema_validate",
        run_id="run-1",
        business_date="2026-05-06",
    )
    assert prefix == (
        "s3://ods-dlq-local/insurance/policies/schema_validate/"
        "date=2026-05-06/run_id=run-1/"
    )


def test_s3_prefix_falls_back_to_unknown_when_no_business_date():
    prefix = s3_prefix(
        env="local", domain="insurance", dataset="x",
        stage="dq_check", run_id="r-2",
    )
    assert "/date=unknown/" in prefix


# ---------------------------------------------------------------------------
# envelope — shape + required fields
# ---------------------------------------------------------------------------


def test_envelope_carries_correlation_and_error_metadata_for_file_pattern():
    env = envelope(
        payload={"policy_id": "POL-1", "premium": "BAD"},
        run_id="run-1",
        domain="insurance",
        dataset="policies",
        source_application="sftp",
        error_type="DQValidationError",
        error_message="premium not numeric",
        stage="dq_check",
        file_id="file-1",
        business_date="2026-05-06",
    )
    assert env["_ods_run_id"] == "run-1"
    assert env["_ods_domain"] == "insurance"
    assert env["_ods_dataset"] == "policies"
    assert env["_ods_failed_stage"] == "dq_check"
    assert env["_ods_error_type"] == "DQValidationError"
    assert env["_ods_error_message"] == "premium not numeric"
    assert env["_ods_replayable"] is True
    assert env["_ods_file_id"] == "file-1"
    assert env["payload"] == {"policy_id": "POL-1", "premium": "BAD"}


def test_envelope_for_message_pattern_requires_correlation():
    """Without a file_id OR a message correlation key, envelope refuses
    to build — DLQ rows MUST be replayable, and replay needs a stable
    identity."""
    with pytest.raises(ValueError, match="DLQ envelope"):
        envelope(
            payload={"x": 1},
            run_id="run-1",
            domain="insurance",
            dataset="event_demo",
            source_application="event_api",
            error_type="MalformedJson",
            error_message="not utf-8",
            stage="message_validate",
            # no file_id, no source_*_id correlation
        )


def test_envelope_replayable_flag_can_be_overridden():
    env = envelope(
        payload={"x": 1},
        run_id="r",
        domain="insurance",
        dataset="policies",
        source_application="sftp",
        error_type="HardFail",
        error_message="schema removed",
        stage="schema_validate",
        file_id="f-1",
        replayable=False,
    )
    assert env["_ods_replayable"] is False


def test_envelope_drops_none_correlation_keys():
    """A file-pattern row with no message correlation should NOT carry
    null _ods_source_*_id fields — they bloat S3 storage and confuse
    operators when they grep DLQ files."""
    env = envelope(
        payload={"x": 1}, run_id="r", domain="insurance", dataset="policies",
        source_application="sftp", error_type="X", error_message="m",
        stage="schema_validate", file_id="f-1",
    )
    for k in (
        "_ods_source_message_id",
        "_ods_source_event_id",
        "_ods_source_request_id",
        "_ods_source_batch_id",
    ):
        assert k not in env


# ---------------------------------------------------------------------------
# DlqWriter
# ---------------------------------------------------------------------------


def _good_envelope() -> dict:
    return envelope(
        payload={"x": 1}, run_id="r-1", domain="insurance", dataset="policies",
        source_application="sftp", error_type="X", error_message="m",
        stage="dq_check", file_id="f-1", business_date="2026-05-06",
    )


def test_dlq_writer_uses_canonical_key_layout():
    s3 = MagicMock()
    writer = DlqWriter(s3, env="local")
    uri = writer.write(_good_envelope(), stage="dq_check", attempt=3)
    s3.put_object.assert_called_once()
    kwargs = s3.put_object.call_args.kwargs
    assert kwargs["Bucket"] == "ods-dlq-local"
    assert kwargs["Key"] == (
        "insurance/policies/dq_check/date=2026-05-06/run_id=r-1/3.json"
    )
    assert kwargs["ContentType"] == "application/json"
    assert uri == f"s3://{kwargs['Bucket']}/{kwargs['Key']}"


def test_dlq_writer_retries_on_transient_errors_then_succeeds():
    s3 = MagicMock()
    s3.put_object.side_effect = [Exception("503"), Exception("503"), None]
    sleep = MagicMock()
    writer = DlqWriter(s3, backoff=(0.0, 0.0, 0.0), sleep=sleep)
    writer.write(_good_envelope(), stage="dq_check", attempt=1)
    assert s3.put_object.call_count == 3
    # Slept twice (between attempts 1->2 and 2->3); not after final success.
    assert sleep.call_count == 2


def test_dlq_writer_raises_after_exhausting_backoff():
    s3 = MagicMock()
    s3.put_object.side_effect = Exception("S3 went away")
    sleep = MagicMock()
    writer = DlqWriter(s3, backoff=(0.0, 0.0), sleep=sleep)
    with pytest.raises(Exception, match="S3 went away"):
        writer.write(_good_envelope(), stage="dq_check", attempt=1)
    # 3 attempts (initial + 2 retries) exhausted backoff tuple.
    assert s3.put_object.call_count == 3


def test_dlq_writer_serialises_envelope_as_json():
    import json
    s3 = MagicMock()
    writer = DlqWriter(s3)
    writer.write(_good_envelope(), stage="dq_check", attempt=1)
    body = s3.put_object.call_args.kwargs["Body"]
    decoded = json.loads(body)
    assert decoded["_ods_run_id"] == "r-1"
    assert decoded["_ods_failed_stage"] == "dq_check"


# ---------------------------------------------------------------------------
# replay_request
# ---------------------------------------------------------------------------


def test_replay_request_carries_minimal_fields_for_replay_cli():
    req = replay_request(
        dlq_uri="s3://ods-dlq-local/insurance/policies/dq_check/date=2026-05-06/run_id=r-1/1.json",
        run_id="r-1",
        domain="insurance",
        dataset="policies",
        target_stage="dq_check",
        reason="re-DQ after rule fix",
    )
    # Replay CLI consumes this; its keys are the operator contract.
    assert set(req.keys()) == {
        "dlq_uri",
        "source_run_id",
        "domain",
        "dataset",
        "target_stage",
        "reason",
    }
    assert req["source_run_id"] == "r-1"
    assert req["dlq_uri"].startswith("s3://")
