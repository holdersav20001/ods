"""Control-plane helpers for message/API ingestion flows."""
from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from ods_pipeline import metadata, reconciliation, runs, stages
from ods_pipeline.models import Stage, StageEvent


def _correlation_metadata(
    *,
    run_id: str,
    domain: str,
    dataset: str,
    source_application: str,
    correlation: Mapping[str, Any],
) -> dict[str, Any]:
    message_meta = metadata.message_metadata(
        run_id=run_id,
        domain=domain,
        dataset=dataset,
        source_application=source_application,
        source_message_id=correlation.get("_ods_source_message_id")
        or correlation.get("source_message_id"),
        source_event_id=correlation.get("_ods_source_event_id")
        or correlation.get("source_event_id"),
        source_request_id=correlation.get("_ods_source_request_id")
        or correlation.get("source_request_id"),
        source_batch_id=correlation.get("_ods_source_batch_id")
        or correlation.get("source_batch_id"),
    )
    return {
        key: message_meta.get(key)
        for key in metadata.MESSAGE_CORRELATION_FIELDS
        if message_meta.get(key)
    }


def start_run(
    conn,
    *,
    run_id: str,
    domain: str,
    dataset: str,
    source_application: str,
    correlation: Mapping[str, Any],
    business_date: str | None = None,
    kafka_topic: str | None = None,
    expected_count: int | None = None,
    parents: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Start a message/API run and open the receive stage.

    ``correlation`` must include at least one of:
    ``source_message_id``, ``source_event_id``, ``source_request_id``, or
    ``source_batch_id``.  The helper intentionally writes only existing
    pipeline tables; payload storage remains S3/Kafka responsibility.
    """
    correlation_meta = _correlation_metadata(
        run_id=run_id,
        domain=domain,
        dataset=dataset,
        source_application=source_application,
        correlation=correlation,
    )
    parent_payload = list(parents or [])
    parent_payload.append({
        "edge_type": "message_correlation",
        "source_application": source_application,
        **correlation_meta,
    })
    runs.start(
        conn,
        run_id=run_id,
        pipeline_type="message_api",
        domain=domain,
        dataset=dataset,
        business_date=business_date,
        kafka_topic=kafka_topic,
        parents=parent_payload,
    )
    stages.start(
        conn,
        run_id=run_id,
        stage=Stage.MESSAGE_RECEIVE,
        record_count_in=expected_count,
        metrics={
            "source_application": source_application,
            "correlation": correlation_meta,
        },
    )
    return correlation_meta


def record_result(
    conn,
    *,
    run_id: str,
    domain: str,
    dataset: str,
    source_count: int,
    published_count: int,
    validation_fail_count: int = 0,
    dlq_count: int = 0,
    archive_count: int | None = None,
    business_date: str | None = None,
    kafka_topic: str | None = None,
    dlq_ref: str | None = None,
    archive_ref: str | None = None,
    extra_detail: Mapping[str, Any] | None = None,
) -> str:
    """Close a message/API run with count reconciliation and stage facts.

    Atomicity (B4): every helper call below uses ``commit=False`` — this
    function does NOT commit on its own.  The caller MUST wrap the
    invocation in ``with conn:`` (or equivalent transactional context) so
    that all stage / recon / run_log writes either commit together at the
    end or roll back together on any exception.
    """
    accepted_count = int(source_count) - int(validation_fail_count) - int(dlq_count)
    discrepancy = int(published_count) - accepted_count
    archive_discrepancy = (
        None if archive_count is None else int(archive_count) - int(source_count)
    )
    ok = discrepancy == 0 and (archive_discrepancy in (None, 0))
    status = "succeeded" if ok else "failed"

    stages.finish(
        conn,
        run_id=run_id,
        stage=Stage.MESSAGE_RECEIVE,
        status="succeeded",
        event_type=StageEvent.COMPLETED,
        record_count_in=source_count,
        record_count_out=source_count,
        commit=False,
    )
    stages.write(
        conn,
        run_id=run_id,
        stage=Stage.MESSAGE_VALIDATE,
        status="warned" if validation_fail_count else "succeeded",
        event_type=StageEvent.WARNED if validation_fail_count else StageEvent.COMPLETED,
        record_count_in=source_count,
        record_count_out=source_count - validation_fail_count,
        metrics={"validation_fail_count": validation_fail_count},
        commit=False,
    )
    if dlq_count:
        stages.write(
            conn,
            run_id=run_id,
            stage=Stage.DLQ_WRITE,
            status="succeeded",
            event_type=StageEvent.COMPLETED,
            output_ref=dlq_ref,
            record_count_in=dlq_count,
            record_count_out=dlq_count,
            commit=False,
        )
    if archive_count is not None:
        stages.write(
            conn,
            run_id=run_id,
            stage=Stage.MESSAGE_ARCHIVE,
            status="succeeded" if archive_discrepancy == 0 else "failed",
            event_type=StageEvent.COMPLETED if archive_discrepancy == 0 else StageEvent.FAILED,
            output_ref=archive_ref,
            record_count_in=source_count,
            record_count_out=archive_count,
            error=None if archive_discrepancy == 0 else f"archive discrepancy={archive_discrepancy}",
            commit=False,
        )

    detail = {
        "source_count": source_count,
        "validation_fail_count": validation_fail_count,
        "dlq_count": dlq_count,
        "accepted_count": accepted_count,
        "published_count": published_count,
        "archive_count": archive_count,
        "archive_discrepancy": archive_discrepancy,
        **dict(extra_detail or {}),
    }
    reconciliation.write_check(
        conn,
        check_type="message_batch_count",
        run_id=run_id,
        domain=domain,
        dataset=dataset,
        business_date=business_date,
        source_count=accepted_count,
        kafka_count=published_count,
        status="ok" if ok else "failed",
        detail=json.dumps(detail, sort_keys=True),
        commit=False,
    )
    stages.write(
        conn,
        run_id=run_id,
        stage=Stage.RECON_MESSAGE,
        status="succeeded" if ok else "failed",
        event_type=StageEvent.COMPLETED if ok else StageEvent.FAILED,
        record_count_in=accepted_count,
        record_count_out=published_count,
        metrics=detail,
        error=None if ok else f"message reconciliation mismatch={discrepancy}",
        commit=False,
    )
    runs.update(
        conn,
        run_id,
        commit=False,
        status=status,
        record_count_source=source_count,
        record_count_dq_fail=validation_fail_count + dlq_count,
        record_count_published=published_count,
        kafka_topic=kafka_topic,
        error_summary=None if ok else "message/API reconciliation failed",
    )
    return status
