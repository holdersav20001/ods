"""Control-plane helpers for message/API ingestion flows.

Stateless contract (since 2026-05-07)
-------------------------------------

Both :func:`start_run` and :func:`record_result` commit per write — the
caller no longer wraps them in a transaction. The strict write order in
``record_result`` (stages → archive → reconciliation → run.status) means
the dashboard never observes a ``status='succeeded'`` run without its
proof rows already landed: succeeded is the LAST commit.

For mid-flight failures the caller's own ``except`` block (or the
:func:`ods_pipeline.stages.stage_scope` context manager) is responsible
for writing the ``stage_failed`` row and calling
:func:`ods_pipeline.runs.update` with ``status='failed'``. If even those
handlers don't run (process killed mid-flight), the heartbeat-staleness
janitor (``airflow.dags.dag_run_janitor``) closes the orphan row.
"""
from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from ods_pipeline import metadata, reconciliation, runs, stages
from ods_pipeline.models import PATTERN_CORRELATION_FIELD, PatternType, Stage, StageEvent


def _caller_must_rollback_on_exception() -> None:
    """Caller-managed transaction contract.

    ``record_result`` (below) calls ``runs.update``, ``stages.write``,
    ``stages.finish``, and ``reconciliation.write_check`` with
    ``commit=False``. The CALLER MUST wrap the invocation in a try/except
    and call ``conn.rollback()`` on exception, otherwise the open
    transaction stays open and partially-written rows remain in pg_locks.

    Recommended pattern::

        try:
            messages.record_result(conn, ...)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    """


def correlate(
    message: Mapping[str, Any],
    context: Mapping[str, Any],
    *,
    pattern_type: str,
) -> bool:
    """Return True if ``message`` belongs to the given ``context`` for ``pattern_type``.

    Each ingestion pattern has its own correlation field (see
    :data:`ods_pipeline.models.PATTERN_CORRELATION_FIELD`). The function:

    - looks up the field for the pattern,
    - returns True if both message and context carry the same non-empty value,
    - returns True when context has no correlation set (broadcast / "all
      messages of this pattern"),
    - falls back to ``_ods_run_id`` cross-check for the FILE pattern only,
      preserving the legacy two-key match in
      :func:`glue.jobs.canonicalize.matches_context`.

    Raises ``ValueError`` for unknown ``pattern_type``.
    """
    if pattern_type not in PatternType.ALL:
        raise ValueError(
            f"unknown pattern_type {pattern_type!r}; "
            f"expected one of {sorted(PatternType.ALL)}"
        )
    field = PATTERN_CORRELATION_FIELD[pattern_type]
    ctx_value = context.get(field) or context.get(field.removeprefix("_ods_"))
    msg_value = message.get(field)

    if pattern_type == PatternType.FILE:
        # Legacy: file pattern allows correlation by run_id as a secondary key.
        ctx_run = context.get("_ods_run_id") or context.get("run_id") or context.get("upstream_run_id")
        msg_run = message.get("_ods_run_id")
        if ctx_value and msg_value and str(ctx_value) == str(msg_value):
            return True
        if ctx_run and msg_run and str(ctx_run) == str(msg_run):
            return True
        # No context = broadcast.
        return not ctx_value and not ctx_run

    if not ctx_value:
        return True  # broadcast / no correlation set
    return msg_value is not None and str(ctx_value) == str(msg_value)


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
    orchestrators: list[dict[str, Any]] | None = None,
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
    parent_payload = list(orchestrators or [])
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
        orchestrators=parent_payload,
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

    Stateless write order (2026-05-07): every helper call below commits
    independently. Order matters — recon row writes BEFORE the run flips
    to terminal status so the dashboard rule "succeeded ⇒ recon row
    present" always holds. If the process dies mid-call the next-minute
    janitor closes the still-running row; the partial stage rows that
    landed remain durable so operators can see exactly where the run
    stopped.
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
        commit=True,
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
        commit=True,
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
            commit=True,
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
            commit=True,
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
        commit=True,
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
        commit=True,
    )
    runs.update(
        conn,
        run_id,
        commit=True,
        status=status,
        record_count_source=source_count,
        record_count_dq_fail=validation_fail_count + dlq_count,
        record_count_published=published_count,
        kafka_topic=kafka_topic,
        error_summary=None if ok else "message/API reconciliation failed",
    )
    return status
