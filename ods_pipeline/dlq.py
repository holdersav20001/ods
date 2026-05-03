"""DLQ helpers shared by file, canonicalize, and message/API flows."""
from __future__ import annotations

import json
import logging
import time
from collections.abc import Mapping
from typing import Any
from urllib.parse import urlparse

from ods_pipeline import metadata

_log = logging.getLogger(__name__)


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


class DlqWriter:
    """Persist DLQ envelopes to S3 with bounded exponential-backoff retry.

    The writer is intentionally small: callers (Glue jobs, the message/API
    service, replay tooling) hand it a fully-formed envelope built via
    :func:`envelope` and a ``stage`` (matching the prefix layout from
    :func:`s3_prefix`). It computes the canonical key, JSON-encodes, and
    PUTs to S3, retrying transient errors with exp backoff (1s, 2s, 4s, 8s,
    16s by default; configurable). Permanent errors raise immediately.

    The S3 key is::

        s3://<dlq-bucket>/<domain>/<dataset>/<stage>/date=<bd>/run_id=<rid>/<attempt>.json

    where ``attempt`` is the per-write counter the caller supplies, so multiple
    failed records from the same run land at distinct keys.
    """

    DEFAULT_BACKOFF = (1.0, 2.0, 4.0, 8.0, 16.0)

    def __init__(
        self,
        s3_client,
        *,
        env: str = "local",
        backoff: tuple[float, ...] = DEFAULT_BACKOFF,
        sleep=time.sleep,
    ) -> None:
        self._s3 = s3_client
        self._env = env
        self._backoff = backoff
        self._sleep = sleep

    def write(
        self,
        envelope_dict: Mapping[str, Any],
        *,
        stage: str,
        attempt: int,
    ) -> str:
        """Write ``envelope_dict`` to S3 and return the resulting ``s3://`` URI.

        Raises the last underlying exception if every retry fails. Caller is
        expected to surface the failure to the run ledger; we do not write
        run_log here to keep the writer side-effect-isolated.
        """
        prefix = s3_prefix(
            env=self._env,
            domain=str(envelope_dict["_ods_domain"]),
            dataset=str(envelope_dict["_ods_dataset"]),
            stage=stage,
            run_id=str(envelope_dict["_ods_run_id"]),
            business_date=envelope_dict.get("_ods_business_date"),
        )
        bucket, key = _split_s3_uri(f"{prefix}{attempt}.json")
        body = json.dumps(envelope_dict, default=str).encode("utf-8")

        last_exc: Exception | None = None
        for attempt_idx in range(len(self._backoff) + 1):
            try:
                self._s3.put_object(
                    Bucket=bucket,
                    Key=key,
                    Body=body,
                    ContentType="application/json",
                )
                return f"s3://{bucket}/{key}"
            except Exception as exc:  # noqa: BLE001 — retry intentionally broad
                last_exc = exc
                if attempt_idx >= len(self._backoff):
                    break
                delay = self._backoff[attempt_idx]
                _log.warning(
                    "DlqWriter put_object failed (attempt %d/%d), retrying in %.1fs: %s",
                    attempt_idx + 1,
                    len(self._backoff) + 1,
                    delay,
                    exc,
                )
                self._sleep(delay)
        assert last_exc is not None  # for type checkers
        raise last_exc


def _split_s3_uri(uri: str) -> tuple[str, str]:
    parsed = urlparse(uri)
    if parsed.scheme != "s3" or not parsed.netloc:
        raise ValueError(f"not an s3 URI: {uri!r}")
    return parsed.netloc, parsed.path.lstrip("/")


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
