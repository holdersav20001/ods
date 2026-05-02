"""Reusable Kafka transactional-publish lifecycle (B5/B6).

Extracted from ``glue/jobs/ods_s3_publish.py`` so the same lifecycle can be
exercised by:

* ``ods_s3_publish.py``  — current production caller.
* ``ods_pipeline/messages.py`` callers — once T6 / T16 land message-pattern
  publishing.
* The unit test ``tests/unit/test_publish_transactional.py`` — exercises the
  exact production code path, so reverting the Glue caller would fail the
  test suite (no tautology — the helper is load-bearing).

Design contract:

* The caller owns ``producer = Producer(config)`` and ``producer.close()``
  (typically inside its own ``finally``). This helper does NOT close the
  producer because the caller may want to reuse the same Producer across
  multiple invocations or wrap close ordering with other cleanup.
* The caller passes a fresh ``OffsetTracker``; on success the tracker is
  populated with broker-acknowledged ``(partition, offset)`` per delivered
  message; ``tracker.delivered_count`` is the recon source-of-truth.
* The caller passes ``payloads`` — any iterable; each item is forwarded to
  ``on_send(producer, payload, on_delivery=tracker.on_delivery)``. The
  ``on_send`` callback is the integration point: production callers serialise
  Avro and call ``producer.produce(...)``; tests use a one-line lambda.

Lifecycle (single transaction):
    init_transactions  →  begin_transaction
                          →  for payload: on_send(...)
                          →  flush
                          →  if tracker.errors: raise
                          →  commit_transaction
    Any exception in the produce loop or in flush  →  abort_transaction
                                                    →  raise
"""
from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import Any

from ods_pipeline.offsets import OffsetTracker


def _default_on_send(producer, payload, *, on_delivery):
    """Default per-payload sender used when caller doesn't supply one.

    Expects ``payload`` to be a mapping with ``topic``, ``key``, ``value`` —
    matches the lightweight test fixture shape. Production callers ALWAYS
    pass their own ``on_send`` because they need to serialise Avro and
    compute message keys.
    """
    producer.produce(
        topic=payload["topic"],
        key=payload.get("key"),
        value=payload.get("value"),
        on_delivery=on_delivery,
    )


def publish_with_transaction(
    producer,
    payloads: Iterable[Any],
    tracker: OffsetTracker,
    *,
    on_send: Callable[..., None] | None = None,
) -> None:
    """Run one transactional publish: init → begin → produce* → commit/abort.

    Caller owns ``producer.close()`` (typically in their own ``finally``).
    Raises whatever the produce loop raises, after first calling
    ``producer.abort_transaction()``. If ``tracker.errors`` is non-empty
    after ``producer.flush()``, raises ``RuntimeError`` describing the first
    failure (and aborts).
    """
    sender = on_send or _default_on_send
    producer.init_transactions()
    producer.begin_transaction()
    try:
        for payload in payloads:
            sender(producer, payload, on_delivery=tracker.on_delivery)
        producer.flush()
        if tracker.errors:
            raise RuntimeError(
                f"{len(tracker.errors)} kafka delivery failures; "
                f"first: {tracker.errors[0]}"
            )
        producer.commit_transaction()
    except Exception:
        producer.abort_transaction()
        raise
