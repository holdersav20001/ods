"""Long-running runner mode for the direct-Kafka api_pull slice.

The scheduled DAG (``dag_api_pull``) calls :func:`run_once` once per
Airflow tick. For datasets where sub-tick latency matters, this module
wraps ``run_once`` in a tight loop that respects an external stop
signal (Airflow heartbeat, SIGTERM, etc).

Design contract — see ``docs/api-pull-direct-kafka-design.md`` §
Components → "Long-running poller":

  * One :func:`run_loop` invocation owns one ``(domain, dataset)``.
  * Each iteration re-reads ``committed_cursor_value`` from the
    watermark store so a parallel finalise step that promoted the
    cursor is reflected immediately.
  * Transient errors from ``run_once`` (HTTP 5xx surfaced as
    ``RuntimeError``, transient kafka delivery RuntimeErrors) are
    swallowed and retried on the next tick. Config / validator errors
    (``ValueError``, ``KeyError``) are unrecoverable and propagate so
    the operator restarts cleanly with a fresh config.
  * The producer (and HTTP session, when applicable) is reused across
    iterations; ``run_once`` accepts an injected ``producer`` so we
    only pay the broker handshake / Schema Registry round-trip once.

The module deliberately does **not** import psycopg2 or
``confluent_kafka`` at module import time; both are produced via the
factories supplied by the caller so unit tests stay infra-free.
"""
from __future__ import annotations

import logging
import time
import uuid
from collections.abc import Callable, Mapping
from datetime import datetime, timezone
from threading import Event
from typing import Any

from ods_pipeline.ingest.api_pull_kafka.runner import (
    PublishedBatch,
    build_avro_producer,
    run_once,
)

logger = logging.getLogger(__name__)

# Sentinel exception types treated as unrecoverable. ``run_once`` raises
# RuntimeError for transient kafka / HTTP issues; ValueError / KeyError /
# TypeError indicate the dataset_config or schema is malformed and a
# retry will not help.
_FATAL_EXCEPTIONS: tuple[type[BaseException], ...] = (
    ValueError,
    KeyError,
    TypeError,
)

_DEFAULT_POLL_INTERVAL_SECONDS = 5.0


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def run_loop(
    dataset_config: Mapping[str, Any],
    *,
    kafka_bootstrap: str,
    schema_registry_url: str,
    schema_str: str,
    watermark_store_factory: Callable[[], Any],
    stop_event: Event | None = None,
    env: Mapping[str, str] | None = None,
    sleep: Callable[[float], None] | None = None,
    producer: Any | None = None,
) -> None:
    """Run the direct-Kafka poller in a loop until ``stop_event`` fires.

    Parameters
    ----------
    dataset_config
        Same shape consumed by :func:`run_once` plus the optional
        ``source.poll_interval_seconds`` knob (default 5s) and the
        ``source.continuous`` selector handled by the caller.
    kafka_bootstrap, schema_registry_url, schema_str
        Forwarded to the Avro producer factory on first iteration.
    watermark_store_factory
        Zero-arg callable returning an object with ``read(...)`` and
        ``record_pending(...)`` methods compatible with
        :class:`ods_pipeline.ingest.api_pull.WatermarkStore`. The
        callable is invoked once per iteration so the underlying
        psycopg2 connection can be refreshed if the loop runs for hours.
    stop_event
        :class:`threading.Event` set by the surrounding runtime
        (Airflow operator, signal handler) to request a clean exit.
    env
        Forwarded to ``run_once`` for auth provider env-var lookup.
    sleep
        Injection point for unit tests — defaults to :func:`time.sleep`.
    producer
        Optional pre-built confluent_kafka producer. When omitted the
        loop builds one from ``kafka_bootstrap`` / ``schema_registry_url``
        / ``schema_str`` on the first iteration and reuses it for the
        lifetime of the loop.

    Raises
    ------
    ValueError, KeyError, TypeError
        Propagated from ``run_once`` when configuration is malformed.
        The supervising operator should treat these as unrecoverable
        and not auto-restart without operator review.
    """
    domain = str(dataset_config["domain"])
    dataset = str(dataset_config["dataset"])
    source = dict(dataset_config.get("source") or {})
    source_application = str(source.get("application", f"{domain}.{dataset}"))
    cursor_style = str((source.get("cursor") or {}).get("style", "since_timestamp"))
    poll_interval = float(
        source.get("poll_interval_seconds", _DEFAULT_POLL_INTERVAL_SECONDS)
    )
    if poll_interval <= 0:
        raise ValueError(
            f"poll_interval_seconds must be positive, got {poll_interval!r}"
        )

    stop = stop_event if stop_event is not None else Event()
    do_sleep = sleep if sleep is not None else time.sleep

    if producer is None:
        producer = build_avro_producer(
            kafka_bootstrap=kafka_bootstrap,
            schema_registry_url=schema_registry_url,
            schema_str=schema_str,
        )

    iteration = 0
    logger.info(
        "api_pull_kafka.loop.start domain=%s dataset=%s poll_interval=%.2fs",
        domain,
        dataset,
        poll_interval,
    )

    try:
        while not stop.is_set():
            iteration += 1
            run_id = str(uuid.uuid4())
            business_date = datetime.now(timezone.utc).date().isoformat()

            store = watermark_store_factory()
            try:
                watermark = store.read(
                    domain=domain,
                    dataset=dataset,
                    source_application=source_application,
                    cursor_type=cursor_style,
                )
                committed = getattr(watermark, "committed_cursor_value", None)
            finally:
                # Best-effort close — store may wrap a connection.
                _maybe_close(store)

            try:
                published: PublishedBatch = run_once(
                    dataset_config=dataset_config,
                    committed_cursor_value=committed,
                    run_id=run_id,
                    business_date=business_date,
                    producer=producer,
                    env=env,
                )
            except _FATAL_EXCEPTIONS:
                logger.exception(
                    "api_pull_kafka.loop.fatal domain=%s dataset=%s iter=%d",
                    domain,
                    dataset,
                    iteration,
                )
                raise
            except Exception as exc:  # noqa: BLE001 — transient, retry next tick
                logger.warning(
                    "api_pull_kafka.loop.transient domain=%s dataset=%s "
                    "iter=%d error=%s",
                    domain,
                    dataset,
                    iteration,
                    exc,
                )
                _wait(do_sleep, poll_interval, stop)
                continue

            if published.no_changes:
                logger.info(
                    "api_pull_kafka.loop.no_changes domain=%s dataset=%s iter=%d "
                    "ts=%s",
                    domain,
                    dataset,
                    iteration,
                    _now_iso(),
                )
            else:
                if published.new_cursor_value is not None:
                    store2 = watermark_store_factory()
                    try:
                        store2.record_pending(
                            domain=domain,
                            dataset=dataset,
                            source_application=source_application,
                            run_id=run_id,
                            new_cursor_value=published.new_cursor_value,
                        )
                    finally:
                        _maybe_close(store2)
                logger.info(
                    "api_pull_kafka.loop.published domain=%s dataset=%s "
                    "iter=%d records=%d topic=%s new_cursor=%s",
                    domain,
                    dataset,
                    iteration,
                    published.record_count,
                    published.target_topic,
                    published.new_cursor_value,
                )

            _wait(do_sleep, poll_interval, stop)
    finally:
        logger.info(
            "api_pull_kafka.loop.exit domain=%s dataset=%s iterations=%d",
            domain,
            dataset,
            iteration,
        )
        # Drain producer best-effort so in-flight messages aren't lost
        # on operator shutdown. Producer creation is owned here when not
        # injected, so we own teardown too.
        try:
            flush = getattr(producer, "flush", None)
            if callable(flush):
                flush(timeout=5)
        except Exception:  # noqa: BLE001
            pass


def _wait(sleep_fn: Callable[[float], None], seconds: float, stop: Event) -> None:
    """Sleep ``seconds`` but break early when ``stop`` is set.

    The injected ``sleep_fn`` is normally :func:`time.sleep`; tests
    pass a fake clock that records the requested durations.
    """
    if stop.is_set():
        return
    sleep_fn(seconds)


def _maybe_close(store: Any) -> None:
    closer = getattr(store, "close", None)
    if callable(closer):
        try:
            closer()
        except Exception:  # noqa: BLE001
            pass
