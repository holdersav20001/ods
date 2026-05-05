"""Direct API → Kafka api_pull runner — one poll/publish cycle.

Reuses the polling primitives from ``ods_pipeline.ingest.api_pull``
(:func:`build_auth`, :func:`build_cursor`) plus an idempotent +
transactional Avro Kafka producer. Output is one Avro message per
source record on the dataset's raw Kafka topic; the message envelope
mirrors the file-pipeline ODS metadata block so downstream consumers
(canonicalize, JDBC sink, S3 sink) see identical correlation fields.

This module does **not** touch the Postgres control-plane. The DAG
dispatcher (``dag_api_pull``) is responsible for ``run_log``,
``run_stage_log``, ``lineage_edge``, ``reconciliation_log`` and the
``api_pull_watermark`` lifecycle. Keeping I/O concerns split makes the
runner unit-testable without psycopg2.
"""
from __future__ import annotations

import io
import json
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from ods_pipeline import metadata as _metadata
from ods_pipeline.ingest.api_pull.auth import AuthProvider, build_auth
from ods_pipeline.ingest.api_pull.cursors import Cursor, CursorRequest, build_cursor


# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------


@dataclass
class PublishedBatch:
    """Outcome of one direct-Kafka poll/publish cycle.

    ``no_changes=True`` means the source returned 0 records or 304 Not
    Modified. The DAG should mark the run skipped, not advance the
    watermark, and skip the produce step's recon and lineage writes.
    """

    domain: str
    dataset: str
    source_application: str
    run_id: str
    target_topic: str
    record_count: int
    page_count: int
    old_cursor_value: str | None
    new_cursor_value: str | None
    source_request_id: str
    offset_start_by_partition: dict[int, int] = field(default_factory=dict)
    offset_end_by_partition: dict[int, int] = field(default_factory=dict)
    no_changes: bool = False


# ---------------------------------------------------------------------------
# HTTP session
# ---------------------------------------------------------------------------


def _build_session(*, auth: AuthProvider, timeout_seconds: float, retries: int) -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=retries,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET", "HEAD"}),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers["Accept"] = "application/json"
    original = session.request

    def _with_timeout(*args, **kwargs):
        kwargs.setdefault("timeout", timeout_seconds)
        return original(*args, **kwargs)

    session.request = _with_timeout  # type: ignore[assignment]
    auth.apply(session)
    return session


def _records_from_body(body: Any) -> Sequence[Mapping[str, Any]]:
    """Accept ``[{...}]`` or ``{"items": [...]}`` etc. Same semantics as
    the file-pipeline poller so wire-shape changes are not required when
    a dataset is moved between delivery modes."""
    if isinstance(body, list):
        return body
    if isinstance(body, dict):
        for key in ("items", "data", "records", "results"):
            value = body.get(key)
            if isinstance(value, list):
                return value
    return []


# ---------------------------------------------------------------------------
# Envelope
# ---------------------------------------------------------------------------


def build_envelope(
    *,
    record: Mapping[str, Any],
    run_id: str,
    domain: str,
    dataset: str,
    source_application: str,
    source_request_id: str,
    business_date: str,
    cursor_value: str | None,
    schema_id: str,
    schema_version: int | str,
) -> dict[str, Any]:
    """Build the per-record Avro envelope.

    Top-level fields are the source record keys (flat). ODS metadata
    fields are stamped alongside so the JDBC sink can persist origin
    correlation without nested types.
    """
    meta = _metadata.message_metadata(
        run_id=run_id,
        domain=domain,
        dataset=dataset,
        source_application=source_application,
        source_request_id=source_request_id,
    )
    envelope: dict[str, Any] = {**dict(record)}
    envelope["_ods_run_id"] = run_id
    envelope["_ods_business_date"] = business_date
    envelope["_ods_source_request_id"] = meta.get("_ods_source_request_id")
    envelope["_ods_source_message_id"] = meta.get("_ods_source_message_id")
    envelope["_ods_source_event_id"] = meta.get("_ods_source_event_id")
    envelope["_ods_source_batch_id"] = meta.get("_ods_source_batch_id")
    envelope["_ods_source_application"] = source_application
    envelope["_ods_source_cursor"] = cursor_value
    envelope["_ods_archive_s3_uri"] = None  # written by the S3 sink later
    envelope["_ods_domain"] = domain
    envelope["_ods_dataset"] = dataset
    envelope["_ods_file_id"] = None  # synthetic file_id is the DAG's job
    envelope["_ods_ingested_at"] = meta.get("_ods_ingested_at")
    envelope["_ods_schema_id"] = schema_id
    envelope["_ods_schema_version"] = (
        int(schema_version) if isinstance(schema_version, (int, str)) and str(schema_version).isdigit()
        else None
    )
    envelope["_ods_kafka_partition"] = None  # filled in by delivery callback
    envelope["_ods_kafka_offset"] = None
    return envelope


# ---------------------------------------------------------------------------
# Producer
# ---------------------------------------------------------------------------


def build_avro_producer(
    *,
    kafka_bootstrap: str,
    schema_registry_url: str,
    schema_str: str,
    transactional_id: str | None = None,
):
    """Construct an idempotent Avro :class:`SerializingProducer`.

    Lazy import so the runner module imports cleanly in environments
    without confluent_kafka (unit tests use the injected ``producer``
    parameter on :func:`run_once`).
    """
    from confluent_kafka import SerializingProducer
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.schema_registry.avro import AvroSerializer
    from confluent_kafka.serialization import StringSerializer

    registry = SchemaRegistryClient({"url": schema_registry_url})
    avro_serializer = AvroSerializer(registry, schema_str)
    config = {
        "bootstrap.servers": kafka_bootstrap,
        "key.serializer": StringSerializer("utf_8"),
        "value.serializer": avro_serializer,
        "enable.idempotence": True,
        "acks": "all",
        "max.in.flight.requests.per.connection": 5,
        "linger.ms": 5,
    }
    if transactional_id:
        config["transactional.id"] = transactional_id
    return SerializingProducer(config)


# ---------------------------------------------------------------------------
# run_once
# ---------------------------------------------------------------------------


def run_once(
    *,
    dataset_config: Mapping[str, Any],
    kafka_bootstrap: str | None = None,
    schema_registry_url: str | None = None,
    schema_str: str | None = None,
    committed_cursor_value: str | None,
    run_id: str,
    business_date: str,
    session: requests.Session | None = None,
    cursor: Cursor | None = None,
    producer: Any = None,
    env: Mapping[str, str] | None = None,
) -> PublishedBatch:
    """Execute one poll/publish cycle.

    Walks pages via the configured cursor, builds Avro envelopes per
    record, produces them to the dataset's raw topic, and waits for
    delivery. Returns the per-partition offset window and the new
    cursor value. If the source returns no records, returns
    ``no_changes=True`` and produces nothing.

    Injectables (``session``, ``cursor``, ``producer``) keep the runner
    unit-testable without HTTP / Kafka / Schema Registry.
    """
    domain = str(dataset_config["domain"])
    dataset = str(dataset_config["dataset"])
    source = dict(dataset_config.get("source") or {})
    source_application = str(source.get("application", f"{domain}.{dataset}"))
    target_topic = str(dataset_config["target_topic"])
    schema_id = str(dataset_config.get("schema_id", f"{domain}.{dataset}"))
    schema_version = dataset_config.get("schema_version", 1)
    timeout_seconds = float(source.get("timeout_seconds", 30))
    retries = int(source.get("retries", 3))

    if session is None:
        auth = build_auth(source.get("auth"), env=env)
        session = _build_session(
            auth=auth,
            timeout_seconds=timeout_seconds,
            retries=retries,
        )
    if cursor is None:
        cursor = build_cursor(source, committed_value=committed_cursor_value)

    source_request_id = str(uuid.uuid4())
    all_records: list[Mapping[str, Any]] = []
    request: CursorRequest | None = cursor.initial_request()
    page_count = 0

    while request is not None:
        response = session.get(request.url, params=request.params or None)
        page_count += 1
        if response.status_code == 304:
            break
        response.raise_for_status()
        body = response.json() if response.content else None
        all_records.extend(_records_from_body(body))
        request = cursor.next_request(response.headers, body)

    new_cursor_value = cursor.advance(all_records)

    if not all_records:
        return PublishedBatch(
            domain=domain,
            dataset=dataset,
            source_application=source_application,
            run_id=run_id,
            target_topic=target_topic,
            record_count=0,
            page_count=page_count,
            old_cursor_value=committed_cursor_value,
            new_cursor_value=None,
            source_request_id=source_request_id,
            no_changes=True,
        )

    if producer is None:
        if not (kafka_bootstrap and schema_registry_url and schema_str):
            raise ValueError(
                "producer must be supplied OR kafka_bootstrap + "
                "schema_registry_url + schema_str must be provided"
            )
        producer = build_avro_producer(
            kafka_bootstrap=kafka_bootstrap,
            schema_registry_url=schema_registry_url,
            schema_str=schema_str,
        )

    delivered_offsets: dict[int, int] = {}
    delivered_starts: dict[int, int] = {}
    delivery_errors: list[str] = []

    def _on_delivery(err, msg):
        if err is not None:
            delivery_errors.append(str(err))
            return
        partition = int(msg.partition())
        offset = int(msg.offset())
        delivered_starts.setdefault(partition, offset)
        if partition not in delivered_offsets or delivered_offsets[partition] < offset:
            delivered_offsets[partition] = offset

    for record in all_records:
        envelope = build_envelope(
            record=record,
            run_id=run_id,
            domain=domain,
            dataset=dataset,
            source_application=source_application,
            source_request_id=source_request_id,
            business_date=business_date,
            cursor_value=committed_cursor_value,
            schema_id=schema_id,
            schema_version=schema_version,
        )
        producer.produce(
            topic=target_topic,
            key=str(envelope["_ods_run_id"]),
            value=envelope,
            on_delivery=_on_delivery,
        )

    remaining = producer.flush(timeout=30)
    if remaining:
        raise RuntimeError(
            f"{remaining} kafka message(s) not delivered for run_id={run_id}"
        )
    if delivery_errors:
        raise RuntimeError(
            f"kafka delivery errors for run_id={run_id}: {delivery_errors[:3]}"
        )

    # End-offset = max delivered offset + 1 (next-message convention).
    offset_end = {p: o + 1 for p, o in delivered_offsets.items()}

    return PublishedBatch(
        domain=domain,
        dataset=dataset,
        source_application=source_application,
        run_id=run_id,
        target_topic=target_topic,
        record_count=len(all_records),
        page_count=page_count,
        old_cursor_value=committed_cursor_value,
        new_cursor_value=new_cursor_value,
        source_request_id=source_request_id,
        offset_start_by_partition=dict(delivered_starts),
        offset_end_by_partition=offset_end,
        no_changes=False,
    )
