"""Publish pipeline run lifecycle events to ods.pipeline.run-events (Kafka + Postgres)."""
from __future__ import annotations

import datetime
import json
import os


TOPIC = "ods.pipeline.run-events"
_SUBJECT = f"{TOPIC}-value"

_SCHEMA_STR = json.dumps({
    "type": "record",
    "name": "RunEvent",
    "namespace": "com.aviva.ods.pipeline",
    "fields": [
        {"name": "run_id",                 "type": "string"},
        {"name": "event_type",             "type": "string"},
        {"name": "domain",                 "type": "string"},
        {"name": "dataset",                "type": "string"},
        {"name": "business_date",          "type": "string"},
        {"name": "status",                 "type": "string"},
        {"name": "record_count_published", "type": ["null", "int"],    "default": None},
        {"name": "kafka_topic",            "type": ["null", "string"], "default": None},
        {"name": "kafka_offset_end",       "type": ["null", "long"],   "default": None},
        {"name": "occurred_at",            "type": "string"},
    ],
})


def produce_run_event(
    event_type: str,
    run_id: str,
    domain: str,
    dataset: str,
    business_date: str,
    status: str,
    *,
    record_count_published: int | None = None,
    kafka_topic: str | None = None,
    kafka_offset_end: int | None = None,
) -> None:
    from confluent_kafka import Producer
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.schema_registry.avro import AvroSerializer
    from confluent_kafka.serialization import MessageField, SerializationContext

    sr_url = os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "broker:29092")

    sr = SchemaRegistryClient({"url": sr_url})
    serializer = AvroSerializer(sr, _SCHEMA_STR)
    producer = Producer({"bootstrap.servers": bootstrap, "acks": "all"})

    payload = {
        "run_id": run_id,
        "event_type": event_type,
        "domain": domain,
        "dataset": dataset,
        "business_date": str(business_date),
        "status": status,
        "record_count_published": record_count_published,
        "kafka_topic": kafka_topic,
        "kafka_offset_end": kafka_offset_end,
        "occurred_at": datetime.datetime.utcnow().isoformat(),
    }

    producer.produce(
        topic=TOPIC,
        key=run_id.encode(),
        value=serializer(payload, SerializationContext(TOPIC, MessageField.VALUE)),
    )
    producer.flush()

    _write_pg(payload)


def _write_pg(payload: dict) -> None:
    pg_dsn = os.environ.get("PG_DSN")
    if not pg_dsn:
        return
    import psycopg2
    with psycopg2.connect(pg_dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline.run_events
                (run_id, event_type, domain, dataset, business_date, status,
                 record_count_published, kafka_topic, kafka_offset_end, occurred_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                payload["run_id"],
                payload["event_type"],
                payload["domain"],
                payload["dataset"],
                payload["business_date"],
                payload["status"],
                payload["record_count_published"],
                payload["kafka_topic"],
                payload["kafka_offset_end"],
                payload["occurred_at"],
            ),
        )
