"""Publish pipeline run lifecycle events to ods.pipeline.run-events (Kafka + Postgres)."""
from __future__ import annotations

import datetime
import json
import os
import sys

import ods_ingestion_control as control
from ods_pipeline._db import build_dsn

TOPIC = "ods.pipeline.run-events"
_SUBJECT = f"{TOPIC}-value"

SCHEMA_STR = json.dumps({
    "type": "record",
    "name": "RunEvent",
    "namespace": "com.aviva.ods.pipeline",
    "fields": [
        {"name": "run_id",                 "type": "string"},
        {"name": "event_type",             "type": "string"},
        {"name": "pipeline_type",          "type": ["null", "string"],  "default": None},
        {"name": "domain",                 "type": "string"},
        {"name": "dataset",                "type": "string"},
        {"name": "business_date",          "type": "string"},
        {"name": "status",                 "type": "string"},
        {"name": "record_count_source",    "type": ["null", "int"],     "default": None},
        {"name": "record_count_dq_pass",   "type": ["null", "int"],     "default": None},
        {"name": "record_count_dq_fail",   "type": ["null", "int"],     "default": None},
        {"name": "record_count_published", "type": ["null", "int"],     "default": None},
        {"name": "kafka_topic",            "type": ["null", "string"],  "default": None},
        {"name": "kafka_offset_end",       "type": ["null", "long"],    "default": None},
        {"name": "error_summary",          "type": ["null", "string"],  "default": None},
        {"name": "occurred_at",            "type": "string"},
        {"name": "file_id",                "type": ["null", "string"],  "default": None},
        {"name": "s3_raw_path",            "type": ["null", "string"],  "default": None},
        {"name": "s3_curated_path",        "type": ["null", "string"],  "default": None},
        {"name": "file_md5",               "type": ["null", "string"],  "default": None},
        {"name": "kafka_offset_start",     "type": ["null", "long"],    "default": None},
        {"name": "stages",                 "type": ["null", "string"],  "default": None},
    ],
})


def produce(
    event_type: str,
    run_id: str,
    domain: str,
    dataset: str,
    business_date: str,
    status: str,
    *,
    pipeline_type: str | None = None,
    record_count_source: int | None = None,
    record_count_dq_pass: int | None = None,
    record_count_dq_fail: int | None = None,
    record_count_published: int | None = None,
    kafka_topic: str | None = None,
    kafka_offset_end: int | None = None,
    error_summary: str | None = None,
    file_id: str | None = None,
    s3_raw_path: str | None = None,
    s3_curated_path: str | None = None,
    file_md5: str | None = None,
    kafka_offset_start: int | None = None,
    stages: str | None = None,
) -> None:
    """Emit a run lifecycle event to Postgres (always) and Kafka (best-effort)."""
    payload = {
        "run_id":                 str(run_id),
        "event_type":             event_type,
        "pipeline_type":          pipeline_type,
        "domain":                 domain,
        "dataset":                dataset,
        "business_date":          str(business_date) if business_date else "",
        "status":                 status,
        "record_count_source":    int(record_count_source)    if record_count_source    is not None else None,
        "record_count_dq_pass":   int(record_count_dq_pass)   if record_count_dq_pass   is not None else None,
        "record_count_dq_fail":   int(record_count_dq_fail)   if record_count_dq_fail   is not None else None,
        "record_count_published": int(record_count_published) if record_count_published is not None else None,
        "kafka_topic":            kafka_topic,
        "kafka_offset_end":       int(kafka_offset_end)       if kafka_offset_end       is not None else None,
        "error_summary":          error_summary,
        "occurred_at":            datetime.datetime.utcnow().isoformat(),
        "file_id":                file_id,
        "s3_raw_path":            s3_raw_path,
        "s3_curated_path":        s3_curated_path,
        "file_md5":               file_md5,
        "kafka_offset_start":     int(kafka_offset_start)     if kafka_offset_start     is not None else None,
        "stages":                 stages,
    }

    # Always write to Postgres regardless of Kafka outcome
    _write_pg(payload)

    # Kafka publish is best-effort — failure must never block the pipeline
    try:
        from confluent_kafka import Producer
        from confluent_kafka.schema_registry import SchemaRegistryClient
        from confluent_kafka.schema_registry.avro import AvroSerializer
        from confluent_kafka.serialization import MessageField, SerializationContext

        sr_url   = os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
        bootstrap = os.environ.get(
            "KAFKA_BOOTSTRAP_SERVERS",
            os.environ.get("KAFKA_BOOTSTRAP", "broker:29092"),
        )
        sr = SchemaRegistryClient({"url": sr_url})
        serializer = AvroSerializer(sr, SCHEMA_STR)
        producer   = Producer({"bootstrap.servers": bootstrap, "acks": "all"})
        producer.produce(
            topic=TOPIC,
            key=run_id.encode(),
            value=serializer(payload, SerializationContext(TOPIC, MessageField.VALUE)),
        )
        producer.flush()
    except Exception as exc:
        print(f"[ods_pipeline.events] WARNING: Kafka publish failed: {exc}", file=sys.stderr)


def _write_pg(payload: dict) -> None:
    """Write *payload* to ``pipeline.run_events``.  Non-fatal on error."""
    try:
        dsn = build_dsn()
    except ValueError:
        return  # No Postgres configured — skip silently

    try:
        import psycopg2
        with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
            # Auto-fetch stage details from run_stage_log if not supplied
            stages_json = payload.get("stages")
            if stages_json is None:
                try:
                    cur.execute(
                        """
                        SELECT stage, status, started_at, ended_at,
                               input_ref, output_ref,
                               record_count_in, record_count_out,
                               metrics, error
                          FROM pipeline.run_stage_log
                         WHERE run_id = %s
                         ORDER BY started_at
                        """,
                        (payload["run_id"],),
                    )
                    rows = cur.fetchall()
                    if rows:
                        cols = [
                            "stage", "status", "started_at", "ended_at",
                            "input_ref", "output_ref",
                            "record_count_in", "record_count_out",
                            "metrics", "error",
                        ]
                        stages_json = json.dumps([
                            {
                                c: (v.isoformat() if hasattr(v, "isoformat") else v)
                                for c, v in zip(cols, row)
                            }
                            for row in rows
                        ])
                except Exception:
                    pass  # stages remain None — non-fatal

            control.record_run_event(
                conn,
                run_id=payload["run_id"],
                event_type=payload["event_type"],
                pipeline_type=payload["pipeline_type"],
                domain=payload["domain"],
                dataset=payload["dataset"],
                business_date=payload["business_date"],
                status=payload["status"],
                record_count_source=payload["record_count_source"],
                record_count_dq_pass=payload["record_count_dq_pass"],
                record_count_dq_fail=payload["record_count_dq_fail"],
                record_count_published=payload["record_count_published"],
                kafka_topic=payload["kafka_topic"],
                kafka_offset_end=payload["kafka_offset_end"],
                error_summary=payload["error_summary"],
                occurred_at=payload["occurred_at"],
                file_id=payload.get("file_id"),
                s3_raw_path=payload.get("s3_raw_path"),
                s3_curated_path=payload.get("s3_curated_path"),
                file_md5=payload.get("file_md5"),
                kafka_offset_start=payload.get("kafka_offset_start"),
                stages=stages_json,
                commit=False,
            )
    except Exception as exc:
        print(f"[ods_pipeline.events] WARNING: PG write failed: {exc}", file=sys.stderr)
