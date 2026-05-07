"""Direct API → Kafka api_pull runner.

Companion to ods_pipeline.ingest.api_pull. The file-pipeline shape
polls and archives to S3 JSONL, then triggers dag_ingest. The
direct-Kafka shape polls and publishes per-record Avro to the raw
Kafka topic; a Kafka Connect S3 sink writes the archive in parallel
and a JDBC sink writes Postgres rows. Same control-plane primitives
(run_log, stages, lineage_edge, reconciliation_log,
api_pull_watermark) — different downstream shape.

Public entrypoints:
  * ``run_once`` performs one poll/publish cycle. The Airflow
    dispatcher in ``dag_api_pull`` invokes it per scheduled tick.
  * ``run_loop`` wraps ``run_once`` in a long-running, stop-event
    aware poller for sub-tick latency datasets. See
    ``dag_api_pull_kafka_continuous`` for the Airflow surface.
"""
from __future__ import annotations

from ods_pipeline.ingest.api_pull_kafka.loop import run_loop
from ods_pipeline.ingest.api_pull_kafka.runner import (
    PublishedBatch,
    build_avro_producer,
    build_envelope,
    run_once,
)

__all__ = [
    "PublishedBatch",
    "build_avro_producer",
    "build_envelope",
    "run_once",
    "run_loop",
]
