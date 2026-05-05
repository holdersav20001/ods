"""Direct API → Kafka api_pull runner.

Companion to ods_pipeline.ingest.api_pull. The file-pipeline shape
polls and archives to S3 JSONL, then triggers dag_ingest. The
direct-Kafka shape polls and publishes per-record Avro to the raw
Kafka topic; a Kafka Connect S3 sink writes the archive in parallel
and a JDBC sink writes Postgres rows. Same control-plane primitives
(run_log, stages, lineage_edge, reconciliation_log,
api_pull_watermark) — different downstream shape.

Public entrypoint: ``run_once`` performs one poll/publish cycle. The
Airflow dispatcher in ``dag_api_pull`` invokes it per scheduled tick;
a future long-running runtime can call it in a loop.
"""
from __future__ import annotations

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
]
