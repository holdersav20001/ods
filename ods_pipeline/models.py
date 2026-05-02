"""Canonical constants for the pipeline control-plane schema."""
from __future__ import annotations


class Stage:
    """Valid values for ``pipeline.run_stage_log.stage``."""

    RAW_READ        = "raw_read"         # ingestion: read CSV/file from S3 raw
    SCHEMA_VALIDATE = "schema_validate"  # validate columns against schema registry
    DQ_CHECK        = "dq_check"         # data quality rules evaluation
    CURATED_WRITE   = "curated_write"    # write Parquet to S3 curated
    CURATED_READ    = "curated_read"     # publish: read curated Parquet
    KAFKA_CONSUME   = "kafka_consume"    # canonicalize: bounded raw-topic consume
    CANONICAL_TRANSFORM = "canonical_transform"  # raw-shape -> canonical-shape
    KAFKA_PUBLISH   = "kafka_publish"    # produce Avro messages to Kafka topic
    RECON_T0        = "recon_t0"         # T0 offset reconciliation check
    RECON_T1        = "recon_t1"         # raw-topic -> canonical-topic reconciliation
    SINK_PG_WAIT    = "sink_pg_wait"     # wait for JDBC sink to consume offsets
    SINK_S3_WAIT    = "sink_s3_wait"     # wait for S3 sink to consume offsets
    FINALISE        = "finalise"         # DAG finalise: mark run succeeded

    @classmethod
    def all_values(cls) -> frozenset[str]:
        return frozenset(
            v for k, v in vars(cls).items()
            if not k.startswith("_") and isinstance(v, str)
        )


class StageEvent:
    """Valid values for ``pipeline.run_stage_log.event_type``.

    Filtering convention:
        terminal events  = stage_completed | stage_failed | stage_skipped | stage_warned
        in-progress      = stage_started
    """

    STARTED   = "stage_started"
    COMPLETED = "stage_completed"
    FAILED    = "stage_failed"
    SKIPPED   = "stage_skipped"
    WARNED    = "stage_warned"   # completed with warnings (e.g. DQ soft blocks)

    TERMINAL: frozenset[str] = frozenset(
        {"stage_completed", "stage_failed", "stage_skipped", "stage_warned"}
    )


class RunStatus:
    """Valid values for ``pipeline.run_log.status``."""

    RUNNING   = "running"
    SUCCEEDED = "succeeded"
    FAILED    = "failed"
    PARTIAL   = "partial"


#: Statuses that close a run (set ``ended_at``).
TERMINAL_STATUSES: frozenset[str] = frozenset(
    {RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.PARTIAL}
)

#: Fields that may be updated on ``pipeline.run_log``.
ALLOWED_RUN_FIELDS: frozenset[str] = frozenset({
    "status",
    "record_count_source",
    "record_count_dq_pass",
    "record_count_dq_fail",
    "record_count_published",
    "kafka_topic",
    "kafka_offset_start",
    "kafka_offset_end",
    "config_version_id",
    "schema_version_id",
    "parents",
    "error_summary",
    "file_id",
    "business_date",
})
