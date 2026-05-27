"""Canonical constants for the pipeline control-plane schema."""
from __future__ import annotations


class PatternType:
    """Ingestion pattern types. Each has its own correlation field.

    Used by ``ods_pipeline.messages.correlate`` and the future
    ``ods_pipeline.patterns.IngestionPattern`` registry (T12).
    """

    FILE  = "file"   # raw file ingestion -> correlate by _ods_file_id
    CDC   = "cdc"    # change data capture -> correlate by _ods_change_lsn
    API   = "api"    # synchronous request  -> correlate by _ods_source_request_id
    EVENT = "event"  # async event stream   -> correlate by _ods_source_event_id

    ALL: frozenset[str] = frozenset({"file", "cdc", "api", "event"})


#: Maps pattern type to the canonical correlation field on the message envelope.
PATTERN_CORRELATION_FIELD: dict[str, str] = {
    PatternType.FILE:  "_ods_file_id",
    PatternType.CDC:   "_ods_change_lsn",
    PatternType.API:   "_ods_source_request_id",
    PatternType.EVENT: "_ods_source_event_id",
}


class Stage:
    """Valid values for ``pipeline.run_stage_log.stage``."""

    RAW_READ        = "raw_read"         # ingestion: read CSV/file from S3 raw
    RAW_POLL        = "raw_poll"         # api_pull: HTTP poll source API for records
    SCHEMA_VALIDATE = "schema_validate"  # validate columns against schema registry
    DQ_CHECK        = "dq_check"         # data quality rules evaluation
    CURATED_WRITE   = "curated_write"    # write Parquet to S3 curated
    CURATED_READ    = "curated_read"     # publish: read curated Parquet
    KAFKA_CONSUME   = "kafka_consume"    # canonicalize: bounded raw-topic consume
    CANONICAL_TRANSFORM = "canonical_transform"  # raw-shape -> canonical-shape
    KAFKA_PUBLISH   = "kafka_publish"    # produce Avro messages to Kafka topic
    MESSAGE_RECEIVE = "message_receive"  # message/API: receive request/batch/window
    MESSAGE_VALIDATE = "message_validate"  # message/API: validate source events
    MESSAGE_ARCHIVE = "message_archive"  # message/API: write S3 archive envelopes
    DLQ_WRITE       = "dlq_write"         # write failed records/events to DLQ
    RECON_T0        = "recon_t0"         # T0 offset reconciliation check
    RECON_T1        = "recon_t1"         # raw-topic -> canonical-topic reconciliation
    RECON_MESSAGE   = "recon_message"    # message/API count reconciliation
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
    HEARTBEAT = "stage_heartbeat"  # long-running stage liveness ping (R8)

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

#: Fields that may be inserted into ``pipeline.glue_job_log`` by
#: ``glue.jobs.utils.write_job_log``. Any caller-supplied key outside this set
#: is rejected before SQL composition to prevent identifier injection.
ALLOWED_JOB_LOG_FIELDS: frozenset[str] = frozenset({
    "run_id",
    "job_name",
    "pipeline_type",
    "domain",
    "dataset",
    "source_path",
    "target_path",
    "business_date",
    "status",
    "record_count",
    "error_reason",
    "error_detail",
    "config_version",
    "config_snapshot",
})


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
    "orchestrators",
    "runtime_context",
    "error_summary",
    "file_id",
    "business_date",
})
