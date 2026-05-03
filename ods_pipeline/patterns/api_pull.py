"""API pull ingestion pattern.

Airflow polls an external HTTP API on a schedule, archives the response
as gzipped JSONL to S3, registers the archive in pipeline.file_catalogue,
and triggers the existing dag_ingest pipeline (raw_read -> Kafka publish
-> canonicalize -> JDBC sink). Cursor commit follows a two-phase
pending/committed split so downstream failure does not lose data.

The registration mirrors the file/event patterns: a declarative
IngestionPattern is the single source of truth for stages, topics,
sinks, recon checks and YAML config.
"""
from __future__ import annotations

from ods_pipeline.models import PatternType, Stage
from ods_pipeline.patterns.base import IngestionPattern, register


# Stages emitted by the api_pull control-plane (poller + dag_api_pull).
# Downstream stages (raw_read, schema_validate, ...) are emitted by the
# triggered dag_ingest run and are not duplicated here.
_API_PULL_STAGES = (
    Stage.RAW_POLL,
    Stage.MESSAGE_ARCHIVE,
    Stage.RECON_MESSAGE,
    Stage.FINALISE,
)


API_PULL_DEMO = register(IngestionPattern(
    name="insurance.api_pull_demo",
    pattern_type=PatternType.API,
    stages=_API_PULL_STAGES,
    topics=(
        "ods.insurance.api_pull_demo",
        "ods.insurance.api_pull_demo.canonical",
    ),
    sinks=("jdbc-sink-api-pull-demo",),
    recon_checks=("api_pull_archive_count",),
    yaml_config="patterns/insurance/api_pull_demo.yaml",
))
