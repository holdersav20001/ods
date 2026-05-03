"""File-based ingestion patterns (risk, policies).

Both flow: SFTP -> S3 raw -> Glue ingestion (CSV -> Parquet, DQ) ->
publish raw Kafka -> canonicalize -> publish canonical Kafka -> JDBC
sink(s) -> recon.

These declarations replace the implicit "Glue + DAG knows what to do"
convention with explicit metadata that DAG factory + Glue entrypoint
can consume.
"""
from __future__ import annotations

from ods_pipeline.models import PatternType, Stage
from ods_pipeline.patterns.base import IngestionPattern, register


_FILE_STAGES = (
    Stage.RAW_READ,
    Stage.SCHEMA_VALIDATE,
    Stage.DQ_CHECK,
    Stage.CURATED_WRITE,
    Stage.CURATED_READ,
    Stage.KAFKA_PUBLISH,
    Stage.KAFKA_CONSUME,
    Stage.CANONICAL_TRANSFORM,
    Stage.RECON_T0,
    Stage.RECON_T1,
    Stage.SINK_PG_WAIT,
    Stage.FINALISE,
)


POLICIES = register(IngestionPattern(
    name="insurance.policies",
    pattern_type=PatternType.FILE,
    stages=_FILE_STAGES,
    topics=("ods.insurance.policies",),
    sinks=("jdbc-sink-policies", "jdbc-sink-policy-history"),
    recon_checks=("t0_publish_count", "t1_canonical_count", "dual_sink_parity"),
    yaml_config="patterns/insurance/policies.yaml",
))


RISK = register(IngestionPattern(
    name="insurance.risk",
    pattern_type=PatternType.FILE,
    stages=_FILE_STAGES,
    topics=("ods.insurance.risk", "ods.insurance.risk-canonical"),
    sinks=("jdbc-sink-risk",),
    recon_checks=("t0_publish_count", "t1_canonical_count"),
    yaml_config="patterns/insurance/risk.yaml",
))
