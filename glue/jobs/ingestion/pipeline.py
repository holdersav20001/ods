"""Ingestion pipeline orchestrator — S3 raw → S3 curated.

This module is the modular replacement for the 636-line
``ods_ingestion._run_impl`` monolith. Each ingestion step lives in its
own sibling module; this file just composes them in canonical order
and wraps each in :func:`ods_pipeline.stages.stage_scope` so failure
rows are written automatically.

Pipeline shape::

    bootstrap → already_completed? → register file
    → runs.start
    → stage_scope(RAW_READ)            reading.read_raw
    → stage_scope(SCHEMA_VALIDATE)     validation.validate_columns
    → stage_scope(DQ_CHECK)            quality.evaluate
    → enrich (no stage row — pure transform)
    → stage_scope(CURATED_WRITE)       curating.write_and_verify
    → finalising.finalise_success | finalise_failure

Every stage_scope failure auto-writes ``stage_failed`` and re-raises;
the outer ``try`` catches and routes through ``finalise_failure``.
"""
from __future__ import annotations

import json
import os
import sys
from typing import Any

import ods_pipeline
from ods_pipeline.stages import stage_scope

Stage = ods_pipeline.Stage
StageEvent = ods_pipeline.StageEvent

# Late imports so unit tests of submodules don't trigger Spark imports.
_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)


def run(
    *,
    run_id: str,
    domain: str,
    dataset: str,
    s3_input_path: str,
    file_id: str | None = None,
    upstream_run_id: str | None = None,
    airflow_dag_id: str | None = None,
    airflow_run_id: str | None = None,
) -> int:
    """Execute the ingestion pipeline. Returns process exit code."""
    conn = ods_pipeline.connect()
    try:
        return _run(
            conn,
            run_id=run_id,
            domain=domain,
            dataset=dataset,
            s3_input_path=s3_input_path,
            file_id=file_id,
            upstream_run_id=upstream_run_id,
            airflow_dag_id=airflow_dag_id,
            airflow_run_id=airflow_run_id,
        )
    finally:
        conn.close()


def _run(
    conn: Any,
    *,
    run_id: str,
    domain: str,
    dataset: str,
    s3_input_path: str,
    file_id: str | None,
    upstream_run_id: str | None,
    airflow_dag_id: str | None,
    airflow_run_id: str | None,
) -> int:
    # Late imports keep the unit-test path Spark-free for submodules
    # while letting the orchestrator pull everything in one place.
    from utils import load_dataset_config  # noqa: WPS433

    from . import (  # noqa: WPS433
        curating,
        finalising,
        quality,
        reading,
        registration,
        validation,
    )
    from .spark import build_spark  # noqa: WPS433

    config = load_dataset_config(conn, domain, dataset)
    raw_format = (config.get("raw_format") or "csv").lower()

    # ------------------------------------------------------------------
    # Idempotency short-circuit — file already curated and completed.
    # ------------------------------------------------------------------
    if registration.already_completed(conn, s3_input_path):
        ods_pipeline.runs.start(
            conn,
            run_id=run_id,
            pipeline_type="ingestion",
            domain=domain,
            dataset=dataset,
            business_date=None,
            orchestrators=(
                [{"run_id": upstream_run_id, "edge_type": "orchestrates"}]
                if upstream_run_id else None
            ),
        )
        ods_pipeline.runs.update(
            conn, run_id,
            status="succeeded",
            error_summary="File already in completed state — skipping.",
        )
        ods_pipeline.stages.write(
            conn, run_id=run_id, stage=Stage.RAW_READ,
            event_type=StageEvent.SKIPPED, status="skipped",
            input_ref=s3_input_path,
            error="File already in completed state — skipping.",
            airflow_dag_id=airflow_dag_id,
            airflow_run_id=airflow_run_id,
        )
        return 0

    # ------------------------------------------------------------------
    # Bootstrap — business_date + file_catalogue + run row.
    # ------------------------------------------------------------------
    business_date = reading.resolve_business_date(
        conn, config=config, s3_input_path=s3_input_path, file_id=file_id,
    )
    file_id, md5, _size = registration.register(
        conn,
        run_id=run_id,
        domain=domain,
        dataset=dataset,
        business_date=business_date,
        s3_input_path=s3_input_path,
        file_id=file_id,
    )

    config_version_id = _coerce_int(config.get("version"))
    ods_pipeline.runs.start(
        conn,
        run_id=run_id,
        pipeline_type="ingestion",
        domain=domain,
        dataset=dataset,
        business_date=business_date,
        config_version_id=config_version_id,
        file_id=file_id,
        orchestrators=(
            [{"run_id": upstream_run_id, "edge_type": "orchestrates"}]
            if upstream_run_id else None
        ),
    )

    spark = build_spark(dataset)
    spark_app_id = spark.sparkContext.applicationId
    common = dict(
        airflow_dag_id=airflow_dag_id,
        airflow_run_id=airflow_run_id,
        spark_app_id=spark_app_id,
    )

    # State threaded between stages — only what the next step needs.
    df: Any = None
    source_count = 0
    failing_count = 0
    written_count = 0
    curated_uri: str | None = None
    passing_df: Any = None

    try:
        # ── stage 1: raw_read ────────────────────────────────────────
        with stage_scope(
            conn, run_id=run_id, stage=Stage.RAW_READ,
            input_ref=s3_input_path, **common,
        ) as s:
            df = reading.read_raw(spark, s3_input_path, raw_format)
            source_count = df.count()
            ods_pipeline.runs.update(
                conn, run_id, record_count_source=source_count,
            )
            s.set_result(record_count_out=source_count)

        # ── stage 2: schema_validate ────────────────────────────────
        with stage_scope(
            conn, run_id=run_id, stage=Stage.SCHEMA_VALIDATE,
            input_ref=s3_input_path, **common,
        ) as s:
            metrics = validation.validate_columns(
                df_columns=df.columns,
                schema_id=str(config["schema_id"]),
                schema_version=str(config["schema_version"]),
            )
            s.set_result(metrics=metrics)

        # ── stage 3: dq_check ────────────────────────────────────────
        with stage_scope(
            conn, run_id=run_id, stage=Stage.DQ_CHECK,
            input_ref=s3_input_path,
            record_count_in=source_count,
            **common,
        ) as s:
            outcome = quality.evaluate(
                df,
                config_dq_rules=config["dq_rules"],
                source_count=source_count,
                domain=domain,
                dataset=dataset,
                business_date=business_date,
                run_id=run_id,
            )
            failing_count = outcome.failing_count
            passing_df = outcome.passing_df
            dq_pass = source_count - failing_count
            ods_pipeline.runs.update(
                conn, run_id,
                record_count_dq_pass=dq_pass,
                record_count_dq_fail=failing_count,
            )
            s.set_result(
                record_count_out=dq_pass,
                metrics={
                    "failing_count": failing_count,
                    "warnings": outcome.warnings,
                },
            )
            if outcome.warnings or failing_count > 0:
                # Soft failure — work succeeded but signals fired.
                # ``s.warn`` records ``status='warned'`` + reason on
                # the row's error column.
                s.warn(json.dumps(outcome.warnings) if outcome.warnings else None)

        # ── enrich — pure transform, no stage row ────────────────────
        passing_df = curating.enrich_with_metadata(
            passing_df,
            file_id=str(file_id),
            run_id=run_id,
            domain=domain,
            dataset=dataset,
            business_date=business_date,
        )

        # ── stage 4: curated_write ───────────────────────────────────
        with stage_scope(
            conn, run_id=run_id, stage=Stage.CURATED_WRITE,
            input_ref=s3_input_path,
            record_count_in=source_count,
            **common,
        ) as s:
            curated_uri, written_count = curating.write_and_verify(
                passing_df,
                domain=domain,
                dataset=dataset,
                business_date=business_date,
                expected_count=source_count - failing_count,
            )
            s.set_result(
                output_ref=curated_uri,
                record_count_out=written_count,
            )

        # ── finalise success ────────────────────────────────────────
        finalising.finalise_success(
            conn,
            run_id=run_id,
            domain=domain,
            dataset=dataset,
            business_date=business_date,
            file_id=str(file_id),
            file_md5=md5,
            s3_input_path=s3_input_path,
            curated_uri=curated_uri,
            source_count=source_count,
            written_count=written_count,
            failing_count=failing_count,
        )
        return 0

    except Exception as exc:
        if isinstance(exc, quality.DQAllRowsFailed):
            failing_count = exc.failing_count
            source_count = exc.source_count
        finalising.finalise_failure(
            conn,
            run_id=run_id,
            domain=domain,
            dataset=dataset,
            business_date=business_date,
            file_id=str(file_id),
            file_md5=md5,
            s3_input_path=s3_input_path,
            error_summary=str(exc)[:500] or type(exc).__name__,
            source_count=source_count,
            dq_pass_count=source_count - failing_count if source_count else None,
            failing_count=failing_count or None,
        )
        return 1
    finally:
        try:
            spark.stop()
        except Exception:
            pass


def _coerce_int(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None
