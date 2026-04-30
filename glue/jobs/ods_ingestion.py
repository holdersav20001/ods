# glue/jobs/ods_ingestion.py
"""
ODS Glue ingestion job — CSV (S3 Raw) → Parquet (S3 Curated).

Usage:
    spark-submit ods_ingestion.py \
        --run_id  <uuid> \
        --domain  insurance \
        --dataset policies \
        --s3_input_path s3://ods-raw-local/insurance/policies/date=20260417/policies_20260417.csv
"""

from __future__ import annotations

import argparse
import json
import os
import sys

import psycopg2
import requests
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from utils import (
    extract_business_date,
    get_file_state,
    load_dataset_config,
    set_file_state,
    upsert_run_header,
    update_run_fields,
    write_stage_row,
)
from dq import evaluate_dq_rules


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_pg_conn():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        dbname=os.environ.get("POSTGRES_DB", "ods_dev"),
        user=os.environ.get("POSTGRES_USER", "ods"),
        password=os.environ.get("POSTGRES_PASSWORD", "ods"),
    )


def _pg_dsn() -> str:
    return (
        f"host={os.environ.get('POSTGRES_HOST', 'localhost')} "
        f"port={os.environ.get('POSTGRES_PORT', '5432')} "
        f"dbname={os.environ.get('POSTGRES_DB', 'ods_dev')} "
        f"user={os.environ.get('POSTGRES_USER', 'ods')} "
        f"password={os.environ.get('POSTGRES_PASSWORD', 'ods')}"
    )


def _build_spark(dataset: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(f"ods_ingestion_{dataset}")
        .config("spark.hadoop.fs.s3a.endpoint",
                os.environ.get("LOCALSTACK_ENDPOINT", ""))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key",
                os.environ.get("AWS_ACCESS_KEY_ID", "test"))
        .config("spark.hadoop.fs.s3a.secret.key",
                os.environ.get("AWS_SECRET_ACCESS_KEY", "test"))
        .getOrCreate()
    )


def _validate_schema_against_registry(
    df_columns: list[str],
    schema_id: str,
    schema_version: str,
) -> tuple[bool, str]:
    """
    GET Schema Registry /subjects/{schema_id}/versions/{schema_version}.
    Returns (ok, error_message).
    Non-_ods_* fields listed in the Avro schema must all be present in df.
    """
    registry_url = os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
    url = f"{registry_url}/subjects/{schema_id}/versions/{schema_version}"
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code == 404:
            return True, ""  # schema not registered yet — pass through
        resp.raise_for_status()
    except Exception as exc:
        return False, f"Schema Registry request failed: {exc}"

    body = resp.json()
    # The registry response wraps the schema string in a 'schema' key.
    schema_str = body.get("schema", "{}")
    try:
        avro_schema = json.loads(schema_str)
    except json.JSONDecodeError as exc:
        return False, f"Could not parse Avro schema JSON: {exc}"

    avro_fields = [
        f["name"] if isinstance(f, dict) else f
        for f in avro_schema.get("fields", [])
    ]
    # Only require non-ODS system columns to be present in the DataFrame.
    required_fields = [f for f in avro_fields if not f.startswith("_ods_")]

    df_col_set = set(df_columns)
    missing = [f for f in required_fields if f not in df_col_set]
    if missing:
        return False, f"Schema validation failed — missing columns: {missing}"
    return True, ""


def _write_dlq(spark, failing_df, domain: str, dataset: str,
               business_date: str, run_id: str) -> None:
    env = os.environ.get("ENV", "local")
    dlq_bucket = f"ods-dlq-{env}"
    key = f"{domain}/{dataset}/date={business_date}/run_id={run_id}/failed.csv"
    dlq_path = f"s3a://{dlq_bucket}/{key}"
    failing_df.write.mode("overwrite").parquet(dlq_path)


# ---------------------------------------------------------------------------
# Main ingestion logic
# ---------------------------------------------------------------------------

def run(run_id: str, domain: str, dataset: str, s3_input_path: str) -> int:
    """Execute the ingestion pipeline. Returns process exit code."""

    pg = _get_pg_conn()

    # ------------------------------------------------------------------
    # Step 1 — Load dataset config + snapshot
    # ------------------------------------------------------------------
    config = load_dataset_config(pg, domain, dataset)
    config_snapshot = json.dumps(
        {k: v for k, v in config.items()},
        default=str,
    )
    config_version = config.get("version")

    # ------------------------------------------------------------------
    # Step 2 — Idempotency check
    # ------------------------------------------------------------------
    pg_dsn = _pg_dsn()
    current_state = get_file_state(pg, s3_input_path)
    if current_state == "completed":
        upsert_run_header(
            pg_dsn,
            run_id=run_id,
            pipeline_type="ingestion",
            domain=domain,
            dataset=dataset,
            business_date=None,
        )
        update_run_fields(
            pg_dsn, run_id,
            status="succeeded",
            error_summary="File already in completed state — skipping.",
        )
        write_stage_row(
            pg_dsn,
            run_id=run_id,
            stage="ingest",
            status="skipped",
            input_ref=s3_input_path,
            error="File already in completed state — skipping.",
        )
        pg.close()
        return 0

    # ------------------------------------------------------------------
    # Step 3 — Extract business_date from filename
    # ------------------------------------------------------------------
    filename = s3_input_path.split("/")[-1]
    business_date = extract_business_date(filename, config["filename_pattern"])
    business_date_str = business_date.strftime("%Y-%m-%d")

    # ------------------------------------------------------------------
    # Step 4 — Log started
    # ------------------------------------------------------------------
    try:
        config_version_id_val = int(config_version) if config_version is not None else None
    except (TypeError, ValueError):
        config_version_id_val = None
    upsert_run_header(
        pg_dsn,
        run_id=run_id,
        pipeline_type="ingestion",
        domain=domain,
        dataset=dataset,
        business_date=business_date_str,
        config_version_id=config_version_id_val,
    )
    write_stage_row(
        pg_dsn,
        run_id=run_id,
        stage="ingest",
        status="running",
        input_ref=s3_input_path,
    )

    spark = _build_spark(dataset)
    s3a_path = s3_input_path.replace("s3://", "s3a://")

    # ------------------------------------------------------------------
    # Step 5 — Read CSV from S3
    # ------------------------------------------------------------------
    df = (
        spark.read
        .option("inferSchema", "true")
        .option("header", "true")
        .csv(s3a_path)
    )

    # ------------------------------------------------------------------
    # Step 6 — Log file_read
    # ------------------------------------------------------------------
    source_count = df.count()
    update_run_fields(pg_dsn, run_id, record_count_source=source_count)

    # ------------------------------------------------------------------
    # Step 7 — Schema validation via Schema Registry
    # ------------------------------------------------------------------
    schema_id = str(config["schema_id"])
    schema_version = str(config["schema_version"])
    ok, err_msg = _validate_schema_against_registry(
        df.columns, schema_id, schema_version
    )
    if not ok:
        update_run_fields(pg_dsn, run_id, status="failed", error_summary=err_msg)
        write_stage_row(
            pg_dsn,
            run_id=run_id,
            stage="ingest",
            status="failed",
            input_ref=s3_input_path,
            error=err_msg,
        )
        set_file_state(pg, s3_input_path, run_id, "failed", error_reason=err_msg)
        pg.close()
        spark.stop()
        return 1

    # Step 8 — schema_validated (no separate log row needed; progress tracked via run_log)

    # ------------------------------------------------------------------
    # Step 9 — Run DQ rules
    # ------------------------------------------------------------------
    dq_rules = (
        config["dq_rules"]
        if isinstance(config["dq_rules"], dict)
        else json.loads(config["dq_rules"])
    )
    passing_df, failing_df, warnings = evaluate_dq_rules(
        df, dq_rules, total_count=source_count
    )
    failing_count = failing_df.count()

    # ------------------------------------------------------------------
    # Step 10 — Write failing rows to DLQ (if any)
    # ------------------------------------------------------------------
    if failing_count > 0:
        _write_dlq(spark, failing_df, domain, dataset, business_date_str, run_id)

    # ------------------------------------------------------------------
    # Step 11 — Log DQ counts
    # ------------------------------------------------------------------
    dq_pass_count = source_count - failing_count
    update_run_fields(
        pg_dsn, run_id,
        record_count_dq_pass=dq_pass_count,
        record_count_dq_fail=failing_count,
    )

    # ------------------------------------------------------------------
    # Step 12 — Add ODS system columns to passing_df
    # ------------------------------------------------------------------
    passing_df = (
        passing_df
        .withColumn("_ods_business_date", F.lit(business_date_str))
        .withColumn("_ods_run_id", F.lit(run_id))
    )

    # ------------------------------------------------------------------
    # Step 13 — Write Parquet to S3 Curated
    # ------------------------------------------------------------------
    env = os.environ.get("ENV", "local")
    curated_path = (
        f"s3a://ods-curated-{env}/{domain}/{dataset}"
        f"/date={business_date_str}/"
    )
    passing_df.write.mode("overwrite").parquet(curated_path)

    # Step 14 — parquet written; output_ref recorded in stage row at completion

    # ------------------------------------------------------------------
    # Step 15 — Count verification
    # ------------------------------------------------------------------
    written_count = passing_df.count()
    expected_count = source_count - failing_count
    if written_count != expected_count:
        err = (
            f"Count mismatch: written={written_count}, "
            f"expected={expected_count} "
            f"(source={source_count} - failed={failing_count})"
        )
        update_run_fields(pg_dsn, run_id, status="failed", error_summary=err)
        write_stage_row(
            pg_dsn,
            run_id=run_id,
            stage="ingest",
            status="failed",
            input_ref=s3_input_path,
            record_count_in=source_count,
            error=err,
        )
        set_file_state(pg, s3_input_path, run_id, "failed", error_reason=err)
        pg.close()
        spark.stop()
        return 1

    # ------------------------------------------------------------------
    # Steps 16 & 17 — count_verified + completed
    # ------------------------------------------------------------------
    curated_output_ref = curated_path.replace("s3a://", "s3://")
    write_stage_row(
        pg_dsn,
        run_id=run_id,
        stage="ingest",
        status="succeeded",
        input_ref=s3_input_path,
        output_ref=curated_output_ref,
        record_count_in=source_count,
        record_count_out=written_count,
    )
    update_run_fields(
        pg_dsn, run_id,
        status="succeeded",
        record_count_source=source_count,
        record_count_dq_pass=written_count,
        record_count_dq_fail=failing_count,
    )

    # ------------------------------------------------------------------
    # Step 18 — Set file_state = completed
    # ------------------------------------------------------------------
    set_file_state(
        pg, s3_input_path, run_id, "completed",
        record_count=written_count,
    )

    pg.close()
    spark.stop()
    return 0


# ---------------------------------------------------------------------------
# CLI entrypoint
# ---------------------------------------------------------------------------

def _parse_args(argv=None):
    parser = argparse.ArgumentParser(description="ODS CSV → Parquet ingestion job")
    parser.add_argument("--run_id", required=True, help="Unique run identifier (UUID)")
    parser.add_argument("--domain", required=True, help="Data domain, e.g. insurance")
    parser.add_argument("--dataset", required=True, help="Dataset name, e.g. policies")
    parser.add_argument("--s3_input_path", required=True,
                        help="S3 path to input CSV, e.g. s3://ods-raw-local/...")
    return parser.parse_args(argv)


if __name__ == "__main__":
    args = _parse_args()
    sys.exit(
        run(
            run_id=args.run_id,
            domain=args.domain,
            dataset=args.dataset,
            s3_input_path=args.s3_input_path,
        )
    )
