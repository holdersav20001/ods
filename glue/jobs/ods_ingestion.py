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

# Add repo root to sys.path so ods_pipeline package is importable from Glue
_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

import ods_pipeline
import requests
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from utils import (
    extract_business_date,
    load_dataset_config,
)
from dq import evaluate_dq_rules

Stage = ods_pipeline.Stage
StageEvent = ods_pipeline.StageEvent


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


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

def run(run_id: str, domain: str, dataset: str, s3_input_path: str,
        file_id: str | None = None,
        parent_run_id: str | None = None,
        airflow_dag_id: str | None = None,
        airflow_run_id: str | None = None) -> int:
    """Execute the ingestion pipeline. Returns process exit code."""

    conn = ods_pipeline.connect()

    try:
        return _run_impl(
            conn, run_id, domain, dataset, s3_input_path,
            file_id=file_id,
            parent_run_id=parent_run_id,
            airflow_dag_id=airflow_dag_id,
            airflow_run_id=airflow_run_id,
        )
    finally:
        conn.close()


def _run_impl(conn, run_id: str, domain: str, dataset: str, s3_input_path: str,
              file_id: str | None = None,
              parent_run_id: str | None = None,
              airflow_dag_id: str | None = None,
              airflow_run_id: str | None = None) -> int:
    """Execute the ingestion pipeline. Returns process exit code."""

    # ------------------------------------------------------------------
    # Step 1 — Load dataset config + snapshot
    # ------------------------------------------------------------------
    config = load_dataset_config(conn, domain, dataset)
    config_snapshot = json.dumps(
        {k: v for k, v in config.items()},
        default=str,
    )
    config_version = config.get("version")

    # ------------------------------------------------------------------
    # Step 2 — Idempotency check
    # ------------------------------------------------------------------
    # spark_app_id starts None; updated once Spark is running.
    # _ws closes over it by reference so post-Spark calls see the real value.
    spark_app_id: str | None = None

    def _ws(**kw):
        ods_pipeline.stages.write(conn, run_id=run_id,
                                  airflow_dag_id=airflow_dag_id,
                                  airflow_run_id=airflow_run_id,
                                  spark_app_id=spark_app_id, **kw)

    current_state = ods_pipeline.files.get_state(conn, s3_input_path)
    if current_state == "completed":
        ods_pipeline.runs.start(
            conn,
            run_id=run_id,
            pipeline_type="ingestion",
            domain=domain,
            dataset=dataset,
            business_date=None,
            parents=[{"run_id": parent_run_id, "edge_type": "orchestrates"}]
            if parent_run_id else None,
        )
        ods_pipeline.runs.update(
            conn, run_id,
            status="succeeded",
            error_summary="File already in completed state — skipping.",
        )
        _ws(
            stage=Stage.RAW_READ,
            event_type=StageEvent.SKIPPED,
            status="skipped",
            input_ref=s3_input_path,
            error="File already in completed state — skipping.",
        )
        return 0

    # ------------------------------------------------------------------
    # Step 3 — Resolve business_date
    # ------------------------------------------------------------------
    # CSV / file pattern: extract from filename via configured regex.
    # JSONL / api_pull pattern: filename has no business_date — fall back
    # to the file_catalogue row already registered by dag_api_pull.
    raw_format = (config.get("raw_format") or "csv").lower()
    if raw_format == "csv":
        filename = s3_input_path.split("/")[-1]
        business_date = extract_business_date(filename, config["filename_pattern"])
        business_date_str = business_date.strftime("%Y-%m-%d")
    elif raw_format == "jsonl":
        with conn.cursor() as _bd_cur:
            _bd_cur.execute(
                "SELECT business_date::text FROM pipeline.file_catalogue "
                "WHERE s3_raw_path=%s OR file_id::text=%s "
                "ORDER BY id DESC LIMIT 1",
                (s3_input_path, file_id or ""),
            )
            _bd_row = _bd_cur.fetchone()
        if not _bd_row or not _bd_row[0]:
            raise ValueError(
                f"jsonl ingestion requires file_catalogue.business_date for "
                f"{s3_input_path!r} (file_id={file_id})"
            )
        business_date_str = _bd_row[0]
    else:
        raise ValueError(
            f"unsupported raw_format={raw_format!r}; expected 'csv' or 'jsonl'"
        )

    # ------------------------------------------------------------------
    # Step 3b — Register file in file_catalogue (upsert on MD5)
    # ------------------------------------------------------------------
    import boto3 as _boto3, hashlib as _hashlib
    _s3_client = _boto3.client(
        "s3",
        endpoint_url=os.environ.get("LOCALSTACK_ENDPOINT") or os.environ.get("S3_ENDPOINT"),
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
        region_name=os.environ.get("AWS_DEFAULT_REGION", "eu-west-1"),
    )
    _bucket, _key = s3_input_path.replace("s3://", "").split("/", 1)
    _head = _s3_client.head_object(Bucket=_bucket, Key=_key)
    _etag = _head.get("ETag", "").strip('"').replace("-", "")  # ETag = MD5 for non-multipart
    _size = _head.get("ContentLength")
    if not _etag or len(_etag) != 32:
        # Fallback: stream and compute MD5
        _obj = _s3_client.get_object(Bucket=_bucket, Key=_key)
        _md5 = _hashlib.md5(_obj["Body"].read()).hexdigest()
    else:
        _md5 = _etag

    if file_id:
        # Explicit file_id from DAG — update existing catalogue record
        with conn.cursor() as _cur:
            _cur.execute(
                "UPDATE pipeline.file_catalogue "
                "SET state='ingesting', last_run_id=%s, state_updated_at=NOW() "
                "WHERE file_id=%s",
                (run_id, file_id),
            )
        conn.commit()
        _file_id = file_id
    else:
        # No explicit file_id — upsert keyed on MD5 (backward compat)
        _file_id = ods_pipeline.files.upsert(
            conn,
            domain=domain,
            dataset=dataset,
            business_date=business_date_str,
            file_md5=_md5,
            s3_raw_path=s3_input_path,
            file_size_bytes=_size,
            state="ingesting",
            last_run_id=run_id,
        )

    # ------------------------------------------------------------------
    # Step 4 — Log started
    # ------------------------------------------------------------------
    try:
        config_version_id_val = int(config_version) if config_version is not None else None
    except (TypeError, ValueError):
        config_version_id_val = None
    ods_pipeline.runs.start(
        conn,
        run_id=run_id,
        pipeline_type="ingestion",
        domain=domain,
        dataset=dataset,
        business_date=business_date_str,
        config_version_id=config_version_id_val,
        file_id=_file_id,
        parents=[{"run_id": parent_run_id, "edge_type": "orchestrates"}]
        if parent_run_id else None,
    )
    ods_pipeline.stages.start(
        conn,
        run_id=run_id,
        stage=Stage.RAW_READ,
        input_ref=s3_input_path,
        airflow_dag_id=airflow_dag_id,
        airflow_run_id=airflow_run_id,
        spark_app_id=spark_app_id,
    )

    spark = _build_spark(dataset)
    spark_app_id = spark.sparkContext.applicationId  # updates the cell _ws closes over

    s3a_path = s3_input_path.replace("s3://", "s3a://")

    # ------------------------------------------------------------------
    # Step 5 — Read raw object from S3 (csv | jsonl)
    # ------------------------------------------------------------------
    # Spark transparently decompresses .gz on read; api_pull archives
    # land as ``.jsonl.gz`` and are read by spark.read.json. The JSONL
    # records carry the standard ODS metadata envelope written by
    # ods_pipeline.ingest.api_pull.archive.write_jsonl_archive.
    if raw_format == "jsonl":
        df = spark.read.json(s3a_path)
    else:
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
    ods_pipeline.runs.update(conn, run_id, record_count_source=source_count)
    ods_pipeline.stages.finish(
        conn,
        run_id=run_id,
        stage=Stage.RAW_READ,
        status="succeeded",
        event_type=StageEvent.COMPLETED,
        input_ref=s3_input_path,
        record_count_out=source_count,
        airflow_dag_id=airflow_dag_id,
        airflow_run_id=airflow_run_id,
        spark_app_id=spark_app_id,
    )

    # ------------------------------------------------------------------
    # Step 7 — Schema validation via Schema Registry
    # ------------------------------------------------------------------
    schema_id = str(config["schema_id"])
    schema_version = str(config["schema_version"])
    ok, err_msg = _validate_schema_against_registry(
        df.columns, schema_id, schema_version
    )
    if not ok:
        ods_pipeline.runs.update(conn, run_id, status="failed", error_summary=err_msg)
        _ws(
            stage=Stage.SCHEMA_VALIDATE,
            event_type=StageEvent.FAILED,
            status="failed",
            input_ref=s3_input_path,
            error=err_msg,
        )
        ods_pipeline.files.set_state(conn, s3_input_path, run_id, "failed", error_reason=err_msg)
        ods_pipeline.events.produce(
            "ingestion.completed", run_id, domain, dataset,
            business_date_str, "failed",
            pipeline_type="ingestion",
            file_id=_file_id, file_md5=_md5, s3_raw_path=s3_input_path,
            error_summary=err_msg,
        )
        spark.stop()
        return 1

    # Step 8 — schema validated successfully
    _ws(
        stage=Stage.SCHEMA_VALIDATE,
        event_type=StageEvent.COMPLETED,
        status="succeeded",
        input_ref=s3_input_path,
        metrics={"schema_id": schema_id, "schema_version": schema_version},
    )

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
    ods_pipeline.runs.update(
        conn, run_id,
        record_count_dq_pass=dq_pass_count,
        record_count_dq_fail=failing_count,
    )
    all_rows_failed_dq = source_count > 0 and dq_pass_count == 0 and failing_count > 0
    _dq_has_issues = bool(warnings or failing_count > 0)
    dq_status = (
        "failed" if all_rows_failed_dq
        else "warned" if _dq_has_issues
        else "succeeded"
    )
    _ws(
        stage=Stage.DQ_CHECK,
        event_type=(
            StageEvent.FAILED if all_rows_failed_dq
            else StageEvent.WARNED if _dq_has_issues
            else StageEvent.COMPLETED
        ),
        status=dq_status,
        input_ref=s3_input_path,
        record_count_in=source_count,
        record_count_out=dq_pass_count,
        metrics={"failing_count": failing_count, "warnings": warnings or []},
        error=json.dumps(warnings) if warnings else None,
    )

    if all_rows_failed_dq:
        err = "All rows failed DQ — nothing curated."
        ods_pipeline.runs.update(conn, run_id, status="failed", error_summary=err)
        ods_pipeline.files.set_state(conn, s3_input_path, run_id, "failed", error_reason=err)
        ods_pipeline.events.produce(
            "ingestion.completed", run_id, domain, dataset,
            business_date_str, "failed",
            pipeline_type="ingestion",
            record_count_source=source_count,
            record_count_dq_pass=dq_pass_count,
            record_count_dq_fail=failing_count,
            error_summary=err,
            file_id=_file_id,
            file_md5=_md5,
            s3_raw_path=s3_input_path,
        )
        spark.stop()
        return 1

    # ------------------------------------------------------------------
    # Step 12 — Add ODS system columns to passing_df
    # ------------------------------------------------------------------
    source_application = os.environ.get("ODS_SOURCE_APPLICATION", "sftp")
    file_meta = ods_pipeline.metadata.file_metadata(
        file_id=str(_file_id),
        run_id=run_id,
        domain=domain,
        dataset=dataset,
        business_date=business_date_str,
        source_application=source_application,
    )
    passing_df = passing_df
    for _field, _value in file_meta.items():
        passing_df = passing_df.withColumn(_field, F.lit(_value))

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
        ods_pipeline.runs.update(conn, run_id, status="failed", error_summary=err)
        _ws(
            stage=Stage.CURATED_WRITE,
            event_type=StageEvent.FAILED,
            status="failed",
            input_ref=s3_input_path,
            record_count_in=source_count,
            error=err,
        )
        ods_pipeline.files.set_state(conn, s3_input_path, run_id, "failed", error_reason=err)
        ods_pipeline.events.produce(
            "ingestion.completed", run_id, domain, dataset,
            business_date_str, "failed",
            pipeline_type="ingestion",
            record_count_source=source_count,
            record_count_dq_pass=dq_pass_count,
            record_count_dq_fail=failing_count,
            error_summary=err,
            file_id=_file_id,
            file_md5=_md5,
            s3_raw_path=s3_input_path,
        )
        spark.stop()
        return 1

    # ------------------------------------------------------------------
    # Steps 16 & 17 — count_verified + completed
    # ------------------------------------------------------------------
    curated_output_ref = curated_path.replace("s3a://", "s3://")

    # Back-fill curated path + final state into file_catalogue
    with conn.cursor() as _cur:
        _cur.execute(
            "UPDATE pipeline.file_catalogue "
            "SET s3_curated_path=%s, state='curated', state_updated_at=NOW() "
            "WHERE file_id=%s",
            (curated_output_ref, _file_id),
        )
    conn.commit()

    # Write raw→curated lineage edge
    ods_pipeline.lineage.write_edge(
        conn,
        child_run_id=run_id,
        parent_file_id=_file_id,
        edge_type="raw_to_curated",
        source_ref=s3_input_path,
        target_ref=curated_output_ref,
        record_count=written_count,
    )

    _ws(
        stage=Stage.CURATED_WRITE,
        event_type=StageEvent.COMPLETED,
        status="succeeded",
        input_ref=s3_input_path,
        output_ref=curated_output_ref,
        record_count_in=source_count,
        record_count_out=written_count,
    )
    ods_pipeline.runs.update(
        conn, run_id,
        status="succeeded",
        record_count_source=source_count,
        record_count_dq_pass=written_count,
        record_count_dq_fail=failing_count,
    )

    # ------------------------------------------------------------------
    # Step 18 — Set file_state = completed
    # ------------------------------------------------------------------
    ods_pipeline.files.set_state(
        conn, s3_input_path, run_id, "completed",
        record_count=written_count,
    )

    ods_pipeline.events.produce(
        "ingestion.completed", run_id, domain, dataset,
        business_date_str, "succeeded",
        pipeline_type="ingestion",
        file_id=_file_id,
        s3_raw_path=s3_input_path,
        s3_curated_path=curated_output_ref,
        file_md5=_md5,
        record_count_source=source_count,
        record_count_dq_pass=written_count,
        record_count_dq_fail=failing_count,
    )

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
    parser.add_argument("--file_id", default=None,
                        help="Explicit file_id UUID from file_catalogue (passed by DAG)")
    parser.add_argument("--parent_run_id", default=None,
                        help="Optional s3_batch parent run id for run hierarchy")
    parser.add_argument("--airflow_dag_id", default=None,
                        help="Airflow DAG id for CloudWatch/Airflow correlation")
    parser.add_argument("--airflow_run_id", default=None,
                        help="Airflow run id (dag_run.run_id) for CloudWatch/Airflow correlation")
    return parser.parse_args(argv)


if __name__ == "__main__":
    args = _parse_args()
    sys.exit(
        run(
            run_id=args.run_id,
            domain=args.domain,
            dataset=args.dataset,
            s3_input_path=args.s3_input_path,
            file_id=args.file_id,
            parent_run_id=args.parent_run_id,
            airflow_dag_id=args.airflow_dag_id,
            airflow_run_id=args.airflow_run_id,
        )
    )
