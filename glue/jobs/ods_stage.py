# glue/jobs/ods_stage.py
"""
ODS Glue staging job — CSV (S3 Raw) → Postgres staging table.

Reads a slot CSV, applies DQ, writes passing rows to the slot's staging table.
Used as the first step in a multi-file merge pipeline.

Usage:
    spark-submit ods_stage.py \
        --run_id  <uuid> \
        --domain  insurance \
        --dataset policies_core \
        --s3_input_path s3://ods-raw-local/insurance/policies_core/date=20260601/policies_core_20260601.csv
"""

from __future__ import annotations

import argparse
import json
import os
import sys

import psycopg2
import requests
from dq import evaluate_dq_rules
from pyspark.sql import SparkSession
from utils import (
    extract_business_date,
    get_file_state,
    set_file_state,
    update_run_fields,
    upsert_run_header,
    write_stage_row,
)


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
        .appName(f"ods_stage_{dataset}")
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


def _load_slot_config(conn, domain: str, dataset: str) -> dict:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT domain, dataset, filename_pattern, schema_id, schema_version,
                   key_fields, dq_rules, version, slot_name, merge_dataset, staging_table
            FROM pipeline.dataset_config
            WHERE domain=%s AND dataset=%s AND active=TRUE
            """,
            (domain, dataset),
        )
        row = cur.fetchone()
    if not row:
        raise ValueError(f"No active config for {domain}/{dataset}")
    cols = ["domain", "dataset", "filename_pattern", "schema_id", "schema_version",
            "key_fields", "dq_rules", "version", "slot_name", "merge_dataset", "staging_table"]
    cfg = dict(zip(cols, row))
    if not cfg.get("slot_name"):
        raise ValueError(f"{domain}/{dataset} is not a slot dataset (slot_name is NULL). Use ods_ingestion.py instead.")
    if not cfg.get("staging_table"):
        raise ValueError(f"{domain}/{dataset} has no staging_table configured.")
    return cfg


def _validate_schema(df_columns: list[str], schema_id: str, schema_version: str) -> tuple[bool, str]:
    registry_url = os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
    url = f"{registry_url}/subjects/{schema_id}/versions/{schema_version}"
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code == 404:
            return True, ""  # schema not registered yet — skip validation
        resp.raise_for_status()
    except Exception as exc:
        return False, f"Schema Registry request failed: {exc}"
    body = resp.json()
    avro_schema = json.loads(body.get("schema", "{}"))
    avro_fields = [f["name"] if isinstance(f, dict) else f for f in avro_schema.get("fields", [])]
    required = [f for f in avro_fields if not f.startswith("_ods_")]
    missing = [f for f in required if f not in set(df_columns)]
    if missing:
        return False, f"Schema validation failed — missing columns: {missing}"
    return True, ""


def _write_dlq(spark, failing_df, domain: str, dataset: str, business_date: str, run_id: str) -> None:
    env = os.environ.get("ENV", "local")
    dlq_path = f"s3a://ods-dlq-{env}/{domain}/{dataset}/date={business_date}/run_id={run_id}/failed.csv"
    failing_df.write.mode("overwrite").parquet(dlq_path)


def _write_to_staging(conn, staging_table: str, rows: list[dict],
                      business_date_str: str, run_id: str) -> int:
    if not rows:
        return 0
    cols = [c for c in rows[0].keys() if not c.startswith("_ods_")]
    all_cols = cols + ["_ods_run_id", "_ods_business_date"]
    placeholders = ", ".join(["%s"] * len(all_cols))
    col_list = ", ".join(f'"{c}"' for c in all_cols)

    with conn.cursor() as cur:
        cur.execute(
            f"DELETE FROM {staging_table} WHERE _ods_business_date = %s",
            (business_date_str,),
        )
        data = [
            tuple(row.get(c) for c in cols) + (run_id, business_date_str)
            for row in rows
        ]
        cur.executemany(
            f"INSERT INTO {staging_table} ({col_list}) VALUES ({placeholders}) "
            f"ON CONFLICT (policy_id, _ods_business_date) DO UPDATE SET "
            + ", ".join(f'"{c}"=EXCLUDED."{c}"' for c in cols)
            + f', _ods_run_id=EXCLUDED._ods_run_id, _ods_staged_at=NOW()',
            data,
        )
    conn.commit()
    return len(data)


def run(run_id: str, domain: str, dataset: str, s3_input_path: str) -> int:
    pg = _get_pg_conn()
    pg_dsn = _pg_dsn()

    config = _load_slot_config(pg, domain, dataset)
    staging_table = config["staging_table"]
    slot_name = config["slot_name"]

    current_state = get_file_state(pg, s3_input_path)
    if current_state == "completed":
        upsert_run_header(pg_dsn, run_id=run_id, pipeline_type="stage",
                          domain=domain, dataset=dataset, business_date=None)
        update_run_fields(pg_dsn, run_id, status="succeeded",
                          error_summary="already staged — skipping.")
        write_stage_row(pg_dsn, run_id=run_id, stage="stage",
                        status="skipped", input_ref=s3_input_path,
                        error="already staged — skipping.")
        pg.close()
        return 0

    filename = s3_input_path.split("/")[-1]
    business_date = extract_business_date(filename, config["filename_pattern"])
    business_date_str = business_date.strftime("%Y-%m-%d")

    try:
        config_version_id = int(config["version"]) if config["version"] else None
    except (TypeError, ValueError):
        config_version_id = None

    upsert_run_header(pg_dsn, run_id=run_id, pipeline_type="stage",
                      domain=domain, dataset=dataset,
                      business_date=business_date_str,
                      config_version_id=config_version_id)
    write_stage_row(pg_dsn, run_id=run_id, stage="stage",
                    status="running", input_ref=s3_input_path)

    spark = _build_spark(dataset)
    s3a_path = s3_input_path.replace("s3://", "s3a://")
    current_stage = "read_csv"

    try:
        df = (spark.read
              .option("inferSchema", "true")
              .option("header", "true")
              .csv(s3a_path))

        source_count = df.count()
        update_run_fields(pg_dsn, run_id, record_count_source=source_count)
        write_stage_row(pg_dsn, run_id=run_id, stage="read_csv",
                        status="succeeded", record_count_in=source_count)

        # Schema validation (lenient — SR subject may not exist for slot yet)
        schema_id = str(config["schema_id"])
        schema_version = str(config["schema_version"])
        ok, err = _validate_schema(df.columns, schema_id, schema_version)
        if not ok:
            raise RuntimeError(f"Schema validation failed: {err}")

        current_stage = "dq_check"
        dq_rules = config.get("dq_rules") or {}
        if isinstance(dq_rules, str):
            dq_rules = json.loads(dq_rules)

        passing_df, failing_df, dq_result = evaluate_dq_rules(df, dq_rules, source_count)
        fail_count = failing_df.count() if failing_df else 0
        pass_count = passing_df.count()

        if fail_count > 0:
            _write_dlq(spark, failing_df, domain, dataset, business_date_str, run_id)

        update_run_fields(pg_dsn, run_id,
                          record_count_dq_pass=pass_count,
                          record_count_dq_fail=fail_count)
        write_stage_row(pg_dsn, run_id=run_id, stage="dq_check",
                        status="succeeded",
                        record_count_in=source_count,
                        record_count_out=pass_count)

        if pass_count == 0:
            update_run_fields(pg_dsn, run_id, status="failed",
                              error_summary="All rows failed DQ — nothing staged.")
            set_file_state(pg, s3_input_path, run_id, "failed",
                           error_reason="All rows failed DQ")
            pg.close()
            return 1

        current_stage = "stage_write"
        rows = [row.asDict() for row in passing_df.collect()]
        written = _write_to_staging(pg, staging_table, rows, business_date_str, run_id)

        write_stage_row(pg_dsn, run_id=run_id, stage="stage_write",
                        status="succeeded",
                        output_ref=staging_table,
                        record_count_out=written)

        set_file_state(pg, s3_input_path, run_id, "completed", record_count=written)
        update_run_fields(pg_dsn, run_id, status="succeeded")

        pg.close()
        return 0

    except Exception as exc:
        msg = str(exc)
        try:
            update_run_fields(pg_dsn, run_id, status="failed", error_summary=msg)
            set_file_state(pg, s3_input_path, run_id, "failed", error_reason=msg)
            write_stage_row(pg_dsn, run_id=run_id, stage=current_stage,
                            status="failed", error=msg)
        except Exception:
            pass
        pg.close()
        print(f"FATAL: {msg}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--domain", required=True)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--s3_input_path", required=True)
    args = parser.parse_args()
    sys.exit(run(args.run_id, args.domain, args.dataset, args.s3_input_path))
