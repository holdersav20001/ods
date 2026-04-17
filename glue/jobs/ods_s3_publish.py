# glue/jobs/ods_s3_publish.py
"""
ODS Glue publish job — Parquet (S3 Curated) → Avro messages (Kafka).

Usage:
    spark-submit ods_s3_publish.py \
        --run_id  <uuid> \
        --domain  insurance \
        --dataset policies \
        --s3_input_path s3://ods-curated-local/insurance/policies/date=2026-05-01/
"""

from __future__ import annotations

import argparse
import io
import json
import os
import sys

import fastavro
import psycopg2
import requests
from confluent_kafka import Producer
from pyspark.sql import SparkSession

from utils import (
    generate_message_key,
    get_file_state,
    load_dataset_config,
    set_file_state,
    write_job_log,
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


def _build_spark(dataset: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(f"ods_s3_publish_{dataset}")
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


def _fetch_avro_schema(schema_id: str, schema_version: str) -> dict:
    """
    Fetch the Avro schema from Schema Registry.
    Returns the parsed schema dict (via fastavro.parse_schema).
    Raises RuntimeError on any failure.
    """
    registry_url = os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
    url = f"{registry_url}/subjects/{schema_id}/versions/{schema_version}"
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
    except Exception as exc:
        raise RuntimeError(f"Schema Registry request failed: {exc}") from exc

    body = resp.json()
    schema_str = body.get("schema", "{}")
    try:
        avro_schema_dict = json.loads(schema_str)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Could not parse Avro schema JSON: {exc}") from exc

    try:
        parsed = fastavro.parse_schema(avro_schema_dict)
    except Exception as exc:
        raise RuntimeError(f"fastavro.parse_schema failed: {exc}") from exc

    return parsed


def _serialize_avro(parsed_schema: dict, record: dict) -> bytes:
    """Serialize a single record dict to Avro bytes using schemaless_writer."""
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, parsed_schema, record)
    return buf.getvalue()


def _write_dlq(spark, failing_df, domain: str, dataset: str,
               business_date: str, run_id: str) -> None:
    env = os.environ.get("ENV", "local")
    dlq_bucket = f"ods-dlq-{env}"
    date_part = business_date if business_date else "unknown"
    key = f"{domain}/{dataset}/date={date_part}/run_id={run_id}/failed.parquet"
    dlq_path = f"s3a://{dlq_bucket}/{key}"
    failing_df.write.mode("overwrite").parquet(dlq_path)


def _write_lineage(pg, run_id: str, domain: str, dataset: str,
                   source_ref: str, target_topic: str,
                   business_date: str | None, record_count: int,
                   schema_version: str) -> None:
    with pg.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline.lineage
              (run_id, domain, dataset, source_type, source_ref, target_topic,
               business_date, record_count, schema_version)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (run_id, domain, dataset, "s3_parquet", source_ref, target_topic,
             business_date, record_count, schema_version),
        )
    pg.commit()


# ---------------------------------------------------------------------------
# Main publish logic
# ---------------------------------------------------------------------------

def run(run_id: str, domain: str, dataset: str, s3_input_path: str) -> int:
    """Execute the publish pipeline. Returns process exit code."""

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

    schema_id = str(config["schema_id"])
    schema_version = str(config["schema_version"])
    target_topic = config["target_topic"]

    # ------------------------------------------------------------------
    # Step 2 — Idempotency check
    # ------------------------------------------------------------------
    current_state = get_file_state(pg, s3_input_path)
    if current_state == "completed":
        write_job_log(
            pg,
            run_id=run_id,
            job_name="ods_s3_publish",
            pipeline_type="publish",
            domain=domain,
            dataset=dataset,
            source_path=s3_input_path,
            status="skipped",
            error_reason="Path already in completed state — skipping.",
        )
        pg.close()
        return 0

    # ------------------------------------------------------------------
    # Step 3 — Log started
    # ------------------------------------------------------------------
    write_job_log(
        pg,
        run_id=run_id,
        job_name="ods_s3_publish",
        pipeline_type="publish",
        domain=domain,
        dataset=dataset,
        source_path=s3_input_path,
        status="started",
        config_version=config_version,
        config_snapshot=config_snapshot,
    )

    # ------------------------------------------------------------------
    # Step 4 — Set file_state = processing
    # ------------------------------------------------------------------
    set_file_state(pg, s3_input_path, run_id, "processing")

    # ------------------------------------------------------------------
    # Step 5 — Fetch Avro schema from Schema Registry
    # ------------------------------------------------------------------
    try:
        parsed_schema = _fetch_avro_schema(schema_id, schema_version)
    except RuntimeError as exc:
        err_msg = str(exc)
        write_job_log(
            pg,
            run_id=run_id,
            job_name="ods_s3_publish",
            pipeline_type="publish",
            domain=domain,
            dataset=dataset,
            source_path=s3_input_path,
            status="failed",
            error_reason="schema_fetch_failed",
            error_detail=err_msg,
        )
        set_file_state(pg, s3_input_path, run_id, "failed",
                       error_reason="schema_fetch_failed")
        pg.close()
        return 1

    # ------------------------------------------------------------------
    # Step 6 — Log schema_fetched
    # ------------------------------------------------------------------
    write_job_log(
        pg,
        run_id=run_id,
        job_name="ods_s3_publish",
        pipeline_type="publish",
        domain=domain,
        dataset=dataset,
        source_path=s3_input_path,
        status="schema_fetched",
    )

    # ------------------------------------------------------------------
    # Step 7 — Build Spark session
    # ------------------------------------------------------------------
    spark = _build_spark(dataset)
    s3a_path = s3_input_path.replace("s3://", "s3a://")

    # ------------------------------------------------------------------
    # Step 8 — Read Parquet from S3 Curated
    # ------------------------------------------------------------------
    df = spark.read.parquet(s3a_path)
    source_count = df.count()

    # Extract business_date from the _ods_business_date column if present.
    business_date: str | None = None
    if "_ods_business_date" in df.columns:
        first_row = df.select("_ods_business_date").first()
        if first_row and first_row[0] is not None:
            business_date = str(first_row[0])

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

    # Write failing rows to DLQ if any
    if failing_count > 0:
        _write_dlq(spark, failing_df, domain, dataset,
                   business_date or "unknown", run_id)

    dq_status = "dq_warned" if (warnings or failing_count > 0) else "dq_passed"
    write_job_log(
        pg,
        run_id=run_id,
        job_name="ods_s3_publish",
        pipeline_type="publish",
        domain=domain,
        dataset=dataset,
        source_path=s3_input_path,
        business_date=business_date,
        status=dq_status,
        record_count=failing_count,
        error_reason=json.dumps(warnings) if warnings else None,
    )

    # ------------------------------------------------------------------
    # Step 10 — Log publishing
    # ------------------------------------------------------------------
    write_job_log(
        pg,
        run_id=run_id,
        job_name="ods_s3_publish",
        pipeline_type="publish",
        domain=domain,
        dataset=dataset,
        source_path=s3_input_path,
        business_date=business_date,
        status="publishing",
        record_count=source_count - failing_count,
    )

    # ------------------------------------------------------------------
    # Step 11 — Create Kafka Producer with transactions
    # ------------------------------------------------------------------
    producer = Producer({
        "bootstrap.servers": os.environ.get(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        ),
        "enable.idempotence": True,
        "acks": "all",
        "transactional.id": f"ods-publish-{run_id}",
    })
    producer.init_transactions()

    # ------------------------------------------------------------------
    # Steps 12–13 — Collect, serialize, produce within a transaction
    # ------------------------------------------------------------------
    key_fields_raw = config.get("key_fields", [])
    key_fields = (
        key_fields_raw
        if isinstance(key_fields_raw, list)
        else json.loads(key_fields_raw)
    )

    # Collect passing rows to the driver
    rows = passing_df.collect()
    published = 0

    try:
        producer.begin_transaction()

        for row in rows:
            row_dict = row.asDict()

            msg_key = generate_message_key(key_fields, row_dict)

            # Build a record that only contains fields present in the Avro schema.
            # We pass the full row dict and let fastavro ignore extra keys
            # (parsed_schema handles union/default resolution).
            try:
                avro_bytes = _serialize_avro(parsed_schema, row_dict)
            except Exception as exc:
                raise RuntimeError(
                    f"Avro serialization failed for row {row_dict}: {exc}"
                ) from exc

            headers = [
                ("x-ods-run-id", run_id),
                ("x-ods-source-ref", s3_input_path),
                ("x-ods-source-type", "file"),
                ("x-ods-business-date", business_date or ""),
                ("x-ods-schema-version", schema_version),
                ("x-ods-pipeline-type", "publish"),
            ]

            producer.produce(
                topic=target_topic,
                key=msg_key,
                value=avro_bytes,
                headers=headers,
            )
            published += 1

        producer.flush()
        producer.commit_transaction()

    except Exception as exc:
        err_msg = str(exc)
        try:
            producer.abort_transaction()
        except Exception:
            pass  # best-effort abort

        write_job_log(
            pg,
            run_id=run_id,
            job_name="ods_s3_publish",
            pipeline_type="publish",
            domain=domain,
            dataset=dataset,
            source_path=s3_input_path,
            business_date=business_date,
            status="failed",
            error_reason="kafka_publish_failed",
            error_detail=err_msg,
        )
        set_file_state(pg, s3_input_path, run_id, "failed",
                       error_reason="kafka_publish_failed")
        pg.close()
        spark.stop()
        return 1

    # ------------------------------------------------------------------
    # Step 14 — Count verification
    # ------------------------------------------------------------------
    expected_published = passing_df.count()
    if published != expected_published:
        err = (
            f"Count mismatch: published={published}, "
            f"expected={expected_published}"
        )
        write_job_log(
            pg,
            run_id=run_id,
            job_name="ods_s3_publish",
            pipeline_type="publish",
            domain=domain,
            dataset=dataset,
            source_path=s3_input_path,
            business_date=business_date,
            status="failed",
            error_reason=err,
        )
        set_file_state(pg, s3_input_path, run_id, "failed", error_reason=err)
        pg.close()
        spark.stop()
        return 1

    # ------------------------------------------------------------------
    # Step 15 — Log count_verified
    # ------------------------------------------------------------------
    write_job_log(
        pg,
        run_id=run_id,
        job_name="ods_s3_publish",
        pipeline_type="publish",
        domain=domain,
        dataset=dataset,
        source_path=s3_input_path,
        business_date=business_date,
        status="count_verified",
        record_count=published,
    )

    # ------------------------------------------------------------------
    # Step 16 — Write lineage row
    # ------------------------------------------------------------------
    _write_lineage(
        pg,
        run_id=run_id,
        domain=domain,
        dataset=dataset,
        source_ref=s3_input_path,
        target_topic=target_topic,
        business_date=business_date,
        record_count=published,
        schema_version=schema_version,
    )

    # ------------------------------------------------------------------
    # Step 17 — Log lineage_written
    # ------------------------------------------------------------------
    write_job_log(
        pg,
        run_id=run_id,
        job_name="ods_s3_publish",
        pipeline_type="publish",
        domain=domain,
        dataset=dataset,
        source_path=s3_input_path,
        business_date=business_date,
        status="lineage_written",
        record_count=published,
    )

    # ------------------------------------------------------------------
    # Step 18 — Log completed
    # ------------------------------------------------------------------
    write_job_log(
        pg,
        run_id=run_id,
        job_name="ods_s3_publish",
        pipeline_type="publish",
        domain=domain,
        dataset=dataset,
        source_path=s3_input_path,
        business_date=business_date,
        status="completed",
        record_count=published,
    )

    # ------------------------------------------------------------------
    # Step 19 — Set file_state = completed
    # ------------------------------------------------------------------
    set_file_state(
        pg, s3_input_path, run_id, "completed",
        record_count=published,
    )

    pg.close()
    spark.stop()
    return 0


# ---------------------------------------------------------------------------
# CLI entrypoint
# ---------------------------------------------------------------------------

def _parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="ODS Parquet → Kafka Avro publish job"
    )
    parser.add_argument("--run_id", required=True,
                        help="Unique run identifier (UUID)")
    parser.add_argument("--domain", required=True,
                        help="Data domain, e.g. insurance")
    parser.add_argument("--dataset", required=True,
                        help="Dataset name, e.g. policies")
    parser.add_argument("--s3_input_path", required=True,
                        help="S3 path to curated Parquet prefix, "
                             "e.g. s3://ods-curated-local/insurance/policies/date=2026-05-01/")
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
