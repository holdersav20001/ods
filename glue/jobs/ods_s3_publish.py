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
import json
import os
import sys
import uuid
from datetime import date, datetime
from decimal import Decimal

import psycopg2
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer
from pyspark.sql import SparkSession

from utils import (
    generate_message_key,
    get_file_state,
    load_dataset_config,
    set_file_state,
    update_run_fields,
    upsert_run_header,
    write_recon_row,
    write_stage_row,
)
from dq import evaluate_dq_rules


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _pg_dsn() -> str:
    return (
        f"host={os.environ.get('POSTGRES_HOST', 'localhost')} "
        f"port={os.environ.get('POSTGRES_PORT', '5432')} "
        f"dbname={os.environ.get('POSTGRES_DB', 'ods_dev')} "
        f"user={os.environ.get('POSTGRES_USER', 'ods')} "
        f"password={os.environ.get('POSTGRES_PASSWORD', 'ods')}"
    )


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


def _fetch_schema_from_registry(sr: SchemaRegistryClient, topic: str) -> tuple[str, int]:
    """Fetch latest Avro schema string for <topic>-value from Schema Registry.

    Returns (schema_str, schema_id).
    Raises RuntimeError if the subject is not registered — never auto-registers.
    """
    subject = f"{topic}-value"
    try:
        latest = sr.get_latest_version(subject)
    except Exception as exc:
        raise RuntimeError(
            f"Schema not found for subject '{subject}' in Schema Registry. "
            f"Register it first via scripts/register_schemas.py. Error: {exc}"
        ) from exc
    return latest.schema.schema_str, latest.schema_id


def _coerce_for_avro(row_dict: dict, schema_str: str) -> dict:
    """Walk the Avro schema and coerce Python values for fastavro/AvroSerializer.

    Handles `date` and `decimal` logical types, which Spark+parquet can return
    as `datetime.datetime`, `float`, or `str`. fastavro 1.9 already accepts
    `datetime.date` and `decimal.Decimal` for these logical types, so the helper
    is mostly a safety net for stray types coming back from Spark.
    """
    try:
        schema = json.loads(schema_str)
    except Exception:
        return row_dict

    def _logical_for(field_type):
        # field_type may be a string, a dict, or a list (union).
        if isinstance(field_type, dict):
            return field_type.get("logicalType")
        if isinstance(field_type, list):
            for branch in field_type:
                if isinstance(branch, dict) and branch.get("logicalType"):
                    return branch.get("logicalType")
        return None

    fields = schema.get("fields", []) if isinstance(schema, dict) else []
    out = dict(row_dict)
    for fld in fields:
        name = fld.get("name")
        if name not in out or out[name] is None:
            continue
        logical = _logical_for(fld.get("type"))
        v = out[name]
        if logical == "date":
            # Convert datetime -> date; string YYYY-MM-DD -> date; Decimal/int passthrough
            if isinstance(v, datetime):
                out[name] = v.date()
            elif isinstance(v, str):
                try:
                    out[name] = datetime.strptime(v[:10], "%Y-%m-%d").date()
                except ValueError:
                    pass  # leave as-is; serializer will error meaningfully
        elif logical in ("decimal", "decimal-bytes"):
            if isinstance(v, Decimal):
                continue
            if isinstance(v, (int, float)):
                out[name] = Decimal(str(v))
            elif isinstance(v, str):
                try:
                    out[name] = Decimal(v)
                except Exception:
                    pass
    return out


def _topic_end_offsets(topic: str, bootstrap: str) -> int:
    """Return the sum of high-watermark offsets across all partitions of *topic*."""
    c = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": f"ods-offset-probe-{uuid.uuid4()}",
    })
    try:
        md = c.list_topics(topic, timeout=10).topics[topic]
        if md.error:
            raise RuntimeError(f"topic metadata error: {md.error}")
        parts = [TopicPartition(topic, p) for p in md.partitions]
        total = 0
        for tp in parts:
            _, high = c.get_watermark_offsets(tp, timeout=10)
            total += high
        return total
    finally:
        c.close()


def _write_dlq(spark, failing_df, domain: str, dataset: str,
               business_date: str, run_id: str) -> None:
    env = os.environ.get("ENV", "local")
    dlq_bucket = f"ods-dlq-{env}"
    date_part = business_date if business_date else "unknown"
    key = f"{domain}/{dataset}/date={date_part}/run_id={run_id}/failed.parquet"
    dlq_path = f"s3a://{dlq_bucket}/{key}"
    failing_df.write.mode("overwrite").parquet(dlq_path)


# ---------------------------------------------------------------------------
# Main publish logic
# ---------------------------------------------------------------------------

def run(run_id: str, domain: str, dataset: str, s3_input_path: str) -> int:
    """Top-level entry: guarantees run_log.status='failed' on any unhandled error."""
    pg_dsn = _pg_dsn()
    try:
        return _run_impl(run_id, domain, dataset, s3_input_path)
    except Exception as exc:
        # Best-effort: mark the run as failed before propagating.
        try:
            update_run_fields(pg_dsn, run_id,
                              status="failed",
                              error_summary=str(exc)[:1000])
        except Exception:
            pass
        raise


def _run_impl(run_id: str, domain: str, dataset: str, s3_input_path: str) -> int:
    """Execute the publish pipeline. Returns process exit code."""

    pg_dsn = _pg_dsn()
    pg = _get_pg_conn()

    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS",
                               os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"))
    sr_url = os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")

    # ------------------------------------------------------------------
    # Step 1 — Load dataset config
    # ------------------------------------------------------------------
    config = load_dataset_config(pg, domain, dataset)
    target_topic = config["target_topic"]
    config_version = config.get("version")

    key_fields_raw = config.get("key_fields", [])
    key_fields = (
        key_fields_raw
        if isinstance(key_fields_raw, list)
        else json.loads(key_fields_raw)
    )

    # ------------------------------------------------------------------
    # Step 2 — Idempotency check
    # ------------------------------------------------------------------
    current_state = get_file_state(pg, s3_input_path)
    if current_state == "completed":
        pg.close()
        return 0

    # ------------------------------------------------------------------
    # Step 3 — Write run_log header (status=running)
    # ------------------------------------------------------------------
    upsert_run_header(
        pg_dsn,
        run_id=run_id,
        pipeline_type="publish",
        domain=domain,
        dataset=dataset,
        business_date=None,  # refined below once parquet is read
        kafka_topic=target_topic,
        config_version_id=config_version,
    )
    set_file_state(pg, s3_input_path, run_id, "processing")

    # ------------------------------------------------------------------
    # Step 4 — Fetch Avro schema from Schema Registry (fail, don't register)
    # ------------------------------------------------------------------
    sr = SchemaRegistryClient({"url": sr_url})
    try:
        avro_schema_str, schema_id = _fetch_schema_from_registry(sr, target_topic)
    except RuntimeError as exc:
        err_msg = str(exc)
        update_run_fields(pg_dsn, run_id, status="failed", error_summary=err_msg)
        write_stage_row(pg_dsn, run_id=run_id, stage="schema_fetch",
                        status="failed", error=err_msg)
        set_file_state(pg, s3_input_path, run_id, "failed",
                       error_reason="schema_fetch_failed")
        pg.close()
        return 1

    # ------------------------------------------------------------------
    # Step 5 — Build Spark + read Parquet
    # ------------------------------------------------------------------
    spark = _build_spark(dataset)
    s3a_path = s3_input_path.replace("s3://", "s3a://")
    df = spark.read.parquet(s3a_path)
    source_count = df.count()

    # Extract business_date from partition column if present
    business_date: str | None = None
    if "_ods_business_date" in df.columns:
        first_row = df.select("_ods_business_date").first()
        if first_row and first_row[0] is not None:
            business_date = str(first_row[0])

    # Back-fill business_date in run_log now that we know it
    if business_date:
        update_run_fields(pg_dsn, run_id, business_date=business_date)

    write_stage_row(pg_dsn, run_id=run_id, stage="read_parquet",
                    status="succeeded", input_ref=s3a_path,
                    record_count_in=source_count, record_count_out=source_count)

    # ------------------------------------------------------------------
    # Step 6 — Run DQ rules
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
    dq_pass_count = source_count - failing_count

    if failing_count > 0:
        _write_dlq(spark, failing_df, domain, dataset,
                   business_date or "unknown", run_id)

    dq_status = "dq_warned" if (warnings or failing_count > 0) else "succeeded"
    write_stage_row(pg_dsn, run_id=run_id, stage="dq_check",
                    status=dq_status,
                    record_count_in=source_count,
                    record_count_out=dq_pass_count,
                    metrics={"failing_count": failing_count,
                             "warnings": warnings or []},
                    error=json.dumps(warnings) if warnings else None)

    update_run_fields(pg_dsn, run_id,
                      record_count_source=source_count,
                      record_count_dq_pass=dq_pass_count,
                      record_count_dq_fail=failing_count)

    # ------------------------------------------------------------------
    # Step 7 — Confluent Avro producer setup
    # ------------------------------------------------------------------
    key_ser = StringSerializer("utf_8")
    value_ser = AvroSerializer(sr, avro_schema_str)

    delivery_errors: list[str] = []

    def _deliver_cb(err, msg):
        # Runs in librdkafka thread — must NOT raise. Accumulate and check after flush.
        if err:
            delivery_errors.append(str(err))

    producer = Producer({
        "bootstrap.servers": bootstrap,
        "enable.idempotence": True,
        "acks": "all",
    })

    # ------------------------------------------------------------------
    # Step 8 — Capture start offsets BEFORE producing
    # ------------------------------------------------------------------
    try:
        offset_start = _topic_end_offsets(target_topic, bootstrap)
    except Exception as exc:
        err_msg = f"Failed to read start offsets: {exc}"
        update_run_fields(pg_dsn, run_id, status="failed", error_summary=err_msg)
        write_stage_row(pg_dsn, run_id=run_id, stage="publish",
                        status="failed", error=err_msg)
        set_file_state(pg, s3_input_path, run_id, "failed",
                       error_reason="offset_read_failed")
        pg.close()
        spark.stop()
        return 1

    # ------------------------------------------------------------------
    # Step 9 — Produce Avro messages
    # ------------------------------------------------------------------
    rows = passing_df.collect()
    try:
        for row in rows:
            row_dict = row.asDict()
            coerced = _coerce_for_avro(row_dict, avro_schema_str)
            msg_key = generate_message_key(key_fields, coerced)
            producer.produce(
                topic=target_topic,
                key=key_ser(msg_key),
                value=value_ser(coerced, SerializationContext(target_topic, MessageField.VALUE)),
                on_delivery=_deliver_cb,
            )
        producer.flush()
        if delivery_errors:
            raise RuntimeError(
                f"{len(delivery_errors)} kafka delivery failures; "
                f"first: {delivery_errors[0]}"
            )
    except Exception as exc:
        err_msg = str(exc)
        update_run_fields(pg_dsn, run_id, status="failed", error_summary=err_msg)
        write_stage_row(pg_dsn, run_id=run_id, stage="publish",
                        status="failed",
                        input_ref=s3a_path,
                        output_ref=f"kafka://{target_topic}",
                        record_count_in=dq_pass_count,
                        error=err_msg)
        set_file_state(pg, s3_input_path, run_id, "failed",
                       error_reason="kafka_publish_failed")
        pg.close()
        spark.stop()
        return 1

    # ------------------------------------------------------------------
    # Step 10 — Capture end offsets and compute published count
    # ------------------------------------------------------------------
    try:
        offset_end = _topic_end_offsets(target_topic, bootstrap)
    except Exception as exc:
        err_msg = f"Failed to read end offsets: {exc}"
        update_run_fields(pg_dsn, run_id, status="failed", error_summary=err_msg)
        set_file_state(pg, s3_input_path, run_id, "failed",
                       error_reason="offset_read_failed")
        pg.close()
        spark.stop()
        return 1

    published_count = offset_end - offset_start
    discrepancy = published_count - dq_pass_count
    t0_passed = discrepancy == 0

    # ------------------------------------------------------------------
    # Step 11 — Write recon row (always, even on success)
    # ------------------------------------------------------------------
    write_recon_row(
        pg_dsn,
        check_type="t0_publish_count",
        run_id=run_id,
        domain=domain,
        dataset=dataset,
        business_date=business_date,
        source_count=dq_pass_count,
        kafka_count=published_count,
        status="ok" if t0_passed else "failed",
        detail=None if t0_passed else f"discrepancy={discrepancy}",
    )

    # ------------------------------------------------------------------
    # Step 12 — Write publish stage row + update run_log
    # ------------------------------------------------------------------
    final_status = "succeeded" if t0_passed else "failed"
    error_summary = None if t0_passed else f"T0 mismatch: {discrepancy}"

    write_stage_row(
        pg_dsn,
        run_id=run_id,
        stage="publish",
        status=final_status,
        input_ref=s3a_path,
        output_ref=f"kafka://{target_topic}",
        record_count_in=dq_pass_count,
        record_count_out=published_count,
        metrics={"offset_start": offset_start, "offset_end": offset_end},
        error=error_summary,
    )

    update_run_fields(
        pg_dsn,
        run_id,
        record_count_published=published_count,
        kafka_topic=target_topic,
        kafka_offset_start=offset_start,
        kafka_offset_end=offset_end,
        status=final_status,
        error_summary=error_summary,
    )

    # ------------------------------------------------------------------
    # Step 13 — Update file_state
    # ------------------------------------------------------------------
    set_file_state(
        pg, s3_input_path, run_id,
        "completed" if t0_passed else "failed",
        record_count=published_count,
        error_reason=error_summary,
    )

    pg.close()
    spark.stop()

    if not t0_passed:
        sys.exit(1)
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
