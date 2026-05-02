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

# Add repo root to sys.path so ods_pipeline package is importable from Glue
_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

import ods_pipeline
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer
from pyspark.sql import SparkSession

from utils import (
    generate_message_key,
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

    def _union_contains_string(field_type):
        return isinstance(field_type, list) and "string" in field_type

    fields = schema.get("fields", []) if isinstance(schema, dict) else []
    out = dict(row_dict)
    for fld in fields:
        name = fld.get("name")
        if name not in out or out[name] is None:
            continue
        field_type = fld.get("type")
        logical = _logical_for(field_type)
        v = out[name]
        # Coerce datetime/date to ISO string for ["null","string"] union fields
        if logical is None and _union_contains_string(field_type):
            if isinstance(v, datetime):
                out[name] = v.isoformat()[:10]
            elif isinstance(v, date):
                out[name] = v.isoformat()
            continue
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
    return sum(_topic_end_offsets_by_partition(topic, bootstrap).values())


def _topic_end_offsets_by_partition(topic: str, bootstrap: str) -> dict[int, int]:
    """Return high-watermark offsets by partition for *topic*."""
    c = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": f"ods-offset-probe-{uuid.uuid4()}",
    })
    try:
        md = c.list_topics(topic, timeout=10).topics[topic]
        if md.error:
            raise RuntimeError(f"topic metadata error: {md.error}")
        offsets: dict[int, int] = {}
        for partition in sorted(md.partitions):
            tp = TopicPartition(topic, partition)
            _, high = c.get_watermark_offsets(tp, timeout=10)
            offsets[partition] = high
        return offsets
    finally:
        c.close()


def _write_dlq(spark, failing_df, domain: str, dataset: str,
               business_date: str, run_id: str) -> None:
    env = os.environ.get("ENV", "local")
    dlq_path = (
        ods_pipeline.dlq.s3_prefix(
            env=env,
            domain=domain,
            dataset=dataset,
            stage="publish",
            business_date=business_date or None,
            run_id=run_id,
        ).replace("s3://", "s3a://")
        + "failed.parquet"
    )
    failing_df.write.mode("overwrite").parquet(dlq_path)


# ---------------------------------------------------------------------------
# Main publish logic
# ---------------------------------------------------------------------------

def run(run_id: str, domain: str, dataset: str, s3_input_path: str,
        file_id: str | None = None,
        parent_run_id: str | None = None,
        airflow_dag_id: str | None = None,
        airflow_run_id: str | None = None) -> int:
    """Top-level entry: guarantees run_log.status='failed' on any unhandled error."""
    conn = ods_pipeline.connect()
    try:
        return _run_impl(conn, run_id, domain, dataset, s3_input_path,
                         file_id=file_id,
                         parent_run_id=parent_run_id,
                         airflow_dag_id=airflow_dag_id,
                         airflow_run_id=airflow_run_id)
    except Exception as exc:
        # Best-effort: mark the run as failed before propagating.
        try:
            ods_pipeline.runs.update(conn, run_id,
                                     status="failed",
                                     error_summary=str(exc)[:1000])
        except Exception:
            pass
        raise
    finally:
        conn.close()


def _run_impl(conn, run_id: str, domain: str, dataset: str, s3_input_path: str,
              file_id: str | None = None,
              parent_run_id: str | None = None,
              airflow_dag_id: str | None = None,
              airflow_run_id: str | None = None) -> int:
    """Execute the publish pipeline. Returns process exit code."""

    # spark_app_id starts None; updated once Spark is running.
    # _ws closes over it by reference so post-Spark calls see the real value.
    spark_app_id: str | None = None

    def _ws(**kw):
        ods_pipeline.stages.write(conn, run_id=run_id,
                                  airflow_dag_id=airflow_dag_id,
                                  airflow_run_id=airflow_run_id,
                                  spark_app_id=spark_app_id, **kw)

    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS",
                               os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"))
    sr_url = os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")

    # ------------------------------------------------------------------
    # Step 1 — Load dataset config
    # ------------------------------------------------------------------
    config = load_dataset_config(conn, domain, dataset)
    target_topic = config["target_topic"]
    config_version = config.get("version")
    pipeline_type = "publish" if config.get("is_canonical", True) else "publish_raw"

    key_fields_raw = config.get("key_fields", [])
    key_fields = (
        key_fields_raw
        if isinstance(key_fields_raw, list)
        else json.loads(key_fields_raw)
    )

    # ------------------------------------------------------------------
    # Step 2 — Idempotency check
    # ------------------------------------------------------------------
    current_state = ods_pipeline.files.get_state(conn, s3_input_path)
    if current_state == "completed":
        return 0

    # ------------------------------------------------------------------
    # Step 3 — Write run_log header (status=running)
    # ------------------------------------------------------------------
    ods_pipeline.runs.start(
        conn,
        run_id=run_id,
        pipeline_type=pipeline_type,
        domain=domain,
        dataset=dataset,
        business_date=None,  # refined below once parquet is read
        file_id=file_id,
        kafka_topic=target_topic,
        config_version_id=config_version,
        parents=[{"run_id": parent_run_id, "edge_type": "orchestrates"}]
        if parent_run_id else None,
    )
    ods_pipeline.files.set_state(conn, s3_input_path, run_id, "processing")

    # ------------------------------------------------------------------
    # Step 4 — Fetch Avro schema from Schema Registry (fail, don't register)
    # ------------------------------------------------------------------
    sr = SchemaRegistryClient({"url": sr_url})
    try:
        avro_schema_str, schema_id = _fetch_schema_from_registry(sr, target_topic)
    except RuntimeError as exc:
        err_msg = str(exc)
        ods_pipeline.runs.update(conn, run_id, status="failed", error_summary=err_msg)
        _ws(stage=Stage.SCHEMA_VALIDATE,
            event_type=StageEvent.FAILED,
            status="failed", error=err_msg)
        ods_pipeline.files.set_state(conn, s3_input_path, run_id, "failed",
                                     error_reason="schema_fetch_failed")
        return 1

    # ------------------------------------------------------------------
    # Step 5 — Build Spark + read Parquet
    # ------------------------------------------------------------------
    spark = _build_spark(dataset)
    spark_app_id = spark.sparkContext.applicationId  # updates the cell _ws closes over

    s3a_path = s3_input_path.replace("s3://", "s3a://")
    df = spark.read.parquet(s3a_path)
    source_count = df.count()

    # Extract business_date from partition column if present
    business_date: str | None = None
    if "_ods_business_date" in df.columns:
        first_row = df.select("_ods_business_date").first()
        if first_row and first_row[0] is not None:
            business_date = str(first_row[0])

    # Back-fill business_date + file_id in run_log now that we know them
    _file_id: str | None = file_id  # use explicit arg when provided
    if business_date:
        if _file_id is None:
            # Fallback: look up by date — only when --file_id not passed (backward compat)
            with conn.cursor() as _cur:
                _cur.execute(
                    "SELECT file_id FROM pipeline.file_catalogue "
                    "WHERE domain=%s AND dataset=%s AND business_date=%s "
                    "ORDER BY first_seen_at DESC LIMIT 1",
                    (domain, dataset, business_date),
                )
                _row = _cur.fetchone()
            _file_id = str(_row[0]) if _row else None
        ods_pipeline.runs.update(conn, run_id, business_date=business_date,
                                 file_id=_file_id)

    _ws(stage=Stage.CURATED_READ,
        event_type=StageEvent.COMPLETED,
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

    _dq_has_issues = bool(warnings or failing_count > 0)
    dq_status = "warned" if _dq_has_issues else "succeeded"
    _ws(stage=Stage.DQ_CHECK,
        event_type=StageEvent.WARNED if _dq_has_issues else StageEvent.COMPLETED,
        status=dq_status,
        input_ref=s3a_path,
        record_count_in=source_count,
        record_count_out=dq_pass_count,
        metrics={"failing_count": failing_count,
                 "warnings": warnings or []},
        error=json.dumps(warnings) if warnings else None)

    ods_pipeline.runs.update(conn, run_id,
                             record_count_source=source_count,
                             record_count_dq_pass=dq_pass_count,
                             record_count_dq_fail=failing_count)

    # ------------------------------------------------------------------
    # Step 7 — Confluent Avro producer setup
    # ------------------------------------------------------------------
    key_ser = StringSerializer("utf_8")
    value_ser = AvroSerializer(sr, avro_schema_str)

    # B5/B6: per-message delivery tracking via OffsetTracker.
    # The tracker captures (partition, offset) per broker-acknowledged message
    # via on_delivery, which runs on the librdkafka poll thread. T0 recon
    # below compares tracker.delivered_count to dq_pass_count instead of the
    # offset-delta approach (which is corrupted by other producers writing to
    # the same topic concurrently).
    tracker = ods_pipeline.offsets.OffsetTracker()

    # B5: transactional producer — exactly-once semantics for the whole batch.
    # transactional.id is keyed on run_id only (not run_id+partition as the
    # plan literal suggests) because this Glue job runs ONE Producer instance
    # writing to all partitions of target_topic via the default partitioner.
    # A per-partition transactional.id would require one Producer per
    # partition, which is not the architecture here.
    producer = Producer({
        "bootstrap.servers": bootstrap,
        "enable.idempotence": True,
        "transactional.id": f"ods-publish-{run_id}",
        "acks": "all",
        "max.in.flight.requests.per.connection": 5,
    })

    # ------------------------------------------------------------------
    # Step 8 — Capture start offsets BEFORE producing
    #
    # Kept for forward compatibility with downstream tooling that reads
    # offset_start_by_partition from run_log; T0 recon itself now uses
    # tracker counts (see Step 10 below).
    # ------------------------------------------------------------------
    try:
        offset_start_by_partition = _topic_end_offsets_by_partition(target_topic, bootstrap)
        offset_start = sum(offset_start_by_partition.values())
    except Exception as exc:
        err_msg = f"Failed to read start offsets: {exc}"
        ods_pipeline.runs.update(conn, run_id, status="failed", error_summary=err_msg)
        _ws(stage=Stage.KAFKA_PUBLISH,
            status="failed", event_type=StageEvent.FAILED,
            error=err_msg)
        ods_pipeline.files.set_state(conn, s3_input_path, run_id, "failed",
                                     error_reason="offset_read_failed")
        spark.stop()
        return 1

    # ------------------------------------------------------------------
    # Step 9 — Produce Avro messages inside a Kafka transaction
    #
    # Lifecycle: init_transactions → begin_transaction → produce*N → flush
    # → (commit_transaction OR abort_transaction). producer.close() in
    # finally so resources release even on uncaught exceptions.
    # ------------------------------------------------------------------
    rows = passing_df.collect()
    source_application = os.environ.get("ODS_SOURCE_APPLICATION", "sftp")

    def _send_row(_producer, row, *, on_delivery):
        """Per-row sender: serialise, key, produce. Closure over schema/topic.

        Used as ``on_send`` for ``ods_pipeline.publish.publish_with_transaction``.
        """
        row_dict = row.asDict()
        file_meta = ods_pipeline.metadata.file_metadata(
            file_id=str(_file_id or ""),
            run_id=str(row_dict.get("_ods_run_id") or run_id),
            domain=domain,
            dataset=dataset,
            business_date=row_dict.get("_ods_business_date") or business_date or "",
            source_application=source_application,
            ingested_at=row_dict.get("_ods_ingested_at"),
        )
        row_dict.update({k: v for k, v in file_meta.items() if v is not None})
        coerced = _coerce_for_avro(row_dict, avro_schema_str)
        msg_key = generate_message_key(key_fields, coerced)
        _producer.produce(
            topic=target_topic,
            key=key_ser(msg_key),
            value=value_ser(coerced, SerializationContext(target_topic, MessageField.VALUE)),
            on_delivery=on_delivery,
        )

    publish_failed = False
    publish_err: Exception | None = None
    try:
        ods_pipeline.publish.publish_with_transaction(
            producer, rows, tracker, on_send=_send_row,
        )
    except Exception as exc:
        publish_failed = True
        publish_err = exc
    finally:
        producer.close()
        # Adversarial R2: surface delivery errors to stdout for log scrapers
        # even when no exception escaped (defensive — should be empty here
        # because publish_with_transaction would have raised on tracker.errors).
        if tracker.errors:
            print(f"[ods_s3_publish] tracker.errors={tracker.errors}", flush=True)

    if publish_failed:
        err_msg = str(publish_err)
        ods_pipeline.runs.update(conn, run_id, status="failed", error_summary=err_msg)
        _ws(stage=Stage.KAFKA_PUBLISH,
            status="failed", event_type=StageEvent.FAILED,
            input_ref=s3a_path,
            output_ref=f"kafka://{target_topic}",
            record_count_in=dq_pass_count,
            error=err_msg)
        ods_pipeline.files.set_state(conn, s3_input_path, run_id, "failed",
                                     error_reason="kafka_publish_failed")
        spark.stop()
        return 1

    # ------------------------------------------------------------------
    # Step 10 — Capture end offsets (forward compat) and compute published
    # count from the tracker, not from the offset delta.
    # ------------------------------------------------------------------
    try:
        offset_end_by_partition = _topic_end_offsets_by_partition(target_topic, bootstrap)
        offset_end = sum(offset_end_by_partition.values())
    except Exception as exc:
        err_msg = f"Failed to read end offsets: {exc}"
        ods_pipeline.runs.update(conn, run_id, status="failed", error_summary=err_msg)
        ods_pipeline.files.set_state(conn, s3_input_path, run_id, "failed",
                                     error_reason="offset_read_failed")
        spark.stop()
        return 1

    # B6: T0 recon source-of-truth = tracker.delivered_count.
    # offset_end - offset_start is unreliable in any topic where another
    # producer can write concurrently; tracker counts only what THIS
    # producer's transaction acked.
    published_count = tracker.delivered_count
    discrepancy = published_count - dq_pass_count
    t0_passed = discrepancy == 0

    # ------------------------------------------------------------------
    # Step 11 — Write recon row (always, even on success)
    # ------------------------------------------------------------------
    ods_pipeline.reconciliation.write_check(
        conn,
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

    _ws(
        stage=Stage.KAFKA_PUBLISH,
        status=final_status,
        event_type=StageEvent.COMPLETED if t0_passed else StageEvent.FAILED,
        input_ref=s3a_path,
        output_ref=f"kafka://{target_topic}",
        record_count_in=dq_pass_count,
        record_count_out=published_count,
        metrics={
            "offset_start": offset_start,
            "offset_end": offset_end,
            "offset_start_by_partition": offset_start_by_partition,
            "offset_end_by_partition": offset_end_by_partition,
        },
        error=error_summary,
    )

    ods_pipeline.runs.update(
        conn,
        run_id,
        record_count_published=published_count,
        kafka_topic=target_topic,
        kafka_offset_start=offset_start,
        kafka_offset_end=offset_end,
        status=final_status,
        error_summary=error_summary,
    )

    # B8 (T6): persist per-partition offset ranges to run_kafka_offsets.
    # On retry, runs.start consumers can call offsets.has_recorded_offsets
    # to detect "Kafka committed AND PG persisted" and skip republish — the
    # exactly-once primitive. Architect R4 noted full atomicity with the
    # run-status update is a T12 follow-up via pattern.atomic(); here both
    # writes commit individually but the Kafka transaction has already
    # acked, so worst-case crash leaves run_log updated without offsets,
    # which a resume run will repair by republishing under idempotent producer.
    if t0_passed:
        ods_pipeline.offsets.persist_ranges(
            conn,
            run_id=run_id,
            stage="kafka_publish",
            topic=target_topic,
            ranges=tracker.per_partition_ranges(),
        )

    # ------------------------------------------------------------------
    # Step 13 — Write curated→kafka lineage edge (on success)
    # ------------------------------------------------------------------
    if t0_passed and _file_id:
        ods_pipeline.lineage.write_edge(
            conn,
            child_run_id=run_id,
            parent_file_id=_file_id,
            edge_type="curated_to_kafka",
            source_ref=s3_input_path,
            target_ref=f"kafka://{target_topic}",
            record_count=published_count,
        )

    # ------------------------------------------------------------------
    # Step 14 — Update file_state
    # ------------------------------------------------------------------
    ods_pipeline.files.set_state(
        conn, s3_input_path, run_id,
        "completed" if t0_passed else "failed",
        record_count=published_count,
        error_reason=error_summary,
    )

    ods_pipeline.events.produce(
        "publish.completed", run_id, domain, dataset,
        business_date or "", final_status,
        pipeline_type=pipeline_type,
        record_count_published=published_count,
        kafka_topic=target_topic,
        kafka_offset_start=offset_start,
        kafka_offset_end=offset_end,
        error_summary=error_summary,
        file_id=_file_id,
        s3_curated_path=s3_input_path,
    )

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
    parser.add_argument("--file_id", required=False, default=None,
                        help="UUID from file_catalogue — explicit lineage contract. "
                             "When provided, skips date-scoped catalogue lookup.")
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
