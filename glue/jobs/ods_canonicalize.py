# glue/jobs/ods_canonicalize.py
"""ODS canonicalize job: raw Kafka topic -> canonical Kafka topic."""
from __future__ import annotations

import argparse
import copy
import json
import os
import sys
import uuid

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

import ods_pipeline
from canonicalize import apply_transform, load_mapping, matches_context
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from utils import generate_message_key, load_dataset_config

Stage = ods_pipeline.Stage
StageEvent = ods_pipeline.StageEvent


def _build_spark(dataset: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(f"ods_canonicalize_{dataset}")
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


def _fetch_schema(sr: SchemaRegistryClient, subject: str) -> str:
    try:
        return sr.get_latest_version(subject).schema.schema_str
    except Exception as exc:
        raise RuntimeError(f"schema not found for subject {subject}: {exc}") from exc


def _topic_end_offsets_by_partition(topic: str, bootstrap: str) -> dict[int, int]:
    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": f"ods-canonical-offset-probe-{uuid.uuid4()}",
    })
    try:
        md = consumer.list_topics(topic, timeout=10).topics[topic]
        if md.error:
            raise RuntimeError(f"topic metadata error: {md.error}")
        offsets: dict[int, int] = {}
        for partition in sorted(md.partitions):
            _, high = consumer.get_watermark_offsets(
                TopicPartition(topic, partition),
                timeout=10,
            )
            offsets[partition] = high
        return offsets
    finally:
        consumer.close()


def _parse_offset_ranges(
    raw: str | None,
    offset_start: int | None,
    offset_end: int | None,
) -> dict[int, tuple[int, int]]:
    if raw:
        loaded = json.loads(raw)
        ranges = {}
        for key, value in loaded.items():
            if isinstance(value, dict):
                ranges[int(key)] = (int(value["start"]), int(value["end"]))
            else:
                ranges[int(key)] = (int(value[0]), int(value[1]))
        return ranges
    if offset_start is None or offset_end is None:
        raise ValueError("provide --offset_ranges or both --offset_start/--offset_end")
    return {0: (offset_start, offset_end)}


def _consume_bounded(
    topic: str,
    ranges: dict[int, tuple[int, int]],
    bootstrap: str,
    deserializer: AvroDeserializer,
) -> list[dict]:
    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": f"ods-canonicalize-{uuid.uuid4()}",
        "enable.auto.commit": False,
        "auto.offset.reset": "error",
    })
    try:
        partitions = [
            TopicPartition(topic, partition, start)
            for partition, (start, end) in ranges.items()
            if end > start
        ]
        if not partitions:
            return []
        consumer.assign(partitions)
        remaining = {partition: end for partition, (_, end) in ranges.items()}
        rows: list[dict] = []
        while remaining:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                raise RuntimeError(str(msg.error()))
            partition = msg.partition()
            end = remaining.get(partition)
            if end is None:
                continue
            if msg.offset() >= end:
                del remaining[partition]
                continue
            value = deserializer(
                msg.value(),
                SerializationContext(topic, MessageField.VALUE),
            )
            if value is not None:
                rows.append(value)
            if msg.offset() + 1 >= end:
                del remaining[partition]
        return rows
    finally:
        consumer.close()


def _write_dlq(df, domain: str, dataset: str, business_date: str,
               run_id: str) -> str:
    env = os.environ.get("ENV", "local")
    path = (
        ods_pipeline.dlq.s3_prefix(
            env=env,
            domain=domain,
            dataset=dataset,
            stage="canonicalize",
            business_date=business_date or None,
            run_id=run_id,
        ).replace("s3://", "s3a://")
        + "failed.parquet"
    )
    df.write.mode("overwrite").parquet(path)
    return path


def _sum_ranges(ranges: dict[int, tuple[int, int]]) -> int:
    return sum(max(0, end - start) for start, end in ranges.values())


def _metadata_fields(raw_columns: set[str]) -> list[dict[str, str]]:
    keep = [
        "_ods_file_id",
        "_ods_domain",
        "_ods_dataset",
        "_ods_business_date",
        "_ods_source_application",
        "_ods_ingested_at",
    ]
    return [
        {"source": col, "target": col, "type": "string"}
        for col in keep
        if col in raw_columns
    ]


def run(
    *,
    run_id: str,
    domain: str,
    dataset: str,
    raw_topic: str,
    canonical_topic: str,
    transform_yaml_path: str,
    offset_ranges: dict[int, tuple[int, int]],
    file_id: str | None = None,
    parent_run_id: str | None = None,
    business_date: str | None = None,
    key_fields: list[str] | None = None,
) -> int:
    conn = ods_pipeline.connect()
    spark = None
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS",
                               os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"))
    sr_url = os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
    sr = SchemaRegistryClient({"url": sr_url})
    try:
        config = load_dataset_config(conn, domain, dataset)
        raw_schema = _fetch_schema(sr, f"{raw_topic}-value")
        canonical_schema = _fetch_schema(sr, f"{canonical_topic}-value")
        key_fields = key_fields or (
            config["key_fields"]
            if isinstance(config["key_fields"], list)
            else json.loads(config["key_fields"])
        )

        ods_pipeline.runs.start(
            conn,
            run_id=run_id,
            pipeline_type="canonicalize",
            domain=domain,
            dataset=dataset,
            business_date=business_date,
            file_id=file_id,
            kafka_topic=canonical_topic,
            config_version_id=config.get("version"),
            parents=[{"run_id": parent_run_id, "edge_type": "raw_to_canonical"}]
            if parent_run_id else None,
        )

        deserializer = AvroDeserializer(sr, raw_schema)
        consumed_rows = _consume_bounded(raw_topic, offset_ranges, bootstrap, deserializer)
        offset_range_count = _sum_ranges(offset_ranges)
        rows = [
            row for row in consumed_rows
            if matches_context(row, file_id=file_id, parent_run_id=parent_run_id)
        ]
        raw_count = len(rows)
        filtered_count = len(consumed_rows) - raw_count
        ods_pipeline.stages.write(
            conn,
            run_id=run_id,
            stage=Stage.KAFKA_CONSUME,
            status="succeeded",
            event_type=StageEvent.COMPLETED,
            input_ref=f"kafka://{raw_topic}#{json.dumps(offset_ranges, sort_keys=True)}",
            record_count_out=raw_count,
            metrics={
                "offset_range_count": offset_range_count,
                "consumed_count": len(consumed_rows),
                "filtered_count": filtered_count,
                "filter": {"file_id": file_id, "parent_run_id": parent_run_id},
            },
        )

        spark = _build_spark(dataset)
        if rows:
            raw_df = spark.createDataFrame(rows)
        else:
            raw_df = spark.createDataFrame([], "placeholder string").drop("placeholder")

        mapping = load_mapping(transform_yaml_path)
        mapping = copy.deepcopy(mapping)
        mapping.setdefault("fields", [])
        mapping["fields"].extend(_metadata_fields(set(raw_df.columns)))
        pass_df, fail_df, warnings = apply_transform(raw_df, mapping)
        fail_count = fail_df.count()
        pass_count = pass_df.count()

        if "_ods_run_id" in pass_df.columns:
            pass_df = pass_df.withColumn("_ods_raw_run_id", F.col("_ods_run_id"))
        else:
            pass_df = pass_df.withColumn("_ods_raw_run_id", F.lit(parent_run_id))
        pass_df = (
            pass_df
            .withColumn("_ods_canonicalize_run_id", F.lit(run_id))
            .withColumn("_ods_run_id", F.lit(run_id))
        )

        dlq_path = None
        if fail_count:
            dlq_path = _write_dlq(fail_df, domain, dataset, business_date or "", run_id)

        ods_pipeline.stages.write(
            conn,
            run_id=run_id,
            stage=Stage.CANONICAL_TRANSFORM,
            status="warned" if fail_count or warnings else "succeeded",
            event_type=StageEvent.WARNED if fail_count or warnings else StageEvent.COMPLETED,
            record_count_in=raw_count,
            record_count_out=pass_count,
            output_ref=dlq_path,
            metrics={"fail_count": fail_count, "warnings": warnings},
        )

        producer = Producer({
            "bootstrap.servers": bootstrap,
            "enable.idempotence": True,
            "acks": "all",
        })
        key_serializer = StringSerializer("utf_8")
        value_serializer = AvroSerializer(sr, canonical_schema)
        canonical_start = _topic_end_offsets_by_partition(canonical_topic, bootstrap)
        delivery_errors: list[str] = []

        def _on_delivery(err, _msg):
            if err:
                delivery_errors.append(str(err))

        for row in pass_df.collect():
            payload = row.asDict()
            msg_key = generate_message_key(key_fields, payload)
            producer.produce(
                canonical_topic,
                key=key_serializer(msg_key),
                value=value_serializer(
                    payload,
                    SerializationContext(canonical_topic, MessageField.VALUE),
                ),
                on_delivery=_on_delivery,
            )
        producer.flush()
        if delivery_errors:
            raise RuntimeError(delivery_errors[0])

        canonical_end = _topic_end_offsets_by_partition(canonical_topic, bootstrap)
        canonical_count = sum(
            canonical_end.get(partition, 0) - canonical_start.get(partition, 0)
            for partition in set(canonical_start) | set(canonical_end)
        )
        expected = raw_count - fail_count
        discrepancy = canonical_count - expected
        status = "ok" if discrepancy == 0 else "failed"

        ods_pipeline.reconciliation.write_check(
            conn,
            check_type="t1_canonicalize_count",
            run_id=run_id,
            domain=domain,
            dataset=dataset,
            business_date=business_date,
            source_count=expected,
            kafka_count=canonical_count,
            status=status,
            detail=(
                f"raw_consumed={raw_count}; dlq_count={fail_count}"
                if status == "ok"
                else f"raw_consumed={raw_count}; dlq_count={fail_count}; discrepancy={discrepancy}"
            ),
        )
        ods_pipeline.stages.write(
            conn,
            run_id=run_id,
            stage=Stage.RECON_T1,
            status="succeeded" if status == "ok" else "failed",
            event_type=StageEvent.COMPLETED if status == "ok" else StageEvent.FAILED,
            record_count_in=expected,
            record_count_out=canonical_count,
            metrics={
                "raw_offset_ranges": offset_ranges,
                "canonical_offset_start": canonical_start,
                "canonical_offset_end": canonical_end,
            },
        )

        ods_pipeline.lineage.write_edge(
            conn,
            child_run_id=run_id,
            parent_file_id=file_id,
            parent_run_id=parent_run_id,
            edge_type="raw_to_canonical",
            source_ref=f"kafka://{raw_topic}#{json.dumps(offset_ranges, sort_keys=True)}",
            target_ref=f"kafka://{canonical_topic}",
            record_count=canonical_count,
        )
        ods_pipeline.runs.update(
            conn,
            run_id,
            status="succeeded" if status == "ok" else "failed",
            record_count_source=raw_count,
            record_count_dq_fail=fail_count,
            record_count_published=canonical_count,
            kafka_topic=canonical_topic,
            kafka_offset_start=sum(canonical_start.values()),
            kafka_offset_end=sum(canonical_end.values()),
            error_summary=None if status == "ok" else f"T1 mismatch: {discrepancy}",
        )
        ods_pipeline.events.produce(
            "canonicalize.completed",
            run_id,
            domain,
            dataset,
            business_date or "",
            "succeeded" if status == "ok" else "failed",
            pipeline_type="canonicalize",
            file_id=file_id,
            kafka_topic=canonical_topic,
            kafka_offset_start=sum(canonical_start.values()),
            kafka_offset_end=sum(canonical_end.values()),
            record_count_published=canonical_count,
        )
        return 0 if status == "ok" else 1
    except Exception as exc:
        try:
            ods_pipeline.runs.update(conn, run_id, status="failed",
                                     error_summary=str(exc)[:1000])
        except Exception:
            pass
        raise
    finally:
        if spark is not None:
            spark.stop()
        conn.close()


def _parse_args(argv=None):
    parser = argparse.ArgumentParser(description="ODS raw Kafka -> canonical Kafka job")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--domain", required=True)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--raw_topic", required=True)
    parser.add_argument("--canonical_topic", required=True)
    parser.add_argument("--transform_yaml_path", required=True)
    parser.add_argument("--offset_ranges", default=None,
                        help='JSON map: {"0":{"start":10,"end":20}}')
    parser.add_argument("--offset_start", type=int, default=None,
                        help="Single-partition fallback start offset")
    parser.add_argument("--offset_end", type=int, default=None,
                        help="Single-partition fallback end offset")
    parser.add_argument("--file_id", default=None)
    parser.add_argument("--parent_run_id", default=None)
    parser.add_argument("--business_date", default=None)
    return parser.parse_args(argv)


if __name__ == "__main__":
    args = _parse_args()
    sys.exit(run(
        run_id=args.run_id,
        domain=args.domain,
        dataset=args.dataset,
        raw_topic=args.raw_topic,
        canonical_topic=args.canonical_topic,
        transform_yaml_path=args.transform_yaml_path,
        offset_ranges=_parse_offset_ranges(
            args.offset_ranges,
            args.offset_start,
            args.offset_end,
        ),
        file_id=args.file_id,
        parent_run_id=args.parent_run_id,
        business_date=args.business_date,
    ))
