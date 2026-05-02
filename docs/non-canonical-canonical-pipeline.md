# Non-canonical To Canonical Two-stage Pipeline

## Context

Today's ODS pipeline assumes every incoming file already matches its canonical downstream schema:

```text
CSV -> S3 raw -> curated Parquet -> Kafka topic -> Postgres sink
```

Some source files, such as `risk`, arrive in source-specific shape and need to be transformed into a canonical schema before downstream consumers or sink tables use them.

Required topology:

```text
risk.csv
  -> S3 raw
  -> curated Parquet
  -> ods.insurance.risk              # raw/source-shape Kafka topic
  -> ods_canonicalize Glue job       # bounded per-file canonicalization
  -> ods.insurance.risk-canonical    # canonical Kafka topic
  -> JDBC sink
  -> ods.insurance_risk
```

Canonicalization remains batch/per-file. The canonicalize job consumes only the raw Kafka records produced for the file/run, applies a declarative YAML mapping, writes bad rows to S3 DLQ, produces canonical records, records reconciliation/lineage, and exits.

Reconciliation spans every boundary:

```text
T0: file / curated -> raw Kafka
T1: raw Kafka -> canonical Kafka
T2: canonical Kafka -> Postgres
```

No row-hash T3 reconciliation in v1.

## Locked Decisions

| Concern | Decision |
|---|---|
| Transform definition | Declarative YAML at `patterns/<domain>/<dataset>.yaml`, synced via existing `yaml_loader.py` |
| Run model | Per-file batch via Airflow |
| Canonicalize scope | Consume only the raw Kafka offsets produced by the `publish_raw` child run |
| Raw topic schema | Source-specific Avro registered in Schema Registry |
| Canonical topic schema | Canonical Avro registered separately |
| Reconciliation scope | T0, T1, T2 only |
| Bad canonicalization rows | S3 DLQ, run continues |
| T1 discrepancy | `raw_consumed - dlq_count - canonical_produced` |
| Topic naming | Explicit `canonical_topic` on `dataset_config` |
| Offset model | Partition-safe offsets, not scalar topic offsets |
| Existing datasets | `is_canonical=true` by default, no behavior change |
| v1 volume | Less than 1M rows/file acceptable, but no long-term reliance on Python row collection |

## Partition-safe Offsets

Do not store or pass Kafka offsets as single integers. Kafka topics may have multiple partitions, and records for one file can be spread across all of them.

Use a per-partition offset contract:

```json
{
  "topic": "ods.insurance.risk",
  "partitions": [
    {"partition": 0, "start": 120, "end": 370},
    {"partition": 1, "start": 98, "end": 348},
    {"partition": 2, "start": 201, "end": 451},
    {"partition": 3, "start": 77, "end": 327}
  ]
}
```

Kafka offset ranges are half-open:

```text
[start, end)
```

So if `start=120` and `end=370`, the consumer reads offsets `120` through `369`.

The publish job should:

```text
discover raw topic partitions
capture start offsets per partition
produce file records
flush producer
capture end offsets per partition
store offset contract in run_stage_log.metrics
```

The canonicalize job should:

```text
read the publish_raw offset contract
optionally verify topic partitions still exist
consume exactly those partition ranges
filter by _ods_file_id / _ods_run_id
```

Kafka metadata tells the job which partitions exist. The stored offset contract tells downstream jobs which offsets belong to this file/run.

Canonicalize must therefore consume from the stored offset contract, not from live topic end offsets at canonicalize time.

## Partition Discovery And Parallelism

Kafka topic partition count is discovered at runtime from Kafka metadata.

For example:

```text
ods.insurance.risk
  partition 0
  partition 1
  partition 2
  partition 3
```

For v1, a single canonicalize job may assign all partitions and poll them together:

```text
one canonicalize job
  assign partition 0 at start offset
  assign partition 1 at start offset
  assign partition 2 at start offset
  assign partition 3 at start offset
  poll all assigned partitions
  stop each partition when it reaches its end offset
  finish when all partitions are complete
```

Do not consume partition 0 fully, then partition 1, then partition 2, and so on. Poll assigned partitions together so one large partition does not unnecessarily block the whole job.

Preferred scalable path:

```text
Spark Kafka source
  startingOffsets = stored per-partition starts
  endingOffsets   = stored per-partition ends
```

Spark can then parallelize reads across Kafka partitions.

## Record Lineage Metadata

Every raw Kafka message must include ODS lineage metadata in the message value.

Required raw topic value fields:

```text
_ods_file_id
_ods_run_id
_ods_source_application
_ods_domain
_ods_dataset
_ods_business_date
```

These fields may also be duplicated into Kafka headers for debugging/routing, but headers must not be the only copy.

Recommended contract:

```text
Kafka message value:
  _ods_file_id
  _ods_run_id
  _ods_source_application
  _ods_domain
  _ods_dataset
  _ods_business_date
  business fields...

Kafka headers:
  _ods_file_id
  _ods_run_id
  _ods_source_application
```

The value is the durable contract. It is visible to Spark, Schema Registry, JDBC Sink, reconciliation, dashboards, and lineage tools. Headers are useful, but some tools/connectors may drop or ignore them.

Canonical topic messages should preserve lineage too:

```text
_ods_file_id
_ods_run_id
_ods_source_application
_ods_canonicalize_run_id
_ods_domain
_ods_dataset
_ods_business_date
```

This matters when two files for the same dataset are published concurrently. Their records may interleave in the same Kafka partitions, so canonicalize should consume the stored offset ranges and then filter by `_ods_file_id` or `_ods_run_id`.

## Concurrent Files For The Same Dataset

If two `risk` files from different source applications are loaded at the same time into the same raw Kafka topic, their records can interleave in the same partitions:

```text
ods.insurance.risk, partition 0

offset 100  app=A file=A1
offset 101  app=B file=B1
offset 102  app=A file=A1
offset 103  app=B file=B1
```

If canonicalize only consumes offset range `[100, 104)`, that range contains both files. Therefore offset ranges alone are not enough when concurrent producers are possible.

The v1 default should be:

```text
same raw topic
per-partition offset ranges
mandatory _ods_file_id / _ods_run_id in every message
canonicalize filters by file_id or publish_raw_run_id
```

Alternative options:

| Option | How it works | View |
|---|---|---|
| Same raw topic, metadata filter | Both applications write to `ods.insurance.risk`; canonicalize filters by `_ods_file_id` / `_ods_run_id` | Best v1 default |
| Topic per source application | Example: `ods.insurance.risk.claims`, `ods.insurance.risk.underwriting` | Cleaner isolation, more config/connectors |
| Sequential publish per dataset | Only one risk file publishes at a time | Simple, but limits throughput |
| File/run-specific staging topics | One topic per file/run | Strong isolation, operationally noisy |

## Run Model

For existing canonical datasets:

```text
s3_batch parent
  ├── ingestion
  └── publish
      └── sink wait
```

For non-canonical datasets:

```text
s3_batch parent
  ├── ingestion
  ├── publish_raw
  └── canonicalize
      └── sink wait
```

Use distinct child run IDs:

| Run | `pipeline_type` | Purpose |
|---|---|---|
| Parent | `s3_batch` | Orchestration / file-level lifecycle |
| Child 1 | `ingestion` | Raw file -> curated Parquet |
| Child 2 | `publish_raw` | Curated Parquet -> raw/source-shape Kafka topic |
| Child 3 | `canonicalize` | Raw Kafka topic -> canonical Kafka topic |

Do not overload `publish` for non-canonical raw publishing. Use `publish_raw` so dashboards, lineage, and reconciliation are clear.

## Dataset Config Changes

Migration 16:

```sql
ALTER TABLE pipeline.dataset_config
    ADD COLUMN IF NOT EXISTS is_canonical             BOOLEAN NOT NULL DEFAULT TRUE,
    ADD COLUMN IF NOT EXISTS canonical_topic          VARCHAR,
    ADD COLUMN IF NOT EXISTS transform_yaml_path      VARCHAR,
    ADD COLUMN IF NOT EXISTS canonical_schema_id      VARCHAR,
    ADD COLUMN IF NOT EXISTS canonical_schema_version INT DEFAULT 1;
```

Existing datasets remain canonical by default.

## YAML Shape

`patterns/insurance/risk.yaml`

```yaml
domain: insurance
dataset: risk
source_type: s3_batch

is_canonical: false

filename_pattern: "risk_(?P<bd>\\d{8})\\.csv"

target_topic: ods.insurance.risk
canonical_topic: ods.insurance.risk-canonical

schema_id: ods.insurance.risk-value
schema_version: 1

canonical_schema_id: ods.insurance.risk-canonical-value
canonical_schema_version: 1

postgres_target_table: ods.insurance_risk

key_fields: [risk_id]

transform_yaml_path: patterns/insurance/risk.yaml

transform:
  fields:
    - source: RskID
      target: risk_id
      type: string
      required: true

    - source: PolNo
      target: policy_id
      type: string
      required: true

    - source: ExposureAmt
      target: exposure_amount
      type: decimal
      scale: 2

    - source: AsOfDt
      target: as_of_date
      type: date
      format: "yyyyMMdd"

  derived:
    - target: risk_key
      expr: "concat(risk_id, '|', as_of_date)"

  required:
    - risk_id
    - policy_id
    - as_of_date
```

## Files To Create

| Path | Purpose |
|---|---|
| `db/migrations/16_canonical_topic_and_transform.sql` | Adds canonical config columns |
| `patterns/insurance/risk.yaml` | Sample non-canonical dataset config and mapping |
| `glue/jobs/canonicalize.py` | Pure Spark transform engine |
| `glue/jobs/ods_canonicalize.py` | Bounded Kafka consume -> transform -> canonical Kafka produce |
| `tests/unit/test_canonicalize.py` | Mapping unit tests |
| `tests/integration/test_canonical_pipeline.py` | Full risk e2e test |

## Files To Modify

| Path | Change |
|---|---|
| `ods_pipeline/models.py` | Add `Stage.KAFKA_CONSUME`, `Stage.CANONICAL_TRANSFORM`, `Stage.RECON_T1` |
| `glue/jobs/ods_s3_publish.py` | Emit `pipeline_type='publish_raw'` when `is_canonical=false`; write partition offset metrics |
| `glue/jobs/utils.py` | Extract shared Kafka offset helpers and DLQ helper |
| `airflow/dags/dag_ingest.py` | Add optional `canonicalize_run_id`; insert `stage_canonicalize` between raw publish and sink wait |
| `airflow/dags/common/yaml_loader.py` | Sync new YAML keys into `dataset_config` |
| `airflow/dags/common/connector_provisioner.py` | Use `canonical_topic` for JDBC sink when present |
| Dashboards | Add filters/views for `publish_raw`, `canonicalize`, T1 recon, and DLQ counts |

## Canonicalize Job Outline

```python
def run(
    run_id,
    domain,
    dataset,
    raw_topic,
    canonical_topic,
    raw_partition_offsets,
    file_id,
    publish_raw_run_id,
    business_date,
    airflow_dag_id,
    airflow_run_id,
):
    conn = ods_pipeline.connect()

    ods_pipeline.runs.start(
        conn,
        run_id=run_id,
        pipeline_type="canonicalize",
        domain=domain,
        dataset=dataset,
        business_date=business_date,
        file_id=file_id,
        kafka_topic=canonical_topic,
        parents=[publish_raw_run_id],
    )

    # Stage: KAFKA_CONSUME
    raw_rows = _consume_bounded(raw_topic, raw_partition_offsets)
    raw_consumed = sum(
        p["end"] - p["start"]
        for p in raw_partition_offsets["partitions"]
    )

    raw_df = spark.createDataFrame(
        raw_rows,
        schema=spark_schema_from_avro(raw_schema),
    )

    # Safety filter for concurrent files in the same topic/range.
    raw_df = raw_df.filter(
        (raw_df["_ods_file_id"] == file_id)
        & (raw_df["_ods_run_id"] == publish_raw_run_id)
    )

    # Stage: CANONICAL_TRANSFORM
    mapping = _load_yaml(transform_yaml_path)
    pass_df, fail_df, warnings = canonicalize.apply_transform(raw_df, mapping)

    dlq_count = fail_df.count()
    if dlq_count:
        _write_dlq(spark, fail_df, domain, dataset, business_date, run_id)

    # Stage: KAFKA_PUBLISH
    canonical_start_offsets = _topic_end_offsets(canonical_topic)
    _produce_avro(pass_df, canonical_topic, canonical_schema)
    canonical_end_offsets = _topic_end_offsets(canonical_topic)

    canonical_produced = offset_delta(
        canonical_start_offsets,
        canonical_end_offsets,
    )

    # T1 reconciliation
    discrepancy = raw_consumed - dlq_count - canonical_produced

    ods_pipeline.reconciliation.write_check(
        conn,
        check_type="t1_canonicalize_count",
        run_id=run_id,
        domain=domain,
        dataset=dataset,
        business_date=business_date,
        source_count=raw_consumed,
        kafka_count=canonical_produced,
        discrepancy_count=discrepancy,
        status="ok" if discrepancy == 0 else "failed",
        detail={
            "raw_topic": raw_topic,
            "canonical_topic": canonical_topic,
            "raw_offsets": raw_partition_offsets,
            "canonical_start_offsets": canonical_start_offsets,
            "canonical_end_offsets": canonical_end_offsets,
            "dlq_count": dlq_count,
            "warnings": warnings,
        },
    )

    ods_pipeline.lineage.write_edge(
        conn,
        child_run_id=run_id,
        parent_run_id=publish_raw_run_id,
        parent_file_id=file_id,
        edge_type="raw_to_canonical",
        source_ref=f"kafka://{raw_topic}",
        target_ref=f"kafka://{canonical_topic}",
        record_count=canonical_produced,
    )

    ods_pipeline.runs.update(
        conn,
        run_id,
        status="succeeded" if discrepancy == 0 else "failed",
        record_count_source=raw_consumed,
        record_count_dq_fail=dlq_count,
        record_count_published=canonical_produced,
        kafka_topic=canonical_topic,
    )
```

## DAG Behavior

`dag_ingest.init_run()` reads:

```text
is_canonical
target_topic
canonical_topic
canonical_schema_id
transform_yaml_path
```

If `is_canonical=true`:

```text
init_run
  -> stage_ingest
  -> stage_publish
  -> wait_sinks
  -> finalise
```

If `is_canonical=false`:

```text
init_run
  -> stage_ingest
  -> stage_publish_raw
  -> stage_canonicalize
  -> wait_sinks
  -> finalise
```

`stage_canonicalize` receives the raw publish offsets from `publish_raw` run/stage metrics.

`wait_sinks` chooses topic as:

```python
sink_topic = canonical_topic if canonical_topic else target_topic
```

The sink wait should also be connector-config aware, not hard-coded to policies.

## Reconciliation

Expected rows for non-canonical risk:

| Check | Boundary | Formula |
|---|---|---|
| `t0_publish_count` | curated/file -> raw Kafka | `curated_count == raw_topic_produced` |
| `t1_canonicalize_count` | raw Kafka -> canonical Kafka | `raw_consumed - dlq_count == canonical_produced` |
| `t2_sink_count` | canonical Kafka -> Postgres | `canonical_produced == postgres_count` |

`reconciliation_log.detail` should include:

```json
{
  "raw_topic": "ods.insurance.risk",
  "canonical_topic": "ods.insurance.risk-canonical",
  "dlq_count": 12,
  "raw_offsets": {},
  "canonical_offsets": {},
  "warnings": []
}
```

## Lineage

For a successful non-canonical file:

```text
file_id
  -> ingestion run
  -> publish_raw run
  -> canonicalize run
  -> JDBC sink / Postgres table
```

Required `lineage_edge.edge_type` values:

```text
raw_to_curated
curated_to_kafka
raw_to_canonical
canonical_to_postgres
```

For `raw_to_canonical`, include both:

```text
parent_run_id = publish_raw_run_id
parent_file_id = file_id
```

This keeps file lineage and run lineage queryable in the same model.

## Performance Notes

`canonicalize.apply_transform()` must be Spark DataFrame-native:

- use `select`
- use `withColumn`
- use Spark SQL expressions
- avoid row-by-row Python UDFs
- avoid `collect()` inside transform logic

For v1, bounded Kafka consume and producer loops may still use Python-side records, but this should be called out as a temporary implementation constraint.

Future scale path:

```text
Spark Kafka source -> DataFrame transform -> Spark Kafka sink
```

or chunked partition-aware consume/produce.

## Verification

```bash
# 1. Apply migration
docker exec avivaods-postgres-1 psql -U ods -d ods_dev \
  -f /migrations/16_canonical_topic_and_transform.sql

# 2. Sync risk YAML
python -m airflow.dags.common.yaml_loader patterns/insurance/risk.yaml

# 3. Register raw and canonical Avro schemas

# 4. Provision connectors
# JDBC sink should subscribe to canonical_topic for non-canonical datasets.

# 5. Unit tests
python -m pytest tests/unit/test_canonicalize.py -v

# 6. Integration test
python -m pytest tests/integration/test_canonical_pipeline.py -v

# 7. Full integration
python -m pytest tests/integration -q
```

Expected reconciliation:

```sql
SELECT check_type, status, source_count, kafka_count, postgres_count, discrepancy_count
FROM pipeline.reconciliation_log
WHERE dataset = 'risk'
ORDER BY created_at;
```

Expected:

```text
t0_publish_count          ok
t1_canonicalize_count     ok
t2_sink_count             ok
```

Expected lineage:

```sql
SELECT edge_type, COUNT(*)
FROM pipeline.lineage_edge le
JOIN pipeline.run_log rl ON rl.run_id = le.child_run_id
WHERE rl.dataset = 'risk'
GROUP BY edge_type;
```

Expected at least:

```text
raw_to_curated
curated_to_kafka
raw_to_canonical
canonical_to_postgres
```

## Out Of Scope

- Spark Structured Streaming / `availableNow`
- Row-level hash reconciliation / T3
- Quarantine Kafka topic for failed canonicalization
- Full 1M+ row performance refactor
- Multi-source canonical joins
