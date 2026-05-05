# File → Direct-Postgres Pattern Design

Adds a third `dataset_config.delivery` value: ``direct_postgres``. For
file-source datasets that do not need a Kafka leg (no streaming
consumers, no S3 sink, no fan-out), the pipeline writes Postgres rows
straight from curated Parquet via Spark JDBC. Skips Kafka publish,
schema-registry produce, JDBC Connect sink, and downstream offset
recon.

This is a **design**, not a build. Implementation gated on the first
real dataset that opts in.

## Context

We currently have three delivery shapes:

| Source | `delivery` | Path |
|---|---|---|
| `s3_batch` (file) | `file_pipeline` (default) | S3 raw → Glue → curated → raw Kafka → canonicalize? → JDBC sink → Postgres |
| `api_pull` | `file_pipeline` (default) | API → S3 raw → Glue → curated → raw Kafka → canonicalize? → JDBC sink → Postgres |
| `api_pull` | `direct_kafka` | API → raw Kafka → S3 sink + JDBC sink → Postgres |

Use cases that don't justify Kafka for file sources:

- Reference / lookup tables refreshed daily; no streaming consumers
  downstream.
- Internal reconciliation tables backed by a single batch source.
- Pre-aggregated / pre-canonicalized batch outputs where the source of
  truth is the curated Parquet, not Kafka events.

For these, every byte the publish/canonicalize/sink loop processes is
overhead. Direct-Postgres halves the number of moving parts.

## Goals

- Add `delivery: direct_postgres` to the existing
  `dataset_config.delivery` column.
- Reuse Glue ingestion + curated Parquet + DQ + schema-validate
  unchanged.
- Skip Kafka publish, canonicalize-via-Kafka, JDBC Connect sink.
- Optional inline canonicalization against the existing transform YAML
  (so non-canonical datasets still get canonical Postgres rows).
- Reuse `dag_ingest` orchestration; branch by delivery.
- Reuse control-plane primitives (`run_log`, `run_stage_log`,
  `file_catalogue`, `lineage_edge`, `reconciliation_log`).

## Non-goals

- Replacing the Kafka path. Both shapes ship side-by-side.
- Lower-latency than file_pipeline — direct_postgres is still a per-file
  batch flow, just with fewer hops.
- Multi-target writes from one job (S3 sink + Postgres). Out of scope;
  add only when a real dataset needs both.

## Architecture

```
SFTP / S3 RAW
   │  (existing) Glue ods_ingestion.py — schema_validate, dq_check,
   ▼  curated parquet write, file_catalogue.state=curated
S3 CURATED (Parquet)
   │  (new) Glue ods_postgres_write.py
   │     • read curated Parquet
   │     • if is_canonical=false: compile_transform + apply (reuse
   │       glue.jobs.canonicalize.compile_transform)
   │     • Spark JDBC write — append OR upsert via stage table + merge
   │     • write_mode from dataset_config (existing column)
   ▼
POSTGRES TARGET TABLE
```

No Kafka topic. No Connect connector. No S3 sink. No JDBC Connect sink.

## YAML delta

```yaml
domain: insurance
dataset: lookup_country_codes_demo
source_type: s3_batch                    # unchanged
filename_pattern: '^country_codes_(?P<bd>\d{8})\.csv$'
delivery: direct_postgres                # NEW value (existing column)
raw_format: csv
is_canonical: true                       # if false, transform runs inline
write_mode: upsert
key_fields: [country_code]
schema_id: ods.insurance.lookup_country_codes_demo-value
postgres_target_table: ods.insurance_lookup_country_codes_demo
s3_curated_path: s3://ods-curated-local/insurance/lookup_country_codes_demo/
recon_tolerance_records: 0
recon_tolerance_pct: 0
schema_def:
  fields:
    - {name: country_code, type: string}
    - {name: country_name, type: string}
dq_rules:
  hard_blocks: []
transform:                               # only used when is_canonical=false
  fields: []
  required: []
```

`target_topic` and `canonical_topic` are NOT required for this
delivery. yaml_loader will accept them as NULL.

## Migrations

### Migration 29 — extend delivery CHECK constraint

```sql
BEGIN;

ALTER TABLE pipeline.dataset_config
    DROP CONSTRAINT IF EXISTS dataset_config_delivery_chk;

ALTER TABLE pipeline.dataset_config
    ADD CONSTRAINT dataset_config_delivery_chk
    CHECK (delivery IN ('file_pipeline', 'direct_kafka', 'direct_postgres'));

COMMIT;
```

### Migration 30 — demo target table

Standard ODS metadata columns plus business columns; PK on
`(country_code)` for the upsert-mode demo.

## yaml_loader changes

Two relaxations:

1. `target_topic` is required today. For `delivery in ('direct_postgres')`,
   accept NULL — there is no Kafka topic.
2. The existing `delivery` round-trip (added in migration 28) needs no
   change.

## dag_ingest dispatcher

Today `dag_ingest.init_run` branches on `is_canonical`. Add a higher-
level branch on `dataset_config.delivery`:

```python
if delivery == "direct_postgres":
    init_run -> stage_ingest -> stage_postgres_write -> finalise
else:
    init_run -> stage_ingest -> stage_publish -> route_canonicalize
              -> [skip_canonicalize | stage_canonicalize]
              -> select_sink_run -> wait_sinks -> finalise
```

`stage_postgres_write` is a new DockerOperator that runs the new Glue
job below. `select_sink_run` and `wait_sinks` are skipped — no Connect
sink to wait on.

## New Glue job — ods_postgres_write.py

Single job, single responsibility: curated Parquet → Postgres.

```python
def run(run_id, domain, dataset, s3_input_path, file_id,
        parent_run_id, ...):
    config = load_dataset_config(...)
    spark = build_spark(...)

    # 1. Read curated parquet (existing convention).
    df = spark.read.parquet(s3_input_path)

    # 2. Optional canonicalize. Reuse glue.jobs.canonicalize helpers.
    if not config.get("is_canonical", True):
        from canonicalize import compile_transform
        transform = load_transform_yaml(config["transform_yaml_path"])
        df = compile_transform(df, transform)

    # 3. Write to Postgres.
    write_mode = config.get("write_mode", "upsert")
    target = config["postgres_target_table"]
    if write_mode == "append":
        _spark_jdbc_append(df, target)
    elif write_mode == "upsert":
        _spark_jdbc_upsert(df, target, key_fields=config["key_fields"])
    else:
        raise ValueError(f"unsupported write_mode={write_mode!r}")

    # 4. Reconciliation: curated_count vs postgres_count.
    curated_count = df.count()
    postgres_count = _count_for_run(target, run_id)
    reconciliation.write_check(
        check_type="direct_postgres_count",
        source_count=curated_count,
        postgres_count=postgres_count,
        status="ok" if curated_count == postgres_count else "failed",
    )

    # 5. Lineage edge curated_to_postgres.
    lineage.write_edge(
        child_run_id=run_id,
        parent_file_id=file_id,
        edge_type="curated_to_postgres",
        source_ref=s3_input_path,
        target_ref=f"jdbc:postgresql://.../{target}",
        record_count=curated_count,
    )

    # 6. file_catalogue.state=sunk via existing helper.
```

### Upsert strategy

Spark JDBC has no native UPSERT. Standard pattern:

1. Write to a unique stage table: `<target>_stage_<run_id>`.
2. Run `MERGE` (Postgres `INSERT ... ON CONFLICT (key_fields) DO UPDATE SET ...`)
   from stage to target inside a single transaction.
3. Drop the stage table.

Implemented via `psycopg2` after Spark write, NOT inside Spark.

### Append strategy

Direct `df.write.format("jdbc").mode("append")...`. Postgres handles
serialization; PK violations fail loudly (no silent dedup) — caller
should set `write_mode='append'` only on history-style tables without
PK.

## Reconciliation contract

Drops T0 / T1 / T2. Adds:

| Check | When | Source | Target | Status |
|---|---|---|---|---|
| `direct_postgres_count` | After Spark JDBC write | curated row count | postgres rows tagged with `_ods_run_id` | tolerance from `dataset_config.recon_tolerance_*` |
| existing `current_history_row_value` | Hourly via `dag_recon_t2` | unchanged | unchanged | unchanged |

`dag_recon_t2._is_source_run` already includes `'ingestion'` as a
source. For direct_postgres, the `ingestion` run IS the authoritative
source (since there's no canonicalize run). Update T2 logic so
`direct_postgres` rows are reconciled against the ingestion run, not a
non-existent canonicalize run.

## Lineage

Three edges, all keyed off `parent_file_id`:

```
api_pull / sftp -> file_catalogue          (existing)
file_catalogue -> S3 curated  (raw_to_curated)        (existing, written by ods_ingestion)
S3 curated     -> Postgres    (curated_to_postgres)   (NEW, written by ods_postgres_write)
```

Reuse the existing `lineage.write_edge` helper.

## Failure / recovery

| Failure | Behaviour |
|---|---|
| Curated parquet missing or empty | run failed; file_catalogue.state='failed'; no Postgres write |
| Spark JDBC write fails (FK, type mismatch) | run failed; transaction rolled back; file_catalogue.state='failed'; replay safe (the file_id is a stable ON CONFLICT key for the next run) |
| Upsert merge fails mid-run | stage table left for ops debugging; merge transaction rolled back; nothing committed |
| Concurrent direct_postgres runs for same file_id | dag_drop_to_raw idempotency on `(domain, dataset, s3_raw_path)` already prevents double-trigger |
| dag_ingest finalise fails after Spark write succeeded | rare; postgres rows present but file_catalogue.state still 'curated'; ops re-runs finalise via existing replay tooling |

## Replay

Same as file_pipeline:

- Replay by `file_id` via `dag_ingest` trigger conf
  (`replay_of_run_id` already supported).
- The Glue job's upsert merges by `key_fields`, so replay is naturally
  idempotent for upsert datasets.
- For append datasets, replay duplicates rows; ops responsibility (or
  add a `_ods_run_id` predicate to the merge to dedup).

## Migration plan from existing datasets

Per dataset:

1. Add `delivery: direct_postgres` to YAML.
2. Drop the JDBC Connect sink connector (manual `curl DELETE`).
3. Drop the Kafka raw + canonical topics if not used by anything else.
4. Trigger `dag_config_sync` to update `dataset_config.delivery`.
5. Next `dag_ingest` invocation routes to the new Glue job.

`dag_drop_to_raw` is unchanged.

## Tests required before ship

- Unit: write_mode dispatch (`append` vs `upsert` vs unsupported).
- Unit: inline canonicalize is invoked iff `is_canonical=false`.
- Integration (live): drop a CSV, run dag_ingest with delivery=direct_postgres,
  assert curated parquet exists, assert target Postgres rows match
  curated count, assert recon row, assert lineage edge.
- Integration (live): write_mode=upsert — second run with overlapping
  keys updates rows in place.
- Integration (live): write_mode=append — second run inserts new rows.
- Integration (live): non-canonical — transform_yaml is applied; output
  columns match the canonical schema, not the raw file shape.
- Integration: failure — bad row triggers Postgres FK violation; whole
  run rolled back.

## Open decisions

| Decision | Default if not pushed back |
|---|---|
| Stage-table-and-merge vs `INSERT ON CONFLICT` per-row | stage-and-merge — single tx, scales to ≥10k rows |
| `_ods_run_id` predicate on merge for replay | yes — guards append datasets against duplicate replay |
| Run inline canonicalize OR force pre-canonicalized curated | inline — keeps the YAML single-source-of-truth |
| Where canonicalize lives — separate Glue job or in `ods_postgres_write` | inline in `ods_postgres_write` — fewer DAG tasks; canonicalize is lightweight Spark |
| New target naming convention (`ods_<dataset>` vs `lookup_<dataset>`) | reuse `postgres_target_table` from dataset_config; convention is the YAML's job |
| Target table FK to `pipeline.run_log.run_id` | NO — keeps target tables decoupled from control-plane evolution; `_ods_run_id` is a string column |

## Out of scope (this design)

- Multi-table writes from one job.
- Non-Postgres targets (BigQuery, Snowflake) via Spark JDBC. Easy
  follow-up; same job, different driver.
- Streaming-from-curated. The shape is batch-only by design.
- Schema migrations on the target table. `auto.evolve` is a Connect
  feature; Spark JDBC has no equivalent. Schema changes are a manual
  Postgres migration.
