# Glue Job Control-Table Python Example

This example shows the preferred Python shape for a restartable Glue-style job
that loads curated data to Postgres and writes ODS control-table evidence.

Use this pattern for application jobs. It is different from a manual SQL replay:

- the job owns one `run_id`;
- upstream identity such as `file_id` and `upstream_run_id` is passed in;
- each control-table write is a durable checkpoint through the existing helpers;
- failures are recorded in the control tables instead of being rolled back;
- restarts can inspect `run_log`, `run_stage_log`, `file_catalogue`,
  `lineage_edge`, and `reconciliation_log`.

The helpers in `ods_pipeline` call the `pipeline.control_*` Postgres functions
underneath. The Spark/JDBC pieces are included as small functions so the
control-table sequence is easy to follow and easy to test.

## How To Start It

1. Confirm the upstream job has already created the curated S3 output and the
   source file exists in `pipeline.file_catalogue`.
2. Get the upstream `file_id` from `pipeline.file_catalogue`.
3. Get the upstream `upstream_run_id` from the raw/curated run that produced the
   curated file. Pass it when you want explicit run-to-run lineage.
4. Start this job with one current `run_id`. Omit `--run_id` for a brand-new
   attempt, or pass the same `--run_id` when intentionally replaying the same
   known attempt.

Example local/Glue-style invocation:

```bash
python glue_curated_to_postgres.py \
  --domain insurance \
  --dataset policies \
  --business_date 2026-04-11 \
  --file_id 11111111-1111-1111-1111-111111111111 \
  --upstream_run_id 22222222-2222-2222-2222-222222222222 \
  --s3_curated_path s3://ods-curated-local/insurance/policies/business_date=2026-04-11/policies.parquet \
  --postgres_target_table ods.insurance_policy
```

For a restart of the same failed attempt, add the original run ID:

```bash
python glue_curated_to_postgres.py \
  --run_id 33333333-3333-3333-3333-333333333333 \
  --domain insurance \
  --dataset policies \
  --business_date 2026-04-11 \
  --file_id 11111111-1111-1111-1111-111111111111 \
  --upstream_run_id 22222222-2222-2222-2222-222222222222 \
  --s3_curated_path s3://ods-curated-local/insurance/policies/business_date=2026-04-11/policies.parquet \
  --postgres_target_table ods.insurance_policy
```

## Numbered Sequence

1. `main()` calls `parse_args()` and then `run(args)`.
2. `start_run()` writes `pipeline.run_log` with `status='running'`.
3. `read_curated_stage()` opens and closes the curated-read stage.
4. `transform_stage()` transforms rows, adds `_ods_*` linkage, and records the
   transform stage.
5. `postgres_load_stage()` writes the target table and records the loaded row
   count.
6. `mark_success()` writes reconciliation, lineage, file catalogue state, and
   the terminal run status.
7. If any step raises, `mark_failed()` records a failed run and failed file
   state before the job exits with `1`.

## YAML Files For This Workload

The Python job does not discover datasets by itself. The DAGs discover work
from `pipeline.dataset_config`, and `pipeline.dataset_config` is populated by
syncing YAML under `datasets/<domain>/<dataset>/`.

For this direct Postgres workload, the YAML sequence is:

1. `dataset.yaml` declares this as `delivery: direct_postgres`.
2. `source.yaml` describes how the raw file arrives.
3. `contract.yaml` defines schema, keys, and target field contract.
4. `quality.yaml` defines hard/soft data-quality rules.
5. `transform.yaml` defines whether the curated data is already canonical or
   needs mapping.
6. `delivery.yaml` defines the Postgres target and curated S3 location.
7. `reconciliation.yaml` defines expected checks/tolerances.
8. `dag_config_sync` merges those files and upserts `pipeline.dataset_config`.
9. `dag_drop_to_raw` sees `delivery='direct_postgres'` and triggers
   `dag_ingest_direct_postgres`.
10. The Glue job receives `file_id`, `upstream_run_id`, `s3_curated_path`, and
    `postgres_target_table` from Airflow/config.

### `dataset.yaml`

Do not set `target_topic` for `direct_postgres`; there is no Kafka leg.

```yaml
domain: insurance
dataset: policies_direct_pg
source_type: s3_batch
delivery: direct_postgres
raw_format: csv
filename_pattern: '^policies_(?P<bd>\d{8})\.csv$'

refs:
  source: source.yaml
  contract: contract.yaml
  quality: quality.yaml
  transform: transform.yaml
  delivery: delivery.yaml
  reconciliation: reconciliation.yaml
```

### `source.yaml`

This tells `dag_drop_to_raw` how to recognise and land source files.

```yaml
source:
  delivery_mechanism: drop_to_raw
  landing:
    bucket: ods-raw
    prefix: insurance/policies/
  business_date:
    source: filename_pattern
    group: bd
```

### `contract.yaml`

`key_fields` is required when `write_mode` is `upsert` or `replace`.

```yaml
schema_id: insurance.policies_direct_pg
schema_version: 1
data_classification: Internal
key_fields:
  - policy_id

fields:
  - name: policy_id
    type: string
    nullable: false
    source_name: policy_id
  - name: status
    type: string
    nullable: false
    source_name: status
  - name: premium
    type: decimal(10,2)
    nullable: true
    source_name: premium
  - name: effective_date
    type: date
    nullable: true
    source_name: effective_date
```

### `quality.yaml`

These become `dataset_config.dq_rules`.

```yaml
hard_blocks:
  - rule: not_null
    field: policy_id
  - rule: unique
    field: policy_id

soft_warns:
  - rule: completeness_pct
    fields:
      - premium
    threshold: 0.95
```

### `transform.yaml`

Use this when the curated data already matches the Postgres target shape:

```yaml
is_canonical: true
mode: identity
```

Use this shape when the job needs a mapping file before Postgres load:

```yaml
is_canonical: false
mode: mapping
transform_yaml_path: s3://ods-config/transforms/insurance/policies_direct_pg.yaml
```

### `delivery.yaml`

This is the key direct-Postgres delivery block. It must have the target table.
It must not have `target_topic`.

```yaml
write_mode: upsert
postgres_target_table: ods.insurance_policy
s3_curated_path: s3://ods-curated/insurance/policies_direct_pg/
```

### `reconciliation.yaml`

Use checks that make sense without Kafka. The direct Postgres path compares
curated/accepted rows to rows loaded into Postgres.

```yaml
tolerances:
  records: 0
  pct: 0

checks:
  - name: curated_to_postgres_count
    compares: curated_rows_to_postgres_rows
    produced_by: glue.jobs.ods_postgres_write
    required: true
  - name: direct_postgres_count
    compares: source_rows_to_postgres_rows
    produced_by: glue.jobs.examples.direct_postgres_control_table_skeleton
    required: false
```

After sync, check that Postgres has the expected config:

```sql
SELECT domain, dataset, delivery, target_topic,
       postgres_target_table, s3_curated_path, write_mode
FROM pipeline.dataset_config
WHERE domain = 'insurance'
  AND dataset = 'policies_direct_pg';
```

Expected:

```text
delivery = direct_postgres
target_topic = NULL
postgres_target_table = ods.insurance_policy
s3_curated_path = s3://ods-curated/insurance/policies_direct_pg/
write_mode = upsert
```

## Code

```python
from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

import ods_pipeline

Stage = ods_pipeline.Stage
StageEvent = ods_pipeline.StageEvent


@dataclass(frozen=True)
class JobArgs:
    """Runtime inputs for one restartable Glue job attempt."""

    run_id: str
    domain: str
    dataset: str
    business_date: str
    file_id: str
    s3_curated_path: str
    postgres_target_table: str
    upstream_run_id: str | None
    select_exprs: tuple[str, ...]


def parse_args(argv: Sequence[str] | None = None) -> JobArgs:
    """1. Parse job arguments.

    Airflow can pass `run_id` when retrying a known job attempt. If it does not,
    this job creates one run ID for this job only.
    """

    parser = argparse.ArgumentParser(
        description="Curated-to-Postgres Glue job with control-table writes"
    )
    parser.add_argument("--domain", required=True)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--business_date", required=True)
    parser.add_argument("--file_id", required=True)
    parser.add_argument("--s3_curated_path", required=True)
    parser.add_argument("--postgres_target_table", required=True)
    parser.add_argument("--run_id", default=None)
    parser.add_argument("--upstream_run_id", default=None)
    parser.add_argument(
        "--select_expr",
        action="append",
        default=[],
        help=(
            "Optional Spark SQL select expression. Repeat for multiple target "
            "columns. Leave empty when curated data already matches target shape."
        ),
    )
    ns = parser.parse_args(argv)
    return JobArgs(
        run_id=ns.run_id or str(uuid4()),
        domain=ns.domain,
        dataset=ns.dataset,
        business_date=ns.business_date,
        file_id=ns.file_id,
        s3_curated_path=ns.s3_curated_path,
        postgres_target_table=ns.postgres_target_table,
        upstream_run_id=ns.upstream_run_id,
        select_exprs=tuple(ns.select_expr),
    )


def build_spark(dataset: str) -> Any:
    """Build the Spark session used for S3 read and JDBC write."""

    from pyspark.sql import SparkSession  # noqa: WPS433

    return (
        SparkSession.builder
        .appName(f"ods_curated_to_postgres_{dataset}")
        .config("spark.hadoop.fs.s3a.endpoint", os.environ.get("LOCALSTACK_ENDPOINT", ""))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key", os.environ.get("AWS_ACCESS_KEY_ID", "test"))
        .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("AWS_SECRET_ACCESS_KEY", "test"))
        .getOrCreate()
    )


def jdbc_url() -> str:
    """Resolve the Postgres JDBC URL for Spark."""

    host = os.environ.get("POSTGRES_HOST", "postgres")
    port = os.environ.get("POSTGRES_PORT", "5432")
    db_name = os.environ.get("POSTGRES_DB", "ods_dev")
    return f"jdbc:postgresql://{host}:{port}/{db_name}"


def jdbc_props() -> dict[str, str]:
    """Resolve JDBC connection properties for Spark."""

    return {
        "user": os.environ.get("POSTGRES_USER", "ods"),
        "password": os.environ.get("POSTGRES_PASSWORD", "ods"),
        "driver": "org.postgresql.Driver",
    }


def read_curated(spark: Any, s3_curated_path: str) -> Any:
    """Read curated Parquet from S3."""

    return spark.read.parquet(s3_curated_path.replace("s3://", "s3a://"))


def transform_for_target(dataframe: Any, select_exprs: Sequence[str]) -> Any:
    """Project curated rows into the Postgres target shape.

    Leave `select_exprs` empty for identity/canonical loads.
    """

    if not select_exprs:
        return dataframe
    metadata_cols = [col for col in dataframe.columns if col.startswith("_ods_")]
    passthrough = [f"`{col}` AS `{col}`" for col in metadata_cols]
    return dataframe.selectExpr(*select_exprs, *passthrough)


def add_linkage_columns(dataframe: Any, args: JobArgs) -> Any:
    """Ensure target rows link back to file/run/domain/dataset metadata."""

    from pyspark.sql import functions as F  # noqa: WPS433

    linked = dataframe.withColumn("_ods_run_id", F.lit(args.run_id))
    if "_ods_file_id" not in linked.columns:
        linked = linked.withColumn("_ods_file_id", F.lit(args.file_id))
    if "_ods_business_date" not in linked.columns:
        linked = linked.withColumn("_ods_business_date", F.lit(args.business_date))
    if "_ods_domain" not in linked.columns:
        linked = linked.withColumn("_ods_domain", F.lit(args.domain))
    if "_ods_dataset" not in linked.columns:
        linked = linked.withColumn("_ods_dataset", F.lit(args.dataset))
    return linked


def write_postgres(dataframe: Any, target_table: str) -> None:
    """Append target-shaped rows to Postgres through Spark JDBC."""

    (
        dataframe.write.format("jdbc")
        .option("url", jdbc_url())
        .option("dbtable", target_table)
        .options(**jdbc_props())
        .mode("append")
        .save()
    )


def split_table(table_ref: str) -> tuple[str, str]:
    """Validate and split a safe `schema.table` reference."""

    schema, separator, table = table_ref.partition(".")
    if not separator or not schema.isidentifier() or not table.isidentifier():
        raise ValueError(f"target table must be safe schema.table: {table_ref!r}")
    return schema, table


def quote_ident(identifier: str) -> str:
    """Quote a known-safe SQL identifier."""

    if not identifier.replace("_", "").isalnum():
        raise ValueError(f"unsafe identifier: {identifier!r}")
    return f'"{identifier}"'


def count_loaded_rows(conn: Any, *, target_table: str, run_id: str) -> int:
    """Count rows written by this run using the target linkage column."""

    schema, table = split_table(target_table)
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT COUNT(*) FROM {quote_ident(schema)}.{quote_ident(table)} "
            "WHERE _ods_run_id = %s",
            (run_id,),
        )
        return int(cur.fetchone()[0])


def start_run(conn: Any, args: JobArgs) -> None:
    """2. Make this job visible as a running control-plane run."""

    orchestrators = (
        [{"run_id": args.upstream_run_id, "edge_type": "orchestrates"}]
        if args.upstream_run_id
        else None
    )
    ods_pipeline.runs.start(
        conn,
        run_id=args.run_id,
        pipeline_type="direct_postgres",
        domain=args.domain,
        dataset=args.dataset,
        business_date=args.business_date,
        file_id=args.file_id,
        orchestrators=orchestrators,
    )


def read_curated_stage(conn: Any, spark: Any, args: JobArgs) -> tuple[Any, int]:
    """3. Read curated data and close the read stage."""

    attempt = ods_pipeline.stages.start(
        conn,
        run_id=args.run_id,
        stage=Stage.CURATED_READ,
        input_ref=args.s3_curated_path,
    )
    dataframe = read_curated(spark, args.s3_curated_path)
    row_count = dataframe.count()
    ods_pipeline.stages.finish(
        conn,
        run_id=args.run_id,
        stage=Stage.CURATED_READ,
        status="succeeded",
        event_type=StageEvent.COMPLETED,
        attempt_number=attempt,
        input_ref=args.s3_curated_path,
        record_count_out=row_count,
    )
    return dataframe, row_count


def transform_stage(
    conn: Any,
    args: JobArgs,
    dataframe: Any,
    source_count: int,
) -> tuple[Any, int]:
    """4. Transform rows, add linkage, and close the transform stage."""

    attempt = ods_pipeline.stages.start(
        conn,
        run_id=args.run_id,
        stage=Stage.CANONICAL_TRANSFORM,
        input_ref=args.s3_curated_path,
    )
    target_dataframe = transform_for_target(dataframe, args.select_exprs)
    target_dataframe = add_linkage_columns(target_dataframe, args)
    target_count = target_dataframe.count()
    ods_pipeline.stages.finish(
        conn,
        run_id=args.run_id,
        stage=Stage.CANONICAL_TRANSFORM,
        status="succeeded",
        event_type=StageEvent.COMPLETED,
        attempt_number=attempt,
        input_ref=args.s3_curated_path,
        record_count_in=source_count,
        record_count_out=target_count,
    )
    return target_dataframe, target_count


def postgres_load_stage(
    conn: Any,
    args: JobArgs,
    dataframe: Any,
    target_count: int,
) -> int:
    """5. Write Postgres and close the load stage."""

    target_ref = f"{jdbc_url()}/{args.postgres_target_table}"
    attempt = ods_pipeline.stages.start(
        conn,
        run_id=args.run_id,
        stage=Stage.SINK_PG_WAIT,
        input_ref=args.s3_curated_path,
        output_ref=target_ref,
    )
    write_postgres(dataframe, args.postgres_target_table)
    loaded_count = count_loaded_rows(
        conn,
        target_table=args.postgres_target_table,
        run_id=args.run_id,
    )
    ods_pipeline.stages.finish(
        conn,
        run_id=args.run_id,
        stage=Stage.SINK_PG_WAIT,
        status="succeeded",
        event_type=StageEvent.COMPLETED,
        attempt_number=attempt,
        input_ref=args.s3_curated_path,
        output_ref=target_ref,
        record_count_in=target_count,
        record_count_out=loaded_count,
    )
    return loaded_count


def mark_success(
    conn: Any,
    args: JobArgs,
    *,
    source_count: int,
    loaded_count: int,
) -> None:
    """6. Write recon, lineage, file state, and terminal run state."""

    recon_status = "ok" if source_count == loaded_count else "failed"
    run_status = "succeeded" if recon_status == "ok" else "partial"
    target_ref = f"{jdbc_url()}/{args.postgres_target_table}"

    ods_pipeline.reconciliation.write_check(
        conn,
        check_type="curated_to_postgres_count",
        run_id=args.run_id,
        domain=args.domain,
        dataset=args.dataset,
        business_date=args.business_date,
        source_count=source_count,
        postgres_count=loaded_count,
        status=recon_status,
        detail="Curated accepted rows compared with Postgres loaded rows.",
    )
    ods_pipeline.lineage.write_edge(
        conn,
        consumer_run_id=args.run_id,
        upstream_run_id=args.upstream_run_id,
        source_file_id=args.file_id,
        edge_type="curated_to_postgres",
        source_ref=args.s3_curated_path,
        target_ref=target_ref,
        record_count=loaded_count,
    )
    ods_pipeline.files.update_catalogue(
        conn,
        file_id=args.file_id,
        state="sunk",
        last_run_id=args.run_id,
    )
    ods_pipeline.runs.update(
        conn,
        args.run_id,
        status=run_status,
        record_count_source=source_count,
        record_count_published=loaded_count,
    )


def mark_failed(conn: Any, args: JobArgs, exc: BaseException) -> None:
    """7. Preserve failure evidence for operators and restart."""

    summary = str(exc)[:500] or type(exc).__name__
    ods_pipeline.runs.update(
        conn,
        args.run_id,
        status="failed",
        error_summary=summary,
    )
    ods_pipeline.files.update_catalogue(
        conn,
        file_id=args.file_id,
        state="failed",
        last_run_id=args.run_id,
    )


def run(args: JobArgs) -> int:
    """Run one curated-to-Postgres job.

    The control-table helpers commit by default. That is deliberate: each stage
    becomes visible to dashboards and survives process death.
    """

    conn = ods_pipeline.connect()
    spark = None
    try:
        # 2. Durable run checkpoint: the dashboard can now see this job.
        start_run(conn, args)
        spark = build_spark(args.dataset)

        # 3. Durable stage checkpoint for reading curated S3.
        curated_dataframe, curated_count = read_curated_stage(conn, spark, args)

        # 4. Durable stage checkpoint for target transformation/linkage.
        target_dataframe, target_count = transform_stage(
            conn,
            args,
            curated_dataframe,
            curated_count,
        )

        # 5. Durable stage checkpoint for the Postgres write.
        loaded_count = postgres_load_stage(
            conn,
            args,
            target_dataframe,
            target_count,
        )

        # 6. Final durable checkpoints: recon, lineage, file state, run status.
        mark_success(
            conn,
            args,
            source_count=target_count,
            loaded_count=loaded_count,
        )
        return 0
    except Exception as exc:
        # 7. Failure checkpoint: record the error instead of losing evidence.
        mark_failed(conn, args, exc)
        return 1
    finally:
        if spark is not None:
            try:
                spark.stop()
            except Exception:
                pass
        conn.close()


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entry point used by Glue/Airflow.

    Sequence:
      1. Parse CLI/runtime arguments.
      2. Execute the restartable job.
      3. Return 0 for success or 1 for recorded failure.
    """

    return run(parse_args(argv))


if __name__ == "__main__":
    sys.exit(main())
```

## Run Shape

```text
1. main()
2. parse_args()
3. run()
4. start_run(direct_postgres)
5. read_curated_stage()
6. transform_stage()
7. postgres_load_stage()
8. mark_success()

on error after step 4 starts:
9. mark_failed()
```

The important restart rule is that each helper call writes through the
Postgres function API and commits its own checkpoint. A retry should reuse a
known `run_id` when replaying the same attempt, or use a new `run_id` for a new
attempt while preserving `file_id` and `upstream_run_id`.
