"""Minimal silver-to-Postgres Glue skeleton with control-table writes.

This example is intentionally small. It assumes another job has already
created the silver/curated Parquet file and registered the original source
file in ``pipeline.file_catalogue``.

What this job demonstrates:

    1. read a silver S3 Parquet path,
    2. transform it into the Postgres target shape,
    3. add/refresh target-row linkage columns,
    4. load Postgres with Spark JDBC,
    5. write the minimum ``pipeline.*`` evidence with ``ods_pipeline``.

The code is split into small functions so developers can unit-test the
transform separately from Spark/JDBC/control-table side effects.
"""
from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

_HERE = os.path.dirname(__file__)
for _root in (
    os.path.abspath(os.path.join(_HERE, "..", "..", "..")),
    "/home/glue_user",
):
    if _root not in sys.path:
        sys.path.insert(0, _root)

import ods_pipeline  # noqa: E402

Stage = ods_pipeline.Stage
StageEvent = ods_pipeline.StageEvent


@dataclass(frozen=True)
class JobArgs:
    """Runtime inputs for the skeleton job."""

    run_id: str
    domain: str
    dataset: str
    business_date: str
    file_id: str
    s3_silver_path: str
    postgres_target_table: str
    upstream_run_id: str | None
    select_exprs: tuple[str, ...]


def build_spark(dataset: str) -> Any:
    """Build a minimal Spark session for S3 read + JDBC write."""
    from pyspark.sql import SparkSession  # noqa: WPS433

    return (
        SparkSession.builder
        .appName(f"example_silver_to_postgres_{dataset}")
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


def jdbc_url() -> str:
    """Resolve the Postgres JDBC URL used by Spark."""
    host = os.environ.get("POSTGRES_HOST", "postgres")
    port = os.environ.get("POSTGRES_PORT", "5432")
    db = os.environ.get("POSTGRES_DB", "ods_dev")
    return f"jdbc:postgresql://{host}:{port}/{db}"


def jdbc_props() -> dict[str, str]:
    """Resolve JDBC properties used by Spark."""
    return {
        "user": os.environ.get("POSTGRES_USER", "ods"),
        "password": os.environ.get("POSTGRES_PASSWORD", "ods"),
        "driver": "org.postgresql.Driver",
    }


def read_silver(spark: Any, s3_silver_path: str) -> Any:
    """Read silver/curated Parquet from S3."""
    return spark.read.parquet(s3_silver_path.replace("s3://", "s3a://"))


def transform_for_target(df: Any, select_exprs: Sequence[str]) -> Any:
    """Return target-shaped rows.

    Leave ``select_exprs`` empty for a canonical dataset.

    For a non-canonical dataset, pass Spark SQL expressions such as:

        ``RskID AS risk_id``
        ``PolNo AS policy_id``
        ``to_date(AsOfDt, 'yyyyMMdd') AS as_of_date``

    ODS metadata columns are passed through so target rows can still link back
    to ``pipeline.file_catalogue`` and ``pipeline.run_log``.
    """
    if not select_exprs:
        return df
    metadata_cols = [c for c in df.columns if c.startswith("_ods_")]
    passthrough = [f"`{c}` AS `{c}`" for c in metadata_cols]
    return df.selectExpr(*select_exprs, *passthrough)


def add_linkage_columns(df: Any, args: JobArgs) -> Any:
    """Ensure the target row links to the source file and this load run."""
    from pyspark.sql import functions as F  # noqa: WPS433

    linked = df.withColumn("_ods_run_id", F.lit(args.run_id))
    if "_ods_file_id" not in linked.columns:
        linked = linked.withColumn("_ods_file_id", F.lit(args.file_id))
    if "_ods_business_date" not in linked.columns:
        linked = linked.withColumn("_ods_business_date", F.lit(args.business_date))
    if "_ods_domain" not in linked.columns:
        linked = linked.withColumn("_ods_domain", F.lit(args.domain))
    if "_ods_dataset" not in linked.columns:
        linked = linked.withColumn("_ods_dataset", F.lit(args.dataset))
    return linked


def write_postgres(df: Any, target_table: str) -> None:
    """Append target-shaped rows into Postgres using Spark JDBC.

    This is deliberately append-only for clarity. Production upsert flows can
    swap this function for a stage-table + merge implementation.
    """
    df.write.format("jdbc") \
        .option("url", jdbc_url()) \
        .option("dbtable", target_table) \
        .options(**jdbc_props()) \
        .mode("append") \
        .save()


def split_table(table_ref: str) -> tuple[str, str]:
    schema, sep, table = table_ref.partition(".")
    if not sep or not schema.isidentifier() or not table.isidentifier():
        raise ValueError(f"target table must be safe schema.table: {table_ref!r}")
    return schema, table


def quote_ident(identifier: str) -> str:
    if not identifier.replace("_", "").isalnum():
        raise ValueError(f"unsafe identifier: {identifier!r}")
    return f'"{identifier}"'


def count_loaded_rows(conn: Any, *, target_table: str, run_id: str) -> int:
    """Count target rows written by this run."""
    schema, table = split_table(target_table)
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT COUNT(*) FROM {quote_ident(schema)}.{quote_ident(table)} "
            "WHERE _ods_run_id = %s",
            (run_id,),
        )
        return int(cur.fetchone()[0])


def start_run(conn: Any, args: JobArgs) -> None:
    """Create the direct_postgres run row."""
    orchestrators = (
        [{"run_id": args.upstream_run_id, "edge_type": "orchestrates"}]
        if args.upstream_run_id else None
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


def mark_success(
    conn: Any,
    args: JobArgs,
    *,
    source_count: int,
    target_count: int,
) -> None:
    """Write the success-path control-table evidence."""
    ods_pipeline.reconciliation.write_check(
        conn,
        check_type="direct_postgres_count",
        run_id=args.run_id,
        domain=args.domain,
        dataset=args.dataset,
        business_date=None,
        source_count=source_count,
        postgres_count=target_count,
        status="ok" if source_count == target_count else "failed",
    )
    ods_pipeline.lineage.write_edge(
        conn,
        consumer_run_id=args.run_id,
        source_file_id=args.file_id,
        edge_type="curated_to_postgres",
        source_ref=args.s3_silver_path,
        target_ref=f"jdbc:postgresql://.../{args.postgres_target_table}",
        record_count=target_count,
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
        status="succeeded",
        record_count_source=source_count,
        record_count_published=target_count,
    )


def mark_failed(conn: Any, args: JobArgs, exc: BaseException) -> None:
    """Write the failure-path control-table evidence."""
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
    """Run silver -> transform -> Postgres and write control-table evidence."""
    conn = ods_pipeline.connect()
    spark = None
    try:
        start_run(conn, args)
        spark = build_spark(args.dataset)

        read_attempt = ods_pipeline.stages.start(
            conn,
            run_id=args.run_id,
            stage=Stage.CURATED_READ,
            input_ref=args.s3_silver_path,
        )
        silver_df = read_silver(spark, args.s3_silver_path)
        source_count = silver_df.count()
        ods_pipeline.stages.finish(
            conn,
            run_id=args.run_id,
            stage=Stage.CURATED_READ,
            status="succeeded",
            event_type=StageEvent.COMPLETED,
            attempt_number=read_attempt,
            record_count_out=source_count,
        )

        transform_attempt = ods_pipeline.stages.start(
            conn,
            run_id=args.run_id,
            stage=Stage.CANONICAL_TRANSFORM,
            input_ref=args.s3_silver_path,
        )
        target_df = transform_for_target(silver_df, args.select_exprs)
        target_df = add_linkage_columns(target_df, args)
        target_count = target_df.count()
        ods_pipeline.stages.finish(
            conn,
            run_id=args.run_id,
            stage=Stage.CANONICAL_TRANSFORM,
            status="succeeded",
            event_type=StageEvent.COMPLETED,
            attempt_number=transform_attempt,
            record_count_in=source_count,
            record_count_out=target_count,
        )

        load_attempt = ods_pipeline.stages.start(
            conn,
            run_id=args.run_id,
            stage=Stage.SINK_PG_WAIT,
            input_ref=args.s3_silver_path,
            output_ref=f"jdbc:postgresql://.../{args.postgres_target_table}",
        )
        write_postgres(target_df, args.postgres_target_table)
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
            attempt_number=load_attempt,
            record_count_in=target_count,
            record_count_out=loaded_count,
        )

        mark_success(
            conn,
            args,
            source_count=target_count,
            target_count=loaded_count,
        )
        return 0
    except Exception as exc:
        mark_failed(conn, args, exc)
        return 1
    finally:
        if spark is not None:
            try:
                spark.stop()
            except Exception:
                pass
        conn.close()


def parse_args(argv: Sequence[str] | None = None) -> JobArgs:
    parser = argparse.ArgumentParser(
        description="Minimal silver-to-Postgres control-table skeleton"
    )
    parser.add_argument("--domain", required=True)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--business_date", required=True)
    parser.add_argument("--file_id", required=True)
    parser.add_argument("--s3_silver_path", required=True)
    parser.add_argument("--postgres_target_table", required=True)
    parser.add_argument("--run_id", default=None)
    parser.add_argument("--upstream_run_id", default=None)
    parser.add_argument(
        "--select_expr",
        action="append",
        default=[],
        help=(
            "Optional Spark SQL select expression. Repeat for multiple "
            "target columns. Leave empty for canonical datasets."
        ),
    )
    ns = parser.parse_args(argv)
    return JobArgs(
        run_id=ns.run_id or str(uuid4()),
        domain=ns.domain,
        dataset=ns.dataset,
        business_date=ns.business_date,
        file_id=ns.file_id,
        s3_silver_path=ns.s3_silver_path,
        postgres_target_table=ns.postgres_target_table,
        upstream_run_id=ns.upstream_run_id,
        select_exprs=tuple(ns.select_expr),
    )


if __name__ == "__main__":
    sys.exit(run(parse_args()))
