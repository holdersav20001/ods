# glue/jobs/ods_postgres_write.py
"""ODS Glue job — curated Parquet → Postgres (no Kafka leg).

Companion to ``ods_ingestion.py`` (CSV/JSONL → curated Parquet) for
``delivery=direct_postgres`` datasets. Skips Kafka publish + Connect
sink; writes Postgres rows directly via Spark JDBC.

Two write modes, YAML-driven via ``dataset_config.write_mode``:

  * ``append``  — straight ``df.write.format("jdbc").mode("append")``.
                 No PK; safe for history / audit tables.
  * ``upsert`` — stage-table-and-merge: Spark JDBC writes to
                 ``<target>_stage_<run_id_short>`` (overwrite mode),
                 then a single transactional ``INSERT ... ON CONFLICT
                 ... DO UPDATE`` merges into the target keyed on
                 ``dataset_config.key_fields``. Stage table is
                 dropped on success.

Inline canonicalize for ``is_canonical=false`` is **not** in this
slice; non-canonical datasets are deferred (see
docs/file-direct-postgres-design.md item §6 follow-up).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import uuid

# Repo root on sys.path so ods_pipeline imports work in Glue.
_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from canonicalize import apply_transform, load_mapping  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from utils import load_dataset_config  # noqa: E402

import ods_pipeline  # noqa: E402

Stage = ods_pipeline.Stage
StageEvent = ods_pipeline.StageEvent


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _build_spark(dataset: str) -> SparkSession:
    # The PostgreSQL JDBC driver is baked into the ods-glue:local image
    # under $SPARK_HOME/jars (see glue/Dockerfile, POSTGRES_JDBC_VERSION
    # ARG). No --packages flag is required at spark-submit time, which
    # avoids the 5-10s Maven resolution cost on every direct_postgres run.
    return (
        SparkSession.builder
        .appName(f"ods_postgres_write_{dataset}")
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


def _jdbc_url() -> str:
    host = os.environ.get("POSTGRES_HOST", "postgres")
    port = os.environ.get("POSTGRES_PORT", "5432")
    db = os.environ.get("POSTGRES_DB", "ods_dev")
    return f"jdbc:postgresql://{host}:{port}/{db}"


def _jdbc_props() -> dict[str, str]:
    return {
        "user": os.environ.get("POSTGRES_USER", "ods"),
        "password": os.environ.get("POSTGRES_PASSWORD", "ods"),
        "driver": "org.postgresql.Driver",
    }


def _split_table(target: str) -> tuple[str, str]:
    if "." not in target:
        raise ValueError(f"postgres_target_table must be schema.table: {target!r}")
    schema, table = target.split(".", 1)
    if not schema.isidentifier() or not table.isidentifier():
        raise ValueError(f"non-identifier schema or table in {target!r}")
    return schema, table


def _quote_ident(ident: str) -> str:
    if not ident.replace("_", "").isalnum():
        raise ValueError(f"unsafe identifier: {ident!r}")
    return f'"{ident}"'


def _short(run_id: str) -> str:
    return uuid.UUID(run_id).hex[:12]


# ---------------------------------------------------------------------------
# Write mode dispatch
# ---------------------------------------------------------------------------


def _write_append(df, *, target: str) -> None:
    """Direct append; the target must be a no-PK history table."""
    df.write.format("jdbc") \
        .option("url", _jdbc_url()) \
        .option("dbtable", target) \
        .options(**_jdbc_props()) \
        .mode("append") \
        .save()


def _write_upsert(df, *, target: str, key_fields: list[str], run_id: str) -> None:
    """Stage-table-and-merge upsert. Idempotent on key_fields.

    Spark writes the curated rows into a per-run stage table; a single
    psycopg2 transaction merges them into the target via INSERT ... ON
    CONFLICT and drops the stage. If the merge fails the stage table
    is left in place for ops debugging.
    """
    if not key_fields:
        raise ValueError("upsert write_mode requires non-empty key_fields")

    schema, table = _split_table(target)
    stage_table = f"{table}_stage_{_short(run_id)}"
    stage_qualified = f"{schema}.{stage_table}"

    df.write.format("jdbc") \
        .option("url", _jdbc_url()) \
        .option("dbtable", stage_qualified) \
        .options(**_jdbc_props()) \
        .option("truncate", "false") \
        .mode("overwrite") \
        .save()

    conn = ods_pipeline.connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema=%s AND table_name=%s "
                "ORDER BY ordinal_position",
                (schema, stage_table),
            )
            cols = [r[0] for r in cur.fetchall()]
            if not cols:
                raise RuntimeError(
                    f"stage table {stage_qualified} not found after Spark write"
                )

            insert_cols = ", ".join(_quote_ident(c) for c in cols)
            select_cols = ", ".join(f"s.{_quote_ident(c)}" for c in cols)
            update_cols = ", ".join(
                f"{_quote_ident(c)}=EXCLUDED.{_quote_ident(c)}"
                for c in cols if c not in key_fields
            )
            key_list = ", ".join(_quote_ident(k) for k in key_fields)

            merge_sql = (
                f"INSERT INTO {_quote_ident(schema)}.{_quote_ident(table)} "
                f"({insert_cols}) "
                f"SELECT {select_cols} FROM {_quote_ident(schema)}."
                f"{_quote_ident(stage_table)} s "
                f"ON CONFLICT ({key_list}) DO UPDATE SET {update_cols}"
            )
            cur.execute(merge_sql)
            cur.execute(
                f"DROP TABLE {_quote_ident(schema)}.{_quote_ident(stage_table)}"
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _dispatch_write(df, *, write_mode: str, target: str,
                   key_fields: list[str], run_id: str) -> None:
    if write_mode == "append":
        _write_append(df, target=target)
    elif write_mode == "upsert":
        _write_upsert(df, target=target, key_fields=key_fields, run_id=run_id)
    else:
        raise ValueError(
            f"unsupported write_mode={write_mode!r}; expected 'append' or 'upsert'"
        )


def _postgres_count_for_run(conn, target: str, run_id: str) -> int:
    schema, table = _split_table(target)
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT COUNT(*) FROM {_quote_ident(schema)}.{_quote_ident(table)} "
            f"WHERE _ods_run_id = %s",
            (run_id,),
        )
        return int(cur.fetchone()[0])


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def run(*, run_id: str, domain: str, dataset: str, s3_input_path: str,
        file_id: str | None = None,
        parent_run_id: str | None = None,
        airflow_dag_id: str | None = None,
        airflow_run_id: str | None = None) -> int:
    conn = ods_pipeline.connect()
    spark_app_id: str | None = None

    def _ws(**kw):
        ods_pipeline.stages.write(
            conn, run_id=run_id,
            airflow_dag_id=airflow_dag_id,
            airflow_run_id=airflow_run_id,
            spark_app_id=spark_app_id,
            **kw,
        )

    try:
        config = load_dataset_config(conn, domain, dataset)
        target = config.get("postgres_target_table")
        if not target:
            raise ValueError(
                f"dataset {domain}/{dataset}: postgres_target_table required"
            )
        write_mode = (config.get("write_mode") or "upsert").lower()
        key_fields = config.get("key_fields") or []
        if isinstance(key_fields, str):
            key_fields = json.loads(key_fields)
        is_canonical = bool(config.get("is_canonical", True))
        transform_yaml_path = config.get("transform_yaml_path")

        ods_pipeline.runs.start(
            conn,
            run_id=run_id,
            pipeline_type="direct_postgres",
            domain=domain,
            dataset=dataset,
            business_date=None,
            file_id=file_id,
            parents=[{"run_id": parent_run_id, "edge_type": "orchestrates"}]
            if parent_run_id else None,
        )
        ods_pipeline.stages.start(
            conn,
            run_id=run_id,
            stage=Stage.CURATED_READ,
            input_ref=s3_input_path,
            airflow_dag_id=airflow_dag_id,
            airflow_run_id=airflow_run_id,
            spark_app_id=spark_app_id,
        )

        spark = _build_spark(dataset)
        spark_app_id = spark.sparkContext.applicationId

        s3a_path = s3_input_path.replace("s3://", "s3a://")
        df = spark.read.parquet(s3a_path)

        # Inline canonicalize for non-canonical datasets. Curated parquet
        # carries source-shape columns (e.g. RskID, AsOfDt); the target
        # table holds canonical-shape columns (risk_id, as_of_date).
        # apply_transform produces ONLY the mapped business columns and
        # drops ODS metadata, so we re-attach the metadata block after
        # transforming. Required-field failures in this slice raise —
        # non-canonical direct_postgres datasets are reference / lookup
        # style, so a hard failure is correct here (not a DLQ flow).
        if not is_canonical:
            if not transform_yaml_path:
                raise ValueError(
                    f"dataset {domain}/{dataset}: is_canonical=false "
                    f"requires transform_yaml_path"
                )
            ods_metadata_cols = [c for c in df.columns if c.startswith("_ods_")]
            mapping = load_mapping(transform_yaml_path)
            transformed, fail_df, warnings = apply_transform(df, mapping)
            fail_count = fail_df.count() if fail_df is not None else 0
            if fail_count > 0:
                raise RuntimeError(
                    f"transform required-field failures: {fail_count} rows; "
                    f"warnings={warnings}"
                )
            # Re-attach ODS metadata via row-aligned join on a synthetic
            # row index. apply_transform preserves row order and count
            # via selectExpr; the join just stitches the columns back.
            from pyspark.sql.window import Window
            row_window = Window.orderBy(F.monotonically_increasing_id())
            df_with_idx = df.withColumn("_row_idx", F.row_number().over(row_window))
            tx_with_idx = transformed.withColumn(
                "_row_idx", F.row_number().over(row_window),
            )
            df = (
                tx_with_idx
                .join(
                    df_with_idx.select("_row_idx", *ods_metadata_cols),
                    on="_row_idx",
                    how="inner",
                )
                .drop("_row_idx")
            )

        # Curated parquet was written by ods_ingestion with its own
        # _ods_run_id; rebrand each row with THIS write's run_id so
        # downstream recon (curated count vs postgres rows tagged with
        # this run_id) matches and dashboards can locate "what did this
        # write touch?". The ingest run is still recoverable via
        # _ods_file_id → file_catalogue → run_log.
        df = df.withColumn("_ods_run_id", F.lit(run_id))
        curated_count = df.count()

        ods_pipeline.stages.finish(
            conn,
            run_id=run_id,
            stage=Stage.CURATED_READ,
            status="succeeded",
            event_type=StageEvent.COMPLETED,
            input_ref=s3_input_path,
            record_count_out=curated_count,
            airflow_dag_id=airflow_dag_id,
            airflow_run_id=airflow_run_id,
            spark_app_id=spark_app_id,
        )

        ods_pipeline.stages.start(
            conn,
            run_id=run_id,
            stage=Stage.SINK_PG_WAIT,
            input_ref=s3_input_path,
            airflow_dag_id=airflow_dag_id,
            airflow_run_id=airflow_run_id,
            spark_app_id=spark_app_id,
        )

        _dispatch_write(
            df,
            write_mode=write_mode,
            target=target,
            key_fields=list(key_fields),
            run_id=run_id,
        )

        # Reconciliation: curated row count vs postgres rows tagged
        # with this run_id. Direct equality check; tolerance honoured
        # via dataset_config.recon_tolerance_records.
        postgres_count = _postgres_count_for_run(conn, target, run_id)
        tolerance = int(config.get("recon_tolerance_records") or 0)
        ok = abs(curated_count - postgres_count) <= tolerance

        ods_pipeline.reconciliation.write_check(
            conn,
            check_type="direct_postgres_count",
            run_id=run_id,
            domain=domain,
            dataset=dataset,
            business_date=None,
            source_count=curated_count,
            postgres_count=postgres_count,
            status="ok" if ok else "failed",
            detail=json.dumps({
                "curated_count": curated_count,
                "postgres_count": postgres_count,
                "write_mode": write_mode,
                "tolerance_records": tolerance,
            }, sort_keys=True),
        )

        # parent_file_id alone is sufficient to anchor lineage; we
        # intentionally do NOT pass parent_run_id here because the
        # orchestration run_id (when present) is already linked via
        # run_log.parents and lineage_edge.parent_run_id has an FK to
        # run_log.run_id which the standalone job cannot guarantee.
        ods_pipeline.lineage.write_edge(
            conn,
            child_run_id=run_id,
            parent_file_id=file_id,
            edge_type="curated_to_postgres",
            source_ref=s3_input_path,
            target_ref=f"jdbc:postgresql://.../{target}",
            record_count=curated_count,
        )

        _ws(
            stage=Stage.SINK_PG_WAIT,
            event_type=StageEvent.COMPLETED if ok else StageEvent.FAILED,
            status="succeeded" if ok else "failed",
            input_ref=s3_input_path,
            output_ref=f"jdbc:postgresql://.../{target}",
            record_count_in=curated_count,
            record_count_out=postgres_count,
            metrics={
                "write_mode": write_mode,
                "curated_count": curated_count,
                "postgres_count": postgres_count,
            },
            error=None if ok else (
                f"direct_postgres recon mismatch: curated={curated_count} "
                f"postgres={postgres_count} tolerance={tolerance}"
            ),
        )

        if file_id:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE pipeline.file_catalogue "
                    "SET state='sunk', state_updated_at=NOW(), last_run_id=%s "
                    "WHERE file_id=%s",
                    (run_id, file_id),
                )
            conn.commit()

        ods_pipeline.runs.update(
            conn, run_id,
            status="succeeded" if ok else "failed",
            record_count_source=curated_count,
            record_count_published=postgres_count,
            error_summary=None if ok else "direct_postgres reconciliation failed",
        )

        spark.stop()
        return 0 if ok else 1
    except Exception as exc:
        try:
            ods_pipeline.runs.update(
                conn, run_id, status="failed",
                error_summary=f"direct_postgres write failed: {exc}",
            )
        except Exception:
            pass
        raise
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="ODS curated Parquet → Postgres direct write"
    )
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--domain", required=True)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--s3_input_path", required=True,
                        help="curated parquet path, e.g. "
                             "s3://ods-curated-local/<domain>/<ds>/date=...")
    parser.add_argument("--file_id", default=None)
    parser.add_argument("--parent_run_id", default=None)
    parser.add_argument("--airflow_dag_id", default=None)
    parser.add_argument("--airflow_run_id", default=None)
    return parser.parse_args(argv)


if __name__ == "__main__":
    args = _parse_args()
    sys.exit(run(
        run_id=args.run_id,
        domain=args.domain,
        dataset=args.dataset,
        s3_input_path=args.s3_input_path,
        file_id=args.file_id,
        parent_run_id=args.parent_run_id,
        airflow_dag_id=args.airflow_dag_id,
        airflow_run_id=args.airflow_run_id,
    ))
