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

Inline canonicalize for ``is_canonical=false`` datasets:
  Curated parquet carries source-shape columns (e.g. RskID, AsOfDt)
  plus the standard ODS metadata block. The transform mapping is
  compiled to Spark SQL ``selectExpr`` expressions, and the ODS
  metadata columns are appended as passthrough expressions in the
  SAME ``selectExpr`` call. This keeps the canonicalize step a single
  narrow projection on the source DataFrame — row identity is
  preserved by construction (no shuffle, no row-aligned join, no
  ``monotonically_increasing_id`` window).

  An earlier slice attempted to re-attach metadata via a row-number
  window join after ``apply_transform``; that pattern is fragile under
  multi-partition Spark execution because two separate
  ``Window.orderBy(monotonically_increasing_id())`` evaluations are
  not guaranteed to produce identical row IDs. The single-projection
  passthrough used here eliminates that risk entirely.
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

from canonicalize import compile_transform, load_mapping  # noqa: E402
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
    """Stage-and-insert append.

    Spark JDBC writes UUID-typed columns as text. To preserve uuid types on
    the target (and the FK from _ods_lineage_link_id), the rows land in a
    per-run stage table first; a psycopg2 INSERT...SELECT with explicit
    casts copies them into the target and drops the stage.
    """
    schema, table = _split_table(target)
    stage_table = f"{table}_append_stage_{uuid.uuid4().hex[:8]}"
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
                    f"append stage table {stage_qualified} not found after Spark write"
                )
            _UUID_COLS = {"_ods_lineage_link_id"}
            insert_cols = ", ".join(_quote_ident(c) for c in cols)
            select_cols = ", ".join(
                (f"s.{_quote_ident(c)}::uuid" if c in _UUID_COLS
                 else f"s.{_quote_ident(c)}")
                for c in cols
            )
            cur.execute(
                f"INSERT INTO {_quote_ident(schema)}.{_quote_ident(table)} "
                f"({insert_cols}) SELECT {select_cols} "
                f"FROM {_quote_ident(schema)}.{_quote_ident(stage_table)} s"
            )
            cur.execute(
                f"DROP TABLE {_quote_ident(schema)}.{_quote_ident(stage_table)}"
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


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
            # Spark JDBC writes UUID-typed columns as text in the stage table;
            # cast on the SELECT so the merge into the typed target succeeds.
            _UUID_COLS = {"_ods_lineage_link_id"}
            select_cols = ", ".join(
                (f"s.{_quote_ident(c)}::uuid" if c in _UUID_COLS
                 else f"s.{_quote_ident(c)}")
                for c in cols
            )
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


def _postgres_count_for_link(conn, target: str, lineage_link_id: str) -> int:
    """Row count in target table stamped with this write event.

    After migration 36, target rows on file-batch routes carry a single
    ``_ods_lineage_link_id`` column. Reconciliation counts via that handle.
    """
    schema, table = _split_table(target)
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT COUNT(*) FROM {_quote_ident(schema)}.{_quote_ident(table)} "
            f"WHERE _ods_lineage_link_id = %s::uuid",
            (lineage_link_id,),
        )
        return int(cur.fetchone()[0])


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def run(*, run_id: str, domain: str, dataset: str, s3_input_path: str,
        file_id: str | None = None,
        upstream_run_id: str | None = None,
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
            orchestrators=[{"run_id": upstream_run_id, "edge_type": "orchestrates"}]
            if upstream_run_id else None,
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
        #
        # Row-alignment safety: we compile the transform to selectExpr
        # expressions, then APPEND the ODS metadata columns as
        # passthrough expressions in the SAME selectExpr call. This
        # makes canonicalize a single narrow projection on ``df``, so
        # each output row is a 1:1 in-place rewrite of its source row
        # — no shuffle, no row-aligned join, no monotonically_increasing
        # _id window. Required-field failures raise (no DLQ flow);
        # non-canonical direct_postgres datasets are reference style.
        if not is_canonical:
            if not transform_yaml_path:
                raise ValueError(
                    f"dataset {domain}/{dataset}: is_canonical=false "
                    f"requires transform_yaml_path"
                )
            mapping = load_mapping(transform_yaml_path)
            ods_metadata_cols = [c for c in df.columns if c.startswith("_ods_")]
            available = set(df.columns)
            # Collision guard (R3 review fix): a transform mapping that
            # names a target / derived field equal to an _ods_* column
            # name would silently shadow the ODS metadata when the
            # passthrough expressions are appended below. Refuse to
            # run rather than corrupt lineage / recon. Same semantic
            # cost as a misconfigured dataset_config — fail loud.
            ods_set = set(ods_metadata_cols)
            transform_targets = {
                str(f.get("target")) for f in mapping.get("fields", [])
                if f.get("target")
            }
            derived_targets = {
                str(d.get("target")) for d in (mapping.get("derived") or [])
                if d.get("target")
            }
            if transform_targets & ods_set:
                raise ValueError(
                    f"dataset {domain}/{dataset}: transform field targets "
                    f"collide with ODS metadata columns: "
                    f"{sorted(transform_targets & ods_set)}"
                )
            if derived_targets & ods_set:
                raise ValueError(
                    f"dataset {domain}/{dataset}: derived field targets "
                    f"collide with ODS metadata columns: "
                    f"{sorted(derived_targets & ods_set)}"
                )
            if transform_targets & derived_targets:
                raise ValueError(
                    f"dataset {domain}/{dataset}: derived targets shadow "
                    f"transform field targets: "
                    f"{sorted(transform_targets & derived_targets)}"
                )
            select_exprs, required_targets, warnings = compile_transform(
                {
                    "fields": mapping.get("fields", []),
                    "required": mapping.get("required", []),
                },
                available_columns=available,
            )
            # Append ODS metadata columns as passthrough expressions —
            # backticked so column names with leading underscores are
            # accepted as identifiers by Spark SQL.
            passthrough_exprs = [f"`{c}` AS `{c}`" for c in ods_metadata_cols]
            transformed = df.selectExpr(*select_exprs, *passthrough_exprs)
            for derived in mapping.get("derived", []) or []:
                transformed = transformed.withColumn(
                    str(derived["target"]), F.expr(derived["expr"]),
                )

            # Required-field check applied AFTER the projection — same
            # semantics as canonicalize.apply_transform but without
            # splitting the DataFrame.
            if required_targets:
                from functools import reduce
                fail_condition = reduce(
                    lambda left, right: left | right,
                    [F.col(c).isNull() for c in required_targets],
                )
                fail_count = transformed.filter(fail_condition).count()
                if fail_count > 0:
                    raise RuntimeError(
                        f"transform required-field failures: {fail_count} "
                        f"rows; warnings={warnings}"
                    )
            df = transformed

        # Curated parquet carries the ingest run's lineage handle. Strip it
        # and stamp THIS write's lineage_link_id on every row so downstream
        # recon, dashboards, and "what did this write touch?" all resolve
        # via a single column on the target table.
        #
        # Migration 36: file-batch target tables have a single
        # _ods_lineage_link_id column (no _ods_run_id / _ods_file_id).
        # Drop the curated copies of the legacy columns if present, then
        # add the new handle.
        for legacy in ("_ods_run_id", "_ods_file_id"):
            if legacy in df.columns:
                df = df.drop(legacy)
        lineage_link_id = str(uuid.uuid4())
        df = df.withColumn("_ods_lineage_link_id", F.lit(lineage_link_id))
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
        # with THIS write event's lineage_link_id. Direct equality;
        # tolerance via dataset_config.recon_tolerance_records.
        postgres_count = _postgres_count_for_link(conn, target, lineage_link_id)
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

        # Autonomous lineage: this task discovers its upstream ingestion
        # run from control tables rather than receiving it via XCom.
        # Migration 36 contract — write one lineage_link row + N
        # lineage_edge contributions, all sharing lineage_link_id. The
        # contributions list has length 1 for single-source loads; merge
        # jobs use the same write_link helper with N entries.
        # Discover the data-lineage upstream for this write event. Order:
        #   1. caller-supplied --upstream_run_id, IF it exists in run_log
        #      (the DAG passes the canonicalize run; integration tests
        #      pass a synthetic orchestration parent that is NOT a
        #      run_log row, which would FK-violate)
        #   2. most recent succeeded canonicalize run for this file
        #   3. most recent succeeded ingestion run for this file
        upstream_ingest_run = None
        if upstream_run_id:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM pipeline.run_log WHERE run_id = %s::uuid",
                    (upstream_run_id,),
                )
                if cur.fetchone():
                    upstream_ingest_run = upstream_run_id
        if not upstream_ingest_run and file_id:
            upstream_ingest_run = ods_pipeline.runs.latest_succeeded_run(
                conn, file_id=file_id, pipeline_type="canonicalize"
            ) or ods_pipeline.runs.latest_succeeded_run(
                conn, file_id=file_id, pipeline_type="ingestion"
            )

        ods_pipeline.lineage.write_link(
            conn,
            lineage_link_id=lineage_link_id,   # minted before stamping rows
            consumer_run_id=run_id,
            edge_type="curated_to_postgres",
            target_ref=f"jdbc:postgresql://.../{target}",
            record_count=postgres_count,
            contributions=[{
                "upstream_run_id": upstream_ingest_run,
                "source_file_id": file_id,
                "source_ref": s3_input_path,
                "record_count": curated_count,
            }],
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
            ods_pipeline.files.update_catalogue(
                conn, file_id, state="loaded", last_run_id=run_id,
            )

        ods_pipeline.runs.update(
            conn, run_id,
            status="succeeded" if ok else "failed",
            record_count_source=curated_count,
            record_count_target=postgres_count,
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
    parser.add_argument("--upstream_run_id", default=None)
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
        upstream_run_id=args.upstream_run_id,
        airflow_dag_id=args.airflow_dag_id,
        airflow_run_id=args.airflow_run_id,
    ))
