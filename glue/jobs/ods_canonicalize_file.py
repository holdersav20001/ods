"""ods_canonicalize_file — apply transform.yaml to curated parquet.

Phase 2 of the file-batch route. Sits between ``ods_ingestion`` (which
writes ``s3://.../curated/...``) and ``ods_postgres_write`` (which loads
into Postgres). Reads the curated parquet, applies the rename / cast /
drop rules in ``datasets/<domain>/<dataset>/transform.yaml``, and writes
canonical parquet to ``s3://.../canonical/<domain>/<dataset>/date=<bd>/``.

Control plane:
  * inserts a ``pipeline.run_log`` row with ``pipeline_type='canonicalize'``
  * records a ``pipeline.lineage_link`` bundle whose single
    ``lineage_edge`` contribution is the upstream ingestion run
  * stamps every output row with the new ``_ods_lineage_link_id`` so the
    canonical parquet is forensically traceable to the source file.

Transform YAML shape (datasets/<d>/<ds>/transform.yaml)::

    transform_version: 1
    is_canonical: true
    renames:
        old_col: new_col
    casts:
        col_name: decimal(10,2)        # any Spark SQL type expression
    drops:
        - tmp_internal_col
"""
from __future__ import annotations

import argparse
import os
import sys
import uuid
from datetime import datetime, timezone

import yaml

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import ods_pipeline
from ods_ingestion_control import (
    start_run as control_start_run,
    patch_run as control_patch_run,
)


CANONICAL_BUCKET = os.environ.get("ODS_CANONICAL_BUCKET", "ods-curated-local")


# ---------------------------------------------------------------------------
# YAML loader
# ---------------------------------------------------------------------------

def _datasets_root() -> str:
    """Resolve the datasets/ root.

    Honours ``ODS_DATASETS_ROOT`` so the job runs the same in the docker
    image (where the repo is bind-mounted) as on a developer's workstation.
    """
    override = os.environ.get("ODS_DATASETS_ROOT")
    return override if override else os.path.join(_REPO_ROOT, "datasets")


def _yaml_path(domain: str, dataset: str) -> str:
    return os.path.join(_datasets_root(), domain, dataset, "transform.yaml")


def _load_transform(domain: str, dataset: str) -> dict:
    path = _yaml_path(domain, dataset)
    if not os.path.exists(path):
        raise FileNotFoundError(f"transform.yaml not found at {path}")
    with open(path, "r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    return {
        "transform_version": int(data.get("transform_version", 1)),
        "is_canonical":      bool(data.get("is_canonical", True)),
        "renames":           dict(data.get("renames", {}) or {}),
        "casts":             dict(data.get("casts", {}) or {}),
        "drops":             list(data.get("drops", []) or []),
        "raw_path":          path,
    }


def apply_transform(df, transform: dict):
    """Apply renames → drops → casts. Returns (df, applied_actions)."""
    actions: list[dict] = []
    cols = set(df.columns)

    for old, new in transform["renames"].items():
        if old in cols:
            df = df.withColumnRenamed(old, new)
            cols.discard(old); cols.add(new)
            actions.append({"op": "rename", "from": old, "to": new})

    for col in transform["drops"]:
        if col in cols:
            df = df.drop(col)
            cols.discard(col)
            actions.append({"op": "drop", "column": col})

    for col, spark_type in transform["casts"].items():
        if col in cols:
            df = df.withColumn(col, F.col(col).cast(spark_type))
            actions.append({"op": "cast", "column": col, "type": spark_type})

    return df, actions


# ---------------------------------------------------------------------------
# Spark
# ---------------------------------------------------------------------------

def _build_spark(name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(name)
        .config("spark.hadoop.fs.s3a.endpoint",
                os.environ.get("LOCALSTACK_ENDPOINT", "http://localstack:4566"))
        .config("spark.hadoop.fs.s3a.access.key",
                os.environ.get("AWS_ACCESS_KEY_ID", "test"))
        .config("spark.hadoop.fs.s3a.secret.key",
                os.environ.get("AWS_SECRET_ACCESS_KEY", "test"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def _canonical_s3_path(domain: str, dataset: str, business_date_iso: str) -> str:
    bd_compact = business_date_iso.replace("-", "")
    return (f"s3a://{CANONICAL_BUCKET}/canonical/{domain}/{dataset}"
            f"/date={bd_compact}/")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(*,
        run_id: str,
        domain: str,
        dataset: str,
        s3_curated_path: str,
        business_date: str,
        upstream_run_id: str,
        airflow_dag_id: str | None = None,
        airflow_run_id: str | None = None) -> int:
    pg = ods_pipeline.connect()
    pg_dsn = ods_pipeline.connect_dsn() if hasattr(ods_pipeline, "connect_dsn") else None

    # Discover upstream file_id from the ingestion run.
    with pg.cursor() as cur:
        cur.execute(
            "SELECT file_id::text FROM pipeline.run_log WHERE run_id = %s::uuid",
            (upstream_run_id,),
        )
        row = cur.fetchone()
    upstream_file_id = row[0] if row and row[0] else None

    transform = _load_transform(domain, dataset)

    # Bootstrap canonical run_log row.
    control_start_run(
        pg,
        run_id=run_id, pipeline_type="canonicalize",
        domain=domain, dataset=dataset, business_date=business_date,
        file_id=upstream_file_id,
        runtime_context={
            "transform_yaml": transform["raw_path"],
            "transform_version": transform["transform_version"],
            "is_canonical": transform["is_canonical"],
            "airflow_dag_id": airflow_dag_id,
            "airflow_run_id": airflow_run_id,
            "started_at": datetime.now(timezone.utc).isoformat(),
        },
    )
    pg.commit()

    spark = _build_spark(f"ods_canonicalize_{domain}_{dataset}")
    s3a_in = s3_curated_path.replace("s3://", "s3a://")

    try:
        df = spark.read.parquet(s3a_in)
        source_count = df.count()
        df, actions = apply_transform(df, transform)

        # Stamp every output row with the lineage handle for this write.
        lineage_link_id = str(uuid.uuid4())
        df = df.withColumn("_ods_lineage_link_id", F.lit(lineage_link_id))

        s3a_out = _canonical_s3_path(domain, dataset, business_date)
        df.write.mode("overwrite").parquet(s3a_out)
        target_count = df.count()

        # Lineage: 1 link, 1 contribution (curated → canonical).
        ods_pipeline.lineage.write_link(
            pg,
            lineage_link_id=lineage_link_id,
            consumer_run_id=run_id,
            edge_type="curated_to_canonical",
            target_ref=s3a_out.replace("s3a://", "s3://"),
            record_count=target_count,
            contributions=[{
                "upstream_run_id": upstream_run_id,
                "source_file_id":  upstream_file_id,
                "source_ref":      s3_curated_path,
                "input_slot":      "canonical",
                "record_count":    source_count,
                "edge_type":       "curated_to_canonical",
            }],
        )

        control_patch_run(
            pg, run_id=run_id, fields={
                "status": "succeeded",
                "record_count_source": source_count,
                "record_count_target": target_count,
                "runtime_context": {
                    "transform_yaml": transform["raw_path"],
                    "transform_version": transform["transform_version"],
                    "is_canonical": transform["is_canonical"],
                    "actions_applied": actions,
                    "output_path": s3a_out.replace("s3a://", "s3://"),
                    "lineage_link_id": lineage_link_id,
                },
            },
        )
        pg.commit()
        return 0
    except Exception as exc:
        try:
            control_patch_run(
                pg, run_id=run_id, fields={
                    "status": "failed",
                    "error_summary": str(exc)[:500],
                },
            )
            pg.commit()
        except Exception:
            pass
        raise
    finally:
        spark.stop()
        pg.close()


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--run_id", required=True)
    p.add_argument("--domain", required=True)
    p.add_argument("--dataset", required=True)
    p.add_argument("--s3_curated_path", required=True,
                   help="s3://.../curated/.../ (input)")
    p.add_argument("--business_date", required=True,
                   help="YYYY-MM-DD")
    p.add_argument("--upstream_run_id", required=True,
                   help="run_id of the ingestion run that produced the curated parquet")
    p.add_argument("--airflow_dag_id", default=None)
    p.add_argument("--airflow_run_id", default=None)
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    sys.exit(run(
        run_id=args.run_id,
        domain=args.domain,
        dataset=args.dataset,
        s3_curated_path=args.s3_curated_path,
        business_date=args.business_date,
        upstream_run_id=args.upstream_run_id,
        airflow_dag_id=args.airflow_dag_id,
        airflow_run_id=args.airflow_run_id,
    ))
