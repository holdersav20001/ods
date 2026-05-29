"""ods_canonicalize_slot — canonicalize one merge slot's staged rows.

For multi-file merge workflows (e.g. policies_enriched = policies_core +
policies_enrichment), each slot is independently staged into a Postgres
``pipeline.slot_staging_<slot>`` table. This job runs AFTER ods_stage and
BEFORE ods_merge, per slot:

    raw -> ods_stage -> slot_staging_<slot>  (Postgres, already there)
                  ↓
                  ods_canonicalize_slot   ← THIS JOB
                  ↓
    silver parquet s3://canonical/<domain>/<slot_dataset>/date=<bd>/
                  ↓
                  ods_merge  (reads silver — phase 3b will switch the
                              merge over; for now staging remains the
                              authoritative source while we surface the
                              canonicalize lineage in the dashboard).

Control plane:
  * run_log row, ``pipeline_type='canonicalize'``
  * lineage_link bundle ``edge_type='staging_to_canonical'``
  * one lineage_edge contribution per upstream slot stage run, with
    ``slot_name=<slot>``

CLI::

    spark-submit ods_canonicalize_slot.py \
        --run_id <uuid>                       \
        --domain <domain>                     \
        --dataset <slot_dataset>              \
        --business_date YYYY-MM-DD            \
        --staging_table pipeline.slot_staging_<slot> \
        --slot_name <slot>                    \
        --upstream_run_id <stage_run_id>
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

# Re-use the YAML loader + transform application from the file-batch
# canonicalize job so behaviour stays identical across workflows.
from ods_canonicalize_file import _datasets_root, apply_transform


CANONICAL_BUCKET = os.environ.get("ODS_CANONICAL_BUCKET", "ods-curated-local")


def _load_transform(domain: str, dataset: str) -> dict:
    path = os.path.join(_datasets_root(), domain, dataset, "transform.yaml")
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


def _jdbc_url() -> str:
    host = os.environ.get("POSTGRES_HOST", "postgres")
    port = os.environ.get("POSTGRES_PORT", "5432")
    db   = os.environ.get("POSTGRES_DB",   "ods_dev")
    return f"jdbc:postgresql://{host}:{port}/{db}"


def _jdbc_props() -> dict:
    return {
        "user":     os.environ.get("POSTGRES_USER",     "ods"),
        "password": os.environ.get("POSTGRES_PASSWORD", "ods"),
        "driver":   "org.postgresql.Driver",
    }


def _silver_path(domain: str, slot_dataset: str, business_date_iso: str) -> str:
    bd = business_date_iso.replace("-", "")
    return (f"s3a://{CANONICAL_BUCKET}/canonical/{domain}/{slot_dataset}"
            f"/date={bd}/")


def run(*,
        run_id: str,
        domain: str,
        dataset: str,
        business_date: str,
        staging_table: str,
        slot_name: str,
        upstream_run_id: str,
        upstream_file_id: str | None = None,
        airflow_dag_id: str | None = None,
        airflow_run_id: str | None = None) -> int:
    pg = ods_pipeline.connect()
    transform = _load_transform(domain, dataset)

    control_start_run(
        pg,
        run_id=run_id, pipeline_type="canonicalize",
        domain=domain, dataset=dataset, business_date=business_date,
        file_id=upstream_file_id,
        runtime_context={
            "transform_yaml": transform["raw_path"],
            "transform_version": transform["transform_version"],
            "is_canonical": transform["is_canonical"],
            "slot_name": slot_name,
            "staging_table": staging_table,
            "airflow_dag_id": airflow_dag_id,
            "airflow_run_id": airflow_run_id,
            "started_at": datetime.now(timezone.utc).isoformat(),
        },
    )
    pg.commit()

    spark = _build_spark(f"ods_canonicalize_slot_{domain}_{dataset}")
    try:
        df = (
            spark.read.format("jdbc")
              .option("url", _jdbc_url())
              .options(**_jdbc_props())
              .option("dbtable",
                      f"(SELECT * FROM {staging_table} "
                      f"  WHERE _ods_business_date = DATE '{business_date}') src")
              .load()
        )
        source_count = df.count()
        df, actions = apply_transform(df, transform)

        lineage_link_id = str(uuid.uuid4())
        df = df.withColumn("_ods_lineage_link_id", F.lit(lineage_link_id))
        out_path = _silver_path(domain, dataset, business_date)
        df.write.mode("overwrite").parquet(out_path)
        target_count = df.count()

        ods_pipeline.lineage.write_link(
            pg,
            lineage_link_id=lineage_link_id,
            consumer_run_id=run_id,
            edge_type="staging_to_canonical",
            target_ref=out_path.replace("s3a://", "s3://"),
            record_count=target_count,
            contributions=[{
                "upstream_run_id": upstream_run_id,
                "source_file_id":  upstream_file_id,
                "source_ref":      f"postgres://{staging_table}",
                "slot_name":       slot_name,
                "record_count":    source_count,
                "edge_type":       "staging_to_canonical",
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
                    "actions_applied": actions,
                    "output_path": out_path.replace("s3a://", "s3://"),
                    "lineage_link_id": lineage_link_id,
                    "slot_name": slot_name,
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
    p.add_argument("--run_id",          required=True)
    p.add_argument("--domain",          required=True)
    p.add_argument("--dataset",         required=True,
                   help="Slot dataset name, e.g. policies_core")
    p.add_argument("--business_date",   required=True)
    p.add_argument("--staging_table",   required=True,
                   help="Fully qualified pg table, e.g. pipeline.slot_staging_core")
    p.add_argument("--slot_name",       required=True)
    p.add_argument("--upstream_run_id", required=True,
                   help="The ods_stage run_id that loaded slot_staging")
    p.add_argument("--upstream_file_id", default=None)
    p.add_argument("--airflow_dag_id",  default=None)
    p.add_argument("--airflow_run_id",  default=None)
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    sys.exit(run(
        run_id=args.run_id, domain=args.domain, dataset=args.dataset,
        business_date=args.business_date,
        staging_table=args.staging_table,
        slot_name=args.slot_name,
        upstream_run_id=args.upstream_run_id,
        upstream_file_id=args.upstream_file_id,
        airflow_dag_id=args.airflow_dag_id,
        airflow_run_id=args.airflow_run_id,
    ))
