# glue/jobs/ods_ingestion.py
"""
ODS Glue ingestion job — CSV (S3 Raw) → Parquet (S3 Curated).

Thin shim. The implementation now lives in the
:mod:`glue.jobs.ingestion` package:

* ``ingestion.reading``      — business_date + Spark reader (csv | jsonl)
* ``ingestion.registration`` — head_object MD5 + file_catalogue upsert
* ``ingestion.validation``   — Schema Registry column check
* ``ingestion.quality``      — DQ rules + DLQ write
* ``ingestion.curating``     — enrichment + parquet write + count verify
* ``ingestion.finalising``   — lineage edge + state + run.status + event
* ``ingestion.pipeline``     — orchestrator wiring stage_scope per step

The CLI surface is unchanged so existing ``--py-files`` lists and
DockerOperator commands keep working.

Usage:
    spark-submit ods_ingestion.py \
        --run_id  <uuid> \
        --domain  insurance \
        --dataset policies \
        --s3_input_path s3://ods-raw-local/insurance/policies/date=20260417/policies_20260417.csv
"""

from __future__ import annotations

import argparse
import os
import sys

# Add repo root to sys.path so ods_pipeline package is importable from Glue
_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from glue.jobs.ingestion import run  # noqa: E402  re-export for callers


def _parse_args(argv=None):
    parser = argparse.ArgumentParser(description="ODS CSV → Parquet ingestion job")
    parser.add_argument("--run_id", required=True, help="Unique run identifier (UUID)")
    parser.add_argument("--domain", required=True, help="Data domain, e.g. insurance")
    parser.add_argument("--dataset", required=True, help="Dataset name, e.g. policies")
    parser.add_argument("--s3_input_path", required=True,
                        help="S3 path to input CSV, e.g. s3://ods-raw-local/...")
    parser.add_argument("--file_id", default=None,
                        help="Explicit file_id UUID from file_catalogue (passed by DAG)")
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
