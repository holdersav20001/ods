"""Drive a couple of real pipeline runs end-to-end and LEAVE THE DATA.

Unlike the integration test suites, this script never wipes the
pipeline.* tables — so the dashboard has something real to browse.

Workflows driven:
  1. Single-file direct-PG  (raw csv -> curated parquet -> postgres)
  2. Single-file with canonicalize (raw -> curated -> canonical -> pg)
  3. Multi-file merge with per-slot canonicalize
     (2 raw csvs -> slot_staging -> canonical parquet -> ods.policies_enriched)

Usage:
    python ops/lineage_dashboard/demo_run.py [workflow]
        workflow := all | direct_pg | canonicalize | merge

Each step is a real spark-submit through the registered glue jobs.
The script imports helper functions from the integration test files
but does NOT use any fixtures, so nothing gets torn down.
"""
from __future__ import annotations

import argparse
import os
import sys
import uuid
from pathlib import Path

import boto3
import psycopg2


_REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO_ROOT))

# Re-use test helpers (regular functions only — no fixtures).
from tests.integration.test_file_direct_pg_e2e_live import (  # type: ignore
    APPEND_DATASET, CURATED_BUCKET, DOMAIN, NETWORK, RAW_BUCKET, REPO_ROOT,
    UPSERT_DATASET,
    _curated_path, _register_file, _run_ingestion, _run_postgres_write,
    _upload_csv,
)
from tests.integration.test_canonicalize_e2e_live import (  # type: ignore
    _run_canonicalize, _run_postgres_write_from_canonical,
    _canonical_s3_path,
)
from tests.integration.test_multi_file_pipeline import (  # type: ignore
    BD, CORE_CSV, CORE_KEY, CORE_PATH, ENRICH_CSV, ENRICH_KEY, ENRICH_PATH,
    _merge, _merge_run_id_for, _stage,
)
from tests.integration.test_multi_file_canonicalize_e2e_live import (  # type: ignore
    _canonicalize_slot,
)


def _s3():
    return boto3.client(
        "s3", endpoint_url=os.environ.get("S3_ENDPOINT", "http://localhost:4566"),
        aws_access_key_id="test", aws_secret_access_key="test",
        region_name="eu-west-1",
    )


def _pg():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", "5440")),
        dbname="ods_dev", user="ods", password="ods",
    )


def _wipe_demo_data():
    """Clear any rows left from a previous demo_run so a re-run starts
    with a clean control-plane and re-uploads files don't FK-collide.
    Also clears the raw + curated + canonical S3 prefixes so stale
    parquet files from a previous demo can't poison the next run."""
    s3 = _s3()
    for bucket, prefixes in (
        (RAW_BUCKET, ["insurance/file_direct_pg_upsert_demo/",
                      "insurance/file_direct_pg_append_demo/",
                      "insurance/policies_core/",
                      "insurance/policies_enrichment/"]),
        (CURATED_BUCKET, ["insurance/file_direct_pg_upsert_demo/",
                           "insurance/file_direct_pg_append_demo/",
                           "canonical/insurance/file_direct_pg_upsert_demo/",
                           "canonical/insurance/file_direct_pg_append_demo/",
                           "canonical/insurance/policies_core/",
                           "canonical/insurance/policies_enrichment/"]),
    ):
        for prefix in prefixes:
            for page in s3.get_paginator("list_objects_v2").paginate(
                Bucket=bucket, Prefix=prefix):
                keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
                if keys:
                    s3.delete_objects(Bucket=bucket, Delete={"Objects": keys})
    pg = _pg()
    try:
        with pg.cursor() as cur:
            datasets = (
                "file_direct_pg_upsert_demo", "file_direct_pg_append_demo",
                "policies_core", "policies_enrichment", "policies_enriched",
            )
            # run_log carries an FK to file_catalogue, so it must be
            # cleared first. lineage_link / lineage_edge / run_stage_log
            # are CASCADE-deleted from run_log (migration 36).
            cur.execute(
                "DELETE FROM pipeline.run_log "
                "WHERE domain='insurance' AND dataset = ANY(%s)",
                (list(datasets),),
            )
            cur.execute(
                "DELETE FROM pipeline.file_catalogue "
                "WHERE domain='insurance' AND dataset = ANY(%s)",
                (list(datasets),),
            )
            # file_processing_attempt remembers 'completed' state for the
            # raw S3 paths, which causes ods_ingestion / ods_stage to
            # short-circuit ("File already in completed state — skipping.")
            # on the second run with the same path. Wipe it too.
            cur.execute(
                "DELETE FROM pipeline.file_processing_attempt "
                "WHERE s3_path LIKE 's3://ods-raw-local/insurance/%'"
            )
            cur.execute("DELETE FROM ods.insurance_file_direct_pg_upsert_demo")
            cur.execute("DELETE FROM ods.insurance_file_direct_pg_append_demo")
            cur.execute("DELETE FROM ods.policies_enriched")
            cur.execute("DELETE FROM pipeline.slot_staging_core")
            cur.execute("DELETE FROM pipeline.slot_staging_enrichment")
        pg.commit()
    finally:
        pg.close()


def _ensure_dataset_config():
    """Make sure dataset_config has the demo rows the glue jobs query.

    The integration test fixtures normally do this but get wiped at the
    end. Here we sync once and leave them in place.
    """
    sys.path.insert(0, str(_REPO_ROOT / "airflow" / "dags" / "common"))
    from yaml_loader import sync_to_db  # type: ignore
    pg = _pg()
    try:
        for f in (
            "patterns/insurance/file_direct_pg_upsert_demo.yaml",
            "patterns/insurance/file_direct_pg_append_demo.yaml",
        ):
            sync_to_db(os.path.join(REPO_ROOT, f), pg)
    finally:
        pg.close()


def workflow_direct_pg():
    print("\n=== Workflow 1: single-file direct-PG ===")
    s3 = _s3()
    pg = _pg()
    try:
        bd = "20260520"
        csv = "country_code,country_name\nGB,United Kingdom\nFR,France\n"
        raw, iso = _upload_csv(
            s3, dataset=UPSERT_DATASET,
            business_date_yyyymmdd=bd, filename_prefix="country_codes",
            content=csv,
        )
        file_id = _register_file(pg, dataset=UPSERT_DATASET,
                                 s3_raw_path=raw, iso=iso, content=csv)
        parent = str(uuid.uuid4())
        ingest_run = str(uuid.uuid4())
        pg_run = str(uuid.uuid4())
        r1 = _run_ingestion(run_id=ingest_run, file_id=file_id,
                            s3_input_path=raw, upstream_run_id=parent,
                            dataset=UPSERT_DATASET)
        assert r1.returncode == 0, f"INGEST STDOUT TAIL:\n{r1.stdout[-2500:]}\n\nSTDERR TAIL:\n{r1.stderr[-1000:]}"
        r2 = _run_postgres_write(
            run_id=pg_run, file_id=file_id,
            curated_path=_curated_path(UPSERT_DATASET, iso),
            upstream_run_id=parent, dataset=UPSERT_DATASET,
        )
        assert r2.returncode == 0, f"STDOUT TAIL:\n{r2.stdout[-2500:]}\n\nSTDERR TAIL:\n{r2.stderr[-1000:]}"
        print(f"  ingest_run = {ingest_run}")
        print(f"  pg_run     = {pg_run}")
    finally:
        pg.close()


def workflow_canonicalize():
    """Workflow 2 uses the APPEND dataset so its target table
    (ods.insurance_file_direct_pg_append_demo) is separate from
    workflow 1's (ods.insurance_file_direct_pg_upsert_demo). That way
    each workflow's 'recent writes (N)' panel only shows its own
    write events — no cross-workflow pollution."""
    print("\n=== Workflow 2: single-file with canonicalize ===")
    s3 = _s3()
    pg = _pg()
    try:
        bd = "20260521"
        # Append demo target shape: event_id (PK-ish), payload (text)
        csv = (
            "event_id,payload\n"
            "evt-100,login attempt user=42\n"
            "evt-101,logout user=42\n"
        )
        raw, iso = _upload_csv(
            s3, dataset=APPEND_DATASET,
            business_date_yyyymmdd=bd, filename_prefix="events",
            content=csv,
        )
        file_id = _register_file(pg, dataset=APPEND_DATASET,
                                 s3_raw_path=raw, iso=iso, content=csv)
        parent = str(uuid.uuid4())
        ingest_run = str(uuid.uuid4())
        canon_run  = str(uuid.uuid4())
        pg_run     = str(uuid.uuid4())
        r1 = _run_ingestion(run_id=ingest_run, file_id=file_id,
                            s3_input_path=raw, upstream_run_id=parent,
                            dataset=APPEND_DATASET)
        assert r1.returncode == 0, f"INGEST STDOUT TAIL:\n{r1.stdout[-2500:]}\n\nSTDERR TAIL:\n{r1.stderr[-1000:]}"
        curated = _curated_path(APPEND_DATASET, iso)
        r2 = _run_canonicalize(
            run_id=canon_run, s3_curated_path=curated,
            business_date=iso, upstream_run_id=ingest_run,
            dataset=APPEND_DATASET,
        )
        assert r2.returncode == 0, f"STDOUT TAIL:\n{r2.stdout[-2500:]}\n\nSTDERR TAIL:\n{r2.stderr[-1000:]}"
        canonical = _canonical_s3_path(APPEND_DATASET, iso)
        r3 = _run_postgres_write_from_canonical(
            run_id=pg_run, file_id=file_id, canonical_path=canonical,
            upstream_run_id=canon_run, dataset=APPEND_DATASET,
        )
        assert r3.returncode == 0, f"STDOUT TAIL:\n{r3.stdout[-2500:]}\n\nSTDERR TAIL:\n{r3.stderr[-1000:]}"
        print(f"  ingest_run = {ingest_run}")
        print(f"  canon_run  = {canon_run}")
        print(f"  pg_run     = {pg_run}")
    finally:
        pg.close()


def workflow_merge():
    print("\n=== Workflow 3: multi-file merge with per-slot canonicalize ===")
    s3 = _s3()
    pg = _pg()
    try:
        s3.put_object(Bucket=RAW_BUCKET, Key=CORE_KEY,   Body=CORE_CSV.encode())
        s3.put_object(Bucket=RAW_BUCKET, Key=ENRICH_KEY, Body=ENRICH_CSV.encode())
        r_core, run_core_stage = _stage(CORE_PATH, "policies_core")
        assert r_core.returncode == 0, r_core.stderr[-1500:]
        r_enr, run_enr_stage   = _stage(ENRICH_PATH, "policies_enrichment")
        assert r_enr.returncode == 0, r_enr.stderr[-1500:]

        canon_core = str(uuid.uuid4())
        canon_enr  = str(uuid.uuid4())
        r1 = _canonicalize_slot(
            run_id=canon_core, slot_dataset="policies_core",
            input_slot="core",
            staging_table="pipeline.slot_staging_core",
            business_date=BD, upstream_run_id=run_core_stage,
        )
        assert r1.returncode == 0, f"INGEST STDOUT TAIL:\n{r1.stdout[-2500:]}\n\nSTDERR TAIL:\n{r1.stderr[-1000:]}"
        r2 = _canonicalize_slot(
            run_id=canon_enr, slot_dataset="policies_enrichment",
            input_slot="enrichment",
            staging_table="pipeline.slot_staging_enrichment",
            business_date=BD, upstream_run_id=run_enr_stage,
        )
        assert r2.returncode == 0, f"STDOUT TAIL:\n{r2.stdout[-2500:]}\n\nSTDERR TAIL:\n{r2.stderr[-1000:]}"

        merge_run_id = _merge_run_id_for()
        r3 = _merge(merge_run_id)
        assert r3.returncode == 0, f"STDOUT TAIL:\n{r3.stdout[-2500:]}\n\nSTDERR TAIL:\n{r3.stderr[-1000:]}"
        print(f"  core stage      = {run_core_stage}")
        print(f"  enrich stage    = {run_enr_stage}")
        print(f"  canon core      = {canon_core}")
        print(f"  canon enrich    = {canon_enr}")
        print(f"  merge_run       = {merge_run_id}")
    finally:
        pg.close()


WORKFLOWS = {
    "direct_pg":     workflow_direct_pg,
    "canonicalize":  workflow_canonicalize,
    "merge":         workflow_merge,
}


def main():
    p = argparse.ArgumentParser()
    p.add_argument("workflow", nargs="?", default="all",
                   choices=["all", *WORKFLOWS])
    args = p.parse_args()
    _wipe_demo_data()
    _ensure_dataset_config()
    targets = WORKFLOWS.values() if args.workflow == "all" else [WORKFLOWS[args.workflow]]
    for fn in targets:
        fn()
    print("\nDone. Visit the dashboard at http://localhost:5180.")


if __name__ == "__main__":
    main()
