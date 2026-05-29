"""End-to-end test for the new ods_canonicalize_file Spark job.

Chain exercised
---------------
  1. upload CSV to S3 (raw)
  2. register file in pipeline.file_catalogue
  3. spark-submit ods_ingestion         → curated parquet
  4. spark-submit ods_canonicalize_file → canonical parquet  ← new step
  5. spark-submit ods_postgres_write    → ods.<table>

Assertions
----------
  * canonicalize run_log row carries pipeline_type='canonicalize',
    status='succeeded', record_count_source == record_count_target
  * pipeline.lineage_link of edge_type='curated_to_canonical' exists for
    the canonicalize run and points back to the ingestion run via one
    lineage_edge with slot_name='canonical'
  * target table rows carry _ods_lineage_link_id stamped by the postgres
    write step; the bundle's lineage_edge has upstream_run_id ==
    canonicalize_run_id (because the postgres write step's upstream is
    now the canonicalize stage, not ingestion)
  * the canonical S3 prefix exists under the curated bucket
"""
from __future__ import annotations

import json
import os
import subprocess
import time
import uuid

import boto3
import psycopg2
import pytest

# Reuse only the regular helpers from the direct-PG suite. Pytest fixtures
# can't cross test modules without sitting in conftest.py, so we redeclare
# the (cheap, scoped-module) s3_client / configs_synced / clean_state
# fixtures locally and delegate to the same upstream helper logic.
from tests.integration.test_file_direct_pg_e2e_live import (  # type: ignore
    CURATED_BUCKET, DOMAIN, NETWORK, RAW_BUCKET, REPO_ROOT, UPSERT_DATASET,
    UPSERT_TABLE, _curated_path, _glue_env_args, _register_file,
    _run_ingestion, _upload_csv,
    _ensure_buckets as _upstream_ensure_buckets,
    _ensure_environment as _upstream_ensure_environment,
    configs_synced as _upstream_configs_synced,
    clean_state as _upstream_clean_state,
    s3_client as _upstream_s3_client,
)


_ensure_environment = pytest.fixture(scope="module", autouse=True)(
    _upstream_ensure_environment.__wrapped__
)
s3_client = pytest.fixture(scope="module")(_upstream_s3_client.__wrapped__)
_ensure_buckets = pytest.fixture(scope="module", autouse=True)(
    _upstream_ensure_buckets.__wrapped__
)
configs_synced = pytest.fixture(scope="module")(
    _upstream_configs_synced.__wrapped__
)
clean_state = pytest.fixture(_upstream_clean_state.__wrapped__)


def _run_canonicalize(*, run_id, s3_curated_path, business_date,
                       upstream_run_id, dataset) -> subprocess.CompletedProcess:
    """spark-submit ods_canonicalize_file inside the glue docker image."""
    extra_mount = [
        "-v", f"{REPO_ROOT}/datasets:/home/glue_user/datasets",
        "-e", "ODS_DATASETS_ROOT=/home/glue_user/datasets",
    ]
    cmd = [
        "docker", "run", "--rm", "--network", NETWORK,
        *_glue_env_args(),
        *extra_mount,
        "ods-glue:local", "spark-submit",
        "/home/glue_user/workspace/jobs/ods_canonicalize_file.py",
        "--run_id", run_id,
        "--domain", DOMAIN,
        "--dataset", dataset,
        "--s3_curated_path", s3_curated_path,
        "--business_date", business_date,
        "--upstream_run_id", upstream_run_id,
    ]
    return subprocess.run(cmd, capture_output=True, text=True, timeout=420)


def _run_postgres_write_from_canonical(*, run_id, file_id, canonical_path,
                                        upstream_run_id, dataset) -> subprocess.CompletedProcess:
    """Same image / args as the existing _run_postgres_write helper, but
    with the canonical S3 path as input and the canonicalize run as the
    declared upstream."""
    cmd = [
        "docker", "run", "--rm", "--network", NETWORK,
        *_glue_env_args(),
        "ods-glue:local", "spark-submit",
        "--py-files",
        "/home/glue_user/workspace/jobs/utils.py,"
        "/home/glue_user/workspace/jobs/utils_bootstrap.py,"
        "/home/glue_user/workspace/jobs/utils_data.py,"
        "/home/glue_user/workspace/jobs/utils_config.py,"
        "/home/glue_user/workspace/jobs/utils_state.py,"
        "/home/glue_user/workspace/jobs/utils_runs.py,"
        "/home/glue_user/workspace/jobs/utils_jobs.py,"
        "/home/glue_user/workspace/jobs/dq.py",
        "/home/glue_user/workspace/jobs/ods_postgres_write.py",
        "--run_id", run_id,
        "--domain", DOMAIN,
        "--dataset", dataset,
        "--s3_input_path", canonical_path,
        "--file_id", file_id,
        "--upstream_run_id", upstream_run_id,
    ]
    return subprocess.run(cmd, capture_output=True, text=True, timeout=600)


def _canonical_s3_path(dataset: str, iso: str) -> str:
    bd_compact = iso.replace("-", "")
    return f"s3://{CURATED_BUCKET}/canonical/{DOMAIN}/{dataset}/date={bd_compact}/"


def test_canonicalize_full_chain(
    s3_client, pg_conn, configs_synced, clean_state,
):
    business_date = "20260510"
    csv = (
        "country_code,country_name\n"
        "GB,United Kingdom\n"
        "FR,France\n"
        "DE,Germany\n"
    )
    raw, iso = _upload_csv(
        s3_client, dataset=UPSERT_DATASET,
        business_date_yyyymmdd=business_date,
        filename_prefix="country_codes", content=csv,
    )
    file_id = _register_file(
        pg_conn, dataset=UPSERT_DATASET, s3_raw_path=raw,
        iso=iso, content=csv,
    )
    parent = str(uuid.uuid4())
    ingest_run  = str(uuid.uuid4())
    canon_run   = str(uuid.uuid4())
    pg_run      = str(uuid.uuid4())

    # 1. ingest -> curated parquet
    r1 = _run_ingestion(
        run_id=ingest_run, file_id=file_id, s3_input_path=raw,
        upstream_run_id=parent, dataset=UPSERT_DATASET,
    )
    assert r1.returncode == 0, r1.stderr[-2000:]

    curated = _curated_path(UPSERT_DATASET, iso)

    # 2. canonicalize -> canonical parquet
    r2 = _run_canonicalize(
        run_id=canon_run,
        s3_curated_path=curated,
        business_date=iso,
        upstream_run_id=ingest_run,
        dataset=UPSERT_DATASET,
    )
    assert r2.returncode == 0, (
        f"canonicalize failed.\nSTDOUT:\n{r2.stdout[-4000:]}\n"
        f"STDERR:\n{r2.stderr[-2000:]}"
    )

    canonical = _canonical_s3_path(UPSERT_DATASET, iso)

    # 3. postgres write reads from canonical, upstream = canonicalize run.
    r3 = _run_postgres_write_from_canonical(
        run_id=pg_run, file_id=file_id,
        canonical_path=canonical,
        upstream_run_id=canon_run, dataset=UPSERT_DATASET,
    )
    assert r3.returncode == 0, r3.stderr[-2000:]

    pg_conn.rollback()

    # canonical S3 prefix exists with at least one parquet object.
    keys = s3_client.list_objects_v2(
        Bucket=CURATED_BUCKET,
        Prefix=f"canonical/{DOMAIN}/{UPSERT_DATASET}/date={iso.replace('-', '')}/",
    ).get("Contents", [])
    assert keys, "expected at least one parquet object under canonical/ prefix"

    with pg_conn.cursor() as cur:
        # canonicalize run recorded correctly.
        cur.execute(
            "SELECT pipeline_type, status, record_count_source, "
            "       record_count_target "
            "  FROM pipeline.run_log "
            " WHERE run_id = %s::uuid",
            (canon_run,),
        )
        row = cur.fetchone()
        assert row is not None, "canonicalize run_log row missing"
        assert row[0] == "canonicalize"
        assert row[1] == "succeeded"
        assert row[2] == row[3] == 3

        # lineage_link bundle written by canonicalize step.
        cur.execute(
            "SELECT lineage_link_id::text, edge_type, target_ref, record_count "
            "  FROM pipeline.lineage_link "
            " WHERE consumer_run_id = %s::uuid",
            (canon_run,),
        )
        link = cur.fetchone()
        assert link is not None, "canonicalize lineage_link missing"
        canon_link_id = link[0]
        assert link[1] == "curated_to_canonical"
        assert canonical.rstrip("/") in (link[2] or "").rstrip("/")
        assert link[3] == 3

        # lineage_edge: 1 contribution, slot_name='canonical', upstream =
        # ingestion run, source_ref = curated path.
        cur.execute(
            "SELECT upstream_run_id::text, source_file_id::text, source_ref, "
            "       slot_name, edge_type, record_count "
            "  FROM pipeline.lineage_edge "
            " WHERE lineage_link_id = %s::uuid",
            (canon_link_id,),
        )
        edges = cur.fetchall()
        assert len(edges) == 1, f"expected 1 edge under canon link, got {len(edges)}"
        e = edges[0]
        assert e[0] == ingest_run
        assert e[1] == file_id
        assert curated.rstrip("/") in (e[2] or "").rstrip("/")
        assert e[3] == "canonical"
        assert e[4] == "curated_to_canonical"
        assert e[5] == 3

        # Target rows tagged via _ods_lineage_link_id by the postgres write.
        # The bundle there points to the canonicalize run as upstream.
        cur.execute(
            f"SELECT DISTINCT _ods_lineage_link_id FROM {UPSERT_TABLE} "
            f"WHERE country_code IN ('GB','FR','DE')"
        )
        link_ids = [r[0] for r in cur.fetchall()]
        assert len(link_ids) == 1, (
            f"expected one lineage_link covering this load, got {link_ids}"
        )
        pg_link_id = link_ids[0]
        cur.execute(
            "SELECT upstream_run_id::text FROM pipeline.lineage_edge "
            " WHERE lineage_link_id = %s::uuid",
            (pg_link_id,),
        )
        pg_upstreams = {r[0] for r in cur.fetchall()}
        assert canon_run in pg_upstreams, (
            f"postgres lineage_edge upstreams={pg_upstreams}, "
            f"expected canonicalize run {canon_run}"
        )
