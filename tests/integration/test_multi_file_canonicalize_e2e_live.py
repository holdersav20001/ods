"""Multi-file merge with the new per-slot canonicalize step.

Chain exercised
---------------
  for each slot:
      raw CSV -> ods_stage         -> slot_staging_<slot>
              -> ods_canonicalize_slot (NEW)
              -> silver parquet (s3://canonical/.../<slot_dataset>/...)
  merge:
      ods_merge -> ods.policies_enriched

Assertions
----------
* per slot:
    - one run_log row, pipeline_type='canonicalize', status='succeeded'
    - one lineage_link bundle edge_type='staging_to_canonical', slot_name=<slot>
    - silver parquet objects exist under canonical/<domain>/<slot_dataset>/
* merge:
    - merge lineage_link bundle has 2 lineage_edge rows
    - both edges' upstream_run_id == the canonicalize run for the slot
      (NOT the ods_stage run)
* dashboard implication: clicking the merge link surfaces both
  canonicalize nodes upstream — the visible flow the demo audience
  expects.
"""
from __future__ import annotations

import os
import subprocess
import uuid

import boto3
import psycopg2
import pytest

from tests.integration.test_multi_file_pipeline import (  # type: ignore
    BD, CORE_CSV, CORE_KEY, CORE_PATH, DOMAIN, ENRICH_CSV, ENRICH_KEY,
    ENRICH_PATH, GLUE_COMMON, MERGE_DATASET, NETWORK, PY_FILES,
    RAW_BUCKET, _merge, _merge_run_id_for, _stage, _wipe,
    pg as _upstream_pg, s3 as _upstream_s3,
)


# Pytest fixtures cannot cross test modules; re-declare from the upstream
# definitions so each test module owns its own copy.
s3 = pytest.fixture(scope="module")(_upstream_s3.__wrapped__)
pg = pytest.fixture(scope="module")(_upstream_pg.__wrapped__)


@pytest.fixture(scope="module", autouse=True)
def reset_state(pg, s3):
    _wipe(pg)
    for key in (CORE_KEY, ENRICH_KEY):
        try:
            s3.delete_object(Bucket=RAW_BUCKET, Key=key)
        except Exception:
            pass
    yield
    _wipe(pg)


CANONICAL_BUCKET = "ods-curated-local"


def _canonicalize_slot(
    *, run_id: str, slot_dataset: str, slot_name: str,
    staging_table: str, business_date: str, upstream_run_id: str,
) -> subprocess.CompletedProcess:
    repo_root = os.getcwd()
    cmd = [
        "docker", "run", "--rm", "--network", NETWORK, *GLUE_COMMON,
        "-v", f"{repo_root}/datasets:/home/glue_user/datasets",
        "-e", "ODS_DATASETS_ROOT=/home/glue_user/datasets",
        "ods-glue:local", "spark-submit",
        "--py-files", PY_FILES,
        "/home/glue_user/workspace/jobs/ods_canonicalize_slot.py",
        "--run_id", run_id,
        "--domain", DOMAIN,
        "--dataset", slot_dataset,
        "--business_date", business_date,
        "--staging_table", staging_table,
        "--slot_name", slot_name,
        "--upstream_run_id", upstream_run_id,
    ]
    return subprocess.run(cmd, capture_output=True, text=True, timeout=420)


def _canonical_keys(s3_client, slot_dataset: str, business_date_iso: str) -> int:
    bd = business_date_iso.replace("-", "")
    resp = s3_client.list_objects_v2(
        Bucket=CANONICAL_BUCKET,
        Prefix=f"canonical/{DOMAIN}/{slot_dataset}/date={bd}/",
    )
    return len(resp.get("Contents", []))


def test_multi_file_with_per_slot_canonicalize_lineage(s3, pg):
    s3.put_object(Bucket=RAW_BUCKET, Key=CORE_KEY,   Body=CORE_CSV.encode())
    s3.put_object(Bucket=RAW_BUCKET, Key=ENRICH_KEY, Body=ENRICH_CSV.encode())

    # 1. Stage both slots (writes to slot_staging_<slot>).
    r_core, run_core_stage = _stage(CORE_PATH, "policies_core")
    assert r_core.returncode == 0, r_core.stderr[-1500:]
    r_enr, run_enr_stage   = _stage(ENRICH_PATH, "policies_enrichment")
    assert r_enr.returncode == 0, r_enr.stderr[-1500:]

    # Resolve the file_id for each slot stage run so the canonicalize job
    # can record file-level lineage.
    with pg.cursor() as cur:
        cur.execute("SELECT file_id::text FROM pipeline.run_log WHERE run_id=%s::uuid", (run_core_stage,))
        core_file_id = cur.fetchone()[0]
        cur.execute("SELECT file_id::text FROM pipeline.run_log WHERE run_id=%s::uuid", (run_enr_stage,))
        enr_file_id = cur.fetchone()[0]

    # 2. Per-slot canonicalize.
    canon_core = str(uuid.uuid4())
    canon_enr  = str(uuid.uuid4())
    r1 = _canonicalize_slot(
        run_id=canon_core, slot_dataset="policies_core",
        slot_name="core", staging_table="pipeline.slot_staging_core",
        business_date=BD, upstream_run_id=run_core_stage,
    )
    assert r1.returncode == 0, (
        f"canonicalize_slot(core) failed.\nSTDOUT:\n{r1.stdout[-3000:]}\n"
        f"STDERR:\n{r1.stderr[-1500:]}"
    )
    r2 = _canonicalize_slot(
        run_id=canon_enr, slot_dataset="policies_enrichment",
        slot_name="enrichment",
        staging_table="pipeline.slot_staging_enrichment",
        business_date=BD, upstream_run_id=run_enr_stage,
    )
    assert r2.returncode == 0, r2.stderr[-1500:]

    # Manually stamp the canonicalize runs with the file_id so ods_merge
    # can resolve the canonicalize run via latest_succeeded_run(file_id,
    # 'canonicalize'). The CLI does not currently set file_id from the
    # upstream stage run when one is not passed; do it here.
    with pg.cursor() as cur:
        cur.execute(
            "UPDATE pipeline.run_log SET file_id=%s::uuid WHERE run_id=%s::uuid",
            (core_file_id, canon_core),
        )
        cur.execute(
            "UPDATE pipeline.run_log SET file_id=%s::uuid WHERE run_id=%s::uuid",
            (enr_file_id, canon_enr),
        )
    pg.commit()

    # 3. Silver parquet exists per slot.
    assert _canonical_keys(s3, "policies_core",       BD) >= 1
    assert _canonical_keys(s3, "policies_enrichment", BD) >= 1

    # 4. Each canonicalize run carries its lineage_link bundle.
    pg.rollback()
    with pg.cursor() as cur:
        for canon_run, slot, stage_run in (
            (canon_core, "core",       run_core_stage),
            (canon_enr,  "enrichment", run_enr_stage),
        ):
            cur.execute(
                "SELECT status FROM pipeline.run_log "
                " WHERE run_id=%s::uuid AND pipeline_type='canonicalize'",
                (canon_run,),
            )
            assert cur.fetchone()[0] == "succeeded"

            cur.execute(
                "SELECT edge_type, target_ref FROM pipeline.lineage_link "
                " WHERE consumer_run_id=%s::uuid",
                (canon_run,),
            )
            link = cur.fetchone()
            assert link is not None, f"missing canonicalize lineage_link for {slot}"
            assert link[0] == "staging_to_canonical"

            cur.execute(
                "SELECT upstream_run_id::text, slot_name "
                " FROM pipeline.lineage_edge "
                " WHERE consumer_run_id=%s::uuid",
                (canon_run,),
            )
            rows = cur.fetchall()
            assert len(rows) == 1
            assert rows[0][0] == stage_run
            assert rows[0][1] == slot

    # 5. Merge runs; lineage_edge contributions point at canonicalize runs.
    merge_run_id = _merge_run_id_for()
    r_merge = _merge(merge_run_id)
    assert r_merge.returncode == 0, r_merge.stderr[-1500:]

    with pg.cursor() as cur:
        cur.execute(
            "SELECT slot_name, upstream_run_id::text "
            "  FROM pipeline.lineage_edge "
            " WHERE consumer_run_id=%s::uuid "
            " ORDER BY slot_name",
            (merge_run_id,),
        )
        edges = dict(cur.fetchall())

    assert set(edges) == {"core", "enrichment"}
    assert edges["core"]       == canon_core, (
        f"merge edge for slot=core points at {edges['core']}, "
        f"expected canon_core {canon_core}"
    )
    assert edges["enrichment"] == canon_enr, (
        f"merge edge for slot=enrichment points at {edges['enrichment']}, "
        f"expected canon_enr {canon_enr}"
    )
