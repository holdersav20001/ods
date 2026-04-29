"""
Integration tests — multi-file staging + merge pipeline.

Scenarios:
  1. Happy path              — both slots stage + merge → 4 wide rows, full lineage
  2. Partial arrival         — only core staged → no merge triggered
  3. Slot rerun              — re-stage core → merge re-runs → updated values
  4. Merge restart           — inject failed merge_run_log → rerun succeeds idempotently
  5. Lineage trace           — POL001 traceable per-column back to both S3 raw paths
  6. Merge idempotency       — second merge call exits 0, row count unchanged
  7. Stage idempotency       — re-stage same path → exits 0, already-staged note

Prerequisites: docker compose up -d (full stack), migrations 06+07 applied.
"""
from __future__ import annotations

import os
import subprocess
import uuid

import boto3
import psycopg2
import pytest

S3_ENDPOINT    = os.environ.get("S3_ENDPOINT", "http://localhost:4566")
RAW_BUCKET     = "ods-raw-local"
NETWORK        = "ods-network"
BD             = "2026-06-01"
DOMAIN         = "insurance"
MERGE_DATASET  = "policies_enriched"

_HOST_JOBS = os.environ.get("HOST_JOBS_PATH", "")
GLUE_COMMON = [
    "-e", "AWS_DEFAULT_REGION=eu-west-1",
    "-e", "AWS_ACCESS_KEY_ID=test",
    "-e", "AWS_SECRET_ACCESS_KEY=test",
    "-e", "LOCALSTACK_ENDPOINT=http://localstack:4566",
    "-e", "POSTGRES_HOST=postgres",
    "-e", "POSTGRES_DB=ods_dev",
    "-e", "POSTGRES_USER=ods",
    "-e", "POSTGRES_PASSWORD=ods",
    "-e", "SCHEMA_REGISTRY_URL=http://schema-registry:8081",
    "-e", "ENV=local",
] + (["-v", f"{_HOST_JOBS}:/home/glue_user/workspace/jobs"] if _HOST_JOBS else [])

PY_FILES = (
    "/home/glue_user/workspace/jobs/utils.py,"
    "/home/glue_user/workspace/jobs/dq.py"
)

CORE_KEY    = f"insurance/policies_core/date=20260601/policies_core_20260601.csv"
ENRICH_KEY  = f"insurance/policies_enrichment/date=20260601/policies_enrichment_20260601.csv"
CORE_PATH   = f"s3://ods-raw-local/{CORE_KEY}"
ENRICH_PATH = f"s3://ods-raw-local/{ENRICH_KEY}"

CORE_CSV = open(f"{os.getcwd()}/tests/fixtures/policies_core_20260601.csv").read()
ENRICH_CSV = open(f"{os.getcwd()}/tests/fixtures/policies_enrichment_20260601.csv").read()

# Modified core CSV — POL001 changed to CANCELLED for rerun test
CORE_CSV_V2 = (
    "policy_id,status,premium,effective_date\n"
    "POL001,CANCELLED,1200.00,2026-01-01\n"
    "POL002,LAPSED,850.50,2025-06-15\n"
    "POL003,ACTIVE,2000.00,2026-03-01\n"
)


@pytest.fixture(scope="module")
def s3():
    return boto3.client(
        "s3", endpoint_url=S3_ENDPOINT,
        aws_access_key_id="test", aws_secret_access_key="test",
        region_name="eu-west-1",
    )


@pytest.fixture(scope="module")
def pg():
    conn = psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", "5440")),
        dbname="ods_dev", user="ods", password="ods",
    )
    yield conn
    conn.close()


@pytest.fixture(scope="module", autouse=True)
def reset_state(pg, s3):
    _wipe(pg)
    for bucket in ("ods-raw-local",):
        for key in (CORE_KEY, ENRICH_KEY):
            try:
                s3.delete_object(Bucket=bucket, Key=key)
            except Exception:
                pass
    yield
    _wipe(pg)


def _wipe(pg):
    cur = pg.cursor()
    cur.execute(
        "DELETE FROM pipeline.merge_contribution_log mc "
        "USING pipeline.merge_run_log mr "
        "WHERE mc.merge_run_id=mr.merge_run_id "
        "AND mr.domain='insurance' AND mr.dataset='policies_enriched'"
    )
    cur.execute(
        "DELETE FROM pipeline.merge_run_log "
        "WHERE domain='insurance' AND dataset='policies_enriched'"
    )
    cur.execute(
        "DELETE FROM pipeline.run_stage_log s USING pipeline.run_log r "
        "WHERE s.run_id=r.run_id AND r.domain='insurance' "
        "AND r.dataset IN ('policies_core','policies_enrichment','policies_enriched')"
    )
    cur.execute(
        "DELETE FROM pipeline.run_log "
        "WHERE domain='insurance' "
        "AND dataset IN ('policies_core','policies_enrichment','policies_enriched')"
    )
    cur.execute(
        "DELETE FROM pipeline.file_state "
        "WHERE s3_path LIKE '%policies_core%' OR s3_path LIKE '%policies_enrichment%'"
    )
    cur.execute("DELETE FROM pipeline.slot_staging_core WHERE _ods_business_date='2026-06-01'")
    cur.execute("DELETE FROM pipeline.slot_staging_enrichment WHERE _ods_business_date='2026-06-01'")
    cur.execute("DELETE FROM ods.policies_enriched WHERE _ods_business_date='2026-06-01'")
    pg.commit()


def _stage(s3_path: str, dataset: str, run_id: str | None = None) -> tuple[subprocess.CompletedProcess, str]:
    run_id = run_id or str(uuid.uuid4())
    cmd = ["docker", "run", "--rm", "--network", NETWORK] + GLUE_COMMON + [
        "ods-glue:local", "spark-submit",
        "--py-files", PY_FILES,
        "/home/glue_user/workspace/jobs/ods_stage.py",
        "--run_id", run_id, "--domain", DOMAIN, "--dataset", dataset,
        "--s3_input_path", s3_path,
    ]
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
    return r, run_id


def _merge(merge_run_id: str, business_date: str = BD) -> subprocess.CompletedProcess:
    cmd = ["docker", "run", "--rm", "--network", NETWORK] + GLUE_COMMON + [
        "ods-glue:local", "spark-submit",
        "--py-files", PY_FILES,
        "/home/glue_user/workspace/jobs/ods_merge.py",
        "--merge_run_id", merge_run_id,
        "--domain", DOMAIN, "--dataset", MERGE_DATASET,
        "--business_date", business_date,
    ]
    return subprocess.run(cmd, capture_output=True, text=True, timeout=300)


def _merge_run_id_for(bd: str = BD) -> str:
    import uuid as _uuid
    return str(_uuid.uuid5(_uuid.NAMESPACE_OID, f"{DOMAIN}/{MERGE_DATASET}/{bd}"))


def _run_log_status(pg, run_id: str) -> str | None:
    cur = pg.cursor()
    cur.execute("SELECT status FROM pipeline.run_log WHERE run_id=%s", (run_id,))
    row = cur.fetchone()
    return row[0] if row else None


# ── Scenario 1 — Happy path ───────────────────────────────────────────────────

def test_1_happy_path(s3, pg):
    s3.put_object(Bucket=RAW_BUCKET, Key=CORE_KEY,   Body=CORE_CSV.encode())
    s3.put_object(Bucket=RAW_BUCKET, Key=ENRICH_KEY, Body=ENRICH_CSV.encode())

    r_core, run_core = _stage(CORE_PATH, "policies_core")
    assert r_core.returncode == 0, r_core.stderr

    r_enrich, run_enrich = _stage(ENRICH_PATH, "policies_enrichment")
    assert r_enrich.returncode == 0, r_enrich.stderr

    merge_run_id = _merge_run_id_for()
    r_merge = _merge(merge_run_id)
    assert r_merge.returncode == 0, r_merge.stderr

    cur = pg.cursor()

    # Wide table: 4 rows (POL001+POL002 have both; POL003 core-only; POL004 enrich-only)
    cur.execute("SELECT count(*) FROM ods.policies_enriched WHERE _ods_business_date='2026-06-01'")
    assert cur.fetchone()[0] == 4

    # POL001 has all columns
    cur.execute("SELECT status, premium, agent_code, postcode FROM ods.policies_enriched WHERE policy_id='POL001'")
    row = cur.fetchone()
    assert row[0] == "ACTIVE"
    assert row[2] == "AGT42"

    # POL003 core-only: enrichment columns NULL
    cur.execute("SELECT agent_code FROM ods.policies_enriched WHERE policy_id='POL003'")
    assert cur.fetchone()[0] is None

    # POL004 enrich-only: core columns NULL
    cur.execute("SELECT status FROM ods.policies_enriched WHERE policy_id='POL004'")
    assert cur.fetchone()[0] is None

    # merge_run_log succeeded
    cur.execute("SELECT status, record_count_out FROM pipeline.merge_run_log WHERE merge_run_id=%s",
                (merge_run_id,))
    mrow = cur.fetchone()
    assert mrow[0] == "succeeded"
    assert mrow[1] == 4

    # merge_contribution_log: 2 rows (one per slot)
    cur.execute("SELECT slot_name FROM pipeline.merge_contribution_log WHERE merge_run_id=%s ORDER BY slot_name",
                (merge_run_id,))
    slots = [r[0] for r in cur.fetchall()]
    assert slots == ["core", "enrichment"]


# ── Scenario 2 — Partial arrival: no merge ───────────────────────────────────

def test_2_partial_arrival_no_merge(s3, pg):
    _wipe(pg)
    s3.put_object(Bucket=RAW_BUCKET, Key=CORE_KEY, Body=CORE_CSV.encode())

    r, _ = _stage(CORE_PATH, "policies_core")
    assert r.returncode == 0, r.stderr

    cur = pg.cursor()
    cur.execute("SELECT count(*) FROM pipeline.slot_staging_core WHERE _ods_business_date='2026-06-01'")
    assert cur.fetchone()[0] == 3

    cur.execute("SELECT count(*) FROM pipeline.slot_staging_enrichment WHERE _ods_business_date='2026-06-01'")
    assert cur.fetchone()[0] == 0

    cur.execute("SELECT count(*) FROM ods.policies_enriched WHERE _ods_business_date='2026-06-01'")
    assert cur.fetchone()[0] == 0


# ── Scenario 3 — Slot rerun updates only that slot ───────────────────────────

def test_3_slot_rerun(s3, pg):
    _wipe(pg)
    s3.put_object(Bucket=RAW_BUCKET, Key=CORE_KEY,   Body=CORE_CSV.encode())
    s3.put_object(Bucket=RAW_BUCKET, Key=ENRICH_KEY, Body=ENRICH_CSV.encode())

    _, run_core_1 = _stage(CORE_PATH, "policies_core")
    _, run_enrich = _stage(ENRICH_PATH, "policies_enrichment")
    merge_run_id = _merge_run_id_for()
    _merge(merge_run_id)

    # Rerun core with modified CSV
    s3.put_object(Bucket=RAW_BUCKET, Key=CORE_KEY, Body=CORE_CSV_V2.encode())
    # Delete file_state so stage doesn't skip
    cur = pg.cursor()
    cur.execute("DELETE FROM pipeline.file_state WHERE s3_path=%s", (CORE_PATH,))
    pg.commit()

    r2, run_core_2 = _stage(CORE_PATH, "policies_core")
    assert r2.returncode == 0, r2.stderr

    # Core staging updated to CANCELLED
    cur.execute("SELECT status FROM pipeline.slot_staging_core "
                "WHERE policy_id='POL001' AND _ods_business_date='2026-06-01'")
    assert cur.fetchone()[0] == "CANCELLED"

    # Enrichment staging unchanged
    cur.execute("SELECT agent_code FROM pipeline.slot_staging_enrichment "
                "WHERE policy_id='POL001' AND _ods_business_date='2026-06-01'")
    assert cur.fetchone()[0] == "AGT42"

    # Re-merge: use new deterministic ID (same — ensures idempotency re-run)
    # But first reset the merge_run_log so it re-runs
    cur.execute("DELETE FROM pipeline.merge_contribution_log mc "
                "USING pipeline.merge_run_log mr WHERE mc.merge_run_id=mr.merge_run_id "
                "AND mr.merge_run_id=%s", (merge_run_id,))
    cur.execute("DELETE FROM pipeline.merge_run_log WHERE merge_run_id=%s", (merge_run_id,))
    pg.commit()

    r3 = _merge(merge_run_id)
    assert r3.returncode == 0, r3.stderr

    cur.execute("SELECT status, _ods_run_id_core FROM ods.policies_enriched WHERE policy_id='POL001'")
    row = cur.fetchone()
    assert row[0] == "CANCELLED"
    assert row[1] == run_core_2

    # Enrichment column still set
    cur.execute("SELECT agent_code FROM ods.policies_enriched WHERE policy_id='POL001'")
    assert cur.fetchone()[0] == "AGT42"


# ── Scenario 4 — Merge restart: inject failed row, rerun succeeds ─────────────

def test_4_merge_restart(s3, pg):
    _wipe(pg)
    s3.put_object(Bucket=RAW_BUCKET, Key=CORE_KEY,   Body=CORE_CSV.encode())
    s3.put_object(Bucket=RAW_BUCKET, Key=ENRICH_KEY, Body=ENRICH_CSV.encode())
    _stage(CORE_PATH, "policies_core")
    _stage(ENRICH_PATH, "policies_enrichment")

    merge_run_id = _merge_run_id_for()

    # Inject failed merge_run_log as if a prior run crashed
    cur = pg.cursor()
    cur.execute(
        "INSERT INTO pipeline.merge_run_log (merge_run_id, domain, dataset, business_date, status, error_summary) "
        "VALUES (%s, 'insurance', 'policies_enriched', '2026-06-01', 'failed', 'simulated crash')",
        (merge_run_id,),
    )
    pg.commit()

    r = _merge(merge_run_id)
    assert r.returncode == 0, r.stderr

    cur.execute("SELECT status FROM pipeline.merge_run_log WHERE merge_run_id=%s", (merge_run_id,))
    assert cur.fetchone()[0] == "succeeded"

    cur.execute("SELECT count(*) FROM ods.policies_enriched WHERE _ods_business_date='2026-06-01'")
    assert cur.fetchone()[0] == 4

    # No duplicate wide rows
    cur.execute("SELECT policy_id, count(*) FROM ods.policies_enriched "
                "WHERE _ods_business_date='2026-06-01' GROUP BY policy_id HAVING count(*) > 1")
    assert cur.fetchone() is None


# ── Scenario 5 — Lineage trace per-column back to S3 raw paths ───────────────

def test_5_lineage_trace(s3, pg):
    _wipe(pg)
    s3.put_object(Bucket=RAW_BUCKET, Key=CORE_KEY,   Body=CORE_CSV.encode())
    s3.put_object(Bucket=RAW_BUCKET, Key=ENRICH_KEY, Body=ENRICH_CSV.encode())
    _stage(CORE_PATH, "policies_core")
    _stage(ENRICH_PATH, "policies_enrichment")
    merge_run_id = _merge_run_id_for()
    _merge(merge_run_id)

    cur = pg.cursor()

    # Wide row carries merge_run_id + per-slot run_ids
    cur.execute(
        "SELECT _ods_merge_run_id, _ods_run_id_core, _ods_run_id_enrich "
        "FROM ods.policies_enriched WHERE policy_id='POL001'"
    )
    row = cur.fetchone()
    assert str(row[0]) == merge_run_id
    assert row[1] is not None
    assert row[2] is not None

    # merge_contribution_log: 2 rows with s3_raw_path and columns_written
    cur.execute(
        "SELECT slot_name, s3_raw_path, columns_written, record_count "
        "FROM pipeline.merge_contribution_log "
        "WHERE merge_run_id=%s ORDER BY slot_name",
        (merge_run_id,),
    )
    rows = cur.fetchall()
    assert len(rows) == 2

    core_row = next(r for r in rows if r[0] == "core")
    enrich_row = next(r for r in rows if r[0] == "enrichment")

    # Core columns
    assert "status" in core_row[2]
    assert "premium" in core_row[2]
    assert "effective_date" in core_row[2]
    assert core_row[3] == 3

    # Enrichment columns
    assert "agent_code" in enrich_row[2]
    assert "postcode" in enrich_row[2]
    assert enrich_row[3] == 3


# ── Scenario 6 — Merge idempotency ───────────────────────────────────────────

def test_6_merge_idempotency(s3, pg):
    _wipe(pg)
    s3.put_object(Bucket=RAW_BUCKET, Key=CORE_KEY,   Body=CORE_CSV.encode())
    s3.put_object(Bucket=RAW_BUCKET, Key=ENRICH_KEY, Body=ENRICH_CSV.encode())
    _stage(CORE_PATH, "policies_core")
    _stage(ENRICH_PATH, "policies_enrichment")

    merge_run_id = _merge_run_id_for()
    r1 = _merge(merge_run_id)
    assert r1.returncode == 0, r1.stderr

    cur = pg.cursor()
    cur.execute("SELECT count(*) FROM ods.policies_enriched WHERE _ods_business_date='2026-06-01'")
    count_before = cur.fetchone()[0]

    # Second call — same merge_run_id, should skip
    r2 = _merge(merge_run_id)
    assert r2.returncode == 0, r2.stderr

    cur.execute("SELECT count(*) FROM ods.policies_enriched WHERE _ods_business_date='2026-06-01'")
    assert cur.fetchone()[0] == count_before

    cur.execute("SELECT count(*) FROM pipeline.merge_run_log "
                "WHERE domain='insurance' AND dataset='policies_enriched' AND business_date='2026-06-01'")
    assert cur.fetchone()[0] == 1


# ── Scenario 7 — Stage idempotency ───────────────────────────────────────────

def test_7_stage_idempotency(s3, pg):
    _wipe(pg)
    s3.put_object(Bucket=RAW_BUCKET, Key=CORE_KEY, Body=CORE_CSV.encode())

    r1, run1 = _stage(CORE_PATH, "policies_core")
    assert r1.returncode == 0, r1.stderr

    # Second stage of same path
    r2, run2 = _stage(CORE_PATH, "policies_core")
    assert r2.returncode == 0, r2.stderr

    cur = pg.cursor()
    cur.execute("SELECT status, error_summary FROM pipeline.run_log WHERE run_id=%s", (run2,))
    row = cur.fetchone()
    assert row[0] == "succeeded"
    assert "already staged" in (row[1] or "")

    # No extra rows in staging
    cur.execute("SELECT count(*) FROM pipeline.slot_staging_core WHERE _ods_business_date='2026-06-01'")
    assert cur.fetchone()[0] == 3
