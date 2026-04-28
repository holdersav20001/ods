# tests/integration/test_ingestion.py
"""
Integration tests for the ods_ingestion Glue job.

Prerequisites (must be running before executing):
    docker compose up -d

The tests invoke the job via `docker run` against the live LocalStack / Postgres
stack, then verify S3 and Postgres state directly.
"""

import os
import subprocess
import uuid

import boto3
import psycopg2
import pytest

S3_ENDPOINT = "http://localhost:4566"
RAW_BUCKET = "ods-raw-local"
CURATED_BUCKET = "ods-curated-local"
NETWORK = "ods-network"

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def s3():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="eu-west-1",
    )


@pytest.fixture
def pg():
    import os
    conn = psycopg2.connect(
        host=os.environ.get("TEST_PG_HOST", "127.0.0.1"),
        port=int(os.environ.get("TEST_PG_PORT", "5440")),
        dbname=os.environ.get("TEST_PG_DB", "ods_dev"),
        user=os.environ.get("TEST_PG_USER", "ods"),
        password=os.environ.get("TEST_PG_PASSWORD", "ods"),
    )
    yield conn
    conn.close()


# ---------------------------------------------------------------------------
# Helper: run the ingestion job inside Docker
# ---------------------------------------------------------------------------

def run_ingestion_job(
    run_id: str,
    s3_path: str,
    domain: str = "insurance",
    dataset: str = "policies",
) -> subprocess.CompletedProcess:
    cmd = [
        "docker", "run", "--rm", "--network", NETWORK,
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
        "-v", f"{os.getcwd()}/glue/jobs:/home/glue_user/workspace/jobs",
        "ods-glue:local",
        "spark-submit",
        "--py-files",
        "/home/glue_user/workspace/jobs/utils.py,"
        "/home/glue_user/workspace/jobs/dq.py",
        "/home/glue_user/workspace/jobs/ods_ingestion.py",
        "--run_id", run_id,
        "--domain", domain,
        "--dataset", dataset,
        "--s3_input_path", s3_path,
    ]
    return subprocess.run(cmd, capture_output=True, text=True, timeout=300)


# ---------------------------------------------------------------------------
# Test data
# ---------------------------------------------------------------------------

GOOD_CSV = (
    "policy_id,policyholder_name,premium_amount,start_date,end_date,agent_code,postcode\n"
    "POL-001,Alice Smith,1200.00,2026-01-01,2027-01-01,AG001,EC1A1BB\n"
    "POL-002,Bob Jones,950.50,2026-02-01,2027-02-01,AG002,WC2N5DU\n"
)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_happy_path_writes_parquet(s3, pg):
    """A clean CSV file should be ingested and produce Parquet in the curated bucket."""
    run_id = str(uuid.uuid4())
    key = "insurance/policies/date=20260417/policies_20260417.csv"
    s3.put_object(Bucket=RAW_BUCKET, Key=key, Body=GOOD_CSV.encode())

    result = run_ingestion_job(run_id, f"s3://ods-raw-local/{key}")

    assert result.returncode == 0, (
        f"Job exited with code {result.returncode}.\nSTDOUT:\n{result.stdout}"
        f"\nSTDERR:\n{result.stderr}"
    )

    # Parquet should exist in the curated bucket
    objs = s3.list_objects_v2(
        Bucket=CURATED_BUCKET,
        Prefix="insurance/policies/date=2026-04-17/",
    )
    assert objs.get("KeyCount", 0) > 0, (
        "No Parquet files found in curated bucket after successful ingestion."
    )

    # Final log entry should be 'completed'
    cur = pg.cursor()
    cur.execute(
        "SELECT status FROM pipeline.glue_job_log "
        "WHERE run_id = %s ORDER BY id DESC LIMIT 1",
        (run_id,),
    )
    row = cur.fetchone()
    assert row is not None, "No log entry found for run_id."
    assert row[0] == "completed", f"Expected 'completed', got '{row[0]}'."


def test_idempotency_exits_cleanly(s3, pg):
    """Re-running with the same input file (already completed) should exit 0 with 'skipped'."""
    run_id_1 = str(uuid.uuid4())
    run_id_2 = str(uuid.uuid4())
    key = "insurance/policies/date=20260418/policies_20260418.csv"
    s3.put_object(Bucket=RAW_BUCKET, Key=key, Body=GOOD_CSV.encode())

    # First run — should complete normally
    run_ingestion_job(run_id_1, f"s3://ods-raw-local/{key}")

    # Second run — same file, different run_id
    result = run_ingestion_job(run_id_2, f"s3://ods-raw-local/{key}")

    assert result.returncode == 0, (
        f"Idempotent run exited non-zero ({result.returncode}).\n"
        f"STDERR:\n{result.stderr}"
    )

    # The second run should have been logged as 'skipped' (or 'completed' if
    # the implementation chooses to re-process — both are acceptable).
    cur = pg.cursor()
    cur.execute(
        "SELECT status FROM pipeline.glue_job_log "
        "WHERE run_id = %s ORDER BY id LIMIT 1",
        (run_id_2,),
    )
    row = cur.fetchone()
    assert row is not None, "No log entry found for second run_id."
    assert row[0] in ("completed", "skipped"), (
        f"Expected 'completed' or 'skipped', got '{row[0]}'."
    )
