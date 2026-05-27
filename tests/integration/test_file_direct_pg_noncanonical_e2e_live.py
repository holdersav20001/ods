"""Live E2E for the file -> direct-Postgres NON-CANONICAL pattern.

Exercises the inline canonicalize step inside ods_postgres_write.py:

  CSV (source-shape: RskID, PolNo, ExposureAmt, AsOfDt)
    -> ods_ingestion (Glue)
    -> curated Parquet (source-shape cols + ODS metadata)
    -> ods_postgres_write (Glue) -- runs YAML transform inline,
                                    re-attaches ODS metadata in the
                                    SAME selectExpr projection (no
                                    row-aligned join)
    -> Postgres rows (canonical-shape: risk_id, policy_id, ...).

The critical assertion is row-alignment: the row whose source ``RskID``
was ``"R-001"`` MUST land in Postgres with ``risk_id="R-001"`` AND the
matching ``_ods_business_date``. If the inline canonicalize step
mis-pairs rows (the failure mode of a row-number window join), this
assertion fails.

Skipped automatically when docker / required containers / the Glue
image are not available.
"""
from __future__ import annotations

import hashlib
import os
import shutil
import subprocess
import sys
import uuid

import boto3
import pytest

# Allow yaml_loader import without Airflow.
sys.path.insert(0, os.path.abspath(os.path.join(
    os.path.dirname(__file__), "..", "..", "airflow", "dags",
)))
from common import yaml_loader  # type: ignore  # noqa: E402


REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
LOCALSTACK_ENDPOINT = os.environ.get("LOCALSTACK_ENDPOINT", "http://localhost:4566")
RAW_BUCKET = "ods-raw-local"
CURATED_BUCKET = "ods-curated-local"
NETWORK = "ods-network"

DOMAIN = "insurance"
DATASET = "file_direct_pg_risk_demo"
TABLE = "ods.insurance_file_direct_pg_risk_demo"


# ---------------------------------------------------------------------------
# Skip guards
# ---------------------------------------------------------------------------


def _docker_available() -> bool:
    return shutil.which("docker") is not None


def _glue_image_present() -> bool:
    try:
        out = subprocess.run(
            ["docker", "image", "inspect", "ods-glue:local"],
            capture_output=True, text=True, timeout=10, check=False,
        )
        return out.returncode == 0
    except Exception:
        return False


def _container_running(name: str) -> bool:
    try:
        out = subprocess.run(
            ["docker", "ps", "--filter", f"name={name}",
             "--format", "{{.Names}}"],
            capture_output=True, text=True, timeout=10, check=False,
        )
        return name in out.stdout.splitlines()
    except Exception:
        return False


@pytest.fixture(scope="module", autouse=True)
def _ensure_environment():
    if not _docker_available():
        pytest.skip("docker not available")
    if not _glue_image_present():
        pytest.skip("ods-glue:local image not built")
    for c in ("avivaods-postgres-1", "avivaods-localstack-1"):
        if not _container_running(c):
            pytest.skip(f"required container {c} not running")


# ---------------------------------------------------------------------------
# Postgres / S3 fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=LOCALSTACK_ENDPOINT,
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
        region_name=os.environ.get("AWS_DEFAULT_REGION", "eu-west-1"),
    )


@pytest.fixture(scope="module", autouse=True)
def _ensure_buckets(s3_client):
    for bucket in (RAW_BUCKET, CURATED_BUCKET):
        try:
            s3_client.head_bucket(Bucket=bucket)
        except Exception:
            s3_client.create_bucket(Bucket=bucket)


@pytest.fixture(scope="module")
def configs_synced(pg_conn):
    yaml_loader.sync_to_db(
        os.path.join(REPO_ROOT, "patterns", "insurance",
                     "file_direct_pg_risk_demo.yaml"),
        pg_conn,
    )
    yield
    pg_conn.rollback()
    with pg_conn.cursor() as cur:
        cur.execute(
            "DELETE FROM pipeline.dataset_config WHERE domain=%s AND dataset=%s",
            (DOMAIN, DATASET),
        )
    pg_conn.commit()


@pytest.fixture
def clean_state(pg_conn):
    def _wipe():
        pg_conn.rollback()
        with pg_conn.cursor() as cur:
            cur.execute(f"DELETE FROM {TABLE}")
            cur.execute(
                "DELETE FROM pipeline.run_stage_log "
                "WHERE run_id IN (SELECT run_id FROM pipeline.run_log "
                "                  WHERE domain=%s AND dataset=%s)",
                (DOMAIN, DATASET),
            )
            cur.execute(
                "DELETE FROM pipeline.lineage_edge "
                "WHERE child_run_id IN (SELECT run_id FROM pipeline.run_log "
                "                        WHERE domain=%s AND dataset=%s)",
                (DOMAIN, DATASET),
            )
            cur.execute(
                "DELETE FROM pipeline.reconciliation_log "
                "WHERE domain=%s AND dataset=%s",
                (DOMAIN, DATASET),
            )
            cur.execute(
                "DELETE FROM pipeline.run_log WHERE domain=%s AND dataset=%s",
                (DOMAIN, DATASET),
            )
            cur.execute(
                "DELETE FROM pipeline.file_catalogue "
                "WHERE domain=%s AND dataset=%s",
                (DOMAIN, DATASET),
            )
            cur.execute(
                "DELETE FROM pipeline.file_state WHERE s3_path LIKE %s",
                (f"s3://{RAW_BUCKET}/{DOMAIN}/{DATASET}/%",),
            )
        pg_conn.commit()
    _wipe()
    yield
    _wipe()


# ---------------------------------------------------------------------------
# Glue subprocess helpers
# ---------------------------------------------------------------------------


def _glue_env_args() -> list[str]:
    return [
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
        "-e", "ODS_SOURCE_APPLICATION=sftp",
        "-v", f"{REPO_ROOT}/glue/jobs:/home/glue_user/workspace/jobs",
        "-v", f"{REPO_ROOT}/ods_pipeline:/home/glue_user/ods_pipeline",
        "-v", f"{REPO_ROOT}/ods_ingestion_control:/home/glue_user/ods_ingestion_control",
        "-v", f"{REPO_ROOT}/patterns:/home/glue_user/workspace/jobs/patterns",
    ]


def _run_ingestion(*, run_id, file_id, s3_input_path, parent_run_id,
                   ) -> subprocess.CompletedProcess:
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
        "/home/glue_user/workspace/jobs/ods_ingestion.py",
        "--run_id", run_id,
        "--domain", DOMAIN,
        "--dataset", DATASET,
        "--s3_input_path", s3_input_path,
        "--file_id", file_id,
        "--parent_run_id", parent_run_id,
    ]
    return subprocess.run(cmd, capture_output=True, text=True, timeout=420)


def _run_postgres_write(*, run_id, file_id, curated_path, parent_run_id,
                        ) -> subprocess.CompletedProcess:
    cmd = [
        "docker", "run", "--rm", "--network", NETWORK,
        *_glue_env_args(),
        "ods-glue:local", "spark-submit",
        "--packages", "org.postgresql:postgresql:42.7.4",
        "--py-files",
        "/home/glue_user/workspace/jobs/utils.py,"
        "/home/glue_user/workspace/jobs/utils_bootstrap.py,"
        "/home/glue_user/workspace/jobs/utils_data.py,"
        "/home/glue_user/workspace/jobs/utils_config.py,"
        "/home/glue_user/workspace/jobs/utils_state.py,"
        "/home/glue_user/workspace/jobs/utils_runs.py,"
        "/home/glue_user/workspace/jobs/utils_jobs.py,"
        "/home/glue_user/workspace/jobs/dq.py,"
        "/home/glue_user/workspace/jobs/canonicalize.py",
        "/home/glue_user/workspace/jobs/ods_postgres_write.py",
        "--run_id", run_id,
        "--domain", DOMAIN,
        "--dataset", DATASET,
        "--s3_input_path", curated_path,
        "--file_id", file_id,
        "--parent_run_id", parent_run_id,
    ]
    return subprocess.run(cmd, capture_output=True, text=True, timeout=600)


# ---------------------------------------------------------------------------
# Fixture builders
# ---------------------------------------------------------------------------


def _upload_csv(s3_client, *, business_date_yyyymmdd: str, content: str):
    iso = (
        f"{business_date_yyyymmdd[:4]}-{business_date_yyyymmdd[4:6]}-"
        f"{business_date_yyyymmdd[6:8]}"
    )
    filename = f"risk_directpg_{business_date_yyyymmdd}.csv"
    key = f"{DOMAIN}/{DATASET}/{iso}/{filename}"
    s3_path = f"s3://{RAW_BUCKET}/{key}"
    s3_client.put_object(Bucket=RAW_BUCKET, Key=key, Body=content.encode())
    return s3_path, iso


def _register_file(pg_conn, *, s3_raw_path: str, iso: str, content: str) -> str:
    md5 = hashlib.md5(content.encode()).hexdigest()
    file_id = str(uuid.uuid4())
    with pg_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO pipeline.file_catalogue "
            "(file_id, domain, dataset, business_date, s3_raw_path, "
            " file_size_bytes, file_md5, state) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,'received')",
            (file_id, DOMAIN, DATASET, iso, s3_raw_path,
             len(content.encode()), md5),
        )
    pg_conn.commit()
    return file_id


def _curated_path(iso: str) -> str:
    return f"s3://{CURATED_BUCKET}/{DOMAIN}/{DATASET}/date={iso}/"


# ---------------------------------------------------------------------------
# The test
# ---------------------------------------------------------------------------


def test_noncanonical_inline_canonicalize_preserves_row_alignment(
    s3_client, pg_conn, configs_synced, clean_state,
):
    """5 source rows -> 5 target rows, each with the correct mapping.

    The CSV is intentionally NOT sorted by RskID so that any sort-based
    row-mis-alignment in ods_postgres_write would land the wrong values
    in the wrong row. We assert each canonical column's value matches
    the source row's value 1:1.
    """
    business_date = "20260507"
    # ods_ingestion stores _ods_business_date in ISO form (2026-05-07).
    business_date_iso = "2026-05-07"
    # Five rows in deliberately scrambled RskID order. Each policy_id
    # and exposure_amount is paired uniquely with its risk_id so any
    # cross-row mis-pairing would be detected.
    csv = (
        "RskID,PolNo,ExposureAmt,AsOfDt\n"
        "R-003,P-300,3000.50,20260507\n"
        "R-001,P-100,1000.10,20260507\n"
        "R-005,P-500,5000.55,20260507\n"
        "R-002,P-200,2000.20,20260507\n"
        "R-004,P-400,4000.40,20260507\n"
    )
    expected = {
        "R-001": ("P-100", 1000.10),
        "R-002": ("P-200", 2000.20),
        "R-003": ("P-300", 3000.50),
        "R-004": ("P-400", 4000.40),
        "R-005": ("P-500", 5000.55),
    }

    raw, iso = _upload_csv(
        s3_client,
        business_date_yyyymmdd=business_date,
        content=csv,
    )
    file_id = _register_file(
        pg_conn, s3_raw_path=raw, iso=iso, content=csv,
    )
    parent = str(uuid.uuid4())
    ingest_run = str(uuid.uuid4())
    pg_run = str(uuid.uuid4())

    r1 = _run_ingestion(
        run_id=ingest_run, file_id=file_id, s3_input_path=raw,
        parent_run_id=parent,
    )
    assert r1.returncode == 0, (
        f"ingestion failed.\nSTDOUT:\n{r1.stdout[-2500:]}\n"
        f"STDERR:\n{r1.stderr[-1500:]}"
    )

    r2 = _run_postgres_write(
        run_id=pg_run, file_id=file_id,
        curated_path=_curated_path(iso),
        parent_run_id=parent,
    )
    assert r2.returncode == 0, (
        f"postgres_write failed.\nSTDOUT:\n{r2.stdout[-4000:]}\n"
        f"STDERR:\n{r2.stderr[-1500:]}"
    )

    # ----- Row alignment assertions: per-row source -> canonical mapping
    pg_conn.rollback()
    with pg_conn.cursor() as cur:
        cur.execute(
            f"SELECT risk_id, policy_id, exposure_amount, as_of_date, "
            f"       _ods_run_id, _ods_business_date, _ods_file_id, "
            f"       _ods_domain, _ods_dataset "
            f"FROM {TABLE} ORDER BY risk_id"
        )
        rows = cur.fetchall()

    assert len(rows) == 5, f"expected 5 rows, got {len(rows)}"

    for risk_id, policy_id, exposure_amount, as_of_date, \
            ods_run, ods_bd, ods_file_id, ods_domain, ods_dataset in rows:
        assert risk_id in expected, f"unexpected risk_id {risk_id!r}"
        exp_pol, exp_amt = expected[risk_id]
        # CRITICAL: each canonical row's policy_id and exposure_amount
        # must match the source row whose RskID == this row's risk_id.
        # If row-alignment is broken, these will be cross-paired.
        assert policy_id == exp_pol, (
            f"row-alignment FAILURE: risk_id={risk_id} got policy_id="
            f"{policy_id!r}, expected {exp_pol!r}"
        )
        assert abs(exposure_amount - exp_amt) < 1e-6, (
            f"row-alignment FAILURE: risk_id={risk_id} got "
            f"exposure_amount={exposure_amount!r}, expected {exp_amt!r}"
        )
        assert str(as_of_date) == "2026-05-07", (
            f"as_of_date for {risk_id}: got {as_of_date!r}"
        )

        # ODS metadata: _ods_run_id was rebranded to THIS write's run_id;
        # _ods_file_id is preserved from the curated parquet (= ingest);
        # _ods_business_date is preserved as the YYYYMMDD source value.
        assert ods_run == pg_run, (
            f"_ods_run_id for {risk_id}: got {ods_run!r}, expected {pg_run!r}"
        )
        assert ods_file_id == file_id, (
            f"_ods_file_id for {risk_id}: got {ods_file_id!r}, "
            f"expected {file_id!r}"
        )
        assert ods_bd == business_date_iso, (
            f"_ods_business_date for {risk_id}: got {ods_bd!r}, "
            f"expected {business_date_iso!r}"
        )
        assert ods_domain == DOMAIN
        assert ods_dataset == DATASET

    # Reconciliation row was emitted and is OK.
    pg_conn.rollback()
    with pg_conn.cursor() as cur:
        cur.execute(
            "SELECT status, source_count, postgres_count "
            "FROM pipeline.reconciliation_log "
            "WHERE run_id::text=%s AND check_type='direct_postgres_count'",
            (pg_run,),
        )
        recon = cur.fetchone()
    assert recon is not None, "no reconciliation row for postgres-write run"
    assert recon[0] == "ok"
    assert recon[1] == 5
    assert recon[2] == 5
