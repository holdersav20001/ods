"""Live E2E for the file → direct-Postgres pattern.

Drives the full flow via real components:
  CSV upload → ods_ingestion (Glue) → curated Parquet → ods_postgres_write
  (Glue, NEW) → Postgres rows.

Two datasets, two write modes, both YAML-driven:

  * file_direct_pg_upsert_demo  — write_mode=upsert, PK on country_code.
    Second run with overlapping keys updates rows in place.
  * file_direct_pg_append_demo  — write_mode=append, no PK. Second run
    accumulates rows.

Skipped automatically when docker / required containers / the Glue
image are not available.
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time
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
UPSERT_DATASET = "file_direct_pg_upsert_demo"
APPEND_DATASET = "file_direct_pg_append_demo"
UPSERT_TABLE = "ods.insurance_file_direct_pg_upsert_demo"
APPEND_TABLE = "ods.insurance_file_direct_pg_append_demo"


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
                     "file_direct_pg_upsert_demo.yaml"),
        pg_conn,
    )
    yaml_loader.sync_to_db(
        os.path.join(REPO_ROOT, "patterns", "insurance",
                     "file_direct_pg_append_demo.yaml"),
        pg_conn,
    )
    yield
    pg_conn.rollback()
    with pg_conn.cursor() as cur:
        cur.execute(
            "DELETE FROM pipeline.dataset_config WHERE domain=%s AND dataset IN (%s, %s)",
            (DOMAIN, UPSERT_DATASET, APPEND_DATASET),
        )
    pg_conn.commit()


@pytest.fixture
def clean_state(pg_conn):
    def _wipe():
        pg_conn.rollback()
        with pg_conn.cursor() as cur:
            cur.execute(f"DELETE FROM {UPSERT_TABLE}")
            cur.execute(f"DELETE FROM {APPEND_TABLE}")
            cur.execute(
                "DELETE FROM pipeline.run_stage_log "
                "WHERE run_id IN (SELECT run_id FROM pipeline.run_log "
                "                  WHERE domain=%s AND dataset IN (%s, %s))",
                (DOMAIN, UPSERT_DATASET, APPEND_DATASET),
            )
            cur.execute(
                "DELETE FROM pipeline.lineage_edge "
                "WHERE consumer_run_id IN (SELECT run_id FROM pipeline.run_log "
                "                        WHERE domain=%s AND dataset IN (%s, %s))",
                (DOMAIN, UPSERT_DATASET, APPEND_DATASET),
            )
            cur.execute(
                "DELETE FROM pipeline.reconciliation_log "
                "WHERE domain=%s AND dataset IN (%s, %s)",
                (DOMAIN, UPSERT_DATASET, APPEND_DATASET),
            )
            cur.execute(
                "DELETE FROM pipeline.run_log WHERE domain=%s AND dataset IN (%s, %s)",
                (DOMAIN, UPSERT_DATASET, APPEND_DATASET),
            )
            cur.execute(
                "DELETE FROM pipeline.file_catalogue "
                "WHERE domain=%s AND dataset IN (%s, %s)",
                (DOMAIN, UPSERT_DATASET, APPEND_DATASET),
            )
            cur.execute(
                "DELETE FROM pipeline.file_processing_attempt WHERE s3_path LIKE %s OR s3_path LIKE %s",
                (
                    f"s3://{RAW_BUCKET}/{DOMAIN}/{UPSERT_DATASET}/%",
                    f"s3://{RAW_BUCKET}/{DOMAIN}/{APPEND_DATASET}/%",
                ),
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
    ]


def _run_ingestion(*, run_id, file_id, s3_input_path, upstream_run_id,
                   dataset) -> subprocess.CompletedProcess:
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
        "--dataset", dataset,
        "--s3_input_path", s3_input_path,
        "--file_id", file_id,
        "--upstream_run_id", upstream_run_id,
    ]
    return subprocess.run(cmd, capture_output=True, text=True, timeout=420)


def _run_postgres_write(*, run_id, file_id, curated_path, upstream_run_id,
                        dataset) -> subprocess.CompletedProcess:
    cmd = [
        "docker", "run", "--rm", "--network", NETWORK,
        *_glue_env_args(),
        "ods-glue:local", "spark-submit",
        # Postgres JDBC driver is baked into ods-glue:local under
        # $SPARK_HOME/jars (see glue/Dockerfile) — no --packages needed.
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
        "--s3_input_path", curated_path,
        "--file_id", file_id,
        "--upstream_run_id", upstream_run_id,
    ]
    return subprocess.run(cmd, capture_output=True, text=True, timeout=600)


# ---------------------------------------------------------------------------
# CSV builders
# ---------------------------------------------------------------------------


def _upload_csv(s3_client, *, dataset: str, business_date_yyyymmdd: str,
                filename_prefix: str, content: str) -> tuple[str, str]:
    iso = (
        f"{business_date_yyyymmdd[:4]}-{business_date_yyyymmdd[4:6]}-"
        f"{business_date_yyyymmdd[6:8]}"
    )
    filename = f"{filename_prefix}_{business_date_yyyymmdd}.csv"
    key = f"{DOMAIN}/{dataset}/{iso}/{filename}"
    s3_path = f"s3://{RAW_BUCKET}/{key}"
    s3_client.put_object(Bucket=RAW_BUCKET, Key=key, Body=content.encode())
    return s3_path, iso


def _register_file(pg_conn, *, dataset: str, s3_raw_path: str, iso: str,
                   content: str) -> str:
    import hashlib
    md5 = hashlib.md5(content.encode()).hexdigest()
    file_id = str(uuid.uuid4())
    with pg_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO pipeline.file_catalogue "
            "(file_id, domain, dataset, business_date, s3_raw_path, "
            " file_size_bytes, file_md5, state) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,'received')",
            (file_id, DOMAIN, dataset, iso, s3_raw_path,
             len(content.encode()), md5),
        )
    pg_conn.commit()
    return file_id


def _curated_path(dataset: str, iso: str) -> str:
    return f"s3://{CURATED_BUCKET}/{DOMAIN}/{dataset}/date={iso}/"


# ---------------------------------------------------------------------------
# UPSERT test
# ---------------------------------------------------------------------------


def test_upsert_first_run_inserts_then_second_run_updates_in_place(
    s3_client, pg_conn, configs_synced, clean_state,
):
    business_date_a = "20260505"
    csv_a = (
        "country_code,country_name\n"
        "GB,United Kingdom\n"
        "FR,France\n"
        "DE,Germany\n"
    )
    raw_a, iso_a = _upload_csv(
        s3_client, dataset=UPSERT_DATASET,
        business_date_yyyymmdd=business_date_a,
        filename_prefix="country_codes", content=csv_a,
    )
    file_id_a = _register_file(
        pg_conn, dataset=UPSERT_DATASET, s3_raw_path=raw_a,
        iso=iso_a, content=csv_a,
    )
    parent_a = str(uuid.uuid4())
    ingest_run_a = str(uuid.uuid4())
    pg_run_a = str(uuid.uuid4())

    r1 = _run_ingestion(
        run_id=ingest_run_a, file_id=file_id_a, s3_input_path=raw_a,
        upstream_run_id=parent_a, dataset=UPSERT_DATASET,
    )
    assert r1.returncode == 0, r1.stderr[-2000:]

    r2 = _run_postgres_write(
        run_id=pg_run_a, file_id=file_id_a,
        curated_path=_curated_path(UPSERT_DATASET, iso_a),
        upstream_run_id=parent_a, dataset=UPSERT_DATASET,
    )
    assert r2.returncode == 0, (
        f"postgres_write failed.\nSTDOUT:\n{r2.stdout[-4000:]}\n"
        f"STDERR:\n{r2.stderr[-1500:]}"
    )

    pg_conn.rollback()
    with pg_conn.cursor() as cur:
        cur.execute(
            f"SELECT t.country_code, t.country_name, ll.consumer_run_id::text "
            f"FROM {UPSERT_TABLE} t "
            f"JOIN pipeline.lineage_link ll "
            f"  ON ll.lineage_link_id = t._ods_lineage_link_id "
            f"ORDER BY t.country_code"
        )
        rows = cur.fetchall()
    assert len(rows) == 3
    assert {r[0] for r in rows} == {"DE", "FR", "GB"}
    assert {r[1] for r in rows} == {"Germany", "France", "United Kingdom"}
    assert {r[2] for r in rows} == {pg_run_a}

    # ----- Second run: same key GB updated, new key US inserted, FR removed
    # is NOT expected (upsert is row-level, no delete).
    business_date_b = "20260506"
    csv_b = (
        "country_code,country_name\n"
        "GB,United Kingdom of Great Britain\n"   # updated value
        "US,United States\n"                      # new key
    )
    raw_b, iso_b = _upload_csv(
        s3_client, dataset=UPSERT_DATASET,
        business_date_yyyymmdd=business_date_b,
        filename_prefix="country_codes", content=csv_b,
    )
    file_id_b = _register_file(
        pg_conn, dataset=UPSERT_DATASET, s3_raw_path=raw_b,
        iso=iso_b, content=csv_b,
    )
    parent_b = str(uuid.uuid4())
    ingest_run_b = str(uuid.uuid4())
    pg_run_b = str(uuid.uuid4())

    r3 = _run_ingestion(
        run_id=ingest_run_b, file_id=file_id_b, s3_input_path=raw_b,
        upstream_run_id=parent_b, dataset=UPSERT_DATASET,
    )
    assert r3.returncode == 0, r3.stderr[-2000:]

    r4 = _run_postgres_write(
        run_id=pg_run_b, file_id=file_id_b,
        curated_path=_curated_path(UPSERT_DATASET, iso_b),
        upstream_run_id=parent_b, dataset=UPSERT_DATASET,
    )
    assert r4.returncode == 0, r4.stderr[-2000:]

    pg_conn.rollback()
    with pg_conn.cursor() as cur:
        cur.execute(
            f"SELECT t.country_code, t.country_name, ll.consumer_run_id::text "
            f"FROM {UPSERT_TABLE} t "
            f"JOIN pipeline.lineage_link ll "
            f"  ON ll.lineage_link_id = t._ods_lineage_link_id "
            f"ORDER BY t.country_code"
        )
        rows2 = cur.fetchall()
    by_code = {r[0]: r for r in rows2}
    # GB row updated in place (value changed, run_id moved to second run).
    assert by_code["GB"][1] == "United Kingdom of Great Britain"
    assert by_code["GB"][2] == pg_run_b
    # FR + DE untouched by second run.
    assert by_code["FR"][2] == pg_run_a
    assert by_code["DE"][2] == pg_run_a
    # US inserted by second run.
    assert by_code["US"][1] == "United States"
    assert by_code["US"][2] == pg_run_b
    assert len(rows2) == 4

    # Reconciliation: each run wrote a direct_postgres_count row.
    pg_conn.rollback()
    with pg_conn.cursor() as cur:
        cur.execute(
            "SELECT run_id::text, status, source_count, postgres_count "
            "FROM pipeline.reconciliation_log "
            "WHERE domain=%s AND dataset=%s "
            "  AND check_type='direct_postgres_count' "
            "ORDER BY created_at",
            (DOMAIN, UPSERT_DATASET),
        )
        recons = cur.fetchall()
    assert any(r[0] == pg_run_a and r[1] == "ok" for r in recons)
    assert any(r[0] == pg_run_b and r[1] == "ok" for r in recons)


# ---------------------------------------------------------------------------
# APPEND test
# ---------------------------------------------------------------------------


def test_append_first_and_second_runs_accumulate(
    s3_client, pg_conn, configs_synced, clean_state,
):
    business_date_a = "20260505"
    csv_a = (
        "event_id,payload\n"
        "evt-001,a\n"
        "evt-002,b\n"
    )
    raw_a, iso_a = _upload_csv(
        s3_client, dataset=APPEND_DATASET,
        business_date_yyyymmdd=business_date_a,
        filename_prefix="events", content=csv_a,
    )
    file_id_a = _register_file(
        pg_conn, dataset=APPEND_DATASET, s3_raw_path=raw_a,
        iso=iso_a, content=csv_a,
    )
    parent_a = str(uuid.uuid4())
    ingest_run_a = str(uuid.uuid4())
    pg_run_a = str(uuid.uuid4())

    r1 = _run_ingestion(
        run_id=ingest_run_a, file_id=file_id_a, s3_input_path=raw_a,
        upstream_run_id=parent_a, dataset=APPEND_DATASET,
    )
    assert r1.returncode == 0, r1.stderr[-2000:]
    r2 = _run_postgres_write(
        run_id=pg_run_a, file_id=file_id_a,
        curated_path=_curated_path(APPEND_DATASET, iso_a),
        upstream_run_id=parent_a, dataset=APPEND_DATASET,
    )
    assert r2.returncode == 0, (
        f"postgres_write failed.\nSTDOUT:\n{r2.stdout[-4000:]}\n"
        f"STDERR:\n{r2.stderr[-1500:]}"
    )

    business_date_b = "20260506"
    csv_b = (
        "event_id,payload\n"
        "evt-003,c\n"
        "evt-004,d\n"
        "evt-005,e\n"
    )
    raw_b, iso_b = _upload_csv(
        s3_client, dataset=APPEND_DATASET,
        business_date_yyyymmdd=business_date_b,
        filename_prefix="events", content=csv_b,
    )
    file_id_b = _register_file(
        pg_conn, dataset=APPEND_DATASET, s3_raw_path=raw_b,
        iso=iso_b, content=csv_b,
    )
    parent_b = str(uuid.uuid4())
    ingest_run_b = str(uuid.uuid4())
    pg_run_b = str(uuid.uuid4())

    r3 = _run_ingestion(
        run_id=ingest_run_b, file_id=file_id_b, s3_input_path=raw_b,
        upstream_run_id=parent_b, dataset=APPEND_DATASET,
    )
    assert r3.returncode == 0, r3.stderr[-2000:]
    r4 = _run_postgres_write(
        run_id=pg_run_b, file_id=file_id_b,
        curated_path=_curated_path(APPEND_DATASET, iso_b),
        upstream_run_id=parent_b, dataset=APPEND_DATASET,
    )
    assert r4.returncode == 0, r4.stderr[-2000:]

    pg_conn.rollback()
    with pg_conn.cursor() as cur:
        cur.execute(
            f"SELECT t.event_id, ll.consumer_run_id::text "
            f"FROM {APPEND_TABLE} t "
            f"JOIN pipeline.lineage_link ll "
            f"  ON ll.lineage_link_id = t._ods_lineage_link_id "
            f"ORDER BY t.event_id"
        )
        rows = cur.fetchall()
    assert {r[0] for r in rows} == {f"evt-00{i}" for i in range(1, 6)}
    # Each run tagged its own subset.
    by_run = {}
    for ev, rid in rows:
        by_run.setdefault(rid, set()).add(ev)
    assert by_run[pg_run_a] == {"evt-001", "evt-002"}
    assert by_run[pg_run_b] == {"evt-003", "evt-004", "evt-005"}
    assert len(rows) == 5

    pg_conn.rollback()
    with pg_conn.cursor() as cur:
        cur.execute(
            "SELECT run_id::text, source_count, postgres_count, status "
            "FROM pipeline.reconciliation_log "
            "WHERE domain=%s AND dataset=%s "
            "  AND check_type='direct_postgres_count' "
            "ORDER BY created_at",
            (DOMAIN, APPEND_DATASET),
        )
        recons = cur.fetchall()
    by_run_recon = {r[0]: r for r in recons}
    assert by_run_recon[pg_run_a][1] == 2 and by_run_recon[pg_run_a][3] == "ok"
    assert by_run_recon[pg_run_b][1] == 3 and by_run_recon[pg_run_b][3] == "ok"
