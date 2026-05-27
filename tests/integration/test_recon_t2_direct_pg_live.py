"""Live T2 reconciliation coverage for the direct_postgres pattern.

Verifies that ``dag_recon_t2.reconcile()`` (whose dispatcher was
extended in 176f673 to recognise ``pipeline_type='direct_postgres'``
runs as authoritative T2 sources) actually emits T2 reconciliation
rows when invoked against direct_postgres run history.

Two cases:

* APPEND target (``file_direct_pg_append_demo``)
  - target carries ``_ods_run_id`` / ``_ods_file_id`` metadata cols.
  - T2 must write a ``t2_append_file_count`` row with
    ``status='passed'`` and ``source_count == postgres_count``.

* UPSERT target without ``_history`` partner
  (``file_direct_pg_upsert_demo``)
  - the current-state target alone is not a valid per-run count
    target (later runs legitimately overwrite earlier rows).
  - T2 must record this explicitly via ``t2_current_history_missing``
    (status='skipped') so we can prove the dispatcher is reaching
    direct_postgres runs.

Skipped automatically when docker / required containers / the
ods-glue:local image are not available — same guard pattern as
``test_file_direct_pg_e2e_live.py``.
"""
from __future__ import annotations

import importlib.util
import os
import shutil
import subprocess
import sys
import uuid

import boto3
import pytest

# yaml_loader lives under airflow/dags. Allow direct import.
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
APPEND_DATASET = "file_direct_pg_append_demo"
UPSERT_DATASET = "file_direct_pg_upsert_demo"
APPEND_TABLE = "ods.insurance_file_direct_pg_append_demo"
UPSERT_TABLE = "ods.insurance_file_direct_pg_upsert_demo"


# ---------------------------------------------------------------------------
# Skip guards (mirror test_file_direct_pg_e2e_live.py)
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
# dag_recon_t2 import — direct file load (avoids Airflow scheduler).
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def reconcile_fn():
    """Load airflow/dags/dag_recon_t2.py and return its reconcile callable.

    The module's ``reconcile`` is wrapped by Airflow's @task decorator
    when Airflow is importable. The except ImportError fallback at the
    top of the module installs a shim ``task`` that exposes ``.func``
    and otherwise leaves the function callable directly. We accept
    either shape so this test runs whether or not Airflow is in the
    venv.

    PIPELINE_PG_DSN is forced to the integration test Postgres before
    the module is imported (the DSN is captured at import time).
    """
    os.environ["PIPELINE_PG_DSN"] = (
        f"host={os.environ.get('TEST_PG_HOST', '127.0.0.1')} "
        f"port={os.environ.get('TEST_PG_PORT', '5440')} "
        f"dbname={os.environ.get('TEST_PG_DB', 'ods_dev')} "
        f"user={os.environ.get('TEST_PG_USER', 'ods')} "
        f"password={os.environ.get('TEST_PG_PASSWORD', 'ods')}"
    )
    path = os.path.join(REPO_ROOT, "airflow", "dags", "dag_recon_t2.py")
    spec = importlib.util.spec_from_file_location("dag_recon_t2_under_test", path)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)

    fn = mod.reconcile
    # When Airflow is present, @task decorates with a TaskDecorator
    # whose underlying python function is exposed via .function.
    # When Airflow is absent, the module's local shim sets fn.func.
    if hasattr(fn, "function"):
        return fn.function
    if hasattr(fn, "func"):
        return fn.func
    return fn


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
                     f"{APPEND_DATASET}.yaml"),
        pg_conn,
    )
    yaml_loader.sync_to_db(
        os.path.join(REPO_ROOT, "patterns", "insurance",
                     f"{UPSERT_DATASET}.yaml"),
        pg_conn,
    )
    yield
    pg_conn.rollback()
    with pg_conn.cursor() as cur:
        cur.execute(
            "DELETE FROM pipeline.dataset_config "
            "WHERE domain=%s AND dataset IN (%s, %s)",
            (DOMAIN, APPEND_DATASET, UPSERT_DATASET),
        )
    pg_conn.commit()


@pytest.fixture
def clean_state(pg_conn):
    """Wipe every persistent artefact for the two demo datasets so the
    T2 LOOKBACK_HOURS=24 sweep sees only this test's runs."""
    def _wipe():
        pg_conn.rollback()
        with pg_conn.cursor() as cur:
            cur.execute(f"DELETE FROM {APPEND_TABLE}")
            cur.execute(f"DELETE FROM {UPSERT_TABLE}")
            cur.execute(
                "DELETE FROM pipeline.run_stage_log "
                "WHERE run_id IN (SELECT run_id FROM pipeline.run_log "
                "                  WHERE domain=%s AND dataset IN (%s, %s))",
                (DOMAIN, APPEND_DATASET, UPSERT_DATASET),
            )
            cur.execute(
                "DELETE FROM pipeline.lineage_edge "
                "WHERE child_run_id IN (SELECT run_id FROM pipeline.run_log "
                "                        WHERE domain=%s AND dataset IN (%s, %s))",
                (DOMAIN, APPEND_DATASET, UPSERT_DATASET),
            )
            cur.execute(
                "DELETE FROM pipeline.reconciliation_log "
                "WHERE domain=%s AND dataset IN (%s, %s)",
                (DOMAIN, APPEND_DATASET, UPSERT_DATASET),
            )
            cur.execute(
                "DELETE FROM pipeline.run_log WHERE domain=%s AND dataset IN (%s, %s)",
                (DOMAIN, APPEND_DATASET, UPSERT_DATASET),
            )
            cur.execute(
                "DELETE FROM pipeline.file_catalogue "
                "WHERE domain=%s AND dataset IN (%s, %s)",
                (DOMAIN, APPEND_DATASET, UPSERT_DATASET),
            )
            cur.execute(
                "DELETE FROM pipeline.file_state "
                "WHERE s3_path LIKE %s OR s3_path LIKE %s",
                (
                    f"s3://{RAW_BUCKET}/{DOMAIN}/{APPEND_DATASET}/%",
                    f"s3://{RAW_BUCKET}/{DOMAIN}/{UPSERT_DATASET}/%",
                ),
            )
        pg_conn.commit()
    _wipe()
    yield
    _wipe()


# ---------------------------------------------------------------------------
# Glue subprocess helpers (copied from test_file_direct_pg_e2e_live.py).
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


def _run_ingestion(*, run_id, file_id, s3_input_path, parent_run_id, dataset):
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
        "--parent_run_id", parent_run_id,
    ]
    return subprocess.run(cmd, capture_output=True, text=True, timeout=420)


def _run_postgres_write(*, run_id, file_id, curated_path, parent_run_id, dataset):
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
        "/home/glue_user/workspace/jobs/dq.py",
        "/home/glue_user/workspace/jobs/ods_postgres_write.py",
        "--run_id", run_id,
        "--domain", DOMAIN,
        "--dataset", dataset,
        "--s3_input_path", curated_path,
        "--file_id", file_id,
        "--parent_run_id", parent_run_id,
    ]
    return subprocess.run(cmd, capture_output=True, text=True, timeout=600)


# ---------------------------------------------------------------------------
# CSV / catalogue helpers
# ---------------------------------------------------------------------------


def _upload_csv(s3_client, *, dataset, business_date_yyyymmdd,
                filename_prefix, content):
    iso = (
        f"{business_date_yyyymmdd[:4]}-{business_date_yyyymmdd[4:6]}-"
        f"{business_date_yyyymmdd[6:8]}"
    )
    filename = f"{filename_prefix}_{business_date_yyyymmdd}.csv"
    key = f"{DOMAIN}/{dataset}/{iso}/{filename}"
    s3_path = f"s3://{RAW_BUCKET}/{key}"
    s3_client.put_object(Bucket=RAW_BUCKET, Key=key, Body=content.encode())
    return s3_path, iso


def _register_file(pg_conn, *, dataset, s3_raw_path, iso, content):
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


def _curated_path(dataset, iso):
    return f"s3://{CURATED_BUCKET}/{DOMAIN}/{dataset}/date={iso}/"


def _drive_one_run(s3_client, pg_conn, *, dataset, csv, prefix, bd_yyyymmdd):
    """Run ingestion + postgres_write end-to-end. Returns the
    direct_postgres run_id (the run that is the T2 source)."""
    raw, iso = _upload_csv(
        s3_client, dataset=dataset,
        business_date_yyyymmdd=bd_yyyymmdd,
        filename_prefix=prefix, content=csv,
    )
    file_id = _register_file(pg_conn, dataset=dataset, s3_raw_path=raw,
                             iso=iso, content=csv)
    parent = str(uuid.uuid4())
    ingest_run = str(uuid.uuid4())
    pg_run = str(uuid.uuid4())

    r1 = _run_ingestion(
        run_id=ingest_run, file_id=file_id, s3_input_path=raw,
        parent_run_id=parent, dataset=dataset,
    )
    assert r1.returncode == 0, r1.stderr[-2000:]
    r2 = _run_postgres_write(
        run_id=pg_run, file_id=file_id,
        curated_path=_curated_path(dataset, iso),
        parent_run_id=parent, dataset=dataset,
    )
    assert r2.returncode == 0, (
        f"postgres_write failed.\nSTDOUT:\n{r2.stdout[-3000:]}\n"
        f"STDERR:\n{r2.stderr[-1500:]}"
    )
    return pg_run, file_id


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_t2_writes_append_file_count_for_direct_postgres_append(
    s3_client, pg_conn, configs_synced, clean_state, reconcile_fn,
):
    """T2 against a direct_postgres + write_mode=append run.

    Drives a single end-to-end direct_postgres run, then invokes
    ``reconcile()`` directly. T2 must dispatch through
    ``_reconcile_append_file_count`` and write a row whose source and
    landed counts both equal the curated row count, with status='passed'.
    """
    csv = (
        "event_id,payload\n"
        "evt-r4-001,a\n"
        "evt-r4-002,b\n"
        "evt-r4-003,c\n"
    )
    pg_run, _file_id = _drive_one_run(
        s3_client, pg_conn,
        dataset=APPEND_DATASET, csv=csv, prefix="events",
        bd_yyyymmdd="20260507",
    )

    # Sanity: target carries the run.
    pg_conn.rollback()
    with pg_conn.cursor() as cur:
        cur.execute(
            f"SELECT count(*) FROM {APPEND_TABLE} WHERE _ods_run_id::text = %s",
            (pg_run,),
        )
        landed = cur.fetchone()[0]
    assert landed == 3, f"expected 3 rows in target tagged {pg_run}, got {landed}"

    # Wipe ONLY direct_postgres_count rows for this dataset so the T2
    # rows are unambiguous. Leave the rest of reconciliation_log alone.
    with pg_conn.cursor() as cur:
        cur.execute(
            "DELETE FROM pipeline.reconciliation_log "
            "WHERE domain=%s AND dataset=%s "
            "  AND check_type='direct_postgres_count'",
            (DOMAIN, APPEND_DATASET),
        )
    pg_conn.commit()

    # Drive T2.
    reconcile_fn()

    pg_conn.rollback()
    with pg_conn.cursor() as cur:
        cur.execute(
            "SELECT check_type, status, source_count, postgres_count, "
            "       discrepancy_count "
            "  FROM pipeline.reconciliation_log "
            " WHERE domain=%s AND dataset=%s AND run_id::text = %s "
            " ORDER BY created_at",
            (DOMAIN, APPEND_DATASET, pg_run),
        )
        rows = cur.fetchall()

    by_check = {r[0]: r for r in rows}
    assert "t2_append_file_count" in by_check, (
        f"T2 did not write t2_append_file_count for direct_postgres "
        f"append run {pg_run}. Got rows: {rows}"
    )
    check_type, status, source_count, postgres_count, disc = by_check[
        "t2_append_file_count"
    ]
    assert status == "passed", f"expected status=passed, got {status} ({rows})"
    assert source_count == 3, f"expected source_count=3, got {source_count}"
    assert postgres_count == 3, f"expected postgres_count=3, got {postgres_count}"
    assert disc == 0, f"expected discrepancy=0, got {disc}"


def test_t2_records_history_missing_for_direct_postgres_upsert_without_history(
    s3_client, pg_conn, configs_synced, clean_state, reconcile_fn,
):
    """T2 against a direct_postgres + write_mode=upsert run when the
    target has no _history partner.

    Per dag_recon_t2._record_current_history_missing(), T2 must emit a
    ``t2_current_history_missing`` row (status='skipped') so we can
    prove the dispatcher reaches direct_postgres upsert runs even
    though it cannot do per-file count recon against current-state.
    """
    csv = (
        "country_code,country_name\n"
        "GB,United Kingdom\n"
        "FR,France\n"
    )
    pg_run, _file_id = _drive_one_run(
        s3_client, pg_conn,
        dataset=UPSERT_DATASET, csv=csv, prefix="country_codes",
        bd_yyyymmdd="20260507",
    )

    pg_conn.rollback()
    with pg_conn.cursor() as cur:
        cur.execute(
            "DELETE FROM pipeline.reconciliation_log "
            "WHERE domain=%s AND dataset=%s "
            "  AND check_type='direct_postgres_count'",
            (DOMAIN, UPSERT_DATASET),
        )
    pg_conn.commit()

    reconcile_fn()

    pg_conn.rollback()
    with pg_conn.cursor() as cur:
        cur.execute(
            "SELECT check_type, status, detail "
            "  FROM pipeline.reconciliation_log "
            " WHERE domain=%s AND dataset=%s AND run_id::text = %s "
            " ORDER BY created_at",
            (DOMAIN, UPSERT_DATASET, pg_run),
        )
        rows = cur.fetchall()

    by_check = {r[0]: r for r in rows}
    assert "t2_current_history_missing" in by_check, (
        f"T2 did not write t2_current_history_missing for direct_postgres "
        f"upsert run {pg_run}. Got rows: {rows}"
    )
    check_type, status, detail = by_check["t2_current_history_missing"]
    assert status == "skipped", f"expected status=skipped, got {status}"
    assert "history" in (detail or ""), (
        f"detail should explain the missing history table: {detail!r}"
    )
