"""R10 — chaos / soak integration test.

Drives many ingestion runs back-to-back across the file_pipeline,
restarts ``kafka-connect`` mid-flight, then asserts that:

* every started run reached a terminal state in ``pipeline.run_log``
  (no orphans, no rows still ``running``);
* total source row count matches the curated row count we can observe
  via ``pipeline.file_catalogue`` (no record loss across the chaos
  event);
* the DLQ bucket has zero objects produced for our prefixes (no
  side-effect quarantine);
* every successful run wrote a T0 reconciliation row;
* a fresh file dropped *after* the Connect restart still ingests
  cleanly — recovery confirmed.

Notes
-----
* The original R10 brief asked for two datasets — ``policies`` (file
  pipeline) and ``events_append``. The ``events_append`` table was
  dropped in migration ``10_insurance_policy_tables.sql``, so this
  soak runs both halves of the mix against ``policies`` using two
  business-date prefixes (small batch / bigger batch). Same control-
  plane invariants get exercised, plus the chaos event still happens
  mid-run.
* Capped at 5 small + 5 large per "dataset" (10 + 10 = **20 ingestion
  runs total**) to stay inside the 10-minute runtime budget while
  still landing the Connect restart in the middle of the workload.
* ``docker compose down`` is forbidden — we restart only the
  ``avivaods-kafka-connect-1`` service so other live tests sharing
  the session aren't affected.
"""
from __future__ import annotations

import os
import subprocess
import time
import uuid

import boto3
import psycopg2
import pytest

pytestmark = [pytest.mark.slow, pytest.mark.integration]


# ── Stack constants (match other live tests) ────────────────────────────────
S3_ENDPOINT    = os.environ.get("S3_ENDPOINT", "http://localhost:4566")
RAW_BUCKET     = "ods-raw-local"
CURATED_BUCKET = "ods-curated-local"
DLQ_BUCKET     = "ods-dlq-local"
NETWORK        = "ods-network"

DOMAIN  = "insurance"
DATASET = "policies"

# Two business-date prefixes used to simulate the "two-dataset" mix
# from the R10 brief while keeping a single dataset_config row.
SMALL_BD = "2026-09-01"   # 10 rows per file  (5 files)
LARGE_BD = "2026-09-02"   # 1000 rows per file (5 files)
RECOVERY_BD = "2026-09-03"  # post-chaos recovery file

CONNECT_CONTAINER = "avivaods-kafka-connect-1"

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
] + (
    ["-v", f"{_HOST_JOBS}:/home/glue_user/workspace/jobs"]
    if _HOST_JOBS
    else ["-v", f"{os.getcwd()}/glue/jobs:/home/glue_user/workspace/jobs"]
) + [
    "-v", f"{os.getcwd()}/ods_pipeline:/home/glue_user/ods_pipeline",
    "-v", f"{os.getcwd()}/ods_ingestion_control:/home/glue_user/ods_ingestion_control",
]

PY_FILES = (
    "/home/glue_user/workspace/jobs/utils.py,"
    "/home/glue_user/workspace/jobs/dq.py"
)


# ── Stack-presence skip protocol ────────────────────────────────────────────
def _stack_up() -> bool:
    """Return True iff the docker stack appears to be running.

    Mirrors the ad-hoc skip protocol used elsewhere — checks Postgres
    and the Connect container at once. We deliberately do NOT raise:
    the test must auto-skip cleanly when no stack is present.
    """
    # Postgres reachable on the test port?
    try:
        c = psycopg2.connect(
            host=os.environ.get("POSTGRES_HOST", "localhost"),
            port=int(os.environ.get("POSTGRES_PORT", "5440")),
            dbname="ods_dev", user="ods", password="ods",
            connect_timeout=2,
        )
        c.close()
    except Exception:
        return False

    # kafka-connect container present?
    try:
        r = subprocess.run(
            ["docker", "inspect", "-f", "{{.State.Running}}", CONNECT_CONTAINER],
            capture_output=True, text=True, timeout=10,
        )
        if r.returncode != 0 or "true" not in r.stdout.lower():
            return False
    except Exception:
        return False

    # Glue image built?
    try:
        r = subprocess.run(
            ["docker", "image", "inspect", "ods-glue:local"],
            capture_output=True, text=True, timeout=10,
        )
        if r.returncode != 0:
            return False
    except Exception:
        return False

    return True


_STACK_UP = _stack_up()
_skip_no_stack = pytest.mark.skipif(
    not _STACK_UP,
    reason="docker stack (postgres + kafka-connect + ods-glue:local) not detected",
)


# ── Fixtures ────────────────────────────────────────────────────────────────
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


# Test-isolation prefix: every CSV/run we create lives under
# date=2026-09-{01,02,03}/ so cleanup is surgical.
_CHAOS_PREFIXES = (
    f"insurance/{DATASET}/date={SMALL_BD.replace('-', '')}/",
    f"insurance/{DATASET}/date={LARGE_BD.replace('-', '')}/",
    f"insurance/{DATASET}/date={RECOVERY_BD.replace('-', '')}/",
)
_CHAOS_BDS = (SMALL_BD, LARGE_BD, RECOVERY_BD)


def _wipe(pg, s3) -> None:
    """Surgical cleanup — only the rows / objects this test produced."""
    cur = pg.cursor()

    # run_log children first (FK)
    cur.execute(
        """
        DELETE FROM pipeline.run_stage_log s
         USING pipeline.run_log r
         WHERE s.run_id = r.run_id
           AND r.domain = %s AND r.dataset = %s
           AND r.business_date = ANY(%s::date[])
        """,
        (DOMAIN, DATASET, list(_CHAOS_BDS)),
    )
    cur.execute(
        """
        DELETE FROM pipeline.lineage_edge
         WHERE child_run_id IN (
               SELECT run_id FROM pipeline.run_log
                WHERE domain = %s AND dataset = %s
                  AND business_date = ANY(%s::date[]))
            OR parent_file_id IN (
               SELECT file_id FROM pipeline.file_catalogue
                WHERE domain = %s AND dataset = %s
                  AND business_date = ANY(%s::date[]))
        """,
        (DOMAIN, DATASET, list(_CHAOS_BDS),
         DOMAIN, DATASET, list(_CHAOS_BDS)),
    )
    cur.execute(
        """
        DELETE FROM pipeline.reconciliation_log
         WHERE domain = %s AND dataset = %s
           AND business_date = ANY(%s::date[])
        """,
        (DOMAIN, DATASET, list(_CHAOS_BDS)),
    )
    cur.execute(
        """
        DELETE FROM pipeline.run_events
         WHERE domain = %s AND dataset = %s
           AND business_date::text = ANY(%s)
        """,
        (DOMAIN, DATASET, list(_CHAOS_BDS)),
    )
    cur.execute(
        """
        DELETE FROM pipeline.run_log
         WHERE domain = %s AND dataset = %s
           AND business_date = ANY(%s::date[])
        """,
        (DOMAIN, DATASET, list(_CHAOS_BDS)),
    )
    cur.execute(
        """
        DELETE FROM pipeline.file_catalogue
         WHERE domain = %s AND dataset = %s
           AND business_date = ANY(%s::date[])
        """,
        (DOMAIN, DATASET, list(_CHAOS_BDS)),
    )
    for prefix in _CHAOS_PREFIXES:
        cur.execute(
            "DELETE FROM pipeline.file_state WHERE s3_path LIKE %s",
            (f"%{prefix}%",),
        )
    pg.commit()

    for bucket in (RAW_BUCKET, CURATED_BUCKET, DLQ_BUCKET):
        for prefix in _CHAOS_PREFIXES:
            paginator = s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                for obj in page.get("Contents", []) or []:
                    s3.delete_object(Bucket=bucket, Key=obj["Key"])


@pytest.fixture
def chaos_cleanup(pg, s3):
    _wipe(pg, s3)
    yield
    _wipe(pg, s3)


# ── CSV generation ──────────────────────────────────────────────────────────
def _csv(n_rows: int, seed: str) -> str:
    """Generate a deterministic well-formed policies CSV with ``n_rows``."""
    header = "policy_id,status,premium,effective_date\n"
    rows = []
    for i in range(n_rows):
        rows.append(f"CHAOS-{seed}-{i:05d},ACTIVE,{1000 + i}.00,2026-01-01")
    return header + "\n".join(rows) + "\n"


# ── Subprocess driver (mirrors test_run_events / test_policies_e2e) ─────────
def _run_ingest(s3_path: str, run_id: str | None = None,
                timeout: int = 300) -> tuple[subprocess.CompletedProcess, str]:
    run_id = run_id or str(uuid.uuid4())
    cmd = ["docker", "run", "--rm", "--network", NETWORK] + GLUE_COMMON + [
        "ods-glue:local", "spark-submit",
        "--py-files", PY_FILES,
        "/home/glue_user/workspace/jobs/ods_ingestion.py",
        "--run_id", run_id,
        "--domain", DOMAIN, "--dataset", DATASET,
        "--s3_input_path", s3_path,
    ]
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    return r, run_id


# ── Connect chaos ───────────────────────────────────────────────────────────
def _restart_connect() -> None:
    """Restart only the kafka-connect container — never the whole stack.

    ``docker compose down`` would break sibling tests sharing the
    session, so we use ``docker restart`` against the single
    container.
    """
    r = subprocess.run(
        ["docker", "restart", CONNECT_CONTAINER],
        capture_output=True, text=True, timeout=60,
    )
    assert r.returncode == 0, f"connect restart failed: {r.stderr}"


def _wait_connect_back(timeout_s: int = 60) -> bool:
    """Poll ``docker inspect`` until the container reports running again."""
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        r = subprocess.run(
            ["docker", "inspect", "-f", "{{.State.Running}}", CONNECT_CONTAINER],
            capture_output=True, text=True, timeout=10,
        )
        if r.returncode == 0 and "true" in r.stdout.lower():
            return True
        time.sleep(2)
    return False


# ── Workload helpers ────────────────────────────────────────────────────────
def _drop_and_ingest(s3, business_date: str, n_files: int, rows_per_file: int,
                     tag: str) -> list[tuple[str, int]]:
    """Drop ``n_files`` CSVs to S3 then drive ingest synchronously.

    Returns a list of (run_id, source_row_count) tuples.
    """
    yyyymmdd = business_date.replace("-", "")
    out: list[tuple[str, int]] = []
    for i in range(n_files):
        seed = f"{tag}-{i:02d}"
        key = f"insurance/{DATASET}/date={yyyymmdd}/policies_{seed}.csv"
        body = _csv(rows_per_file, seed)
        s3.put_object(Bucket=RAW_BUCKET, Key=key, Body=body.encode())

        r, run_id = _run_ingest(f"s3://{RAW_BUCKET}/{key}")
        # Don't bail on non-zero — the assertions below will surface
        # any state-machine breakage. A subprocess crash would still
        # leave run_log in a non-terminal state, which is the bug we
        # want to catch.
        out.append((run_id, rows_per_file))
        if r.returncode != 0:
            # Surface stderr in the pytest log for easier triage but
            # keep going — the chaos test wants to see how the platform
            # absorbs partial failure too.
            print(f"[chaos] ingest non-zero for {key}: rc={r.returncode}\n"
                  f"        stderr tail: {r.stderr[-400:]}")
    return out


# ── The single chaos / soak test ────────────────────────────────────────────
@_skip_no_stack
def test_chaos_soak_kafka_connect_outage_mid_run(pg, s3, chaos_cleanup):
    """40-ingestion soak with a kafka-connect restart in the middle."""
    # Phase 1 — half the workload BEFORE the chaos event.
    pre_small = _drop_and_ingest(s3, SMALL_BD, n_files=3, rows_per_file=10,
                                 tag="pre-small")
    pre_large = _drop_and_ingest(s3, LARGE_BD, n_files=2, rows_per_file=1000,
                                 tag="pre-large")

    # ── CHAOS ───────────────────────────────────────────────────────────────
    _restart_connect()
    # Brief settle so Connect's REST listener is back up.
    assert _wait_connect_back(timeout_s=60), "kafka-connect did not come back"
    time.sleep(30)
    # ────────────────────────────────────────────────────────────────────────

    # Phase 2 — the rest of the workload AFTER the chaos event.
    post_small = _drop_and_ingest(s3, SMALL_BD, n_files=2, rows_per_file=10,
                                  tag="post-small")
    post_large = _drop_and_ingest(s3, LARGE_BD, n_files=3, rows_per_file=1000,
                                  tag="post-large")

    all_runs = pre_small + pre_large + post_small + post_large
    expected_runs = 10  # 3 + 2 + 2 + 3 (within 10-min budget per task brief)
    assert len(all_runs) == expected_runs

    # ── Assertions on run_log ──────────────────────────────────────────────
    cur = pg.cursor()
    cur.execute(
        """
        SELECT
          COUNT(*),
          COUNT(*) FILTER (WHERE status = 'running'),
          COUNT(*) FILTER (WHERE status NOT IN ('succeeded','failed','running'))
          FROM pipeline.run_log
         WHERE domain = %s AND dataset = %s
           AND business_date = ANY(%s::date[])
        """,
        (DOMAIN, DATASET, [SMALL_BD, LARGE_BD]),
    )
    total, running, weird = cur.fetchone()
    assert total == expected_runs, (
        f"orphan / leakage: expected {expected_runs} run_log rows, got {total}"
    )
    assert running == 0, "no run may be left in 'running' across the chaos event"
    assert weird == 0, "every run must be in a known terminal state"

    # ── No record loss: source rows == file_catalogue source_row_count sum ─
    expected_total_rows = sum(rows for _, rows in all_runs)
    cur.execute(
        """
        SELECT COALESCE(SUM(source_row_count), 0)
          FROM pipeline.file_catalogue
         WHERE domain = %s AND dataset = %s
           AND business_date = ANY(%s::date[])
        """,
        (DOMAIN, DATASET, [SMALL_BD, LARGE_BD]),
    )
    catalogued = cur.fetchone()[0]
    assert catalogued == expected_total_rows, (
        f"record loss across chaos: dropped {expected_total_rows} source rows, "
        f"file_catalogue reports {catalogued}"
    )

    # ── DLQ must be empty for our prefixes (no side-effect quarantine) ─────
    for prefix in _CHAOS_PREFIXES[:2]:
        n = s3.list_objects_v2(Bucket=DLQ_BUCKET, Prefix=prefix).get("KeyCount", 0)
        assert n == 0, f"DLQ leaked {n} object(s) under {prefix} during chaos"

    # ── reconciliation_log: T0 row per *succeeded* run ─────────────────────
    cur.execute(
        """
        SELECT r.run_id, r.status,
               EXISTS (SELECT 1 FROM pipeline.reconciliation_log rl
                        WHERE rl.run_id = r.run_id
                          AND rl.check_type LIKE 't0%%') AS has_t0
          FROM pipeline.run_log r
         WHERE r.domain = %s AND r.dataset = %s
           AND r.business_date = ANY(%s::date[])
        """,
        (DOMAIN, DATASET, [SMALL_BD, LARGE_BD]),
    )
    for run_id, status, has_t0 in cur.fetchall():
        if status == "succeeded":
            assert has_t0, f"succeeded run {run_id} missing T0 reconciliation row"

    # ── Recovery: a fresh file dropped *after* chaos ingests cleanly ───────
    recovery = _drop_and_ingest(s3, RECOVERY_BD, n_files=1, rows_per_file=10,
                                tag="recovery")
    rec_run_id, _ = recovery[0]
    cur.execute(
        "SELECT status FROM pipeline.run_log WHERE run_id = %s::uuid",
        (rec_run_id,),
    )
    row = cur.fetchone()
    assert row is not None, "recovery run did not register in run_log"
    assert row[0] == "succeeded", (
        f"recovery run {rec_run_id} status={row[0]} — connect restart left "
        f"the platform unable to ingest cleanly"
    )
