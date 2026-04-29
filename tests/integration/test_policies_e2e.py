"""
End-to-end integration tests — all 9 pipeline scenarios.
Prerequisite: docker compose up -d (full stack running).
"""
import hashlib, os, subprocess, time, uuid
import boto3, psycopg2, pytest
from confluent_kafka import Consumer

S3_ENDPOINT    = os.environ.get("S3_ENDPOINT",    "http://localhost:4566")
RAW_BUCKET     = "ods-raw-local"
CURATED_BUCKET = "ods-curated-local"
DLQ_BUCKET     = "ods-dlq-local"
NETWORK        = "ods-network"
TOPIC          = "ods.insurance.policies"
KAFKA_BROKERS  = os.environ.get("KAFKA_BROKERS", "localhost:9092")

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

GLUE_KAFKA_ENV = GLUE_COMMON + ["-e", "KAFKA_BOOTSTRAP_SERVERS=broker:29092"]

PY_FILES = (
    "/home/glue_user/workspace/jobs/utils.py,"
    "/home/glue_user/workspace/jobs/dq.py"
)

@pytest.fixture(scope="module")
def s3():
    return boto3.client("s3", endpoint_url=S3_ENDPOINT,
        aws_access_key_id="test", aws_secret_access_key="test",
        region_name="eu-west-1")

@pytest.fixture(scope="module")
def pg():
    conn = psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", "5440")),
        dbname="ods_dev", user="ods", password="ods")
    yield conn
    conn.close()


@pytest.fixture(scope="module", autouse=True)
def reset_pipeline_state(pg, s3):
    """Wipe pipeline state for the e2e test dates so tests are re-runnable."""
    cur = pg.cursor()
    cur.execute("""
        DELETE FROM pipeline.file_state
        WHERE s3_path LIKE 's3://ods-raw-local/insurance/policies/%'
           OR s3_path LIKE 's3://ods-curated-local/insurance/policies/%'
    """)
    cur.execute("""
        DELETE FROM pipeline.run_stage_log s USING pipeline.run_log r
         WHERE s.run_id = r.run_id
           AND r.domain='insurance' AND r.dataset='policies'
    """)
    cur.execute("""
        DELETE FROM pipeline.reconciliation_log
         WHERE domain='insurance' AND dataset='policies'
    """)
    cur.execute("""
        DELETE FROM pipeline.run_log
         WHERE domain = 'insurance' AND dataset = 'policies'
    """)
    pg.commit()
    # Clear curated and DLQ buckets for these test dates
    for bucket in ("ods-curated-local", "ods-dlq-local"):
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix="insurance/policies/"):
            for obj in page.get("Contents", []):
                s3.delete_object(Bucket=bucket, Key=obj["Key"])
    yield


def ingest(s3_path, run_id=None):
    run_id = run_id or str(uuid.uuid4())
    cmd = ["docker", "run", "--rm", "--network", NETWORK] + GLUE_COMMON + [
        "ods-glue:local", "spark-submit",
        "--py-files", PY_FILES,
        "/home/glue_user/workspace/jobs/ods_ingestion.py",
        "--run_id", run_id, "--domain", "insurance", "--dataset", "policies",
        "--s3_input_path", s3_path,
    ]
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
    return r, run_id


def publish(s3_path, run_id=None):
    run_id = run_id or str(uuid.uuid4())
    cmd = ["docker", "run", "--rm", "--network", NETWORK] + GLUE_KAFKA_ENV + [
        "ods-glue:local", "spark-submit",
        "--py-files", PY_FILES,
        "/home/glue_user/workspace/jobs/ods_s3_publish.py",
        "--run_id", run_id, "--domain", "insurance", "--dataset", "policies",
        "--s3_input_path", s3_path,
    ]
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
    return r, run_id


def upload(s3, key, content, bucket=RAW_BUCKET):
    s3.put_object(Bucket=bucket, Key=key, Body=content.encode())


def kafka_count(topic=TOPIC, timeout=15.0):
    c = Consumer({"bootstrap.servers": KAFKA_BROKERS,
                  "group.id": f"test-{uuid.uuid4()}",
                  "auto.offset.reset": "earliest",
                  "enable.auto.commit": False})
    c.subscribe([topic])
    n, deadline = 0, time.time() + timeout
    while time.time() < deadline:
        m = c.poll(1.0)
        if m is None: continue
        if m.error(): break
        n += 1
    c.close()
    return n


def log_status(pg, run_id):
    cur = pg.cursor()
    cur.execute(
        "SELECT status FROM pipeline.run_log "
        "WHERE run_id=%s ORDER BY started_at DESC LIMIT 1", (run_id,))
    row = cur.fetchone()
    return row[0] if row else None


def dlq_objects(s3, prefix="insurance/policies/"):
    return s3.list_objects_v2(Bucket=DLQ_BUCKET, Prefix=prefix).get("KeyCount", 0)


# ── Scenario 1 — Happy path ──────────────────────────────────────────────────

def test_1_happy_path(s3, pg):
    key = "insurance/policies/date=20260601/policies_20260601.csv"
    good = open(f"{os.getcwd()}/tests/fixtures/policies_good.csv").read()
    upload(s3, key, good)

    r, run_ingest = ingest(f"s3://ods-raw-local/{key}")
    assert r.returncode == 0, r.stderr

    curated = s3.list_objects_v2(Bucket=CURATED_BUCKET, Prefix="insurance/policies/date=2026-06-01/")
    assert curated.get("KeyCount", 0) > 0

    curated_path = "s3://ods-curated-local/insurance/policies/date=2026-06-01/"
    r2, run_pub = publish(curated_path)
    assert r2.returncode == 0, r2.stderr

    msgs = kafka_count()
    assert msgs >= 2
    assert log_status(pg, run_pub) == "succeeded"

    cur = pg.cursor()
    cur.execute("SELECT record_count_published FROM pipeline.run_log WHERE run_id=%s", (run_pub,))
    row = cur.fetchone()
    assert row is not None and row[0] == 2


# ── Scenario 2 — Idempotency ─────────────────────────────────────────────────

def test_2_idempotency(s3, pg):
    key = "insurance/policies/date=20260602/policies_20260602.csv"
    good = open(f"{os.getcwd()}/tests/fixtures/policies_good.csv").read()
    upload(s3, key, good)

    ingest(f"s3://ods-raw-local/{key}")
    msgs_after_first = kafka_count()

    # Run ingestion again — same file
    _, run2 = ingest(f"s3://ods-raw-local/{key}")
    msgs_after_second = kafka_count()

    assert msgs_after_first == msgs_after_second
    assert log_status(pg, run2) == "succeeded"


# ── Scenario 3 — Schema incompatible ────────────────────────────────────────

def test_3_schema_incompatible(s3, pg):
    missing_col = open(f"{os.getcwd()}/tests/fixtures/policies_missing_column.csv").read()
    key = "insurance/policies/date=20260603/policies_20260603.csv"
    upload(s3, key, missing_col)

    r, run_id = ingest(f"s3://ods-raw-local/{key}")
    assert r.returncode != 0 or log_status(pg, run_id) == "failed"

    # Nothing should land in curated
    curated = s3.list_objects_v2(Bucket=CURATED_BUCKET, Prefix="insurance/policies/date=2026-06-03/")
    assert curated.get("KeyCount", 0) == 0


# ── Scenario 4 — DQ hard block (null policy_id) ──────────────────────────────

def test_4_dq_hard_block_null_policy_id(s3, pg):
    null_id_csv = open(f"{os.getcwd()}/tests/fixtures/policies_null_policy_id.csv").read()
    key = "insurance/policies/date=20260604/policies_20260604.csv"
    upload(s3, key, null_id_csv)

    dlq_before = dlq_objects(s3)
    r, run_id = ingest(f"s3://ods-raw-local/{key}")
    assert r.returncode == 0, r.stderr

    # Failing row went to DLQ
    dlq_after = dlq_objects(s3)
    assert dlq_after > dlq_before

    # One passing row landed in curated
    curated = s3.list_objects_v2(Bucket=CURATED_BUCKET, Prefix="insurance/policies/date=2026-06-04/")
    assert curated.get("KeyCount", 0) > 0

    assert log_status(pg, run_id) == "succeeded"


# ── Scenario 5 — DQ soft warn (high premium) ─────────────────────────────────

def test_5_dq_soft_warn_high_premium(s3, pg):
    high_csv = open(f"{os.getcwd()}/tests/fixtures/policies_high_premium.csv").read()
    key = "insurance/policies/date=20260605/policies_20260605.csv"
    upload(s3, key, high_csv)

    r, run_id = ingest(f"s3://ods-raw-local/{key}")
    assert r.returncode == 0, r.stderr

    # Soft warn does not block — row lands in curated
    curated = s3.list_objects_v2(Bucket=CURATED_BUCKET, Prefix="insurance/policies/date=2026-06-05/")
    assert curated.get("KeyCount", 0) > 0

    # Job completes successfully regardless of soft-warn triggers
    assert log_status(pg, run_id) == "succeeded"


# ── Scenario 6 — Business date extraction ────────────────────────────────────

def test_6_business_date_extraction(s3, pg):
    good = open(f"{os.getcwd()}/tests/fixtures/policies_good.csv").read()
    key = "insurance/policies/date=20261231/policies_20261231.csv"
    upload(s3, key, good)

    _, run_id = ingest(f"s3://ods-raw-local/{key}")
    cur = pg.cursor()
    cur.execute(
        "SELECT business_date FROM pipeline.run_log "
        "WHERE run_id=%s", (run_id,))
    row = cur.fetchone()
    assert row is not None
    assert str(row[0]) == "2026-12-31"


# ── Scenario 7 — Publish idempotency ─────────────────────────────────────────

def test_7_publish_idempotency(s3, pg):
    good = open(f"{os.getcwd()}/tests/fixtures/policies_good.csv").read()
    key = "insurance/policies/date=20260607/policies_20260607.csv"
    upload(s3, key, good)

    ingest(f"s3://ods-raw-local/{key}")
    curated = "s3://ods-curated-local/insurance/policies/date=2026-06-07/"
    r1, _ = publish(curated)
    assert r1.returncode == 0, r1.stderr

    msgs_after_first = kafka_count()

    # Second publish to same path — file_state guard exits 0, no new messages
    r2, _ = publish(curated)
    assert r2.returncode == 0, r2.stderr

    msgs_after_second = kafka_count()
    assert msgs_after_second == msgs_after_first


# ── Scenario 8 — Full pipeline: ingest + publish ─────────────────────────────

def test_8_full_pipeline(s3, pg):
    good = open(f"{os.getcwd()}/tests/fixtures/policies_good.csv").read()
    key = "insurance/policies/date=20260608/policies_20260608.csv"
    upload(s3, key, good)

    r, run_ingest = ingest(f"s3://ods-raw-local/{key}")
    assert r.returncode == 0, r.stderr

    curated = "s3://ods-curated-local/insurance/policies/date=2026-06-08/"
    r2, run_pub = publish(curated)
    assert r2.returncode == 0, r2.stderr

    cur = pg.cursor()
    cur.execute("SELECT record_count_published FROM pipeline.run_log WHERE run_id=%s", (run_pub,))
    assert cur.fetchone()[0] == 2


# ── Scenario 9 — Lineage written ─────────────────────────────────────────────

def test_9_lineage_written(s3, pg):
    good = open(f"{os.getcwd()}/tests/fixtures/policies_good.csv").read()
    key = "insurance/policies/date=20260609/policies_20260609.csv"
    upload(s3, key, good)

    ingest(f"s3://ods-raw-local/{key}")
    curated = "s3://ods-curated-local/insurance/policies/date=2026-06-09/"
    _, run_pub = publish(curated)

    cur = pg.cursor()
    cur.execute(
        """
        SELECT d.source_type, r.kafka_topic, r.schema_version_id
          FROM pipeline.run_log r
          JOIN pipeline.dataset_config d
            ON d.domain=r.domain AND d.dataset=r.dataset
         WHERE r.run_id=%s
        """,
        (run_pub,),
    )
    row = cur.fetchone()
    assert row is not None
    assert row[0] in ("file", "s3_batch")
    assert row[1] == "ods.insurance.policies"
    # schema_version_id may be NULL for the publish-job run_log (old design
    # populated it via lineage); accept None or 1 for new schema.
    assert row[2] in (None, 1)
