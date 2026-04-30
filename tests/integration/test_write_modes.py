"""
Integration tests for write-mode patterns: append and upsert.
Prerequisite: docker compose up -d (full stack running).
Tests bypass Airflow and invoke Glue jobs directly via Docker.
"""
import os, subprocess, time, uuid
import boto3, psycopg2, pytest, requests

# ── Constants ─────────────────────────────────────────────────────────────────

LOCALSTACK   = os.environ.get("LOCALSTACK_ENDPOINT", "http://localhost:4566")
PG_PORT      = int(os.environ.get("POSTGRES_PORT", "5440"))
CONNECT_URL  = os.environ.get("CONNECT_URL", "http://localhost:8083")
SR_URL       = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")
NETWORK      = "ods-network"
GLUE_IMAGE   = "ods-glue:local"
RAW_BUCKET   = "ods-raw-local"

_HOST_JOBS = os.environ.get("HOST_JOBS_PATH", "")

GLUE_COMMON = [
    "docker", "run", "--rm", "--network", NETWORK,
    "-e", "AWS_ACCESS_KEY_ID=test",
    "-e", "AWS_SECRET_ACCESS_KEY=test",
    "-e", "AWS_DEFAULT_REGION=eu-west-1",
    "-e", "LOCALSTACK_ENDPOINT=http://localstack:4566",
    "-e", "KAFKA_BOOTSTRAP_SERVERS=broker:29092",
    "-e", "SCHEMA_REGISTRY_URL=http://schema-registry:8081",
    "-e", "POSTGRES_HOST=postgres",
    "-e", "POSTGRES_DB=ods_dev",
    "-e", "POSTGRES_USER=ods",
    "-e", "POSTGRES_PASSWORD=ods",
    "-e", "ENV=local",
] + (["-v", f"{_HOST_JOBS}:/home/glue_user/workspace/jobs"] if _HOST_JOBS else
     ["-v", f"{os.getcwd()}/glue/jobs:/home/glue_user/workspace/jobs"])

SPARK = [
    GLUE_IMAGE,
    "spark-submit",
    "--py-files",
    "/home/glue_user/workspace/jobs/utils.py,/home/glue_user/workspace/jobs/dq.py",
]

# ── Avro schemas ──────────────────────────────────────────────────────────────

_EVENTS_APPEND_SCHEMA = {
    "type": "record",
    "name": "events_append",
    "namespace": "ods.insurance",
    "fields": [
        {"name": "event_id",            "type": "string"},
        {"name": "policy_id",           "type": "string"},
        {"name": "event_type",          "type": "string"},
        {"name": "event_date",          "type": ["null", {"type": "int", "logicalType": "date"}], "default": None},
        {"name": "amount",              "type": ["null", "double"], "default": None},
        {"name": "_ods_business_date",  "type": "string"},
        {"name": "_ods_run_id",         "type": "string"},
    ],
}

_POLICIES_UPSERT_SCHEMA = {
    "type": "record",
    "name": "policies_upsert",
    "namespace": "ods.insurance",
    "fields": [
        {"name": "policy_id",           "type": "string"},
        {"name": "status",              "type": ["null", "string"], "default": None},
        {"name": "premium",             "type": ["null", "double"], "default": None},
        {"name": "effective_date",      "type": ["null", {"type": "int", "logicalType": "date"}], "default": None},
        {"name": "_ods_business_date",  "type": "string"},
        {"name": "_ods_run_id",         "type": "string"},
    ],
}

# ── JDBC connector configs ────────────────────────────────────────────────────

_APPEND_CONNECTOR_NAME  = "jdbc-sink-insurance-events-append"
_UPSERT_CONNECTOR_NAME  = "jdbc-sink-insurance-policies-upsert"

_APPEND_CONNECTOR_CONFIG = {
    "name": _APPEND_CONNECTOR_NAME,
    "config": {
        "connector.class":           "io.confluent.connect.jdbc.JdbcSinkConnector",
        "tasks.max":                 "1",
        "topics":                    "ods.insurance.events_append",
        "connection.url":            "jdbc:postgresql://postgres:5432/ods_dev",
        "connection.user":           "ods",
        "connection.password":       "ods",
        "auto.create":               "true",
        "auto.evolve":               "true",
        "insert.mode":               "insert",
        "pk.mode":                   "none",
        "table.name.format":         "ods.events_append",
        "key.converter":             "org.apache.kafka.connect.storage.StringConverter",
        "value.converter":           "io.confluent.connect.avro.AvroConverter",
        "value.converter.schema.registry.url": "http://schema-registry:8081",
    },
}

_UPSERT_CONNECTOR_CONFIG = {
    "name": _UPSERT_CONNECTOR_NAME,
    "config": {
        "connector.class":           "io.confluent.connect.jdbc.JdbcSinkConnector",
        "tasks.max":                 "1",
        "topics":                    "ods.insurance.policies_upsert",
        "connection.url":            "jdbc:postgresql://postgres:5432/ods_dev",
        "connection.user":           "ods",
        "connection.password":       "ods",
        "auto.create":               "true",
        "auto.evolve":               "true",
        "insert.mode":               "upsert",
        "pk.mode":                   "record_value",
        "pk.fields":                 "policy_id",
        "table.name.format":         "ods.policies_upsert",
        "key.converter":             "org.apache.kafka.connect.storage.StringConverter",
        "value.converter":           "io.confluent.connect.avro.AvroConverter",
        "value.converter.schema.registry.url": "http://schema-registry:8081",
    },
}

# ── Helpers ───────────────────────────────────────────────────────────────────

def _s3():
    return boto3.client(
        "s3", endpoint_url=LOCALSTACK,
        aws_access_key_id="test", aws_secret_access_key="test",
        region_name="eu-west-1",
    )


def _upload_csv(bucket: str, key: str, body: str) -> None:
    s3 = _s3()
    try:
        s3.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-1"},
        )
    except Exception:
        pass
    s3.put_object(Bucket=bucket, Key=key, Body=body.encode())


def _ingest(run_id: str, domain: str, dataset: str, s3_path: str):
    cmd = GLUE_COMMON + SPARK + [
        "/home/glue_user/workspace/jobs/ods_ingestion.py",
        "--run_id", run_id,
        "--domain", domain,
        "--dataset", dataset,
        "--s3_input_path", s3_path,
    ]
    return subprocess.run(cmd, capture_output=True, text=True, timeout=300)


def _publish(run_id: str, domain: str, dataset: str, business_date: str):
    # business_date is YYYYMMDD; curated path uses YYYY-MM-DD partition
    bd_fmt = f"{business_date[:4]}-{business_date[4:6]}-{business_date[6:]}"
    curated_path = f"s3://ods-curated-local/{domain}/{dataset}/date={bd_fmt}/"
    cmd = GLUE_COMMON + SPARK + [
        "/home/glue_user/workspace/jobs/ods_s3_publish.py",
        "--run_id", run_id,
        "--domain", domain,
        "--dataset", dataset,
        "--s3_input_path", curated_path,
    ]
    return subprocess.run(cmd, capture_output=True, text=True, timeout=300)


def _clean(pg_conn, domain: str, dataset: str, business_date: str = None) -> None:
    with pg_conn.cursor() as cur:
        if business_date:
            cur.execute(
                "DELETE FROM pipeline.run_stage_log "
                "WHERE run_id IN (SELECT run_id FROM pipeline.run_log "
                "WHERE domain=%s AND dataset=%s AND business_date=%s)",
                (domain, dataset, business_date),
            )
            cur.execute(
                "DELETE FROM pipeline.run_log WHERE domain=%s AND dataset=%s AND business_date=%s",
                (domain, dataset, business_date),
            )
            cur.execute(
                "DELETE FROM pipeline.file_catalogue WHERE domain=%s AND dataset=%s AND business_date=%s",
                (domain, dataset, business_date),
            )
        else:
            cur.execute(
                "DELETE FROM pipeline.run_stage_log "
                "WHERE run_id IN (SELECT run_id FROM pipeline.run_log "
                "WHERE domain=%s AND dataset=%s)",
                (domain, dataset),
            )
            cur.execute(
                "DELETE FROM pipeline.run_log WHERE domain=%s AND dataset=%s",
                (domain, dataset),
            )
            cur.execute(
                "DELETE FROM pipeline.file_catalogue WHERE domain=%s AND dataset=%s",
                (domain, dataset),
            )
    pg_conn.commit()


def _wait_for_count(
    pg_conn,
    table: str,
    where_clause: str,
    where_args: tuple,
    min_count: int,
    timeout: int = 90,
) -> int:
    """Poll Postgres every 3 s until row count >= min_count or timeout expires.
    Returns the actual row count at the time of return."""
    deadline = time.time() + timeout
    count = 0
    query = f"SELECT COUNT(*) FROM {table}"
    if where_clause:
        query += f" WHERE {where_clause}"
    while time.time() < deadline:
        with pg_conn.cursor() as cur:
            cur.execute(query, where_args)
            count = cur.fetchone()[0]
        if count >= min_count:
            return count
        time.sleep(3)
    return count


# ── Module-level fixtures ─────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def pg_conn():
    conn = psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=PG_PORT,
        dbname="ods_dev",
        user="ods",
        password="ods",
    )
    yield conn
    conn.close()


@pytest.fixture(scope="module", autouse=True)
def setup_schemas():
    """Register Avro schemas in Schema Registry for events_append and policies_upsert."""
    subjects = {
        "ods.insurance.events_append-value":   _EVENTS_APPEND_SCHEMA,
        "ods.insurance.policies_upsert-value": _POLICIES_UPSERT_SCHEMA,
    }
    import json as _json
    for subject, schema in subjects.items():
        url = f"{SR_URL}/subjects/{subject}/versions"
        payload = {"schemaType": "AVRO", "schema": _json.dumps(schema)}
        resp = requests.post(
            url, json=payload,
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
            timeout=10,
        )
        assert resp.status_code in (200, 201, 409), (
            f"Schema registration failed for {subject}: {resp.status_code} {resp.text}"
        )
    yield


@pytest.fixture(scope="module", autouse=True)
def provision_connectors():
    """Provision JDBC sink connectors if they do not already exist."""
    for cfg in (_APPEND_CONNECTOR_CONFIG, _UPSERT_CONNECTOR_CONFIG):
        name = cfg["name"]
        check = requests.get(f"{CONNECT_URL}/connectors/{name}", timeout=10)
        if check.status_code == 404:
            resp = requests.post(
                f"{CONNECT_URL}/connectors",
                json=cfg,
                headers={"Content-Type": "application/json"},
                timeout=10,
            )
            assert resp.status_code in (200, 201), (
                f"Failed to provision connector {name}: {resp.status_code} {resp.text}"
            )
    yield


@pytest.fixture(scope="module", autouse=True)
def setup_dataset_configs(pg_conn):
    """Insert dataset_config rows for events_append and policies_upsert if absent."""
    import json

    events_append_row = {
        "domain":                "insurance",
        "dataset":               "events_append",
        "source_type":           "s3_batch",
        "filename_pattern":      r"events_append_(?P<bd>\d{8})\.csv",
        "target_topic":          "ods.insurance.events_append",
        "schema_id":             "ods.insurance.events_append-value",
        "schema_version":        1,
        "key_fields":            json.dumps([]),
        "dq_rules":              json.dumps({
            "hard_blocks": [{"field": "event_id", "rule": "not_null"}],
            "soft_warns": [],
        }),
        "postgres_target_table": "ods.events_append",
        "s3_curated_path":       "s3://ods-curated-local/insurance/events_append/",
        "version":               1,
        "data_classification":   "Internal",
        "active":                True,
    }

    policies_upsert_row = {
        "domain":                "insurance",
        "dataset":               "policies_upsert",
        "source_type":           "s3_batch",
        "filename_pattern":      r"policies_upsert_(?P<bd>\d{8})\.csv",
        "target_topic":          "ods.insurance.policies_upsert",
        "schema_id":             "ods.insurance.policies_upsert-value",
        "schema_version":        1,
        "key_fields":            json.dumps(["policy_id"]),
        "dq_rules":              json.dumps({
            "hard_blocks": [{"field": "policy_id", "rule": "not_null"}],
            "soft_warns": [],
        }),
        "postgres_target_table": "ods.policies_upsert",
        "s3_curated_path":       "s3://ods-curated-local/insurance/policies_upsert/",
        "version":               1,
        "data_classification":   "Internal",
        "active":                True,
    }

    base_sql = """
        INSERT INTO pipeline.dataset_config
            (domain, dataset, source_type, filename_pattern, target_topic,
             schema_id, schema_version, key_fields, dq_rules,
             postgres_target_table, s3_curated_path, version,
             data_classification, active)
        VALUES
            (%(domain)s, %(dataset)s, %(source_type)s, %(filename_pattern)s,
             %(target_topic)s, %(schema_id)s, %(schema_version)s,
             %(key_fields)s::jsonb, %(dq_rules)s::jsonb,
             %(postgres_target_table)s, %(s3_curated_path)s, %(version)s,
             %(data_classification)s, %(active)s)
        ON CONFLICT (domain, dataset) DO UPDATE SET
            schema_id = EXCLUDED.schema_id
    """

    # Attempt insert with write_mode column; fall back gracefully if column absent
    write_mode_sql = """
        INSERT INTO pipeline.dataset_config
            (domain, dataset, source_type, filename_pattern, target_topic,
             schema_id, schema_version, key_fields, dq_rules,
             postgres_target_table, s3_curated_path, version,
             data_classification, active, write_mode)
        VALUES
            (%(domain)s, %(dataset)s, %(source_type)s, %(filename_pattern)s,
             %(target_topic)s, %(schema_id)s, %(schema_version)s,
             %(key_fields)s::jsonb, %(dq_rules)s::jsonb,
             %(postgres_target_table)s, %(s3_curated_path)s, %(version)s,
             %(data_classification)s, %(active)s, %(write_mode)s)
        ON CONFLICT (domain, dataset) DO UPDATE SET
            schema_id = EXCLUDED.schema_id,
            write_mode = EXCLUDED.write_mode
    """

    rows = [
        {**events_append_row,  "write_mode": "append"},
        {**policies_upsert_row, "write_mode": "upsert"},
    ]

    with pg_conn.cursor() as cur:
        for row in rows:
            try:
                cur.execute(write_mode_sql, row)
            except Exception:
                pg_conn.rollback()
                cur.execute(base_sql, row)
    pg_conn.commit()
    yield


@pytest.fixture(scope="module", autouse=True)
def reset_write_mode_state(pg_conn):
    """Wipe pipeline state and target tables before the module runs."""
    with pg_conn.cursor() as cur:
        # Remove pipeline metadata for both datasets
        for domain, dataset in [("insurance", "events_append"), ("insurance", "policies_upsert")]:
            cur.execute(
                "DELETE FROM pipeline.run_stage_log "
                "WHERE run_id IN (SELECT run_id FROM pipeline.run_log WHERE domain=%s AND dataset=%s)",
                (domain, dataset),
            )
            cur.execute(
                "DELETE FROM pipeline.run_log WHERE domain=%s AND dataset=%s",
                (domain, dataset),
            )
            cur.execute(
                "DELETE FROM pipeline.file_catalogue WHERE domain=%s AND dataset=%s",
                (domain, dataset),
            )
            cur.execute(
                "DELETE FROM pipeline.file_state WHERE s3_path LIKE %s",
                (f"s3://ods-raw-local/{domain}/{dataset}/%",),
            )
            cur.execute(
                "DELETE FROM pipeline.file_state WHERE s3_path LIKE %s",
                (f"s3://ods-curated-local/{domain}/{dataset}/%",),
            )
        # Truncate target tables (preserve schema so JDBC connector keeps working)
        cur.execute("TRUNCATE TABLE ods.events_append")
        cur.execute("TRUNCATE TABLE ods.policies_upsert")
    pg_conn.commit()

    # Clear curated S3 prefixes
    s3 = _s3()
    for prefix in ("insurance/events_append/", "insurance/policies_upsert/"):
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket="ods-curated-local", Prefix=prefix):
            for obj in page.get("Contents", []):
                s3.delete_object(Bucket="ods-curated-local", Key=obj["Key"])

    # Purge Kafka topics so connector starts from offset 0 with no stale messages
    import subprocess as _sp
    for topic in ("ods.insurance.events_append", "ods.insurance.policies_upsert"):
        _sp.run(
            ["docker", "exec", "avivaods-broker-1", "kafka-topics",
             "--bootstrap-server", "localhost:9092", "--delete", "--topic", topic],
            capture_output=True,
        )
        time.sleep(1)
        _sp.run(
            ["docker", "exec", "avivaods-broker-1", "kafka-topics",
             "--bootstrap-server", "localhost:9092", "--create", "--topic", topic,
             "--partitions", "1", "--replication-factor", "1"],
            capture_output=True,
        )
    # Restart connector tasks after topic recreation
    time.sleep(2)
    for name in (_APPEND_CONNECTOR_NAME, _UPSERT_CONNECTOR_NAME):
        requests.post(f"{CONNECT_URL}/connectors/{name}/tasks/0/restart", timeout=10)
    time.sleep(3)
    yield


# ── Connector provisioner tests (no Glue) ────────────────────────────────────

def test_append_connector_insert_mode():
    """JDBC sink for events_append must use insert mode with no PK."""
    resp = requests.get(
        f"{CONNECT_URL}/connectors/{_APPEND_CONNECTOR_NAME}/config",
        timeout=10,
    )
    assert resp.status_code == 200, (
        f"Connector {_APPEND_CONNECTOR_NAME} not found: {resp.status_code}"
    )
    cfg = resp.json()
    assert cfg.get("insert.mode") == "insert", (
        f"Expected insert.mode=insert, got {cfg.get('insert.mode')}"
    )
    assert cfg.get("pk.mode") == "none", (
        f"Expected pk.mode=none, got {cfg.get('pk.mode')}"
    )


def test_upsert_connector_upsert_mode():
    """JDBC sink for policies_upsert must use upsert mode keyed on policy_id."""
    resp = requests.get(
        f"{CONNECT_URL}/connectors/{_UPSERT_CONNECTOR_NAME}/config",
        timeout=10,
    )
    assert resp.status_code == 200, (
        f"Connector {_UPSERT_CONNECTOR_NAME} not found: {resp.status_code}"
    )
    cfg = resp.json()
    assert cfg.get("insert.mode") == "upsert", (
        f"Expected insert.mode=upsert, got {cfg.get('insert.mode')}"
    )
    assert cfg.get("pk.fields") == "policy_id", (
        f"Expected pk.fields=policy_id, got {cfg.get('pk.fields')}"
    )


# ── Append pattern e2e tests ──────────────────────────────────────────────────

def test_append_first_file(pg_conn):
    """First events_append file: 2 rows should land in ods.events_append."""
    domain   = "insurance"
    dataset  = "events_append"
    bd       = "20260701"
    filename = f"events_append_{bd}.csv"
    s3_key   = f"{domain}/{dataset}/date={bd}/{filename}"
    csv_body = (
        "event_id,policy_id,event_type,event_date,amount\n"
        "E001,P1,RENEWAL,2026-07-01,100.00\n"
        "E002,P2,NEW,2026-07-01,200.00\n"
    )

    _upload_csv(RAW_BUCKET, s3_key, csv_body)

    run_id = str(uuid.uuid4())
    r_ingest = _ingest(run_id, domain, dataset, f"s3://{RAW_BUCKET}/{s3_key}")
    assert r_ingest.returncode == 0, (
        f"Ingestion failed:\nSTDOUT: {r_ingest.stdout}\nSTDERR: {r_ingest.stderr}"
    )

    run_id_pub = str(uuid.uuid4())
    r_pub = _publish(run_id_pub, domain, dataset, bd)
    assert r_pub.returncode == 0, (
        f"Publish failed:\nSTDOUT: {r_pub.stdout}\nSTDERR: {r_pub.stderr}"
    )

    count = _wait_for_count(pg_conn, "ods.events_append", None, (), 2, timeout=90)
    assert count >= 2, f"Expected >= 2 rows in ods.events_append after first file, got {count}"


def test_append_second_file_accumulates(pg_conn):
    """Second events_append file: rows accumulate (no replace), total >= 3."""
    domain   = "insurance"
    dataset  = "events_append"
    bd       = "20260702"
    filename = f"events_append_{bd}.csv"
    s3_key   = f"{domain}/{dataset}/date={bd}/{filename}"
    csv_body = (
        "event_id,policy_id,event_type,event_date,amount\n"
        "E003,P3,RENEWAL,2026-07-02,150.00\n"
    )

    _upload_csv(RAW_BUCKET, s3_key, csv_body)

    run_id = str(uuid.uuid4())
    r_ingest = _ingest(run_id, domain, dataset, f"s3://{RAW_BUCKET}/{s3_key}")
    assert r_ingest.returncode == 0, (
        f"Ingestion failed:\nSTDOUT: {r_ingest.stdout}\nSTDERR: {r_ingest.stderr}"
    )

    run_id_pub = str(uuid.uuid4())
    r_pub = _publish(run_id_pub, domain, dataset, bd)
    assert r_pub.returncode == 0, (
        f"Publish failed:\nSTDOUT: {r_pub.stdout}\nSTDERR: {r_pub.stderr}"
    )

    # Total should be at least 3 (2 from first file + 1 new)
    count = _wait_for_count(pg_conn, "ods.events_append", None, (), 3, timeout=90)
    assert count >= 3, (
        f"Expected >= 3 accumulated rows in ods.events_append, got {count}. "
        "Append mode must not replace existing rows."
    )


def test_append_rerun_idempotent(pg_conn):
    """Re-ingesting the same events_append file must not add duplicate rows."""
    domain   = "insurance"
    dataset  = "events_append"
    bd       = "20260701"
    filename = f"events_append_{bd}.csv"
    s3_key   = f"{domain}/{dataset}/date={bd}/{filename}"
    csv_body = (
        "event_id,policy_id,event_type,event_date,amount\n"
        "E001,P1,RENEWAL,2026-07-01,100.00\n"
        "E002,P2,NEW,2026-07-01,200.00\n"
    )

    # Re-upload same content (same MD5 → file_state should block)
    _upload_csv(RAW_BUCKET, s3_key, csv_body)

    count_before = _wait_for_count(pg_conn, "ods.events_append", None, (), 1, timeout=10)

    run_id = str(uuid.uuid4())
    r_ingest = _ingest(run_id, domain, dataset, f"s3://{RAW_BUCKET}/{s3_key}")
    # file_state guard exits 0 (graceful skip), not an error
    assert r_ingest.returncode == 0, (
        f"Re-ingest should exit 0 (idempotency skip):\nSTDERR: {r_ingest.stderr}"
    )

    # Give Connect a moment to flush anything unexpected
    time.sleep(5)

    count_after = _wait_for_count(pg_conn, "ods.events_append", None, (), 1, timeout=10)
    assert count_after == count_before, (
        f"Duplicate rows added on re-run: before={count_before}, after={count_after}"
    )


def test_append_lineage(pg_conn):
    """pipeline.run_log must have >= 2 succeeded rows for events_append."""
    with pg_conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM pipeline.run_log "
            "WHERE domain=%s AND dataset=%s AND status=%s",
            ("insurance", "events_append", "succeeded"),
        )
        count = cur.fetchone()[0]
    assert count >= 2, (
        f"Expected >= 2 succeeded run_log entries for events_append, got {count}"
    )


# ── Upsert pattern e2e tests ──────────────────────────────────────────────────

def test_upsert_first_file(pg_conn):
    """First policies_upsert file: U001 with status=ACTIVE lands in ods.policies_upsert."""
    domain   = "insurance"
    dataset  = "policies_upsert"
    bd       = "20260701"
    filename = f"policies_upsert_{bd}.csv"
    s3_key   = f"{domain}/{dataset}/date={bd}/{filename}"
    csv_body = (
        "policy_id,status,premium,effective_date\n"
        "U001,ACTIVE,500.00,2026-01-01\n"
    )

    _upload_csv(RAW_BUCKET, s3_key, csv_body)

    run_id = str(uuid.uuid4())
    r_ingest = _ingest(run_id, domain, dataset, f"s3://{RAW_BUCKET}/{s3_key}")
    assert r_ingest.returncode == 0, (
        f"Ingestion failed:\nSTDOUT: {r_ingest.stdout}\nSTDERR: {r_ingest.stderr}"
    )

    run_id_pub = str(uuid.uuid4())
    r_pub = _publish(run_id_pub, domain, dataset, bd)
    assert r_pub.returncode == 0, (
        f"Publish failed:\nSTDOUT: {r_pub.stdout}\nSTDERR: {r_pub.stderr}"
    )

    count = _wait_for_count(
        pg_conn, "ods.policies_upsert", "policy_id=%s AND status=%s",
        ("U001", "ACTIVE"), 1, timeout=90,
    )
    assert count >= 1, "Expected U001 with status=ACTIVE in ods.policies_upsert"


def test_upsert_second_file_updates(pg_conn):
    """Second file with same policy_id updates the row (status ACTIVE → LAPSED), no duplicate."""
    domain   = "insurance"
    dataset  = "policies_upsert"
    bd       = "20260702"
    filename = f"policies_upsert_{bd}.csv"
    s3_key   = f"{domain}/{dataset}/date={bd}/{filename}"
    csv_body = (
        "policy_id,status,premium,effective_date\n"
        "U001,LAPSED,500.00,2026-01-01\n"
    )

    _upload_csv(RAW_BUCKET, s3_key, csv_body)

    run_id = str(uuid.uuid4())
    r_ingest = _ingest(run_id, domain, dataset, f"s3://{RAW_BUCKET}/{s3_key}")
    assert r_ingest.returncode == 0, (
        f"Ingestion failed:\nSTDOUT: {r_ingest.stdout}\nSTDERR: {r_ingest.stderr}"
    )

    run_id_pub = str(uuid.uuid4())
    r_pub = _publish(run_id_pub, domain, dataset, bd)
    assert r_pub.returncode == 0, (
        f"Publish failed:\nSTDOUT: {r_pub.stdout}\nSTDERR: {r_pub.stderr}"
    )

    # Wait for upserted status to appear
    count_lapsed = _wait_for_count(
        pg_conn, "ods.policies_upsert", "policy_id=%s AND status=%s",
        ("U001", "LAPSED"), 1, timeout=90,
    )
    assert count_lapsed >= 1, "Expected U001 status to be LAPSED after upsert"

    # Only 1 total row for U001 — upsert must not duplicate
    with pg_conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM ods.policies_upsert WHERE policy_id=%s",
            ("U001",),
        )
        total = cur.fetchone()[0]
    assert total == 1, (
        f"Upsert created {total} rows for U001 — expected exactly 1 (no duplicate)"
    )


def test_upsert_rerun_idempotent(pg_conn):
    """Re-ingesting the same policies_upsert file must not create extra rows."""
    domain   = "insurance"
    dataset  = "policies_upsert"
    bd       = "20260701"
    filename = f"policies_upsert_{bd}.csv"
    s3_key   = f"{domain}/{dataset}/date={bd}/{filename}"
    csv_body = (
        "policy_id,status,premium,effective_date\n"
        "U001,ACTIVE,500.00,2026-01-01\n"
    )

    _upload_csv(RAW_BUCKET, s3_key, csv_body)

    with pg_conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM ods.policies_upsert WHERE policy_id=%s", ("U001",))
        count_before = cur.fetchone()[0]

    run_id = str(uuid.uuid4())
    r_ingest = _ingest(run_id, domain, dataset, f"s3://{RAW_BUCKET}/{s3_key}")
    assert r_ingest.returncode == 0, (
        f"Re-ingest should exit 0 (idempotency skip):\nSTDERR: {r_ingest.stderr}"
    )

    time.sleep(5)

    with pg_conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM ods.policies_upsert WHERE policy_id=%s", ("U001",))
        count_after = cur.fetchone()[0]

    assert count_after == count_before, (
        f"Re-run changed row count for U001: before={count_before}, after={count_after}"
    )


def test_upsert_lineage(pg_conn):
    """pipeline.run_log must have >= 2 succeeded rows for policies_upsert."""
    with pg_conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM pipeline.run_log "
            "WHERE domain=%s AND dataset=%s AND status=%s",
            ("insurance", "policies_upsert", "succeeded"),
        )
        count = cur.fetchone()[0]
    assert count >= 2, (
        f"Expected >= 2 succeeded run_log entries for policies_upsert, got {count}"
    )
