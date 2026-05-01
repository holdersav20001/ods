"""
Integration tests: dual-write pattern for insurance policies.

Day 1 (daily load):
  - policies dataset  → ods.insurance_policy (upsert, 1 row per policy)
  - insurance_policy_history dataset → ods.insurance_policy_history (append)

Day 2 (incremental):
  - policies upserted  → existing policy updated, new policy added
  - history accumulated → previous rows kept, new rows added

Prerequisite: docker compose up -d (full stack running).
"""
import json, os, subprocess, time, uuid
import boto3, psycopg2, pytest, requests

# ── Constants ─────────────────────────────────────────────────────────────────

LOCALSTACK  = os.environ.get("LOCALSTACK_ENDPOINT", "http://localhost:4566")
PG_PORT     = int(os.environ.get("POSTGRES_PORT", "5440"))
CONNECT_URL = os.environ.get("CONNECT_URL", "http://localhost:8083")
SR_URL      = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")
NETWORK     = "ods-network"
GLUE_IMAGE  = "ods-glue:local"
RAW_BUCKET  = "ods-raw-local"

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
     ["-v", f"{os.getcwd()}/glue/jobs:/home/glue_user/workspace/jobs"]) + [
    "-v", f"{os.getcwd()}/airflow/dags/common:/home/glue_user/airflow/dags/common",
    "-v", f"{os.getcwd()}/ods_pipeline:/home/glue_user/ods_pipeline",
]

SPARK = [
    GLUE_IMAGE,
    "spark-submit",
    "--py-files",
    "/home/glue_user/workspace/jobs/utils.py,/home/glue_user/workspace/jobs/dq.py",
]

# ── Avro schemas ──────────────────────────────────────────────────────────────

_POLICY_FIELDS = [
    {"name": "policy_id",           "type": "string"},
    {"name": "status",              "type": ["null", "string"],  "default": None},
    {"name": "premium",             "type": ["null", "double"],  "default": None},
    {"name": "effective_date",      "type": ["null", {"type": "int", "logicalType": "date"}], "default": None},
    {"name": "_ods_business_date",  "type": "string"},
    {"name": "_ods_run_id",         "type": "string"},
    {"name": "_ods_file_id",        "type": ["null", "string"],  "default": None},
    {"name": "_ods_domain",         "type": ["null", "string"],  "default": None},
    {"name": "_ods_dataset",        "type": ["null", "string"],  "default": None},
    {"name": "_ods_source_application", "type": ["null", "string"], "default": None},
]

_POLICIES_SCHEMA = {
    "type": "record", "name": "policies",
    "namespace": "ods.insurance",
    "fields": _POLICY_FIELDS,
}

# ── JDBC connector configs ────────────────────────────────────────────────────

_POLICIES_CONNECTOR = {
    "name": "jdbc-sink-policies",
    "config": {
        "connector.class":  "io.confluent.connect.jdbc.JdbcSinkConnector",
        "tasks.max":        "1",
        "topics":           "ods.insurance.policies",
        "connection.url":   "jdbc:postgresql://postgres:5432/ods_dev",
        "connection.user":  "ods",
        "connection.password": "ods",
        "insert.mode":      "upsert",
        "pk.mode":          "record_value",
        "pk.fields":        "policy_id",
        "auto.create":      "false",
        "auto.evolve":      "false",
        "table.name.format": "ods.insurance_policy",
        "key.converter":    "org.apache.kafka.connect.storage.StringConverter",
        "value.converter":  "io.confluent.connect.avro.AvroConverter",
        "value.converter.schema.registry.url": "http://schema-registry:8081",
    },
}

_HISTORY_CONNECTOR = {
    "name": "jdbc-sink-policy-history",
    "config": {
        "connector.class":  "io.confluent.connect.jdbc.JdbcSinkConnector",
        "tasks.max":        "1",
        "topics":           "ods.insurance.policies",
        "connection.url":   "jdbc:postgresql://postgres:5432/ods_dev",
        "connection.user":  "ods",
        "connection.password": "ods",
        "insert.mode":      "insert",
        "pk.mode":          "none",
        "auto.create":      "false",
        "auto.evolve":      "false",
        "table.name.format": "ods.insurance_policy_history",
        "key.converter":    "org.apache.kafka.connect.storage.StringConverter",
        "value.converter":  "io.confluent.connect.avro.AvroConverter",
        "value.converter.schema.registry.url": "http://schema-registry:8081",
    },
}

# ── Day fixtures ──────────────────────────────────────────────────────────────

_DAY1_BD  = "20260801"
_DAY2_BD  = "20260802"
_POLICY_IDS = ("POL-001", "POL-002", "POL-003")

_DAY1_CSV = (
    "policy_id,status,premium,effective_date\n"
    "POL-001,ACTIVE,1200.00,2026-01-01\n"
    "POL-002,ACTIVE,950.50,2026-02-01\n"
)

_DAY2_CSV = (
    "policy_id,status,premium,effective_date\n"
    "POL-001,LAPSED,1200.00,2026-01-01\n"   # existing — status changes
    "POL-003,NEW,750.00,2026-08-02\n"        # brand new policy
)

# ── Helpers ───────────────────────────────────────────────────────────────────

def _s3():
    return boto3.client(
        "s3", endpoint_url=LOCALSTACK,
        aws_access_key_id="test", aws_secret_access_key="test",
        region_name="eu-west-1",
    )


def _upload(bucket: str, key: str, body: str) -> None:
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
        "--run_id", run_id, "--domain", domain, "--dataset", dataset,
        "--s3_input_path", s3_path,
    ]
    return subprocess.run(cmd, capture_output=True, text=True, timeout=300)


def _get_file_id_from_pg(domain: str, dataset: str, business_date_yyyymmdd: str) -> str | None:
    """Fetch file_id from file_catalogue after ingest creates the entry."""
    from datetime import date as _date
    bd = _date(int(business_date_yyyymmdd[:4]),
               int(business_date_yyyymmdd[4:6]),
               int(business_date_yyyymmdd[6:]))
    conn = psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=PG_PORT, dbname="ods_dev", user="ods", password="ods",
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT file_id FROM pipeline.file_catalogue "
                "WHERE domain=%s AND dataset=%s AND business_date=%s "
                "ORDER BY first_seen_at DESC LIMIT 1",
                (domain, dataset, bd),
            )
            row = cur.fetchone()
        return str(row[0]) if row else None
    finally:
        conn.close()


def _publish(run_id: str, domain: str, dataset: str, business_date: str,
             file_id: str | None = None):
    bd_fmt = f"{business_date[:4]}-{business_date[4:6]}-{business_date[6:]}"
    curated_path = f"s3://ods-curated-local/{domain}/{dataset}/date={bd_fmt}/"
    cmd = GLUE_COMMON + SPARK + [
        "/home/glue_user/workspace/jobs/ods_s3_publish.py",
        "--run_id", run_id, "--domain", domain, "--dataset", dataset,
        "--s3_input_path", curated_path,
    ]
    if file_id:
        cmd += ["--file_id", file_id]
    return subprocess.run(cmd, capture_output=True, text=True, timeout=300)


def _run_pipeline(domain: str, dataset: str, bd: str, s3_key: str):
    """Ingest, look up file_id, then publish with explicit lineage contract."""
    r_in = _ingest(str(uuid.uuid4()), domain, dataset, f"s3://{RAW_BUCKET}/{s3_key}")
    # Fetch the file_id created/upserted during ingest so publish uses it explicitly
    file_id = _get_file_id_from_pg(domain, dataset, bd)
    r_pub = _publish(str(uuid.uuid4()), domain, dataset, bd, file_id=file_id)
    return r_in, r_pub


def _wait_count(pg, table: str, where: str = None, args: tuple = (), min_count: int = 1, timeout: int = 90) -> int:
    deadline = time.time() + timeout
    query = f"SELECT COUNT(*) FROM {table}" + (f" WHERE {where}" if where else "")
    while time.time() < deadline:
        with pg.cursor() as cur:
            cur.execute(query, args)
            n = cur.fetchone()[0]
        if n >= min_count:
            return n
        time.sleep(3)
    return n


def _register_schema(subject: str, schema: dict):
    # Set NONE compatibility so we can force-update without evolution rules blocking
    requests.put(
        f"{SR_URL}/config/{subject}",
        json={"compatibility": "NONE"},
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        timeout=10,
    )
    url = f"{SR_URL}/subjects/{subject}/versions"
    payload = {"schemaType": "AVRO", "schema": json.dumps(schema)}
    resp = requests.post(url, json=payload,
                         headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
                         timeout=10)
    assert resp.status_code in (200, 201, 409), \
        f"Schema reg failed {subject}: {resp.status_code} {resp.text}"


def _provision_connector(cfg: dict):
    name = cfg["name"]
    resp = requests.put(
        f"{CONNECT_URL}/connectors/{name}/config",
        json=cfg["config"],
        headers={"Content-Type": "application/json"},
        timeout=10,
    )
    if resp.status_code == 404:
        resp = requests.post(f"{CONNECT_URL}/connectors", json=cfg,
                             headers={"Content-Type": "application/json"}, timeout=10)
    assert resp.status_code in (200, 201), \
        f"Connector provision failed {name}: {resp.status_code} {resp.text}"


def _delete_connector(name: str):
    requests.delete(f"{CONNECT_URL}/connectors/{name}", timeout=10)


def _recreate_topic(topic: str):
    import subprocess as _sp
    _sp.run(["docker", "exec", "avivaods-broker-1", "kafka-topics",
             "--bootstrap-server", "localhost:9092", "--delete", "--topic", topic],
            capture_output=True)
    time.sleep(1)
    _sp.run(["docker", "exec", "avivaods-broker-1", "kafka-topics",
             "--bootstrap-server", "localhost:9092", "--create", "--topic", topic,
             "--partitions", "1", "--replication-factor", "1"],
            capture_output=True)


def _clean_pipeline(pg, domain: str, dataset: str):
    with pg.cursor() as cur:
        cur.execute(
            "DELETE FROM pipeline.lineage_edge WHERE child_run_id IN "
            "(SELECT run_id FROM pipeline.run_log WHERE domain=%s AND dataset=%s)",
            (domain, dataset))
        cur.execute(
            "DELETE FROM pipeline.run_stage_log WHERE run_id IN "
            "(SELECT run_id FROM pipeline.run_log WHERE domain=%s AND dataset=%s)",
            (domain, dataset))
        cur.execute("DELETE FROM pipeline.run_events WHERE domain=%s AND dataset=%s",
                    (domain, dataset))
        cur.execute("DELETE FROM pipeline.run_log WHERE domain=%s AND dataset=%s", (domain, dataset))
        cur.execute("DELETE FROM pipeline.file_catalogue WHERE domain=%s AND dataset=%s", (domain, dataset))
        cur.execute("DELETE FROM pipeline.file_state WHERE s3_path LIKE %s",
                    (f"s3://ods-raw-local/{domain}/{dataset}/%",))
        cur.execute("DELETE FROM pipeline.file_state WHERE s3_path LIKE %s",
                    (f"s3://ods-curated-local/{domain}/{dataset}/%",))
    pg.commit()


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def pg():
    conn = psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=PG_PORT, dbname="ods_dev", user="ods", password="ods")
    yield conn
    conn.close()


@pytest.fixture(scope="module", autouse=True)
def setup_schemas():
    _register_schema("ods.insurance.policies-value", _POLICIES_SCHEMA)
    # Restart connectors to flush cached schema versions
    for name in (_POLICIES_CONNECTOR["name"], _HISTORY_CONNECTOR["name"]):
        requests.post(f"{CONNECT_URL}/connectors/{name}/restart?includeTasks=true",
                      timeout=10)
    time.sleep(3)
    yield


@pytest.fixture(scope="module", autouse=True)
def setup_connectors():
    _provision_connector(_POLICIES_CONNECTOR)
    _provision_connector(_HISTORY_CONNECTOR)
    yield


@pytest.fixture(scope="module", autouse=True)
def setup_dataset_configs(pg):
    rows = [
        {
            "domain": "insurance", "dataset": "policies",
            "source_type": "s3_batch",
            "filename_pattern": r"policies_(?P<bd>\d{8})\.csv",
            "target_topic": "ods.insurance.policies",
            "schema_id": "ods.insurance.policies-value", "schema_version": 1,
            "key_fields": json.dumps(["policy_id"]),
            "dq_rules": json.dumps({"hard_blocks": [{"field": "policy_id", "rule": "not_null"}], "soft_warns": []}),
            "postgres_target_table": "ods.insurance_policy",
            "s3_curated_path": "s3://ods-curated-local/insurance/policies/",
            "version": 1, "data_classification": "Internal", "active": True, "write_mode": "upsert",
        },
    ]
    sql = """
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
            write_mode = EXCLUDED.write_mode,
            target_topic = EXCLUDED.target_topic,
            postgres_target_table = EXCLUDED.postgres_target_table
    """
    with pg.cursor() as cur:
        for row in rows:
            cur.execute(sql, row)
    pg.commit()
    yield


@pytest.fixture(scope="module", autouse=True)
def reset_state(pg, setup_dataset_configs, setup_connectors):
    """Wipe both tables and pipeline state before module runs."""
    with pg.cursor() as cur:
        cur.execute("TRUNCATE TABLE ods.insurance_policy")
        cur.execute("TRUNCATE TABLE ods.insurance_policy_history")
    pg.commit()

    _clean_pipeline(pg, "insurance", "policies")

    s3 = _s3()
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket="ods-curated-local", Prefix="insurance/policies/"):
        for obj in page.get("Contents", []):
            s3.delete_object(Bucket="ods-curated-local", Key=obj["Key"])

    _recreate_topic("ods.insurance.policies")

    time.sleep(3)
    for name in (_POLICIES_CONNECTOR["name"], _HISTORY_CONNECTOR["name"]):
        requests.post(f"{CONNECT_URL}/connectors/{name}/tasks/0/restart", timeout=10)
    time.sleep(3)
    yield


# ── Day 1: daily load ─────────────────────────────────────────────────────────

def test_day1_insurance_policies_upsert(pg):
    """Day 1 daily load: 2 policies land in ods.insurance_policy (upsert)."""
    key = f"insurance/policies/date={_DAY1_BD}/policies_{_DAY1_BD}.csv"
    _upload(RAW_BUCKET, key, _DAY1_CSV)

    r_in, r_pub = _run_pipeline("insurance", "policies", _DAY1_BD, key)
    assert r_in.returncode == 0, f"Ingest failed:\n{r_in.stderr}"
    assert r_pub.returncode == 0, f"Publish failed:\n{r_pub.stderr}"

    count = _wait_count(
        pg,
        "ods.insurance_policy",
        "policy_id IN (%s,%s)",
        ("POL-001", "POL-002"),
        min_count=2,
    )
    assert count == 2, f"Expected 2 rows in insurance_policies after Day 1, got {count}"


def test_day1_both_tables_same_count(pg):
    """After Day 1: both tables have same row count — history sink reads same topic as upsert sink."""
    _wait_count(pg, "ods.insurance_policy_history", min_count=2)
    with pg.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM ods.insurance_policy WHERE policy_id IN (%s,%s)",
            ("POL-001", "POL-002"),
        )
        pol_count = cur.fetchone()[0]
        cur.execute(
            "SELECT COUNT(*) FROM ods.insurance_policy_history "
            "WHERE policy_id IN (%s,%s) AND _ods_business_date::text=%s",
            ("POL-001", "POL-002", "2026-08-01"),
        )
        hist_count = cur.fetchone()[0]
    assert pol_count == 2
    assert hist_count == 2


# ── Day 2: incremental load ───────────────────────────────────────────────────

def test_day2_insurance_policies_upserted(pg):
    """Day 2 incremental: POL-001 status ACTIVE→LAPSED, POL-003 added. insurance_policies = 3 rows."""
    key = f"insurance/policies/date={_DAY2_BD}/policies_{_DAY2_BD}.csv"
    _upload(RAW_BUCKET, key, _DAY2_CSV)

    r_in, r_pub = _run_pipeline("insurance", "policies", _DAY2_BD, key)
    assert r_in.returncode == 0, f"Ingest failed:\n{r_in.stderr}"
    assert r_pub.returncode == 0, f"Publish failed:\n{r_pub.stderr}"

    # Total 3 policies: POL-001 (upserted), POL-002 (unchanged), POL-003 (new)
    count = _wait_count(
        pg,
        "ods.insurance_policy",
        "policy_id IN (%s,%s,%s)",
        _POLICY_IDS,
        min_count=3,
    )
    assert count == 3, f"Expected 3 rows in insurance_policies after Day 2, got {count}"

    # POL-001 must be LAPSED (not ACTIVE)
    lapsed = _wait_count(pg, "ods.insurance_policy",
                         "policy_id=%s AND status=%s", ("POL-001", "LAPSED"), 1)
    assert lapsed == 1, "POL-001 should be LAPSED after Day 2 upsert"

    # POL-001 row count must be exactly 1 (upsert, not duplicate)
    with pg.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM ods.insurance_policy WHERE policy_id=%s", ("POL-001",))
        total = cur.fetchone()[0]
    assert total == 1, f"Upsert created {total} rows for POL-001, expected exactly 1"


def test_day2_policy_history_accumulates(pg):
    """Day 2 incremental: history sink appends Day 2 messages from same topic → total 4."""
    count = _wait_count(
        pg,
        "ods.insurance_policy_history",
        "policy_id IN (%s,%s,%s) AND _ods_business_date::text IN (%s,%s)",
        _POLICY_IDS + ("2026-08-01", "2026-08-02"),
        min_count=4,
    )
    assert count == 4, f"Expected 4 rows in history after Day 2 (2+2), got {count}"


def test_history_exceeds_current_policies(pg):
    """History row count > insurance_policies row count: history preserves all versions."""
    with pg.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM ods.insurance_policy WHERE policy_id IN (%s,%s,%s)",
            _POLICY_IDS,
        )
        pol_count = cur.fetchone()[0]
        cur.execute(
            "SELECT COUNT(*) FROM ods.insurance_policy_history "
            "WHERE policy_id IN (%s,%s,%s) AND _ods_business_date::text IN (%s,%s)",
            _POLICY_IDS + ("2026-08-01", "2026-08-02"),
        )
        hist_count = cur.fetchone()[0]

    assert hist_count > pol_count, (
        f"History ({hist_count}) should exceed current policies ({pol_count}) "
        "after 2 loads — history must accumulate all versions"
    )


def test_pol001_has_both_versions_in_history(pg):
    """POL-001 has 2 rows in history (ACTIVE from Day 1, LAPSED from Day 2)."""
    with pg.cursor() as cur:
        cur.execute(
            "SELECT status FROM ods.insurance_policy_history "
            "WHERE policy_id=%s ORDER BY _ods_business_date",
            ("POL-001",))
        rows = [r[0] for r in cur.fetchall()]
    assert "ACTIVE" in rows, "POL-001 ACTIVE version missing from history"
    assert "LAPSED" in rows, "POL-001 LAPSED version missing from history"
    assert len(rows) == 2, f"Expected 2 history rows for POL-001, got {len(rows)}"


# ── Lineage assertions ────────────────────────────────────────────────────────

def test_insurance_policy_file_id_populated(pg):
    """Every row in ods.insurance_policy has a non-null _ods_file_id."""
    with pg.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM ods.insurance_policy "
            "WHERE policy_id IN (%s,%s,%s) "
            "AND (_ods_file_id IS NULL OR _ods_file_id = '')",
            _POLICY_IDS,
        )
        nulls = cur.fetchone()[0]
    assert nulls == 0, f"{nulls} rows in insurance_policy missing _ods_file_id"


def test_insurance_policy_history_file_id_populated(pg):
    """Every row in ods.insurance_policy_history has a non-null _ods_file_id."""
    with pg.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM ods.insurance_policy_history "
            "WHERE policy_id IN (%s,%s,%s) "
            "AND (_ods_file_id IS NULL OR _ods_file_id = '')",
            _POLICY_IDS,
        )
        nulls = cur.fetchone()[0]
    assert nulls == 0, f"{nulls} rows in insurance_policy_history missing _ods_file_id"


def test_insurance_policy_metadata_contract_populated(pg):
    """Current policy rows carry the shared ODS metadata contract."""
    with pg.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*) FROM ods.insurance_policy
            WHERE policy_id IN (%s,%s,%s)
              AND (
                  _ods_domain IS DISTINCT FROM 'insurance'
               OR _ods_dataset IS DISTINCT FROM 'policies'
               OR _ods_source_application IS DISTINCT FROM 'sftp'
              )
            """,
            _POLICY_IDS,
        )
        mismatches = cur.fetchone()[0]
    assert mismatches == 0, (
        f"{mismatches} insurance_policy rows do not carry expected ODS metadata"
    )


def test_insurance_policy_history_metadata_contract_populated(pg):
    """History policy rows carry the shared ODS metadata contract."""
    with pg.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*) FROM ods.insurance_policy_history
            WHERE policy_id IN (%s,%s,%s)
              AND (
                  _ods_domain IS DISTINCT FROM 'insurance'
               OR _ods_dataset IS DISTINCT FROM 'policies'
               OR _ods_source_application IS DISTINCT FROM 'sftp'
              )
            """,
            _POLICY_IDS,
        )
        mismatches = cur.fetchone()[0]
    assert mismatches == 0, (
        f"{mismatches} insurance_policy_history rows do not carry expected ODS metadata"
    )


def test_file_id_joins_to_catalogue(pg):
    """_ods_file_id in data tables resolves to a file_catalogue row with s3_raw_path populated."""
    with pg.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) FROM ods.insurance_policy p
            LEFT JOIN pipeline.file_catalogue fc ON fc.file_id = p._ods_file_id::uuid
            WHERE p.policy_id IN (%s,%s,%s)
              AND fc.s3_raw_path IS NULL
        """, _POLICY_IDS)
        unlinked = cur.fetchone()[0]
    assert unlinked == 0, f"{unlinked} insurance_policy rows have _ods_file_id not in file_catalogue"

    with pg.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) FROM ods.insurance_policy_history h
            LEFT JOIN pipeline.file_catalogue fc ON fc.file_id = h._ods_file_id::uuid
            WHERE h.policy_id IN (%s,%s,%s)
              AND fc.s3_raw_path IS NULL
        """, _POLICY_IDS)
        unlinked = cur.fetchone()[0]
    assert unlinked == 0, f"{unlinked} insurance_policy_history rows have _ods_file_id not in file_catalogue"


def test_catalogue_has_raw_and_curated_paths(pg):
    """file_catalogue rows for insurance/policies have both s3_raw_path and s3_curated_path."""
    with pg.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) FROM pipeline.file_catalogue
            WHERE domain='insurance' AND dataset='policies'
              AND business_date IN ('2026-08-01', '2026-08-02')
              AND (s3_raw_path IS NULL OR s3_curated_path IS NULL)
        """)
        incomplete = cur.fetchone()[0]
    assert incomplete == 0, f"{incomplete} file_catalogue rows missing raw or curated path"


def test_run_log_file_id_populated(pg):
    """All publish run_log rows for insurance/policies have file_id linked to file_catalogue."""
    with pg.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) FROM pipeline.run_log r
            LEFT JOIN pipeline.file_catalogue fc ON fc.file_id = r.file_id
            WHERE r.domain='insurance' AND r.dataset='policies'
              AND r.pipeline_type='publish'
              AND r.business_date IN ('2026-08-01', '2026-08-02')
              AND fc.file_id IS NULL
        """)
        unlinked = cur.fetchone()[0]
    assert unlinked == 0, f"{unlinked} publish run_log rows missing file_id linkage"


def test_pol001_history_different_file_ids(pg):
    """POL-001 ACTIVE and LAPSED rows in history came from different source files."""
    with pg.cursor() as cur:
        cur.execute(
            "SELECT DISTINCT _ods_file_id FROM ods.insurance_policy_history WHERE policy_id=%s",
            ("POL-001",))
        file_ids = [r[0] for r in cur.fetchall()]
    assert len(file_ids) == 2, (
        f"Expected 2 distinct file_ids for POL-001 history, got {len(file_ids)}: {file_ids}"
    )


# ── Connector config assertions ───────────────────────────────────────────────

def test_policies_connector_is_upsert():
    """jdbc-sink-policies must use upsert mode keyed on policy_id."""
    cfg = requests.get(f"{CONNECT_URL}/connectors/jdbc-sink-policies/config", timeout=10).json()
    assert cfg.get("insert.mode") == "upsert"
    assert cfg.get("pk.fields") == "policy_id"


def test_history_connector_is_insert():
    """jdbc-sink-policy-history must use insert mode, no PK, and read from policies topic."""
    cfg = requests.get(f"{CONNECT_URL}/connectors/jdbc-sink-policy-history/config", timeout=10).json()
    assert cfg.get("insert.mode") == "insert"
    assert cfg.get("pk.mode") == "none"
    assert cfg.get("topics") == "ods.insurance.policies"


# ── Lineage edge + run_events enrichment ──────────────────────────────────────

def test_run_events_contain_file_id(pg):
    """pipeline.run_events rows for insurance/policies all carry a non-null file_id."""
    with pg.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM pipeline.run_events "
            "WHERE domain='insurance' AND dataset='policies' "
            "AND business_date IN ('2026-08-01', '2026-08-02') "
            "AND file_id IS NULL"
        )
        nulls = cur.fetchone()[0]
    assert nulls == 0, f"{nulls} run_events rows for insurance/policies have null file_id"

    # Every file_id in run_events resolves to a file_catalogue row
    with pg.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) FROM pipeline.run_events re
            LEFT JOIN pipeline.file_catalogue fc ON fc.file_id::text = re.file_id
            WHERE re.domain='insurance' AND re.dataset='policies'
              AND re.business_date IN ('2026-08-01', '2026-08-02')
              AND re.file_id IS NOT NULL AND fc.file_id IS NULL
        """)
        unlinked = cur.fetchone()[0]
    assert unlinked == 0, \
        f"{unlinked} run_events rows have file_id not present in file_catalogue"


def test_lineage_edges_written(pg):
    """pipeline.lineage_edge has raw_to_curated and curated_to_kafka edges for each run."""
    with pg.cursor() as cur:
        cur.execute("""
            SELECT le.edge_type, COUNT(*)
              FROM pipeline.lineage_edge le
              JOIN pipeline.run_log rl ON rl.run_id = le.child_run_id
             WHERE rl.domain='insurance' AND rl.dataset='policies'
               AND rl.business_date IN ('2026-08-01', '2026-08-02')
             GROUP BY le.edge_type
        """)
        rows = {r[0]: r[1] for r in cur.fetchall()}

    assert "raw_to_curated" in rows, \
        f"No raw_to_curated edges in lineage_edge; found: {list(rows.keys())}"
    assert "curated_to_kafka" in rows, \
        f"No curated_to_kafka edges in lineage_edge; found: {list(rows.keys())}"
    # 2 pipeline runs (Day 1 + Day 2) → ≥2 edges per type
    assert rows["raw_to_curated"] >= 2, \
        f"Expected >=2 raw_to_curated edges, got {rows['raw_to_curated']}"
    assert rows["curated_to_kafka"] >= 2, \
        f"Expected >=2 curated_to_kafka edges, got {rows['curated_to_kafka']}"
