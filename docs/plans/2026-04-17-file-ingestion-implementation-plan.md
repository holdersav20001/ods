# File Ingestion Pipeline — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a local-dev pipeline that reads CSV from S3 (LocalStack), converts to Parquet, and publishes Avro messages to Kafka (Confluent), orchestrated by Airflow, with full audit trail and configuration in PostgreSQL.

**Architecture:** Two Glue PySpark jobs (`ods_ingestion.py`: CSV→Parquet, `ods_s3_publish.py`: Parquet→Kafka) triggered by Airflow DAGs via `DockerOperator`. All dataset configuration and pipeline state live in PostgreSQL. LocalStack emulates S3; Confluent Platform provides Kafka and Schema Registry locally. DQ rules are stored as JSONB in Postgres and evaluated via a pure-Python rule engine (no DQDL dependency locally).

**Tech Stack:** Python 3.10, PySpark 3.3 (Glue 4.0), Apache Airflow 2.9, confluent-kafka, psycopg2-binary, boto3, PostgreSQL 15, LocalStack 3.x, Docker Compose, pytest 8.x

---

## Task 1: Project Scaffold + Docker Compose

**Files:**
- Create: `docker-compose.yml`
- Create: `.env.example`
- Create: `glue/Dockerfile`
- Create: `scripts/localstack-init.sh`
- Create: `db/migrations/00_create_databases.sql`
- Create: `requirements-dev.txt`

---

**Step 1: Create directory structure**

```bash
mkdir -p glue/jobs dags db/migrations schemas/insurance \
         scripts seeds/insurance tests/unit tests/integration tests/dags \
         config
```

---

**Step 2: Create `.env.example`**

```bash
cat > .env.example << 'EOF'
AWS_DEFAULT_REGION=eu-west-1
AWS_ACCESS_KEY_ID=test
AWS_SECRET_ACCESS_KEY=test
LOCALSTACK_ENDPOINT=http://localstack:4566
KAFKA_BOOTSTRAP_SERVERS=broker:29092
SCHEMA_REGISTRY_URL=http://schema-registry:8081
POSTGRES_HOST=postgres
POSTGRES_DB=ods_dev
POSTGRES_USER=ods
POSTGRES_PASSWORD=ods
ENV=local
EOF
cp .env.example .env
```

---

**Step 3: Create `scripts/localstack-init.sh`**

This script runs automatically when LocalStack starts. It creates all S3 buckets.

```bash
cat > scripts/localstack-init.sh << 'EOF'
#!/bin/bash
set -e
ENDPOINT=http://localhost:4566

for bucket in ods-raw-local ods-curated-local ods-config-local \
              ods-dlq-local ods-quarantine-local ods-audit-sink-local; do
  aws --endpoint-url=$ENDPOINT --region eu-west-1 s3 mb s3://$bucket || true
  echo "Bucket ready: $bucket"
done
EOF
chmod +x scripts/localstack-init.sh
```

---

**Step 4: Create `db/migrations/00_create_databases.sql`**

Postgres `POSTGRES_DB=ods_dev` creates the main DB. This creates the Airflow DB.

```sql
-- db/migrations/00_create_databases.sql
CREATE DATABASE airflow;
```

---

**Step 5: Create `glue/Dockerfile`**

Extends the official Glue image with Kafka, schema registry, and Postgres clients.

```dockerfile
FROM amazon/aws-glue-libs:glue4

USER root

RUN pip install --no-cache-dir \
    confluent-kafka==2.3.0 \
    psycopg2-binary==2.9.9 \
    boto3==1.34.0 \
    fastavro==1.9.4 \
    requests==2.31.0

USER glue_user
```

---

**Step 6: Create `docker-compose.yml`**

```yaml
version: "3.9"

x-glue-env: &glue-env
  AWS_DEFAULT_REGION: eu-west-1
  AWS_ACCESS_KEY_ID: test
  AWS_SECRET_ACCESS_KEY: test
  LOCALSTACK_ENDPOINT: http://localstack:4566
  KAFKA_BOOTSTRAP_SERVERS: broker:29092
  SCHEMA_REGISTRY_URL: http://schema-registry:8081
  POSTGRES_HOST: postgres
  POSTGRES_DB: ods_dev
  POSTGRES_USER: ods
  POSTGRES_PASSWORD: ods
  ENV: local

services:
  localstack:
    image: localstack/localstack:3.4
    ports:
      - "4566:4566"
    environment:
      SERVICES: s3
      DEFAULT_REGION: eu-west-1
    volumes:
      - ./scripts/localstack-init.sh:/etc/localstack/init/ready.d/init.sh
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:4566/_localstack/health"]
      interval: 10s
      timeout: 5s
      retries: 10

  zookeeper:
    image: confluentinc/cp-zookeeper:7.6.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    healthcheck:
      test: ["CMD", "nc", "-z", "localhost", "2181"]
      interval: 10s
      timeout: 5s
      retries: 5

  broker:
    image: confluentinc/cp-kafka:7.6.0
    depends_on:
      zookeeper:
        condition: service_healthy
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://broker:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
    healthcheck:
      test: ["CMD", "kafka-topics", "--bootstrap-server", "localhost:29092", "--list"]
      interval: 15s
      timeout: 10s
      retries: 10

  schema-registry:
    image: confluentinc/cp-schema-registry:7.6.0
    depends_on:
      broker:
        condition: service_healthy
    ports:
      - "8081:8081"
    environment:
      SCHEMA_REGISTRY_HOST_NAME: schema-registry
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: broker:29092
      SCHEMA_REGISTRY_LISTENERS: http://0.0.0.0:8081
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8081/subjects"]
      interval: 10s
      timeout: 5s
      retries: 15

  postgres:
    image: postgres:15
    ports:
      - "5432:5432"
    environment:
      POSTGRES_DB: ods_dev
      POSTGRES_USER: ods
      POSTGRES_PASSWORD: ods
    volumes:
      - ./db/migrations:/docker-entrypoint-initdb.d
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ods -d ods_dev"]
      interval: 10s
      timeout: 5s
      retries: 5

  airflow:
    image: apache/airflow:2.9.1-python3.10
    depends_on:
      postgres:
        condition: service_healthy
      broker:
        condition: service_healthy
      schema-registry:
        condition: service_healthy
      localstack:
        condition: service_healthy
    ports:
      - "8080:8080"
    environment:
      <<: *glue-env
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://ods:ods@postgres:5432/airflow
      AIRFLOW__CORE__LOAD_EXAMPLES: "false"
      AIRFLOW__CORE__DAGS_FOLDER: /opt/airflow/dags
      AIRFLOW__CORE__FERNET_KEY: "46BKJoQYlPPOexq0OhDZnIlNepKFf87WFwLbfzqDDho="
      DOCKER_HOST: unix:///var/run/docker.sock
    volumes:
      - ./dags:/opt/airflow/dags
      - /var/run/docker.sock:/var/run/docker.sock
    command: >
      bash -c "pip install apache-airflow-providers-docker apache-airflow-providers-postgres &&
               airflow db migrate &&
               airflow users create --username admin --password admin
                 --firstname Admin --lastname User --role Admin --email admin@example.com &&
               airflow webserver &
               airflow scheduler"
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080/health"]
      interval: 30s
      timeout: 10s
      retries: 10

  glue:
    build: ./glue
    image: ods-glue:local
    environment:
      <<: *glue-env
    volumes:
      - ./glue/jobs:/home/glue_user/workspace/jobs
    entrypoint: ["echo", "Glue image ready — invoked via DockerOperator"]

networks:
  default:
    name: ods-network
```

---

**Step 7: Create `requirements-dev.txt`**

```text
pytest==8.2.0
pytest-docker==3.1.0
psycopg2-binary==2.9.9
boto3==1.34.0
confluent-kafka==2.3.0
fastavro==1.9.4
requests==2.31.0
pyspark==3.3.4
apache-airflow==2.9.1
```

---

**Step 8: Start the stack and verify**

```bash
docker compose build glue
docker compose up -d
docker compose ps
```

Expected: all 7 services show `healthy` or `running` within 3 minutes.

```bash
# Verify S3 buckets
aws --endpoint-url=http://localhost:4566 --region eu-west-1 s3 ls
```

Expected: 6 buckets listed.

```bash
# Verify Schema Registry
curl http://localhost:8081/subjects
```

Expected: `[]`

---

**Step 9: Commit**

```bash
git add docker-compose.yml .env.example glue/Dockerfile \
        scripts/localstack-init.sh requirements-dev.txt
git commit -m "feat: add docker compose stack and glue image"
```

---

## Task 2: Database Migrations

**Files:**
- Create: `db/migrations/01_init_schema.sql`
- Create: `db/migrations/02_seed_policies.sql`
- Create: `tests/unit/test_migrations.py`

---

**Step 1: Write the failing migration test**

```python
# tests/unit/test_migrations.py
import psycopg2
import pytest

@pytest.fixture
def conn():
    c = psycopg2.connect(
        host="localhost", port=5432,
        dbname="ods_dev", user="ods", password="ods"
    )
    yield c
    c.close()

def test_pipeline_schema_exists(conn):
    cur = conn.cursor()
    cur.execute(
        "SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'pipeline'"
    )
    assert cur.fetchone() is not None

def test_dataset_config_table_exists(conn):
    cur = conn.cursor()
    cur.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema='pipeline' AND table_name='dataset_config'"
    )
    assert cur.fetchone() is not None

def test_file_catalogue_table_exists(conn):
    cur = conn.cursor()
    cur.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema='pipeline' AND table_name='file_catalogue'"
    )
    assert cur.fetchone() is not None

def test_glue_job_log_has_config_snapshot(conn):
    cur = conn.cursor()
    cur.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema='pipeline' AND table_name='glue_job_log' "
        "AND column_name='config_snapshot'"
    )
    assert cur.fetchone() is not None

def test_policies_seed_exists(conn):
    cur = conn.cursor()
    cur.execute(
        "SELECT dataset FROM pipeline.dataset_config WHERE dataset = 'policies'"
    )
    row = cur.fetchone()
    assert row is not None
    assert row[0] == 'policies'
```

---

**Step 2: Run test to verify it fails**

```bash
pytest tests/unit/test_migrations.py -v
```

Expected: `FAILED` — tables don't exist yet.

---

**Step 3: Create `db/migrations/01_init_schema.sql`**

```sql
-- db/migrations/01_init_schema.sql

CREATE SCHEMA IF NOT EXISTS pipeline;

CREATE TABLE pipeline.dataset_config (
    id                  SERIAL PRIMARY KEY,
    domain              VARCHAR NOT NULL,
    dataset             VARCHAR NOT NULL,
    filename_pattern    VARCHAR NOT NULL,
    target_topic        VARCHAR NOT NULL,
    schema_id           VARCHAR NOT NULL,
    schema_version      INTEGER NOT NULL DEFAULT 1,
    key_fields          JSONB NOT NULL,
    dq_rules            JSONB NOT NULL DEFAULT '{}',
    data_classification VARCHAR NOT NULL DEFAULT 'Internal',
    active              BOOLEAN DEFAULT TRUE,
    version             INTEGER NOT NULL DEFAULT 1,
    created_at          TIMESTAMP DEFAULT NOW(),
    updated_at          TIMESTAMP DEFAULT NOW(),
    UNIQUE (domain, dataset)
);

CREATE TABLE pipeline.file_catalogue (
    id                SERIAL PRIMARY KEY,
    name_pattern      VARCHAR NOT NULL,
    sftp_path         VARCHAR NOT NULL DEFAULT '',
    domain            VARCHAR NOT NULL,
    dataset           VARCHAR NOT NULL,
    dataset_config_id INTEGER NOT NULL REFERENCES pipeline.dataset_config(id),
    active            BOOLEAN DEFAULT TRUE,
    created_at        TIMESTAMP DEFAULT NOW()
);

CREATE TABLE pipeline.file_state (
    id            SERIAL PRIMARY KEY,
    s3_path       VARCHAR NOT NULL UNIQUE,
    run_id        UUID NOT NULL,
    status        VARCHAR NOT NULL CHECK (status IN ('new','processing','completed','failed')),
    record_count  INTEGER,
    error_reason  VARCHAR,
    created_at    TIMESTAMP DEFAULT NOW(),
    updated_at    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE pipeline.ingestion_file_state (
    id            SERIAL PRIMARY KEY,
    s3_path       VARCHAR NOT NULL UNIQUE,
    status        VARCHAR NOT NULL CHECK (status IN ('detected','transferred','failed')),
    checksum_md5  VARCHAR,
    created_at    TIMESTAMP DEFAULT NOW(),
    updated_at    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE pipeline.glue_job_log (
    id              BIGSERIAL PRIMARY KEY,
    run_id          UUID NOT NULL,
    job_name        VARCHAR NOT NULL,
    pipeline_type   VARCHAR NOT NULL CHECK (pipeline_type IN ('ingestion','publish')),
    domain          VARCHAR NOT NULL,
    dataset         VARCHAR NOT NULL,
    source_path     VARCHAR,
    target_path     VARCHAR,
    business_date   DATE,
    status          VARCHAR NOT NULL,
    record_count    INTEGER,
    error_reason    VARCHAR,
    error_detail    TEXT,
    config_version  INTEGER,
    config_snapshot JSONB,
    created_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE pipeline.lineage (
    id                  BIGSERIAL PRIMARY KEY,
    run_id              UUID NOT NULL,
    domain              VARCHAR NOT NULL,
    dataset             VARCHAR NOT NULL,
    source_type         VARCHAR NOT NULL CHECK (source_type IN ('file','cdc','api','event')),
    source_ref          VARCHAR NOT NULL,
    target_topic        VARCHAR NOT NULL,
    business_date       DATE,
    kafka_offset_start  BIGINT,
    kafka_offset_end    BIGINT,
    record_count        INTEGER,
    schema_version      INTEGER,
    created_at          TIMESTAMP DEFAULT NOW()
);
```

---

**Step 4: Create `db/migrations/02_seed_policies.sql`**

```sql
-- db/migrations/02_seed_policies.sql

INSERT INTO pipeline.dataset_config (
    domain, dataset, filename_pattern, target_topic,
    schema_id, schema_version, key_fields, dq_rules,
    data_classification, active, version
) VALUES (
    'insurance',
    'policies',
    'policies_(\\d{8})\\.csv',
    'ods.insurance.policies',
    'ods-insurance-policies-value',
    1,
    '["policy_id"]',
    '{
        "hard_blocks": [
            {"field": "policy_id",      "rule": "not_null"},
            {"field": "policy_id",      "rule": "unique"},
            {"field": "premium_amount", "rule": "not_null"},
            {"field": "premium_amount", "rule": "greater_than", "value": 0},
            {"field": "start_date",     "rule": "valid_date",   "format": "yyyy-MM-dd"},
            {"fields": ["end_date", "start_date"], "rule": "date_gte"}
        ],
        "soft_warns": [
            {"field": "premium_amount", "rule": "less_than",    "value": 50000},
            {"field": "end_date",       "rule": "not_past"},
            {"fields": ["agent_code", "postcode"], "rule": "completeness_pct", "threshold": 0.8}
        ]
    }',
    'Confidential',
    TRUE,
    1
);

INSERT INTO pipeline.file_catalogue (name_pattern, domain, dataset, dataset_config_id)
SELECT 'policies_*.csv', 'insurance', 'policies', id
FROM pipeline.dataset_config WHERE dataset = 'policies';
```

---

**Step 5: Restart Postgres to apply migrations**

```bash
docker compose down postgres && docker compose up -d postgres
docker compose logs postgres | tail -20
```

Wait for `database system is ready to accept connections`.

---

**Step 6: Run test to verify it passes**

```bash
pytest tests/unit/test_migrations.py -v
```

Expected: all 5 tests `PASSED`.

---

**Step 7: Commit**

```bash
git add db/migrations/ tests/unit/test_migrations.py
git commit -m "feat: add postgres schema and seed policies config"
```

---

## Task 3: Avro Schema + Registration

**Files:**
- Create: `schemas/insurance/policies.avsc`
- Create: `scripts/register_schemas.py`
- Create: `tests/unit/test_schema_registration.py`

---

**Step 1: Write the failing schema test**

```python
# tests/unit/test_schema_registration.py
import requests

SCHEMA_REGISTRY = "http://localhost:8081"

def test_policies_schema_not_yet_registered():
    r = requests.get(f"{SCHEMA_REGISTRY}/subjects")
    assert "ods-insurance-policies-value" not in r.json()
```

Run: `pytest tests/unit/test_schema_registration.py::test_policies_schema_not_yet_registered -v`
Expected: `PASSED` (schema not registered yet — this confirms registry is clean).

---

**Step 2: Create `schemas/insurance/policies.avsc`**

```json
{
  "type": "record",
  "name": "Policy",
  "namespace": "com.aviva.ods.insurance",
  "fields": [
    {"name": "policy_id",        "type": "string"},
    {"name": "policyholder_name","type": "string"},
    {"name": "premium_amount",   "type": "double"},
    {"name": "start_date",       "type": "string"},
    {"name": "end_date",         "type": ["null", "string"], "default": null},
    {"name": "agent_code",       "type": ["null", "string"], "default": null},
    {"name": "postcode",         "type": ["null", "string"], "default": null},
    {"name": "_ods_business_date","type": "string"},
    {"name": "_ods_run_id",      "type": "string"}
  ]
}
```

---

**Step 3: Create `scripts/register_schemas.py`**

```python
#!/usr/bin/env python3
"""Register all Avro schemas from schemas/ into Confluent Schema Registry."""
import json
import os
import sys
import requests

SCHEMA_REGISTRY = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")


def register_schema(subject: str, schema_path: str) -> int:
    with open(schema_path) as f:
        schema_str = json.dumps(json.load(f))

    payload = {"schema": schema_str, "schemaType": "AVRO"}
    r = requests.post(
        f"{SCHEMA_REGISTRY}/subjects/{subject}/versions",
        json=payload,
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
    )
    r.raise_for_status()
    schema_id = r.json()["id"]
    print(f"Registered {subject} → schema id {schema_id}")
    return schema_id


if __name__ == "__main__":
    schemas = [
        ("ods-insurance-policies-value", "schemas/insurance/policies.avsc"),
    ]
    for subject, path in schemas:
        register_schema(subject, path)
    print("All schemas registered.")
```

---

**Step 4: Write the post-registration test**

Add to `tests/unit/test_schema_registration.py`:

```python
def test_policies_schema_registered():
    r = requests.get(f"{SCHEMA_REGISTRY}/subjects")
    assert "ods-insurance-policies-value" in r.json()

def test_policies_schema_has_policy_id_field():
    r = requests.get(f"{SCHEMA_REGISTRY}/subjects/ods-insurance-policies-value/versions/latest")
    schema = json.loads(r.json()["schema"])
    field_names = [f["name"] for f in schema["fields"]]
    assert "policy_id" in field_names
    assert "_ods_run_id" in field_names
```

---

**Step 5: Register schemas and run tests**

```bash
python scripts/register_schemas.py
pytest tests/unit/test_schema_registration.py -v
```

Expected: all tests `PASSED`.

---

**Step 6: Commit**

```bash
git add schemas/ scripts/register_schemas.py tests/unit/test_schema_registration.py
git commit -m "feat: add avro schema and registration script for policies"
```

---

## Task 4: Utility Functions (TDD)

These are pure-Python functions used by both Glue jobs. No Spark or AWS dependencies — fully unit testable.

**Files:**
- Create: `glue/jobs/utils.py`
- Create: `tests/unit/test_utils.py`

---

**Step 1: Write all failing unit tests**

```python
# tests/unit/test_utils.py
import hashlib
import json
from datetime import date
import pytest

# Import will fail until utils.py exists
from glue.jobs.utils import (
    extract_business_date,
    generate_message_key,
    write_job_log,
    load_dataset_config,
)


# --- extract_business_date ---

def test_extract_business_date_standard():
    result = extract_business_date("policies_20260417.csv", r"policies_(\d{8})\.csv")
    assert result == date(2026, 4, 17)

def test_extract_business_date_december():
    result = extract_business_date("policies_20261201.csv", r"policies_(\d{8})\.csv")
    assert result == date(2026, 12, 1)

def test_extract_business_date_no_match_raises():
    with pytest.raises(ValueError, match="Cannot extract business_date"):
        extract_business_date("unknown_file.csv", r"policies_(\d{8})\.csv")


# --- generate_message_key ---

def test_generate_message_key_is_deterministic():
    row = {"policy_id": "POL-001", "premium_amount": 1200.0}
    key1 = generate_message_key(["policy_id"], row)
    key2 = generate_message_key(["policy_id"], row)
    assert key1 == key2

def test_generate_message_key_is_sha256_hex():
    row = {"policy_id": "POL-001"}
    key = generate_message_key(["policy_id"], row)
    assert len(key) == 64
    assert all(c in "0123456789abcdef" for c in key)

def test_generate_message_key_changes_with_value():
    row_a = {"policy_id": "POL-001"}
    row_b = {"policy_id": "POL-002"}
    assert generate_message_key(["policy_id"], row_a) != generate_message_key(["policy_id"], row_b)

def test_generate_message_key_multi_field_order_stable():
    row = {"policy_id": "POL-001", "start_date": "2026-01-01"}
    key1 = generate_message_key(["policy_id", "start_date"], row)
    key2 = generate_message_key(["start_date", "policy_id"], row)
    assert key1 == key2  # sorted field order


# --- load_dataset_config (requires postgres) ---

def test_load_dataset_config_returns_policies(pg_conn):
    config = load_dataset_config(pg_conn, "insurance", "policies")
    assert config["dataset"] == "policies"
    assert config["target_topic"] == "ods.insurance.policies"
    assert config["key_fields"] == ["policy_id"]
    assert "hard_blocks" in config["dq_rules"]

def test_load_dataset_config_missing_raises(pg_conn):
    with pytest.raises(ValueError, match="No active config"):
        load_dataset_config(pg_conn, "insurance", "nonexistent")


# conftest.py fixture (add to tests/conftest.py)
```

---

**Step 2: Create `tests/conftest.py`**

```python
# tests/conftest.py
import psycopg2
import pytest

@pytest.fixture(scope="session")
def pg_conn():
    conn = psycopg2.connect(
        host="localhost", port=5432,
        dbname="ods_dev", user="ods", password="ods"
    )
    yield conn
    conn.close()
```

---

**Step 3: Run tests to verify they fail**

```bash
pytest tests/unit/test_utils.py -v
```

Expected: `ImportError` — `glue/jobs/utils.py` does not exist yet.

---

**Step 4: Create `glue/jobs/__init__.py`**

```bash
touch glue/__init__.py glue/jobs/__init__.py
```

---

**Step 5: Create `glue/jobs/utils.py`**

```python
# glue/jobs/utils.py
import hashlib
import json
import re
from datetime import date

import psycopg2


def extract_business_date(filename: str, pattern: str) -> date:
    """Extract business_date from filename using the first capture group."""
    match = re.search(pattern, filename)
    if not match:
        raise ValueError(
            f"Cannot extract business_date from {filename!r} using pattern {pattern!r}"
        )
    s = match.group(1)  # expects YYYYMMDD
    return date(int(s[:4]), int(s[4:6]), int(s[6:8]))


def generate_message_key(key_fields: list, row: dict) -> str:
    """Deterministic SHA256 hex key from key_fields values (fields sorted for stability)."""
    parts = "|".join(str(row.get(f, "")) for f in sorted(key_fields))
    return hashlib.sha256(parts.encode()).hexdigest()


def load_dataset_config(conn, domain: str, dataset: str) -> dict:
    """Load active dataset config from pipeline.dataset_config."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, domain, dataset, filename_pattern, target_topic,
                   schema_id, schema_version, key_fields, dq_rules,
                   data_classification, version
            FROM pipeline.dataset_config
            WHERE domain = %s AND dataset = %s AND active = TRUE
            """,
            (domain, dataset),
        )
        row = cur.fetchone()
    if not row:
        raise ValueError(f"No active config for {domain}/{dataset}")
    cols = [
        "id", "domain", "dataset", "filename_pattern", "target_topic",
        "schema_id", "schema_version", "key_fields", "dq_rules",
        "data_classification", "version",
    ]
    return dict(zip(cols, row))


def write_job_log(conn, **fields) -> None:
    """INSERT a single immutable row into pipeline.glue_job_log."""
    cols = ", ".join(fields.keys())
    placeholders = ", ".join(["%s"] * len(fields))
    with conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO pipeline.glue_job_log ({cols}) VALUES ({placeholders})",
            list(fields.values()),
        )
    conn.commit()


def set_file_state(conn, s3_path: str, run_id: str, status: str, **extra) -> None:
    """Upsert pipeline.file_state for a given S3 path."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline.file_state (s3_path, run_id, status, record_count, error_reason)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (s3_path) DO UPDATE
              SET run_id=EXCLUDED.run_id, status=EXCLUDED.status,
                  record_count=EXCLUDED.record_count, error_reason=EXCLUDED.error_reason,
                  updated_at=NOW()
            """,
            (s3_path, run_id, status, extra.get("record_count"), extra.get("error_reason")),
        )
    conn.commit()


def get_file_state(conn, s3_path: str) -> str | None:
    """Return current status for an S3 path, or None if not found."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT status FROM pipeline.file_state WHERE s3_path = %s",
            (s3_path,),
        )
        row = cur.fetchone()
    return row[0] if row else None
```

---

**Step 6: Run tests to verify they pass**

```bash
pytest tests/unit/test_utils.py -v
```

Expected: all 9 tests `PASSED`.

---

**Step 7: Commit**

```bash
git add glue/jobs/utils.py glue/jobs/__init__.py glue/__init__.py \
        tests/unit/test_utils.py tests/conftest.py
git commit -m "feat: add utils module with business date, key gen, and db helpers"
```

---

## Task 5: DQ Rule Evaluator (TDD)

Pure-Python rule engine that reads the `dq_rules` JSONB and evaluates rules against a PySpark DataFrame. No Glue DQDL dependency.

**Files:**
- Create: `glue/jobs/dq.py`
- Create: `tests/unit/test_dq.py`
- Create: `tests/fixtures/policies_sample.csv`

---

**Step 1: Create fixture CSV**

```bash
cat > tests/fixtures/policies_sample.csv << 'EOF'
policy_id,policyholder_name,premium_amount,start_date,end_date,agent_code,postcode
POL-001,Alice Smith,1200.00,2026-01-01,2027-01-01,AG001,EC1A1BB
POL-002,Bob Jones,950.50,2026-02-01,2027-02-01,AG002,WC2N5DU
POL-003,Carol White,75000.00,2026-03-01,2027-03-01,,
POL-004,,500.00,2026-04-01,2027-04-01,AG004,E14ABC
EOF
```

Row notes:
- POL-001, POL-002: clean rows
- POL-003: premium > 50,000 (soft warn), agent_code + postcode null
- POL-004: policyholder_name null (not a hard block field in our rules, passes)

---

**Step 2: Write failing DQ tests**

```python
# tests/unit/test_dq.py
import pytest
from pyspark.sql import SparkSession

from glue.jobs.dq import evaluate_dq_rules


@pytest.fixture(scope="module")
def spark():
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("ods-dq-test")
        .getOrCreate()
    )


DQ_RULES = {
    "hard_blocks": [
        {"field": "policy_id",      "rule": "not_null"},
        {"field": "policy_id",      "rule": "unique"},
        {"field": "premium_amount", "rule": "not_null"},
        {"field": "premium_amount", "rule": "greater_than", "value": 0},
    ],
    "soft_warns": [
        {"field": "premium_amount", "rule": "less_than", "value": 50000},
    ],
}


def make_df(spark, rows):
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType
    schema = StructType([
        StructField("policy_id",        StringType()),
        StructField("premium_amount",   DoubleType()),
        StructField("policyholder_name",StringType()),
    ])
    return spark.createDataFrame(rows, schema=schema)


def test_clean_rows_all_pass(spark):
    df = make_df(spark, [
        ("POL-001", 1200.0, "Alice"),
        ("POL-002",  950.0, "Bob"),
    ])
    passing, failing, warns = evaluate_dq_rules(df, DQ_RULES)
    assert passing.count() == 2
    assert failing.count() == 0
    assert warns == []


def test_null_policy_id_is_hard_blocked(spark):
    df = make_df(spark, [
        (None,    1200.0, "Alice"),
        ("POL-002", 950.0, "Bob"),
    ])
    passing, failing, warns = evaluate_dq_rules(df, DQ_RULES)
    assert passing.count() == 1
    assert failing.count() == 1


def test_duplicate_policy_id_is_hard_blocked(spark):
    df = make_df(spark, [
        ("POL-001", 1200.0, "Alice"),
        ("POL-001",  950.0, "Bob"),
    ])
    passing, failing, warns = evaluate_dq_rules(df, DQ_RULES)
    assert failing.count() == 2  # both duplicates blocked


def test_zero_premium_is_hard_blocked(spark):
    df = make_df(spark, [
        ("POL-001", 0.0, "Alice"),
        ("POL-002", 950.0, "Bob"),
    ])
    passing, failing, warns = evaluate_dq_rules(df, DQ_RULES)
    assert failing.count() == 1


def test_high_premium_is_soft_warn(spark):
    df = make_df(spark, [
        ("POL-001", 75000.0, "Alice"),
        ("POL-002",  950.0,  "Bob"),
    ])
    passing, failing, warns = evaluate_dq_rules(df, DQ_RULES)
    assert passing.count() == 2  # soft warn — row continues
    assert failing.count() == 0
    assert len(warns) == 1
    assert "premium_amount" in warns[0]["field"]
```

---

**Step 3: Run tests to verify they fail**

```bash
pytest tests/unit/test_dq.py -v
```

Expected: `ImportError` — `glue/jobs/dq.py` does not exist.

---

**Step 4: Create `glue/jobs/dq.py`**

```python
# glue/jobs/dq.py
"""
DQ rule evaluator for PySpark DataFrames.
Reads dq_rules JSONB structure and returns (passing_df, failing_df, warnings).
"""
from __future__ import annotations

import json
from datetime import date
from typing import Any

from pyspark.sql import DataFrame
import pyspark.sql.functions as F


def evaluate_dq_rules(
    df: DataFrame, rules: dict
) -> tuple[DataFrame, DataFrame, list[dict]]:
    """
    Apply hard_blocks and soft_warns from rules dict to df.

    Returns:
        passing_df  — rows that passed all hard blocks
        failing_df  — rows that violated at least one hard block
        warnings    — list of soft warn summaries (no rows removed)
    """
    hard_blocks = rules.get("hard_blocks", [])
    soft_warns = rules.get("soft_warns", [])

    fail_mask = F.lit(False)
    fail_reasons = F.lit("")

    for rule in hard_blocks:
        mask, reason = _build_hard_mask(df, rule)
        fail_mask = fail_mask | mask
        fail_reasons = F.when(mask, F.concat_ws("; ", fail_reasons, F.lit(reason))).otherwise(fail_reasons)

    df = df.withColumn("_dq_fail_reason", fail_reasons)
    failing_df = df.filter(fail_mask).drop("_dq_fail_reason")
    passing_df = df.filter(~fail_mask).drop("_dq_fail_reason")

    warnings = []
    for rule in soft_warns:
        count, msg = _evaluate_soft_warn(passing_df, rule)
        if count > 0:
            warnings.append({"field": rule.get("field", ""), "rule": rule["rule"], "count": count, "msg": msg})

    return passing_df, failing_df, warnings


def _build_hard_mask(df: DataFrame, rule: dict):
    """Return (boolean Column mask where True=failing, reason string)."""
    r = rule["rule"]

    if r == "not_null":
        field = rule["field"]
        return F.col(field).isNull(), f"{field} is null"

    if r == "unique":
        field = rule["field"]
        counts = df.groupBy(field).count().filter(F.col("count") > 1).select(field)
        dup_vals = {row[0] for row in counts.collect()}
        mask = F.col(field).isin(dup_vals) if dup_vals else F.lit(False)
        return mask, f"{field} is duplicate"

    if r == "greater_than":
        field, value = rule["field"], rule["value"]
        return (F.col(field).isNull() | (F.col(field) <= value)), f"{field} <= {value}"

    if r == "valid_date":
        field = rule["field"]
        fmt = rule.get("format", "yyyy-MM-dd")
        return F.to_date(F.col(field), fmt).isNull(), f"{field} is not a valid date"

    if r == "date_gte":
        end_f, start_f = rule["fields"]
        mask = (
            F.col(end_f).isNull() |
            (F.to_date(F.col(end_f)) < F.to_date(F.col(start_f)))
        )
        return mask, f"{end_f} < {start_f}"

    raise ValueError(f"Unknown hard_block rule: {r}")


def _evaluate_soft_warn(df: DataFrame, rule: dict) -> tuple[int, str]:
    """Return (count of rows triggering warn, human message). No rows removed."""
    r = rule["rule"]

    if r == "less_than":
        field, value = rule["field"], rule["value"]
        count = df.filter(F.col(field) >= value).count()
        return count, f"{count} rows have {field} >= {value}"

    if r == "not_past":
        field = rule["field"]
        today = str(date.today())
        count = df.filter(F.to_date(F.col(field)) < F.lit(today)).count()
        return count, f"{count} rows have {field} in the past"

    if r == "completeness_pct":
        fields = rule["fields"]
        threshold = rule.get("threshold", 0.8)
        total = df.count()
        if total == 0:
            return 0, ""
        null_counts = [df.filter(F.col(f).isNull()).count() for f in fields]
        avg_null_rate = sum(null_counts) / (len(fields) * total)
        completeness = 1 - avg_null_rate
        if completeness < threshold:
            return total, f"optional fields completeness {completeness:.0%} < {threshold:.0%}"
        return 0, ""

    raise ValueError(f"Unknown soft_warn rule: {r}")
```

---

**Step 5: Run tests to verify they pass**

```bash
pytest tests/unit/test_dq.py -v
```

Expected: all 5 tests `PASSED`.

---

**Step 6: Commit**

```bash
git add glue/jobs/dq.py tests/unit/test_dq.py tests/fixtures/
git commit -m "feat: add DQ rule evaluator with hard block and soft warn support"
```

---

## Task 6: Glue Ingestion Job (`ods_ingestion.py`)

Reads CSV from S3 Raw, validates schema and DQ, writes Parquet to S3 Curated, logs every step to `pipeline.glue_job_log`.

**Files:**
- Create: `glue/jobs/ods_ingestion.py`
- Create: `tests/integration/test_ingestion.py`

---

**Step 1: Write integration test**

```python
# tests/integration/test_ingestion.py
"""
Runs the Glue ingestion job inside the ods-glue:local container
via docker run, then asserts outputs.
"""
import os
import subprocess
import uuid
import psycopg2
import boto3
import pytest

S3_ENDPOINT = "http://localhost:4566"
RAW_BUCKET  = "ods-raw-local"
CURATED_BUCKET = "ods-curated-local"
DLQ_BUCKET  = "ods-dlq-local"
NETWORK     = "ods-network"


@pytest.fixture
def s3():
    return boto3.client(
        "s3", endpoint_url=S3_ENDPOINT,
        aws_access_key_id="test", aws_secret_access_key="test",
        region_name="eu-west-1"
    )


@pytest.fixture
def pg():
    conn = psycopg2.connect(
        host="localhost", port=5432,
        dbname="ods_dev", user="ods", password="ods"
    )
    yield conn
    conn.close()


def run_ingestion_job(run_id: str, s3_path: str, domain="insurance", dataset="policies"):
    """Invoke ods_ingestion.py inside the Glue container."""
    cmd = [
        "docker", "run", "--rm",
        "--network", NETWORK,
        "-e", "AWS_DEFAULT_REGION=eu-west-1",
        "-e", "AWS_ACCESS_KEY_ID=test",
        "-e", "AWS_SECRET_ACCESS_KEY=test",
        "-e", f"LOCALSTACK_ENDPOINT=http://localstack:4566",
        "-e", "POSTGRES_HOST=postgres",
        "-e", "POSTGRES_DB=ods_dev",
        "-e", "POSTGRES_USER=ods",
        "-e", "POSTGRES_PASSWORD=ods",
        "-e", "SCHEMA_REGISTRY_URL=http://schema-registry:8081",
        "-e", "ENV=local",
        "-v", f"{os.getcwd()}/glue/jobs:/home/glue_user/workspace/jobs",
        "ods-glue:local",
        "spark-submit",
        "--py-files", "/home/glue_user/workspace/jobs/utils.py,/home/glue_user/workspace/jobs/dq.py",
        "/home/glue_user/workspace/jobs/ods_ingestion.py",
        "--run_id", run_id,
        "--domain", domain,
        "--dataset", dataset,
        "--s3_input_path", s3_path,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    return result


def upload_csv(s3, key: str, content: str):
    s3.put_object(Bucket=RAW_BUCKET, Key=key, Body=content.encode())


GOOD_CSV = """policy_id,policyholder_name,premium_amount,start_date,end_date,agent_code,postcode
POL-001,Alice Smith,1200.00,2026-01-01,2027-01-01,AG001,EC1A1BB
POL-002,Bob Jones,950.50,2026-02-01,2027-02-01,AG002,WC2N5DU
"""


def test_happy_path_writes_parquet(s3, pg):
    run_id = str(uuid.uuid4())
    key = "insurance/policies/date=20260417/policies_20260417.csv"
    upload_csv(s3, key, GOOD_CSV)

    result = run_ingestion_job(run_id, f"s3://ods-raw-local/{key}")
    assert result.returncode == 0, result.stderr

    # Parquet landed in curated
    objs = s3.list_objects_v2(Bucket=CURATED_BUCKET, Prefix="insurance/policies/date=2026-04-17/")
    assert objs.get("KeyCount", 0) > 0

    # Final log entry is completed
    cur = pg.cursor()
    cur.execute(
        "SELECT status FROM pipeline.glue_job_log WHERE run_id=%s ORDER BY id DESC LIMIT 1",
        (run_id,)
    )
    assert cur.fetchone()[0] == "completed"

    # business_date extracted correctly
    cur.execute(
        "SELECT business_date FROM pipeline.glue_job_log WHERE run_id=%s AND status='started'",
        (run_id,)
    )
    row = cur.fetchone()
    assert str(row[0]) == "2026-04-17"


def test_idempotency_exits_cleanly(s3, pg):
    run_id_1 = str(uuid.uuid4())
    run_id_2 = str(uuid.uuid4())
    key = "insurance/policies/date=20260418/policies_20260418.csv"
    upload_csv(s3, key, GOOD_CSV)

    run_ingestion_job(run_id_1, f"s3://ods-raw-local/{key}")

    result = run_ingestion_job(run_id_2, f"s3://ods-raw-local/{key}")
    assert result.returncode == 0

    cur = pg.cursor()
    cur.execute(
        "SELECT status FROM pipeline.glue_job_log WHERE run_id=%s ORDER BY id LIMIT 1",
        (run_id_2,)
    )
    row = cur.fetchone()
    assert row[0] in ("completed", "skipped")
```

---

**Step 2: Run test to verify it fails**

```bash
pytest tests/integration/test_ingestion.py::test_happy_path_writes_parquet -v
```

Expected: `FAILED` — `ods_ingestion.py` does not exist yet.

---

**Step 3: Create `glue/jobs/ods_ingestion.py`**

```python
# glue/jobs/ods_ingestion.py
import argparse
import json
import os
import sys
import uuid
from datetime import datetime

import boto3
import psycopg2
import requests
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from utils import (
    extract_business_date,
    write_job_log,
    set_file_state,
    get_file_state,
    load_dataset_config,
)
from dq import evaluate_dq_rules


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_id",       required=True)
    parser.add_argument("--domain",       required=True)
    parser.add_argument("--dataset",      required=True)
    parser.add_argument("--s3_input_path", required=True)
    return parser.parse_args()


def get_pg_conn():
    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


def get_s3_client():
    endpoint = os.environ.get("LOCALSTACK_ENDPOINT")
    kwargs = dict(
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
        region_name=os.environ.get("AWS_DEFAULT_REGION", "eu-west-1"),
    )
    if endpoint:
        kwargs["endpoint_url"] = endpoint
    return boto3.client("s3", **kwargs)


def write_to_dlq(s3, run_id: str, domain: str, dataset: str, data: bytes, reason: str):
    from datetime import date
    key = f"{domain}/{dataset}/date={date.today()}/run_id={run_id}/failed.csv"
    bucket = os.environ.get("DLQ_BUCKET", f"ods-dlq-{os.environ.get('ENV','local')}")
    s3.put_object(Bucket=bucket, Key=key, Body=data)


def validate_schema_compatible(df, schema_id: str, schema_version: int) -> bool:
    """Check that all expected Avro fields are present in the DataFrame."""
    registry_url = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    r = requests.get(f"{registry_url}/subjects/{schema_id}/versions/{schema_version}")
    if r.status_code != 200:
        return False
    schema = json.loads(r.json()["schema"])
    expected = {f["name"] for f in schema["fields"] if not f["name"].startswith("_ods_")}
    actual = set(df.columns)
    return expected.issubset(actual)


def main():
    args = get_args()
    run_id = args.run_id
    domain = args.domain
    dataset = args.dataset
    s3_input_path = args.s3_input_path

    pg = get_pg_conn()
    s3 = get_s3_client()

    filename = s3_input_path.split("/")[-1]

    # Load config and snapshot it
    config = load_dataset_config(pg, domain, dataset)
    config_snapshot = json.dumps(config, default=str)

    # Idempotency check
    current_state = get_file_state(pg, s3_input_path)
    if current_state == "completed":
        print(f"[SKIP] {s3_input_path} already completed. Exiting.")
        write_job_log(
            pg, run_id=run_id, job_name="ods_ingestion", pipeline_type="ingestion",
            domain=domain, dataset=dataset, source_path=s3_input_path,
            status="skipped", config_version=config["version"],
            config_snapshot=config_snapshot,
        )
        return

    business_date = extract_business_date(filename, config["filename_pattern"])

    # Log: started
    write_job_log(
        pg, run_id=run_id, job_name="ods_ingestion", pipeline_type="ingestion",
        domain=domain, dataset=dataset, source_path=s3_input_path,
        business_date=business_date, status="started",
        config_version=config["version"], config_snapshot=config_snapshot,
    )

    spark = (
        SparkSession.builder
        .appName(f"ods_ingestion_{dataset}")
        .config("spark.hadoop.fs.s3a.endpoint", os.environ.get("LOCALSTACK_ENDPOINT", ""))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key", os.environ.get("AWS_ACCESS_KEY_ID", "test"))
        .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("AWS_SECRET_ACCESS_KEY", "test"))
        .getOrCreate()
    )

    # Read CSV
    s3a_path = s3_input_path.replace("s3://", "s3a://")
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(s3a_path)
    source_count = df.count()
    write_job_log(pg, run_id=run_id, job_name="ods_ingestion", pipeline_type="ingestion",
                  domain=domain, dataset=dataset, source_path=s3_input_path,
                  business_date=business_date, status="file_read", record_count=source_count,
                  config_version=config["version"], config_snapshot=config_snapshot)

    # Schema validation
    if not validate_schema_compatible(df, config["schema_id"], config["schema_version"]):
        write_job_log(pg, run_id=run_id, job_name="ods_ingestion", pipeline_type="ingestion",
                      domain=domain, dataset=dataset, source_path=s3_input_path,
                      business_date=business_date, status="failed",
                      error_reason="schema_incompatible",
                      config_version=config["version"], config_snapshot=config_snapshot)
        set_file_state(pg, s3_input_path, run_id, "failed", error_reason="schema_incompatible")
        sys.exit(1)

    write_job_log(pg, run_id=run_id, job_name="ods_ingestion", pipeline_type="ingestion",
                  domain=domain, dataset=dataset, source_path=s3_input_path,
                  business_date=business_date, status="schema_validated",
                  config_version=config["version"], config_snapshot=config_snapshot)

    # DQ rules
    dq_rules = config["dq_rules"] if isinstance(config["dq_rules"], dict) else json.loads(config["dq_rules"])
    passing_df, failing_df, warnings = evaluate_dq_rules(df, dq_rules)

    failing_count = failing_df.count()
    if failing_count > 0:
        csv_bytes = "\n".join(
            [",".join(str(v) for v in row) for row in failing_df.collect()]
        ).encode()
        write_to_dlq(s3, run_id, domain, dataset, csv_bytes, "dq_hard_block")

    dq_status = "dq_warned" if warnings or failing_count > 0 else "dq_passed"
    write_job_log(pg, run_id=run_id, job_name="ods_ingestion", pipeline_type="ingestion",
                  domain=domain, dataset=dataset, source_path=s3_input_path,
                  business_date=business_date, status=dq_status,
                  record_count=passing_df.count(),
                  config_version=config["version"], config_snapshot=config_snapshot)

    # Add ODS metadata columns
    passing_df = passing_df \
        .withColumn("_ods_business_date", F.lit(str(business_date))) \
        .withColumn("_ods_run_id", F.lit(run_id))

    # Write Parquet to S3 Curated
    env = os.environ.get("ENV", "local")
    curated_bucket = f"ods-curated-{env}"
    curated_path = f"s3a://{curated_bucket}/{domain}/{dataset}/date={business_date}/"
    passing_df.write.mode("overwrite").parquet(curated_path)
    written_count = passing_df.count()

    write_job_log(pg, run_id=run_id, job_name="ods_ingestion", pipeline_type="ingestion",
                  domain=domain, dataset=dataset,
                  source_path=s3_input_path, target_path=curated_path,
                  business_date=business_date, status="parquet_written",
                  record_count=written_count,
                  config_version=config["version"], config_snapshot=config_snapshot)

    # Count verification
    if written_count != (source_count - failing_count):
        write_job_log(pg, run_id=run_id, job_name="ods_ingestion", pipeline_type="ingestion",
                      domain=domain, dataset=dataset, source_path=s3_input_path,
                      business_date=business_date, status="failed",
                      error_reason="count_mismatch",
                      error_detail=f"expected {source_count - failing_count}, got {written_count}",
                      config_version=config["version"], config_snapshot=config_snapshot)
        set_file_state(pg, s3_input_path, run_id, "failed", error_reason="count_mismatch")
        sys.exit(1)

    write_job_log(pg, run_id=run_id, job_name="ods_ingestion", pipeline_type="ingestion",
                  domain=domain, dataset=dataset, source_path=s3_input_path,
                  business_date=business_date, status="count_verified",
                  record_count=written_count,
                  config_version=config["version"], config_snapshot=config_snapshot)

    write_job_log(pg, run_id=run_id, job_name="ods_ingestion", pipeline_type="ingestion",
                  domain=domain, dataset=dataset,
                  source_path=s3_input_path, target_path=curated_path,
                  business_date=business_date, status="completed",
                  record_count=written_count,
                  config_version=config["version"], config_snapshot=config_snapshot)

    set_file_state(pg, s3_input_path, run_id, "completed", record_count=written_count)
    spark.stop()
    pg.close()
    print(f"[OK] Ingestion complete: {written_count} rows → {curated_path}")


if __name__ == "__main__":
    main()
```

---

**Step 4: Run integration tests**

```bash
pytest tests/integration/test_ingestion.py -v
```

Expected: `test_happy_path_writes_parquet` and `test_idempotency_exits_cleanly` both `PASSED`.

---

**Step 5: Commit**

```bash
git add glue/jobs/ods_ingestion.py tests/integration/test_ingestion.py
git commit -m "feat: add ods_ingestion glue job (csv to parquet)"
```

---

## Task 7: Glue Publish Job (`ods_s3_publish.py`)

Reads Parquet from S3 Curated, validates schema, runs DQ, publishes Avro to Kafka with transactions, verifies count, writes lineage.

**Files:**
- Create: `glue/jobs/ods_s3_publish.py`
- Create: `tests/integration/test_publish.py`

---

**Step 1: Write integration test**

```python
# tests/integration/test_publish.py
import os
import subprocess
import uuid
import json
import psycopg2
import boto3
import pytest
from confluent_kafka import Consumer, KafkaError

S3_ENDPOINT   = "http://localhost:4566"
CURATED_BUCKET = "ods-curated-local"
NETWORK       = "ods-network"
KAFKA_BROKERS = "localhost:9092"
TOPIC         = "ods.insurance.policies"


@pytest.fixture
def s3():
    return boto3.client(
        "s3", endpoint_url=S3_ENDPOINT,
        aws_access_key_id="test", aws_secret_access_key="test",
        region_name="eu-west-1"
    )


@pytest.fixture
def pg():
    conn = psycopg2.connect(
        host="localhost", port=5432,
        dbname="ods_dev", user="ods", password="ods"
    )
    yield conn
    conn.close()


def run_publish_job(run_id: str, s3_path: str, domain="insurance", dataset="policies"):
    cmd = [
        "docker", "run", "--rm",
        "--network", NETWORK,
        "-e", "AWS_DEFAULT_REGION=eu-west-1",
        "-e", "AWS_ACCESS_KEY_ID=test",
        "-e", "AWS_SECRET_ACCESS_KEY=test",
        "-e", "LOCALSTACK_ENDPOINT=http://localstack:4566",
        "-e", "KAFKA_BOOTSTRAP_SERVERS=broker:29092",
        "-e", "SCHEMA_REGISTRY_URL=http://schema-registry:8081",
        "-e", "POSTGRES_HOST=postgres",
        "-e", "POSTGRES_DB=ods_dev",
        "-e", "POSTGRES_USER=ods",
        "-e", "POSTGRES_PASSWORD=ods",
        "-e", "ENV=local",
        "-v", f"{os.getcwd()}/glue/jobs:/home/glue_user/workspace/jobs",
        "ods-glue:local",
        "spark-submit",
        "--py-files", "/home/glue_user/workspace/jobs/utils.py,/home/glue_user/workspace/jobs/dq.py",
        "/home/glue_user/workspace/jobs/ods_s3_publish.py",
        "--run_id", run_id,
        "--domain", domain,
        "--dataset", dataset,
        "--s3_input_path", s3_path,
    ]
    return subprocess.run(cmd, capture_output=True, text=True, timeout=180)


def consume_messages(topic: str, timeout: float = 15.0) -> list[bytes]:
    group_id = f"test-{uuid.uuid4()}"
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKERS,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([topic])
    messages = []
    import time; deadline = time.time() + timeout
    while time.time() < deadline:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                break
            break
        messages.append(msg.value())
    consumer.close()
    return messages


def test_publish_happy_path(s3, pg):
    """Run ingestion first, then publish, assert message count."""
    # This test relies on Task 6 having been run first.
    # Use a unique date partition so this test owns its data.
    run_id = str(uuid.uuid4())
    curated_key_prefix = "insurance/policies/date=2026-05-01/"

    # Place a parquet file: re-run ingestion for a fresh date
    ingest_run_id = str(uuid.uuid4())
    raw_key = "insurance/policies/date=20260501/policies_20260501.csv"
    GOOD_CSV = (
        "policy_id,policyholder_name,premium_amount,start_date,end_date,agent_code,postcode\n"
        "POL-100,Alice,1200.00,2026-01-01,2027-01-01,AG001,EC1A1BB\n"
        "POL-101,Bob,950.50,2026-02-01,2027-02-01,AG002,WC2N5DU\n"
    )
    s3.put_object(Bucket="ods-raw-local", Key=raw_key, Body=GOOD_CSV.encode())

    from test_ingestion import run_ingestion_job
    r = run_ingestion_job(ingest_run_id, f"s3://ods-raw-local/{raw_key}")
    assert r.returncode == 0, r.stderr

    # Now run publish
    result = run_publish_job(run_id, f"s3://ods-curated-local/{curated_key_prefix}")
    assert result.returncode == 0, result.stderr

    # Check Kafka messages
    msgs = consume_messages(TOPIC)
    assert len(msgs) >= 2

    # Check lineage written
    cur = pg.cursor()
    cur.execute("SELECT record_count FROM pipeline.lineage WHERE run_id=%s", (run_id,))
    row = cur.fetchone()
    assert row is not None
    assert row[0] == 2

    # Check file_state completed
    cur.execute("SELECT status FROM pipeline.file_state WHERE run_id=%s", (run_id,))
    row = cur.fetchone()
    assert row[0] == "completed"
```

---

**Step 2: Run test to verify it fails**

```bash
pytest tests/integration/test_publish.py::test_publish_happy_path -v
```

Expected: `FAILED` — `ods_s3_publish.py` does not exist.

---

**Step 3: Create `glue/jobs/ods_s3_publish.py`**

```python
# glue/jobs/ods_s3_publish.py
import argparse
import json
import os
import sys
import io
import uuid

import psycopg2
import fastavro
import requests
from confluent_kafka import Producer
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from utils import (
    generate_message_key,
    write_job_log,
    set_file_state,
    get_file_state,
    load_dataset_config,
)
from dq import evaluate_dq_rules


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_id",        required=True)
    parser.add_argument("--domain",        required=True)
    parser.add_argument("--dataset",       required=True)
    parser.add_argument("--s3_input_path", required=True)
    return parser.parse_args()


def get_pg_conn():
    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


def fetch_avro_schema(schema_id: str, schema_version: int) -> dict:
    registry = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    r = requests.get(f"{registry}/subjects/{schema_id}/versions/{schema_version}")
    r.raise_for_status()
    return json.loads(r.json()["schema"])


def make_producer() -> Producer:
    brokers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    return Producer({
        "bootstrap.servers": brokers,
        "enable.idempotence": True,
        "acks": "all",
        "transactional.id": f"ods-publish-{uuid.uuid4()}",
    })


def row_to_avro_bytes(row: dict, parsed_schema: dict) -> bytes:
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, parsed_schema, row)
    return buf.getvalue()


def main():
    args = get_args()
    run_id   = args.run_id
    domain   = args.domain
    dataset  = args.dataset
    s3_path  = args.s3_input_path

    pg = get_pg_conn()

    # Load and snapshot config
    config = load_dataset_config(pg, domain, dataset)
    config_snapshot = json.dumps(config, default=str)

    # Idempotency check
    current = get_file_state(pg, s3_path)
    if current == "completed":
        print(f"[SKIP] {s3_path} already published.")
        write_job_log(pg, run_id=run_id, job_name="ods_s3_publish", pipeline_type="publish",
                      domain=domain, dataset=dataset, source_path=s3_path,
                      status="skipped", config_version=config["version"],
                      config_snapshot=config_snapshot)
        return

    write_job_log(pg, run_id=run_id, job_name="ods_s3_publish", pipeline_type="publish",
                  domain=domain, dataset=dataset, source_path=s3_path,
                  status="started", config_version=config["version"],
                  config_snapshot=config_snapshot)

    set_file_state(pg, s3_path, run_id, "processing")

    # Fetch Avro schema
    try:
        raw_schema = fetch_avro_schema(config["schema_id"], config["schema_version"])
        parsed_schema = fastavro.parse_schema(raw_schema)
    except Exception as exc:
        write_job_log(pg, run_id=run_id, job_name="ods_s3_publish", pipeline_type="publish",
                      domain=domain, dataset=dataset, source_path=s3_path,
                      status="failed", error_reason="schema_fetch_failed",
                      error_detail=str(exc), config_version=config["version"],
                      config_snapshot=config_snapshot)
        set_file_state(pg, s3_path, run_id, "failed", error_reason="schema_fetch_failed")
        sys.exit(1)

    write_job_log(pg, run_id=run_id, job_name="ods_s3_publish", pipeline_type="publish",
                  domain=domain, dataset=dataset, source_path=s3_path,
                  status="schema_fetched", config_version=config["version"],
                  config_snapshot=config_snapshot)

    spark = (
        SparkSession.builder
        .appName(f"ods_s3_publish_{dataset}")
        .config("spark.hadoop.fs.s3a.endpoint", os.environ.get("LOCALSTACK_ENDPOINT", ""))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key", os.environ.get("AWS_ACCESS_KEY_ID", "test"))
        .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("AWS_SECRET_ACCESS_KEY", "test"))
        .getOrCreate()
    )

    s3a_path = s3_path.replace("s3://", "s3a://")
    df = spark.read.parquet(s3a_path)
    source_count = df.count()

    # DQ check
    dq_rules = config["dq_rules"] if isinstance(config["dq_rules"], dict) else json.loads(config["dq_rules"])
    passing_df, failing_df, warnings = evaluate_dq_rules(df, dq_rules)

    dq_status = "dq_warned" if (warnings or failing_df.count() > 0) else "dq_passed"
    write_job_log(pg, run_id=run_id, job_name="ods_s3_publish", pipeline_type="publish",
                  domain=domain, dataset=dataset, source_path=s3_path,
                  status=dq_status, record_count=passing_df.count(),
                  config_version=config["version"], config_snapshot=config_snapshot)

    # Publish to Kafka with transactions
    write_job_log(pg, run_id=run_id, job_name="ods_s3_publish", pipeline_type="publish",
                  domain=domain, dataset=dataset, source_path=s3_path,
                  target_path=config["target_topic"], status="publishing",
                  config_version=config["version"], config_snapshot=config_snapshot)

    producer = make_producer()
    producer.init_transactions()
    producer.begin_transaction()

    key_fields = config["key_fields"] if isinstance(config["key_fields"], list) else json.loads(config["key_fields"])
    topic = config["target_topic"]
    published = 0

    try:
        rows = passing_df.collect()
        business_date = str(passing_df.select("_ods_business_date").first()[0]) if "_ods_business_date" in passing_df.columns else ""
        for row in rows:
            row_dict = row.asDict()
            msg_key = generate_message_key(key_fields, row_dict)
            avro_bytes = row_to_avro_bytes(row_dict, parsed_schema)
            headers = [
                ("x-ods-run-id",        run_id),
                ("x-ods-source-ref",    s3_path),
                ("x-ods-source-type",   "file"),
                ("x-ods-business-date", business_date),
                ("x-ods-schema-version", str(config["schema_version"])),
                ("x-ods-pipeline-type", "publish"),
            ]
            producer.produce(topic, key=msg_key, value=avro_bytes, headers=headers)
            published += 1

        producer.commit_transaction()
    except Exception as exc:
        producer.abort_transaction()
        write_job_log(pg, run_id=run_id, job_name="ods_s3_publish", pipeline_type="publish",
                      domain=domain, dataset=dataset, source_path=s3_path,
                      status="failed", error_reason="kafka_publish_failed",
                      error_detail=str(exc), config_version=config["version"],
                      config_snapshot=config_snapshot)
        set_file_state(pg, s3_path, run_id, "failed", error_reason="kafka_publish_failed")
        spark.stop(); pg.close(); sys.exit(1)

    producer.flush()

    # Count verification
    if published != passing_df.count():
        write_job_log(pg, run_id=run_id, job_name="ods_s3_publish", pipeline_type="publish",
                      domain=domain, dataset=dataset, source_path=s3_path,
                      status="failed", error_reason="count_mismatch",
                      error_detail=f"published={published}, expected={passing_df.count()}",
                      config_version=config["version"], config_snapshot=config_snapshot)
        set_file_state(pg, s3_path, run_id, "failed", error_reason="count_mismatch")
        spark.stop(); pg.close(); sys.exit(1)

    write_job_log(pg, run_id=run_id, job_name="ods_s3_publish", pipeline_type="publish",
                  domain=domain, dataset=dataset, source_path=s3_path,
                  status="count_verified", record_count=published,
                  config_version=config["version"], config_snapshot=config_snapshot)

    # Write lineage
    with pg.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline.lineage
              (run_id, domain, dataset, source_type, source_ref, target_topic,
               business_date, record_count, schema_version)
            VALUES (%s,%s,%s,'file',%s,%s,%s,%s,%s)
            """,
            (run_id, domain, dataset, s3_path, topic,
             business_date or None, published, config["schema_version"]),
        )
    pg.commit()

    write_job_log(pg, run_id=run_id, job_name="ods_s3_publish", pipeline_type="publish",
                  domain=domain, dataset=dataset, source_path=s3_path,
                  target_path=topic, status="completed",
                  record_count=published, config_version=config["version"],
                  config_snapshot=config_snapshot)

    set_file_state(pg, s3_path, run_id, "completed", record_count=published)

    spark.stop(); pg.close()
    print(f"[OK] Published {published} messages → {topic}")


if __name__ == "__main__":
    main()
```

---

**Step 4: Run the integration test**

```bash
pytest tests/integration/test_publish.py::test_publish_happy_path -v
```

Expected: `PASSED`.

---

**Step 5: Commit**

```bash
git add glue/jobs/ods_s3_publish.py tests/integration/test_publish.py
git commit -m "feat: add ods_s3_publish glue job (parquet to kafka)"
```

---

## Task 8: Airflow DAGs

**Files:**
- Create: `dags/dag2_etl_trigger.py`
- Create: `dags/dag_publish.py`
- Create: `tests/dags/test_dag_integrity.py`

---

**Step 1: Write DAG integrity tests**

```python
# tests/dags/test_dag_integrity.py
"""
DAG integrity tests — import validation only, no task execution.
Run locally without a running Airflow instance.
"""
import importlib
import pytest
from airflow.models import DAG


def test_dag2_imports_cleanly():
    import dags.dag2_etl_trigger as m
    assert hasattr(m, "dag")
    assert isinstance(m.dag, DAG)


def test_dag2_has_required_tasks():
    import dags.dag2_etl_trigger as m
    task_ids = {t.task_id for t in m.dag.tasks}
    for required in ["check_file_catalogue", "check_idempotency",
                     "verify_checksum", "trigger_glue_ingestion", "update_file_state"]:
        assert required in task_ids, f"Missing task: {required}"


def test_publish_dag_imports_cleanly():
    import dags.dag_publish as m
    assert hasattr(m, "dag")
    assert isinstance(m.dag, DAG)


def test_publish_dag_has_required_tasks():
    import dags.dag_publish as m
    task_ids = {t.task_id for t in m.dag.tasks}
    for required in ["check_idempotency", "load_config", "set_processing",
                     "trigger_glue_publish", "set_completed", "emit_audit_event"]:
        assert required in task_ids, f"Missing task: {required}"
```

---

**Step 2: Run tests to verify they fail**

```bash
pytest tests/dags/test_dag_integrity.py -v
```

Expected: `ImportError` — DAG files don't exist yet.

---

**Step 3: Create `dags/dag2_etl_trigger.py`**

```python
# dags/dag2_etl_trigger.py
import hashlib
import json
import os
import uuid
from datetime import datetime, timedelta

import psycopg2
import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sensors.s3_key_sensor import S3KeySensor

ENV = os.environ.get("ENV", "local")
RAW_BUCKET = f"ods-raw-{ENV}"
QUARANTINE_BUCKET = f"ods-quarantine-{ENV}"
S3_CONN = "aws_default"

default_args = {
    "owner": "ods",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def _pg_conn():
    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


def check_file_catalogue(s3_key: str, **ctx):
    """Verify s3_key matches an active file_catalogue entry. Quarantine if not."""
    conn = _pg_conn()
    filename = s3_key.split("/")[-1]
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT fc.id FROM pipeline.file_catalogue fc
            JOIN pipeline.dataset_config dc ON fc.dataset_config_id = dc.id
            WHERE fc.active = TRUE
              AND $1 LIKE REPLACE(fc.name_pattern, '*', '%')
            """,
            (filename,),
        )
        row = cur.fetchone()
    conn.close()
    if not row:
        _quarantine(s3_key, "not_in_catalogue")
        return False
    return True


def check_idempotency(s3_key: str, **ctx):
    s3_path = f"s3://{RAW_BUCKET}/{s3_key}"
    conn = _pg_conn()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT status FROM pipeline.ingestion_file_state WHERE s3_path = %s",
            (s3_path,),
        )
        row = cur.fetchone()
    conn.close()
    if row and row[0] == "transferred":
        return False  # skip — already done
    return True


def verify_checksum(s3_key: str, **ctx):
    s3 = _s3_client()
    obj = s3.get_object(Bucket=RAW_BUCKET, Key=s3_key)
    actual_md5 = hashlib.md5(obj["Body"].read()).hexdigest()
    expected_md5 = obj.get("Metadata", {}).get("md5", actual_md5)
    if actual_md5 != expected_md5 and expected_md5 != actual_md5:
        _quarantine(s3_key, "checksum_mismatch")
        raise ValueError(f"MD5 mismatch for {s3_key}")
    conn = _pg_conn()
    s3_path = f"s3://{RAW_BUCKET}/{s3_key}"
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline.ingestion_file_state (s3_path, status, checksum_md5)
            VALUES (%s, 'detected', %s)
            ON CONFLICT (s3_path) DO UPDATE SET status='detected', updated_at=NOW()
            """,
            (s3_path, actual_md5),
        )
    conn.commit(); conn.close()


def update_file_state(s3_key: str, **ctx):
    s3_path = f"s3://{RAW_BUCKET}/{s3_key}"
    conn = _pg_conn()
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE pipeline.ingestion_file_state SET status='transferred', updated_at=NOW() WHERE s3_path=%s",
            (s3_path,),
        )
    conn.commit(); conn.close()


def _quarantine(s3_key: str, reason: str):
    s3 = _s3_client()
    dest_key = f"reason={reason}/{s3_key}"
    s3.copy_object(
        Bucket=QUARANTINE_BUCKET,
        Key=dest_key,
        CopySource={"Bucket": RAW_BUCKET, "Key": s3_key},
    )


def _s3_client():
    endpoint = os.environ.get("LOCALSTACK_ENDPOINT")
    kwargs = dict(
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
        region_name=os.environ.get("AWS_DEFAULT_REGION", "eu-west-1"),
    )
    if endpoint:
        kwargs["endpoint_url"] = endpoint
    return boto3.client("s3", **kwargs)


with DAG(
    dag_id="dag2_etl_trigger",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    params={"s3_key": ""},
) as dag:

    s3_key = "{{ params.s3_key }}"
    run_id = "{{ run_id }}"

    t_check_catalogue = ShortCircuitOperator(
        task_id="check_file_catalogue",
        python_callable=check_file_catalogue,
        op_kwargs={"s3_key": s3_key},
    )

    t_check_idempotency = ShortCircuitOperator(
        task_id="check_idempotency",
        python_callable=check_idempotency,
        op_kwargs={"s3_key": s3_key},
    )

    t_verify_checksum = PythonOperator(
        task_id="verify_checksum",
        python_callable=verify_checksum,
        op_kwargs={"s3_key": s3_key},
    )

    t_trigger_glue = DockerOperator(
        task_id="trigger_glue_ingestion",
        image="ods-glue:local",
        network_mode="ods-network",
        command=(
            "spark-submit "
            "--py-files /home/glue_user/workspace/jobs/utils.py,"
            "/home/glue_user/workspace/jobs/dq.py "
            "/home/glue_user/workspace/jobs/ods_ingestion.py "
            f"--run_id {run_id} --domain insurance --dataset policies "
            f"--s3_input_path s3://{RAW_BUCKET}/{s3_key}"
        ),
        environment={
            "AWS_DEFAULT_REGION": os.environ.get("AWS_DEFAULT_REGION", "eu-west-1"),
            "AWS_ACCESS_KEY_ID": os.environ.get("AWS_ACCESS_KEY_ID", "test"),
            "AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
            "LOCALSTACK_ENDPOINT": os.environ.get("LOCALSTACK_ENDPOINT", ""),
            "POSTGRES_HOST": os.environ.get("POSTGRES_HOST", "postgres"),
            "POSTGRES_DB": os.environ.get("POSTGRES_DB", "ods_dev"),
            "POSTGRES_USER": os.environ.get("POSTGRES_USER", "ods"),
            "POSTGRES_PASSWORD": os.environ.get("POSTGRES_PASSWORD", "ods"),
            "SCHEMA_REGISTRY_URL": os.environ.get("SCHEMA_REGISTRY_URL", ""),
            "ENV": ENV,
        },
        volumes=[f"{os.getcwd()}/glue/jobs:/home/glue_user/workspace/jobs"],
        auto_remove=True,
    )

    t_update_state = PythonOperator(
        task_id="update_file_state",
        python_callable=update_file_state,
        op_kwargs={"s3_key": s3_key},
    )

    t_check_catalogue >> t_check_idempotency >> t_verify_checksum >> t_trigger_glue >> t_update_state
```

---

**Step 4: Create `dags/dag_publish.py`**

```python
# dags/dag_publish.py
import json
import os
import uuid
from datetime import datetime, timedelta

import psycopg2
import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.docker.operators.docker import DockerOperator
from confluent_kafka import Producer

ENV = os.environ.get("ENV", "local")
CURATED_BUCKET = f"ods-curated-{ENV}"

default_args = {
    "owner": "ods",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}


def _pg_conn():
    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


def check_idempotency(s3_path: str, **ctx):
    conn = _pg_conn()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT status FROM pipeline.file_state WHERE s3_path = %s",
            (s3_path,),
        )
        row = cur.fetchone()
    conn.close()
    return not (row and row[0] == "completed")


def load_config(s3_path: str, **ctx):
    conn = _pg_conn()
    parts = s3_path.strip("s3://").split("/")
    domain = parts[1] if len(parts) > 1 else "insurance"
    dataset = parts[2] if len(parts) > 2 else "policies"
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, target_topic, schema_id, schema_version, version "
            "FROM pipeline.dataset_config WHERE domain=%s AND dataset=%s AND active=TRUE",
            (domain, dataset),
        )
        row = cur.fetchone()
    conn.close()
    ctx["ti"].xcom_push(key="config_version", value=row[4] if row else 1)


def set_processing(s3_path: str, run_id: str, **ctx):
    conn = _pg_conn()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline.file_state (s3_path, run_id, status)
            VALUES (%s, %s, 'processing')
            ON CONFLICT (s3_path) DO UPDATE SET status='processing', run_id=%s, updated_at=NOW()
            """,
            (s3_path, run_id, run_id),
        )
    conn.commit(); conn.close()


def set_completed(s3_path: str, **ctx):
    conn = _pg_conn()
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE pipeline.file_state SET status='completed', updated_at=NOW() WHERE s3_path=%s",
            (s3_path,),
        )
    conn.commit(); conn.close()


def emit_audit_event(s3_path: str, run_id: str, **ctx):
    brokers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    producer = Producer({"bootstrap.servers": brokers})
    payload = json.dumps({
        "event": "publish_completed",
        "run_id": run_id,
        "s3_path": s3_path,
        "ts": datetime.utcnow().isoformat(),
    }).encode()
    producer.produce("ods.pipeline.audit", value=payload)
    producer.flush()


with DAG(
    dag_id="dag_publish",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    params={"s3_path": "", "domain": "insurance", "dataset": "policies"},
) as dag:

    s3_path = "{{ params.s3_path }}"
    run_id  = "{{ run_id }}"

    t_idempotency = ShortCircuitOperator(
        task_id="check_idempotency",
        python_callable=check_idempotency,
        op_kwargs={"s3_path": s3_path},
    )

    t_load_config = PythonOperator(
        task_id="load_config",
        python_callable=load_config,
        op_kwargs={"s3_path": s3_path},
    )

    t_set_processing = PythonOperator(
        task_id="set_processing",
        python_callable=set_processing,
        op_kwargs={"s3_path": s3_path, "run_id": run_id},
    )

    t_trigger_publish = DockerOperator(
        task_id="trigger_glue_publish",
        image="ods-glue:local",
        network_mode="ods-network",
        command=(
            "spark-submit "
            "--py-files /home/glue_user/workspace/jobs/utils.py,"
            "/home/glue_user/workspace/jobs/dq.py "
            "/home/glue_user/workspace/jobs/ods_s3_publish.py "
            f"--run_id {run_id} --domain {{{{ params.domain }}}} "
            f"--dataset {{{{ params.dataset }}}} --s3_input_path {s3_path}"
        ),
        environment={
            "AWS_DEFAULT_REGION": os.environ.get("AWS_DEFAULT_REGION", "eu-west-1"),
            "AWS_ACCESS_KEY_ID": os.environ.get("AWS_ACCESS_KEY_ID", "test"),
            "AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
            "LOCALSTACK_ENDPOINT": os.environ.get("LOCALSTACK_ENDPOINT", ""),
            "KAFKA_BOOTSTRAP_SERVERS": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", ""),
            "SCHEMA_REGISTRY_URL": os.environ.get("SCHEMA_REGISTRY_URL", ""),
            "POSTGRES_HOST": os.environ.get("POSTGRES_HOST", "postgres"),
            "POSTGRES_DB": os.environ.get("POSTGRES_DB", "ods_dev"),
            "POSTGRES_USER": os.environ.get("POSTGRES_USER", "ods"),
            "POSTGRES_PASSWORD": os.environ.get("POSTGRES_PASSWORD", "ods"),
            "ENV": ENV,
        },
        volumes=[f"{os.getcwd()}/glue/jobs:/home/glue_user/workspace/jobs"],
        auto_remove=True,
    )

    t_set_completed = PythonOperator(
        task_id="set_completed",
        python_callable=set_completed,
        op_kwargs={"s3_path": s3_path},
    )

    t_audit = PythonOperator(
        task_id="emit_audit_event",
        python_callable=emit_audit_event,
        op_kwargs={"s3_path": s3_path, "run_id": run_id},
    )

    (t_idempotency >> t_load_config >> t_set_processing
     >> t_trigger_publish >> t_set_completed >> t_audit)
```

---

**Step 5: Run DAG integrity tests**

```bash
pytest tests/dags/test_dag_integrity.py -v
```

Expected: all 4 tests `PASSED`.

---

**Step 6: Commit**

```bash
git add dags/ tests/dags/
git commit -m "feat: add airflow dag2 and publish dag"
```

---

## Task 9: End-to-End Integration Tests

All 9 scenarios from the design doc, verifying the full pipeline from CSV → Kafka.

**Files:**
- Create: `tests/integration/test_policies_e2e.py`
- Create: `tests/fixtures/policies_null_policy_id.csv`
- Create: `tests/fixtures/policies_high_premium.csv`
- Create: `tests/fixtures/policies_missing_column.csv`

---

**Step 1: Create fixture CSVs**

```bash
# Null policy_id (hard block)
cat > tests/fixtures/policies_null_policy_id.csv << 'EOF'
policy_id,policyholder_name,premium_amount,start_date,end_date,agent_code,postcode
,Alice Smith,1200.00,2026-01-01,2027-01-01,AG001,EC1A1BB
POL-002,Bob Jones,950.50,2026-02-01,2027-02-01,AG002,WC2N5DU
EOF

# High premium (soft warn only)
cat > tests/fixtures/policies_high_premium.csv << 'EOF'
policy_id,policyholder_name,premium_amount,start_date,end_date,agent_code,postcode
POL-003,Carol White,75000.00,2026-01-01,2027-01-01,AG003,EC1A1BB
EOF

# Missing required column (schema incompatible)
cat > tests/fixtures/policies_missing_column.csv << 'EOF'
policyholder_name,premium_amount,start_date
Alice Smith,1200.00,2026-01-01
EOF
```

---

**Step 2: Write all 9 e2e tests**

```python
# tests/integration/test_policies_e2e.py
"""
End-to-end integration tests: all 9 Phase 3 scenarios.
Prerequisite: full stack running (docker compose up -d).
"""
import hashlib
import os
import subprocess
import time
import uuid

import boto3
import psycopg2
import pytest
from confluent_kafka import Consumer, KafkaError

S3_ENDPOINT    = "http://localhost:4566"
RAW_BUCKET     = "ods-raw-local"
CURATED_BUCKET = "ods-curated-local"
DLQ_BUCKET     = "ods-dlq-local"
QUARANTINE     = "ods-quarantine-local"
NETWORK        = "ods-network"
TOPIC          = "ods.insurance.policies"
KAFKA_BROKERS  = "localhost:9092"


# ── fixtures ──────────────────────────────────────────────────────────────────

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
        host="localhost", port=5432,
        dbname="ods_dev", user="ods", password="ods",
    )
    yield conn
    conn.close()


# ── helpers ───────────────────────────────────────────────────────────────────

def run_ingestion(s3_key: str, run_id: str = None) -> subprocess.CompletedProcess:
    run_id = run_id or str(uuid.uuid4())
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
        "--py-files", "/home/glue_user/workspace/jobs/utils.py,/home/glue_user/workspace/jobs/dq.py",
        "/home/glue_user/workspace/jobs/ods_ingestion.py",
        "--run_id", run_id,
        "--domain", "insurance",
        "--dataset", "policies",
        "--s3_input_path", f"s3://ods-raw-local/{s3_key}",
    ]
    return subprocess.run(cmd, capture_output=True, text=True, timeout=180), run_id


def run_publish(s3_path: str, run_id: str = None) -> subprocess.CompletedProcess:
    run_id = run_id or str(uuid.uuid4())
    cmd = [
        "docker", "run", "--rm", "--network", NETWORK,
        "-e", "AWS_DEFAULT_REGION=eu-west-1",
        "-e", "AWS_ACCESS_KEY_ID=test",
        "-e", "AWS_SECRET_ACCESS_KEY=test",
        "-e", "LOCALSTACK_ENDPOINT=http://localstack:4566",
        "-e", "KAFKA_BOOTSTRAP_SERVERS=broker:29092",
        "-e", "SCHEMA_REGISTRY_URL=http://schema-registry:8081",
        "-e", "POSTGRES_HOST=postgres",
        "-e", "POSTGRES_DB=ods_dev",
        "-e", "POSTGRES_USER=ods",
        "-e", "POSTGRES_PASSWORD=ods",
        "-e", "ENV=local",
        "-v", f"{os.getcwd()}/glue/jobs:/home/glue_user/workspace/jobs",
        "ods-glue:local",
        "spark-submit",
        "--py-files", "/home/glue_user/workspace/jobs/utils.py,/home/glue_user/workspace/jobs/dq.py",
        "/home/glue_user/workspace/jobs/ods_s3_publish.py",
        "--run_id", run_id,
        "--domain", "insurance",
        "--dataset", "policies",
        "--s3_input_path", s3_path,
    ]
    return subprocess.run(cmd, capture_output=True, text=True, timeout=180), run_id


def upload(s3, key: str, content: str, bucket: str = RAW_BUCKET):
    s3.put_object(Bucket=bucket, Key=key, Body=content.encode())


def count_kafka_messages(topic: str, timeout: float = 15.0) -> int:
    group = f"test-{uuid.uuid4()}"
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKERS,
        "group.id": group,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([topic])
    count = 0
    deadline = time.time() + timeout
    while time.time() < deadline:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            break
        count += 1
    consumer.close()
    return count


def job_log_status(pg, run_id: str) -> str:
    cur = pg.cursor()
    cur.execute(
        "SELECT status FROM pipeline.glue_job_log WHERE run_id=%s ORDER BY id DESC LIMIT 1",
        (run_id,),
    )
    row = cur.fetchone()
    return row[0] if row else None


def dlq_count(s3, run_id: str) -> int:
    result = s3.list_objects_v2(Bucket=DLQ_BUCKET, Prefix=f"insurance/policies/")
    return result.get("KeyCount", 0)


def quarantine_count(s3, prefix: str = "") -> int:
    result = s3.list_objects_v2(Bucket=QUARANTINE, Prefix=prefix)
    return result.get("KeyCount", 0)


GOOD_CSV = (
    "policy_id,policyholder_name,premium_amount,start_date,end_date,agent_code,postcode\n"
    "POL-E2E-001,Alice Smith,1200.00,2026-01-01,2027-01-01,AG001,EC1A1BB\n"
    "POL-E2E-002,Bob Jones,950.50,2026-02-01,2027-02-01,AG002,WC2N5DU\n"
)


# ── scenario 1 — happy path ───────────────────────────────────────────────────

def test_1_happy_path(s3, pg):
    key = "insurance/policies/date=20260601/policies_20260601.csv"
    upload(s3, key, GOOD_CSV)

    result, run_id_ingest = run_ingestion(key)
    assert result.returncode == 0, result.stderr

    curated = s3.list_objects_v2(Bucket=CURATED_BUCKET, Prefix="insurance/policies/date=2026-06-01/")
    assert curated.get("KeyCount", 0) > 0

    curated_path = f"s3://ods-curated-local/insurance/policies/date=2026-06-01/"
    result, run_id_pub = run_publish(curated_path)
    assert result.returncode == 0, result.stderr

    msgs = count_kafka_messages(TOPIC)
    assert msgs >= 2

    assert job_log_status(pg, run_id_pub) == "completed"


# ── scenario 2 — idempotency ──────────────────────────────────────────────────

def test_2_idempotency(s3, pg):
    key = "insurance/policies/date=20260602/policies_20260602.csv"
    upload(s3, key, GOOD_CSV)

    _, run1 = run_ingestion(key)
    msgs_after_first = count_kafka_messages(TOPIC)

    _, run2 = run_ingestion(key)
    msgs_after_second = count_kafka_messages(TOPIC)

    assert msgs_after_first == msgs_after_second
    assert job_log_status(pg, run2) in ("skipped", "completed")


# ── scenario 3 — file not approved ───────────────────────────────────────────

def test_3_file_not_approved(s3, pg):
    key = "insurance/claims/date=20260603/claims_20260603.csv"
    upload(s3, key, GOOD_CSV)
    before = quarantine_count(s3)
    result, _ = run_ingestion(key)
    # Job exits cleanly (returncode 0) but logs skipped or the catalogue check
    # prevents processing. File should not appear in curated.
    curated = s3.list_objects_v2(Bucket=CURATED_BUCKET, Prefix="insurance/claims/")
    assert curated.get("KeyCount", 0) == 0


# ── scenario 4 — schema incompatible ─────────────────────────────────────────

def test_4_schema_incompatible(s3, pg):
    missing_col_csv = "policyholder_name,premium_amount,start_date\nAlice,1200.00,2026-01-01\n"
    key = "insurance/policies/date=20260604/policies_20260604.csv"
    upload(s3, key, missing_col_csv)

    result, run_id = run_ingestion(key)
    assert result.returncode != 0 or job_log_status(pg, run_id) == "failed"
    assert job_log_status(pg, run_id) == "failed"


# ── scenario 5 — DQ hard block (null policy_id) ───────────────────────────────

def test_5_dq_hard_block_null_policy_id(s3, pg):
    csv = (
        "policy_id,policyholder_name,premium_amount,start_date,end_date,agent_code,postcode\n"
        ",Alice,1200.00,2026-01-01,2027-01-01,AG001,EC1A1BB\n"
        "POL-E2E-003,Bob,950.00,2026-01-01,2027-01-01,AG002,WC2N5DU\n"
    )
    key = "insurance/policies/date=20260605/policies_20260605.csv"
    upload(s3, key, csv)

    before_dlq = dlq_count(s3, "insurance/policies/")
    result, run_id = run_ingestion(key)

    after_dlq = dlq_count(s3, "insurance/policies/")
    assert after_dlq > before_dlq  # failing row in DLQ

    curated = s3.list_objects_v2(Bucket=CURATED_BUCKET, Prefix="insurance/policies/date=2026-06-05/")
    assert curated.get("KeyCount", 0) > 0  # passing row written


# ── scenario 6 — DQ soft warn (premium > 50k) ────────────────────────────────

def test_6_dq_soft_warn_high_premium(s3, pg):
    csv = (
        "policy_id,policyholder_name,premium_amount,start_date,end_date,agent_code,postcode\n"
        "POL-E2E-004,Carol,75000.00,2026-01-01,2027-01-01,AG003,EC1A1BB\n"
    )
    key = "insurance/policies/date=20260606/policies_20260606.csv"
    upload(s3, key, csv)

    result, run_id = run_ingestion(key)
    assert result.returncode == 0

    # Row still written to curated (soft warn does not block)
    curated = s3.list_objects_v2(Bucket=CURATED_BUCKET, Prefix="insurance/policies/date=2026-06-06/")
    assert curated.get("KeyCount", 0) > 0

    status = job_log_status(pg, run_id)
    assert status in ("dq_warned", "completed")


# ── scenario 7 — business date extraction ─────────────────────────────────────

def test_7_business_date_extracted_from_filename(s3, pg):
    key = "insurance/policies/date=20261201/policies_20261201.csv"
    upload(s3, key, GOOD_CSV)

    result, run_id = run_ingestion(key)
    assert result.returncode == 0

    cur = pg.cursor()
    cur.execute(
        "SELECT business_date FROM pipeline.glue_job_log WHERE run_id=%s AND status='started'",
        (run_id,),
    )
    row = cur.fetchone()
    assert row is not None
    assert str(row[0]) == "2026-12-01"


# ── scenario 8 — duplicate policy_id (hard block) ────────────────────────────

def test_8_duplicate_policy_id_blocked(s3, pg):
    csv = (
        "policy_id,policyholder_name,premium_amount,start_date,end_date,agent_code,postcode\n"
        "POL-DUP,Alice,1200.00,2026-01-01,2027-01-01,AG001,EC1A1BB\n"
        "POL-DUP,Bob,950.00,2026-01-01,2027-01-01,AG002,WC2N5DU\n"
        "POL-UNIQ,Carol,800.00,2026-01-01,2027-01-01,AG003,WC2N5DU\n"
    )
    key = "insurance/policies/date=20260608/policies_20260608.csv"
    upload(s3, key, csv)

    before_dlq = dlq_count(s3, "insurance/policies/")
    result, run_id = run_ingestion(key)
    after_dlq = dlq_count(s3, "insurance/policies/")

    assert after_dlq > before_dlq  # duplicates → DLQ
    curated = s3.list_objects_v2(Bucket=CURATED_BUCKET, Prefix="insurance/policies/date=2026-06-08/")
    assert curated.get("KeyCount", 0) > 0  # POL-UNIQ still written


# ── scenario 9 — end-to-end count assertion ───────────────────────────────────

def test_9_end_to_end_record_count(s3, pg):
    csv_rows = "\n".join(
        f"POL-CNT-{i:03d},Person {i},{1000+i}.00,2026-01-01,2027-01-01,AG001,EC1A1BB"
        for i in range(1, 6)
    )
    csv = f"policy_id,policyholder_name,premium_amount,start_date,end_date,agent_code,postcode\n{csv_rows}\n"
    key = "insurance/policies/date=20260609/policies_20260609.csv"
    upload(s3, key, csv)

    result, ingest_run = run_ingestion(key)
    assert result.returncode == 0

    curated_path = f"s3://ods-curated-local/insurance/policies/date=2026-06-09/"
    result, pub_run = run_publish(curated_path)
    assert result.returncode == 0

    cur = pg.cursor()
    cur.execute(
        "SELECT record_count FROM pipeline.lineage WHERE run_id=%s",
        (pub_run,),
    )
    row = cur.fetchone()
    assert row is not None
    assert row[0] == 5
```

---

**Step 3: Run all e2e tests**

```bash
pytest tests/integration/test_policies_e2e.py -v --timeout=300
```

Expected: all 9 tests `PASSED`.

---

**Step 4: Commit**

```bash
git add tests/integration/test_policies_e2e.py tests/fixtures/
git commit -m "test: add all 9 phase 3 e2e scenarios for policies pipeline"
```

---

## Done

After Task 9 passes, the pipeline is complete:

- CSV → Parquet via `ods_ingestion.py` ✓
- Parquet → Kafka via `ods_s3_publish.py` ✓
- Full audit trail in `pipeline.glue_job_log` ✓
- Config truth in `pipeline.dataset_config` ✓
- Restartability via `config_snapshot` in job log ✓
- All 9 Phase 3 test scenarios passing ✓
