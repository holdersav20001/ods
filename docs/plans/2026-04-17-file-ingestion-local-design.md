# File Ingestion Pipeline — Local Development Design

**Date:** 2026-04-17
**Status:** Approved
**Scope:** CSV → Parquet → Kafka (Pattern 1, no SFTP — starts from S3 Raw)
**Environment:** LocalStack + Docker Compose (local dev)

---

## 1. Approach

Single `docker-compose.yml` brings up the full stack with one command. All services run locally; production differences are isolated behind an `ENV` environment variable.

PostgreSQL is the **single source of truth for all configuration** — dataset config, topic mapping, schema IDs, DQ rules, and the file whitelist all live in Postgres. YAML files are used only as a seed/import format to bootstrap the database. This makes configuration queryable, versionable, auditable, and ready for a future management API.

---

## 2. Repository Structure

```
aviva-ods/
├── docker-compose.yml
├── .env                          # LOCAL_* vars (not committed)
├── seeds/
│   └── insurance/
│       └── policies.yaml         # seed format only — imports into pipeline.dataset_config
├── glue/
│   └── jobs/
│       ├── ods_ingestion.py      # CSV → Parquet → S3 Curated
│       └── ods_s3_publish.py     # Parquet → Kafka
├── dags/
│   ├── dag2_etl_trigger.py       # S3 Raw event → Glue ingestion job
│   └── dag_publish.py            # S3 Curated event → Glue publish job
├── db/
│   └── migrations/
│       ├── V1__init_schema.sql   # pipeline.* tables
│       └── V2__seed_policies.sql # initial dataset_config + file_catalogue rows
└── tests/
    └── integration/
        └── test_policies_e2e.py  # happy path + all failure scenarios
```

---

## 3. Docker Compose Services

| Service | Image | Purpose |
|---|---|---|
| `localstack` | `localstack/localstack` | S3 buckets (raw, curated, config, dlq, quarantine) |
| `zookeeper` | `confluentinc/cp-zookeeper` | Kafka coordination |
| `broker` | `confluentinc/cp-kafka` | MSK substitute |
| `schema-registry` | `confluentinc/cp-schema-registry` | Avro schema governance |
| `postgres` | `postgres:15` | All pipeline state and configuration |
| `airflow` | `apache/airflow:2.9` | DAG orchestration (local executor, single container) |
| `glue` | `amazon/aws-glue-libs:glue4` | PySpark job runner (spawned per job via DockerOperator) |

Airflow runs in **local executor** mode — no Celery, no Redis. The `glue` container is not long-running; Airflow's `DockerOperator` spawns it per job run against the shared Docker socket.

---

## 4. PostgreSQL Schema

### `pipeline.dataset_config` — primary config store (replaces YAML at runtime)

```sql
CREATE TABLE pipeline.dataset_config (
    id                  SERIAL PRIMARY KEY,
    domain              VARCHAR NOT NULL,
    dataset             VARCHAR NOT NULL,
    filename_pattern    VARCHAR NOT NULL,        -- e.g. policies_(\d{8})\.csv
    target_topic        VARCHAR NOT NULL,        -- e.g. ods.insurance.policies
    schema_id           VARCHAR NOT NULL,        -- Confluent Schema Registry subject
    schema_version      INTEGER NOT NULL,        -- pinned version
    key_fields          JSONB NOT NULL,          -- e.g. ["policy_id"]
    dq_rules            JSONB NOT NULL,          -- see Section 7
    data_classification VARCHAR NOT NULL,        -- Public | Internal | Confidential | Restricted-PII
    active              BOOLEAN DEFAULT TRUE,
    version             INTEGER NOT NULL DEFAULT 1,  -- increments on every change
    created_at          TIMESTAMP DEFAULT NOW(),
    updated_at          TIMESTAMP DEFAULT NOW(),
    UNIQUE (domain, dataset)
);
```

### `pipeline.file_catalogue` — approved file whitelist

```sql
CREATE TABLE pipeline.file_catalogue (
    id                SERIAL PRIMARY KEY,
    name_pattern      VARCHAR NOT NULL,          -- e.g. policies_*.csv
    sftp_path         VARCHAR NOT NULL,
    domain            VARCHAR NOT NULL,
    dataset           VARCHAR NOT NULL,
    dataset_config_id INTEGER NOT NULL REFERENCES pipeline.dataset_config(id),
    active            BOOLEAN DEFAULT TRUE,
    created_at        TIMESTAMP DEFAULT NOW()
);
```

### `pipeline.file_state` — coarse-grained idempotency per file

```sql
CREATE TABLE pipeline.file_state (
    id            SERIAL PRIMARY KEY,
    s3_path       VARCHAR NOT NULL UNIQUE,
    run_id        UUID NOT NULL,
    status        VARCHAR NOT NULL,             -- new | processing | completed | failed
    record_count  INTEGER,
    error_reason  VARCHAR,
    created_at    TIMESTAMP DEFAULT NOW(),
    updated_at    TIMESTAMP DEFAULT NOW()
);
```

### `pipeline.ingestion_file_state` — DAG1/DAG2 transfer state

```sql
CREATE TABLE pipeline.ingestion_file_state (
    id            SERIAL PRIMARY KEY,
    s3_path       VARCHAR NOT NULL UNIQUE,
    status        VARCHAR NOT NULL,             -- detected | transferred | failed
    checksum_md5  VARCHAR,
    created_at    TIMESTAMP DEFAULT NOW(),
    updated_at    TIMESTAMP DEFAULT NOW()
);
```

### `pipeline.glue_job_log` — immutable step audit log

```sql
CREATE TABLE pipeline.glue_job_log (
    id              BIGSERIAL PRIMARY KEY,
    run_id          UUID NOT NULL,
    job_name        VARCHAR NOT NULL,
    pipeline_type   VARCHAR NOT NULL,           -- ingestion | publish
    domain          VARCHAR NOT NULL,
    dataset         VARCHAR NOT NULL,
    source_path     VARCHAR,
    target_path     VARCHAR,
    business_date   DATE,
    status          VARCHAR NOT NULL,
    -- status values: started | file_read | schema_validated | dq_passed | dq_warned |
    --                parquet_written | count_verified | publishing | lineage_written |
    --                completed | failed
    record_count    INTEGER,
    error_reason    VARCHAR,
    error_detail    TEXT,
    config_version  INTEGER,                    -- pipeline.dataset_config.version at run time
    config_snapshot JSONB,                      -- full config copy at run time (restart-safe)
    created_at      TIMESTAMP DEFAULT NOW()
);
-- INSERT only — never UPDATE or DELETE
```

### `pipeline.lineage` — Kafka publish provenance

```sql
CREATE TABLE pipeline.lineage (
    id                BIGSERIAL PRIMARY KEY,
    run_id            UUID NOT NULL,
    domain            VARCHAR NOT NULL,
    dataset           VARCHAR NOT NULL,
    source_type       VARCHAR NOT NULL,         -- file | cdc | api | event
    source_ref        VARCHAR NOT NULL,         -- S3 path or equivalent
    target_topic      VARCHAR NOT NULL,
    business_date     DATE,
    kafka_offset_start BIGINT,
    kafka_offset_end   BIGINT,
    record_count      INTEGER,
    schema_version    INTEGER,
    created_at        TIMESTAMP DEFAULT NOW()
);
```

---

## 5. Glue Job 1 — `ods_ingestion.py` (CSV → Parquet)

**Triggered by:** DAG2 via `DockerOperator`, passing `--run_id`, `--dataset`, `--s3_input_path`.

**On startup:** reads `pipeline.dataset_config` for the dataset, snapshots `config_version` and `config_snapshot` into `glue_job_log`. All subsequent steps use the snapshotted config — immune to mid-flight config changes.

**On restart:** reads existing `glue_job_log` row for `run_id`, reloads `config_snapshot`, resumes from last non-terminal step.

### Processing steps

| Step | Status logged | Failure action |
|---|---|---|
| Job start | `started` | — |
| Read CSV from S3 Raw | `file_read` | — |
| Validate schema vs Schema Registry | `schema_validated` | DLQ, `failed`, stop |
| Run DQ rules (from `config_snapshot`) | `dq_passed` / `dq_warned` | Hard block: failing rows → DLQ; soft warn: CloudWatch metric, continue |
| Convert to Parquet, write S3 Curated | `parquet_written` | DLQ, `failed` |
| Assert written count == source count | `count_verified` | DLQ, `failed` |
| Complete | `completed` | — |

**S3 Curated path:** `{domain}/{dataset}/date={business_date}/`
**`business_date`** extracted from filename via `filename_pattern` regex in `dataset_config`.

---

## 6. Glue Job 2 — `ods_s3_publish.py` (Parquet → Kafka)

**Triggered by:** Publish DAG via `DockerOperator`, passing `--run_id`, `--dataset`, `--s3_input_path`.

**On startup:** same config snapshot pattern as Job 1. Checks `pipeline.file_state` — exits immediately if `completed`.

### Processing steps

| Step | Status logged | Failure action |
|---|---|---|
| Job start + idempotency check | `started` | Exit if `completed` |
| Set processing lock | `processing` | Concurrent run blocked |
| Read Parquet from S3 Curated | `parquet_read` | — |
| Fetch pinned Avro schema (version from `config_snapshot`) | `schema_fetched` | DLQ, `failed` |
| Run DQ rules | `dq_passed` / `dq_warned` | Hard block → DLQ; soft warn → CloudWatch metric |
| Generate SHA256 keys from `key_fields`, publish to Kafka (acks=all, transactions) | `publishing` | Transaction rollback, Airflow retry |
| Assert Kafka offset delta == source count | `count_verified` | DLQ, `failed` |
| Write to `pipeline.lineage` | `lineage_written` | — |
| Complete | `completed` | — |

### Kafka message headers

```
x-ods-run-id
x-ods-source-ref        (S3 path)
x-ods-source-type       file
x-ods-business-date
x-ods-schema-version
x-ods-pipeline-type     publish
```

---

## 7. DQ Rules

Stored as JSONB in `pipeline.dataset_config.dq_rules`. Two categories:

### Hard blocks — failing rows routed to DLQ, passing rows continue

| Rule | Field | Condition |
|---|---|---|
| Not null | `policy_id` | Key field — SHA256 key cannot be generated without it |
| Unique within file | `policy_id` | Duplicates would cause silent overwrites on compacted topic |
| Not null | `premium_amount` | Core business field |
| Range | `premium_amount` | > 0 |
| Valid date | `start_date` | Required for partitioning and business logic |
| Date ordering | `end_date` | >= `start_date` |

### Soft warns — CloudWatch metric emitted, row continues

| Rule | Field | Condition |
|---|---|---|
| Range | `premium_amount` | < 50,000 (unusually large — possible data entry error) |
| Date range | `end_date` | Not in the past (expired at ingestion time) |
| Completeness | Optional fields | > 80% populated (e.g. `agent_code`, `postcode`) |

---

## 8. Airflow DAGs

### DAG2 — `dag2_etl_trigger.py`

**Trigger (local):** `S3KeySensor` polling LocalStack every 30 seconds on `ods-raw-local/`.
**Trigger (prod):** EventBridge S3 Object Created rule.

```
check_file_catalogue    → query file_catalogue + dataset_config: approved?
                          → not approved: quarantine, stop
check_idempotency       → query ingestion_file_state: already completed? exit cleanly
verify_checksum         → MD5 of S3 object
                          → mismatch: quarantine, alarm
trigger_glue_ingestion  → DockerOperator: ods_ingestion.py (passes run_id)
wait_for_completion     → poll glue_job_log for terminal status
update_file_state       → ingestion_file_state = transferred
```

### Publish DAG — `dag_publish.py`

**Trigger (local):** `S3KeySensor` polling LocalStack every 30 seconds on `ods-curated-local/`.
**Trigger (prod):** EventBridge S3 Object Created rule (`ods-curated-file-rule`).

```
check_idempotency       → file_state = completed? exit cleanly
load_config             → read dataset_config from Postgres, record version
set_processing          → file_state = processing (guards concurrent runs)
trigger_glue_publish    → DockerOperator: ods_s3_publish.py (passes run_id)
wait_for_completion     → poll glue_job_log for terminal status
register_catalog        → Glue Data Catalog registration
trigger_crawler         → async, no wait
set_completed           → file_state = completed
emit_audit_event        → publish to ods.pipeline.audit
```

---

## 9. End-to-End Data Flow (Local)

```
[test CSV placed in ods-raw-local (LocalStack S3)]
        │
        ▼
DAG2 S3KeySensor fires
        │
        ├── file_catalogue check (Postgres)
        ├── idempotency check (Postgres)
        ├── MD5 checksum verify
        └── DockerOperator → ods_ingestion.py (Glue/PySpark container)
                │
                ├── load + snapshot dataset_config from Postgres
                ├── schema validate (Confluent Schema Registry)
                ├── DQ rules (hard block + soft warn)
                ├── CSV → Parquet
                └── write ods-curated-local (LocalStack S3)
                        │
                        ▼
        Publish DAG S3KeySensor fires
                │
                ├── idempotency check (Postgres)
                ├── load dataset_config from Postgres (pin version)
                ├── set file_state = processing
                └── DockerOperator → ods_s3_publish.py (Glue/PySpark container)
                        │
                        ├── load config_snapshot (from glue_job_log, restart-safe)
                        ├── fetch pinned Avro schema (Confluent Schema Registry)
                        ├── DQ rules
                        ├── SHA256 message keys
                        ├── publish → ods.insurance.policies (Confluent Kafka)
                        ├── pipeline.lineage written
                        └── file_state = completed → audit event → ods.pipeline.audit
```

---

## 10. Failure Routing

| Failure | Caught by | Destination | Final state |
|---|---|---|---|
| File not in `file_catalogue` | DAG2 | `ods-quarantine-local` | — |
| Checksum mismatch | DAG2 | `ods-quarantine-local` | `failed` |
| Schema incompatible (ingestion) | `ods_ingestion.py` | `ods-dlq-local` | `failed` |
| DQ hard block (ingestion) | `ods_ingestion.py` | failing rows → DLQ, passing rows continue | `dq_warned` |
| Count mismatch (ingestion) | `ods_ingestion.py` | `ods-dlq-local` | `failed` |
| Schema fetch fails (publish) | `ods_s3_publish.py` | `ods-dlq-local` | `failed` |
| Kafka publish fails | `ods_s3_publish.py` | transaction rolled back, Airflow retry | `processing` |
| Count mismatch (publish) | `ods_s3_publish.py` | `ods-dlq-local` | `failed` |

All failures write a final `status=failed` row to `glue_job_log` with `error_reason` and `error_detail`.

---

## 11. Local Environment Variables (`.env`)

```
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
AIRFLOW_CONN_ODS_POSTGRES=postgresql://ods:ods@postgres:5432/ods_dev
ENV=local
```

---

## 12. Integration Test Scenarios

File: `tests/integration/test_policies_e2e.py`

Each test: places a fixture CSV in LocalStack S3, triggers the DAG, polls for completion, asserts Kafka consumer count, DB state, and `glue_job_log` step sequence.

| # | Scenario | Assert |
|---|---|---|
| 1 | Happy path | Kafka count == CSV row count; all steps logged; lineage written |
| 2 | Idempotency (same file twice) | Kafka count unchanged; second run exits at idempotency check |
| 3 | File not in `file_catalogue` | File in quarantine bucket; DAG stopped at step 1 |
| 4 | Checksum mismatch | File in quarantine; `ingestion_file_state` = failed |
| 5 | Schema incompatible (missing required column) | File in DLQ; `glue_job_log` status = failed at `schema_validated` |
| 6 | DQ hard block (null `policy_id`) | Failing rows in DLQ; passing rows in Kafka; status = `dq_warned` |
| 7 | DQ soft warn (premium > 50,000) | CloudWatch metric emitted; all rows in Kafka |
| 8 | Kafka count mismatch (simulated) | DLQ write; `file_state` = failed |
| 9 | Business date extraction | `business_date` in `glue_job_log` matches filename date |

---

## 13. API Readiness

Configuration in `pipeline.dataset_config` is structured to accept future write operations via a management API:

- `POST /datasets` — insert new `dataset_config` row, seed `file_catalogue`
- `PUT /datasets/{id}/config` — increment `version`, update fields
- `GET /datasets/{id}/runs` — query `glue_job_log` by domain+dataset
- `GET /runs/{run_id}` — full audit trail for a single run including `config_snapshot`

No API is built in this phase. The schema is designed not to require migration when the API is added.
