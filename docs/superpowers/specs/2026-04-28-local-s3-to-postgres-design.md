# ODS Platform — Local S3-to-Postgres Pipeline Design

**Date:** 2026-04-28
**Status:** Draft (awaiting review)
**Scope:** Local docker-compose implementation of the S3 batch ingestion pattern, terminating in Postgres (and S3 curated). Consolidates the lineage, observability, and reconciliation plans (T0 + T2 only) into a single set of `pipeline.*` tables.

Out of scope for v1: T3 daily reconciliation, watermarks, late-arrival handling, full DR runbooks, CDC/API/Event patterns.

---

## 1. Goals

1. Run the full S3 batch ingestion pipeline locally on Docker, end-to-end: file drop → SFTP → S3 raw → Glue (Parquet) → Glue (Avro→Kafka) → Kafka Connect sinks → Postgres + S3 curated.
2. Replace the four overlapping pipeline tables (`file_state`, `ingestion_file_state`, `glue_job_log`, `lineage`) with a consolidated, polymorphic model (`run_log` header + `run_stage_log` child) that survives the addition of CDC/API/Event patterns without schema changes.
3. Provide T0 (publish-time) and T2 (hourly count) reconciliation across the three planes: source row count → Kafka offsets → Postgres rows.
4. Make local development closely mirror the planned production stack so configuration — not code — is what differs between local and AWS.

## 2. Non-goals

- Cloud deployment, IaC, or AWS resource provisioning.
- T3 daily aggregate reconciliation, checksum verification, watermarks, retroactive correction.
- DR runbooks, cross-region replication.
- CDC, API, and Event ingestion patterns (the consolidated table model anticipates them but no code is written for them in v1).
- Production-grade security (mTLS, IAM, secrets manager).

## 3. Architecture

```
┌─ atmoz/sftp ──────┐  airflow_sftp_sensor
│ /upload/<dataset>/│ ─────────► [Airflow DAG: dag_drop_to_raw]
└───────────────────┘                 │ uploads → s3://ods-raw/<domain>/<dataset>/<bd>/file.csv
                                      ▼
                                  LocalStack S3 (raw, staging, curated, dlq)
                                      │
        Airflow DAG: dag_ingest ──────┤
        ├─ stage_ingest    glue ods_ingestion          (CSV → Parquet)
        ├─ stage_dq        glue dq.py                  (hard/soft rules; fail rows → DLQ)
        ├─ stage_publish   glue ods_s3_publish         (Parquet → Avro → Kafka, T0 check)
        ├─ stage_wait_sinks  airflow polls Connect REST
        └─ finalise        run_log status update
                                      │
                                      ▼
        Redpanda  ◄── Confluent Schema Registry
                │
                ├── Connect: JDBC Sink ──► postgres ods.<domain>_<dataset>
                └── Connect: S3  Sink ──► LocalStack s3://ods-curated/...

   Airflow DAG: dag_recon_t2 (hourly)
        compares run_log.record_count ↔ kafka offset delta ↔ postgres row count
        → pipeline.reconciliation_log

   Grafana → Postgres datasource → dashboards (run health, recon, DLQ)
```

### 3.1 Containers (`docker-compose.yml`)

| Service           | Image                                       | Purpose                                  |
|-------------------|---------------------------------------------|------------------------------------------|
| `localstack`      | `localstack/localstack:3`                   | S3 (raw, staging, curated, dlq buckets)  |
| `postgres`        | `postgres:16` (port 5440)                   | Pipeline metadata + ODS target tables + Airflow metadata |
| `airflow-init`    | derived                                      | DB init + admin user                     |
| `airflow-webserver` | derived                                    | UI on 8080                               |
| `airflow-scheduler` | derived                                    | DAG execution                            |
| `redpanda`        | `redpandadata/redpanda:latest`              | Kafka API                                |
| `schema-registry` | `confluentinc/cp-schema-registry:7.5.x`     | Avro schemas                             |
| `kafka-connect`   | `confluentinc/cp-kafka-connect:7.5.x` + JDBC + S3 plugins | Sinks                  |
| `sftp`            | `atmoz/sftp:latest`                         | File drop surface                        |
| `glue`            | existing `ods-glue:local`                   | Spark job runner                         |
| `grafana`         | `grafana/grafana:latest`                    | Dashboards                               |

### 3.2 Decisions captured

| Decision                       | Choice                                         |
|--------------------------------|-----------------------------------------------|
| Sink mechanism                 | Kafka Connect (JDBC + S3 sinks)                |
| Schema Registry                | Confluent Schema Registry                      |
| Drop surface                   | SFTP container                                 |
| Reconciliation runner          | Airflow (no EventBridge anywhere)              |
| Observability stack            | Postgres + Grafana (no Prometheus in v1)       |
| Run granularity                | Hybrid — `run_log` header + `run_stage_log` child |
| Config source                  | YAML in repo, synced into `dataset_config`    |
| Reconciliation tiers in v1     | T0 (publish) + T2 (hourly cross-plane counts) |

## 4. Data Model

All under schema `pipeline`. Production swap is via Postgres-compatible target — no model changes.

### 4.1 `pipeline.dataset_config` (extended)
```sql
CREATE TABLE pipeline.dataset_config (
    domain                   VARCHAR NOT NULL,
    dataset                  VARCHAR NOT NULL,
    source_type              VARCHAR NOT NULL,           -- 's3_batch' | 'cdc' | 'api' | 'event'
    schema_def               JSONB NOT NULL,
    dq_rules                 JSONB NOT NULL,
    kafka_topic              VARCHAR NOT NULL,
    postgres_target_table    VARCHAR NOT NULL,
    s3_curated_path          VARCHAR NOT NULL,
    config_version_id        BIGINT NOT NULL,
    config_yaml_hash         CHAR(64) NOT NULL,
    config_pinned_at         TIMESTAMP NOT NULL DEFAULT NOW(),
    recon_tolerance_records  INT  NOT NULL DEFAULT 0,
    recon_tolerance_pct      NUMERIC(6,4) NOT NULL DEFAULT 0,
    PRIMARY KEY (domain, dataset)
);
```

### 4.2 `pipeline.file_catalogue` (replaces `file_state` + `ingestion_file_state`)
```sql
CREATE TABLE pipeline.file_catalogue (
    file_id              UUID PRIMARY KEY,
    domain               VARCHAR NOT NULL,
    dataset              VARCHAR NOT NULL,
    business_date        DATE NOT NULL,
    sftp_path            VARCHAR,
    s3_raw_path          VARCHAR,
    s3_staging_parquet_path VARCHAR,
    s3_curated_path      VARCHAR,
    file_size_bytes      BIGINT,
    source_row_count     BIGINT,
    file_md5             CHAR(32) NOT NULL,
    state                VARCHAR NOT NULL,               -- received|ingesting|ingested|published|sunk|failed|dlq
    state_updated_at     TIMESTAMP NOT NULL DEFAULT NOW(),
    first_seen_at        TIMESTAMP NOT NULL DEFAULT NOW(),
    last_run_id          UUID,
    UNIQUE (domain, dataset, file_md5)
);
```

### 4.3 `pipeline.run_log` (header)
```sql
CREATE TABLE pipeline.run_log (
    run_id                  UUID PRIMARY KEY,
    pipeline_type           VARCHAR NOT NULL,            -- 's3_batch' for v1
    domain                  VARCHAR NOT NULL,
    dataset                 VARCHAR NOT NULL,
    business_date           DATE,
    file_id                 UUID REFERENCES pipeline.file_catalogue(file_id),
    status                  VARCHAR NOT NULL,            -- running|succeeded|failed|partial
    started_at              TIMESTAMP NOT NULL DEFAULT NOW(),
    ended_at                TIMESTAMP,
    record_count_source     BIGINT,
    record_count_dq_pass    BIGINT,
    record_count_dq_fail    BIGINT,
    record_count_published  BIGINT,
    kafka_topic             VARCHAR,
    kafka_offset_start      BIGINT,
    kafka_offset_end        BIGINT,
    config_version_id       BIGINT,
    schema_version_id       INT,
    parents                 JSONB,                        -- [{run_id, role}] upstream lineage
    error_summary           TEXT,
    created_at              TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX ON pipeline.run_log (domain, dataset, business_date);
CREATE INDEX ON pipeline.run_log (status, started_at);
```

### 4.4 `pipeline.run_stage_log` (child)
```sql
CREATE TABLE pipeline.run_stage_log (
    id              BIGSERIAL PRIMARY KEY,
    run_id          UUID NOT NULL REFERENCES pipeline.run_log(run_id),
    stage           VARCHAR NOT NULL,                    -- drop|ingest|dq|publish|sink_pg|sink_s3
    status          VARCHAR NOT NULL,
    started_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    ended_at        TIMESTAMP,
    input_ref       TEXT,
    output_ref      TEXT,
    record_count_in  BIGINT,
    record_count_out BIGINT,
    metrics         JSONB,
    error           TEXT
);
CREATE INDEX ON pipeline.run_stage_log (run_id, stage);
```

### 4.5 `pipeline.reconciliation_log`
```sql
CREATE TABLE pipeline.reconciliation_log (
    id                BIGSERIAL PRIMARY KEY,
    check_type        VARCHAR NOT NULL,                  -- t0_publish_count | t2_source_kafka | t2_kafka_postgres
    run_id            UUID,
    domain            VARCHAR NOT NULL,
    dataset           VARCHAR NOT NULL,
    business_date     DATE,
    window_start      TIMESTAMP,
    window_end        TIMESTAMP,
    source_count      BIGINT,
    kafka_count       BIGINT,
    postgres_count    BIGINT,
    discrepancy_count BIGINT,
    discrepancy_pct   NUMERIC(8,4),
    status            VARCHAR NOT NULL,                  -- ok | warning | failed
    detail            TEXT,
    created_at        TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX ON pipeline.reconciliation_log (domain, dataset, check_type, created_at);
```

### 4.6 Compat view (kept until cutover stable)
```sql
CREATE VIEW pipeline.v_lineage AS
SELECT r.run_id, r.pipeline_type, r.domain, r.dataset, r.business_date,
       fc.s3_raw_path AS source_ref, r.kafka_topic, r.kafka_offset_start, r.kafka_offset_end,
       r.config_version_id, r.schema_version_id, r.parents, r.created_at
FROM pipeline.run_log r
LEFT JOIN pipeline.file_catalogue fc ON fc.file_id = r.file_id;
```

## 5. Airflow DAGs

### 5.1 `dag_drop_to_raw`
SFTPSensor poll → `register_file` (md5, idempotent insert into `file_catalogue`, state=`received`) → `upload_to_s3` → `TriggerDagRunOperator` → `dag_ingest`.

### 5.2 `dag_ingest`
Triggered per file. Generates `run_id`. Tasks:
1. `stage_ingest` — Spark `ods_ingestion.py`. Writes `run_stage_log(stage='ingest')` and updates `run_log.record_count_source`.
2. `stage_dq` — `dq.py`. Hard-block fails the run; failing rows written as Parquet to `s3://ods-dlq/...`.
3. `stage_publish` — `ods_s3_publish.py`, Avro via Confluent SR. Records `kafka_offset_start/end`. T0 check: `record_count_published == offset_end - offset_start`. On mismatch → `reconciliation_log(check_type='t0_publish_count', status='failed')` + run fails.
4. `stage_wait_sinks` — polls Connect REST `/connectors/{jdbc,s3}/status`; verifies sink offsets passed `kafka_offset_end` (timeout configurable per dataset).
5. `finalise` — `run_log.status='succeeded'`, `file_catalogue.state='sunk'`.

### 5.3 `dag_recon_t2` (cron `0 * * * *`)
For each `(domain, dataset)`: source = `SUM(record_count_published)` from `run_log` (today, succeeded); kafka = `endOffsets - beginningOffsets`; postgres = `count(*)` on target table for today. Two rows written: `t2_source_kafka`, `t2_kafka_postgres`. Tolerance from `dataset_config`.

### 5.4 `dag_config_sync`
Reads `datasets/<domain>/<dataset>.yaml`, computes SHA-256, UPSERTs `dataset_config`; bumps `config_version_id` on hash change.

## 6. Kafka Connect

Provisioned at compose start by an `airflow-init`-style bootstrap container that POSTs JSON configs to Connect REST.

- **JDBC sink** — `topics=ods.<domain>.<dataset>`, `connection.url=jdbc:postgresql://postgres:5432/ods`, `insert.mode=upsert`, `pk.mode=record_key`, `auto.create=false`, `auto.evolve=false`.
- **S3 sink** — `s3.bucket.name=ods-curated`, `format.class=ParquetFormat`, `partitioner.class=TimeBasedPartitioner`, `path.format='year'=YYYY/'month'=MM/'day'=dd`, `rotate.interval.ms=3600000`.

## 7. Repo Layout

```
datasets/                       # YAML config (source of truth)
docker/
  sftp/                          # users.conf
  kafka-connect/                 # Dockerfile + connector plugins
  connect-config/                # sink JSON configs
  grafana/provisioning/          # datasource + dashboards
db/migrations/
  03_consolidate_pipeline_tables.sql
  04_dataset_config_extensions.sql
airflow/dags/
  dag_drop_to_raw.py
  dag_ingest.py
  dag_recon_t2.py
  dag_config_sync.py
  common/run_log.py              # write helpers
  common/connect_admin.py
glue/jobs/                       # existing — adapted to new tables
tests/
  unit/                          # run_log helpers, T0 math, config hash
  integration/                   # 6 e2e scenarios listed §8
```

## 8. Testing

### 8.1 Unit
- Run-log + stage-log write helpers (transactional behaviour, idempotency).
- T0 count check math.
- YAML hash + `config_version_id` bump.
- DLQ Parquet row write for array-typed `_dq_fail_reason`.

### 8.2 Integration (compose up + service fixtures)
1. `test_e2e_sftp_to_postgres` — drop file in SFTP volume, assert row visible in `ods.insurance_policies` and S3 curated.
2. `test_idempotent_drop` — drop same file twice, single run, single row.
3. `test_dq_block_routes_to_dlq` — DQ-failing CSV → run failed, DLQ Parquet present, no Kafka publish.
4. `test_t0_mismatch_fails_run` — induce offset mismatch (mock); assert `reconciliation_log` row + run failed.
5. `test_t2_recon_ok_and_drift` — happy path + induced postgres drift; assert recon rows.
6. `test_sink_failure_blocks_run` — pause Connect; assert `stage_wait_sinks` times out, run partial, file state stays `published` not `sunk`.

### 8.3 Existing 9 E2E tests
Migrate to new tables. Must remain green throughout.

## 9. Migration

`03_consolidate_pipeline_tables.sql`:
1. Create `run_log`, `run_stage_log`, `reconciliation_log` (extend existing `file_catalogue`).
2. Backfill from `glue_job_log` + `lineage` + `*_file_state`.
3. Rename old tables `_deprecated_2026_04_28`.
4. Create compat view `pipeline.v_lineage`.
5. Drop deprecated in a follow-up after one stable week.

## 10. Open Items

| Item                                          | Default until decided                              |
|----------------------------------------------|---------------------------------------------------|
| Sink wait timeout per dataset                | 60s                                                |
| `business_date` extraction when not in path  | Derived from filename via regex in `dataset_config`|
| Topic auto-creation                          | Disabled — created by `dag_config_sync`            |
| JDBC PK source when message has no key       | Composite key declared in YAML                     |
| Grafana dashboard JSON                       | Hand-built first, exported to repo                 |
| Schema evolution policy                      | `auto.evolve=false` in v1; manual `ALTER TABLE`    |

## 11. Implementation Order

1. Migrations + new tables; backfill from existing tables; keep old tests green.
2. `dag_config_sync` + first `policies.yaml`.
3. SFTP container + `dag_drop_to_raw`.
4. Adapt `ods_ingestion.py` and `ods_s3_publish.py` to write new tables and use Confluent SR Avro.
5. T0 check in publish stage.
6. Redpanda + Schema Registry + Connect with JDBC sink + S3 sink; bootstrap container.
7. `dag_ingest` (orchestrating existing Glue jobs + sink wait).
8. `dag_recon_t2`.
9. Grafana datasource + dashboards.
10. Integration test suite (6 scenarios) + migrate existing 9 tests.
