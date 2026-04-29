# Ingestion Pipeline Design
**Date:** 2026-04-14  
**Status:** Approved  
**Pattern:** SFTP → S3 Raw → Glue ETL → S3 Curated (feeds S3→Kafka publish pipeline)

---

## 1. Context

The ingestion pipeline is the upstream of the ODS platform. It is responsible for moving CSV files from an internal SFTP server into the AWS data lake, converting them to Parquet, and landing them in the S3 Curated Zone where the Publish Pipeline takes over.

```
[Ingestion Pipeline]                         [Publish Pipeline]
SFTP → S3 Raw → Glue ETL → S3 Curated  ───► Airflow Sensor → Glue → MSK
                                   ▲
                            handoff point
```

The two pipelines are joined at the S3 Curated Zone. No explicit handoff is needed — the Publish Pipeline's S3 Sensor fires automatically when a new Parquet file lands.

---

## 2. Open Items

| Item | Status | Owner |
|---|---|---|
| SFTP → AWS network connectivity | **Unresolved** | Infrastructure team |
| ETL engine decision (Glue confirmed as likely) | Pending confirmation | — |

---

## 3. Architecture Decisions

| Decision | Choice | Rationale |
|---|---|---|
| SFTP transfer | Airflow SFTPToS3Operator (DAG 1) | Agreed. Requires network connectivity from MWAA to internal SFTP (open item) |
| ETL trigger | AWS EventBridge (S3 Raw Object Created) | Decouples transfer from ETL — DAG 2 fires event-driven when file lands in S3 Raw |
| File approval | PostgreSQL `file_catalogue` whitelist | Config-driven, queryable, consistent with platform PostgreSQL pattern |
| Idempotency | PostgreSQL `ingestion_file_state` | Same pattern as publish pipeline — atomic, queryable, handles concurrent arrivals |
| File integrity | MD5 checksum post-transfer | Catches corruption in transit before ETL runs |
| Raw Zone | Permanent archive (`ods-raw-{env}`) | Full audit trail, enables complete replay |
| ETL engine | AWS Glue (likely) | Consistent with publish pipeline, serverless, native AWS |
| Schema validation | Glue Schema Registry | Same registry as publish pipeline — shared schema governance |
| Data Quality | Glue DQDL (hard block + soft warn) | Consistent with publish pipeline |
| Parquet partitioning | `date={date}/dataset={dataset}/` | Enables efficient downstream queries and partition pruning |
| Unapproved files | Quarantine to `ods-quarantine-{env}` | Explicit quarantine path — no silent drops |
| Observability | CloudWatch metrics + alarms | Consistent with publish pipeline |
| Audit trail | `ods.pipeline.audit` Kafka topic | Same shared audit topic as publish pipeline |

---

## 4. Infrastructure & Naming Conventions

### S3 Buckets
```
ods-raw-{env}           # permanent archive of all ingested CSV files
                        # path: {domain}/{dataset}/date={date}/{filename}.csv
ods-curated-{env}       # Parquet output (shared with publish pipeline)
                        # path: {domain}/{dataset}/date={date}/
ods-config-{env}        # YAML configs (S3 versioning enabled — shared)
ods-quarantine-{env}    # unapproved or corrupt files pending investigation
ods-dlq-{env}           # failed records from schema/DQ/conversion failures
ods-dq-results-{env}    # Glue Data Quality rule results
ods-audit-sink-{env}    # Kafka audit topic sinked via S3 Connector
```

### PostgreSQL (`ods_{env}`)
```sql
-- Approved file whitelist
CREATE TABLE pipeline.file_catalogue (
    id              SERIAL PRIMARY KEY,
    name_pattern    VARCHAR NOT NULL,       -- e.g. policies_*.csv
    sftp_path       VARCHAR NOT NULL,       -- e.g. /outbound/insurance/policies/
    domain          VARCHAR NOT NULL,
    dataset         VARCHAR NOT NULL,
    config_ref      VARCHAR NOT NULL,       -- S3 config path
    active          BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMP DEFAULT NOW()
);

-- Glue job execution audit log (INSERT only — never update)
-- business_date extracted from filename using pattern in YAML config
CREATE TABLE pipeline.glue_job_log (
    id            BIGSERIAL PRIMARY KEY,
    run_id        UUID NOT NULL,
    job_name      VARCHAR NOT NULL,           -- e.g. ods-ingestion-policies
    pipeline_type VARCHAR NOT NULL,           -- ingestion
    domain        VARCHAR NOT NULL,
    dataset       VARCHAR NOT NULL,
    source_path   VARCHAR,                    -- S3 Raw path
    target_path   VARCHAR,                    -- S3 Curated path
    business_date DATE,                       -- extracted from filename
    status        VARCHAR NOT NULL,           -- started | schema_validated | dq_passed | dq_warned | converting | completed | failed
    record_count  INTEGER,
    error_reason  VARCHAR,
    error_detail  TEXT,
    created_at    TIMESTAMP DEFAULT NOW()
);

-- Per-file processing state
CREATE TABLE pipeline.ingestion_file_state (
    id              SERIAL PRIMARY KEY,
    sftp_path       VARCHAR NOT NULL UNIQUE,
    run_id          UUID NOT NULL,
    status          VARCHAR NOT NULL,  -- detected | transferred | etl_processing | completed | failed
    record_count    INTEGER,
    error_reason    VARCHAR,
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);
```

### AWS EventBridge
```
Rules:  ods-raw-file-rule-{env}       # triggers DAG 2 on S3 Raw Object Created
        ods-curated-file-rule-{env}   # triggers publish pipeline on S3 Curated Object Created (shared)
```

### AWS Glue
```
JDBC Connection:  ods-postgres-{env}           # Glue connection to PostgreSQL for job log writes (shared)
Schema Registry:  ods-schema-registry-{env}    # shared with publish pipeline
Data Catalog DB:  ods_{domain}                 # shared with publish pipeline
Crawlers:         ods-{dataset}-crawler        # shared with publish pipeline
Glue Jobs:        ods-ingestion-{dataset}      # e.g. ods-ingestion-policies
```

### CloudWatch
```
Log Groups:  /ods/{env}/airflow
             /ods/{env}/glue
Namespace:   ods/{env}
Alarms:      ods-file-not-approved-{env}
             ods-checksum-mismatch-{env}
             ods-schema-failure-{env}
             ods-dq-hard-failure-{env}
             ods-write-count-mismatch-{env}
             ods-job-failure-{env}
```

### Environments
`dev` | `staging` | `prod`

---

## 5. Data Flow

See companion diagrams:
- `ods-ingestion-overview.mmd` — flowchart overview
- `ods-ingestion-overview-simple.mmd` — numbered sequence (no CloudWatch)
- `ods-ingestion-overview-sequence.mmd` — full sequence with CloudWatch
- `ods-ingestion-glue-detail.mmd` — Glue ETL internals
- `ods-ingestion-commentary.md` — step-by-step commentary

### Four phases

**Phase 1 · Detect & Validate**
MWAA SFTP Sensor polls every 5 minutes. On new file: (1) check `file_catalogue` whitelist — not approved → quarantine + alarm; (2) check `ingestion_file_state` — already processed → skip; (3) trigger DAG, set state to `detected`.

**Phase 2 · Transfer (DAG 1)**
`SFTPToS3Operator` copies CSV to `ods-raw-{env}` (permanent archive). MD5 checksum verified post-copy — mismatch → quarantine + alarm. State set to `transferred`. DAG 1 completes here.

**EventBridge Handoff**
S3 Raw emits `Object Created` event → EventBridge rule `ods-raw-file-rule-{env}` → triggers DAG 2. Transfer and ETL are fully decoupled — if ETL fails, DAG 1 does not retry unnecessarily.

**Phase 3 · Glue ETL (DAG 2)**
Glue job reads CSV from S3 Raw, loads pinned YAML config, runs:
1. **Schema Validation** — validate against Glue Schema Registry. Compatible changes auto-register. Breaking changes → DLQ.
2. **Data Quality** — DQDL rules. Dataset-level hard block → abort job. Row-level hard block → failing rows to DLQ, passing rows continue. Soft warn → CloudWatch metric.
3. **Convert & Write** — CSV to Parquet, write to `ods-curated-{env}` partitioned by date. Record count verified against source.

**Phase 4 · Post-ETL**
Dataset registered in Glue Data Catalog. Crawler triggered async. File state set to `completed`. Audit event emitted to `ods.pipeline.audit`. Parquet landing in S3 Curated automatically triggers the Publish Pipeline.

---

## 6. Idempotency

| Layer | Mechanism | Failure mode covered |
|---|---|---|
| 1 — File catalogue | PostgreSQL whitelist check | Prevents unapproved files entering pipeline |
| 2 — File state | PostgreSQL `ingestion_file_state` | Prevents duplicate processing of same file |
| 3 — Checksum | MD5 post-transfer verification | Detects file corruption in transit |

---

## 7. Data Quality

Same DQDL approach as the publish pipeline. Rules stored per dataset in versioned YAML config.

```
Rules = [
    RowCount >= 1,                              # hard block (dataset-level)
    Completeness "policy_id" >= 1.0,            # hard block (row-level)
    IsUnique "policy_id",                       # hard block (row-level)
    Completeness "phone_number" >= 0.8,         # soft warn
    ColumnValues "premium" between 0 and 1000000  # soft warn
]
```

DQ results published to `ods-dq-results-{env}` and CloudWatch.

---

## 8. Observability

### CloudWatch Metrics (Namespace: `ods/{env}`)
| Metric | Emitted by | Alarm |
|---|---|---|
| `file.not.approved` | Airflow Sensor | Yes |
| `dag.triggered` | Airflow DAG | — |
| `file.transferred` | Airflow DAG | — |
| `checksum.verified` | Airflow DAG | — |
| `checksum.mismatch` | Airflow DAG | Yes |
| `glue.job.started` | Airflow DAG | — |
| `schema.evolved` | Glue Job | — |
| `schema.incompatible` | Glue Job | Yes |
| `dq.hard.failure` | Glue Job | Yes |
| `dq.soft.warning` | Glue Job | — |
| `write.count.mismatch` | Glue Job | Yes |
| `etl.success` | Glue Job | — |
| `pipeline.completed` | Airflow DAG | — |
| `job.failed` | Airflow DAG | Yes |

### Audit Topic Schema (`ods.pipeline.audit`)
```json
{
  "run_id": "uuid",
  "source_type": "sftp",
  "source_ref": "/outbound/insurance/policies/policies_2026-04-14.csv",
  "sftp_host": "internal-sftp.company.com",
  "target_path": "s3://ods-curated-prod/insurance/policies/date=2026-04-14/",
  "record_count": 10000,
  "status": "success | failed | quarantine",
  "reason": "not_approved | checksum_mismatch | schema_incompatible | dq_hard_block | count_mismatch | null",
  "timestamp": "2026-04-14T13:00:00Z"
}
```

---

## 9. YAML Config Structure

```yaml
# ods-config-{env}/insurance/policies.yaml
dataset:
  domain: insurance
  name: policies
  sftp_path: /outbound/insurance/policies/
  sftp_filename_pattern: "policies_*.csv"
  poll_interval_minutes: 5
  source_format: csv
  source_encoding: utf-8
  has_header: true
  raw_path: s3://ods-raw-{env}/insurance/policies/
  curated_path: s3://ods-curated-{env}/insurance/policies/
  schema_id: ods-schema-registry-{env}/insurance-policies
  key_fields:
    - policy_id
  dq_rules_ref: s3://ods-config-{env}/dq-rules/policies.dqdl
  catalog:
    database: ods_insurance
    table: policies
    crawler: ods-policies-crawler
```

---

## 10. Shared Platform Components

The following components are shared between the ingestion and publish pipelines:

| Component | Shared resource |
|---|---|
| PostgreSQL | Same `ods_{env}` database, different tables |
| EventBridge | `ods-curated-file-rule-{env}` shared — ingestion writes to curated, publish pipeline consumes |
| Schema Registry | `ods-schema-registry-{env}` |
| Glue Data Catalog | `ods_{domain}` |
| Glue Crawlers | `ods-{dataset}-crawler` |
| Audit topic | `ods.pipeline.audit` |
| CloudWatch namespace | `ods/{env}` |
| DLQ | `ods-dlq-{env}` |
| Config bucket | `ods-config-{env}` |

---

## 11. Out of Scope

- SFTP → AWS network connectivity (infrastructure team dependency)
- Upstream file generation (owned by third party)
- Consumer-side processing (downstream of `ods.{domain}.{dataset}`)
- CDC, API, and Event ingestion patterns
