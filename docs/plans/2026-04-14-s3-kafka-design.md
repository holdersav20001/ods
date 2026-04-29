# S3 → Kafka Pipeline Design
**Date:** 2026-04-14  
**Status:** Approved  
**Pattern:** S3 Curated Zone → MSK (template for CDC, API, Event patterns)

---

## 1. Context

Aviva ODS is a multi-source Kafka ingestion platform with four source patterns. This document covers the **S3 → Kafka** pattern — batch Parquet files published from the S3 Curated Zone to MSK. It is designed as the **platform template**: the shared layer (Schema Registry, DLQ, idempotency, observability, audit) is defined here and reused across the remaining three patterns.

### Four ingestion patterns
| # | Pattern | Status |
|---|---|---|
| 1 | S3 Curated Zone → Kafka | This document |
| 2 | CDC → Kafka | Inherits shared layer |
| 3 | API → Kafka | Inherits shared layer |
| 4 | Event → Kafka | Inherits shared layer |

---

## 2. Architecture Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Orchestration | MWAA (Airflow) — Option C discrete tasks | Maximum observability, restartable at any step, maps to all 4 patterns |
| DAG trigger | AWS EventBridge (S3 Object Created) | Event-driven, near-instant, no polling overhead on MWAA workers |
| Schema Registry | AWS Glue Schema Registry only | Native Glue + MSK integration, no extra infrastructure |
| Schema evolution | Fully automated | Auto-register compatible changes; breaking changes → DLQ + notify |
| Data Quality | AWS Glue DQDL (hard block + soft warn) | Native Glue, no extra infrastructure, CloudWatch integration |
| Idempotency | 3-layer (PostgreSQL + Kafka transactions + message keys) | Each layer covers a different failure mode |
| State store | PostgreSQL | Atomic writes, queryable, handles concurrent file arrival safely |
| DLQ | S3 bucket | Simple, durable, partitioned by date/topic/run_id |
| Count reconciliation | Synchronous (source row count vs Kafka offset delta) | Batch pattern — row count known upfront, verify before marking success |
| Audit trail | Kafka topic `ods.pipeline.audit` | Event-driven, works uniformly across all 4 patterns, multiple consumers |
| Config versioning | S3 versioned objects, pin version ID at trigger time | Prevents mid-flight config changes affecting in-progress runs |
| Catalog crawler | Async trigger post-publish | Crawler latency must not affect pipeline throughput |
| Observability | CloudWatch metrics + alarms | Native AWS, no extra infrastructure |

---

## 3. Infrastructure & Naming Conventions

### S3 Buckets
```
ods-curated-{env}        # source Parquet files (read-only to pipeline)
ods-config-{env}         # YAML configs (S3 versioning enabled)
ods-dlq-{env}            # failed records (partitioned by date/topic/run_id)
ods-audit-sink-{env}     # Kafka audit topic sinked via S3 Connector
ods-dq-results-{env}     # Glue Data Quality rule results
```

### MSK Topics
```
ods.{domain}.{dataset}         # e.g. ods.insurance.policies
ods.pipeline.audit             # all 4 patterns emit here
```

### PostgreSQL
```
Database:  ods_{env}
Schema:    pipeline
Tables:    file_state         — coarse-grained idempotency state per file
           glue_job_log       — fine-grained immutable job execution audit log

-- Glue job execution audit log (INSERT only — never update)
-- business_date extracted from filename using pattern in YAML config
CREATE TABLE pipeline.glue_job_log (
    id            BIGSERIAL PRIMARY KEY,
    run_id        UUID NOT NULL,
    job_name      VARCHAR NOT NULL,           -- e.g. ods-s3-publish-policies
    pipeline_type VARCHAR NOT NULL,           -- publish
    domain        VARCHAR NOT NULL,
    dataset       VARCHAR NOT NULL,
    source_path   VARCHAR,                    -- S3 Curated path
    target_path   VARCHAR,                    -- MSK topic
    business_date DATE,                       -- extracted from filename
    status        VARCHAR NOT NULL,           -- started | schema_validated | dq_passed | dq_warned | publishing | completed | failed
    record_count  INTEGER,
    error_reason  VARCHAR,
    error_detail  TEXT,
    created_at    TIMESTAMP DEFAULT NOW()
);

-- Coarse-grained idempotency state per file
CREATE TABLE pipeline.file_state (
    id          SERIAL PRIMARY KEY,
    s3_path     VARCHAR NOT NULL UNIQUE,
    run_id      UUID NOT NULL,
    status      VARCHAR NOT NULL,  -- new | processing | completed | failed
    record_count INTEGER,
    error_reason VARCHAR,
    created_at  TIMESTAMP DEFAULT NOW(),
    updated_at  TIMESTAMP DEFAULT NOW()
);
```

### AWS Glue
```
JDBC Connection:  ods-postgres-{env}           # Glue connection to PostgreSQL for job log writes
Schema Registry:  ods-schema-registry-{env}
Data Catalog DB:  ods_{domain}              # e.g. ods_insurance
Crawlers:         ods-{dataset}-crawler
Glue Jobs:        ods-s3-publish-{dataset}
```

### AWS EventBridge
```
Rules:  ods-curated-file-rule-{env}    # triggers publish pipeline DAG on S3 Curated Object Created
```

### CloudWatch
```
Log Groups:  /ods/{env}/airflow
             /ods/{env}/glue
Namespace:   ods/{env}
Alarms:      ods-dlq-records-{env}
             ods-schema-failure-{env}
             ods-job-failure-{env}
             ods-dq-hard-failure-{env}
             ods-count-mismatch-{env}
```

### Environments
`dev` | `staging` | `prod`

---

## 4. Data Flow

See companion diagrams:
- `ods-s3-kafka-overview-simple.mmd` — 4-phase overview (no CloudWatch)
- `ods-s3-kafka-overview-sequence.mmd` — full overview including CloudWatch
- `ods-s3-kafka-glue-detail.mmd` — Glue processing internals
- `ods-s3-kafka-overview-commentary.md` — step-by-step commentary

### Four phases

**Phase 1 · Detect & Guard**  
S3 emits an `Object Created` event to AWS EventBridge when a Parquet file lands in `ods-curated-{env}`. EventBridge rule `ods-curated-file-rule-{env}` triggers the MWAA DAG immediately — no polling. DAG checks PostgreSQL idempotency on entry — if `status=completed` the run exits immediately.

**Phase 2 · Orchestrate**  
Airflow DAG loads versioned config from S3 (version ID pinned at trigger time), sets file state to `processing` in PostgreSQL.

**Phase 3 · Glue Processing**  
Glue job reads Parquet file, captures source row count, then runs three sequential steps:
1. **Schema Validation** — validate against Glue Schema Registry (pinned schema ID). Compatible changes auto-register a new schema version. Breaking changes route the file to DLQ.
2. **Data Quality** — DQDL rules evaluated per row. Hard block failures route rows to DLQ. Soft warns emit CloudWatch metric and continue.
3. **Publish** — deterministic message keys generated (hash of key fields). Records published to MSK with Kafka transactions and `acks=all`. Post-commit, Kafka partition offset delta verified against source row count. Mismatch → undelivered records to DLQ.

**Phase 4 · Post-Publish**  
File state set to `completed`. Dataset registered in Glue Data Catalog. Glue Crawler triggered asynchronously. Audit event emitted to `ods.pipeline.audit`.

---

## 5. Idempotency — Three Layers

| Layer | Mechanism | Failure mode covered |
|---|---|---|
| 1 — File level | PostgreSQL state (`processing` / `completed`) | Prevents double-triggering at DAG level |
| 2 — Job level | Kafka transactions (atomic publish per batch) | Prevents partial publish on Glue crash |
| 3 — Consumer level | Deterministic message keys (hash of key fields) | Consumer-side deduplication safety net |

---

## 6. Data Quality

Rules are defined in DQDL and stored in the versioned YAML config per dataset. Two rule severities:

- **Hard block** — row fails → row routed to DLQ, pipeline continues with remaining rows. Dataset-level hard blocks (e.g. `RowCount`) fail the entire job.
- **Soft warn** — row fails → CloudWatch metric emitted, row continues to Kafka.

```
Rules = [
    Completeness "policy_id" >= 1.0,           # hard block
    IsUnique "policy_id",                       # hard block
    RowCount >= 1,                              # hard block (dataset-level)
    Completeness "phone_number" >= 0.8,         # soft warn
    ColumnValues "premium" between 0 and 1000000  # soft warn
]
```

DQ results published to `ods-dq-results-{env}` and CloudWatch.

---

## 7. Observability

### CloudWatch Metrics (Namespace: `ods/{env}`)
| Metric | Emitted by | Alarm |
|---|---|---|
| `dag.triggered` | Airflow DAG | — |
| `glue.job.started` | Airflow DAG | — |
| `schema.evolved` | Glue Job | — |
| `schema.incompatible` | Glue Job | Yes |
| `dq.hard.failure` | Glue Job | Yes |
| `dq.soft.warning` | Glue Job | — |
| `publish.success` | Glue Job | — |
| `publish.count.mismatch` | Glue Job | Yes |
| `glue.job.completed` | Airflow DAG | — |
| `job.failed` | Airflow DAG | Yes |
| `catalog.registered` | Airflow DAG | — |

### Audit Topic Schema (`ods.pipeline.audit`)
```json
{
  "run_id": "uuid",
  "source_type": "s3",
  "source_ref": "s3://ods-curated-prod/insurance/policies/file.parquet",
  "target_topic": "ods.insurance.policies",
  "schema_version": "3",
  "record_count": 10000,
  "status": "success | failed | dlq",
  "reason": "schema_incompatible | dq_hard_block | count_mismatch | null",
  "timestamp": "2026-04-14T13:00:00Z"
}
```

Audit topic sinked to `ods-audit-sink-{env}` via Kafka Connect S3 Sink Connector for long-term retention and Athena querying.

---

## 8. Restartability

| Failure point | Recovery behaviour |
|---|---|
| DAG fails before Glue trigger | PostgreSQL state=processing — DAG retry re-runs from Phase 2 |
| Glue job fails mid-publish | Kafka transaction aborted — no partial data in topic. DAG retry re-triggers Glue from scratch |
| Count mismatch | Undelivered records in DLQ — engineer replays from DLQ after investigation |
| DQ hard block | Failing rows in DLQ — fix upstream data, replay from DLQ |
| Schema incompatible | File in DLQ — resolve schema conflict, re-run DAG with updated config |

Glue Job Bookmarks enabled to prevent S3 source re-reads on job retry.

---

## 9. YAML Config Structure

```yaml
# ods-config-{env}/insurance/policies.yaml
dataset:
  domain: insurance
  name: policies
  source_path: s3://ods-curated-{env}/insurance/policies/
  target_topic: ods.insurance.policies
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

## 10. Out of Scope

- Consumer-side processing (downstream of `ods.{domain}.{dataset}`)
- CDC, API, and Event ingestion patterns (separate design docs — inherit shared layer from this document)
- Kafka Connect S3 Sink Connector configuration (operational concern)
- MWAA environment setup and IAM roles
