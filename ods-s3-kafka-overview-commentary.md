# S3 → Kafka Pipeline — Step Commentary

---

## Phase 1 · Detect & Guard

**❶ Object Created event**
A curated Parquet file is written to `ods-curated-{env}`. S3 immediately emits an `Object Created` event to AWS EventBridge — no polling, no delay. The EventBridge rule `ods-curated-file-rule-{env}` matches events on the curated bucket prefix and forwards them to MWAA.

**❷ Trigger MWAA DAG**
EventBridge triggers the Airflow DAG directly via the MWAA REST API, passing the file path from the S3 event payload. Triggering is event-driven and near-instant — no polling loop.

**❸ Check file state (idempotency guard)**
The DAG immediately checks PostgreSQL `pipeline.file_state` using the file path as the key. If the file has already been processed (`status=completed`), the DAG exits immediately. This prevents duplicate Kafka publishes if EventBridge fires twice (S3 events can be delivered more than once) or a DAG is manually re-run.

---

## Phase 2 · Orchestrate

**❹ Load config — resolve pinned S3 version ID**
The DAG loads the YAML config from `ods-config-{env}`. Critically, it resolves the **current S3 object version ID** at this moment and pins it for the rest of the run. A config change deployed mid-flight cannot affect an in-progress job — the Glue job always uses the exact same config bytes the DAG loaded.

**❺ Set file state → processing**
PostgreSQL `pipeline.file_state` is updated to `status=processing`. If the pipeline crashes here and a retry fires, the idempotency check at ❸ will see `processing` and proceed — the guard only blocks `completed` files.

---

## Phase 3 · Glue Processing

**❻ Trigger Glue job**
The DAG triggers `ods-s3-publish-{dataset}`, passing the config reference and pinned S3 version ID. Glue receives everything it needs upfront — no further config lookups at runtime.

**❼ Read Parquet file → capture row count**
The Glue job reads the Parquet file from S3 and captures the source row count (e.g. 10,000 rows). Parquet row counts are cheap to read from footer metadata without scanning all data. This number is the ground truth for count reconciliation at the end of the job.

The Glue job immediately writes its first log entry to `pipeline.glue_job_log`:
```sql
INSERT INTO pipeline.glue_job_log
  (run_id, job_name, pipeline_type, domain, dataset,
   source_path, target_path, business_date, status)
VALUES
  ('abc-123', 'ods-s3-publish-policies', 'publish', 'insurance', 'policies',
   's3://ods-curated-prod/insurance/policies/policies_20260414.parquet',
   'ods.insurance.policies', '2026-04-14', 'started');
```

`business_date` is extracted from the filename using the pattern defined in the YAML config (e.g. `policies_{yyyyMMdd}.parquet`).

**❽ Failure reasons**
Three things can cause the job to fail — each writes a `failed` entry to `pipeline.glue_job_log` and routes data to the DLQ:

- **a) Schema incompatible** — breaking change in the Parquet schema (e.g. required field removed). Entire file routed to DLQ. Engineer resolves schema conflict and re-runs.
- **b) DQ hard block** — hard block rules failed. Dataset-level: entire job fails. Row-level: failing rows to DLQ, passing rows continue. Soft warn rules emit a CloudWatch metric and continue.
- **c) Count mismatch** — after publishing, Kafka partition offset delta ≠ source row count. Undelivered records routed to DLQ.

At each failure point the Glue job inserts:
```sql
INSERT INTO pipeline.glue_job_log
  (run_id, job_name, ..., status, error_reason, error_detail)
VALUES
  ('abc-123', 'ods-s3-publish-policies', ..., 'failed', 'schema_incompatible', '...');
```

**❾ Emit audit event (failed)**
A structured event is published to `ods.pipeline.audit` recording the failure reason, dataset, file path, and timestamp.

---

## Phase 4 · Post-Publish

**❿ Set file state → completed**
PostgreSQL `pipeline.file_state` updated to `status=completed`. Future EventBridge triggers for this file path will exit immediately at ❸.

**⓫ Register dataset in Glue Data Catalog**
The dataset is registered or updated in `ods_{domain}`. Discoverable via Athena and other AWS services.

**⓬ Trigger Glue Crawler (async · no wait)**
The crawler `ods-{dataset}-crawler` is triggered asynchronously. The DAG does not wait — crawler latency does not affect pipeline throughput.

**⓭ Emit audit event (success)**
A structured success event is published to `ods.pipeline.audit` with record count, schema version, target topic, and end-to-end latency. The S3 Sink Connector drains this topic to `ods-audit-sink-{env}` for long-term retention and Athena querying.

---

## PostgreSQL Tables

```sql
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

### Useful queries

```sql
-- Full execution history for a specific file
SELECT id, status, record_count, error_reason, created_at
FROM pipeline.glue_job_log
WHERE source_path = 's3://ods-curated-prod/insurance/policies/policies_20260414.parquet'
ORDER BY id;

-- All failures today
SELECT id, run_id, dataset, business_date, status, error_reason, created_at
FROM pipeline.glue_job_log
WHERE status = 'failed'
  AND created_at >= CURRENT_DATE
ORDER BY created_at DESC;

-- Record counts by business date for a dataset
SELECT business_date, SUM(record_count) AS total_records
FROM pipeline.glue_job_log
WHERE dataset = 'policies'
  AND status = 'completed'
GROUP BY business_date
ORDER BY business_date DESC;
```
