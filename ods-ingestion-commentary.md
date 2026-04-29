# Ingestion Pipeline — Step Commentary

---

## Phase 1 · Detect & Validate

**❶ Poll for new CSV files**
The Airflow SFTP Sensor connects to the internal SFTP server every 5 minutes and lists files at the configured paths. The poll interval is configurable per dataset in the YAML config. Requires network connectivity from MWAA to the internal SFTP — connectivity method is an open infrastructure decision.

**❷ New file detected**
The sensor identifies files not previously seen by comparing against `pipeline.ingestion_file_state`. The filename and SFTP path are captured for the next step.

**❸ Check file_catalogue — is this file approved?**
Before doing any work, the sensor queries `pipeline.file_catalogue` in PostgreSQL. This is the approved file whitelist — it stores naming patterns, expected SFTP paths, domain, and dataset mappings. A file is approved if its filename matches a pattern entry AND its SFTP path matches the registered location.

This guard prevents rogue or unexpected files from entering the pipeline. Without it, any file placed on the SFTP — accidental, malicious, or misconfigured — would be ingested.

**❹ Quarantine unapproved file**
If no catalogue match is found, the file details are written to `ods-quarantine-{env}` and a CloudWatch alarm fires. The file is NOT copied — it stays on the SFTP until investigated. An engineer reviews the quarantine log and either adds the file to the catalogue (if legitimate) or escalates.

**❺ Check ingestion_file_state — already processed?**
If approved, the sensor checks `pipeline.ingestion_file_state` using the SFTP file path as the key. If `status=completed`, the pipeline exits immediately — idempotency guard prevents duplicate processing if the sensor fires on an already-processed file.

**❻ Trigger DAG run**
The sensor hands off the file path and catalogue reference to the Airflow DAG. Each DAG run is scoped to a single file.

**❼ Set file state → detected**
PostgreSQL `pipeline.ingestion_file_state` is updated to `status=detected`. The file is now tracked through the pipeline.

---

## Phase 2 · Transfer

**❽ SFTPToS3Operator — copy CSV to S3 Raw**
The Airflow `SFTPToS3Operator` connects to the internal SFTP and copies the CSV file to `ods-raw-{env}/{domain}/{dataset}/date={date}/{filename}.csv`. The Raw Zone is a **permanent archive** — files are never deleted. This gives a complete audit trail of every file ever ingested, enabling full replay if needed.

**❾ Verify MD5 checksum**
After the copy, the pipeline computes the MD5 hash of the S3 object and compares it against the MD5 of the source file on the SFTP. If they differ, the file was corrupted in transit.

A checksum mismatch routes the corrupted S3 copy to `ods-quarantine-{env}`, sets file state to `failed`, and fires a CloudWatch alarm. The original file remains on the SFTP untouched. An engineer investigates and can retry the transfer.

**❿ Set file state → transferred**
PostgreSQL updated to `status=transferred`. The CSV is now safely in S3 Raw. DAG 1 completes here.

---

## Phase 3 · Glue ETL

**⓫ Object Created event → EventBridge**
When the CSV file lands in `ods-raw-{env}`, S3 immediately emits an `Object Created` event to AWS EventBridge. The rule `ods-raw-file-rule-{env}` matches the event and triggers DAG 2 via the MWAA REST API, passing the S3 Raw file path from the event payload. This is the handoff point between DAG 1 (transfer) and DAG 2 (ETL) — the two are fully decoupled. If ETL fails and needs retrying, DAG 1 is unaffected.

**⓬ Trigger DAG 2 — ETL**
DAG 2 starts with the S3 Raw file path from the EventBridge payload. Each DAG 2 run is scoped to a single file.

**⓭ Load config — resolve pinned S3 version ID**
DAG 2 loads the YAML config from `ods-config-{env}`, resolving the **current S3 object version ID** at this moment and pinning it for the rest of the run. The Glue job uses this exact config version — a config change deployed mid-flight cannot affect an in-progress run.

**⓮ Trigger Glue ETL job**
The DAG triggers `ods-ingestion-{dataset}`, passing the config ref and pinned S3 version ID. The Glue job reads the CSV from S3 Raw, captures the source row count, and extracts `business_date` from the filename using the pattern defined in the YAML config (e.g. `policies_{yyyyMMdd}.csv`).

The Glue job immediately writes its first log entry to `pipeline.glue_job_log`:
```sql
INSERT INTO pipeline.glue_job_log
  (run_id, job_name, pipeline_type, domain, dataset,
   source_path, target_path, business_date, status)
VALUES
  ('abc-123', 'ods-ingestion-policies', 'ingestion', 'insurance', 'policies',
   's3://ods-raw-prod/insurance/policies/date=2026-04-14/policies_20260414.csv',
   's3://ods-curated-prod/insurance/policies/', '2026-04-14', 'started');
```

The three steps inside the Glue job are:

- **Schema Validation** — validate CSV columns against the registered schema in Glue Schema Registry. Compatible changes auto-register a new version. Breaking changes route the file to DLQ.
- **Data Quality** — DQDL rules evaluated. Dataset-level hard blocks abort the job. Row-level hard blocks route failing rows to DLQ; passing rows continue. Soft warns emit a CloudWatch metric.
- **Convert & Write** — CSV rows converted to Parquet (type casting, null handling, encoding). Written to `ods-curated-{env}` partitioned by `date={date}/dataset={dataset}/`. Record count verified against source row count before job completes.

At each failure point the Glue job inserts a `failed` status entry:
```sql
INSERT INTO pipeline.glue_job_log
  (run_id, job_name, ..., status, error_reason, error_detail)
VALUES
  ('abc-123', 'ods-ingestion-policies', ..., 'failed', 'schema_incompatible', '...');
```

**⓯ Emit audit event (failed)**
If the Glue job fails for any reason, a structured event is published to `ods.pipeline.audit` with the failure reason, dataset, file path, and timestamp.

---

## Phase 4 · Post-ETL

**⓰ Set file state → completed**
PostgreSQL `pipeline.ingestion_file_state` updated to `status=completed`. Future sensor polls for this file path will be skipped at ❺.

**⓱ Register dataset in Glue Data Catalog**
The dataset is registered or updated in `ods_{domain}`. This makes the curated Parquet discoverable via Athena and other AWS services.

**⓲ Trigger Glue Crawler (async · no wait)**
The crawler `ods-{dataset}-crawler` is triggered asynchronously to update partition metadata. The DAG does not wait for crawler completion — crawler latency does not affect pipeline throughput.

**⓳ Emit audit event (success)**
A structured success event is published to `ods.pipeline.audit` with record count, dataset, and end-to-end latency.

**⓴ Publish pipeline triggered**
The Parquet file landing in `ods-curated-{env}` automatically fires the EventBridge rule `ods-curated-file-rule-{env}` — the two pipelines are joined at the S3 Curated Zone. No explicit handoff is needed.

---

## PostgreSQL Tables

```sql
-- Approved file whitelist
CREATE TABLE pipeline.file_catalogue (
    id              SERIAL PRIMARY KEY,
    name_pattern    VARCHAR NOT NULL,       -- regex or glob pattern, e.g. policies_*.csv
    sftp_path       VARCHAR NOT NULL,       -- e.g. /outbound/insurance/policies/
    domain          VARCHAR NOT NULL,       -- e.g. insurance
    dataset         VARCHAR NOT NULL,       -- e.g. policies
    config_ref      VARCHAR NOT NULL,       -- S3 config path
    active          BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMP DEFAULT NOW()
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
```

### Useful queries

```sql
-- Full execution history for a specific file
SELECT id, status, record_count, error_reason, created_at
FROM pipeline.glue_job_log
WHERE source_path = 's3://ods-raw-prod/insurance/policies/date=2026-04-14/policies_20260414.csv'
ORDER BY id;

-- All ingestion failures today
SELECT id, run_id, dataset, business_date, status, error_reason, created_at
FROM pipeline.glue_job_log
WHERE pipeline_type = 'ingestion'
  AND status = 'failed'
  AND created_at >= CURRENT_DATE
ORDER BY created_at DESC;

-- Record counts by business date for a dataset
SELECT business_date, SUM(record_count) AS total_records
FROM pipeline.glue_job_log
WHERE dataset = 'policies'
  AND pipeline_type = 'ingestion'
  AND status = 'completed'
GROUP BY business_date
ORDER BY business_date DESC;
```
