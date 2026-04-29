# Ingestion Pipeline — Failures, Recovery & Resubmission

---

## 1. Failure Scenarios

### 1.1 File Not Approved (Catalogue Mismatch)

**What happens:**
A file lands on the SFTP but its filename or path does not match any active entry in `pipeline.file_catalogue`.

**Pipeline behaviour:**
- File details logged to `ods-quarantine-{env}/not-approved/date={date}/`
- CloudWatch alarm fires: `ods-file-not-approved-{env}`
- Audit event emitted: `status=quarantine · reason=not_approved`
- File is NOT copied from SFTP — it stays on the SFTP server

**Common causes:**
- Upstream system changed the filename format without notifying the data team
- New dataset not yet registered in the catalogue
- File placed in wrong SFTP directory

**Recovery:**
1. Check the quarantine log to identify the file
2. Either add a new catalogue entry (if legitimate) or notify the upstream team to fix the filename/path
3. Once the catalogue is updated, the next sensor poll will pick up the file if it is still on the SFTP

---

### 1.2 Duplicate File Arrival

**What happens:**
The same SFTP file path is detected again after it has already been processed (`status=completed`).

**Pipeline behaviour:**
- Sensor checks `pipeline.ingestion_file_state` — sees `status=completed`
- DAG exits immediately — no transfer, no ETL, no Kafka publish
- No alarm fires (this is expected behaviour)

**Common causes:**
- SFTP sensor fired twice on the same file
- Upstream system re-placed the same file without changing the path

**Note:** If the upstream system places a corrected file at the same SFTP path, the idempotency guard will block it. Use the resubmission procedure in Section 3.3 to force reprocessing.

---

### 1.3 Checksum Mismatch

**What happens:**
The MD5 hash of the file copied to S3 Raw does not match the MD5 of the source file on the SFTP.

**Pipeline behaviour:**
- Corrupt S3 copy moved to `ods-quarantine-{env}/checksum-mismatch/date={date}/`
- CloudWatch alarm fires: `ods-checksum-mismatch-{env}`
- File state set to `failed` with `error_reason=checksum_mismatch`
- Audit event emitted: `status=failed · reason=checksum_mismatch`
- Original file remains on SFTP untouched

**Recovery:**
1. Investigate: network instability during transfer, S3 write error, or corrupted source file
2. If the source file is intact on SFTP, reset file state (Section 3.2) and retry — the pipeline will re-copy
3. If the source file itself is corrupt, notify the upstream team to regenerate

---

### 1.4 Schema Incompatibility

**What happens:**
The CSV file has a structure that does not match the registered schema — e.g. a required column is missing, a column has been renamed, or a data type is incompatible.

**Pipeline behaviour:**
- File routed to `ods-dlq-{env}/schema-incompatible/date={date}/dataset={dataset}/`
- CloudWatch alarm fires: `ods-schema-failure-{env}`
- File state set to `failed` with `error_reason=schema_incompatible`
- No Parquet written to S3 Curated — publish pipeline is not triggered

**Compatible changes (auto-evolved — no failure):**
- New optional column added to the CSV
- Column order changed (schema maps by name, not position)

**Incompatible changes (causes failure):**
- Required column removed or renamed
- Column type changed incompatibly (e.g. numeric → string)
- New required column with no default value

**Recovery:**
1. Inspect the DLQ file to understand the structural difference
2. Update the schema in Glue Schema Registry and YAML config
3. Reset file state and resubmit (Section 3.2)

---

### 1.5 Data Quality Hard Block

**Two sub-cases — same behaviour as the publish pipeline:**

**a) Dataset-level hard block** (e.g. `RowCount >= 1`):
- Entire file fails — no Parquet written
- File routed to `ods-dlq-{env}/dq-dataset-failure/`
- Job fails, CloudWatch alarm fires

**b) Row-level hard block** (e.g. `Completeness "policy_id" >= 1.0`):
- Failing rows written to `ods-dlq-{env}/dq-row-failure/date={date}/dataset={dataset}/run_id={run_id}/`
- Passing rows continue to Phase 3 (convert + write)
- Job completes with partial write — CloudWatch alarm fires
- Publish pipeline is triggered for the partial Parquet (passing rows only)

**Recovery:**
- Fix the upstream data or adjust the DQ rule threshold with approval
- For row-level failures: replay only the failed rows from DLQ (Section 3.4)

---

### 1.6 CSV → Parquet Conversion Failure

**What happens:**
A record cannot be converted to Parquet — e.g. a value cannot be cast to the target type (`"N/A"` in a numeric column), encoding error, or malformed CSV row.

**Pipeline behaviour:**
- Failing rows written to DLQ with reason and row number
- Passing rows continue
- CloudWatch metric emitted: `dq.hard.failure`

**Recovery:**
1. Inspect DLQ rows — identify conversion error
2. Fix data in the DLQ file, resubmit as a corrected partial file (Section 3.4)

---

### 1.7 Write Count Mismatch

**What happens:**
After writing Parquet to S3 Curated, Glue verifies that the written record count equals the source row count from the CSV. If they differ, some records were lost during conversion or write.

**Pipeline behaviour:**
- Unwritten records routed to DLQ: `ods-dlq-{env}/count-mismatch/`
- CloudWatch alarm fires: `ods-write-count-mismatch-{env}`
- File state set to `failed`

---

### 1.8 Glue Job Crash (Mid-ETL)

**What happens:**
The Glue job fails unexpectedly mid-execution (e.g. out of memory, worker lost, timeout).

**Pipeline behaviour:**
- Any partial Parquet written to S3 Curated is incomplete — may trigger the EventBridge rule `ods-curated-file-rule-{env}` and the Publish Pipeline prematurely
- MWAA (DAG 2) detects job failure, sets file state to `failed`
- MWAA retries the DAG 2 task automatically (configurable retry count)

**Important:** If a partial Parquet landed in S3 Curated before the crash, EventBridge may have already triggered the Publish Pipeline. Because Kafka message keys are deterministic, a full re-publish after recovery will safely overwrite any partially published records.

**Recovery:**
1. Ensure partial Parquet files are removed from S3 Curated before retrying (or accept overwrite at publish layer)
2. Reset file state to `transferred` (not `new`) to skip the SFTP copy step
3. Retry Glue job

---

### 1.9 SFTP Transfer Failure

**What happens:**
The `SFTPToS3Operator` fails to copy the file — network timeout, SFTP connection dropped, S3 write error.

**Pipeline behaviour:**
- MWAA retries the task automatically (configurable)
- File state remains `detected`
- If all retries exhausted, file state set to `failed`

**Recovery:**
- Check network connectivity to SFTP
- Reset file state to `detected` and retry DAG

---

## 2. Failure → DLQ Location Reference

| Failure type | DLQ location |
|---|---|
| Unapproved file | `ods-quarantine-{env}/not-approved/date={date}/` |
| Checksum mismatch | `ods-quarantine-{env}/checksum-mismatch/date={date}/` |
| Schema incompatible | `ods-dlq-{env}/schema-incompatible/date={date}/dataset={dataset}/` |
| DQ dataset-level fail | `ods-dlq-{env}/dq-dataset-failure/date={date}/dataset={dataset}/` |
| DQ row-level fail | `ods-dlq-{env}/dq-row-failure/date={date}/dataset={dataset}/run_id={run_id}/` |
| Count mismatch | `ods-dlq-{env}/count-mismatch/date={date}/dataset={dataset}/run_id={run_id}/` |

---

## 3. Recovery & Resubmission Procedures

### 3.1 Restarting a Failed DAG Run

```sql
-- Check ingestion file state
SELECT sftp_path, status, error_reason, updated_at
FROM pipeline.ingestion_file_state
WHERE status = 'failed'
ORDER BY updated_at DESC;

-- Full Glue job execution history for a specific file
SELECT id, status, record_count, error_reason, error_detail, created_at
FROM pipeline.glue_job_log
WHERE source_path LIKE '%policies_20260414%'
  AND pipeline_type = 'ingestion'
ORDER BY id;

-- All ingestion failures today
SELECT id, run_id, dataset, business_date, status, error_reason, created_at
FROM pipeline.glue_job_log
WHERE pipeline_type = 'ingestion'
  AND status = 'failed'
  AND created_at >= CURRENT_DATE
ORDER BY created_at DESC;
```

Once root cause is resolved, clear the failed task in MWAA and trigger a re-run of DAG 2. The idempotency guard allows `failed` files to be retried — it only blocks `completed` files.

---

### 3.2 Resubmitting a Failed File

1. Identify the failure in CloudWatch or `ods.pipeline.audit`
2. Fix the root cause (schema, data, connectivity)
3. Reset file state in PostgreSQL:

```sql
-- Reset for reprocessing from SFTP copy step
UPDATE pipeline.ingestion_file_state
SET status = 'new', error_reason = NULL, updated_at = NOW()
WHERE sftp_path = '/outbound/insurance/policies/policies_2026-04-14.csv';
```

4. The next SFTP sensor poll will detect the file and re-run DAG 1 (transfer), which writes to S3 Raw and triggers DAG 2 (ETL) via EventBridge

---

### 3.3 Force Reprocessing a Completed File

If a file has `status=completed` but needs to be reprocessed (e.g. upstream data correction at the same SFTP path):

```sql
UPDATE pipeline.ingestion_file_state
SET status = 'new', error_reason = 'manual_resubmit', updated_at = NOW()
WHERE sftp_path = '/outbound/insurance/policies/policies_2026-04-14.csv';
```

The pipeline will re-copy from SFTP, re-run ETL, and re-write Parquet to S3 Curated. Because Kafka message keys are deterministic, the downstream publish pipeline will safely overwrite existing records rather than creating duplicates.

---

### 3.4 Partial Resubmission (DQ Row Failures)

When only some rows failed DQ and were routed to the DLQ:

1. The original file was partially processed — passing rows are already in S3 Curated and Kafka
2. Fix the failing rows in the DLQ file (correct the data)
3. Write the corrected rows as a new CSV file to the SFTP at a **new path**
4. Ensure the new filename matches the `file_catalogue` pattern
5. The pipeline processes the new file — deterministic keys ensure Kafka records are overwritten, not duplicated

Do **not** resubmit the original full file — passing rows would be re-converted unnecessarily.

---

### 3.5 Adding a New File to the Catalogue

When a legitimate file arrives and is quarantined as not-approved:

```sql
INSERT INTO pipeline.file_catalogue
    (name_pattern, sftp_path, domain, dataset, config_ref, active)
VALUES
    ('policies_*.csv', '/outbound/insurance/policies/', 'insurance', 'policies',
     's3://ods-config-prod/insurance/policies.yaml', TRUE);
```

After inserting, the file must still be present on the SFTP for the sensor to pick it up. If it has been removed, coordinate with the upstream team to re-place it.

---

## 4. Operational Checklist — When the Ingestion Pipeline Fails

```
1. Identify failure type from CloudWatch alarm:
   ods-file-not-approved-{env}      → Section 1.1
   ods-checksum-mismatch-{env}      → Section 1.3
   ods-schema-failure-{env}         → Section 1.4
   ods-dq-hard-failure-{env}        → Section 1.5
   ods-write-count-mismatch-{env}   → Section 1.7
   ods-job-failure-{env}            → Section 1.6 or 1.8

2. Query pipeline.glue_job_log for the detailed execution trail (ETL failures)
   SELECT id, status, record_count, error_reason, error_detail, created_at
   FROM pipeline.glue_job_log
   WHERE pipeline_type = 'ingestion'
     AND status = 'failed'
     AND created_at >= CURRENT_DATE
   ORDER BY created_at DESC;

3. Query ods.pipeline.audit for run summary
   Filter: source_type=sftp, status=failed

4. Inspect quarantine or DLQ location (Section 2)

5. Fix root cause

6. Reset PostgreSQL state (Section 3.2)

7. Wait for SFTP sensor to pick up file (DAG 1 → EventBridge → DAG 2),
   or trigger DAG 2 manually in MWAA if the CSV is already in S3 Raw
```
