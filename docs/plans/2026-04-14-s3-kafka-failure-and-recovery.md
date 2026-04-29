# S3 → Kafka Pipeline — Failures, Recovery & Message Keys

---

## 1. Why Deterministic Message Keys?

Every record published to Kafka is given a message key. This key is a deterministic hash of the record's identifying fields (e.g. `policy_id`, `effective_date`). The same record will always produce the same key.

### Why this matters

**Kafka routes messages to partitions based on the key.** All records with the same key always land on the same partition. This gives two guarantees:

1. **Ordering** — records for the same entity (e.g. the same policy) are always processed in order by consumers. Without a key, a policy update could arrive before the policy creation.

2. **Consumer-side idempotency** — if a duplicate record somehow reaches Kafka (e.g. a retry after a network fault), consumers can detect it because the key is identical. A consumer maintaining a local state store (e.g. Kafka Streams, Flink) will simply overwrite the previous value rather than treating it as a new event.

### Without deterministic keys

If keys were random or absent:
- The same record published twice lands on different partitions
- Consumers see two distinct events — both get processed
- Downstream systems accumulate duplicate records silently

### Key generation

```python
import hashlib, json

def generate_key(record: dict, key_fields: list[str]) -> str:
    key_values = {f: record[f] for f in key_fields}
    return hashlib.sha256(
        json.dumps(key_values, sort_keys=True).encode()
    ).hexdigest()

# Example: key_fields = ["policy_id", "effective_date"]
# record = {"policy_id": "POL-001", "effective_date": "2026-01-01", "premium": 1200}
# key = sha256({"effective_date": "2026-01-01", "policy_id": "POL-001"})
```

The key fields are defined per dataset in the YAML config and never change. Changing key fields is a breaking change — it must be coordinated with all consumers.

---

## 2. Failure Scenarios

### 2.1 Schema Incompatibility

**What happens:**  
The incoming Parquet file has a schema that cannot be automatically evolved — for example, a required field has been removed or renamed, or a field type has changed incompatibly (e.g. `int` → `string`).

**Pipeline behaviour:**  
- Glue detects the incompatibility during Phase 1 (Schema Validation)
- The entire file is routed to the DLQ: `ods-dlq-{env}/schema-incompatible/date={date}/topic={topic}/`
- A CloudWatch alarm fires: `ods-schema-failure-{env}`
- The Glue job fails and reports back to the Airflow DAG
- PostgreSQL file state is set to `failed` with `error_reason=schema_incompatible`
- An audit event is emitted to `ods.pipeline.audit`

**What does NOT happen:**  
No records are published to Kafka. The topic is untouched.

**Compatible changes (auto-evolved — no failure):**  
- Adding a new optional field
- Widening a numeric type (e.g. `int` → `long`)

**Incompatible changes (causes failure):**  
- Removing a field
- Renaming a field
- Narrowing a type (e.g. `long` → `int`)
- Changing a field from optional to required

---

### 2.2 Data Quality Hard Block

**What happens:**  
One or more DQDL rules marked as hard blocks have failed.

**Two sub-cases:**

**a) Dataset-level hard block** (e.g. `RowCount >= 1`):  
- Applies to the file as a whole
- If the file has zero rows or fewer rows than the threshold, the entire job fails
- The file is routed to the DLQ: `ods-dlq-{env}/dq-dataset-failure/date={date}/topic={topic}/`
- No records are published

**b) Row-level hard block** (e.g. `Completeness "policy_id" >= 1.0`):  
- Applies per row
- Rows that fail the rule are written to the DLQ: `ods-dlq-{env}/dq-row-failure/date={date}/topic={topic}/run_id={run_id}/`
- Rows that pass continue to Phase 3 (Publish)
- The job does NOT fail entirely — it completes with a partial publish and raises a CloudWatch alarm

**Soft warn (not a failure):**  
Rules marked as soft warn emit a CloudWatch metric and allow the row to continue to Kafka. These are used for non-critical fields where some tolerance is acceptable.

---

### 2.3 Count Mismatch

**What happens:**  
After publishing to Kafka, the Glue job checks that the number of records published matches the source row count from the Parquet file.

**Example:**  
- Parquet file contains 10,000 rows
- 9,997 records confirmed in Kafka (offset delta = 9,997)
- Mismatch: 3 records unaccounted for

**Why this can happen:**  
- A Kafka broker briefly unavailable mid-batch (rare with `acks=all` but possible on timeout)
- A batch silently dropped between retries at the producer level
- A serialisation error on specific records that did not raise an exception

**Pipeline behaviour:**  
- Undelivered records are written to the DLQ: `ods-dlq-{env}/count-mismatch/date={date}/topic={topic}/run_id={run_id}/`
- A CloudWatch alarm fires: `ods-count-mismatch-{env}`
- The Glue job reports the mismatch (expected count, actual count) back to the DAG
- PostgreSQL file state is set to `failed` with `error_reason=count_mismatch`

**Note:** Because records are published with Kafka transactions and deterministic message keys, replaying the DLQ records is safe — any records that did make it to Kafka will simply be overwritten by the consumer (same key = same partition = idempotent update).

---

### 2.4 Glue Job Crash (Mid-Publish)

**What happens:**  
The Glue job fails unexpectedly mid-publish (e.g. out of memory, worker node lost, timeout).

**Pipeline behaviour:**  
- Kafka transactions abort automatically — any uncommitted records in the current transaction are rolled back
- No partial data lands in the Kafka topic
- MWAA detects the job failure via the GlueJobOperator status check
- PostgreSQL file state remains `processing`

**Recovery:**  
- MWAA retries the DAG task (configurable retry count)
- On retry, the idempotency check in Phase 1 sees `status=processing` — the pipeline proceeds
- The Glue job restarts from scratch: re-reads the Parquet file, re-runs DQ, re-publishes
- Because message keys are deterministic, any records already committed to Kafka in a previous transaction are safely overwritten — no duplicates

---

### 2.5 DAG Failure (Before Glue Trigger)

**What happens:**  
The Airflow DAG fails after setting PostgreSQL state to `processing` but before triggering the Glue job (e.g. config load error, network timeout).

**Pipeline behaviour:**  
- No Glue job was triggered — Kafka topic is untouched
- PostgreSQL file state is `processing`

**Recovery:**  
- MWAA retries the DAG automatically (configurable)
- Idempotency check sees `processing` → proceeds to re-load config and trigger Glue
- No data loss, no duplicates

---

### 2.6 Duplicate File Arrival (Same File Lands Twice)

**What happens:**  
The S3 Curated Zone receives the same Parquet file a second time (e.g. upstream pipeline re-runs, S3 event delivered more than once).

**Pipeline behaviour:**  
- EventBridge fires on the second S3 Object Created event and triggers the DAG
- Phase 1 idempotency check queries PostgreSQL: `status=completed`
- The DAG exits immediately — no Glue job triggered, no re-publish

**Why this is safe:**  
The idempotency guard uses the S3 file path as the unique key. As long as the same file path = the same logical dataset, duplicates are suppressed entirely at the sensor level.

**Edge case — same data, different file path:**  
If upstream generates a new file path for the same data (e.g. a reprocessed file with a new timestamp in the path), the guard will not catch it. In this case, deterministic message keys are the safety net — consumers overwrite the previous record rather than creating a duplicate.

---

## 3. Restart & Resubmission Procedures

### 3.1 Restarting a Failed DAG Run

When a DAG run has `status=failed` in MWAA:

1. Investigate the failure reason in CloudWatch: `/ods/{env}/airflow` log group or the `ods.pipeline.audit` Kafka topic
2. If the root cause is resolved (e.g. schema updated, upstream data fixed), clear the failed task in MWAA and trigger a re-run
3. The PostgreSQL state for the file will be `failed` — the pipeline will proceed on retry (the guard only blocks `completed` files)

```sql
-- Check file state before resubmitting
SELECT s3_path, status, error_reason, updated_at
FROM pipeline.file_state
WHERE status = 'failed'
ORDER BY updated_at DESC;

-- Full Glue job execution history for a specific file
SELECT id, status, record_count, error_reason, error_detail, created_at
FROM pipeline.glue_job_log
WHERE source_path = 's3://ods-curated-prod/insurance/policies/policies_20260414.parquet'
ORDER BY id;

-- All publish failures today
SELECT id, run_id, dataset, business_date, status, error_reason, created_at
FROM pipeline.glue_job_log
WHERE pipeline_type = 'publish'
  AND status = 'failed'
  AND created_at >= CURRENT_DATE
ORDER BY created_at DESC;
```

---

### 3.2 Resubmitting a File from the DLQ

Files and records in the DLQ are partitioned by failure type:

```
ods-dlq-{env}/
  schema-incompatible/date={date}/topic={topic}/
  dq-dataset-failure/date={date}/topic={topic}/
  dq-row-failure/date={date}/topic={topic}/run_id={run_id}/
  count-mismatch/date={date}/topic={topic}/run_id={run_id}/
```

**Steps to resubmit:**

1. **Identify the DLQ records** — use S3 console or Athena to query `ods-dlq-{env}`
2. **Investigate and fix the root cause:**
   - Schema incompatible → update schema in Glue Schema Registry and YAML config
   - DQ hard block → fix upstream data or adjust DQ rule threshold with approval
   - Count mismatch → investigate Kafka broker logs, verify no data loss
3. **Copy the fixed file back to the curated zone** at the original S3 path (or a new path if the original was corrupted)
4. **Reset PostgreSQL state** to allow reprocessing:

```sql
-- Reset a specific file for reprocessing
UPDATE pipeline.file_state
SET status = 'new', error_reason = NULL, updated_at = NOW()
WHERE s3_path = 's3://ods-curated-prod/insurance/policies/2026-04-14/file.parquet';
```

5. **Trigger the DAG manually** in MWAA with the file path, or re-place the file in S3 Curated to trigger EventBridge automatically

---

### 3.3 Force Reprocessing a Completed File

If a file has already been processed (`status=completed`) but needs to be republished (e.g. data correction):

```sql
-- Force reprocessing — use with caution in production
UPDATE pipeline.file_state
SET status = 'new', error_reason = 'manual_resubmit', updated_at = NOW()
WHERE s3_path = 's3://ods-curated-prod/insurance/policies/2026-04-14/file.parquet';
```

**Important:** Because message keys are deterministic, re-publishing the same records will overwrite existing Kafka messages at the consumer level — not append duplicates. Downstream consumers must support idempotent updates (last-write-wins by key).

---

### 3.4 Partial Resubmission (DQ Row Failures Only)

When only some rows failed DQ hard block rules and were routed to the DLQ:

1. The original file was partially published — passing rows are already in Kafka
2. Fix the failing rows in the DLQ file (correct the data)
3. Write the corrected rows as a new Parquet file to the curated zone
4. The pipeline will process the new file normally — deterministic keys ensure existing Kafka records are overwritten, not duplicated

Do **not** resubmit the original full file — passing rows would be re-published unnecessarily (safe due to message keys, but wasteful).

---

## 4. Operational Checklist — When a Pipeline Fails

```
1. Check CloudWatch alarm to identify failure type
   Alarm: ods-schema-failure-{env}   → Section 2.1
   Alarm: ods-dq-hard-failure-{env}  → Section 2.2
   Alarm: ods-count-mismatch-{env}   → Section 2.3
   Alarm: ods-job-failure-{env}      → Section 2.4 or 2.5

2. Query pipeline.glue_job_log for the detailed execution trail
   SELECT id, status, record_count, error_reason, error_detail, created_at
   FROM pipeline.glue_job_log
   WHERE status = 'failed' AND created_at >= CURRENT_DATE
   ORDER BY created_at DESC;

3. Query ods.pipeline.audit for the run summary
   Filter: status=failed, dataset=<name>

4. Inspect DLQ records in ods-dlq-{env}

5. Fix root cause

6. Reset PostgreSQL file_state (Section 3.2)

7. Resubmit via MWAA or re-place the file in S3 Curated to trigger EventBridge
```
