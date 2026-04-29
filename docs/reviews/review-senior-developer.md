# Technical Review — Senior Developer / Backend Lead
**Reviewer role:** Senior Developer / Backend Lead  
**Date:** 2026-04-16  
**Documents reviewed:**
1. `2026-04-14-s3-kafka-failure-and-recovery.md` — S3→Kafka publish pipeline failure modes
2. `2026-04-14-ingestion-failure-and-recovery.md` — SFTP ingestion pipeline failure modes
3. `2026-04-15-testing-strategy.md` — Testing approach
4. `2026-04-15-reconciliation-design.md` — Reconciliation / T+1 / T+2 design
5. `2026-04-14-ingestion-design.md` — Ingestion pipeline design (reference)
6. `2026-04-14-s3-kafka-design.md` — S3→Kafka publish pipeline design (reference)
7. `2026-04-14-architecture-decisions.md` — Architecture Decision Records (reference)

---

## Executive Summary

The four documents under review are well-structured and show a consistent architectural philosophy throughout. The S3→Kafka failure doc is the stronger of the two failure docs — it is specific, actionable, and tightly aligned with the design. The ingestion failure doc has several gaps that could leave an on-call engineer making decisions not covered by procedure. The testing strategy is thorough and philosophically sound, but it does not explicitly test four failure scenarios described in the failure docs, which is a gap for a document that is marked "Approved for implementation". The reconciliation design is the most forward-looking document and is well-reasoned, but it introduces infrastructure (the `ods.pipeline.reconciliation` topic and the `pipeline.reconciliation_log` table) that are referenced nowhere in the failure docs, creating a consistency gap that will widen as the platform grows.

---

## Critical Issues (must fix)

### C1 — Ingestion failure doc 1.8 recommends a state transition that conflicts with the design's state machine

**Location:** `2026-04-14-ingestion-failure-and-recovery.md`, Section 1.8, recovery step 2

**Issue:** The recovery procedure states: "Reset file state to `transferred` (not `new`) to skip the SFTP copy step." However, the ingestion design (`2026-04-14-ingestion-design.md`, Section 4, PostgreSQL schema) defines the `ingestion_file_state.status` values as: `detected | transferred | etl_processing | completed | failed`. Setting state back to `transferred` to skip the SFTP copy is a reasonable instruction, but the idempotency logic described in Section 1.2 of the same failure doc says the DAG exits immediately when it sees `status=completed`, and Section 3.2 resets to `new` for a full reprocessing. There is no documented path in either the design or the failure doc that describes how DAG 2 is triggered independently when state is manually set to `transferred` — specifically, whether the SFTP sensor (DAG 1) or a manual MWAA trigger is required, and which DAG reads the `transferred` state to decide to skip the SFTP copy. An on-call engineer following this instruction verbatim will not know what to trigger next.

**Suggested fix:** Add an explicit step to 1.8 recovery: after resetting state to `transferred`, manually trigger DAG 2 in MWAA (not DAG 1, which would attempt an SFTP copy). Cross-reference Section 3.2 explicitly so the engineer understands why `new` versus `transferred` produces different behaviour. Consider also whether the operational checklist in Section 4 should distinguish between Glue crash (re-trigger DAG 2 only) and earlier-stage failures (re-trigger DAG 1).

---

### C2 — Ingestion failure doc 1.8 leaves a real data hazard unaddressed: partial Parquet triggering the publish pipeline prematurely

**Location:** `2026-04-14-ingestion-failure-and-recovery.md`, Section 1.8

**Issue:** The document correctly identifies the hazard: "If a partial Parquet landed in S3 Curated before the crash, EventBridge may have already triggered the Publish Pipeline." Recovery step 1 says: "Ensure partial Parquet files are removed from S3 Curated before retrying (or accept overwrite at publish layer)." The phrase "or accept overwrite at publish layer" is not safe guidance — if the partial Parquet triggered the publish pipeline and that pipeline completed with `status=completed` in `pipeline.file_state`, then a re-run of the ingestion ETL will write a new Parquet to S3 Curated, EventBridge will fire again, and the publish pipeline's idempotency guard will block re-publishing because `status=completed`. The overwrite will silently not happen.

This is not just a documentation ambiguity — it is a correctness hole. The downstream data in Kafka will remain partial, with no alarm and no flag, because the publish pipeline believes it already succeeded.

**Suggested fix:** Remove "or accept overwrite at publish layer" from step 1. Make step 1 mandatory: partial Parquet files must be deleted from S3 Curated before retrying. Add a step to also reset `pipeline.file_state` for the corresponding S3 Curated path to `new` if the partial publish pipeline run completed. Cross-reference `2026-04-14-s3-kafka-failure-and-recovery.md` Section 3.3 (Force Reprocessing a Completed File) as the procedure for that reset.

---

### C3 — S3→Kafka failure doc describes `status=processing` as the state on Glue crash, but this contradicts the design

**Location:** `2026-04-14-s3-kafka-failure-and-recovery.md`, Section 2.4

**Issue:** Section 2.4 states: "PostgreSQL file state remains `processing`" after a Glue job crash. However, `2026-04-14-s3-kafka-design.md` Section 3 defines `pipeline.file_state` status values as: `new | processing | completed | failed`. The design's Section 8 (Restartability) states: "Glue job fails mid-publish: Kafka transaction aborted — no partial data in topic. DAG retry re-triggers Glue from scratch." This is consistent with the failure doc. However, Section 2.4 recovery says "the idempotency check in Phase 1 sees `status=processing` — the pipeline proceeds." This only works if the DAG retry is automatic (MWAA retry count > 0). If all retries are exhausted, `status` may remain `processing` indefinitely — not `failed` — blocking any future automated or manual re-run, because Section 3.1 says the guard blocks `completed` files but Section 2.5 says a `failed` state allows retry. A file stuck at `processing` after exhausted retries is a dead end with no documented recovery path.

**Suggested fix:** Clarify what MWAA does to PostgreSQL state when all retries are exhausted on a Glue crash. Either (a) the DAG's final-failure handler must set state to `failed`, or (b) the recovery procedure for 2.4 must include a manual SQL step to reset `processing` → `new` / `failed` before resubmission. Add an alarm for files that remain in `status=processing` beyond a configurable timeout (e.g., 2 hours).

---

## Significant Issues (should fix)

### S1 — The two failure docs use different table names for the same concept without explanation

**Location:** `2026-04-14-s3-kafka-failure-and-recovery.md` Section 3.1 vs. `2026-04-14-ingestion-failure-and-recovery.md` Section 3.1

**Issue:** The publish pipeline failure doc refers to `pipeline.file_state` throughout. The ingestion failure doc refers to `pipeline.ingestion_file_state`. These are correctly two different tables (as confirmed by the design docs), but neither failure doc mentions the other table or explains why they are different. An engineer who reads both docs in quick succession under pressure may update the wrong table. The SQL examples in each doc use different column names for the file path: `s3_path` in `file_state` vs. `sftp_path` in `ingestion_file_state`. This is correct per the schema but the difference is never highlighted.

**Suggested fix:** Add a brief note to each failure doc's Section 3 header: "This pipeline uses `pipeline.ingestion_file_state` (keyed on `sftp_path`). The publish pipeline uses the separate `pipeline.file_state` (keyed on `s3_path`). Do not update the wrong table." This takes two sentences and prevents a class of operational error.

---

### S2 — The ingestion failure doc's operational checklist (Section 4) maps `ods-job-failure-{env}` to Sections 1.6 or 1.8, but Section 1.6 (CSV conversion failure) has no CloudWatch alarm defined in the design

**Location:** `2026-04-14-ingestion-failure-and-recovery.md`, Section 4 (operational checklist) and Section 1.6

**Issue:** The checklist maps `ods-job-failure-{env}` → "Section 1.6 or 1.8". Section 1.6 describes a CSV → Parquet conversion failure. It says "CloudWatch metric emitted: `dq.hard.failure`" but does not describe an alarm. The ingestion design's CloudWatch section (`2026-04-14-ingestion-design.md`, Section 8) lists `ods-dq-hard-failure-{env}` as the alarm for DQ failures — not specifically a conversion failure alarm. A conversion failure (e.g. `"N/A"` cast error) is not a DQ rule failure — it is a Spark exception. If the conversion failure is not caught by a DQ rule and instead throws an exception, the resulting alarm would be `ods-job-failure-{env}`, not `ods-dq-hard-failure-{env}`. The failure taxonomy conflates DQ failures with type-cast exceptions.

**Suggested fix:** Separate conversion failures from DQ failures in the classification. Clarify whether conversion failures are caught by DQDL rules (in which case the DQ alarm fires) or by Glue exception handling (in which case `ods-job-failure-{env}` fires). Update Section 1.6 to specify the exact alarm, not just the metric.

---

### S3 — Testing strategy scenario 2 describes the wrong quarantine mechanism for the ingestion pipeline

**Location:** `2026-04-15-testing-strategy.md`, Test Scenarios Matrix, Scenario 2

**Issue:** Scenario 2 states: "File not in `pipeline.file_catalogue` → File moved to S3 quarantine prefix; PostgreSQL state=quarantined." However, the ingestion failure doc (Section 1.1) states: "File is NOT copied from SFTP — it stays on the SFTP server." The file is not moved to quarantine — only its details are logged to `ods-quarantine-{env}/not-approved/`. Scenario 2's expected outcome "File moved to S3 quarantine prefix" implies a file copy operation that the ingestion design explicitly says does not happen. This will cause the integration test to assert on a behaviour that does not exist, making the test fail for the wrong reason — or worse, cause someone to implement an unnecessary file copy to make the test pass.

Also, "PostgreSQL state=quarantined" is not a valid status value per the `ingestion_file_state` schema in the ingestion design, which defines states as: `detected | transferred | etl_processing | completed | failed`. There is no `quarantined` state.

**Suggested fix:** Update Scenario 2 expected outcome to: "File details logged to `ods-quarantine-{env}/not-approved/`; no record in `pipeline.ingestion_file_state` (file was never transferred or registered); CloudWatch alarm `ods-file-not-approved-{env}` fires." Remove "PostgreSQL state=quarantined."

---

### S4 — Ingestion failure doc 1.7 (Write Count Mismatch) has no recovery procedure

**Location:** `2026-04-14-ingestion-failure-and-recovery.md`, Section 1.7

**Issue:** Section 1.7 describes the failure mode and pipeline behaviour but ends without a recovery procedure. Every other section in the document (1.3, 1.4, 1.5, 1.6, 1.8, 1.9) has an explicit "Recovery:" block. Section 1.7 is missing this. The operational checklist in Section 4 maps `ods-write-count-mismatch-{env}` to Section 1.7, so an engineer will navigate there looking for recovery steps and find none. The sister document (`2026-04-14-s3-kafka-failure-and-recovery.md` Section 2.3) has a recovery procedure for the equivalent Kafka publish count mismatch, but the recovery steps are not directly applicable here (the Parquet write mismatch is a different failure than a Kafka publish mismatch).

**Suggested fix:** Add a "Recovery:" block to Section 1.7 covering: (1) identify the unwritten records in the DLQ, (2) investigate whether the Parquet file in S3 Curated is complete or partial, (3) if partial, remove it and reset `ingestion_file_state` to `transferred` to re-run ETL only, (4) resubmit per Section 3.2.

---

### S5 — The reconciliation design introduces `pipeline.reconciliation_log` and `ods.pipeline.reconciliation` topic, but neither appears in the failure docs or testing strategy

**Location:** `2026-04-15-reconciliation-design.md` Sections 3.5 and 8; `2026-04-14-s3-kafka-failure-and-recovery.md`; `2026-04-15-testing-strategy.md`

**Issue:** The reconciliation design defines a new PostgreSQL table (`pipeline.reconciliation_log`) and a new Kafka topic (`ods.pipeline.reconciliation`). Neither appears in the S3→Kafka failure doc's operational checklist (which would be the natural place to reference reconciliation failures as a failure mode). Neither is covered by any test scenario in the testing strategy. If a T2 or T3 reconciliation job fails, or if `pipeline.reconciliation_log` shows a failed check, there is no documented recovery path anywhere and no test that validates the reconciliation job's output. This is an important gap because the reconciliation design itself states in Section 10 that reconciliation SLOs are unresolved.

**Suggested fix:** Add at minimum a placeholder Section 2.7 to `2026-04-14-s3-kafka-failure-and-recovery.md` titled "Reconciliation Job Failure" that describes what to do when a T2/T3 check fails. Add a test scenario for T0 reconciliation (count mismatch path) that explicitly validates a write to `pipeline.reconciliation_log`. The reconciliation topic should be listed in the testing strategy's Kafka topic cleanup fixtures.

---

### S6 — S3→Kafka failure doc Section 3.2 step 3 says "copy the fixed file back to the curated zone at the original S3 path" but the design's Glue Job Bookmarks would prevent re-reading it

**Location:** `2026-04-14-s3-kafka-failure-and-recovery.md`, Section 3.2 step 3; `2026-04-14-s3-kafka-design.md`, Section 8

**Issue:** The S3→Kafka design doc states: "Glue Job Bookmarks enabled to prevent S3 source re-reads on job retry." If Glue Job Bookmarks are enabled, re-placing a file at its original S3 path will not cause Glue to re-read it, because the bookmark will record that path as already processed. Section 3.2's instruction to copy the fixed file back to the original path is therefore ineffective — the Glue job will skip it, the pipeline will appear to succeed, and no records will be published. This is a silent failure mode in the recovery procedure itself.

**Suggested fix:** Either (a) specify that Glue Job Bookmarks must be reset or disabled before DLQ resubmission (add the AWS CLI command to reset a bookmark), or (b) always use a new S3 path for resubmission and update the idempotency reset SQL to use the new path. Clarify this in the operational checklist in Section 4.

---

## Minor Issues / Improvements

### M1 — S3→Kafka failure doc Section 2.4 and 2.5 have inconsistent terminology for MWAA retry behaviour

**Location:** `2026-04-14-s3-kafka-failure-and-recovery.md`, Sections 2.4 and 2.5

**Issue:** Section 2.4 says "MWAA retries the DAG task (configurable retry count)." Section 2.5 says "MWAA retries the DAG automatically (configurable)." These describe the same mechanism but at different levels: task-level retry (2.4) vs. DAG-level retry (2.5). Airflow retries are configured at the task level, not the DAG level. Using "DAG automatically" in 2.5 implies MWAA reschedules the entire DAG, which is not what happens — only the failed task is retried. Consistent use of "task-level retry" across both sections would avoid ambiguity.

---

### M2 — The ingestion failure doc's DLQ reference table (Section 2) is missing the `dq-dataset-failure` entry's `dataset=` partition component

**Location:** `2026-04-14-ingestion-failure-and-recovery.md`, Section 2, DLQ reference table

**Issue:** The DLQ location for "DQ dataset-level fail" is listed as `ods-dlq-{env}/dq-dataset-failure/date={date}/dataset={dataset}/`. However, Section 1.5a (Dataset-level hard block) says "File routed to `ods-dlq-{env}/dq-dataset-failure/`" — it omits both `date=` and `dataset=` partitions in the prose, which contradicts the table. The S3→Kafka failure doc Section 2.2a lists the DLQ as `ods-dlq-{env}/dq-dataset-failure/date={date}/topic={topic}/` (uses `topic=` not `dataset=`). The ADR Section 1.7 shows `dq-dataset-failure/date={date}/topic|dataset={name}/`, acknowledging both variants exist. This inconsistency across three documents makes it unclear which partition key is actually used, which matters for Athena queries and operational investigation.

**Suggested fix:** Standardise on one key name across both pipelines. For the ingestion pipeline, `dataset=` is appropriate (there is no Kafka topic). For the publish pipeline, `topic=` is appropriate (the topic is the target). Update the ADR Section 1.7 to remove the ambiguous `topic|dataset=` notation and show the correct key per pipeline type.

---

### M3 — Testing strategy scenario 5 lists "PostgreSQL state=schema_error" which does not match the defined status values

**Location:** `2026-04-15-testing-strategy.md`, Test Scenarios Matrix, Scenario 5

**Issue:** Scenario 5 expected outcome includes "PostgreSQL state=schema_error." The `pipeline.file_state` status values defined in `2026-04-14-s3-kafka-design.md` are: `new | processing | completed | failed`. The failure doc for the same scenario (S3→Kafka failure doc, Section 2.1) says "PostgreSQL file state is set to `failed` with `error_reason=schema_incompatible`." The test scenario uses a non-existent status value. The integration test asserting on `status='schema_error'` will never find a matching row.

**Suggested fix:** Change Scenario 5 expected outcome to: "PostgreSQL state=failed, error_reason=schema_incompatible."

---

### M4 — The ingestion failure doc Section 3.4 instructs placing a corrected file at a "new path" on SFTP, but `pipeline.file_catalogue` uses name patterns — the new filename must match the pattern or it will be quarantined

**Location:** `2026-04-14-ingestion-failure-and-recovery.md`, Section 3.4, step 3

**Issue:** Step 3 says "Write the corrected rows as a new CSV file to the SFTP at a new path." Step 4 says "Ensure the new filename matches the `file_catalogue` pattern." Step 4 partially addresses this, but it does not warn the engineer that if the pattern uses date-based naming (e.g., `policies_*.csv`), a second file for the same business date with a different run suffix may conflict with downstream downstream date-based partitioning in S3 Curated and produce a second Parquet partition for the same date. This could cause double-counting in T3 reconciliation unless consumers handle multiple Parquet files per date.

**Suggested fix:** Add a note: "If the corrected file represents the same business date as the original, ensure the Parquet output path in S3 Curated is either merged with or replaces the original partition. A second Parquet file for the same `date=` partition will be treated as an additional batch by the publish pipeline and may cause double-counting at T3 reconciliation."

---

### M5 — The reconciliation design document begins with "ar#" — stray characters in the first line

**Location:** `2026-04-15-reconciliation-design.md`, line 1

**Issue:** The document opens with `ar# ODS Platform — Reconciliation Design`. The `ar` prefix is clearly a typo — likely a partial keystroke that was not caught. While it does not affect content, it renders incorrectly in any Markdown viewer.

**Suggested fix:** Remove `ar` from line 1 so it reads `# ODS Platform — Reconciliation Design`.

---

### M6 — S3→Kafka failure doc Section 2.3 says "A Kafka broker briefly unavailable mid-batch (rare with `acks=all` but possible on timeout)" — this understates the risk

**Location:** `2026-04-14-s3-kafka-failure-and-recovery.md`, Section 2.3

**Issue:** The explanation of count mismatch causes lists broker unavailability as "rare with `acks=all`." With `acks=all`, a broker being temporarily unavailable does not cause silent record loss — it causes the producer to throw a retriable exception or, after retries are exhausted, a non-retriable exception. In either case the Kafka transaction will not silently commit partial records; it will fail or retry visibly. The more likely cause of a count mismatch with `acks=all` and transactions enabled is a serialisation error on specific records (already listed) or a Glue DPU memory issue causing a silent truncation at the Spark DataFrame level before the Kafka producer is even invoked. The broker unavailability framing may mislead engineers investigating a mismatch into checking Kafka broker health when the root cause is in the Glue job.

**Suggested fix:** Reorder the causes to lead with serialisation errors and Spark-level truncation, note that `acks=all` with transactions makes silent broker-level loss extremely unlikely, and suggest Glue executor logs as the first investigation target for count mismatches.

---

### M7 — Testing strategy component tests mock the Kafka producer but Section 2.2 says this tests that "`send()` was called with the correct arguments" — this does not validate message key correctness

**Location:** `2026-04-15-testing-strategy.md`, Section 2.2 (Component Tests)

**Issue:** The component tests mock the MSK Kafka producer and assert that `send()` was called with the correct arguments. However, "correct arguments" is not defined in the document. The most important correctness property of `send()` for this platform is that the message key is the deterministic SHA256 hash of the correct key fields. A mock that checks `send()` was called does not verify that the key was computed from the right fields or with the right hash function. This is a gap given that the idempotency and deduplication of the entire platform depends on correct key generation. The unit tests do test `generate_key()` in isolation, but the component tests do not verify the end-to-end path from record fields → key generation → `send()` argument.

**Suggested fix:** In component tests, assert that the key argument passed to the mocked `send()` matches the expected SHA256 hash of the test record's key fields. This can be computed inline in the test using the same `generate_key()` function under test in unit tests, providing a cross-level consistency check.

---

## Cross-Document Linking Gaps

1. **Ingestion failure doc → S3→Kafka failure doc for Glue crash (1.8):** Section 1.8 mentions that the publish pipeline may be triggered prematurely by a partial Parquet. It should explicitly reference `2026-04-14-s3-kafka-failure-and-recovery.md` Section 3.3 (Force Reprocessing a Completed File) as the procedure to use after recovery.

2. **S3→Kafka failure doc → Reconciliation design for count mismatch (2.3):** Section 2.3 describes count mismatch but does not reference the reconciliation design's T0 check or `pipeline.reconciliation_log`. After DLQ replay, the engineer should know to verify the reconciliation log for the business date.

3. **Testing strategy → Ingestion failure doc for Scenario 2:** The test matrix scenario for "File not in `pipeline.file_catalogue`" should reference Section 1.1 of the ingestion failure doc for the exact expected pipeline behaviour.

4. **Testing strategy → Reconciliation design:** The testing strategy has no section for reconciliation tests. Section 11 of the reconciliation design says T1 consumer lag alarms are the "lowest effort, highest operational value" improvement. There is no test scenario for consumer lag or T2/T3 reconciliation job execution.

5. **Architecture decisions doc (1.7 DLQ) → Both failure docs:** The ADR defines the DLQ partition structure using the notation `topic|dataset={name}`, which is ambiguous. Both failure docs should be the canonical source for which partition key each pipeline uses, and the ADR should reference them.

6. **S3→Kafka failure doc → Architecture decisions doc (1.5, Glue Job Bookmarks):** Section 3.2 (DLQ resubmission) should reference the ADR discussion of Glue Job Bookmarks and state explicitly whether bookmarks must be reset before resubmission.

7. **Ingestion failure doc Section 3.5 (Adding a New File to Catalogue) → Ingestion design Section 4 (file_catalogue schema):** Section 3.5 includes a raw INSERT SQL for `pipeline.file_catalogue` but does not reference the design doc that defines the full column set. An engineer might miss the `config_ref` or `active` columns without the cross-reference.

8. **Reconciliation design Section 8 (`ods.pipeline.reconciliation` topic) → S3→Kafka design Section 3 (MSK Topics):** The reconciliation topic is not listed in the MSK topics section of the S3→Kafka design. As a platform-level infrastructure topic, it should be added there alongside `ods.pipeline.audit`.

---

## Positive Observations

1. **The three-layer idempotency design is consistently described and correctly applied.** All three documents (S3→Kafka design, S3→Kafka failure doc, and the ADR) explain PostgreSQL-level file state guard, Kafka transaction atomicity, and deterministic message keys in a way that is mutually consistent. This is the most important correctness property of the platform and it is well-documented.

2. **The S3→Kafka failure doc operational checklist (Section 4) is genuinely on-call-ready.** The alarm → section mapping is exact, the SQL queries are copy-paste executable, and the seven-step flow would survive a 3am incident. This is the standard all failure docs should be held to.

3. **The SQL examples throughout both failure docs use realistic paths and table/column names that match the design.** There are no placeholder values like `<your_value_here>` — the examples use the actual bucket names, schema names, and column names defined in the design docs. This significantly reduces friction during incident response.

4. **The testing strategy's philosophy section (1.1 and 1.2) is unusually honest about the limits of mocking for data pipeline testing.** The explicit statement that "all mocked tests pass, but in production the DynamicFrame schema inference selects the wrong type for a nullable column and every record silently drops that field" reflects real-world experience and will guide the team toward more valuable tests. The test pyramid for data pipelines (heavier integration, lighter unit) is appropriate for this architecture.

5. **The reconciliation design's tiered approach (T0–T3) is architecturally well-grounded.** Separating real-time publish-time checking (T0) from consumer lag (T1), periodic count reconciliation (T2), and full business-level reconciliation (T3) gives the platform a graduated response to data quality issues rather than a single binary alarm. The design is ahead of implementation, which is correct — design before build. The implementation order in Section 11 is pragmatic.

6. **The ingestion pipeline's DAG 1 / DAG 2 split is correctly reflected in both the design and failure docs.** The failure doc correctly identifies that a Glue crash (1.8) may leave state at `transferred` and that DAG 2 should be retried independently, which is consistent with the ADR's rationale for the split (ADR 1.10). This cross-doc consistency suggests the design decisions have been genuinely internalised rather than treated as a separate planning artefact.
