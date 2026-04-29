# ODS Platform — Consolidated Review Action List
**Date:** 2026-04-16  
**Sources:** 6 peer reviews (Architect, Senior Developer, Data Engineering Lead, SRE Lead, Security Engineer, Technical Writer)  
**Total issues:** 28 critical/significant, 18 minor  
**De-duplicated:** Issues found by multiple reviewers are merged into a single action item

---

## How to Read This

- **P0 — Regulatory/Legal:** Must resolve before any production-bound sprint locks. Involves GDPR, FCA, or direct compliance exposure.  
- **P1 — Architecture Contradiction:** Two approved documents disagree. Build work cannot start safely until resolved.  
- **P2 — Operational Safety:** A gap that will cause silent data loss, incorrect incident response, or a dead-end recovery procedure.  
- **P3 — Should Fix:** Real issues that will cause confusion or operational risk if not addressed before go-live.  
- **P4 — Minor/Polish:** Correctness, clarity, or diagram issues that should be fixed but are not blockers.

**Owner codes:** Platform = core platform/data engineering team; Security = security/DPO; DevOps = CI/CD/infrastructure; SRE = site reliability; DataGov = data governance lead; Business = CTO/business sign-off required.

---

## P0 — Regulatory / Legal (resolve before any production-bound sprint)

### P0-1: GDPR Article 17 — S3 Raw Zone plaintext PII not addressed by crypto-shredding
**Found by:** Security (C3), referenced in Data Eng retention doc open item  
**Documents:** `security-data-privacy.md §3.2`, `data-retention.md` Appendix A Open Item 5, `disaster-recovery.md` (S3 Object Lock recommendation)  
**Issue:** Crypto-shredding covers Kafka messages and curated Parquet. Original plaintext CSV files in `ods-raw-{env}` are written before any encryption and are untouched by DEK deletion. If `ods-raw-{env}` uses S3 Object Lock (GOVERNANCE mode, 7 years, as recommended in the DR doc), object-level deletion may be structurally impossible, making GDPR Article 17 compliance unachievable without a design change.  
**Action:** Make a binding decision (with DPO and Legal) from three options: (a) encrypt Raw Zone files at write time with the same per-entity DEK; (b) pseudonymise at ingest and destroy the mapping; (c) accept a short Raw Zone retention (≤90 days) so files are gone before an erasure request can arrive. Document the decision in `security-data-privacy.md §3.2` and resolve Appendix A Open Item 5 in `data-retention.md`. Do not apply S3 Object Lock to `ods-raw-{env}` until this is resolved.  
**Owner:** Security + DPO + Legal  
**Gates:** Phase 5 (Security & Compliance) cannot sign off without this resolved.

---

### P0-2: Audit sink retention — 365 days (security doc) contradicts 6-year regulatory requirement (retention doc)
**Found by:** Security (C2)  
**Documents:** `security-data-privacy.md §9.1`, `data-retention.md §3.2`  
**Issue:** Security doc specifies 365 days for `ods-audit-sink-{env}`. Retention doc specifies a tiered lifecycle to 6 years. FCA SYSC 9.1 and MiFID II Article 25 require financial firms to retain records for a minimum of 5–7 years. The shorter figure would constitute a regulatory breach.  
**Action:** Update `security-data-privacy.md §9.1` to align with the 6-year schedule in `data-retention.md §3.2`. Treat 365 days as the Standard→Standard-IA tier transition, not the deletion date. Add a cross-reference from the security doc to the retention doc rather than duplicating figures.  
**Owner:** Security + Data Governance  
**Gates:** Must resolve before Phase 5.

---

### P0-3: Cross-region S3 replication GDPR Chapter V implications unaddressed
**Found by:** Security (S6)  
**Documents:** `disaster-recovery.md §8.3`, `security-data-privacy.md`  
**Issue:** DR doc recommends S3 Cross-Region Replication for `ods-raw-{env}` and `ods-curated-{env}`, which contain Restricted-PII. UK GDPR Chapter V restricts transfers to third countries. Whether the DR region is within UK/EEA, or whether an adequacy decision applies, is not addressed.  
**Action:** Specify the DR region explicitly. Confirm it is within UK/EEA or document the transfer mechanism (SCCs or equivalent). Add a DPIA reference if outside UK/EEA. Update `security-data-privacy.md §8` to include cross-region transfer in the threat model. Confirm with DPO.  
**Owner:** Security + DPO  
**Gates:** Must resolve before Phase 7 (DR strategy confirmed).

---

## P1 — Architecture Contradictions (resolve before Phase 3 build starts)

### P1-1: SSE-KMS vs SSE-S3 — approved ADR contradicts security doc
**Found by:** Security (C1)  
**Documents:** `architecture-decisions.md §5.1` (Approved), `security-data-privacy.md §4.1, §6`  
**Issue:** ADR (status: Approved) says "SSE-S3 or SSE-KMS (standard AWS default)" — standard AWS default is SSE-S3 (AWS-managed keys). Security doc mandates SSE-KMS with CMKs for all buckets. These are mutually exclusive: SSE-S3 cannot support KMS key rotation control, `Deny` conditions, or the KMS key-per-purpose strategy. Terraform built to the ADR as written will produce buckets that make the entire KMS access-control and crypto-shredding architecture non-functional.  
**Action:** Update `architecture-decisions.md §5.1` to explicitly state "SSE-KMS with CMKs only; SSE-S3 is not permitted on any bucket containing Internal, Confidential, or Restricted-PII data." Trigger a formal revision of the ADR (it is currently Approved). Audit any Terraform written to the current ADR.  
**Owner:** Platform + Security  
**Gates:** Phase 1 (IaC) cannot be finalised until resolved.

---

### P1-2: Entity key storage — Secrets Manager (implementation plan) vs PostgreSQL (security doc)
**Found by:** Security (C6)  
**Documents:** `implementation-plan.md` Phase 5, `security-data-privacy.md §3.2`  
**Issue:** Implementation plan describes GDPR erasure entity keys in Secrets Manager (`ods/gdpr/entity-keys/{entity_id}`). Security doc describes a PostgreSQL `privacy.entity_keys` table with KMS-held key material. Two teams building Phase 5 from these two documents will produce incompatible erasure key architectures. A split implementation makes erasure verification impossible and will produce orphaned key material.  
**Action:** Designate one canonical design. Recommendation: security doc's PostgreSQL `privacy.entity_keys` table (supports audit queries, erasure status tracking, batch erasure). Update `implementation-plan.md` Phase 5 to match. If Secrets Manager is preferred, document the reason in the security doc and remove the PostgreSQL approach.  
**Owner:** Security + Platform  
**Gates:** Phase 5 engineering cannot begin until resolved.

---

### P1-3: RTO/RPO targets — DR doc proposes values; implementation plan lists D5 as an unresolved blocker
**Found by:** SRE (C1)  
**Documents:** `disaster-recovery.md §2.2`, `implementation-plan.md` Blocking Decisions table (D5)  
**Issue:** DR doc proposes specific RTO/RPO values marked `[REQUIRES SIGN-OFF]`. Implementation plan lists D5 ("RTO/RPO targets for production") as a High-priority blocking decision gating Phase 2, with no target values. These two documents disagree on whether this decision has been made. If Phase 2 proceeds using the DR doc's proposed values without formal sign-off, the platform is built to targets that have not received business approval.  
**Action:** Merge D5 into the DR doc's Open Decisions table (OD-1/OD-2/OD-3). Update the implementation plan to reference the DR doc: "D5 is resolved by sign-off on `disaster-recovery.md` Section 11 items OD-1, OD-2, OD-3." Obtain CTO + Business sign-off on the DR doc's proposed targets. Close D5 formally.  
**Owner:** Business (CTO sign-off) + SRE  
**Gates:** Phase 2 is blocked until D5 is closed.

---

### P1-4: MSK authentication mode (D2) — critical Phase 2 blocker with no trade-off analysis anywhere
**Found by:** Architect (C4)  
**Documents:** `implementation-plan.md` (D2: Critical, gates Phase 2), `architecture-decisions.md`, `s3-kafka-design.md §3`  
**Issue:** D2 (IAM auth vs SASL/SCRAM) gates Phase 2 and is marked Critical, but no document records the trade-offs between the two options. The ADR is silent on MSK authentication. The S3→Kafka design does not describe how Glue jobs authenticate to MSK. The Phase 1 Terraform MSK module notes "auth per D2" — it cannot be written. No one has the information needed to make this decision.  
**Action:** Add an ADR entry for MSK authentication (even if still Proposed) covering: IAM auth (simpler operationally, no credential rotation, per-request latency overhead, not retrofittable after creation) vs SASL/SCRAM (Secrets Manager rotation required, broader Kafka client library compatibility). Assign a named owner and a decision deadline aligned with Phase 1 completion.  
**Owner:** Security + Platform  
**Gates:** Phase 1 Terraform MSK module and Phase 2 are blocked.

---

### P1-5: YAML config security fields absent from both approved design documents
**Found by:** Security (C4), Architect (cross-reference gap 9)  
**Documents:** `ingestion-design.md §9` (Approved), `s3-kafka-design.md §9` (Approved), `security-data-privacy.md §2.3, §3`  
**Issue:** Both approved design documents define the YAML config schema. Neither includes `data_classification`, `pii_fields`, `gdpr_erasure_strategy`, `retention_raw_days`, `retention_curated_days` — the mandatory security fields. The S3→Kafka design is the template for all four ingestion patterns, so this gap propagates to CDC, API, and Event patterns. Teams building against these Approved documents will produce non-compliant configs.  
**Action:** Update both design documents to include all mandatory security YAML fields with examples. Add `data_classification` as a required field with allowed values `public|internal|confidential|restricted_pii`. Formally revise the document status (Approved → Revised). Also update the YAML schema in the dataset onboarding checklist.  
**Owner:** Platform + Security  
**Gates:** Resolves the root cause of P1-6 below.

---

### P1-6: CI config validator does not check mandatory security fields — non-compliant configs reach production today
**Found by:** Security (C5)  
**Documents:** `deployment-cicd.md §3.5` (`ci/validate_config.py`), `security-data-privacy.md §2.3`  
**Issue:** CI validator checks structural fields (`dataset_name`, `domain`, `schema_id`, etc.) but not `data_classification`, `pii_fields`, or `gdpr_erasure_strategy`. A config without these fields passes CI and can be merged and deployed. This is an active production path for non-compliant datasets.  
**Action:** Add to `ci/validate_config.py`: (1) `data_classification` must be present and one of the allowed values; (2) if `confidential` or `restricted_pii`, `pii_fields` must be a non-empty list; (3) if `restricted_pii`, `gdpr_erasure_strategy` must be present. Block PR merge (not just warn) on failure. Resolve P1-5 first so the YAML schema is formally defined.  
**Owner:** DevOps + Platform  
**Gates:** Must be in place before any dataset onboards to staging.

---

### P1-7: Schema subject naming — 4 incompatible formats across 4 documents
**Found by:** Data Engineer (C1), Technical Writer (Terminology §8)  
**Documents:** `schema-governance.md §6.1` (`{domain}-{dataset}`), `dataset-onboarding.md §3` (`ods-{domain}-{dataset}`), `consumer-onboarding.md §5.1` (`ods.{domain}.{dataset}`), `data-lineage.md §6.2` (yet another form)  
**Issue:** Four different formats for the same identifier. Any CI tooling or automation built on one format will silently reject schemas registered under the others. Dataset engineers and consumer engineers will look for incompatible schema subjects.  
**Action:** Decide on one canonical format. Strongest candidate: `ods.{domain}.{dataset}` (dot-separated, matches topic name convention as used in `schema-governance.md §12` governance table and `consumer-onboarding.md §5.1`). Update every reference in all four documents, the ADR, and the dataset onboarding YAML example. Mark `schema-governance.md §6.1` as the authority; add one-line cross-references from the other three documents pointing to it.  
**Owner:** Data Governance + Platform  
**Gates:** Must resolve before any schema tooling or CI checks are built.

---

### P1-8: Two conflicting DDL schemas for `pipeline.glue_job_log`
**Found by:** Technical Writer (Critical Issue 1), Data Engineer (M5 cross-reference)  
**Documents:** `s3-kafka-design.md §3`, `data-retention.md §6.3`  
**Issue:** The two DDL definitions have different column names (`run_id` vs `job_run_id`, `record_count` vs `output_rows`, `error_reason`/`error_detail` vs `error_message`). Every query across the failure docs, lineage doc, and observability doc uses the design doc column names. The retention doc DDL also adds `PARTITION BY RANGE` which the design doc DDL omits — and the partition is needed for the retention archival strategy to work.  
**Action:** Align the retention doc DDL to the design doc DDL exactly (design doc is authoritative). Add `PARTITION BY RANGE (created_at)` to the design doc DDL to support the archival strategy. Add a note in the retention doc: "See `s3-kafka-design.md §3` for the canonical schema." Update the data lineage doc which references `pipeline.run_log` — see P1-9.  
**Owner:** Platform  
**Gates:** Blocks DDL for Phase 1.

---

### P1-9: `pipeline.lineage` / `pipeline.run_log` — referenced across multiple phases but never defined
**Found by:** Architect (C3), Senior Developer (S5), Technical Writer (Critical Issue 3)  
**Documents:** `implementation-plan.md` Phases 1, 3, 6, 7; `data-lineage.md` (uses `pipeline.run_log`); `s3-kafka-design.md` (no definition)  
**Issue:** `pipeline.lineage` is a Phase 3 deliverable (publish Glue job writes to it), a Phase 1 DDL item, and a Phase 6/7 testing dependency. It is defined nowhere. `data-lineage.md` independently introduces `pipeline.run_log` with its own schema — appearing to be either a rename or a separate concept, never clarified. Engineers cannot build Phase 3 without this schema.  
**Action:** (1) Decide whether `pipeline.lineage` and `pipeline.run_log` are the same table or different. (2) Define the canonical DDL in `s3-kafka-design.md §3` with at minimum: `run_id`, `source_type`, `source_ref`, `target_topic`, `message_key`, `kafka_partition`, `kafka_offset`, `business_date`, `created_at`. (3) Update `data-lineage.md` to either adopt this DDL or explicitly document the distinction. (4) Add an ADR entry covering the lineage table's purpose, retention, and why it is in PostgreSQL rather than the audit Kafka topic.  
**Owner:** Platform + Data Governance  
**Gates:** Phase 3 build cannot start without the DDL.

---

## P2 — Operational Safety (resolve before production go-live)

### P2-1: Partial Glue ETL crash can publish partial data to Kafka permanently with no alarm
**Found by:** Senior Developer (C2), SRE (C5)  
**Documents:** `ingestion-failure-and-recovery.md §1.8`, `disaster-recovery.md` (no coverage)  
**Issue:** Recovery step 1.8 says "accept overwrite at publish layer" as an option. This is incorrect: if the partial Parquet already triggered the publish pipeline and it completed with `status=completed`, a subsequent correct Parquet will trigger EventBridge again, but the idempotency guard will block re-publishing. Partial data remains in Kafka permanently, silently, with no alarm.  
**Action:** (1) Remove "or accept overwrite at publish layer" from step 1.8. Make partial file deletion mandatory. (2) Add a mandatory step: reset `pipeline.file_state` for the affected S3 Curated path to `new` before retrying (cross-reference `s3-kafka-failure-and-recovery.md §3.3`). (3) Add Scenario 3.3.1b to the DR doc: Glue ETL crash with partial S3 write. Recommended mitigation: write Parquet to a staging prefix and atomically rename to the final prefix on job completion; EventBridge rule targets final prefix only.  
**Owner:** Platform + SRE  

---

### P2-2: `status=processing` dead-end — no recovery path when all MWAA retries are exhausted
**Found by:** Senior Developer (C3)  
**Documents:** `s3-kafka-failure-and-recovery.md §2.4, §2.5`  
**Issue:** If a Glue crash exhausts all MWAA retries, `status` remains `processing` indefinitely. The idempotency guard does not block `processing` files (only `completed`), but there is no documented recovery path and no alarm for files stuck in `processing` beyond a reasonable timeout. The file cannot be automatically or manually re-run without knowing to do a manual SQL reset.  
**Action:** (1) Clarify in the failure doc what the DAG's on-failure handler does to PostgreSQL state when retries are exhausted — must set to `failed`, not leave at `processing`. (2) Add a CloudWatch alarm for files remaining at `status=processing` beyond a configurable timeout (suggested: 2 hours). (3) Add a recovery procedure: manual SQL to reset `processing → new` before resubmission.  
**Owner:** Platform  

---

### P2-3: Ingestion recovery step 1.8 — DAG 2 re-trigger not documented (on-call dead end)
**Found by:** Senior Developer (C1), SRE (M4)  
**Documents:** `ingestion-failure-and-recovery.md §1.8`  
**Issue:** Recovery says "Reset file state to `transferred` to skip SFTP copy." No guidance on what to trigger next. An on-call engineer has no documented path for re-running DAG 2 independently after a mid-ETL crash.  
**Action:** Add explicit step: "After resetting state to `transferred`, manually trigger DAG 2 in MWAA (not DAG 1 — DAG 1 would attempt a redundant SFTP copy). Cross-reference `s3-kafka-failure-and-recovery.md §3.3` for how to handle the publish pipeline if it already ran against the partial Parquet." Add a state machine diagram (see P4-3) showing valid state transitions so engineers understand why `transferred` vs `new` produces different behaviour.  
**Owner:** Platform  

---

### P2-4: DAG bucket name mismatch — DR runbook points to wrong bucket at 2am
**Found by:** SRE (C2, S3)  
**Documents:** `disaster-recovery.md §3.2.2, §4.4` (uses `ods-config-{env}/dags/`), `deployment-cicd.md §6.3` and `implementation-plan.md` Phase 1 (both use `ods-dags-{env}`)  
**Issue:** An engineer following the MWAA Environment Failure recovery runbook at 2am will point the new MWAA environment at `ods-config-{env}/dags/`. The actual DAG bucket is `ods-dags-{env}`. The environment provisions successfully but no DAGs load.  
**Action:** Update `disaster-recovery.md §3.2.2` and `§4.4` to use `ods-dags-{env}` throughout. Confirm which bucket is authoritative (CI/CD and impl plan both say `ods-dags-{env}`) and grep all documents for the incorrect reference.  
**Owner:** Platform + SRE  

---

### P2-5: P1 alarms routing to the ADR instead of runbooks
**Found by:** SRE (C4), cross-confirmed by Architect (gap 4)  
**Documents:** `observability.md §9.1, §9.3`  
**Issue:** `ods-mwaa-queue-critical-{env}`, `ods-rds-connections-critical-{env}` (both P1), and `ods-eventbridge-failure-{env}` (P1) all point to `architecture-decisions.md` as their runbook. The ADR explains design rationale; it does not tell an on-call engineer what to do.  
**Action:** Update the runbook reference column in `observability.md`: `ods-mwaa-queue-*` → `disaster-recovery.md §3.2.1`; `ods-rds-connections-*` → `disaster-recovery.md §3.4.1`; `ods-eventbridge-failure-{env}` → `disaster-recovery.md §3.7.1`.  
**Owner:** SRE  

---

### P2-6: Missing DR runbooks for CDC, API, and Event patterns — P1 alarms pointing to (TBD)
**Found by:** SRE (C3)  
**Documents:** `observability.md §9.4, §9.5, §9.6`, `disaster-recovery.md §3`  
**Issue:** Six P1/P2 alarms for CDC, API, and Event patterns point to `(TBD)` runbooks. No such runbooks exist. The CDC replication slot bloat scenario can cause disk exhaustion on the source DB with zero documented response path.  
**Action:** Before each pattern goes to production: add DR scenario sections 3.10–3.12 covering CDC connector failure, API cursor drift, and event routing failures. At minimum, the replication slot bloat runbook must exist before CDC goes live — it is the one scenario with cascading physical impact.  
**Owner:** SRE + Platform  

---

### P2-7: Stale Kafka header `x-ods-source-path` in consumer-onboarding guide
**Found by:** Data Engineer (C3), Technical Writer (S9)  
**Documents:** `consumer-onboarding.md §5.4`, `data-lineage.md §3.1`  
**Issue:** Consumer guide documents `x-ods-source-path`. Lineage doc explicitly states it was replaced by `x-ods-source-ref` and a new `x-ods-source-type` was added. Consumers built on the guide will look for a header that doesn't exist and miss one that does.  
**Action:** Replace the header table in `consumer-onboarding.md §5.4` with the authoritative six-header set from `data-lineage.md §3.1` (`x-ods-run-id`, `x-ods-source-type`, `x-ods-source-ref`, `x-ods-business-date`, `x-ods-schema-version`, `x-ods-pipeline-type`). Add: "For the full header spec including per-pattern `source-ref` formats, see the Data Lineage Design."  
**Owner:** Data Governance  

---

### P2-8: Flyway DB password exposed as CLI argument (visible in ps aux and CI logs)
**Found by:** Security (C7)  
**Documents:** `deployment-cicd.md §9.5`  
**Issue:** `flyway -password="${DB_PASSWORD}"` — process arguments are visible in `/proc/<pid>/cmdline` and `ps aux` to co-tenanted workloads and appear in CI logs if debug logging is on.  
**Action:** Switch to environment variable form: set `FLYWAY_PASSWORD` as an environment variable sourced from GitHub Actions secrets or AWS Secrets Manager. Remove the `-password` CLI flag. Verify CI log masking is in place for all secret values.  
**Owner:** DevOps  

---

### P2-9: `pipeline.reconciliation_log` DDL missing partitioning — retention archival unimplementable
**Found by:** Data Engineer (C4)  
**Documents:** `reconciliation-design.md §3.5` (DDL), `data-retention.md §3.2, §6.4`  
**Issue:** Retention doc assumes monthly partition-drop archival for `pipeline.reconciliation_log` (same as `glue_job_log`). Reconciliation doc's DDL uses `BIGSERIAL PRIMARY KEY` with no `PARTITION BY` clause. The archival strategy cannot be applied to an unpartitioned table without costly row-by-row DELETE.  
**Action:** Add `PARTITION BY RANGE (created_at)` to the `pipeline.reconciliation_log` DDL in `reconciliation-design.md §3.5`. Add a cross-reference to `data-retention.md §6` for the partition management strategy.  
**Owner:** Platform  

---

### P2-10: DLQ resubmission procedure defeated by Glue Job Bookmarks (silent non-reprocess)
**Found by:** Senior Developer (S6)  
**Documents:** `s3-kafka-failure-and-recovery.md §3.2`, `s3-kafka-design.md §8`  
**Issue:** The DLQ resubmission procedure says "copy the fixed file back to the original S3 path." The design doc says Glue Job Bookmarks are enabled. Bookmarks record the original path as already processed — Glue silently skips it. No records are published. No error is raised.  
**Action:** Either: (a) specify that Glue Job Bookmarks must be reset before DLQ resubmission (add the AWS CLI reset command to the procedure); or (b) always use a new S3 path for resubmission and update the idempotency reset SQL to use the new path. Document the chosen approach in the operational checklist in Section 4.  
**Owner:** Platform  

---

## P3 — Should Fix Before Go-Live

### P3-1: Consumer-onboarding says topic retention is "TBD" — the retention doc decided 7 days
**Found by:** Data Engineer (S1)  
**Documents:** `consumer-onboarding.md §1.4, §7.1`, `data-retention.md §4.3`  
**Action:** Replace TBD text with: "Topic retention is 7 days (`retention.ms=604800000`). Do not design a consumer that relies on history beyond 7 days." Cross-reference the retention doc.  
**Owner:** Data Governance  

---

### P3-2: Consumer group naming convention inconsistent between the two onboarding documents
**Found by:** Data Engineer (S2), Technical Writer (S10)  
**Documents:** `consumer-onboarding.md §3.1` (`{team}.{application}.{dataset}`), `dataset-onboarding.md §3 Step 12` (`cg.{team}.{dataset}`)  
**Issue:** The IAM policy in consumer-onboarding is scoped to the three-segment format. A consumer following the dataset-onboarding notification will be denied.  
**Action:** Remove `cg.{team}.{dataset}` from `dataset-onboarding.md` Step 12. Replace with the three-segment `{team}.{application}.{dataset}` format. Add a cross-reference link to `consumer-onboarding.md §3`.  
**Owner:** Data Governance  

---

### P3-3: Kafka 7-day retention leaves only 1-day tombstone margin for GDPR erasure consumers
**Found by:** Data Engineer (C2)  
**Documents:** `data-retention.md §4.3`, `reconciliation-design.md §4.3, §10`  
**Issue:** `min.compaction.lag.ms` = 5 days + `delete.retention.ms` = 1 day = tombstone visible for 6 days in a 7-day window. Any consumer at 5-day lag loses tombstone visibility — meaning GDPR erasure tombstones may be invisible to lagging consumers. The reconciliation doc open item never closes this.  
**Action:** Either increase `retention.ms` to 14 days for topics with CDC delete operations or T3 tombstone checks, or explicitly accept the 1-day margin and mandate a consumer lag alarm at the 4-day mark. Close the open item in `reconciliation-design.md §10` with the accepted decision. Cross-reference both documents.  
**Owner:** Platform + Data Governance  

---

### P3-4: CDC schema compatibility mode contradicts between schema-governance and dataset-onboarding
**Found by:** Data Engineer (C5)  
**Documents:** `schema-governance.md §5.2` (`FULL`), `dataset-onboarding.md §4 Step 3` (`FORWARD`)  
**Action:** Align `dataset-onboarding.md §4 Step 3` to `FULL`, consistent with the governance doc and the governance table. If `FORWARD` is genuinely needed as an exception for high-churn CDC sources, document it as a named exception in `schema-governance.md §5.2`, not a silent contradiction.  
**Owner:** Data Governance  

---

### P3-5: Dual PII classification systems — `none/low/medium/high` vs `Tier 1–4` with no mapping
**Found by:** Security (S1)  
**Documents:** `schema-governance.md §12` (`pipeline.schema_governance` table), `security-data-privacy.md §2.1`  
**Action:** Standardise on one classification system. Recommendation: security doc's Tier 1–4 framework (more precisely defined, already used in IAM access control matrix). Update `pipeline.schema_governance` DDL to use `data_classification VARCHAR CHECK (data_classification IN ('public','internal','confidential','restricted_pii'))`. Write a migration for any existing `none/low/medium/high` values.  
**Owner:** Security + Data Governance  

---

### P3-6: Dual PII field tagging conventions in Avro schemas
**Found by:** Security (S2)  
**Documents:** `schema-governance.md §6.4` (`"pii": true`), `security-data-privacy.md §2.2` (`"doc": "PII:personal_data"`)  
**Action:** Standardise on the Avro field property approach (`"pii": true`, `"pii_category"`, `"gdpr_article9": true` — machine-parseable without string matching). Update `security-data-privacy.md §2.2` to reference the schema governance convention. Add a CI schema linting rule rejecting `doc`-based PII tagging.  
**Owner:** Security + Data Governance  

---

### P3-7: `pipeline.lineage.business_key` potentially re-identifies data subjects — not in GDPR erasure scope
**Found by:** Security (S3)  
**Documents:** `data-lineage.md §4–6`, `security-data-privacy.md §3.2`  
**Action:** (1) Assess with DPO whether `pipeline.lineage.business_key` constitutes personal data for Restricted-PII datasets. (2) If yes, add `pipeline.lineage` to the erasure procedure: on erasure, null out `business_key` for affected rows. (3) Add a retention policy for `pipeline.lineage` to the data retention doc (suggested: 90 days for completed-run rows, 7 years for error/audit rows).  
**Owner:** Security + DPO + Platform  

---

### P3-8: Kafka message headers expose internal infrastructure to all consumers
**Found by:** Security (S4)  
**Documents:** `data-lineage.md §5`  
**Issue:** `x-ods-source-ref` contains S3 bucket paths, internal API URLs with cursor tokens, and RDS+LSN positions — visible to any ACL-authorised consumer.  
**Action:** Move full lineage metadata to the `pipeline.lineage` side-channel table. Reduce Kafka headers to `x-ods-run-id` only. Consumers resolve full lineage from the lineage table using the run ID. This is the minimum-necessary-information principle.  
**Owner:** Platform + Security  

---

### P3-9: No SAST, SCA, or secrets detection in the CI pipeline
**Found by:** Security (S7)  
**Documents:** `deployment-cicd.md §3`  
**Action:** Add three required status checks before PR merge: (1) Semgrep SAST with `p/owasp-top-ten` and `p/python` rulesets on all Python source; (2) Trivy or pip-audit SCA on requirements files, blocking on CRITICAL/HIGH CVEs; (3) Gitleaks secrets detection, blocking on any detected secret.  
**Owner:** DevOps + Security  

---

### P3-10: Dataset onboarding Kafka topic uses `cleanup.policy=delete` instead of `compact,delete`
**Found by:** Data Engineer (M2)  
**Documents:** `dataset-onboarding.md §3 Step 2`, `data-retention.md §4.3`  
**Issue:** A dataset onboarded with `delete`-only policy has no compaction semantics, no safe consumer catch-up by replaying latest state, and no tombstone mechanism for GDPR erasure.  
**Action:** Update the `kafka-topics.sh` command in `dataset-onboarding.md §3 Step 2` to use `--config cleanup.policy=compact,delete` plus the full recommended config from `data-retention.md §4.3` (`min.compaction.lag.ms`, `delete.retention.ms`). Note: `delete`-only remains correct for audit and reconciliation system topics.  
**Owner:** Data Governance  

---

### P3-11: Schema example in dataset-onboarding missing 5 required metadata fields
**Found by:** Data Engineer (S3)  
**Documents:** `dataset-onboarding.md §3 Step 1` (Avro example), `schema-governance.md §6.5, §7.2`  
**Issue:** The schema governance doc mandates `event_id`, `created_at`, `ingested_at`, `source_system`, `schema_version` on every schema. The onboarding Avro example contains none of them. An engineer following the runbook will fail the schema review checklist.  
**Action:** Update the Avro examples in `dataset-onboarding.md` (§3 Step 1 for S3 batch, §4 Step 3 for CDC) to include all five required metadata fields. Alternatively, add a callout box immediately after each example pointing to `schema-governance.md §6.5`.  
**Owner:** Data Governance  

---

### P3-12: No consumer offboarding procedure anywhere in the document set
**Found by:** Data Engineer (S4)  
**Documents:** `consumer-onboarding.md` (missing), `schema-governance.md §10.2`, `dataset-onboarding.md §12`  
**Issue:** Dataset offboarding exists; consumer offboarding does not. Disconnected consumers without deregistering will pollute IAM policies, impact analyses for breaking schema changes, and lag monitoring forever.  
**Action:** Add a "Consumer Offboarding" section to `consumer-onboarding.md` covering: platform notification, `pipeline.schema_consumers` deregistration, IAM policy revocation, consumer group deletion, lag alarm removal. Cross-reference `schema-governance.md §10.2`.  
**Owner:** Data Governance  

---

### P3-13: SLO targets and DR RTO not calibrated — a Glue failure breaches the latency SLO before recovery completes
**Found by:** SRE (S1)  
**Documents:** `observability.md §4.2` (e2e latency p95 < 10 min), `disaster-recovery.md §2.2` (single component RTO = 15 min)  
**Action:** Add a column to the DR RTO table: "SLO impact at this RTO." For each scenario, state whether the 10-min e2e SLO is breached and what error budget consumption rate results. Force an explicit business discussion on whether P95 < 10-min is achievable with a 15-min component RTO.  
**Owner:** SRE + Business  

---

### P3-14: Consumer lag alarm thresholds defined in two incompatible ways
**Found by:** SRE (S7)  
**Documents:** `observability.md §3.3` (50K/200K absolute count), `implementation-plan.md` Phase 4 ("not shrinking for 15 min")  
**Action:** Reconcile into one threshold set in the observability doc. Keep the absolute count thresholds (50K/200K) as the primary alarms. Add a "staleness alarm" (lag not shrinking for N minutes) as a third alarm type. Update the implementation plan to reference the observability doc as the authority.  
**Owner:** SRE  

---

## P4 — Minor / Polish

| # | Issue | Document(s) | Owner |
|---|-------|-------------|-------|
| P4-1 | `ods-publish-flow.mmd` shows superseded Airflow Sensor trigger — all other docs show EventBridge | `ods-publish-flow.mmd` | Platform |
| P4-2 | Ingestion context diagram in design doc still shows Airflow Sensor, not EventBridge | `ingestion-design.md §1` | Platform |
| P4-3 | No state machine diagram — 3 different state values (`transferred`, `new`, `pending`) used across docs for "retry this file" | `ingestion-failure-and-recovery.md`, `disaster-recovery.md §5.2` | Platform |
| P4-4 | Testing Scenario 5 asserts `PostgreSQL state=schema_error` — not a valid status value (should be `failed, error_reason=schema_incompatible`) | `testing-strategy.md` | Platform |
| P4-5 | Testing Scenario 2 asserts "File moved to S3 quarantine" — design says file stays on SFTP; status `quarantined` is not a valid enum value | `testing-strategy.md` | Platform |
| P4-6 | Both failure docs missing date/status/scope metadata header | `s3-kafka-failure-and-recovery.md`, `ingestion-failure-and-recovery.md` | Platform |
| P4-7 | `reconciliation-design.md` line 1 starts with stray `ar#` prefix | `reconciliation-design.md` | Platform |
| P4-8 | SFTP polling latency (up to 5 min per poll cycle) not reflected in the SLO table — SLO says "from S3 Curated" not from SFTP arrival | `architecture-decisions.md §4.1` | Platform |
| P4-9 | Idempotency layers: ADR presents one model for both pipelines, but ingestion and publish have genuinely different three-layer models | `architecture-decisions.md §1.5` | Platform |
| P4-10 | Observability doc labels publish DAG as "DAG 3" — publish DAG is unnamed numerically in design docs | `observability.md §2.1` | Platform |
| P4-11 | `dq_passed` and `dq_warned` are mutually exclusive per run — ADR and status sequence imply they are sequential | `architecture-decisions.md §1.11` | Platform |
| P4-12 | DLQ partition key inconsistency: `topic=` (publish pipeline), `dataset=` (ingestion pipeline) — correct but never stated as intentional | `architecture-decisions.md §1.7`, both failure docs | Platform |
| P4-13 | Glue Job Bookmarks mentioned in publish pipeline design but absent from ingestion design — intentional or gap? | `s3-kafka-design.md §8`, `ingestion-design.md` | Platform |
| P4-14 | `etl_processing` status in ingestion DDL not shown in any state transition in the commentary | `ingestion-design.md §4`, `ods-ingestion-commentary.md` | Platform |
| P4-15 | Acronyms not defined on first use: DQDL, MWAA, LSN, DPIA, DPO | Multiple docs | Technical Writer |
| P4-16 | DR doc Section 8.6 Mermaid diagram references "Option F" which does not exist as a label | `disaster-recovery.md §8.6` | SRE |
| P4-17 | Flyway undo naming: `V{N}__.undo.sql` (CI/CD doc) vs `U{N}__` (implementation plan) — incompatible with open-source Flyway | `deployment-cicd.md §9.2`, `implementation-plan.md` Phase 8 | DevOps |
| P4-18 | GLUE-001 alarm threshold conflict: DR doc says P2 at 3 failures in 30 min; observability doc says P1 at 1 failure | `disaster-recovery.md` Appendix A, `observability.md §3.2` | SRE |

---

## Documentation Structure Actions (no technical change required)

These are structural improvements that help all future readers and reduce the risk of future contradictions.

| # | Action | Owner |
|---|--------|-------|
| DS-1 | Create `docs/plans/README.md` (or `INDEX.md`) with the Document Map (6 groups) and Recommended Reading Order produced by the Technical Writer review | Technical Writer |
| DS-2 | Add a "Document Relations" section to each design doc listing companion failure doc, companion diagrams, and related governance docs | Technical Writer |
| DS-3 | The EventBridge→MWAA REST API trigger mechanism is described only in commentary docs — move it into `architecture-decisions.md §1.2a` as a sub-decision with the integration pattern, IAM requirements, and DAG trigger payload schema | Platform |
| DS-4 | Add an ADR entry for `file_catalogue` whitelist pattern: why PostgreSQL vs YAML-only allowlist, which is the source of truth, who owns catalogue entries in production | Platform |
| DS-5 | Add an ADR entry for count reconciliation: offset-delta mechanism, exclusive partition write assumption, relationship to T2/T3 reconciliation | Platform |
| DS-6 | Add a note to `consumer-onboarding.md §9.2` about the upcoming T2/T3 consumer state store dependency — consumers will be asked to expose count endpoints; mechanism is TBD | Data Governance |
| DS-7 | Standardise on `pipeline.file_state` (schema-qualified, no database prefix) for the publish pipeline state table across all documents and diagrams; `ods.pipeline.file_state` in the sequence diagram is inconsistent | Platform |
| DS-8 | `ods.pipeline.reconciliation` Kafka topic (from reconciliation design) is not listed in `s3-kafka-design.md §3` MSK topics section — add it alongside `ods.pipeline.audit` | Platform |

---

## Issue Count Summary

| Priority | Count | Deadline |
|---|---|---|
| P0 — Regulatory/Legal | 3 | Before any production-bound sprint |
| P1 — Architecture Contradictions | 9 | Before Phase 3 build starts |
| P2 — Operational Safety | 10 | Before production go-live |
| P3 — Should Fix | 14 | Before go-live |
| P4 — Minor/Polish | 18 | Before go-live (low effort) |
| Documentation Structure | 8 | Ongoing |

---

*Consolidated by: Document Review Team (Architect, Senior Developer, Data Engineering Lead, SRE Lead, Security Engineer, Technical Writer). 2026-04-16.*
