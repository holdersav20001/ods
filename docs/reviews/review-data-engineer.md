# Data Governance Review — Data Engineering Lead
**Reviewer role:** Data Engineering Lead / Data Governance Lead  
**Date:** 2026-04-16  
**Documents reviewed:**
1. `2026-04-15-data-lineage.md`
2. `2026-04-15-data-retention.md`
3. `2026-04-15-schema-governance.md`
4. `2026-04-15-dataset-onboarding.md`
5. `2026-04-15-consumer-onboarding.md`
6. `2026-04-15-reconciliation-design.md`
7. `2026-04-14-architecture-decisions.md` (context)
8. `2026-04-14-ingestion-design.md` (context)
9. `2026-04-15-security-data-privacy.md` (context)

---

## Executive Summary

The six governance documents are substantially well-written and individually coherent; each document covers its subject with sufficient depth for a draft. However, the set has not yet been edited as a suite: schema subject naming conventions differ across three documents, the consumer-onboarding guide contains several stale or under-specified references (TBD retention, an outdated header name), and the two onboarding documents do not form a complete end-to-end path because the hand-off from dataset go-live to consumer registration in the governance table is never made explicit. Reconciliation is the weakest area of cross-document integration: the retention doc sets a 7-day Kafka window while the reconciliation doc identifies tombstone-expiry as a risk without resolving it, and the retention doc defines `pipeline.reconciliation_log` with a 90-day hot window while the reconciliation doc's DDL for that same table is defined without partitioning. Three significant data lifecycle gaps exist: there is no dataset deprecation / offboarding procedure that triggers corresponding consumer offboarding, the consumer-onboarding guide has no consumer offboarding section at all, and data quality result retention is defined in the retention doc but never referenced in the onboarding checklist. The team should resolve the schema subject naming contradiction before it propagates into any tooling, and should close the Kafka 7-day vs T3-tombstone gap before production deployment of any Tier 4 dataset.

---

## Critical Issues (must fix)

### C-1: Schema subject naming convention contradicts itself across three documents

**Issue:** Three documents define the schema subject naming pattern differently and none of them agree.

- `schema-governance.md` Section 6.1 defines the pattern as `{domain}-{dataset}`, e.g. `insurance-policies`.
- `dataset-onboarding.md` Step 1 (S3 batch, Step 3 CDC) instructs engineers to use `ods-{domain}-{dataset}`, e.g. `ods-insurance-policies`.
- `consumer-onboarding.md` Section 5.1 states: "Schema names follow the pattern: `ods.{domain}.{dataset}` — matching the topic name."

The `schema-governance.md` Section 12 governance table itself uses `insurance-policies` (no `ods-` prefix). The `data-lineage.md` Section 6.2 Glue CLI example uses `ods-schema-registry-prod` with `SchemaName: ods.insurance.policies` (dot-separated, with prefix). This is four distinct formats for the same identifier. Any automation or CI check built on one of these patterns will silently reject all schemas registered under the others.

**Location:** `schema-governance.md` §6.1; `dataset-onboarding.md` §3 Step 1, §4 Step 3; `consumer-onboarding.md` §5.1; `data-lineage.md` §6.2.

**Suggested fix:** Decide on one canonical format — the strongest candidate given the `{domain}-{dataset}` form already in the governance table is `{domain}-{dataset}` (e.g. `insurance-policies`). Update every reference in all six documents and the ADR to use that single format. Annotate the rule clearly in `schema-governance.md` §6.1 and add a one-line note in `dataset-onboarding.md` and `consumer-onboarding.md` pointing back to §6.1 as the authority.

---

### C-2: Kafka 7-day retention is likely insufficient for the T3 tombstone guarantee

**Issue:** `data-retention.md` §4.3 sets `retention.ms=604800000` (7 days) for all business data topics. `reconciliation-design.md` §4.3 explicitly warns: "If the Kafka topic has a retention policy shorter than the period between T3 reconciliation checks, tombstones may have been compacted away before T3 runs." Section 4.3 of the reconciliation doc then gives an example showing a tombstone emitted at `2026-04-14 22:00` that must still be present for the T3 check at `2026-04-15 06:00` — that example is fine with 7 days. However the reconciliation doc §3.5 defines T3 as a daily check, `data-retention.md` §4.3 states tombstones need `min.compaction.lag.ms = 432,000,000` (5 days), and the reconciliation doc §10 open items says "Is 7-day retention sufficient?" without resolving it. The retention doc answers the tombstone lag question by setting `delete.retention.ms=86400000` (1 day), which means a tombstone is eligible for deletion just 1 day after the compaction lag expires. The combined guarantee is 5 days compaction lag + 1 day delete retention = tombstone visible for 6 days — within a 7-day topic window. The margin is exactly 1 day. Any consumer with lag approaching the 5-day compaction lag threshold will lose tombstone visibility.

**Location:** `data-retention.md` §4.3; `reconciliation-design.md` §4.3, §10.

**Suggested fix:** Either increase `retention.ms` to 14 days for any topic that has CDC delete operations or is subject to T3 tombstone checks, or document explicitly in both documents that the 7-day window is accepted with the acknowledged 1-day margin and that consumer lag alarms at the 4-day mark are mandatory. Close the open item in `reconciliation-design.md` §10 with the accepted decision. Cross-reference both documents.

---

### C-3: Consumer-onboarding guide documents a stale Kafka message header name

**Issue:** `consumer-onboarding.md` §5.4 lists the header `x-ods-source-path` as the S3 path of the source file. `data-lineage.md` §3.1 explicitly states: "`x-ods-source-ref` replaces the old `x-ods-source-path` (which was S3-only)." The lineage doc further documents `x-ods-source-type` as a new header that the consumer-onboarding guide does not list at all. A consumer built on the consumer-onboarding guide will look for `x-ods-source-path` and find nothing; they will miss `x-ods-source-type` entirely.

**Location:** `consumer-onboarding.md` §5.4; `data-lineage.md` §3.1.

**Suggested fix:** Replace the header table in `consumer-onboarding.md` §5.4 with the authoritative six-header set from `data-lineage.md` §3.1 (`x-ods-run-id`, `x-ods-source-type`, `x-ods-source-ref`, `x-ods-business-date`, `x-ods-schema-version`, `x-ods-pipeline-type`). Add a sentence: "For the full header specification including per-pattern `source-ref` formats, see the Data Lineage Design."

---

### C-4: `pipeline.reconciliation_log` DDL defined without partitioning, contradicting the retention strategy

**Issue:** `data-retention.md` §3.2 sets `pipeline.reconciliation_log` to 90-day hot retention with archival to S3 after 1 year, and §6.4 states "The same archive pattern applies to `pipeline.reconciliation_log`, with adjusted retention windows per the table in section 3.2." This clearly assumes the table is partitioned identically to `glue_job_log`. However, `reconciliation-design.md` §3.5 provides the DDL for `pipeline.reconciliation_log` with `BIGSERIAL PRIMARY KEY` and no `PARTITION BY` clause. The archive Glue job pattern (monthly partition drop) cannot be applied to an unpartitioned table without a costly row-by-row DELETE. This means the retention strategy documented for this table cannot be implemented as written.

**Location:** `reconciliation-design.md` §3.5 (DDL); `data-retention.md` §3.2, §6.4.

**Suggested fix:** Add `PARTITION BY RANGE (created_at)` to the `pipeline.reconciliation_log` DDL in `reconciliation-design.md` §3.5, consistent with the `glue_job_log` pattern in `data-retention.md` §6.3. Add a note in the reconciliation doc pointing to `data-retention.md` §6 for the partition management strategy.

---

### C-5: CDC compatibility mode contradicts schema-governance recommendation in dataset-onboarding

**Issue:** `schema-governance.md` §5.2 recommends `FULL` compatibility mode for CDC / Avro. `dataset-onboarding.md` §4 Step 3 instructs engineers onboarding a CDC dataset to register with `FORWARD` compatibility mode: "Compatibility mode: `FORWARD` (CDC sources have higher schema churn; new fields added to the source table must not break existing consumers)." These are contradictory instructions. A team following the onboarding runbook will register CDC schemas as `FORWARD`; the schema governance doc says `FULL` is required. The governance table in `schema-governance.md` §12 shows `ods.motor.drivers` (a CDC topic) with `FULL` — consistent with the governance doc but contradicting the onboarding runbook.

**Location:** `schema-governance.md` §5.2, §12; `dataset-onboarding.md` §4 Step 3.

**Suggested fix:** Align `dataset-onboarding.md` §4 Step 3 to use `FULL` compatibility, consistent with `schema-governance.md` §5.2 and the governance table. If there is a deliberate reason to allow `FORWARD` for CDC (e.g. high source schema churn), that exception must be documented in `schema-governance.md` §5.2 as a named exception, not a silent contradiction.

---

## Significant Issues (should fix)

### S-1: Consumer-onboarding guide states topic retention is "TBD" — the retention doc has decided it

**Issue:** `consumer-onboarding.md` §1.4 states: "Infinite retention. Topic retention has not yet been formally decided (TBD)." Section 7.1 repeats: "Topic retention has not yet been formally decided (TBD)." `data-retention.md` §4.3 has decided it: 7-day `retention.ms`, `compact,delete` for business topics. This contradiction actively misleads consumers into building consumers that rely on topic history beyond 7 days without knowing the risk.

**Location:** `consumer-onboarding.md` §1.4, §7.1; `data-retention.md` §4.3.

**Suggested fix:** Replace the TBD text with: "Topic retention is 7 days (`retention.ms=604800000`) for business data topics. Do not design a consumer that relies on the topic holding more than 7 days of history. See the Data Retention Policy for topic-level configuration details." Update §7.1 to remove the TBD caveat — the initial load process should be described as mandatory for any consumer that needs history beyond 7 days, not as a workaround for an undecided retention policy.

---

### S-2: Consumer group naming convention is inconsistent between the two onboarding documents

**Issue:** `consumer-onboarding.md` §3.1 mandates the pattern `{team}.{application}.{dataset}` (dot-separated, three segments). `dataset-onboarding.md` §12 (Step 12 — notify consumer teams) tells data engineers to recommend the pattern `cg.{team}.{dataset}` (with a `cg.` prefix, only two variable segments). A consumer that follows the dataset-onboarding notification will use `cg.actuarial.policies`; a consumer that follows the consumer-onboarding guide will use `actuarial.risk-model.policies`. The IAM policy in `consumer-onboarding.md` §2.1 is scoped to the `{team}.{application}.{dataset}` format — a consumer using the `cg.` prefix will be denied.

**Location:** `consumer-onboarding.md` §3.1; `dataset-onboarding.md` §3 Step 12.

**Suggested fix:** Remove the `cg.{team}.{dataset}` suggestion from `dataset-onboarding.md` Step 12 and replace with the three-segment `{team}.{application}.{dataset}` format defined in `consumer-onboarding.md` §3. Add a cross-reference link to `consumer-onboarding.md` §3 in the Step 12 notification template.

---

### S-3: Schema-governance metadata fields conflict with what dataset-onboarding registers

**Issue:** `schema-governance.md` §6.5 mandates five required metadata fields on every schema: `event_id`, `created_at`, `ingested_at`, `source_system`, `schema_version`. The example Avro schema in `dataset-onboarding.md` §3 Step 1 includes none of these fields — only `policy_id`, `customer_id`, `premium_amount`, `effective_date`, and `status`. An engineer following the onboarding runbook will produce a schema that fails the review checklist in `schema-governance.md` §7.2 (which explicitly checks for "Required metadata fields present"). This will cause a silent schema review failure at the first real onboarding attempt.

**Location:** `dataset-onboarding.md` §3 Step 1 (Avro example); `schema-governance.md` §6.5, §7.2.

**Suggested fix:** Update the example Avro schemas in `dataset-onboarding.md` (§3 Step 1 for S3 batch, §4 Step 3 for CDC) to include all five required metadata fields from `schema-governance.md` §6.5. Alternatively add a note immediately after the example: "The five required metadata fields (`event_id`, `created_at`, `ingested_at`, `source_system`, `schema_version`) must be added to every schema — see `schema-governance.md` §6.5."

---

### S-4: Dataset offboarding exists in dataset-onboarding but consumer offboarding is entirely absent from consumer-onboarding

**Issue:** `dataset-onboarding.md` §12 covers offboarding a dataset (topic deprecation, YAML config update, gate approval). `schema-governance.md` §10.2 covers topic deprecation and retirement in detail, including tombstone publishing, MSK topic deletion, and marking `schema_consumers` records inactive. However, `consumer-onboarding.md` has no section on consumer offboarding at all. There is no documented procedure for: a consumer team notifying the platform that they are disconnecting, deregistering from `pipeline.schema_consumers`, having their IAM policy revoked, or removing their consumer group from lag monitoring. A consumer that disconnects without deregistering will continue to appear in the impact analysis for future breaking schema changes, requiring manual cleanup.

**Location:** `consumer-onboarding.md` (missing section); `schema-governance.md` §10.2; `dataset-onboarding.md` §12.

**Suggested fix:** Add a "Consumer Offboarding" section to `consumer-onboarding.md` (between §11 and the appendices) covering: notifying the platform team, deregistering from `pipeline.schema_consumers`, IAM policy revocation, consumer group deletion, and lag monitoring alarm removal. Cross-reference `schema-governance.md` §10.2 for the dataset-side deprecation process.

---

### S-5: Security doc defines per-dataset retention fields in YAML config that the retention doc ignores

**Issue:** `security-data-privacy.md` §1.4 defines two dataset-level retention fields in the YAML config: `retention_raw_days` and `retention_curated_days`. `data-retention.md` §5.3–5.4 defines S3 lifecycle rules at the bucket level with prefix filters for per-dataset overrides — but the retention doc does not reference these YAML fields and does not describe the mechanism by which a YAML-specified `retention_raw_days: 90` would override the bucket-level lifecycle rule. A data engineer reading the retention doc will not know these fields exist; a data engineer reading the security doc will not know how to implement them. The two approaches may also conflict if a dataset YAML specifies `retention_raw_days: 365` but the bucket lifecycle rule transitions it to Glacier at 90 days.

**Location:** `security-data-privacy.md` §1.4; `data-retention.md` §5.2–5.4.

**Suggested fix:** Either (a) document in `data-retention.md` §5.2 how `retention_raw_days`/`retention_curated_days` from the YAML config are translated into per-prefix S3 lifecycle rule overrides and who applies them, or (b) remove these fields from the YAML config in `security-data-privacy.md` §1.4 if bucket-level lifecycle rules with dataset prefixes are the intended mechanism. The two docs must agree on the mechanism.

---

### S-6: Reconciliation doc's T2/T3 require consumer state store access — this dependency is unresolved and not acknowledged in onboarding

**Issue:** `reconciliation-design.md` §3.4 defines T2 as comparing three planes: source, Kafka, and "consumer state store." The T3 aggregate check also queries `consumer_aggregate`. The doc's open items §10 acknowledges: "Do consumer teams report their materialised counts to the platform, or does the platform query consumer state directly?" This dependency is never resolved and is not mentioned anywhere in `consumer-onboarding.md`. Consumers going live today have no obligation to support T2/T3 cross-plane reconciliation, which means the T2 and T3 checks as designed cannot run at all until this is resolved and consumers implement the necessary reporting interface.

**Location:** `reconciliation-design.md` §3.4, §10; `consumer-onboarding.md` §9.2 (consumer responsibilities).

**Suggested fix:** Add a note to `consumer-onboarding.md` §9.2 under "Consumer responsibilities" that the platform intends to implement T2/T3 cross-plane reconciliation and that consumers will be asked to either (a) expose a count endpoint or (b) report materialised counts to `ods.pipeline.reconciliation`. State that the mechanism is TBD and that consumers will be notified before any obligation is introduced. Add the same note to `reconciliation-design.md` §10 with a target decision date.

---

### S-7: DQ rules file retention is defined but never referenced in the dataset onboarding checklist

**Issue:** `data-retention.md` §3.2 defines a retention policy for `S3 DQ Results` (`ods-dq-results-{env}`). The `dataset-onboarding.md` §2.6 pre-onboarding checklist and §9 DQ rules template reference creating a `.dqdl` rules file in `ods-config-{env}/dq-rules/`. No section in dataset-onboarding mentions: (a) that DQ results are written to `ods-dq-results-{env}`, (b) that these results have a retention policy, or (c) that there is a stale quarantine alarm the team should be aware of. This means engineers treating data quality failures may not know where results are stored or how long they are available.

**Location:** `data-retention.md` §3.2 (S3 DQ Results row, S3 Quarantine row); `dataset-onboarding.md` §9, §11 (post-onboarding checklist).

**Suggested fix:** Add a bullet to `dataset-onboarding.md` §11 (Post-onboarding checklist): "Confirm DQ result writes to `ods-dq-results-{env}` and that the stale quarantine alarm is configured (see Data Retention Policy §7 and §10.3)."

---

## Minor Issues / Improvements

### M-1: Lineage doc Appendix A references `pipeline.ingestion_file_state` but retention doc names it differently in §3.2

**Issue:** `data-lineage.md` Appendix A table lists `pipeline.ingestion_file_state` as a lineage store. `data-retention.md` §3.2 lists this table as `PostgreSQL pipeline.ingestion_file_state` and gives it a 90-day hot retention. `ingestion-design.md` defines the table as `pipeline.ingestion_file_state`. These are consistent. However, `data-retention.md` §3.2 also lists `pipeline.file_state` separately. The lineage Appendix A does not list `pipeline.file_state` separately — it is mentioned only in passing in the text. This is a minor presentation inconsistency rather than a contradiction, but it could confuse engineers checking retention for a table they found in the lineage appendix.

**Location:** `data-lineage.md` Appendix A; `data-retention.md` §3.2.

**Suggested fix:** Ensure `pipeline.file_state` appears explicitly in `data-lineage.md` Appendix A as a separate row, distinct from `pipeline.ingestion_file_state`, with a note on what each covers.

---

### M-2: Dataset-onboarding Step 2 (S3 batch) creates topic with `cleanup.policy=delete` — retention doc recommends `compact,delete`

**Issue:** `dataset-onboarding.md` §3 Step 2 creates the Kafka topic with `--config cleanup.policy=delete`. `data-retention.md` §4.3 defines the recommended configuration for business data topics as `cleanup.policy=compact,delete`. The reconciliation doc also explains why compaction is beneficial for consumer catch-up. A dataset onboarded following the runbook will have a delete-only topic, which means no compaction semantics, no safe consumer catch-up by replaying latest state, and no tombstone mechanism for GDPR erasure — all of which are important for production topics.

**Location:** `dataset-onboarding.md` §3 Step 2; `data-retention.md` §4.3.

**Suggested fix:** Update the `kafka-topics.sh` command in §3 Step 2 to use `--config cleanup.policy=compact,delete` and add the full recommended topic configuration from `data-retention.md` §4.3 (including `min.compaction.lag.ms` and `delete.retention.ms`). Note that `cleanup.policy=delete` is still appropriate for the audit and reconciliation system topics, so those should remain as specified.

---

### M-3: Reconciliation document uses "T+1/T+2/T+3" and "T0/T1/T2/T3" as different concepts without clarifying the distinction

**Issue:** `dataset-onboarding.md` §2.9 and §10 use "T2 reconciliation" and "T3 check" to refer to what appear to be "T+2 days" and "T+3 days" type checks (end-of-day business reconciliation terminology). `reconciliation-design.md` uses T0/T1/T2/T3 as latency tiers (real-time, near-real-time, hourly, daily). The same abbreviations mean fundamentally different things in the two documents. A reader moving between them will be confused: in the dataset-onboarding doc, "T3 reconciliation" means "business date count + aggregate sum" at a daily schedule; in the reconciliation doc, "T3" means the fourth latency tier (daily full reconciliation) which may or may not align with "T+3 days."

**Location:** `dataset-onboarding.md` §2.6, §2.9, §10; `reconciliation-design.md` §3.1–3.5.

**Suggested fix:** Add a terminology box at the top of `reconciliation-design.md` §3 clarifying that T0/T1/T2/T3 in this document are latency tiers, not business day offsets. Update `dataset-onboarding.md` §2.9 to replace "T2 reconciliation" and "T3 check" with "hourly count reconciliation (Tier 2)" and "daily business reconciliation (Tier 3)" the first time they appear, with a cross-reference to the reconciliation design.

---

### M-4: Schema-governance emergency change process does not cross-reference the dataset-onboarding approval gates

**Issue:** `schema-governance.md` §11.2 defines an emergency schema change fast-track that bypasses the normal dev → staging → prod promotion and the 30-day notice period. `dataset-onboarding.md` §7 defines formal Gate 1 and Gate 2 approval checkpoints that govern environment promotion. The emergency process says "Change is deployed directly to prod (bypassing the normal dev → staging → prod promotion)" but does not reference whether or how the gate approvals are satisfied in this scenario.

**Location:** `schema-governance.md` §11.2; `dataset-onboarding.md` §7.

**Suggested fix:** Add a note to `schema-governance.md` §11.2 that emergency changes to schemas for datasets that have already passed Gate 2 do not require re-approval of the onboarding gates, but that any emergency change to a schema that is in the process of onboarding (i.e. has not yet reached prod) must still obtain gate sign-off before the emergency change is back-applied to staging.

---

### M-5: Lineage doc treats `pipeline.run_log` as authoritative but this table is not defined in any DDL in the platform

**Issue:** `data-lineage.md` §2.3 ER diagram and Appendix A list `PIPELINE_RUN_LOG` as a first-class lineage store, and §4.3 SQL lookups use `pipeline.run_log`. The `ingestion-design.md` and `architecture-decisions.md` define `pipeline.glue_job_log` as the INSERT-only job audit table. The lineage doc introduces `pipeline.run_log` as a separate table but provides no DDL for it (only the ER diagram), and no other document defines or references `pipeline.run_log`. It is unclear whether `pipeline.run_log` is intended to be the same table as `pipeline.glue_job_log` under a different name, or a new separate table.

**Location:** `data-lineage.md` §2.3, Appendix A, SQL examples throughout; `ingestion-design.md` §4; `architecture-decisions.md` §1.11.

**Suggested fix:** Either (a) clarify in `data-lineage.md` §2.3 that `pipeline.run_log` is an alias for `pipeline.glue_job_log` (and add the missing columns that `run_log` has that `glue_job_log` does not, e.g. `target_ref`, `schema_version`), or (b) add a DDL block for `pipeline.run_log` as a distinct table and explain its relationship to `pipeline.glue_job_log`. Whichever is chosen, ensure `data-retention.md` §3.2 covers it — the current retention table does not mention `pipeline.run_log` at all.

---

### M-6: `pipeline.schema_consumers` registration described as "not enforced at runtime" but consumer offboarding depends on it

**Issue:** `schema-governance.md` §2.3 states consumer registration in `pipeline.schema_consumers` "is not enforced by the platform at runtime, but it is required for consumer sign-off during breaking changes." This is a voluntary control. `schema-governance.md` §10.2 step 5 includes "PostgreSQL `schema_consumers` records are marked inactive" as part of topic retirement. If registration is voluntary, topic retirement may leave active consumers unidentified, and breaking-change notifications will miss unregistered consumers — yet the only defined escalation path in §9.3 explicitly assumes all consumers are registered.

**Location:** `schema-governance.md` §2.3, §9.3, §10.2.

**Suggested fix:** Either make consumer registration a gate requirement in `consumer-onboarding.md` §11 checklist (item 16: "Consumer group registered in `pipeline.schema_consumers` — platform team confirmed"), or document the risk explicitly in `schema-governance.md` §2.3 and define a fallback notification mechanism for unregistered consumers that is triggered during breaking change and topic retirement processes.

---

### M-7: Data lineage doc claims `x-ods-pipeline-type` value is `publish` for S3 batch, but dataset-onboarding refers to it as `s3-batch`

**Issue:** `data-lineage.md` §3.1 defines the `x-ods-pipeline-type` header with example values `publish | cdc | api | event`. The S3 batch example in §3.2 shows `"x-ods-pipeline-type": "publish"`. `consumer-onboarding.md` §5.4 (before it was updated — currently uses the old header set) lists `x-ods-pipeline-type` with values `s3-batch`, `cdc`, `api`, `event` — note `s3-batch` rather than `publish`. These two value sets are inconsistent.

**Location:** `data-lineage.md` §3.1, §3.2; `consumer-onboarding.md` §5.4.

**Suggested fix:** Standardise the `x-ods-pipeline-type` values. The lineage doc's use of `publish` for S3 batch is semantically odd given that all patterns publish to Kafka — `s3_batch` (underscore, consistent with `x-ods-source-type`) is clearer. Update `data-lineage.md` §3.1 and all header examples to use `s3_batch | cdc | api | event`. This also requires updating the Python code example in `data-lineage.md` §3.4 which uses `"x-ods-pipeline-type": "publish"`.

---

## Cross-Document Linking Gaps

The following locations contain content that should reference another document but do not.

| # | Location | Missing reference | Reason it matters |
|---|---|---|---|
| 1 | `consumer-onboarding.md` §1.4 (guarantees and non-guarantees) | Should reference `data-retention.md` §4.3 for the actual Kafka retention window | Consumers are told retention is TBD; the retention doc has decided it |
| 2 | `consumer-onboarding.md` §5.4 (message headers table) | Should reference `data-lineage.md` §3.1 as the canonical header spec | Header table in consumer guide is outdated; lineage doc is authoritative |
| 3 | `dataset-onboarding.md` §3 Step 2 (topic creation) | Should reference `data-retention.md` §4.3 for the full recommended topic configuration | Step 2 only sets `retention.ms`; misses `min.compaction.lag.ms`, `delete.retention.ms`, `cleanup.policy` |
| 4 | `dataset-onboarding.md` §3 Step 1 and §4 Step 3 (schema registration) | Should reference `schema-governance.md` §6.5 for required metadata fields | Example schemas are incomplete; engineers will miss the five mandatory fields |
| 5 | `dataset-onboarding.md` §11 (post-onboarding checklist) | Should reference `schema-governance.md` §2.3 for the consumer registration step | Consumer registration in `pipeline.schema_consumers` is not in the post-onboarding checklist |
| 6 | `dataset-onboarding.md` §11 (post-onboarding checklist) | Should reference `data-retention.md` §7 (DLQ) and §10.3 (cost monitoring alarms) | Engineers won't know to check DLQ alarm configuration at go-live |
| 7 | `schema-governance.md` §2.3 (consumer registration) | Should reference `consumer-onboarding.md` §11 (go-live checklist) | Governance doc says registration is required for breaking changes; onboarding checklist should enforce it |
| 8 | `schema-governance.md` §10 (deprecation) | Should reference `dataset-onboarding.md` §12 (offboarding) | The two describe the same lifecycle event from different angles with no cross-reference |
| 9 | `reconciliation-design.md` §4.3 (delete reconciliation, tombstone retention risk) | Should reference `data-retention.md` §4.3 (tombstone retention config) | Risk is identified but the resolution (retention.ms, min.compaction.lag.ms) is only in the retention doc |
| 10 | `data-retention.md` §4.3 (business topic config) | Should reference `reconciliation-design.md` §4.3 (tombstone reconciliation requirement) | Retention doc sets tombstone config without explaining why those specific values are needed |
| 11 | `data-lineage.md` §1.4 (compliance — GDPR, Solvency II) | Should reference `security-data-privacy.md` §3 (right to erasure strategy) | Lineage doc cites GDPR compliance as a driver but does not link to the erasure design |
| 12 | `data-retention.md` §2.1 (right to erasure, crypto-shredding) | Should reference `security-data-privacy.md` §3.2 for the full crypto-shredding design | Retention doc describes the approach in one paragraph; security doc has the full architecture |
| 13 | `consumer-onboarding.md` §9.2 (consumer responsibilities) | Should reference `reconciliation-design.md` §10 (consumer state store dependency) | T2/T3 reconciliation requires consumer cooperation; consumers are not told this |
| 14 | `data-lineage.md` Appendix A (`pipeline.run_log`) | Should reference the DDL source (currently undefined — see M-5) | Table is used throughout but never defined |
| 15 | `dataset-onboarding.md` §2.7 (data classification) | Should reference `security-data-privacy.md` §1 (classification framework) | Classification decision flow is fully defined in the security doc; onboarding just mentions it |

---

## Positive Observations

1. **Lineage design is exceptionally thorough.** The polymorphic `pipeline.lineage` table with pattern-specific nullable columns, the two-axis `run_id`/`source_ref` correlation model, and the step-by-step forensic walkthrough for each of the four ingestion patterns (including full SQL and CloudWatch Insights queries) represent the kind of operational depth that is rare in draft governance documents. Any on-call engineer will be able to trace a problem record from a Kafka header to its source in minutes.

2. **Retention policy correctly separates compliance from operational need.** The `data-retention.md` three-force model (operational, compliance, cost), the explicit sign-off table in Appendix A, and the distinction between minimum and maximum retention under UK GDPR are well-reasoned. The decision to keep the compliance delete rule disabled by default (§5.3) with a gate for legal confirmation is the right call.

3. **Schema governance breaking change process is production-grade.** The 30-day notice period, RFC issue workflow, consumer sign-off protocol, consumer notification template, and post-cutover 24-hour DLQ monitoring window in `schema-governance.md` §4 and §9 are all sound. The key field immutability CI check and the versioned topic migration procedure are the kind of safeguards that prevent the most common production-breaking schema incidents.

4. **Dataset onboarding is config-driven and the rationale is explained.** The statement that no new Glue job code is required for a new dataset following an existing pattern, with the mechanism explained (shared job code reads all parameters from YAML at runtime), sets the right expectation for data engineers and data owners. The per-pattern step-by-step sections are genuinely actionable.

5. **Reconciliation tiers are well-structured.** The T0/T1/T2/T3 tiering with explicit latency targets, the rationale for why each tier exists, and the per-pattern reconciliation checklists with implementation status are clear and honest. Marking every T1–T3 check as "Not implemented" is exactly the right level of transparency for a draft.

6. **Security doc's crypto-shredding recommendation is well-reasoned.** The detailed trade-off table for Strategy A vs Strategy B and the risk mitigations section (separate IAM role for `privacy.entity_keys`, 24-hour review gate before KMS key deletion, quarterly audit) show that the security implications have been thought through rather than handwaved.

7. **ADR (architecture-decisions.md) provides clear rationale for every decision.** The consistent format (Decision / Rationale / Trade-offs / Alternative considered) and the explicit statement of what was rejected and why (DynamoDB vs PostgreSQL, Confluent vs Glue Schema Registry) will make this document useful far beyond the initial build phase.
