# Technical Writing Review — Senior Technical Writer
**Reviewer role:** Senior Technical Writer  
**Date:** 2026-04-16  
**Documents reviewed:** 27 files across `docs/plans/` and the repository root

| # | File | Location |
|---|---|---|
| 1 | `2026-04-14-architecture-decisions.md` | docs/plans/ |
| 2 | `2026-04-14-ingestion-design.md` | docs/plans/ |
| 3 | `2026-04-14-s3-kafka-design.md` | docs/plans/ |
| 4 | `2026-04-14-ingestion-failure-and-recovery.md` | docs/plans/ |
| 5 | `2026-04-14-s3-kafka-failure-and-recovery.md` | docs/plans/ |
| 6 | `2026-04-15-consumer-onboarding.md` | docs/plans/ |
| 7 | `2026-04-15-data-lineage.md` | docs/plans/ |
| 8 | `2026-04-15-data-retention.md` | docs/plans/ |
| 9 | `2026-04-15-dataset-onboarding.md` | docs/plans/ |
| 10 | `2026-04-15-deployment-cicd.md` | docs/plans/ |
| 11 | `2026-04-15-disaster-recovery.md` | docs/plans/ |
| 12 | `2026-04-15-implementation-plan.md` | docs/plans/ |
| 13 | `2026-04-15-observability.md` | docs/plans/ |
| 14 | `2026-04-15-reconciliation-design.md` | docs/plans/ |
| 15 | `2026-04-15-schema-governance.md` | docs/plans/ |
| 16 | `2026-04-15-security-data-privacy.md` | docs/plans/ |
| 17 | `2026-04-15-testing-strategy.md` | docs/plans/ |
| 18 | `ods-ingestion-commentary.md` | root |
| 19 | `ods-s3-kafka-overview-commentary.md` | root |
| 20 | `ods-ingestion-overview.mmd` | root |
| 21 | `ods-s3-kafka-overview.mmd` | root |
| 22 | `ods-s3-kafka-overview-simple.mmd` | root |
| 23 | `ods-s3-kafka-overview-sequence.mmd` | root |
| 24 | `ods-ingestion-overview-sequence.mmd` | root |
| 25 | `ods-ingestion-glue-detail.mmd` | root |
| 26 | `ods-s3-kafka-glue-detail.mmd` | root |
| 27 | `ods-publish-flow.mmd` | root |

---

## Executive Summary

This is a technically sophisticated and unusually complete documentation set for a platform still in design. The writing is consistently clear, the code examples are thorough, and the use of tables, diagrams, and SQL snippets throughout is effective. The two April-14 pipeline design documents and their companion diagrams and commentaries form an exemplary self-contained unit. The April-15 operational documents (data-lineage, data-retention, schema-governance, security, reconciliation-design) are among the strongest individual documents: they are thorough, honest about gaps, and reader-centric.

The primary weaknesses are structural rather than prose-level. The document set lacks a front door: there is no index document, no recommended reading order, and no single place that explains how the 27 files relate to each other. Cross-linking between documents is inconsistent — the design docs reference their diagrams and commentaries correctly, but the operational documents almost never link back to the design docs that established the components they describe. There are also meaningful terminology inconsistencies (documented below) that will confuse readers who move between documents, and one significant disagreement between a diagram and the text that describes it.

---

## Document Map

The documents cluster into six natural groups. Understanding these groups is the key to navigating the set.

### Group A — Architecture and Pipeline Design (the foundation)
These are the first documents written and the ones everything else depends on.

- `2026-04-14-architecture-decisions.md` — the "why" document; records every major design choice with rationale and trade-offs
- `2026-04-14-s3-kafka-design.md` — detailed design of Pipeline 2 (S3 Curated → MSK); the platform template
- `2026-04-14-ingestion-design.md` — detailed design of Pipeline 1 (SFTP → S3 Raw → Glue → S3 Curated)

### Group B — Failure Handling (paired with Group A)
Each pipeline design doc has a companion failure doc.

- `2026-04-14-s3-kafka-failure-and-recovery.md` — failure scenarios and resubmission procedures for the publish pipeline
- `2026-04-14-ingestion-failure-and-recovery.md` — same for the ingestion pipeline

### Group C — Companion Diagrams and Commentary (visual Group A)
These live in the repo root and are referenced by the Group A documents.

- `ods-ingestion-overview.mmd` / `ods-ingestion-overview-sequence.mmd` / `ods-ingestion-glue-detail.mmd`
- `ods-s3-kafka-overview.mmd` / `ods-s3-kafka-overview-simple.mmd` / `ods-s3-kafka-overview-sequence.mmd` / `ods-s3-kafka-glue-detail.mmd`
- `ods-ingestion-commentary.md` / `ods-s3-kafka-overview-commentary.md`
- `ods-publish-flow.mmd` — outlier; appears to be an earlier or simpler draft of the publish pipeline

### Group D — Consumer and Dataset Operations (audience: teams using the platform)
These translate platform design into actionable guidance for external teams.

- `2026-04-15-consumer-onboarding.md` — how downstream teams connect to Kafka topics
- `2026-04-15-dataset-onboarding.md` — how new datasets are added to the platform

### Group E — Platform Governance and Compliance
High-level cross-cutting concerns applicable to all four ingestion patterns.

- `2026-04-15-schema-governance.md`
- `2026-04-15-data-retention.md`
- `2026-04-15-security-data-privacy.md`
- `2026-04-15-data-lineage.md`

### Group F — Operations and Delivery
Implementation execution, monitoring, and recovery.

- `2026-04-15-implementation-plan.md` — phased delivery roadmap
- `2026-04-15-deployment-cicd.md` — CI/CD pipeline design
- `2026-04-15-observability.md` — metrics, logs, alarms
- `2026-04-15-reconciliation-design.md` — completeness and correctness checking
- `2026-04-15-disaster-recovery.md` — DR and business continuity
- `2026-04-15-testing-strategy.md`

---

## Recommended Reading Order

For a new engineer joining the platform team:

1. `2026-04-14-architecture-decisions.md` — understand the "why" before any "what"
2. `2026-04-14-s3-kafka-design.md` + `ods-s3-kafka-overview-commentary.md` together — the publish pipeline end-to-end
3. `ods-s3-kafka-overview.mmd` / `ods-s3-kafka-glue-detail.mmd` — visual companion to step 2
4. `2026-04-14-ingestion-design.md` + `ods-ingestion-commentary.md` together — the ingestion pipeline
5. `ods-ingestion-overview.mmd` / `ods-ingestion-glue-detail.mmd` — visual companion to step 4
6. `2026-04-14-s3-kafka-failure-and-recovery.md` + `2026-04-14-ingestion-failure-and-recovery.md` — what breaks and how to fix it
7. `2026-04-15-observability.md` — how to watch the platform
8. `2026-04-15-schema-governance.md` — the contract layer
9. `2026-04-15-data-lineage.md` — tracing data through the system
10. `2026-04-15-dataset-onboarding.md` — how to add a new dataset
11. `2026-04-15-consumer-onboarding.md` — how downstream teams connect
12. `2026-04-15-implementation-plan.md` — where things stand and what is next

For a consumer team (not platform engineers), the reading order is shorter: documents 1 (skim), 11, 8, 9.

For SREs joining the on-call rotation: documents 1 (skim), 6, 7, 15 (disaster-recovery), 13 (deployment-cicd rollback sections).

**This reading order is not documented anywhere in the set.** It should be. An `INDEX.md` or `README.md` at the `docs/plans/` level would pay for itself immediately.

---

## Terminology Inconsistencies

These are terms where the same concept is described by different names across documents. Each inconsistency listed here is a genuine reader confusion risk.

### 1. The Kafka broker service: "Kafka" vs "MSK" vs "MSK Kafka"
- `2026-04-14-s3-kafka-design.md`: uses "MSK" and "Kafka" interchangeably; section heading uses "S3 → Kafka"
- `2026-04-15-consumer-onboarding.md`: consistently uses "MSK" and "MSK Kafka"
- `2026-04-14-architecture-decisions.md`: uses "Kafka" and "MSK" interchangeably
- `ods-s3-kafka-overview.mmd`: uses "MSK Topics" in diagram labels
- **Recommendation:** Adopt "MSK (Kafka)" on first use per document; thereafter use "MSK" for the service and "Kafka" for the protocol/concepts.

### 2. The idempotency state table: "file_state" vs "pipeline.file_state" vs "ods.pipeline.file_state"
- `2026-04-14-s3-kafka-design.md` Section 3: `file_state` (unqualified)
- `2026-04-14-s3-kafka-failure-and-recovery.md` Section 3.1: queries use `pipeline.file_state`
- `2026-04-15-consumer-onboarding.md` Section 1.3: no reference to state tables (correct for audience)
- `ods-s3-kafka-overview-commentary.md` Step 3: "PostgreSQL `pipeline.file_state`"
- `ods-s3-kafka-overview-sequence.mmd`: participant label "PostgreSQL `ods.pipeline.file_state`" — note the `ods.` prefix, which conflicts with `pipeline.` in all other documents
- **Recommendation:** Standardise on `pipeline.file_state` (schema-qualified, no database prefix) throughout all documents and diagrams.

### 3. The ingestion state table: "ingestion_file_state" vs "pipeline.ingestion_file_state"
Same pattern as above. The sequence diagram uses `pipeline.ingestion_file_state`; the design doc uses just `ingestion_file_state`. 

### 4. The publish pipeline name: "S3→Kafka pipeline" vs "Publish Pipeline" vs "S3 batch pipeline"
- `2026-04-14-s3-kafka-design.md` title: "S3 → Kafka Pipeline"
- `2026-04-14-architecture-decisions.md` Section 1: "S3→Kafka publish pipeline"
- `2026-04-15-data-lineage.md` Section 10 table: "S3 Batch (Parquet)" and "s3_batch" (source_type discriminator)
- `2026-04-15-consumer-onboarding.md` Section 1.2: "S3-batch pattern"
- `2026-04-15-testing-strategy.md` title: "S3 batch ingestion pattern"
- **Recommendation:** Adopt "S3 batch pattern" for the ingestion pattern name (consistent with lineage's `source_type = 's3_batch'`) and "publish pipeline" for the Pipeline 2 step specifically.

### 5. The pipeline log table: "glue_job_log" vs "pipeline.glue_job_log" vs two different DDL schemas
This is a significant inconsistency. Two different DDL schemas exist for `glue_job_log`:
- `2026-04-14-s3-kafka-design.md` defines `glue_job_log` with columns: `run_id`, `job_name`, `pipeline_type`, `domain`, `dataset`, `source_path`, `target_path`, `business_date`, `status`, `record_count`, `error_reason`, `error_detail`, `created_at`
- `2026-04-15-data-retention.md` Section 6.3 defines a **different** `glue_job_log` schema with columns: `id`, `job_name`, `job_run_id` (not `run_id`), `domain`, `dataset`, `status`, `error_message` (not `error_reason`/`error_detail`), `input_files`, `output_rows` (not `record_count`), `duration_ms`, `config_version`, `created_at`, `updated_at`
- The `data-retention.md` DDL uses `PARTITION BY RANGE` which the design doc DDL does not
- **Recommendation:** Reconcile to a single canonical DDL. The design doc DDL appears to be the intended authoritative version. The retention doc DDL appears to have been independently written. One of them must be retired and the other marked as the source of truth.

### 6. DLQ partition path: "topic={topic}" vs "dataset={dataset}"
- `2026-04-14-architecture-decisions.md` Section 1.7 DLQ partition structure uses `topic|dataset={name}` (a hybrid notation)
- `2026-04-14-s3-kafka-failure-and-recovery.md` Section 3.2 shows: `schema-incompatible/date={date}/topic={topic}/`
- `2026-04-14-ingestion-failure-and-recovery.md` Section 2 shows: `schema-incompatible/date={date}/dataset={dataset}/`
- **Recommendation:** The ingestion pipeline routes to `dataset=`, the publish pipeline to `topic=`. This may be intentional (different partitioning by pipeline). If so, it should be explicitly stated. If not, standardise on one.

### 7. The audit trail topic: "ods.pipeline.audit" vs "pipeline.audit"
All documents consistently use `ods.pipeline.audit` — this is fine. However, `ods-ingestion-design.md` Section 8 Audit Topic Schema shows `"source_type": "sftp"` while `2026-04-15-data-lineage.md` Section 3.1 defines `"x-ods-source-type": "s3_batch"` as the canonical value. These are different things (audit event payload vs Kafka message header) but could confuse readers if they try to reconcile them.

### 8. Schema Registry subject naming: "ods-insurance-policies" vs "ods.insurance.policies"
- `2026-04-14-ingestion-design.md` YAML config: `schema_id: ods-schema-registry-{env}/insurance-policies`
- `2026-04-15-dataset-onboarding.md` Step 1: "Subject name: `ods-{domain}-{dataset}`" (hyphen-separated)
- `2026-04-15-consumer-onboarding.md` Section 5.1: "Schema names follow the pattern: `ods.{domain}.{dataset}`" (dot-separated, matching the topic name)
- `2026-04-15-schema-governance.md` Section 3: examples use `ods.insurance.policies` (dot-separated)
- **Recommendation:** Standardise on `ods.{domain}.{dataset}` (dot-separated, matching the topic name convention). Update `dataset-onboarding.md` Step 1 and the ingestion design YAML example accordingly.

### 9. "Run log" vs "job log": `pipeline.run_log` vs `pipeline.glue_job_log`
- `2026-04-15-data-lineage.md` introduces a table called `pipeline.run_log` (visible in the ER diagram and all SQL queries in that document)
- All other documents use `pipeline.glue_job_log`
- These appear to be the same table — the lineage doc renames it or describes a future unified abstraction
- **Recommendation:** Either reconcile these into one table definition, or explicitly document in `data-lineage.md` that `pipeline.run_log` is a planned extension/rename of `pipeline.glue_job_log`.

### 10. Status values for `file_state`: "new" vs status not set
- `2026-04-14-s3-kafka-design.md` Section 3: `status VARCHAR NOT NULL -- new | processing | completed | failed`
- `2026-04-14-s3-kafka-failure-and-recovery.md` Section 3.2: `SET status = 'new'` (reset instruction)
- `ods-s3-kafka-overview-sequence.mmd`: idempotency alt shows "status=new → proceed"
- This is consistent. However, `2026-04-14-s3-kafka-failure-and-recovery.md` Section 2.5 states "PostgreSQL file state remains `processing`" for a DAG failure before Glue trigger — but Section 2.4 says the same thing for a mid-publish crash. The distinction is correct but the identical wording may mislead; consider clarifying each scenario's starting state.

---

## Critical Issues (must fix)

**Issue 1: Two conflicting DDL definitions for `pipeline.glue_job_log`**  
Location: `2026-04-14-s3-kafka-design.md` Section 3 vs `2026-04-15-data-retention.md` Section 6.3  
The column names differ (`run_id` vs `job_run_id`, `record_count` vs `output_rows`, `error_reason`/`error_detail` vs `error_message`). Queries in failure-and-recovery docs, lineage doc, and observability doc all use the design-doc column names. The retention doc DDL is inconsistent with all of those.  
Suggested fix: Align the retention doc DDL to match the design doc DDL exactly. Add a note: "See `2026-04-14-s3-kafka-design.md` Section 3 for the canonical schema."

**Issue 2: `ods-publish-flow.mmd` is inconsistent with the canonical pipeline design**  
Location: `ods-publish-flow.mmd`  
This diagram describes the publish pipeline using "Airflow Sensor" as the trigger mechanism ("New curated Parquet file → Airflow Sensor → File detected"). All other documents (including `2026-04-14-s3-kafka-design.md` Section 1.2, `2026-04-14-architecture-decisions.md` Section 1.2, and all the sequence diagrams) explicitly state that an **EventBridge rule** (not a sensor) triggers the publish pipeline. The architecture decision doc notes that the Airflow S3 Sensor polling approach was **replaced** by EventBridge. `ods-publish-flow.mmd` reflects the superseded design.  
Suggested fix: Either update `ods-publish-flow.mmd` to match the current EventBridge-driven architecture, or delete it and add a note in `2026-04-14-s3-kafka-design.md` Section 4 explaining why it was removed. Keeping a diagram that contradicts the design creates genuine confusion for new readers.

**Issue 3: `pipeline.run_log` in `data-lineage.md` is undefined relative to existing tables**  
Location: `2026-04-15-data-lineage.md` throughout (ER diagram, SQL queries, Appendix A)  
The document introduces `pipeline.run_log` as a first-class table with its own DDL shown in the ER diagram. However, it is not defined in any other document. All other documents use `pipeline.glue_job_log`. The lineage document's Appendix A summary table lists both `pipeline.run_log` and implicitly references `pipeline.glue_job_log` as different things ("Audit trail, incident forensics, replay" vs another row). The relationship between these is never stated.  
Suggested fix: Decide whether `pipeline.run_log` is (a) a planned replacement for `pipeline.glue_job_log`, (b) a new separate table, or (c) the same table referred to by a different name. Document the decision explicitly in the lineage doc's introduction, and either add the DDL to the design docs or add a cross-reference.

**Issue 4: No index document / front door**  
Location: `docs/plans/` directory (missing)  
A reader arriving at this documentation set for the first time has no way to know where to start. There is no `README.md`, no `INDEX.md`, and no document that says "here is how these 17+ documents relate to each other and in what order to read them."  
Suggested fix: Create `docs/plans/README.md` with the Document Map and Recommended Reading Order from this review.

---

## Significant Issues (should fix)

**Issue 5: `2026-04-14-ingestion-failure-and-recovery.md` has no document header (Date/Status/Scope)**  
Location: `2026-04-14-ingestion-failure-and-recovery.md` — first line is `# Ingestion Pipeline — Failures, Recovery & Resubmission` immediately followed by `---` and `## 1. Failure Scenarios`  
The document has no Date, Status, or Scope metadata block. All other plan documents have this header.  
Suggested fix: Add the standard metadata block:
```
**Date:** 2026-04-14  
**Status:** Approved  
**Scope:** SFTP → S3 Raw → Glue ETL ingestion pipeline (pattern 2 of 4)
```

**Issue 6: `2026-04-14-s3-kafka-failure-and-recovery.md` has no document header**  
Location: Same issue as Issue 5. The document starts directly with `# S3 → Kafka Pipeline — Failures, Recovery & Message Keys` and `## 1. Why Deterministic Message Keys?`  
Suggested fix: Add the standard metadata block:
```
**Date:** 2026-04-14  
**Status:** Approved  
**Scope:** S3 Curated Zone → MSK publish pipeline (pattern 1 of 4)
```

**Issue 7: Schema registry subject naming inconsistency (hyphen vs dot)**  
Location: `2026-04-15-dataset-onboarding.md` Step 1 and `2026-04-14-ingestion-design.md` YAML config vs `2026-04-15-consumer-onboarding.md` Section 5.1 and `2026-04-15-schema-governance.md`  
Detailed under terminology. Impact: a data engineer following `dataset-onboarding.md` registers a schema as `ods-insurance-policies` but a consumer engineer reading `consumer-onboarding.md` looks for `ods.insurance.policies` — they will not find it.  
Suggested fix: Standardise on dot-separated (`ods.{domain}.{dataset}`) throughout. Update `dataset-onboarding.md` Step 1 and the YAML config example in `ingestion-design.md`.

**Issue 8: `2026-04-15-observability.md` references MWAA "DAG 3" but the design uses two DAGs for ingestion plus one for publish**  
Location: `2026-04-15-observability.md` Section 2.1 flow diagram, participant labels: "MWAA DAG 1 (SFTP→S3)", "MWAA DAG 2 (ETL)", "MWAA DAG 3 (Publish)"  
The ingestion design uses DAG 1 (transfer) and DAG 2 (ETL). The publish pipeline has its own DAG. Calling this "DAG 3" in the observability diagram implies a fixed numbering that doesn't exist in the design documents, where the publish DAG is unnamed numerically.  
Suggested fix: Rename the observability diagram participants to "MWAA Ingestion DAG 1 (SFTP Transfer)", "MWAA Ingestion DAG 2 (ETL)", "MWAA Publish DAG" to match terminology in the design docs.

**Issue 9: `2026-04-15-data-lineage.md` Section 3.1 states "`x-ods-source-path` is replaced" but `consumer-onboarding.md` Section 5.4 still documents `x-ods-source-path`**  
Location: `2026-04-15-data-lineage.md` Section 3.1 states "`x-ods-source-ref` replaces the old `x-ods-source-path` (which was S3-only)." However, `2026-04-15-consumer-onboarding.md` Section 5.4 Message Headers Reference table lists `x-ods-source-path` (not `x-ods-source-ref`) as the header name.  
This is a genuine contradiction. Consumers reading the onboarding guide will implement `x-ods-source-path`; the lineage document says that header no longer exists.  
Suggested fix: Update `consumer-onboarding.md` Section 5.4 to use `x-ods-source-ref` and `x-ods-source-type` as defined in the lineage document. Add a note that `x-ods-source-path` was the predecessor name.

**Issue 10: `2026-04-15-dataset-onboarding.md` consumer group naming convention differs from `consumer-onboarding.md`**  
Location: `2026-04-15-dataset-onboarding.md` Section 2.8: "Kafka consumer group naming agreed (e.g. `cg.{team}.{dataset}`)"  
Location: `2026-04-15-consumer-onboarding.md` Section 3.1: "Required pattern: `{team}.{application}.{dataset}`" (no `cg.` prefix, three segments instead of two)  
These conflict on both prefix and segment count.  
Suggested fix: Standardise on the full pattern from consumer-onboarding (`{team}.{application}.{dataset}`) and update dataset-onboarding Section 2.8 accordingly.

**Issue 11: `2026-04-15-disaster-recovery.md` is visible as only partially read in this review**  
The document was read to approximately line 150 (Section 2.1 start) before the token limit was reached. A full review of that document could not be completed. The sections covering failure scenario catalogue, runbooks, DR testing, and open decisions were not reviewed.  
Suggested action: Re-review sections 3–11 of this document separately. The first two sections (purpose, architecture overview, RTO/RPO targets) are well-structured.

---

## Minor Issues / Improvements

**Minor 1: Acronyms not defined on first use**  
Locations across multiple documents:
- "DQDL" is used throughout all documents without ever being spelled out (AWS Glue Data Quality Definition Language). First define it on first use in `2026-04-14-architecture-decisions.md` Section 1.4.
- "MWAA" is used throughout without a first-use expansion. Add "(AWS Managed Workflows for Apache Airflow)" on first use in each document where it is the first mention.
- "LSN" in `2026-04-15-reconciliation-design.md` is used before being defined (defined in Section 2.2 but used in the CDC concept description above it). Reorder the definition.
- "DPIA" appears in `2026-04-15-implementation-plan.md` Blocking Decisions D10 without expansion. Spell out (Data Protection Impact Assessment).
- "DPO" first appears in `2026-04-15-data-retention.md` Section 2.1 without expansion (Data Protection Officer). Define on first use.
- "SMT" in `2026-04-15-data-lineage.md` Section 3.4 (Debezium CDC section). Define as "Single Message Transform".

**Minor 2: `2026-04-15-reconciliation-design.md` starts with a typo**  
Location: Line 1 of the file: `ar# ODS Platform — Reconciliation Design`  
The file begins with `ar` before the Markdown heading marker. This will render incorrectly in any Markdown viewer.  
Suggested fix: Delete `ar` from line 1.

**Minor 3: `ods-s3-kafka-overview-simple.mmd` is misnamed**  
Location: The file name says "simple" but the file content is actually the full overview sequence diagram (includes CloudWatch as a participant). `ods-s3-kafka-overview-sequence.mmd` is the "simple" one (no CloudWatch).  
The ingestion design doc (`2026-04-14-ingestion-design.md`) references "`ods-ingestion-overview-simple.mmd`" but this file does not exist in the repository. The closest file is `ods-ingestion-overview.mmd`.  
Suggested fix: Verify what `ods-ingestion-overview-simple.mmd` was meant to be. Either create it or update the reference in `ingestion-design.md` Section 5.

**Minor 4: The `2026-04-14-ingestion-design.md` Section 6 Idempotency table has different content from the architecture decisions doc**  
Location: `2026-04-14-ingestion-design.md` Section 6 lists three idempotency layers as: (1) File catalogue, (2) File state, (3) Checksum. This is a different framing from `2026-04-14-architecture-decisions.md` Section 1.5 which lists: (1) File level (PostgreSQL file_state), (2) Job level (Kafka transactions), (3) Consumer level (deterministic message keys). The ingestion doc replaces "Kafka transactions" and "deterministic message keys" with "Checksum verification."  
This is not necessarily wrong — the ingestion pipeline has no Kafka publish step so the Kafka idempotency layers do not apply — but it is confusing to use the same "three layers" framing with completely different layer definitions across documents. Readers who read architecture-decisions first will expect the same three layers everywhere.  
Suggested fix: Add a note to the ingestion idempotency table: "Note: Layer 2 (Kafka transactions) and Layer 3 (deterministic message keys) apply at the publish pipeline stage — see `2026-04-14-s3-kafka-design.md` Section 5."

**Minor 5: `2026-04-15-data-retention.md` status says "Draft — requires legal/compliance sign-off" but `2026-04-15-testing-strategy.md` status says "Approved for implementation"**  
Inconsistency in status values is expected, but two documents warrant attention:
- `2026-04-15-schema-governance.md` says "Draft" — but it is cited by `dataset-onboarding.md` as the source of schema governance rules, implying teams should follow it. A document cited as authoritative should arguably be "Approved."
- `2026-04-15-disaster-recovery.md` says "Requires business sign-off on RTO/RPO targets" — its status is "DRAFT FOR REVIEW" in the heading body but the standard metadata block would clarify the signal. Consider adding a "Pending sign-off" or "Under review" status to the standard set.

**Minor 6: `2026-04-15-consumer-onboarding.md` Section 4.2 authentication TBD**  
Location: Section 4.2 states "TBD — the ODS platform supports IAM authentication and SASL/SCRAM. The final choice for each environment is not yet formally decided."  
This is important: consumers cannot connect without knowing the auth method. The `2026-04-15-implementation-plan.md` lists D2 (MSK authentication mode) as a blocking decision. However, consumer-onboarding.md does not cross-reference the implementation plan's D2 decision.  
Suggested fix: Add a cross-reference: "See implementation plan D2 for the decision status on MSK authentication mode."

**Minor 7: `2026-04-15-data-lineage.md` Section 4.2 sequence diagram step 5 queries S3 Raw directly using Athena but earlier assumes data in S3 Raw is CSV while the s3_raw_path in the lineage record example (Section 8.3) ends in `.parquet`**  
Location: `2026-04-15-data-lineage.md` Section 4.2 and Section 8.3 example rows  
The S3 Raw zone stores CSV files (per ingestion design). The lineage record example shows `s3_raw_path = s3://ods-raw-prod/insurance/policies/2026-04-15/policies_2026-04-15.parquet`. Raw Zone files are CSV, not Parquet (Parquet is the Curated Zone format).  
Suggested fix: Correct the `sftp_filename` and `s3_raw_path` example to show a `.csv` extension in the S3 raw path example, matching what the ingestion pipeline actually writes.

**Minor 8: `2026-04-15-deployment-cicd.md` CI pipeline references a `ods-published-{env}` S3 bucket**  
Location: `2026-04-15-deployment-cicd.md` Section 8.3, the Terraform module for `claims_glue_crawler`:
```
s3_target = "s3://ods-published-{env}/claims/"
```
No document defines a bucket called `ods-published-{env}`. The standard bucket names defined in the design documents are: `ods-raw-{env}`, `ods-curated-{env}`, `ods-config-{env}`, `ods-dlq-{env}`, `ods-quarantine-{env}`, `ods-dq-results-{env}`, `ods-audit-sink-{env}`, `ods-scripts-{env}`, `ods-dags-{env}`.  
Suggested fix: Replace `ods-published-{env}` with `ods-curated-{env}` in the Terraform example.

**Minor 9: `2026-04-15-dataset-onboarding.md` Section 2.8 consumer group naming conflicts with `consumer-onboarding.md`**  
Documented above as Issue 10. Listed here also as a minor because the dataset onboarding doc's phrasing "e.g. `cg.{team}.{dataset}`" treats the inconsistency as illustrative rather than normative, but it will still mislead readers.

**Minor 10: `ods-ingestion-commentary.md` uses circled numbers (❶–⓴) but the diagram `ods-ingestion-overview.mmd` does not**  
Location: `ods-ingestion-commentary.md` numbers each step with circled numerals (❶, ❷, etc.). The flowchart diagram (`ods-ingestion-overview.mmd`) has no step numbers. This makes it harder to cross-reference between commentary and diagram.  
Suggested fix: Consider adding step number annotations to the diagram nodes, or add a note at the top of the commentary: "Step numbers in this document correspond to the numbered steps in `ods-ingestion-overview-sequence.mmd`."

---

## Missing Cross-References

These are places where one document should link to another but does not. For a documentation set of this size, missing cross-references mean readers dead-end and must search manually.

1. **`2026-04-14-ingestion-failure-and-recovery.md` should link to `2026-04-14-s3-kafka-failure-and-recovery.md`** because the ingestion pipeline handoff triggers the publish pipeline. When an ingestion failure is resolved and the file is resubmitted, the publish pipeline will re-trigger. The ingestion failure doc never mentions this.

2. **`2026-04-14-s3-kafka-design.md` should link to `2026-04-14-ingestion-design.md`** in its "Context" section (Section 1). It describes how the S3 Curated Zone receives Parquet files but does not tell readers how those files got there.

3. **`2026-04-15-observability.md` should link to `2026-04-14-s3-kafka-failure-and-recovery.md` and `2026-04-14-ingestion-failure-and-recovery.md`** in its alarm sections. When an alarm fires, the on-call engineer needs the runbook, but the observability doc never tells them where to find it.

4. **`2026-04-15-consumer-onboarding.md` should link to `2026-04-15-schema-governance.md`** in Section 5.3 (Schema Evolution). The consumer onboarding doc describes schema evolution from a consumer perspective; schema-governance.md defines the full change classification matrix. Consumers would benefit from that reference.

5. **`2026-04-15-data-lineage.md` should link to `2026-04-14-s3-kafka-design.md`** in Section 2 (Lineage Model), because the lineage model is built on top of the pipeline architecture defined there. Currently the lineage doc introduces `pipeline.lineage` and `pipeline.run_log` with no cross-reference to where the underlying pipeline components are designed.

6. **`2026-04-15-dataset-onboarding.md` should link to `2026-04-15-deployment-cicd.md`** in Step 3 (Infrastructure provisioning). The onboarding doc describes creating Terraform resources but does not point to the CI/CD doc's Section 8 which defines the IaC module structure.

7. **`2026-04-15-dataset-onboarding.md` should link to `2026-04-15-schema-governance.md`** in Step 1 (schema registration). The schema governance doc defines the change classification matrix and compatibility mode rules that govern schema decisions.

8. **`2026-04-15-disaster-recovery.md` should link to `2026-04-14-ingestion-failure-and-recovery.md` and `2026-04-14-s3-kafka-failure-and-recovery.md`** in its runbook sections. The DR doc is about infrastructure-level failures; the failure-and-recovery docs are about pipeline-level recovery procedures. Both are needed in a real incident.

9. **`2026-04-15-data-retention.md` should link to `2026-04-15-security-data-privacy.md`** in Section 2 (GDPR). These two documents are closely related — crypto-shredding (retention doc Section 2.1) is defined in more detail in security doc Section 3. Readers of one will need the other.

10. **`2026-04-15-implementation-plan.md` should link to all domain-specific design documents** when listing deliverables for each phase. Currently the implementation plan names deliverables as bare text. Adding links to the relevant design doc would help the reader understand what "design" means for each phase.

11. **`2026-04-15-reconciliation-design.md` should link to `2026-04-14-architecture-decisions.md` Section 1.5** (idempotency three layers) in Section 2.1 (S3 Batch). The reconciliation doc describes count-check reconciliation as "already implemented" but does not point to where that design was documented.

12. **`2026-04-15-testing-strategy.md` should link to `2026-04-15-dataset-onboarding.md`** in its test data management section. The onboarding runbook and the testing strategy both describe bringing a new dataset online, but neither references the other.

---

## Diagrams vs Text Gaps

### 1. `ods-publish-flow.mmd` — superseded design (Critical)
This diagram shows "Airflow Sensor" as the publish pipeline trigger, but the canonical design uses EventBridge. This diagram appears to pre-date the architecture decision recorded in `2026-04-14-architecture-decisions.md` Section 1.2. It is not referenced by any text document, which means a reader stumbling on it gets incorrect information.  
Action required: Update or delete.

### 2. `ods-s3-kafka-overview.mmd` — PostgreSQL table name discrepancy
The diagram's PostgreSQL participant is labelled "ods.pipeline.file_state" (database.schema.table). All text documents use `pipeline.file_state` (schema.table only). This is minor but creates confusion about where the database prefix belongs.

### 3. `ods-ingestion-overview.mmd` — the diagram does not show the `glue_job_log` write
The flowchart shows the Glue ETL job writing to S3 Curated, DLQ, and Glue Data Catalog. The step-by-step commentary (`ods-ingestion-commentary.md`) explains in detail that the Glue job writes INSERT rows to `pipeline.glue_job_log` at each status transition. This JDBC write is architecturally important (it is part of the observability design) but does not appear in the flowchart. The sequence diagram (`ods-ingestion-overview-sequence.mmd`) also omits this.  
The s3-kafka-glue-detail.mmd does show `PG: INSERT status=...` steps. The ingestion equivalent (`ods-ingestion-glue-detail.mmd`) also shows these writes. The gap is in the high-level flowchart and sequence, not the detail diagram.  
Action: Add a PostgreSQL write node to the high-level flowchart or add a note that the glue-detail diagram must be consulted for the full picture.

### 4. `ods-ingestion-overview.mmd` — missing EventBridge trigger notation for the Publish Pipeline
The diagram ends with: `S3C -->|triggers publish pipeline EventBridge| AUDIT`. This arrow connects S3 Curated to the Audit topic, which is incorrect — the trigger is the publish pipeline DAG, and the audit topic is a separate output. The diagram's routing of this arrow implies that S3C writes directly to AUDIT, which is wrong.  
Action: Correct the diagram to show S3C → EventBridge (ods-curated-file-rule) → Publish DAG as the handoff, with the audit topic shown as a separate output from the Publish Pipeline.

### 5. Diagrams not referenced by text documents
The following diagrams exist in the repository root but are not explicitly referenced (by filename) from any of the `docs/plans/` documents:
- `ods-ingestion-overview.mmd` — referenced by `2026-04-14-ingestion-design.md` Section 5 but as a filename: "`ods-ingestion-overview.mmd`" — correct.
- `ods-s3-kafka-overview.mmd` — NOT referenced by `2026-04-14-s3-kafka-design.md`. Section 4 references `ods-s3-kafka-overview-simple.mmd` and `ods-s3-kafka-overview-sequence.mmd` but not `ods-s3-kafka-overview.mmd` itself.
- `ods-publish-flow.mmd` — referenced by NO document.

### 6. Mermaid diagrams in documents vs standalone `.mmd` files
`2026-04-15-data-lineage.md` contains embedded Mermaid diagrams (ER diagram, sequence diagrams). `2026-04-15-data-retention.md` contains an embedded flowchart. `2026-04-15-security-data-privacy.md` contains an embedded flowchart. `2026-04-15-observability.md` contains an embedded flowchart. These are consistent and correct. However, there is no pattern guidance saying when to embed diagrams vs use separate `.mmd` files. This is a documentation standards gap.

---

## Positive Observations

The following elements are genuinely well-done and should be maintained as standards for future documents.

**1. The architecture-decisions document is exceptional.** Each decision follows a consistent pattern: Decision → Rationale → Trade-offs → Alternative considered. This is a model ADR (Architecture Decision Record) format. The strength-and-weakness analysis (Section 2) and the open-items table (Section 3) are particularly valuable for a platform in active development.

**2. The consumer-onboarding document is the best reader-centric document in the set.** It correctly identifies its audience, anticipates exactly the questions a new consumer team will have, provides working Java and Python code examples for both connection and schema deserialization, and the going-live checklist (Section 11) is thorough and actionable. The mermaid sequence diagram in Section 7.2 (initial data load) is particularly helpful.

**3. The failure-and-recovery documents are operationally excellent.** SQL queries are provided for every investigation scenario. The "Operational Checklist" at the end of each doc maps alarm names to section numbers — this is exactly what on-call engineers need at 2am. The pattern of naming exact alarm names (e.g. `ods-file-not-approved-{env}`) throughout is consistent and accurate.

**4. The data-lineage document demonstrates strong technical depth.** The polymorphic lineage table design, the four-pattern coverage, the worked tracing examples (S3 batch, CDC, Event), and the OpenLineage consideration section (recommending against it with clear reasoning) show mature thinking. The SQL appendix is immediately usable.

**5. The data-retention document is honest about what requires sign-off.** Clearly marking decisions as "Pending DPO review" or "Pending Compliance identification" throughout, rather than burying assumptions in prose, reduces governance risk and makes the document's limitations visible.

**6. Schema governance document's change matrix tables are clear and complete.** The side-by-side classification of compatible vs breaking changes by source type (S3 Parquet, CDC Avro, etc.) is exactly the right format for this content. Readers can find the answer to "is my proposed change breaking?" in under 30 seconds.

**7. Naming conventions are rigorous and consistent.** The `ods-{component}-{env}` pattern for all AWS resources, `ods/{env}` for CloudWatch namespaces, `ods.{domain}.{dataset}` for Kafka topics, and `{team}.{application}.{dataset}` for consumer groups are all applied consistently across almost all documents. This level of naming discipline is relatively rare and highly valuable for operational scripting and IAM policy authoring.

**8. The detail-level Mermaid sequence diagrams (`ods-ingestion-glue-detail.mmd` and `ods-s3-kafka-glue-detail.mmd`) are excellent.** The `alt` branches in the sequence diagrams correctly show all three failure modes with specific DLQ routing and CloudWatch metric names. The INSERT statements shown inline make the glue_job_log write path clear without requiring the reader to consult the DDL separately.

**9. The deployment-cicd document is complete for its scope.** The coverage of rollback procedures per artifact type (Glue script, DAG, YAML config, DQ rules, Terraform, database migration) is thorough, and each procedure includes the actual AWS CLI commands needed. The production deployment checklist (Section 12) is detailed enough to be used as a literal sign-off sheet.

**10. The implementation plan correctly identifies blocking decisions upfront.** Listing D1–D10 before any phase details, with criticality flags and owner assignments, is the right approach. It prevents the common mistake of designing around unresolved constraints.
