# Architecture Review — Software Architect
**Reviewer role:** Software Architect
**Date:** 2026-04-16
**Documents reviewed:**
- `docs/plans/2026-04-14-architecture-decisions.md` (ADR)
- `docs/plans/2026-04-14-ingestion-design.md` (Ingestion Design)
- `docs/plans/2026-04-14-s3-kafka-design.md` (S3→Kafka Design)
- `docs/plans/2026-04-15-implementation-plan.md` (Implementation Plan)
- `ods-ingestion-commentary.md` (Ingestion Commentary)
- `ods-s3-kafka-overview-commentary.md` (S3→Kafka Commentary)

---

## Executive Summary

The design set is substantively coherent and architecturally sound. The event-driven trigger model, three-layer idempotency, config versioning, and INSERT-only audit log are all well-reasoned decisions with documented trade-offs. The two design documents (ingestion and S3→Kafka) are internally consistent with each other and with the ADR in all major decisions. The implementation plan is the weakest document in the set: it introduces new concepts (reconciliation tables, a lineage table, a `pipeline.lineage` write in the publish Glue job, MSK auth mode as a critical blocker) that have no corresponding ADR and no definition in the design docs. There are also two structural contradictions between documents — one in the idempotency model and one in the trigger description — plus a cluster of small naming and completeness gaps. None of the issues would stop a senior engineer from starting implementation, but several will cause coordination problems if left unresolved before Phase 3 build work begins.

---

## Critical Issues (must fix)

### C1 — Contradiction: Ingestion design describes polling-based DAG 1 trigger; ADR 1.10 and EventBridge design describe event-driven trigger

**Location:** `2026-04-14-ingestion-design.md` Section 5, Phase 1 "Detect & Validate" vs `2026-04-14-architecture-decisions.md` Section 1.10 and Section 1.2

**Issue:** The ingestion design says in Phase 1: "MWAA SFTP Sensor polls every 5 minutes. On new file..." It also says in Section 2 Architecture Decisions table: "ETL trigger: AWS EventBridge (S3 Raw Object Created) — Decouples transfer from ETL". The commentary (`ods-ingestion-commentary.md` step ❶) reinforces polling: "The Airflow SFTP Sensor connects to the internal SFTP server every 5 minutes and lists files at the configured paths."

This is not a contradiction — SFTP polling for DAG 1 is correct and EventBridge handles the DAG 1→DAG 2 handoff. However, the YAML config field `poll_interval_minutes: 5` (ingestion design Section 9) implies this is explicitly configurable per dataset, but there is no ADR decision covering the SFTP polling mechanism, its interval range, or whether it is replaced by a future push/webhook approach. The ADR Section 1.2 states "Replaced: Airflow S3 Sensor polling every N seconds" but does not acknowledge that SFTP polling (a different form of polling) remains on the critical path in DAG 1.

**Why this matters:** A reader of the ADR alone will conclude the platform has eliminated all polling. It has not — SFTP polling is permanent for Pattern 1 as long as the upstream only provides SFTP. This creates a latency floor of up to 5 minutes before the file even reaches S3, which is invisible in the SLO table in ADR Section 4.1 ("File-to-Kafka latency (p95) < 10 minutes from file landing in S3 Curated") — the 5-minute SFTP polling lag is not in S3 Curated, it is before S3 Raw.

**Fix:** Add an ADR entry (or a sub-note under 1.2) explicitly stating that SFTP polling survives for Pattern 1 DAG 1 and is intentional. Revise the SLO table to either (a) define "file landing" as landing on SFTP (not S3 Curated), adding up to 5 minutes of polling lag, or (b) explicitly note that the p95 < 10-minute target is measured from S3 Raw landing, not SFTP arrival.

---

### C2 — Contradiction: Idempotency layer descriptions differ between ADR, ingestion design, and S3→Kafka design

**Location:** `2026-04-14-architecture-decisions.md` Section 1.5 vs `2026-04-14-ingestion-design.md` Section 6 vs `2026-04-14-s3-kafka-design.md` Section 5

**Issue:** The three documents each describe three idempotency layers, but the layer definitions do not agree:

| Layer | ADR (1.5) | Ingestion Design (§6) | S3→Kafka Design (§5) |
|---|---|---|---|
| 1 | PostgreSQL `file_state` / `ingestion_file_state` — prevents duplicate DAG execution per file path | `file_catalogue` whitelist check — prevents unapproved files entering pipeline | PostgreSQL state (`processing` / `completed`) — prevents double-triggering at DAG level |
| 2 | Kafka transactions — prevents partial publish on Glue crash | PostgreSQL `ingestion_file_state` — prevents duplicate processing | Kafka transactions — prevents partial publish |
| 3 | Deterministic message keys (SHA256) — consumer deduplication | MD5 checksum — detects file corruption in transit | Deterministic message keys — consumer deduplication safety net |

The ingestion design has remapped the three layers entirely relative to the ADR: it has promoted the catalogue whitelist to Layer 1, demoted the state table to Layer 2, and replaced the Kafka transaction layer (which is not relevant for ingestion, only for publish) with the MD5 checksum. This is arguably correct for the ingestion pipeline specifically, but it means the ADR's "three idempotency layers" description applies only to the publish pipeline, not to the ingestion pipeline. The ADR does not acknowledge this distinction.

**Fix:** The ADR Section 1.5 should distinguish idempotency layers by pipeline type. Either: (a) split into "1.5a Publish Pipeline Idempotency" and "1.5b Ingestion Pipeline Idempotency" with separate three-layer tables; or (b) note that the ingestion pipeline has its own three-layer model (catalogue guard, state table, MD5 checksum) and the publish pipeline has a different three-layer model (state table, Kafka transactions, message keys). As written, a reader will assume both pipelines share the same three layers, which is false.

---

### C3 — Architectural gap: `pipeline.lineage` table referenced in implementation plan but undefined in all design documents

**Location:** `2026-04-15-implementation-plan.md` Phase 3 Deliverables (`glue/jobs/ods_s3_publish.py` bullet: "Writes to `pipeline.lineage` after confirmed publish") and Phase 1 PostgreSQL schema (`pipeline.lineage` in the CREATE SCHEMA list) vs all other documents

**Issue:** `pipeline.lineage` appears in the implementation plan as a table the publish Glue job writes to after confirmed publish, and as an expected table in the Phase 1 infrastructure DDL. It does not appear in the PostgreSQL schema definitions in `2026-04-14-s3-kafka-design.md` Section 3, `ods-s3-kafka-overview-commentary.md` PostgreSQL section, the ADR, or the ingestion design. There is no schema definition for this table anywhere in the reviewed documents. Phase 6 testing gate also references "query `pipeline.lineage` for duplicates within business date" and Phase 7 T3 reconciliation references "duplicate message key check (query `pipeline.lineage`)". The CDC testing in Phase 6 references "CDC trace walkthrough (`2026-04-15-data-lineage.md` Section 4b)" — a document not included in this review set.

**Why this matters:** The implementation team cannot build the publish Glue job to the Phase 3 spec without knowing the schema of `pipeline.lineage`. It is a core output of Pattern 1 and a dependency of Patterns 2–4 and the T3 reconciliation job.

**Fix:** Define `pipeline.lineage` in the S3→Kafka design document (Section 3, PostgreSQL block) with at minimum: primary key, `run_id`, `source_type`, `source_ref`, `target_topic`, `message_key`, `kafka_partition`, `kafka_offset`, `business_date`, `lsn_position` (nullable, CDC only), `created_at`. Add a corresponding ADR decision for the lineage table (what it stores, why it is in PostgreSQL rather than the audit Kafka topic, and what its retention policy is). Reference `2026-04-15-data-lineage.md` explicitly in the S3→Kafka design so readers know it exists.

---

### C4 — Architectural gap: MSK authentication mode (D2) is a Phase 1 and Phase 2 blocker but has no design-level treatment

**Location:** `2026-04-15-implementation-plan.md` blocking decisions table (D2: "MSK authentication mode (IAM auth vs SASL/SCRAM) — Gates Phase 2 — Critical") vs `2026-04-14-s3-kafka-design.md` Section 3 (MSK section lists only topic names) vs `2026-04-14-architecture-decisions.md` Section 5

**Issue:** D2 is marked Critical and gates Phase 2, but there is no ADR for MSK authentication. The security section (ADR Section 5.1) says "MSK in-transit encryption enabled (TLS)" but is silent on authentication. The S3→Kafka design makes no mention of how Glue jobs authenticate to MSK (IAM roles vs SASL/SCRAM credentials). The implementation plan's Phase 1 Terraform module for MSK notes "auth per D2" — meaning it cannot be written until D2 is resolved — but no document gives the team any basis for making D2. The trade-offs between IAM auth and SASL/SCRAM are not captured anywhere.

**Fix:** Add an ADR entry for MSK authentication (even if the decision is still Proposed). Document the trade-offs: IAM auth is operationally simpler (no credential rotation) but adds per-request latency overhead and requires MSK to be in IAM-auth mode from creation (not retrofittable); SASL/SCRAM requires Secrets Manager credential management but is compatible with more Kafka client libraries. Give D2 an owner and a decision deadline — it is on the critical path for Phase 1.

---

## Significant Issues (should fix)

### S1 — Trigger mechanism contradiction between ingestion design and ingestion commentary

**Location:** `2026-04-14-ingestion-design.md` Section 1 Context diagram vs `ods-ingestion-commentary.md`

**Issue:** The ingestion design's context diagram (Section 1) shows the Publish Pipeline triggered by "Airflow Sensor → Glue → MSK". The correct trigger mechanism per the ADR (Section 1.2) and the S3→Kafka design is EventBridge, not an Airflow Sensor. The ADR explicitly states "What was replaced: Airflow S3 Sensor polling every N seconds". The commentary document correctly describes EventBridge (step ⓫ and step ⓴), but the design document's diagram still shows the old sensor model.

**Fix:** Update the context diagram in `2026-04-14-ingestion-design.md` Section 1 to show EventBridge as the trigger from S3 Curated to the Publish Pipeline, consistent with the EventBridge rule `ods-curated-file-rule-{env}` defined in that same document's Section 4.

---

### S2 — Missing ADR for the DAG trigger mechanism: EventBridge→MWAA REST API

**Location:** `2026-04-14-architecture-decisions.md` Section 1.2 vs `ods-ingestion-commentary.md` step ⓫ and `ods-s3-kafka-overview-commentary.md` step ❷

**Issue:** ADR Section 1.2 decides to use EventBridge for event-driven triggers, but the mechanism by which EventBridge triggers an MWAA DAG — specifically via the MWAA REST API — is mentioned only in the commentary documents, not in the ADR or either design document. This is a significant implementation detail: it requires the EventBridge target to be a Lambda (or API destination) that calls the MWAA REST API, or a direct EventBridge API destination configured for MWAA. The IAM permissions required for this target are non-trivial and not covered anywhere. The implementation plan Phase 1 includes an `eventbridge/` Terraform module and an `iam/` module but gives no detail on what the EventBridge→MWAA trigger target looks like.

**Fix:** Add to ADR Section 1.2 (or a new 1.2a): the integration pattern for EventBridge→MWAA (Lambda intermediary vs API destination), IAM requirements for the EventBridge execution role targeting MWAA, and the DAG trigger payload schema (file path, version ID, timestamp). This directly gates the Terraform `eventbridge/` and `iam/` modules in Phase 1.

---

### S3 — `file_catalogue` is referenced in the ADR idempotency table but not acknowledged as an ADR decision

**Location:** `2026-04-14-architecture-decisions.md` Section 1.5 mentions `file_catalogue` in the layer 1 description; `2026-04-14-ingestion-design.md` Section 3 lists "File approval: PostgreSQL `file_catalogue` whitelist" as a design decision; no ADR decision covers the catalogue whitelist pattern

**Issue:** The `file_catalogue` whitelist is a meaningful architectural choice — it is a pre-processing security and governance gate that prevents unapproved files entering the pipeline. It is presented as a decision in the ingestion design's architecture decisions table (Section 3) but has no corresponding ADR entry. The consequences of this design choice include: a new dataset cannot be onboarded without a database INSERT (not just a YAML config), operations must manage two artefacts per dataset (catalogue row + YAML config), and the catalogue becomes a source of truth that can go out of sync with the config bucket.

**Fix:** Add an ADR decision covering the file whitelist pattern: why PostgreSQL rather than a YAML-only allowlist, how the catalogue and YAML config relate (which is the source of truth for what?), and who owns catalogue entries in production.

---

### S4 — `glue_job_log` status sequence for ingestion is inconsistent between ADR and ingestion design

**Location:** `2026-04-14-architecture-decisions.md` Section 1.11 vs `2026-04-14-ingestion-design.md` Section 4 (PostgreSQL DDL) and `ods-ingestion-commentary.md` PostgreSQL section

**Issue:** ADR Section 1.11 defines the ingestion status sequence as: `started → schema_validated → dq_passed|dq_warned → converting → completed|failed`. The ingestion design DDL comment lists the same statuses: `started | schema_validated | dq_passed | dq_warned | converting | completed | failed`. However, the commentary document's DDL shows the same sequence for ingestion, so this is consistent. The issue is that the ADR lists `dq_passed|dq_warned` as a single transition step, which implies both are emitted per run. In practice, a run either passes DQ (emitting `dq_passed`) OR emits warnings (emitting `dq_warned`) — the two are not emitted sequentially for the same run. This ambiguity will cause log parsing code to handle the sequence incorrectly if it expects both states to always appear.

**Fix:** Clarify in ADR Section 1.11 that `dq_passed` and `dq_warned` are mutually exclusive status values for the same log row (not sequential rows). The status sequence should be written as: `started → schema_validated → dq_passed OR dq_warned → converting → completed | failed`.

---

### S5 — Implementation plan references documents not included in the design set and not yet established as existing

**Location:** `2026-04-15-implementation-plan.md` throughout

**Issue:** The implementation plan references the following documents in deliverable descriptions and testing gates:
- `2026-04-15-deployment-cicd.md` (Section 1, Phase 3, Phase 8)
- `2026-04-15-security-data-privacy.md` (Phase 5, Phase 1)
- `2026-04-15-schema-governance.md` (Phase 2)
- `2026-04-15-observability.md` (Phase 4, Phase 6)
- `2026-04-15-dataset-onboarding.md` (Phase 3, Phase 9)
- `2026-04-15-testing-strategy.md` (Phase 6, Phase 9, Phase 10)
- `2026-04-15-data-retention.md` (Phase 2)
- `2026-04-15-data-lineage.md` (Phase 6)

None of these documents are confirmed to exist in the repository or were available for this review. The implementation plan's testing gates cannot be evaluated without them. Phase 3's testing gate says "Run the full test checklist from `2026-04-15-dataset-onboarding.md` Section 10" — if that document does not exist or differs from what Phase 3 expects, the testing gate cannot be passed.

**Fix:** Confirm which of these companion documents exist. For those that do not exist, either: (a) inline the relevant content into the implementation plan sections that depend on it, or (b) track them as deliverables in Phase 0. At minimum, the testing gate for Phase 3 must be self-contained or link to a document that verifiably exists.

---

### S6 — No ADR for the count reconciliation mechanism (synchronous offset delta check)

**Location:** `2026-04-14-s3-kafka-design.md` Section 2 (Architecture Decisions table: "Count reconciliation: Synchronous (source row count vs Kafka offset delta)") and Section 4 Phase 3 description vs `2026-04-14-architecture-decisions.md`

**Issue:** Count reconciliation is listed as an architecture decision in the S3→Kafka design but has no corresponding ADR entry. The mechanism — comparing source row count to Kafka partition offset delta post-commit — has failure modes and trade-offs that warrant ADR treatment: the offset delta check assumes no other producer is writing to the same partition during the job (which may not hold in a multi-tenant MSK cluster), and a count mismatch routes records to DLQ but cannot identify which specific records were not published. The ingestion design has a parallel count check (written Parquet row count vs source CSV row count) which is also undocumented in the ADR.

**Fix:** Add an ADR entry covering count reconciliation for both pipelines, documenting: the offset-delta mechanism, its assumption about exclusive partition write access during a Glue job, what "undelivered records to DLQ" means in practice (the DLQ receives the full file, not the diff), and the relationship between the synchronous per-job count check and the asynchronous T2/T3 reconciliation jobs introduced in the implementation plan.

---

## Minor Issues / Improvements

### M1 — `ods-quarantine-{env}` bucket is in the ingestion design but not in the S3→Kafka design or ADR

**Location:** `2026-04-14-ingestion-design.md` Section 4 (S3 buckets list) vs `2026-04-14-s3-kafka-design.md` Section 3 (S3 buckets list)

The quarantine bucket appears in the ingestion design and is provisioned in the implementation plan Phase 1, but the S3→Kafka design's bucket list omits it. This is correct — the quarantine is only relevant to ingestion — but a reader comparing the two bucket lists will wonder whether the omission is intentional. A comment noting "quarantine bucket is ingestion-only, not listed here" would prevent confusion.

---

### M2 — DLQ partition structure uses `topic|dataset={name}` notation inconsistently

**Location:** `2026-04-14-architecture-decisions.md` Section 1.7 DLQ partition structure

The ADR shows `topic|dataset={name}` as a partition key. The pipe character is not a valid S3 path separator and will be URL-encoded or rejected by some tools. The intent is presumably `topic={name}` for publish pipeline DLQ writes and `dataset={name}` for ingestion pipeline DLQ writes (since the ingestion pipeline has no Kafka topic yet at DLQ time). If so, these should be two distinct path patterns, not a single pattern with a pipe. Confirm whether the DLQ path for ingestion failures uses `dataset=` and publish failures use `topic=`, and update the ADR accordingly.

---

### M3 — `ingestion_file_state` is missing the `etl_processing` status transition in some descriptions

**Location:** `2026-04-14-ingestion-design.md` Section 4 DDL (`status: detected | transferred | etl_processing | completed | failed`) vs `ods-ingestion-commentary.md` step descriptions

The commentary sets `status=detected` (step ❼), `status=transferred` (step ❿), then `status=completed` (step ⓰). There is no mention in the commentary of the state transitioning through `etl_processing`. If the state does not pass through `etl_processing`, the DDL comment is misleading. If it should transition through `etl_processing` (when DAG 2 starts), the commentary is missing a step and the implementation will omit the write.

**Fix:** Either add step ⓭+½ "Set file state → etl_processing when DAG 2 starts" to the commentary, or remove `etl_processing` from the DDL status enum if it is not actually used.

---

### M4 — Glue Job Bookmarks mentioned in S3→Kafka design but not in ingestion design or ADR

**Location:** `2026-04-14-s3-kafka-design.md` Section 8 Restartability: "Glue Job Bookmarks enabled to prevent S3 source re-reads on job retry"

The publish pipeline design mentions Glue Job Bookmarks as a restartability mechanism. The ingestion design has no equivalent mention, even though its Glue job also reads from S3 (S3 Raw → S3 Curated). Either Bookmarks are also used in the ingestion Glue job (in which case it should be documented) or they are deliberately not used (in which case the design should explain why — potentially because the ingestion job is expected to be fully re-runnable from scratch on retry, which is different from the publish pipeline's behaviour). This is also not captured in the ADR.

---

### M5 — Schema Registry compatibility mode is unresolved in ADR but resolved in implementation plan

**Location:** `2026-04-14-architecture-decisions.md` Section 3.2 (open items: "Schema Registry compatibility mode — Backward / Forward / Full per topic — Currently unspecified") vs `2026-04-15-implementation-plan.md` Phase 2 Deliverables ("Compatibility mode policy documented and applied per topic type (`BACKWARD` for S3/API, `FULL` for CDC/Events — per `2026-04-15-schema-governance.md` Section 5)")

The ADR marks the compatibility mode as an unresolved open item. The implementation plan treats it as resolved, specifying BACKWARD for S3/API and FULL for CDC/Events, and cites `2026-04-15-schema-governance.md` Section 5 as the source. If this decision has been made in `schema-governance.md`, the ADR open item in Section 3.2 should be closed and the decision cross-referenced. As written, the ADR and the implementation plan are in direct contradiction on whether this is decided.

**Fix:** If the decision is made in `schema-governance.md`, update ADR Section 3.2 to remove the open item and note "Resolved: see `2026-04-15-schema-governance.md` Section 5". If not yet resolved, remove the specific values from the implementation plan Phase 2 deliverables and replace with "per decision to be made in Phase 0 / D-new".

---

### M6 — `pipeline.reconciliation_log` and `ods.pipeline.reconciliation` Kafka topic appear only in the implementation plan

**Location:** `2026-04-15-implementation-plan.md` Phase 7 vs all other documents

The implementation plan introduces `pipeline.reconciliation_log` (PostgreSQL), `ods.pipeline.reconciliation` (Kafka topic), and `ods_reconciliation_t2.py` / `ods_reconciliation_t3.py` (Glue jobs) — none of which appear in any design document. The ADR does not have an entry for the reconciliation architecture. This is a significant platform component that adds to the shared layer and affects MSK topic inventory, PostgreSQL schema, and Glue job count.

**Fix:** Either add a reconciliation design document before Phase 7 starts, or add a minimal ADR entry covering: T1/T2/T3 reconciliation tier definitions, what `pipeline.reconciliation_log` stores, and why the reconciliation result is published to a Kafka topic (rather than only written to PostgreSQL).

---

### M7 — `dq_warned` status appears in `glue_job_log` statuses but the DQ section does not explain when a run can complete with `dq_warned` vs `dq_passed`

**Location:** `2026-04-14-s3-kafka-design.md` Section 6 vs Section 3 PostgreSQL DDL status list

The DQ section describes soft warns as emitting a CloudWatch metric and allowing the row to continue. The status sequence in the DDL includes both `dq_passed` and `dq_warned`. However, if soft warn rows continue to Kafka, does the job log status record `dq_warned` and then continue to `publishing → completed`, or does `dq_warned` only appear for jobs where at least one soft warn fired while the overall job still succeeds? This is a parsing detail that matters for the reconciliation and monitoring queries.

---

## Cross-Document Linking Gaps

The following are places where a concept defined in one document is used in another without a reference, creating a context gap for the reader.

1. **`file_catalogue` table** — Defined in `2026-04-14-ingestion-design.md` Section 4 and `ods-ingestion-commentary.md`. Referenced as Layer 1 in the ADR Section 1.5 idempotency table. The ADR does not cite the ingestion design as the source of the table definition.

2. **`pipeline.lineage` table** — First mentioned in `2026-04-15-implementation-plan.md` Phase 3 deliverables and Phase 1 DDL. Not defined in any design document. Implementation plan cites `2026-04-15-data-lineage.md` but that document is not in the review set.

3. **Glue Job Bookmarks** — Mentioned in `2026-04-14-s3-kafka-design.md` Section 8 without any ADR entry and without appearing in the ingestion design. No cross-reference either way.

4. **EventBridge → MWAA REST API trigger mechanism** — Described in `ods-ingestion-commentary.md` step ⓫ and `ods-s3-kafka-overview-commentary.md` step ❷. Not mentioned in either design document's data flow sections or in the ADR. The Terraform `eventbridge/` module in Phase 1 of the implementation plan cannot be specified without this detail.

5. **T2/T3 reconciliation tiers** — Defined in `2026-04-15-implementation-plan.md` Phase 7. Referenced in Phase 6 testing gate ("T3 check") and Phase 7. No design document and no ADR entry. The term "T1 consumer lag" in Phase 7 ("T1 already delivered in Phase 4") is introduced without prior definition anywhere.

6. **`2026-04-14-*-failure-and-recovery.md` documents** — Referenced in ADR Section 4.2 ("The failure/recovery documents are technical references"). These documents are not in the review set and are not listed as inputs to any phase in the implementation plan. If they exist, they should be listed as companion documents. If they do not exist, the reference should be removed.

7. **`ods-curated-file-rule-{env}` EventBridge rule** — Defined in both `2026-04-14-ingestion-design.md` Section 4 and `2026-04-14-s3-kafka-design.md` Section 3 as a shared rule. The ADR does not mention that this rule is shared between the two pipelines (ingestion writes to curated, publish consumes from curated). The shared ownership of this rule is a coordination risk — a change to the rule prefix or bucket by the ingestion team would silently break the publish pipeline.

8. **Kafka message lineage headers** — `x-ods-run-id`, `x-ods-source-ref`, `x-ods-source-type`, `x-ods-business-date`, `x-ods-schema-version`, `x-ods-pipeline-type` are listed as deliverables in `2026-04-15-implementation-plan.md` Phase 3 but do not appear in `2026-04-14-s3-kafka-design.md` and have no ADR entry. Consumer teams relying on these headers for their own lineage tracking have no stable spec to build against.

9. **`data_classification` YAML field** — Introduced in `2026-04-15-implementation-plan.md` Phase 5 as a required field in the dataset YAML config, but not present in the YAML config structures shown in `2026-04-14-ingestion-design.md` Section 9 or `2026-04-14-s3-kafka-design.md` Section 9. The YAML schema defined in the design documents is therefore incomplete relative to the implementation plan's requirements.

10. **`t3_check_schedule` and `t3_aggregate_fields` YAML fields** — Introduced in `2026-04-15-implementation-plan.md` Phase 7 as fields expected in the per-dataset YAML config, but absent from the YAML config structures in both design documents.

---

## Positive Observations

**Event-driven architecture is consistently applied and well-motivated.** The decision to replace S3 Sensor polling with EventBridge is captured in the ADR with explicit before/after comparison, the trade-offs are honest (S3 duplicate events, no backpressure, silent misconfiguration risk), and the mitigations (idempotency guard, zero-invocation alarm) are called out. This is a model ADR entry.

**Three-layer idempotency is a strong design.** Each layer addresses a distinct failure class (orchestration duplicate, partial write, consumer replay), the layers are independently effective, and the failure modes covered are documented. The decision to not rely on any single layer is architecturally sound for a batch pipeline with manual DLQ replay.

**INSERT-only audit log is the right call.** The decision to never UPDATE `glue_job_log` gives a complete execution timeline per run, enables duration analysis per stage, and avoids write conflicts under concurrent Glue job retry. The `BIGSERIAL` primary key providing chronological ordering is a practical operational detail that many teams miss.

**Config versioning with pinned S3 version IDs is elegant.** Pinning the config version ID at DAG trigger time and passing it to the Glue job ensures reproducibility and eliminates an entire class of "config changed mid-run" bugs. The trade-off (engineers must not use `latest`) is correctly identified.

**The S3 DLQ with structured partitioning is well-designed.** Partitioning by failure type, date, topic/dataset, and run_id makes DLQ contents queryable by Athena and scriptable for replay. The decision to make replay fully manual (requiring root cause investigation before re-processing) is the right operational stance for a data platform.

**The implementation plan testing gates are concrete and actionable.** Phase 3's nine specific test scenarios (happy path, idempotency, file not approved, checksum mismatch, schema incompatible, DQ hard block, DQ soft warn, count mismatch, business date extraction) are the right set of tests and are specified at exactly the right level of detail — enough for an engineer to implement without ambiguity.

**Shared platform layer design is future-proof.** The explicit decision to design Pattern 1 (S3→Kafka) as the platform template, with shared Schema Registry, DLQ, audit topic, PostgreSQL, and CloudWatch namespace, means adding CDC/API/Event patterns is genuinely incremental rather than a rewrite. The shared `ods-curated-file-rule-{env}` as the join point between ingestion and publish is a clean seam.

**Open items are tracked with appropriate honesty.** The ADR Sections 3.1 and 3.2 and the implementation plan blocking decisions table are unusually candid about what is not yet decided. This is professionally valuable — it gives an incoming team member a clear picture of where risk sits without requiring them to read between the lines.
