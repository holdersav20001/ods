# SRE / Operations Review — SRE Lead
**Reviewer role:** SRE Lead
**Date:** 2026-04-16
**Documents reviewed:**
1. `2026-04-15-deployment-cicd.md` — CI/CD pipeline and deployment strategy
2. `2026-04-15-disaster-recovery.md` — DR strategy, RTO/RPO, runbooks
3. `2026-04-15-observability.md` — Monitoring, alerting, dashboards, SLOs
4. `2026-04-15-implementation-plan.md` — Phased implementation plan
5. `2026-04-14-ingestion-failure-and-recovery.md` — Ingestion failure modes
6. `2026-04-14-s3-kafka-failure-and-recovery.md` — Publish pipeline failure modes
7. `2026-04-14-architecture-decisions.md` — Architecture decisions record

---

## Executive Summary

The ODS platform documentation set is substantially complete and internally consistent for a draft-stage design. The DR, observability, and CI/CD documents are individually well-reasoned and show strong engineering thinking. However, the suite has several meaningful gaps and inconsistencies that must be resolved before the platform can be operated safely in production. The most serious problems are: (1) RTO/RPO targets are in formal conflict between the DR doc and the implementation plan, creating ambiguity about what the platform is actually required to achieve; (2) the DR runbook set is incomplete — four of the six CDC, API, and Event pattern alarms point to runbooks marked "TBD", meaning on-call engineers have no documented recovery path for those patterns; (3) observability and CI/CD coverage of the CDC, API, and Event patterns is ahead of the DR coverage for those same patterns; and (4) the DAG S3 bucket is inconsistently named across documents, which will cause deployment failures. These issues are addressable, but they must be fixed before Phase 4 sign-off.

---

## Critical Issues (must fix)

### C-1. RTO/RPO targets are inconsistent between DR doc and implementation plan

**Issue:** The DR document (Section 2.2) proposes RTO/RPO targets and explicitly labels them `[REQUIRES SIGN-OFF]`. However, the implementation plan (Decision D5) lists "RTO/RPO targets for production" as a blocking decision that gates Phase 2 — stating it is `High` priority and must be resolved before Phase 2 can start. These two documents disagree on what has been decided:

- DR doc implies targets are proposed but not yet agreed, and are listed with specific values (e.g. MWAA environment failure RTO = 2 hr).
- Implementation plan implies D5 is an unresolved blocking decision, with no target values stated, that gates Phase 2.

If Phase 2 proceeds with the DR doc's proposed values (which are the only written-down targets), engineers will build to targets that have not received business sign-off. If Phase 2 blocks on D5, but D5 is never resolved because the DR doc values are treated as provisional, the platform can never start Phase 2.

**Location:** `2026-04-15-disaster-recovery.md` Section 2.2 vs `2026-04-15-implementation-plan.md` Blocking Decisions table, row D5.

**Exact text causing the conflict:**
- DR doc: `"[REQUIRES SIGN-OFF] ... MWAA environment failure ... Proposed RTO: 2 hr"`
- Implementation plan: `"D5 | RTO/RPO targets for production | Phase 2 | CTO + Business | High"`

**Suggested fix:** Either (a) collapse D5 into the DR doc Open Decisions table (OD-1 through OD-3 already cover this) and remove the D5 row from the implementation plan, or (b) explicitly cross-reference in both documents that OD-1/OD-2/OD-3 in the DR doc are the canonical location for D5. The DR doc is the right home for these targets. The implementation plan should reference it: "D5 resolved by sign-off on DR doc Section 11 items OD-1, OD-2, OD-3."

---

### C-2. DAG S3 bucket name inconsistency between documents

**Issue:** The CI/CD document uses `ods-dags-{env}` as the MWAA DAG bucket name throughout (Sections 6.1, 6.3, 6.5, and the directory layout in Section 1.2). The DR document (Section 3.2.2, Scenario MWAA Environment Failure recovery step 3) refers to the DAG location as `ods-config-{env}/dags/` S3 prefix. Section 4.4 of the DR document reinforces this with `"DAG location: ods-config-{env}/dags/ S3 prefix"`. The implementation plan (Phase 1, S3 buckets list) creates `ods-dags-{env}` as a separate bucket — which aligns with the CI/CD doc. The DR doc's reference to `ods-config-{env}/dags/` appears to be an error.

**Location:**
- `2026-04-15-disaster-recovery.md` Sections 3.2.2 and 4.4: `"ods-config-{env}/dags/"` and `"ods-config-{env}/plugins/"`
- `2026-04-15-deployment-cicd.md` Section 6.3: `"BUCKET="ods-dags-${ENV}"`
- `2026-04-15-implementation-plan.md` Phase 1: `"ods-dags-{env} — MWAA DAG bucket"`

**Exact text in DR doc (incorrect):**
- Section 3.2.2: `"DAGs are stored in ods-config-{env} S3 bucket — re-point new environment to this DAG folder"`
- Section 4.4: `"DAG location: ods-config-{env}/dags/ S3 prefix"`

**Suggested fix:** Update DR doc Sections 3.2.2 and 4.4 to use `ods-dags-{env}` throughout. `ods-config-{env}` should be referenced only for dataset configs and DQ rules. If DAG files genuinely are intended to live in `ods-config-{env}` for some recovery-specific reason, this must be explicitly reconciled with the CI/CD and implementation plan docs — the current state is an unresolved contradiction that will cause a deployment failure when the DR recovery procedure is followed.

---

### C-3. Missing DR runbooks for CDC, API, and Event patterns

**Issue:** The observability document defines alarms for CDC, API, and Event patterns (Sections 3.5, 3.6, 3.7). The alarm-to-runbook mapping table (Section 9) for those patterns consistently points to `"CDC runbook (TBD)"`, `"API runbook (TBD)"`, and `"Event runbook (TBD)"`. No such runbooks exist anywhere in the document set, and none are planned in the DR document. The DR doc's failure scenario catalogue (Section 3) covers only S3 batch pipeline components (MWAA, Glue, RDS, MSK, S3, EventBridge, SFTP) — it does not address CDC connector failures, API cursor drift, or event sequence gaps at the DR level.

For the CDC pattern specifically, there is a P1 alarm (`ods-cdc-replication-slot-bloat-{env}`) whose runbook note says "alert DBA" — but there is no documented escalation path, no guidance on how to safely pause the connector without data loss, and no procedure for recovering from replication slot exhaustion on the source DB.

**Location:** `2026-04-15-observability.md` Sections 9.4, 9.5, 9.6 (all point to TBD runbooks); `2026-04-15-disaster-recovery.md` Section 3 (no CDC/API/Event scenarios).

**Suggested fix:** Either (a) add a Section 3.10–3.12 to the DR document covering CDC connector failure, API cursor drift, and event routing failures as platform-level DR scenarios, or (b) create separate pattern-specific runbook documents before those patterns go to production. At minimum, the replication slot bloat scenario must have a concrete runbook before CDC goes live — it is the one failure mode that can cause disk exhaustion on the source DB if undetected, and there is currently zero documented response.

---

### C-4. Observability doc routes an infrastructure alarm to the ADR, not a runbook

**Issue:** The observability document (Section 9.3) routes two infrastructure alarms — `ods-mwaa-queue-high-{env}` and `ods-mwaa-queue-critical-{env}` — to `2026-04-14-architecture-decisions.md` Section 1.1. The ADR is a design decisions document, not an operational runbook. Section 1.1 of that document explains the rationale for choosing MWAA; it does not tell an on-call engineer what to do when the queue is critical at 02:00. Similarly, `ods-rds-connections-high-{env}` and `ods-rds-connections-critical-{env}` are routed to the ADR Section on "PostgreSQL State Store."

**Location:** `2026-04-15-observability.md` Sections 9.3 rows for MWAA queue and RDS connection alarms.

**Exact text (incorrect runbook references):**
- `"ods-mwaa-queue-high-{env} ... Runbook Document: 2026-04-14-architecture-decisions.md | Section 1.1 — MWAA Orchestration"`
- `"ods-rds-connections-critical-{env} ... Runbook Document: 2026-04-14-architecture-decisions.md | Section — PostgreSQL State Store"`

**Suggested fix:** These four alarms should point to the DR document scenarios (3.2.1 for MWAA saturation, 3.4.1 for RDS connection exhaustion). The DR doc has the correct step-by-step recovery procedures. Update the runbook reference column in Sections 9.3 to: `2026-04-15-disaster-recovery.md` Sections 3.2.1 and 3.4.1 respectively.

---

### C-5. Partial Glue crash during ETL can silently trigger the publish pipeline

**Issue:** The ingestion failure doc (Section 1.8) notes: "If a partial Parquet landed in S3 Curated before the crash, EventBridge may have already triggered the Publish Pipeline." It then says the fix is to either remove partial files or "accept overwrite at publish layer." The DR document does not address this race condition at all. The CI/CD doc does not include a pre-deployment step to detect or protect against in-flight jobs.

This is more than a race condition in normal operation — it is a DR gap. If a Glue ETL job crashes at 60% through writing a large Parquet file, the partial Parquet triggers the Publish DAG, which publishes a partial dataset to Kafka. When the ETL job is retried and succeeds, a second Publish DAG run fires and publishes the full dataset. Consumers then receive the partial dataset followed by the full dataset. While deterministic keys protect against semantic duplication, consumers of systems that do not implement last-write-wins upserts (e.g. append-only sinks) will receive the partial data permanently.

**Location:** `2026-04-14-ingestion-failure-and-recovery.md` Section 1.8 vs `2026-04-15-disaster-recovery.md` (no coverage of this scenario).

**Suggested fix:** Add this as Scenario 3.3.1b in the DR doc (Glue ETL crash with partial S3 write). The recommended mitigation is to write Parquet to a staging prefix (e.g. `ods-curated-{env}/_staging/{run_id}/`) and atomically move (rename) to the final prefix on job completion. The EventBridge rule should target the final prefix, not the staging prefix. This eliminates the race condition entirely. If the atomic rename pattern is not adopted, the ingestion failure doc should at minimum describe the explicit cleanup steps (delete partial file before retry) rather than leaving it as an open option.

---

## Significant Issues (should fix)

### S-1. SLO targets in observability doc are not reflected in the DR RTO table

**Issue:** The observability document defines three SLOs: File-to-Kafka p95 < 10 minutes, File-to-Curated p95 < 15 minutes, and Pipeline Success Rate > 99.5%. The DR document's RTO table (Section 2.2) for "Single component failure (Glue job, EventBridge rule)" is 15 minutes, and for "MWAA worker saturation" is 30 minutes. A single Glue job failure with a 15-minute RTO means the pipeline can be down for 15 minutes before recovery — during which time the e2e latency SLO (10-minute p95) is already being breached. The RTO and SLO targets are not calibrated against each other.

**Location:** `2026-04-15-observability.md` Section 4.2 vs `2026-04-15-disaster-recovery.md` Section 2.2.

**Suggested fix:** Add a column to the DR RTO table: "SLO impact at this RTO." For each scenario, state whether the 10-minute e2e latency SLO is breached at the proposed RTO and what error budget consumption rate results. This forces an explicit business discussion: "A Glue failure with 15-minute RTO means every file affected by that failure breaches the 10-minute SLO. Is that acceptable within the 5% error budget?"

---

### S-2. CI/CD doc's post-deploy monitoring period does not reference SLO burn rate

**Issue:** The production deployment checklist (Section 12) specifies a 30-minute post-deployment observation window, with instructions to watch for `JobFailed` and `JobTimeout` alarms. It does not mention checking the SLO burn rate alarms (`ods-slo-fast-burn-{env}`) during the observation window. A deployment that causes subtle latency increases rather than outright failures could be silently burning the error budget without triggering any of the alarms on the checklist.

**Location:** `2026-04-15-deployment-cicd.md` Section 12, "Post-Deployment (30-Minute Observation Window)."

**Suggested fix:** Add two line items to the post-deployment checklist:
- `[ ] SLO burn rate alarm `ods-slo-fast-burn-{env}` is not firing`
- `[ ] E2E latency p95 (`pipeline.e2e.latency`) is below 8 minutes (well within the 10-minute SLO — any drift toward the threshold during the observation window is cause for rollback)`

---

### S-3. DR doc MWAA environment recovery step points to wrong DAG bucket

**Issue:** Already flagged as C-2, but the specific recovery procedure in Scenario 3.2.2 has a downstream consequence: Step 3 of the MWAA Environment Failure recovery procedure tells the operator to "re-point new environment to this DAG folder" (`ods-config-{env}`). If the actual bucket is `ods-dags-{env}`, an engineer following this procedure during a 02:00 outage will point the new MWAA environment at the wrong bucket. The environment will provision successfully but no DAGs will load, causing wasted time diagnosing a phantom empty-DAG environment.

**Location:** `2026-04-15-disaster-recovery.md` Section 3.2.2, Step 3.

**Exact text:** `"DAG definitions are stored in ods-config-{env} S3 bucket — re-point new environment to this DAG folder."`

**Suggested fix:** This is the same root fix as C-2, but it needs to be correct specifically in this recovery procedure because it is the one place where a wrong bucket name causes a cascade failure at 02:00.

---

### S-4. No MWAA DAG bucket referenced in infrastructure alarm for DAG import errors

**Issue:** The DR doc (Appendix A) does not include an alarm for DAG import errors (`MWAA Import Error` state). The observability doc (Section 3.2 alarm mapping) lists `ods-job-failure-{env}` as a P1 alarm but this fires on DAG task failures, not on DAG parse errors. A broken DAG import prevents the DAG from ever running and thus never fires a task failure. The DR doc scenario 3.2.3 (DAG Import Error) correctly identifies this failure but Appendix A does not include it in the alarm reference table.

The CI/CD doc (Section 6.4) shows a post-deployment check that polls MWAA for import errors, but this is a deployment-time check only — not a continuous runtime alarm.

**Location:** `2026-04-15-disaster-recovery.md` Appendix A (alarm table has no MWAA import error entry); `2026-04-15-observability.md` (no alarm defined for DAG import error state).

**Suggested fix:** Add an alarm on MWAA `ImportError` count to both the observability alarm catalogue and the DR Appendix A reference table. MWAA emits the `AWS/MWAA` metric `SyntaxErrors` (or equivalent) for DAG parse failures. This should be a P2 alarm (single DAG broken), escalating to P1 if the import error is in a shared utility (`dag_utils.py`).

---

### S-5. DR doc's EventBridge SQS DLQ is not mentioned in the observability or CI/CD docs

**Issue:** The DR document (Section 4.5) recommends an SQS DLQ per EventBridge rule to capture undeliverable events. This is sound. However, neither the observability doc nor the CI/CD doc mentions this SQS DLQ. The observability doc defines a `dlq.record.count` metric emitted by a Lambda on S3 DLQ writes (Section 2.4) — but the EventBridge SQS DLQ is a different construct that gets populated when EventBridge cannot invoke MWAA, not when Glue writes to S3. There is no metric or alarm defined for the EventBridge SQS DLQ depth. If EventBridge events start landing in the SQS DLQ (meaning MWAA is unavailable and EventBridge cannot trigger it), those events are silently accumulating with no alert.

**Location:** `2026-04-15-disaster-recovery.md` Section 4.5 vs `2026-04-15-observability.md` Section 2.4 and Appendix A.

**Suggested fix:** Add a CloudWatch alarm on the SQS DLQ depth for each EventBridge rule's DLQ. This is a native SQS metric (`ApproximateNumberOfMessagesVisible > 0`). Add it to the observability Appendix A implementation checklist and the alarm mapping table.

---

### S-6. Flyway undo scripts use inconsistent naming convention within the CI/CD doc

**Issue:** The CI/CD document Section 9.2 defines the migration file naming convention as `V{version}__{description}.sql` for forward migrations and `V{version}__{description}.undo.sql` for undo scripts. However, the Phase 8 deliverables in the implementation plan (Section on Flyway migration pipeline) describe undo scripts as `U{N}__{description}.sql` — a different naming format. These two conventions are incompatible. Flyway open-source uses `U` prefix for undo; the CI/CD doc's `.undo.sql` suffix is a custom convention that requires custom scripting to invoke (as acknowledged in Section 10.6: "Manual undo (open-source Flyway)"). The implementation plan description implies standard Flyway `U` prefix, which works with the built-in `flyway undo` command in Flyway Teams but not open-source Flyway.

**Location:** `2026-04-15-deployment-cicd.md` Section 9.2 (`.undo.sql` suffix) vs `2026-04-15-implementation-plan.md` Phase 8 deliverables (`U{N}__` prefix).

**Exact text (implementation plan):** `"All migration files in db/migrations/V{N}__{description}.sql ... Corresponding db/migrations/U{N}__{description}.sql undo scripts"`

**Suggested fix:** Standardise on one convention. Given the CI/CD doc explicitly uses open-source Flyway (Section 9.1) and invokes undo via `psql` manually (Section 10.6), the `.undo.sql` suffix with manual invocation is the consistent choice. Update the implementation plan to use `V{N}__{description}.undo.sql` throughout. Alternatively, if Flyway Teams is adopted, update the CI/CD doc to use `U{N}__` prefix and remove the manual psql undo approach.

---

### S-7. Consumer lag alarm thresholds differ between observability and implementation plan

**Issue:** The observability document (Section 3.3) defines consumer lag alarms with thresholds of `> 50,000` (P2) and `> 200,000` (P1). The implementation plan Phase 4 deliverables specify consumer lag warning at `lag > 1,000 records for > 5 minutes` and critical at `lag not shrinking for > 15 minutes`. The implementation plan also does not use a message count threshold — it uses a "not shrinking" time-based criterion, which is a fundamentally different alarm model.

**Location:** `2026-04-15-observability.md` Section 3.3 vs `2026-04-15-implementation-plan.md` Phase 4 deliverables.

**Exact text (implementation plan):** `"Warning threshold: lag > 1,000 records for > 5 minutes ... Critical threshold: lag not shrinking for > 15 minutes"`

**Suggested fix:** Reconcile these into a single set of thresholds in the observability doc. The observability doc's absolute count thresholds (50K/200K) are more operationally precise. The implementation plan's "not shrinking" criterion could be added as a third alarm type (staleness alarm) that complements the count threshold alarms. Update the implementation plan to reference the observability doc as the authoritative source of alarm thresholds, rather than defining them independently.

---

## Minor Issues / Improvements

### M-1. DR Appendix A alarm `GLUE-001` threshold does not match observability doc

**Issue:** DR Appendix A defines `GLUE-001` as `Glue/JobRunsFailed > 3 in 30 min` at P2. The observability doc (Section 3.2) defines `ods-job-failure-{env}` as `job.failed ≥ 1` at P1. One Glue job failure is P1 in the observability doc but requires 3 failures before alarming at P2 in the DR doc. These two tables describe the same alarm differently.

**Location:** `2026-04-15-disaster-recovery.md` Appendix A vs `2026-04-15-observability.md` Section 3.2.

**Suggested fix:** Align on P1 at first failure. The observability doc's definition is correct for a data pipeline — one failed file not reaching Kafka is a P1 event.

---

### M-2. DR doc cites "RDS Proxy — Required in prod" but implementation plan does not include it

**Issue:** The DR document (Section 4.1) states `"RDS Proxy: Required in prod"` as a component resilience configuration item. The implementation plan's Phase 1 deliverables list RDS as a Terraform module but do not include RDS Proxy. Phase 5 (Security Hardening) does not include it either. If RDS Proxy is required for production, it needs to be in the Phase 1 or Phase 2 Terraform deliverables.

**Location:** `2026-04-15-disaster-recovery.md` Section 4.1 vs `2026-04-15-implementation-plan.md` Phase 1 deliverables.

**Suggested fix:** Add RDS Proxy to the Phase 1 Terraform module deliverables (`infra/modules/rds/` should include an RDS Proxy resource). Gate its activation on load testing results if cost is a concern, but the Terraform resource should exist from Phase 1.

---

### M-3. Observability doc's `ods-rds-storage-low` runbook points to "Ops runbook (TBD)"

**Issue:** The observability doc (Section 9.3) routes `ods-rds-storage-low-{env}` (P2) to `"Ops runbook (TBD) — RDS Storage Expansion"`. The DR document has a complete procedure for storage full in Scenario 3.4.4, including enabling autoscaling and manually increasing allocation. This procedure already exists and should be referenced.

**Location:** `2026-04-15-observability.md` Section 9.3.

**Suggested fix:** Change the runbook reference to `2026-04-15-disaster-recovery.md` Section 3.4.4.

---

### M-4. Glue job crash warning in ingestion failure doc uses ambiguous language

**Issue:** Section 1.8 of the ingestion failure doc describes the Glue crash recovery as "Reset file state to `transferred` (not `new`) to skip the SFTP copy step." However, the DR document's full replay runbook (Section 5.2 Step 2) resets state to `pending`, and the resubmission procedure in the same doc (Section 3.2) resets to `new`. Three different state values are used in three different documents for what appear to be similar "retry this file" operations. The state machine is not described in any document — the valid state transitions (`new` → `detected` → `transferring` → `transferred` → `processing` → `completed` / `failed`) are only implicit.

**Location:** `2026-04-14-ingestion-failure-and-recovery.md` Section 1.8 vs Section 3.2 vs `2026-04-15-disaster-recovery.md` Section 5.2.

**Suggested fix:** Add a brief state machine diagram to either the ingestion failure doc or the DR doc showing valid states and transitions. Each recovery procedure should then specify which state to reset to and why — not just the value, but the semantic reason (e.g. "reset to `transferred` to re-run ETL only, skipping SFTP copy").

---

### M-5. CI/CD doc's MWAA post-deployment verification is incomplete

**Issue:** Section 6.4 of the CI/CD doc queries `MWAA/LastDagProcessorActivity` and acknowledges in a comment: `"A more complete check would query the MWAA REST API for DAG import errors."` This is a known incomplete implementation that is left as a stub.

**Location:** `2026-04-15-deployment-cicd.md` Section 6.4, the bash block comment.

**Suggested fix:** Replace the stub with a functional check. The MWAA web server URL can be retrieved via `aws mwaa get-environment --name ${MWAA_ENV} --query Environment.WebserverUrl`. The MWAA REST API endpoint `/dags?only_active=false&limit=100` returns DAGs with their `has_import_errors` field. A CI check can assert `has_import_errors=false` for all DAGs after deployment. This is straightforward to implement and closes a real deployment safety gap.

---

### M-6. DR doc Section 8.5 recommendation refers to "Option F" which does not exist

**Issue:** The regional outage decision tree (Section 8.6, Mermaid diagram) refers to node U: `"When region recovers: follow Option F path"` with an arrow `U --> F`. "Option F" does not exist in the diagram — the recovery path after waiting is node F (`"Region recovered?"`), but calling it "Option F" is confusing. The Mermaid node identifier `F` and the label "Option F" are conflated.

**Location:** `2026-04-15-disaster-recovery.md` Section 8.6, Mermaid diagram node U and T.

**Suggested fix:** Change node U's label from `"When region recovers: follow Option F path"` to `"When region recovers: return to monitoring loop"` and add an arrow from U directly to F (the "Region recovered?" decision node). This makes the diagram self-describing without relying on an unstated "Option F" label.

---

### M-7. Implementation plan Phase 4 does not reference the DR runbook deliverables

**Issue:** Phase 4 (Observability and Operations) lists runbooks as deliverables: MWAA worker saturation, RDS unavailable, MSK broker unavailable, Glue DPU quota exhausted, DLQ growth response. These exact scenarios are already documented in `2026-04-15-disaster-recovery.md` Sections 3.2.1, 9.1, 9.2, 3.3.2, and 3.5.2 respectively. The implementation plan does not reference the DR document — it implies these runbooks must be written from scratch as Phase 4 deliverables, when in fact the DR document already provides them.

**Location:** `2026-04-15-implementation-plan.md` Phase 4 deliverables, "On-call runbooks written" section.

**Suggested fix:** Update the Phase 4 runbook deliverables to say: "On-call runbooks reviewed and confirmed against `2026-04-15-disaster-recovery.md` Sections 9.1–9.3. Any gaps between DR doc and Phase 4 operational requirements documented as follow-up tickets." This prevents duplicating content and ensures the DR doc remains the single authoritative source.

---

## Cross-Document Linking Gaps

The following places in the documents should reference another document but do not:

1. **DR doc Section 2.2 (RTO/RPO table) → observability doc Section 4 (SLOs).** The RTO table should note which SLOs are breached at each RTO level. Currently these exist in separate documents with no cross-reference.

2. **DR doc Section 5.2 (replay procedure) → CI/CD doc Section 10.1–10.3 (rollback procedures).** The replay procedure resets PostgreSQL state but does not reference the CI/CD doc's config/script rollback procedures, which may also be needed if a bad Glue job version caused the data corruption requiring replay.

3. **CI/CD doc Section 10 (rollback procedures) → DR doc Section 9.3 (full replay runbook).** The CI/CD rollback covers artifact-level rollback (Glue scripts, DAGs, configs). It should cross-reference the DR doc for the scenario where a rollback is insufficient and a full data replay is required.

4. **Ingestion failure doc Section 3 (recovery procedures) → DR doc Section 5.2 (S3 replay).** The ingestion failure doc's recovery steps describe file-level resubmission but do not reference the DR doc's bulk replay capability, which is more appropriate when many files are affected.

5. **Observability doc Section 9.1 alarm table row `ods-eventbridge-failure-{env}` → DR doc Section 3.7.1.** The observability doc currently routes this alarm to `2026-04-14-architecture-decisions.md Section 1.2` (an ADR, not a runbook). It should point to `2026-04-15-disaster-recovery.md` Section 3.7.1 (EventBridge Rule Misconfiguration recovery procedure).

6. **Implementation plan Phase 3 deliverables → DR doc Section 10 (DR testing).** Phase 3 establishes the replay infrastructure but does not reference the DR testing requirements. The Phase 3 testing gate should include at least one replay exercise as a prerequisite to Phase 4, since Phase 4 is supposed to make the platform production-ready.

7. **Implementation plan Phase 4 → DR doc Section 10.2 (quarterly DR test plan).** Phase 4 signs off on "observability and operations" but does not establish the DR test cadence. The DR test plan (Q1–Q4 tests) should be referenced as a Phase 4 deliverable, not an implied follow-on activity.

8. **DR doc Appendix B (S3 bucket reference) → CI/CD doc Section 1.2 (directory layout).** The bucket name `ods-scripts-{env}` and `ods-dags-{env}` appear in Appendix B but not in the DR doc's bucket table (which lists raw, curated, config, dlq, audit-sink). The Glue scripts bucket and DAG bucket should be in the DR Appendix B reference table since their loss during a DR event affects the ability to run recovery procedures.

9. **CI/CD doc Section 12 production deployment checklist → observability doc Section 10.6 (first check after alarm).** The post-deploy observation guidance in the observability doc is the most actionable description of what "normal" looks like. The CI/CD checklist should reference it rather than listing a partial set of monitoring items.

---

## Positive Observations

1. **The three-layer idempotency design is a genuine reliability asset.** The combination of PostgreSQL file state guard, Kafka exactly-once transactions, and deterministic SHA-256 message keys means that replay and recovery operations are safe by design, not by convention. This is correctly identified in the DR doc as the primary DR capability and it is consistently implemented across the ingestion and publish failure docs.

2. **The S3 Raw permanent archive as a DR foundation is architecturally sound.** The statement "the entire state of ods-curated-{env} and all Kafka topics can be reconstructed from ods-raw-{env} alone" is backed up by the detailed replay procedure in DR Section 5.2. Few platforms can make this claim with operational procedures to back it up.

3. **The observability document's coverage of all four ingestion patterns is exceptional.** Having metrics catalogues for CDC (Section 2.5), API (Section 2.6), and Event (Section 2.7) patterns ready before those patterns are implemented is exactly the right approach. It forces the pattern implementation to satisfy observable requirements rather than retrofitting metrics later.

4. **The SLO burn rate alarm model is correctly implemented.** The dual-window approach (short window AND long window must both breach) in Section 4.5 of the observability doc is a best-practice SLO alerting pattern that avoids false positives from brief spikes. The alarm names, thresholds, and severity tiers are well-calibrated.

5. **The CI/CD rollback procedures are specific and actionable.** Sections 10.1–10.6 of the CI/CD doc give the exact AWS CLI commands for each artifact type's rollback, including how to identify the previous version from the audit log. An on-call engineer can follow these without reference to other documents.

6. **The deployment ordering rationale is explicitly documented.** The CI/CD doc (Section 4.3) explains why the deployment order is: DB migrations → IaC → configs → Glue scripts → DAGs. This prevents the common mistake of deploying code before the schema is ready. The ordering is correct and the rationale is clear.

7. **The quarterly DR test plan is structured with pass criteria.** DR Section 10.2 defines each test with a specific pass criterion rather than a vague "verify it works." "Failover completes in < 120 seconds; pipeline DAG runs resume without manual intervention within 3 minutes" is measurable and testable.

8. **The `run_id` correlation key is well-designed and pervasive.** Section 7 of the observability doc shows that `run_id` is propagated to PostgreSQL, Kafka headers, log fields, Glue job parameters, and CloudWatch metric dimensions. The step-by-step trace guide (Section 7.2) is exactly what an on-call engineer needs at 02:00.

9. **The shadow config deployment pattern is an operationally mature approach.** CI/CD Section 11.2 describes a genuine canary approach for high-risk config changes (new `schema_id`, changed `key_fields`) using a shadow config key. This reduces the blast radius of config changes without requiring a separate deployment pipeline.

10. **The open decisions tables in both the DR doc (Section 11) and the implementation plan are explicit about what is unresolved.** Naming the decision owner and tagging items `[REQUIRES SIGN-OFF]` means that gaps in the design are visible and owned, not buried in text. This is good governance practice.

---

*End of review. 2026-04-16. Prepared by SRE Lead.*
