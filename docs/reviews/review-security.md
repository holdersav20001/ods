# Security Review — Aviva ODS Platform

**Reviewer:** Security Engineer (peer review)
**Date:** 2026-04-16
**Status:** Draft — awaiting response to open questions

## Documents Reviewed

| # | Document | Status at review time |
|---|----------|-----------------------|
| 1 | `2026-04-15-security-data-privacy.md` | Draft — 15 open decisions, 5 marked Blocker |
| 2 | `2026-04-14-architecture-decisions.md` | Approved |
| 3 | `2026-04-14-ingestion-design.md` | Approved |
| 4 | `2026-04-14-s3-kafka-design.md` | Approved |
| 5 | `2026-04-15-data-retention.md` | Draft |
| 6 | `2026-04-15-data-lineage.md` | Draft |
| 7 | `2026-04-15-schema-governance.md` | Draft |
| 8 | `2026-04-15-disaster-recovery.md` | Draft |
| 9 | `2026-04-15-deployment-cicd.md` | Draft |
| 10 | `2026-04-15-implementation-plan.md` | Draft |

---

## Executive Summary

The security document is substantially complete and reflects strong security thinking: least-privilege IAM, KMS key-per-purpose, VPC isolation, and a clear data classification framework are all well-designed. However, cross-document consistency is a significant problem. At least two approved documents (architecture-decisions and ingestion-design) contradict or predate the security requirements in ways that will block production go-live: the architecture ADR explicitly permits SSE-S3 encryption where the security doc requires SSE-KMS with customer-managed keys, and both approved design documents are missing the security configuration fields that the security doc mandates. Additionally, the GDPR right-to-erasure strategy has a material gap — the crypto-shredding design addresses Kafka and curated data but does not resolve deletion of plaintext PII files in the S3 Raw Zone, which are the original source of truth. The CI/CD pipeline lacks security scanning gates (SAST, secrets detection, dependency auditing) and the config validator does not check for mandatory data-classification fields, meaning non-compliant configurations can reach production today. Seven of the thirteen issues below should be resolved before any production-bound sprint is locked.

---

## Critical Issues

These issues represent either a direct regulatory exposure, an architectural contradiction that will require rework, or a gap that could allow PII to reach production without appropriate controls.

---

### C1 — SSE-KMS vs SSE-S3 contradiction between ADR and security doc

**Location:** `2026-04-14-architecture-decisions.md` Section 5.1 ("Data at Rest") vs `2026-04-15-security-data-privacy.md` Section 4.1 and Section 6

**Issue:** The architecture ADR (status: Approved) states: "All data at rest in S3 encrypted with SSE-S3 or SSE-KMS (standard AWS default)." The phrase "standard AWS default" refers to SSE-S3, which uses AWS-managed keys. The security doc mandates SSE-KMS with customer-managed keys (CMKs) for all layers. These are mutually exclusive: SSE-S3 does not permit key rotation control, does not allow `Deny` conditions on key use, and does not support the KMS key-per-purpose strategy described in the security doc. If Terraform is built to the ADR as written, buckets may be provisioned with SSE-S3, making the KMS IAM policies inoperative.

**Risk:** Critical. Regulatory (GDPR, FCA), and operational — the entire KMS-based access control and crypto-shredding architecture requires CMKs. SSE-S3 buckets would render Sections 4, 6, 7, and the erasure strategy non-functional.

**Suggested fix:** Update architecture-decisions.md Section 5.1 to explicitly state "SSE-KMS with customer-managed keys only; SSE-S3 is not permitted on any bucket containing Internal, Confidential, or Restricted-PII data." Trigger a review of any Terraform modules that may already have been written to the ADR.

---

### C2 — Audit sink S3 retention: 365 days (security doc) contradicts 6 years (retention doc)

**Location:** `2026-04-15-security-data-privacy.md` Section 9.1 vs `2026-04-15-data-retention.md` Section 3.2

**Issue:** The security doc specifies "365 days (S3)" for `ods-audit-sink-{env}`. The data retention doc specifies a tiered lifecycle for the same bucket: Standard 90 days → Standard-IA 1 year → Glacier Instant 3 years → Glacier Deep Archive 6 years → delete. These are flatly inconsistent. The audit sink contains CloudTrail logs, Kafka audit topic data, and pgaudit output. FCA SYSC 9.1 and MiFID II Article 25 require financial firms to retain records for a minimum of 5-7 years. Deleting after 365 days would create a regulatory breach.

**Risk:** Critical. Regulatory (FCA, potential PRA). Audit evidence required for incident response or regulatory investigation would be unavailable.

**Suggested fix:** The security doc must be updated to match the retention doc's 6-year schedule. The 365-day figure should be treated as the Standard-tier transition point, not the deletion date. Add a single source of truth for retention by cross-referencing the retention doc from the security doc rather than duplicating figures.

---

### C3 — GDPR erasure gap: S3 Raw Zone plaintext PII not addressed

**Location:** `2026-04-15-security-data-privacy.md` Section 3.2 (crypto-shredding) vs `2026-04-15-data-retention.md` Appendix A Open Item 5

**Issue:** The crypto-shredding strategy encrypts data before writing to Kafka using a per-entity DEK. Deleting the DEK from the KMS key store renders Kafka messages and curated database rows unreadable. However, the S3 Raw Zone (`ods-raw-{env}`) holds the original plaintext CSV files ingested via SFTP — these are written before any encryption occurs. Deleting the Kafka DEK has no effect on the S3 Raw file. A UK GDPR Article 17 erasure request satisfied by crypto-shredding alone leaves the data subject's plaintext PII intact in `ods-raw-{env}`. The retention doc Appendix A Open Item 5 acknowledges this as an unresolved pending item.

**Risk:** Critical. UK GDPR Article 17 non-compliance. The right to erasure is not fulfilled unless the Raw Zone files are also addressed. If `ods-raw-{env}` uses S3 Object Lock (GOVERNANCE mode, 7 years, as recommended in the DR doc), object-level deletion may not be possible at all, making this gap structurally unresolvable without a different Raw Zone approach.

**Suggested fix:** Make a binding decision on one of these options: (a) encrypt Raw Zone files at write time with the same per-entity DEK so that crypto-shredding erases them too; (b) store Raw Zone files with only a pseudonymous key and destroy the mapping table on erasure; (c) accept that Raw Zone files will be purged on a short retention schedule (e.g., 90 days) before an erasure request could be acted upon. Option (a) integrates cleanly with the existing crypto-shredding design. This must be resolved before production and before S3 Object Lock is applied. Coordinate with the DPO and Legal.

---

### C4 — YAML pipeline config security fields absent in both approved design documents

**Location:** `2026-04-14-ingestion-design.md` Section 9 and `2026-04-14-s3-kafka-design.md` Section 9 vs `2026-04-15-security-data-privacy.md` Section 2.3 and Section 3

**Issue:** Both approved design documents define the YAML configuration schema for pipeline datasets. Neither includes the fields mandated by the security doc: `data_classification`, `pii_fields`, `gdpr_erasure_strategy`, `retention_raw_days`, `retention_curated_days`. These fields are the mechanism by which the security classification framework is applied per dataset. The `s3-kafka-design.md` is also the template for all four ingestion patterns — this gap will propagate to CDC, API, and Event pipeline definitions unless corrected now. Status "Approved" on both documents means teams may build to this schema.

**Risk:** Critical. Datasets will reach production with no machine-readable data classification, making it impossible to automate PII handling, enforce erasure procedures, or apply correct encryption. This is the root cause of C6 (CI validator gap).

**Suggested fix:** Update both design documents to include the missing fields in the YAML schema (with examples). Add `data_classification` as a required field with allowed values `public|internal|confidential|restricted_pii`. Make the documents' status conditional on the security doc's finalisation. As these documents are already "Approved," a formal revision cycle is required.

---

### C5 — CI config validator does not check mandatory security fields

**Location:** `2026-04-15-deployment-cicd.md` Section 3.5 (`ci/validate_config.py`) vs `2026-04-15-security-data-privacy.md` Section 2.3

**Issue:** The CI validation script checks for: `dataset_name`, `domain`, `source_bucket`, `key_fields`, `schema_id`, `partition_keys`, `dq_rules_path`. It does not validate `data_classification`, `pii_fields`, or `gdpr_erasure_strategy`. A YAML config file without a `data_classification` field passes CI today. There is no gate preventing a non-compliant config from being deployed to production.

**Risk:** Critical. PII datasets with no classification, no erasure strategy, and no PII field inventory can be deployed to production. This is not a theoretical gap — it is an active path to production for non-compliant configs.

**Suggested fix:** Add validation to `ci/validate_config.py`:
1. `data_classification` must be present and must be one of the allowed values.
2. If `data_classification` is `confidential` or `restricted_pii`, then `pii_fields` must be a non-empty list.
3. If `data_classification` is `restricted_pii`, then `gdpr_erasure_strategy` must be present and must be one of `crypto_shredding|pseudonymisation|short_retention`.
4. Block PR merge (not just warn) on validation failure.

---

### C6 — Crypto-shredding entity key storage: Secrets Manager (implementation plan) vs PostgreSQL (security doc)

**Location:** `2026-04-15-implementation-plan.md` Phase 5 vs `2026-04-15-security-data-privacy.md` Section 3.2

**Issue:** The implementation plan Phase 5 describes the GDPR erasure implementation as: "Crypto-shredding: `ods/gdpr/entity-keys/{entity_id}` in Secrets Manager." The security doc Section 3.2 describes the key management architecture using a PostgreSQL table `privacy.entity_keys` (key ID, entity type, entity ID, KMS ARN, created/deleted timestamps) with actual key material in KMS. These represent two different architectures: Secrets Manager stores a path to a KMS-wrapped key; the security doc stores the key lineage in PostgreSQL. This ambiguity means the Terraform and application code could be built to different designs by different teams.

**Risk:** Critical. A split implementation where some entity keys are tracked in Secrets Manager and others in PostgreSQL will make erasure verification impossible and will likely leave orphaned key material. This must be resolved before Phase 5 engineering begins.

**Suggested fix:** Designate one canonical design: the security doc's PostgreSQL `privacy.entity_keys` table with KMS-held key material is the more robust choice (it supports audit queries, erasure status tracking, and batch erasure). Update the implementation plan to match. If Secrets Manager was chosen for a specific reason (e.g., automatic rotation), document it in the security doc and remove the PostgreSQL approach.

---

### C7 — Flyway DB_PASSWORD exposed as CLI argument

**Location:** `2026-04-15-deployment-cicd.md` Section 9.5

**Issue:** The database migration step passes the database password as a CLI argument: `flyway -password="${DB_PASSWORD}"`. On Linux, process arguments are visible in `/proc/<pid>/cmdline` and in the output of `ps aux` to any user on the same host (including other CI job containers on the same node). The value also appears in shell history and in CI log output if debug logging is enabled. This is a well-known credential exposure vector.

**Risk:** High-Critical. Credentials can be harvested from process listings by co-tenanted workloads or from CI logs. In a shared GitHub Actions runner environment, this is particularly significant.

**Suggested fix:** Use the Flyway environment variable form instead: set `FLYWAY_PASSWORD` as an environment variable (sourced from GitHub Actions secrets or AWS Secrets Manager at runtime). Remove the `-password` CLI flag entirely. Verify that CI log masking is in place for all secret values. Reference: Flyway documentation on configuration via environment variables.

---

## Significant Issues

These issues require resolution before production go-live but do not represent immediate regulatory exposure if addressed in the current sprint cycle.

---

### S1 — Dual incompatible PII classification systems with no mapping

**Location:** `2026-04-15-schema-governance.md` Section 12 (`pipeline.schema_governance` table) vs `2026-04-15-security-data-privacy.md` Section 2.1

**Issue:** The schema governance doc uses a four-level classification stored in `pii_classification VARCHAR`: `none | low | medium | high`. The security doc uses a four-tier framework: `Tier 1 Public | Tier 2 Internal | Tier 3 Confidential | Tier 4 Restricted-PII`. These scales do not map to each other (is `medium` equivalent to `Tier 2 Internal` or `Tier 3 Confidential`?). Any tooling that reads `pii_classification` to make access control or erasure decisions will apply the wrong logic depending on which framework it was built against.

**Suggested fix:** Standardise on one classification system across all documents and all database tables. The security doc's Tier 1-4 framework is more precisely defined and already used in the IAM access control matrix. Update `pipeline.schema_governance` to use `data_classification VARCHAR CHECK (data_classification IN ('public','internal','confidential','restricted_pii'))` and add a migration to translate any existing `none/low/medium/high` values.

---

### S2 — Dual PII field tagging conventions in Avro schema

**Location:** `2026-04-15-schema-governance.md` Section 6.4 vs `2026-04-15-security-data-privacy.md` Section 2.2

**Issue:** The schema governance doc tags PII fields using Avro field properties: `"pii": true` and `"pii_category": "contact_info"`. The security doc tags PII fields using the `doc` string field: `"doc": "PII:personal_data"` and `"doc": "PII:special_category"`. These are incompatible annotation mechanisms — code written to parse `pii: true` will not find fields tagged via the `doc` approach, and vice versa. The CI schema linter or any downstream PII discovery tooling will have unpredictable behaviour depending on which convention the dataset author followed.

**Suggested fix:** Standardise on the Avro field property approach (`"pii": true`, `"pii_category"`, `"gdpr_article9": true`) as it is machine-parseable without string matching. Reserve the `doc` field for human-readable descriptions. Update the security doc Section 2.2 to reference the schema governance convention. Add a CI schema validation rule that rejects schemas using the `doc`-based PII tagging.

---

### S3 — `pipeline.lineage` table not included in GDPR erasure scope

**Location:** `2026-04-15-data-lineage.md` Sections 4-6 vs `2026-04-15-security-data-privacy.md` Section 3.2

**Issue:** The `pipeline.lineage` polymorphic table stores: `sftp_filename`, `s3_raw_path`, `api_endpoint`, `api_cursor`, `lsn_position`, and `business_key` — the primary identifier used to link a pipeline run to a specific data subject's records. These fields are sufficient to re-identify a data subject through correlation (business key + source path + timestamp). The GDPR erasure procedure described in the security doc covers Kafka messages and curated database rows. It does not mention `pipeline.lineage`. An erasure request that deletes the data subject's records from all target stores but leaves their `business_key` in `pipeline.lineage` may not fully satisfy Article 17. Additionally, no retention period is defined for the `pipeline.lineage` table in either the data lineage doc or the data retention doc.

**Suggested fix:** (1) Assess with DPO whether `pipeline.lineage.business_key` constitutes personal data under UK GDPR (it likely does for Restricted-PII datasets). (2) Add `pipeline.lineage` to the erasure procedure: on erasure, set `business_key = NULL` or replace with a tombstone token for affected rows. (3) Add a retention policy for `pipeline.lineage` to the data retention doc — a reasonable default is 90 days for completed-run rows, 7 years for error and audit rows.

---

### S4 — Kafka message headers expose internal infrastructure details to all topic consumers

**Location:** `2026-04-15-data-lineage.md` Section 5 (Kafka message headers)

**Issue:** Every Kafka message carries these headers: `x-ods-source-ref` (contains S3 paths like `s3://ods-raw-prod/sftp/aviva-hr/2026-04-14/employees_20260414.csv.enc`, API URLs with cursor tokens like `https://api.internal/v1/policies?cursor=eyJpZCI6MTAwMH0`, and DB+LSN positions like `ods-rds-prod.postgres/public.policies@0/1234567`). Any consumer that is ACL-authorised to read the topic business data also receives this infrastructure metadata. Cursor tokens may embed encoded query state. Internal S3 bucket names, RDS instance names, and internal API hostnames are exposed to all topic consumers, including potential future external or cross-team consumers.

**Suggested fix:** Move lineage metadata out of Kafka message headers and into a separate lineage side-channel (the `pipeline.lineage` table already exists for this purpose). If headers must be retained for operational debugging, restrict them to a hashed run reference (`x-ods-run-id` only) and resolve the full lineage from the lineage table using that ID. This reduces the information available to topic consumers to the minimum necessary.

---

### S5 — DR runbooks lack security controls for replay and erasure-in-flight scenarios

**Location:** `2026-04-15-disaster-recovery.md` Section 9

**Issue:** The DR runbooks describe state reset and replay procedures for pipeline failures and regional failover. None of the runbooks specify: (a) which IAM role is authorised to execute replay state resets (the PostgreSQL SQL uses `current_user` for audit, but the `ods_engineer_ro` role has no DELETE/UPDATE permission — a higher-privilege role is required but not defined); (b) what audit logging must be produced during a DR replay to maintain an immutable audit trail; (c) whether a GDPR erasure request that was in-flight at the time of the incident must be re-processed or whether it was lost; (d) whether the DPO must be notified if PII data is replayed across boundaries during a DR event.

**Suggested fix:** Add a "Security Controls During DR" section to the DR doc covering: authorised role for state resets (define `ods_dr_operator` role in IAM and PostgreSQL), mandatory CloudTrail and pgaudit logging during DR execution, erasure-in-flight handling procedure (check `privacy.entity_keys.deleted_at` before replaying — skip erased entities), and DPO notification trigger criteria.

---

### S6 — Cross-region S3 replication GDPR implications unaddressed

**Location:** `2026-04-15-disaster-recovery.md` Section 8.3

**Issue:** The DR doc recommends S3 Cross-Region Replication (CRR) for `ods-raw-{env}` and `ods-curated-{env}`. These buckets contain PII (Restricted-PII classification). Replicating to another AWS region transfers personal data to a different geographic jurisdiction. UK GDPR Chapter V restricts transfers of personal data to third countries or international organisations. Whether the DR region is within the UK/EEA, or whether an adequacy decision applies (e.g., if the DR region is in Ireland or Frankfurt vs. a US region), determines whether this transfer is lawful without additional safeguards (SCCs, BCRs). None of this is addressed in the DR doc or security doc.

**Suggested fix:** Specify the DR region explicitly and confirm it is within the UK/EEA or that an adequacy decision applies. If the DR region is outside the UK/EEA, document the transfer mechanism (SCCs or equivalent) and add a DPIA reference. Update the security doc Section 8 (network security) to include cross-region data transfer as part of the threat model. This is a legal/compliance item that must be confirmed by the DPO.

---

### S7 — No SAST, SCA, or secrets detection in CI/CD pipeline

**Location:** `2026-04-15-deployment-cicd.md` Section 3

**Issue:** The CI pipeline described in Section 3 includes linting, unit tests, config validation, and schema validation. It does not include: static application security testing (SAST) to detect injection vulnerabilities and insecure coding patterns; software composition analysis (SCA) to detect known-vulnerable dependencies; or secrets detection (e.g., Gitleaks, TruffleHog) to catch credentials committed to the repository. For a platform handling Restricted-PII data for a regulated financial firm, these are baseline security controls. The security doc Appendix A does not address CI/CD security scanning.

**Suggested fix:** Add three pipeline stages:
1. **SAST:** Semgrep with the `p/owasp-top-ten` and `p/python` rulesets on all Python source (Glue jobs, Airflow DAGs, Lambda functions, CI scripts).
2. **SCA:** Trivy filesystem scan or pip-audit on `requirements.txt` / `pyproject.toml`, failing the build on CRITICAL or HIGH CVEs.
3. **Secrets detection:** Gitleaks on full git history (`--no-git` for PR commits, full scan on main branch). Block merge if any secrets are detected.
Stage 7 (security scan) should be a required status check before PR merge to main.

---

### S8 — RDS encryption key type not specified in DR doc

**Location:** `2026-04-15-disaster-recovery.md` Section 4.1

**Issue:** The DR doc specifies "Encryption at rest: aws:rds KMS key" for the PostgreSQL RDS instance. The prefix `aws:rds` typically refers to the AWS-managed default RDS encryption key (`aws/rds`), not a customer-managed key. The security doc mandates customer-managed KMS keys for all data stores holding Confidential or Restricted-PII data. If the RDS instance uses the `aws/rds` managed key, the IAM key-usage policies in the security doc are inoperative for that key (AWS-managed keys cannot have resource-based policies controlled by the customer).

**Suggested fix:** Update the DR doc Section 4.1 to specify the customer-managed KMS key ARN pattern (e.g., `arn:aws:kms:{region}:{account}:alias/ods-rds-{env}`) consistent with the security doc's key-per-purpose strategy. Verify in Terraform that the RDS resource uses `kms_key_id` pointing to a CMK, not the default.

---

## Minor Issues / Improvements

---

### M1 — Audit topic schema leaks internal SFTP hostname

**Location:** `2026-04-14-ingestion-design.md` Section 8

**Issue:** The audit Kafka topic schema example includes `"sftp_host": "internal-sftp.company.com"`. This field exposes the internal SFTP server hostname to all consumers of the audit topic. The security doc does not mandate consumer ACLs on the audit topic, meaning any MSK principal could read this.

**Suggested fix:** Remove `sftp_host` from the audit event schema or replace with an opaque source identifier (e.g., a UUID registered in a source registry). Infrastructure hostnames should not be distributed in data events.

---

### M2 — `pipeline.glue_job_log` error_detail column has no PII controls

**Location:** `2026-04-14-ingestion-design.md` DDL section

**Issue:** The `error_detail TEXT` column in `pipeline.glue_job_log` stores Glue job error messages. Both the ingestion design and security docs flag the risk of PII leaking into error messages (e.g., a failed CSV row with a name or NI number in the error text). No mitigating control is implemented in the DDL — no scrubbing function, no access restriction, no retention limit on this column.

**Suggested fix:** Add a column-level comment flagging PII risk. Restrict SELECT on `error_detail` to `ods_engineer_rw` (not `ods_analyst_ro`). Implement a scrubbing step in the Glue error handler that replaces field values matching PII patterns (NI number regex, email regex, date-of-birth pattern) with `[REDACTED]` before writing to `error_detail`. Enforce a 90-day row retention on `pipeline.glue_job_log`.

---

### M3 — KMS policy wildcard resource in Glue Publish role

**Location:** `2026-04-15-security-data-privacy.md` Section 5.4

**Issue:** The Glue Publish role IAM policy `KMSForPII` statement uses `"Resource": "arn:aws:kms:{region}:{account}:key/*"` with a `StringLike` condition on the KMS key alias. The wildcard with a condition is functionally correct but is a wider resource scope than necessary. A misconfigured key alias pattern in the condition would grant access to all KMS keys in the account. Per the security doc's own least-privilege principle, the resource should enumerate the specific key ARNs the Glue Publish role requires.

**Suggested fix:** Replace the wildcard resource with the explicit key ARN(s) for the S3 Curated and Kafka topic encryption keys. Maintain the condition as a defence-in-depth layer. This is straightforward to implement in Terraform using a `data.aws_kms_key` data source.

---

### M4 — Terraform state file security not addressed

**Location:** `2026-04-15-deployment-cicd.md` Section 8.2

**Issue:** Terraform state is stored in S3 with DynamoDB locking, but there is no discussion of state file encryption, access controls on the state bucket, or the risk that sensitive Terraform outputs (RDS passwords, KMS key IDs, IAM role ARNs) may be written to state in plaintext. Terraform state is a known high-value target and must be treated as a secrets store.

**Suggested fix:** Ensure the Terraform state S3 bucket uses SSE-KMS with a dedicated CMK. Restrict bucket access to CI/CD roles only (deny `s3:GetObject` for all other principals). Enable S3 versioning and MFA Delete on the state bucket. Use `sensitive = true` on all Terraform output values that contain secrets. Consider using `terraform state` access auditing via CloudTrail.

---

### M5 — DPIA not referenced in implementation plan production gate

**Location:** `2026-04-15-implementation-plan.md` Production Promotion Checklist

**Issue:** The production promotion checklist includes "GDPR erasure strategy confirmed per dataset" but does not include a Data Protection Impact Assessment (DPIA) completion gate. UK GDPR Article 35 requires a DPIA prior to processing that is "likely to result in a high risk to the rights and freedoms of natural persons." A platform ingesting HR data, policy data, and special-category health data for a regulated insurer almost certainly triggers this requirement. The security doc Section 1 mentions DPIA as a requirement but it is not in the deployment gate.

**Suggested fix:** Add "DPIA completed and DPO sign-off received" as a blocking gate in the production promotion checklist. The DPIA should reference the security doc's data classification framework and the GDPR erasure strategy decision (Open Decision 3).

---

### M6 — Open Decision 2 (MSK auth mechanism) blocks all MSK IAM policy design

**Location:** `2026-04-15-security-data-privacy.md` Open Decision 2

**Issue:** The MSK authentication mechanism (SASL/SCRAM vs IAM vs mTLS) is marked as a Blocker with no target decision date. The choice materially affects how MSK ACLs are defined, how Kafka producer/consumer credentials are managed, and whether the Secrets Manager integration described in the security doc is needed at all. IAM auth does not use Secrets Manager; SCRAM does. Until this is decided, the MSK IAM policy design in Section 5.5 may need to be rewritten.

**Suggested fix:** Convene a decision within the current sprint. For a managed AWS service with VPC-private connectivity only, IAM authentication (MSK IAM auth) is the recommended approach — it eliminates credential rotation overhead and integrates with the existing IAM least-privilege framework. Set a decision date and update the security doc.

---

## Cross-Document Linking Gaps

The following gaps exist where documents reference concepts from other documents without formal cross-reference, creating a risk that changes in one document will not be propagated to the others.

| Gap | Doc A | Doc B | Missing link |
|-----|-------|-------|--------------|
| Encryption standard | `architecture-decisions.md` §5.1 | `security-data-privacy.md` §4.1 | ADR does not reference security doc; security doc does not reference ADR |
| YAML config schema | `ingestion-design.md` §9 | `security-data-privacy.md` §2.3 | Neither doc references the other for config schema fields |
| PII tagging convention | `schema-governance.md` §6.4 | `security-data-privacy.md` §2.2 | Inconsistent convention with no cross-reference |
| Retention figures | `data-retention.md` §3.2 | `security-data-privacy.md` §9.1 | Audit sink retention stated independently in both docs |
| Entity key storage | `implementation-plan.md` Phase 5 | `security-data-privacy.md` §3.2 | Conflicting architecture described in isolation |
| Erasure scope | `data-lineage.md` §4-6 | `security-data-privacy.md` §3.2 | `pipeline.lineage` not referenced in erasure procedure |
| DR security controls | `disaster-recovery.md` §9 | `security-data-privacy.md` §8 | DR runbooks have no pointer to security requirements |
| CI validation | `deployment-cicd.md` §3.5 | `security-data-privacy.md` §2.3 | CI validator built without reference to security field requirements |
| DPIA requirement | `security-data-privacy.md` §1 | `implementation-plan.md` checklist | DPIA gate absent from production promotion checklist |
| lineage retention | `data-lineage.md` | `data-retention.md` | No retention policy defined for `pipeline.lineage` in either doc |

**Recommendation:** Establish a single cross-reference table in the security doc that lists which sections of each other document must be kept consistent with it, and assign a named owner for each cross-reference. The security doc should be the source of truth for: data classification values, PII tagging convention, retention figures for audit stores, and erasure procedure scope. Other documents should reference it rather than duplicate its content.

---

## Positive Observations

The following aspects of the security design are well-executed and reflect mature security thinking. They should be preserved as the issues above are resolved.

1. **IAM least-privilege with explicit Deny.** The Glue Publish role policy includes an explicit `Deny` on `s3:PutObject` to the raw bucket — this defence-in-depth control correctly prevents a compromised Glue job from writing back to the raw tier. The pattern of one role per component with no shared credentials is the right approach.

2. **KMS key-per-purpose-per-environment strategy.** Having separate CMKs for Raw, Curated, Audit, RDS, and Secrets Manager (per environment) means a key compromise is isolated to one tier and one environment. Key rotation and deletion are independently controllable per tier.

3. **Blocker discipline on open decisions.** Identifying five decisions as production Blockers (MSK auth, GDPR erasure strategy, S3 Object Lock, DPIA, SIEM integration) with clear criteria shows good governance hygiene. The security doc correctly does not paper over these gaps.

4. **VPC endpoint architecture.** Routing all AWS service traffic (S3, KMS, Secrets Manager, MSK) through VPC endpoints eliminates the internet data path for all inter-service communication. The no-public-subnet design for Glue, MWAA, and RDS is the right default.

5. **Audit trail coverage matrix.** The matrix in Section 9 covering event type, source system, destination, and retention is a useful reference and demonstrates that audit trail coverage was designed systematically rather than added ad-hoc.

6. **Avro schema PII field tagging.** Even though the tagging convention needs to be standardised (see S2), the intent to tag PII fields in the schema itself — rather than relying on external documentation — is the correct approach for enabling automated PII discovery and data catalogue integration.

7. **Secrets Manager for all credentials.** The explicit prohibition on hardcoded credentials, environment variables for secrets, and SSM Parameter Store for secrets (in favour of Secrets Manager with automatic rotation) is consistently applied across the design. The Flyway password issue (C7) is an implementation slip against an otherwise sound policy.

8. **Four-tier data classification with clear handling rules.** The Tier 1-4 framework with per-tier encryption, access, and retention rules is well-defined and actionable. It gives engineers a clear decision tree for handling new data sources.
