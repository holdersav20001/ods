# ODS Platform — Documentation Index

**Last updated:** 2026-04-16

---

## What is ODS?

The Aviva Operational Data Store (ODS) is an AWS-based data pipeline platform that ingests data from multiple source systems and publishes it to Apache Kafka (MSK) topics for downstream consumers. It solves the problem of fragmented, point-to-point data feeds by providing a single governed platform where data producers write once and any number of consumer teams subscribe. The primary users are platform engineers who build and operate the pipelines, domain teams who onboard datasets, and consumer teams who read from Kafka topics to power their applications.

---

## Platform Status

| Item | State |
|---|---|
| **Pattern 1 — S3 batch** (SFTP → S3 Raw → Glue ETL → S3 Curated → MSK) | **Designed. Build starting.** |
| **Pattern 2 — CDC → Kafka** | In design. Technology evaluation complete — see D-CDC. |
| **Pattern 3 — API → Kafka** | Not started. |
| **Pattern 4 — Event → Kafka** | Not started. |
| **Shared layer** (Schema Registry, DLQ, PostgreSQL state store, audit topic, CloudWatch) | Designed as part of Pattern 1. Reused by Patterns 2–4. |
| **Current phase** | Phase 0 (Decisions and Prerequisites). See blocking decisions below. |

---

## Blocking Decisions

The following decisions gate the build. They must be resolved by the named owners before the affected phase can start. See `2026-04-15-implementation-plan.md` for full context.

| # | Decision | Gates | Owner | Priority |
|---|---|---|---|---|
| D1 | SFTP → MWAA network connectivity model (VPN / Transit Gateway / PrivateLink) | Phase 3 | Infrastructure + Security | Critical |
| D2 | MSK authentication mode (IAM auth vs SASL/SCRAM) | Phase 2 | Security + Platform | Critical |
| D3 | GDPR erasure strategy per dataset (crypto-shredding vs pseudonymisation) | Phase 5 | DPO + Platform | Critical |
| D4 | Data classification — classify every dataset as Public / Internal / Confidential / Restricted-PII | Phase 5 | Data Governance | Critical |
| D5 | RTO/RPO targets for production | Phase 2 | CTO + Business | High |
| D-CDC | CDC connector technology (MSK Connect + Debezium vs Debezium on ECS) | Pattern 2 build | Platform lead | High |

---

## Document Map

### Group A — Architecture and Pipeline Design

Read these first. Everything else in this set depends on them.

| Document | Description | Status |
|---|---|---|
| [2026-04-14-architecture-decisions.md](2026-04-14-architecture-decisions.md) | Every major design choice with rationale, trade-offs, and alternatives rejected. The "why" document for the entire platform. | Living |
| [2026-04-14-s3-kafka-design.md](2026-04-14-s3-kafka-design.md) | Detailed design of the publish pipeline (S3 Curated Zone → MSK). Serves as the platform template — the shared layer is defined here. | Approved |
| [2026-04-14-ingestion-design.md](2026-04-14-ingestion-design.md) | Detailed design of the ingestion pipeline (SFTP → S3 Raw → Glue ETL → S3 Curated). | Approved |
| [2026-04-16-cdc-technology-evaluation.md](2026-04-16-cdc-technology-evaluation.md) | Evaluation of all CDC connector options (Debezium, DMS, Flink, OpenFlow, Firehose) for Pattern 2. Includes decision matrix and recommendation. Decision D-CDC open. | Decision Required |
| [2026-04-17-pipeline-paths.md](2026-04-17-pipeline-paths.md) | The two ingestion paths (File-based and CDC) with every step mapped to its draw.io diagram. Use this to navigate between diagrams or trace a record end-to-end. | Living |

### Group B — Failure Handling

Each pipeline design document has a companion failure document. Read these alongside Group A.

| Document | Description | Status |
|---|---|---|
| [2026-04-14-s3-kafka-failure-and-recovery.md](2026-04-14-s3-kafka-failure-and-recovery.md) | Failure scenarios, recovery procedures, and DLQ resubmission for the publish pipeline. Includes SQL investigation queries and an operational checklist keyed to alarm names. | Approved |
| [2026-04-14-ingestion-failure-and-recovery.md](2026-04-14-ingestion-failure-and-recovery.md) | Same for the ingestion pipeline. | Approved |

### Group C — Companion Diagrams

These live in the **repository root** (not in `docs/plans/`). They are draw.io files — open in draw.io Desktop or diagrams.net. Each diagram has a navigation bar linking to related diagrams. For a guided walkthrough of how they connect, read `2026-04-17-pipeline-paths.md` first.

#### File-Based Path (SFTP → Parquet → Canonical Kafka → Storage)

| File | Type | Description |
|---|---|---|
| `../../ods-ingestion-overview.drawio` | Overview | SFTP → S3 Raw → Glue ETL → S3 Curated (Parquet) |
| `../../ods-ingestion-sequence.drawio` | Sequence | Same pipeline — step-by-step with participants and handoffs |
| `../../ods-s3-kafka-overview.drawio` | Overview | S3 Curated → Glue Job → canonical MSK topic |
| `../../ods-s3-kafka-sequence.drawio` | Sequence | Same pipeline — schema validation, DQ, publish, post-publish steps |

#### CDC Path (Source DB → non-canonical Kafka → Canonical Kafka → Storage)

| File | Type | Description |
|---|---|---|
| `../../patterns/openflow_extraction.drawio` | Sequence | Source DB → OpenFlow (NiFi) → raw Kafka topic (K1) |
| `../../ods-kafka-canonical-overview.drawio` | Overview | raw Kafka (K1) → ECS Kafka Streams → canonical topic (K2) |
| `../../ods-kafka-canonical-sequence.drawio` | Sequence | Same pipeline — schema fetch, DQ, SCD state, EOS v2 transaction |

#### Convergence (both paths → Storage)

| File | Type | Description |
|---|---|---|
| `../../ods-kafka-sink-overview.drawio` | Overview | Canonical MSK topic → Iceberg (S3) + PostgreSQL |
| `../../ods-kafka-sink-sequence.drawio` | Sequence | Same pipeline — Iceberg write (2a), JDBC write (2b), post-write audit |

### Group D — Consumer and Dataset Operations

For teams using the platform: onboarding new datasets and connecting as Kafka consumers.

| Document | Description | Status |
|---|---|---|
| [2026-04-15-consumer-onboarding.md](2026-04-15-consumer-onboarding.md) | How downstream teams connect to Kafka topics. Includes connection config, Java and Python code examples, schema deserialisation, and a going-live checklist. | Approved |
| [2026-04-15-dataset-onboarding.md](2026-04-15-dataset-onboarding.md) | How platform engineers add a new dataset to the platform. Step-by-step runbook: schema registration, YAML config, DQ rules, infrastructure, catalogue entry. | Approved |

### Group E — Platform Governance and Compliance

Cross-cutting concerns that apply to all four ingestion patterns.

| Document | Description | Status |
|---|---|---|
| [2026-04-15-schema-governance.md](2026-04-15-schema-governance.md) | Schema change classification matrix (compatible vs breaking), registry rules, and approval process. | Draft |
| [2026-04-15-data-retention.md](2026-04-15-data-retention.md) | Retention periods per storage layer, GDPR erasure approach, crypto-shredding design. Requires DPO sign-off. | Draft |
| [2026-04-15-security-data-privacy.md](2026-04-15-security-data-privacy.md) | IAM, encryption, PII handling, data classification tiers, and GDPR controls. | Draft |
| [2026-04-15-data-lineage.md](2026-04-15-data-lineage.md) | Lineage model, tracing a record from source to Kafka, SQL appendix. | Draft |

### Group F — Operations and Delivery

Implementation execution, monitoring, disaster recovery, and testing.

| Document | Description | Status |
|---|---|---|
| [2026-04-15-implementation-plan.md](2026-04-15-implementation-plan.md) | Phased delivery roadmap (10 phases), blocking decisions table, testing gates per phase. | Draft |
| [2026-04-15-deployment-cicd.md](2026-04-15-deployment-cicd.md) | CI/CD pipeline design, artifact promotion (dev → staging → prod), rollback procedures per artifact type. | Approved |
| [2026-04-15-observability.md](2026-04-15-observability.md) | CloudWatch metrics, alarms, log groups, and dashboard design. Maps alarm names to pipeline failure modes. | Approved |
| [2026-04-15-reconciliation-design.md](2026-04-15-reconciliation-design.md) | T0–T3 latency tiers for completeness and correctness checking. Count reconciliation approach. | Draft |
| [2026-04-15-disaster-recovery.md](2026-04-15-disaster-recovery.md) | DR strategy options, RTO/RPO targets (pending sign-off), recovery procedures per component. | Draft |
| [2026-04-15-testing-strategy.md](2026-04-15-testing-strategy.md) | Test levels (unit, integration, contract, DQ, idempotency, load), tooling, and test data management. | Approved |

---

## Recommended Reading Order

### New platform engineer

Start here before touching any code or infrastructure.

1. [2026-04-14-architecture-decisions.md](2026-04-14-architecture-decisions.md) — understand the "why" before the "what"
2. [2026-04-17-pipeline-paths.md](2026-04-17-pipeline-paths.md) — the two ingestion paths end-to-end with diagram references; read before opening any draw.io file
3. [2026-04-14-s3-kafka-design.md](2026-04-14-s3-kafka-design.md) — the file-based publish pipeline in detail; open `../../ods-s3-kafka-overview.drawio` alongside
4. [2026-04-14-ingestion-design.md](2026-04-14-ingestion-design.md) — the ingestion pipeline in detail; open `../../ods-ingestion-overview.drawio` alongside
5. [2026-04-14-s3-kafka-failure-and-recovery.md](2026-04-14-s3-kafka-failure-and-recovery.md) and [2026-04-14-ingestion-failure-and-recovery.md](2026-04-14-ingestion-failure-and-recovery.md) — what breaks and how to recover
6. [2026-04-15-observability.md](2026-04-15-observability.md) — how to watch the platform
7. [2026-04-15-schema-governance.md](2026-04-15-schema-governance.md) — the contract layer
8. [2026-04-15-data-lineage.md](2026-04-15-data-lineage.md) — tracing data through the system
9. [2026-04-15-dataset-onboarding.md](2026-04-15-dataset-onboarding.md) — how to add a new dataset
10. [2026-04-15-consumer-onboarding.md](2026-04-15-consumer-onboarding.md) — how downstream teams connect
11. [2026-04-15-implementation-plan.md](2026-04-15-implementation-plan.md) — where things stand and what is next

### Consumer team connecting to Kafka topics

You do not need to understand the full platform internals. Read these four documents.

1. [2026-04-14-architecture-decisions.md](2026-04-14-architecture-decisions.md) — skim Sections 1.1–1.5 for platform context
2. [2026-04-15-consumer-onboarding.md](2026-04-15-consumer-onboarding.md) — your primary guide: connection, authentication, schema deserialisation, going-live checklist
3. [2026-04-15-schema-governance.md](2026-04-15-schema-governance.md) — understand schema evolution so you know what changes to expect
4. [2026-04-15-data-lineage.md](2026-04-15-data-lineage.md) — understand Kafka message headers and how to trace a record to its source

### SRE joining on-call

Focus on failure modes, observability, and recovery. Read these five documents.

1. [2026-04-14-architecture-decisions.md](2026-04-14-architecture-decisions.md) — skim Sections 1–2 for platform architecture overview; read Section 4 (SRE considerations) in full
2. [2026-04-14-s3-kafka-failure-and-recovery.md](2026-04-14-s3-kafka-failure-and-recovery.md) — publish pipeline failure runbook (alarm name index at the end)
3. [2026-04-14-ingestion-failure-and-recovery.md](2026-04-14-ingestion-failure-and-recovery.md) — ingestion pipeline failure runbook
4. [2026-04-15-observability.md](2026-04-15-observability.md) — alarms, metrics, dashboards
5. [2026-04-15-disaster-recovery.md](2026-04-15-disaster-recovery.md) — infrastructure-level recovery; complements the failure-and-recovery docs above

---

## Glossary

All platform terminology is defined in [2026-04-16-glossary.md](2026-04-16-glossary.md). Consult it if you encounter an unfamiliar acronym or see a term used in two different ways across documents.
