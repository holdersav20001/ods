# ODS Platform — Pipeline Paths and Diagram Navigation

**Last updated:** 2026-04-17

This document describes the two ingestion paths supported by the ODS platform and maps each step to its draw.io diagram. Use it to navigate between diagrams or to trace a record from source to its final storage destination.

---

## Path 1 — File-Based (SFTP → Parquet → Canonical Kafka → PostgreSQL / Iceberg)

Data arrives as a structured file over SFTP. It is validated, transferred to S3, transformed to Parquet, schema-validated against the canonical Avro schema, and published directly to the canonical Kafka topic. There is no non-canonical Kafka staging hop.

```
SFTP Source
  │
  ▼
DAG1 — Detect, validate, transfer to S3 Raw
  │
  ▼
DAG2 — Glue ETL → Parquet written to S3 Curated
  │  handoff: EventBridge S3 Object Created rule fires (ods-curated-file-rule)
  ▼
Airflow DAG — idempotency check → trigger Glue publish job
  │
  ▼
Glue Job — read Parquet · fetch canonical schema · run DQ · publish Avro
  │  output: ods.{domain}.{dataset}  ← canonical topic (no raw Kafka hop)
  ▼
Kafka Sink Connector — consume canonical topic
  │
  ├──► Iceberg (S3)        — analytical / historical store
  └──► PostgreSQL (ods.*)  — operational query layer
```

### Draw.io diagrams for this path

| Step | Overview diagram | Sequence diagram |
|------|-----------------|-----------------|
| SFTP → S3 Raw → S3 Curated (Parquet) | `ods-ingestion-overview.drawio` | `ods-ingestion-sequence.drawio` |
| S3 Curated → Canonical Kafka topic | `ods-s3-kafka-overview.drawio` | `ods-s3-kafka-sequence.drawio` |
| Canonical Kafka → Iceberg + PostgreSQL | `ods-kafka-sink-overview.drawio` | `ods-kafka-sink-sequence.drawio` |

**Handoff between diagrams:** `ods-ingestion-sequence` → `ods-s3-kafka-sequence` is triggered by the EventBridge `ods-curated-file-rule` firing when Parquet lands in S3 Curated.

---

## Path 2 — CDC (Source DB → non-canonical Kafka → Canonical Kafka → PostgreSQL / Iceberg)

Data arrives as a continuous stream of database change events (INSERT / UPDATE / DELETE) via WAL replication. Events are captured into a raw non-canonical Kafka topic first, then an ECS Kafka Streams service reads those events, maps them to the canonical schema, and produces to the canonical topic using EOS v2 exactly-once transactions.

```
Source DB (Postgres / Oracle)
  │  WAL / XStream replication slot
  ▼
OpenFlow (NiFi) — initial snapshot + incremental CDC
  │  output: ods.raw.{domain}.{dataset}  ← non-canonical (JSON CDC envelope, 24h retention)
  ▼
ECS Kafka Streams — ods-kafka-canonicalize-{dataset}
  │  reads raw topic · maps fields (YAML config) · validates schema · DQ check · SCD state (RocksDB)
  │  output: ods.{domain}.{dataset}  ← canonical Avro (EOS v2 transaction)
  ▼
Kafka Sink Connector — consume canonical topic
  │
  ├──► Iceberg (S3)        — analytical / historical store
  └──► PostgreSQL (ods.*)  — operational query layer
```

### Draw.io diagrams for this path

| Step | Overview diagram | Sequence diagram |
|------|-----------------|-----------------|
| Source DB → non-canonical Kafka (K1) | — | `patterns/openflow_extraction.drawio` |
| non-canonical Kafka → Canonical Kafka (K2) | `ods-kafka-canonical-overview.drawio` | `ods-kafka-canonical-sequence.drawio` |
| Canonical Kafka → Iceberg + PostgreSQL | `ods-kafka-sink-overview.drawio` | `ods-kafka-sink-sequence.drawio` |

**Handoff between diagrams:** `openflow_extraction` produces to `ods.raw.{domain}.{dataset}`. The ECS Kafka Streams service (consumer group `ods-cdc-canonicalize-{dataset}`) polls that topic continuously — there is no discrete event trigger; it is a streaming handoff.

---

## Where the paths converge

Both paths write to the same canonical topic `ods.{domain}.{dataset}`. From that point on the pipeline is identical. The Kafka Sink connector does not know or care which ingestion path produced a record.

```
Path 1 (File) ──────────────────────────┐
                                         ▼
                              ods.{domain}.{dataset}  (canonical Avro)
                                         │
Path 2 (CDC)  ──────────────────────────┘
                                         ▼
                              ods-kafka-sink-overview.drawio
                              ods-kafka-sink-sequence.drawio
                                         │
                              ├──► Iceberg (S3)
                              └──► PostgreSQL (ods.*)
```

---

## Key Kafka topics per path

| Topic | Path | Format | Retention | Schema Registry |
|-------|------|--------|-----------|-----------------|
| `ods.raw.{domain}.{dataset}` | CDC only | JSON CDC envelope | 24 h (delete) | No |
| `ods.raw.{dataset}.changelog` | CDC only | Kafka Streams state | Indefinite (compact) | No |
| `ods.raw.{dataset}.quarantine` | CDC only | Original record | 30 days (delete) | No |
| `ods.{domain}.{dataset}` | Both | Canonical Avro | Per data classification | Yes |
| `ods.pipeline.audit` | Both | Audit event | 90 days | No |

Full topic catalogue: `docs/plans/2026-04-16-kafka-topic-catalogue.md`

---

## Failure routing per path

| Failure point | Path | Destination |
|---------------|------|-------------|
| Schema incompatible (Glue publish) | File | `ods-dlq-{env}` S3 DLQ |
| DQ hard block (Glue publish) | File | `ods-dlq-{env}` S3 DLQ |
| Count mismatch after publish | File | `ods-dlq-{env}` S3 DLQ |
| Schema incompatible (ECS canonicalise) | CDC | `ods-dlq-{env}` S3 DLQ |
| DQ hard block (ECS canonicalise) | CDC | `ods.raw.{dataset}.quarantine` (replayable) |
| Unrecoverable ECS failure | CDC | `ods-dlq-{env}` S3 DLQ |
| Iceberg write failure | Both | Kafka offset not committed — retry on restart |
| JDBC write failure | Both | Kafka offset not committed — retry on restart |

Failure runbooks: `docs/plans/2026-04-14-s3-kafka-failure-and-recovery.md`
