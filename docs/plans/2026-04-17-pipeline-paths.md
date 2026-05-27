# ODS Platform — Pipeline Paths and Diagram Navigation

**Last updated:** 2026-05-21

This document describes the two ingestion paths supported by the ODS platform and maps each step to its draw.io diagram. Use it to navigate between diagrams or to trace a record from source to its final storage destination.

---

## Path 1 — File-Based (SFTP → S3 → Kafka → PostgreSQL / Iceberg)

Data arrives as a structured file over SFTP. It is validated, transferred to S3 Raw, then a Glue Spark job reads, validates, DQ-checks, and writes Parquet to S3 Curated. A second Glue job publishes records as Avro to a raw Kafka topic. An optional canonicalization step transforms raw → canonical Avro. Kafka Sink connectors deliver to PostgreSQL (upsert current + append history) and Iceberg. Airflow is the sole scheduler — no EventBridge.

```
SFTP Source
  │
  ▼
dag_drop_to_raw — detect, validate, transfer to S3 Raw
  │  writes: pipeline.file_catalogue (file_id, state=registered)
  ▼
dag_ingest — stage_ingest (Glue Spark / ods_ingestion.py)
  │  RAW_READ → SCHEMA_VALIDATE → DQ_CHECK → CURATED_WRITE
  │  writes: run_log, stage_log, lineage, reconciliation_log (t0_input_count)
  │  DQ failures → s3://ods-dlq-{env}/
  ▼
dag_ingest — stage_publish (Glue / ods_s3_publish.py)
  │  reads s3://ods-curated-{env}/ · produces Avro via OffsetTracker
  │  output: {domain}.{dataset}.raw  ← raw Kafka topic
  │  writes: publish_stage, reconciliation_log (t0_publish_count)
  ▼
dag_ingest — stage_canonicalize (optional — is_canonical?)
  │  output: {domain}.{dataset}  ← canonical Avro topic
  ▼
Kafka Sink Connector — consume canonical topic
  │
  ├──► PostgreSQL (ods.*)   — upsert current + append history
  └──► Iceberg (S3)         — analytical / historical store
  │
  ▼
dag_ingest — finalise
  │  writes: reconciliation_log (dual_sink_parity), run_log (completed)
```

### Draw.io diagrams for this path

| Diagram | Pages | Location |
|---------|-------|----------|
| **file-ingestion-route.drawio** — Process, Data Flow, Sequence | 3 pages covering full path end-to-end | `docs/dev-guides/file-ingestion-route.drawio` |
| **reconciliation-patterns-file-and-message.drawio** — Recon checkpoints + check-type reference | 2 pages | `docs/reconciliation-patterns-file-and-message.drawio` |
| **pipeline-table-population-guide.drawio** — Control table write order + schema quick-ref | 2 pages | `docs/pipeline-table-population-guide.drawio` |

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
                              {domain}.{dataset}  (canonical Avro)
                                         │
Path 2 (CDC)  ──────────────────────────┘
                                         ▼
                              Kafka Sink Connector
                                         │
                              ├──► PostgreSQL ods.*   (JDBC upsert)
                              └──► Iceberg (S3)       (append)
```

See `docs/dev-guides/file-ingestion-route.drawio` (Data Flow page) for the full visual.

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
