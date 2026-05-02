# ODS Architecture Decision Pack

## Purpose

This document consolidates the design decisions from the recent lineage, reconciliation, schema, event archive, and non-canonical pipeline discussions.

The goal is to make the next implementation simpler and more deliberate. Before building the non-canonical to canonical pipeline, the product owner and architecture team should agree the core operating model.

## Decisions Needed

### 1. Do we accept two ingestion patterns?

Recommended decision:

```text
Yes.
ODS supports:
1. File based ingestion
2. Message/API based ingestion
```

Why:

```text
files, events, and API pushes do not have the same reconciliation unit
```

### 2. What is the primary business reconciliation key?

Recommended decision:

```text
File based:
  _ods_file_id

Message/API based:
  _ods_source_message_id, _ods_source_event_id, _ods_source_request_id,
  _ods_source_batch_id, or processing window
```

In short:

```text
business reconciliation follows the source correlation key
```

### 3. Are Kafka offsets business reconciliation identity?

Recommended decision:

```text
No.
Kafka offsets are operational metadata.
```

Use offsets for:

```text
connector lag
debugging
bounded replay
consumer progress
Kafka investigation
```

Do not require product users to reason about partition offsets to understand whether a file or message batch reconciled.

### 4. What is the Postgres reconciliation target?

Recommended decision:

```text
Use history/audit tables for source-unit count reconciliation.
Use current tables for latest-state reconciliation.
```

That means:

```text
file/message/batch count -> history/audit table
current table -> latest history/current-state consistency
```

### 5. What happens to complex events that are not stored in Postgres?

Recommended decision:

```text
Store full payloads in S3 raw event archive.
Store failed events in S3 DLQ.
Store searchable metadata/status/pointers in pipeline tables.
Use Athena for archive access.
Create Parquet curated archive only when analytics needs it.
```

### 6. What should hold schemas?

Recommended decision:

```text
AWS Glue Schema Registry is a sensible production target if AWS is strategic.
ODS should keep a provider-neutral schema registry abstraction.
```

Default:

```text
Avro for structured file/canonical topics.
JSON Schema or Protobuf allowed for API/event topics where appropriate.
```

### 7. Can Airflow/OpenLineage replace ODS pipeline tables?

Recommended decision:

```text
No.
Airflow/OpenLineage can enrich orchestration and Spark lineage.
ODS pipeline tables remain the source of business counts, status, reconciliation, and operational process state.
```

## Recommended Operating Model

Adopt a hybrid model:

```text
Business lineage/reconciliation:
  source correlation key + _ods_run_id

Operational observability:
  Kafka offsets, connector lag, Airflow task state, Spark/OpenLineage events

Payload archive:
  S3 for raw complex events and DLQ

Postgres proof:
  history/audit tables for landed counts
  current tables reconciled to latest history
```

## Pattern 1: File Based Ingestion

### Identity

Primary key:

```text
_ods_file_id
```

Supporting metadata:

```text
_ods_run_id
_ods_domain
_ods_dataset
_ods_business_date
_ods_source_application
_ods_ingested_at
```

### Flow

```text
file
  -> S3 raw
  -> curated Parquet
  -> Kafka topic
  -> Postgres history/audit
  -> Postgres current state, where applicable
```

### Reconciliation

```text
source file rows
- DQ failed rows
- canonicalization DLQ rows, if applicable
= published rows
= history/audit rows
```

### Current Table Rule

Do not use current table count as file-level reconciliation.

Current tables represent latest state:

```text
latest row per business key in history/audit
==
current table row per business key
```

## Pattern 2: Message/API Based Ingestion

### Identity

Primary key depends on source:

```text
_ods_source_message_id
_ods_source_event_id
_ods_source_request_id
_ods_source_batch_id
processing window
```

Recommendation:

```text
Use batch/window-level reconciliation for dashboards.
Retain message-level traceability through source message/event ID.
```

### Flow

```text
source message / API request / event batch
  -> Kafka or API landing
  -> optional non-canonical to canonical transform
  -> canonical Kafka topic
  -> Postgres history/audit, if relational storage is required
  -> S3 archive, if payload is complex or not relational
```

### Reconciliation

For request/batch/window:

```text
accepted_count
- validation_fail_count
- canonicalization_dlq_count
= canonical_published_count
= history/audit_count, if stored in Postgres
= archive_count, if archived to S3 only
```

### DLQ Is Not Enough

Topic DLQ is required, but it does not replace reconciliation.

DLQ proves:

```text
some bad messages were captured
```

It does not prove:

```text
all good messages were transformed
all transformed messages were published
no duplicates were produced
consumer lag is within SLA
```

Minimum event reconciliation:

```text
consumed_count - dlq_count = published_count
```

plus:

```text
consumer lag below SLA
```

## Kafka Offsets

Offsets should still be captured per partition.

Example:

```json
{
  "topic": "ods.insurance.policy",
  "partitions": [
    {"partition": 0, "start": 120, "end": 370},
    {"partition": 1, "start": 98, "end": 348}
  ]
}
```

Use offsets for:

```text
connector lag
bounded replay
consumer progress
debugging
investigation
```

Do not use offsets as the main product-facing reconciliation key.

## Pipeline Tables

No existing pipeline tables should be removed.

### Used by file based flows

```text
dataset_config
file_catalogue
file_state
run_log
run_stage_log
run_events
reconciliation_log
lineage_edge
```

### Used by message/API flows

```text
dataset_config
run_log
run_stage_log
run_events
reconciliation_log
lineage_edge
```

Usually not used by message/API flows:

```text
file_catalogue
file_state
```

unless an API/event batch is explicitly materialized as a file-like artifact.

### Merge flows

Still use:

```text
merge_run_log
merge_contribution_log
```

## Schema Registry

Schema Registry governs Kafka message contracts.

ODS control-plane tables govern:

```text
lineage
status
counts
reconciliation
process state
```

Recommended provider model:

```text
schema_registry_provider = aws_glue | confluent | local
schema_format = avro | json_schema | protobuf
schema_id
schema_version
```

Production target:

```text
AWS Glue Schema Registry, if AWS is strategic
```

Default format guidance:

```text
structured file feeds -> Avro
canonical business topics -> Avro
API/event topics -> JSON Schema or Protobuf where appropriate
```

## Event Archive

For complex events not stored in Postgres:

```text
S3 = durable payload archive and replay source
Postgres = searchable operational index and reconciliation state
Kafka = transport and streaming integration layer
Athena = query tool for archived payloads
```

Recommended raw archive:

```text
s3://ods-event-archive/
  domain=<domain>/
  dataset=<dataset>/
  source_application=<app>/
  ingest_date=YYYY-MM-DD/
  hour=HH/
  part-00001.jsonl
```

JSONL is text, not binary, and can be queried through Athena.

Recommended DLQ:

```text
s3://ods-dlq/
  domain=<domain>/
  dataset=<dataset>/
  source_application=<app>/
  ingest_date=YYYY-MM-DD/
  hour=HH/
  part-00001.jsonl
```

Use Parquet curated archive only when analytics performance or typed reporting is needed.

## Airflow And OpenLineage

Airflow gives orchestration lineage:

```text
dag_id
dag_run_id
task_id
task order
task status
parameters passed
```

OpenLineage can add Spark lineage:

```text
Spark job
input datasets
output datasets
schema facets
parent Airflow run
START / COMPLETE / FAIL events
```

These are valuable, but they do not replace ODS reconciliation facts:

```text
source counts
DQ failures
DLQ counts
published counts
Postgres history counts
current-state consistency
business correlation keys
```

Recommended use:

```text
Use Airflow/OpenLineage to enrich lineage skeletons.
Keep ODS pipeline tables as the business reconciliation source of truth.
```

## Non-canonical To Canonical Pipeline

Before building the non-canonical to canonical flow, agree whether the input is:

```text
file based
message/API based
```

For file based non-canonical:

```text
_ods_file_id remains the business key
canonicalization records _ods_canonicalize_run_id
reconciliation is file scoped
```

For message/API based non-canonical:

```text
source message/request/batch/window is the business key
canonicalization records _ods_canonicalize_run_id
reconciliation is message, batch, or window scoped
```

Avoid making Kafka offsets the primary business lineage identity.

## Recommended Implementation Order

### 1. Approve decision pack

Product owner / architecture team confirm:

```text
two ingestion patterns
source correlation key model
history/current reconciliation split
offsets as operational metadata
S3 archive for complex events
schema registry direction
```

### 2. Define metadata contract

Create a formal ODS metadata contract for:

```text
file records
message/API records
canonical records
history/current tables
S3 archive envelopes
DLQ records
```

### 3. Update reconciliation requirements

Document:

```text
file-level reconciliation
message/request/batch/window reconciliation
history table count reconciliation
current table latest-state reconciliation
event archive count reconciliation
```

### 4. Update dashboards

Add views for:

```text
file based flows
message/API based flows
history vs current consistency
DLQ / failed validation
Kafka offsets and lag as operational telemetry
```

### 5. Build non-canonical canonical pipeline

Only after the above decisions are agreed.

### 6. Optional OpenLineage spike

Run one Spark/Glue job with OpenLineage enabled and inspect the emitted events.

Treat OpenLineage as enrichment, not as a replacement for ODS tables.

## Final Recommendation

Proceed with the simpler hybrid model:

```text
Business reconciliation:
  source correlation keys

Operational observability:
  Kafka offsets, lag, Airflow, OpenLineage

Payload retention:
  S3 archive / DLQ for complex events

Postgres proof:
  history/audit tables for counts
  current tables reconciled to latest history state
```

This keeps the design understandable for product owners while preserving enough technical detail for replay, debugging, and audit.

