# Event Archive In S3 And Athena

## Executive Summary

For complex message/API events that are not suitable for direct storage in Postgres, S3 should be the durable archive.

Recommended pattern:

```text
complex event
  -> Kafka / processing path
  -> immutable S3 raw archive
  -> optional S3 curated Parquet layer for analytics
```

Postgres should store searchable metadata, status, reconciliation, and pointers to the archived event payload. S3 should store the full raw payload.

## Why S3 For Complex Events

Some event payloads are too nested, variable, or large to model cleanly in relational tables.

S3 is a good default archive because it provides:

```text
cheap retention
durable storage
immutability options
partitioned layout
replay source
audit evidence
Athena / Glue queryability
decoupling from Postgres table design
```

Postgres remains useful for:

```text
event index / metadata
processing status
counts and reconciliation
error summaries
S3 archive locations
```

## Recommended Storage Layers

### Raw event archive

Purpose:

```text
audit
replay
evidence of exactly what was received
```

Recommended format:

```text
JSONL
```

or, where schema/binary storage is preferred:

```text
Avro object container files
```

JSONL is not binary. It is UTF-8 text with one JSON object per line.

Example:

```jsonl
{"_ods_source_message_id":"msg-001","_ods_source_application":"claims-app","payload":{"claimId":"C1","amount":100}}
{"_ods_source_message_id":"msg-002","_ods_source_application":"claims-app","payload":{"claimId":"C2","amount":250}}
```

### Curated analytics archive

Purpose:

```text
analytics
Athena performance
flattened/typed reporting
```

Recommended format:

```text
Parquet
```

This can be created later from the raw archive if needed.

## Suggested S3 Layout

Raw archive:

```text
s3://ods-event-archive/
  domain=insurance/
  dataset=claims_event/
  source_application=claims-app/
  ingest_date=2026-05-01/
  hour=10/
  part-00001.jsonl
```

DLQ archive:

```text
s3://ods-dlq/
  domain=insurance/
  dataset=claims_event/
  source_application=claims-app/
  ingest_date=2026-05-01/
  hour=10/
  part-00001.jsonl
```

Curated Parquet:

```text
s3://ods-event-curated/
  domain=insurance/
  dataset=claims_event/
  ingest_date=2026-05-01/
  hour=10/
  part-00001.parquet
```

## Raw Event Envelope

Each archived event should include an ODS envelope plus the original payload.

Example:

```json
{
  "_ods_source_message_id": "msg-123",
  "_ods_source_event_id": "evt-456",
  "_ods_source_request_id": "req-789",
  "_ods_source_batch_id": "batch-001",
  "_ods_source_application": "claims-app",
  "_ods_domain": "insurance",
  "_ods_dataset": "claims_event",
  "_ods_ingested_at": "2026-05-01T10:15:00Z",
  "_ods_schema_id": "ods.insurance.claim-event",
  "_ods_schema_version": 3,
  "_ods_run_id": "8f8b8c88-a9e0-4f41-a46c-b2b18446991b",
  "payload": {
    "claimId": "C123",
    "status": "OPEN",
    "nested": {
      "example": true
    }
  }
}
```

Not every source needs all source IDs. The rule is:

```text
provide at least one stable source correlation key
provide a batch/request key when multiple records are submitted together
```

## Athena Access

JSONL files in S3 are queryable through Amazon Athena.

Athena can query JSON-encoded data where each record is on a separate line. Each JSON object should be a single line.

Example external table:

```sql
CREATE EXTERNAL TABLE ods_event_archive (
  `_ods_source_message_id` string,
  `_ods_source_event_id` string,
  `_ods_source_request_id` string,
  `_ods_source_batch_id` string,
  `_ods_source_application` string,
  `_ods_ingested_at` string,
  `_ods_schema_id` string,
  `_ods_schema_version` int,
  `_ods_run_id` string,
  `payload` string
)
PARTITIONED BY (
  domain string,
  dataset string,
  source_application string,
  ingest_date string,
  hour string
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION 's3://ods-event-archive/';
```

Example query:

```sql
SELECT
  _ods_source_message_id,
  _ods_source_application,
  _ods_ingested_at,
  _ods_run_id
FROM ods_event_archive
WHERE domain = 'insurance'
  AND dataset = 'claims_event'
  AND source_application = 'claims-app'
  AND ingest_date = '2026-05-01';
```

For nested payload queries, Athena can extract fields from JSON. For frequent analytics, prefer a curated Parquet layer.

## What Goes In Postgres

Even if the full payload is not stored in Postgres, keep a searchable index/status record.

This can initially live in existing pipeline tables:

```text
run_log
run_stage_log
reconciliation_log
lineage_edge
```

For high-volume event streams, a dedicated event index table may be useful later.

Minimum searchable metadata:

```text
_ods_source_message_id
_ods_source_event_id
_ods_source_request_id
_ods_source_batch_id
_ods_source_application
domain
dataset
schema_id
schema_version
ingested_at
archive_s3_uri
processing_status
run_id / batch_id / window_id
error_summary
```

This allows the platform to answer:

```text
Did we receive message X?
Where is the full payload?
Was it processed?
Did it fail?
Can we replay it?
```

## DLQ Pattern

Bad events should go to S3 DLQ.

The DLQ record should include:

```text
original payload
failure reason
failed stage
run_id
source message/event/request/batch ID
schema ID and version
ingested timestamp
```

Example DLQ envelope:

```json
{
  "_ods_source_message_id": "msg-123",
  "_ods_run_id": "8f8b8c88-a9e0-4f41-a46c-b2b18446991b",
  "_ods_failed_stage": "schema_validate",
  "_ods_error_summary": "required field claimId missing",
  "_ods_failed_at": "2026-05-01T10:16:00Z",
  "payload": {
    "status": "OPEN"
  }
}
```

## Reconciliation

For message/API flows, reconciliation should normally be by:

```text
source batch
request
processing window
```

rather than by every individual event.

Example:

```text
accepted_count
- validation_fail_count
- canonicalization_dlq_count
= published_count
= archive_count
```

If events are not stored in Postgres target tables, then the archive count becomes part of the proof:

```text
accepted_count
= S3 raw archive count
```

For processed/canonicalized events:

```text
accepted_count
- DLQ count
= canonical published count
```

## How Events Get Archived

Options:

### Kafka Connect S3 Sink

Use when events are already in Kafka.

Flow:

```text
Kafka topic
  -> S3 sink connector
  -> JSONL / Avro / Parquet archive
```

Benefits:

```text
standard connector pattern
decoupled from application code
continuous archive
```

### ODS archiver job / consumer

Use when custom envelopes, routing, or error handling are needed.

Flow:

```text
Kafka/API event
  -> ODS archiver
  -> S3 archive
  -> pipeline status/reconciliation
```

Benefits:

```text
full control over envelope
custom partitioning
custom metadata/index writes
```

### API landing writes archive directly

Use when API gateway/landing service receives events before Kafka.

Flow:

```text
API request
  -> write S3 raw archive
  -> publish/process event
```

Benefits:

```text
archive exactly what was received
strong audit trail before downstream processing
```

## Recommended Direction

For complex events not stored in Postgres:

```text
1. Store full raw payload in S3 JSONL archive.
2. Store failed events in S3 DLQ with failure details.
3. Store searchable metadata/status/pointers in pipeline tables.
4. Use Athena for ad hoc access to raw JSONL.
5. Create curated Parquet when analytics performance or typed reporting is needed.
```

Recommended mental model:

```text
S3 = durable payload archive and replay source
Postgres = searchable operational index and reconciliation state
Kafka = transport and streaming integration layer
Athena = query tool for archived payloads
```

