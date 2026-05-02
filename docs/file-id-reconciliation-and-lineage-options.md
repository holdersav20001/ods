# File And Message Based Reconciliation And Lineage Options

## Executive Summary

The ODS has two ingestion patterns:

```text
1. File based
2. Message based
```

This changes the framing of the design, but not the main recommendation.

Recommended principle:

```text
Business lineage should be driven by a stable source correlation key.
Kafka offsets should be operational metadata.
```

For file based ingestion, the source correlation key is:

```text
_ods_file_id
```

For message based ingestion, the source correlation key is one of:

```text
_ods_source_message_id
_ods_source_event_id
_ods_source_request_id
_ods_source_batch_id
```

If every Kafka record and every Postgres row carries the appropriate source correlation key and `_ods_run_id`, the product and operations model becomes simpler:

```text
What happened to this file?
  -> find rows by _ods_file_id
  -> join to run_log, run_stage_log, reconciliation_log, lineage_edge
  -> count rows in history/audit/sink tables by _ods_file_id

What happened to this message or API batch?
  -> find rows by source message/event/request/batch ID
  -> join to run_log, run_stage_log, reconciliation_log, lineage_edge
  -> count rows in history/audit/sink tables by the same key
```

Offsets remain useful for debugging, lag monitoring, sink progress, and replay optimization, but they do not need to be the primary business reconciliation mechanism.

## Decision Options

### Option 1: Offset-led reconciliation

Reconcile work using Kafka partition offset ranges:

```text
source unit produced topic partition offsets [start, end)
canonicalize consumed exactly those offsets
sink consumed to that end offset
```

Benefits:

- Strong Kafka-native proof.
- Useful for replay and consumer lag.
- Works even if records do not carry good metadata.

Costs:

- More complex to explain.
- Must track offsets per partition, not as one number.
- Concurrent producers can interleave records in the same offset range.
- Harder dashboards.
- Harder developer contract.

Best when:

- Kafka itself is the audit system.
- Consumers require exact offset replay.
- Records cannot reliably carry ODS metadata.

### Option 2: Source-correlation-led reconciliation

Reconcile using ODS metadata in the records.

For file based ingestion:

```text
source rows for _ods_file_id = A
- DQ failures for _ods_file_id = A
= Kafka records produced for _ods_file_id = A
= Postgres history rows for _ods_file_id = A
```

For message based ingestion:

```text
message/event/request/batch received
- validation failures
- canonicalization DLQ records
= records published
= rows landed in history/audit
```

Benefits:

- Much easier to explain and operate.
- Works across files, APIs, and events.
- Handles concurrent files/messages in the same Kafka topic.
- Dashboards can filter by file/run/source message/source batch.
- Lineage queries become simple joins.

Costs:

- Requires strict metadata propagation.
- Kafka counts by source key should be captured at publish time, not by repeatedly scanning Kafka.
- Current-state upsert tables cannot always be used directly for source-unit count reconciliation.

Best when:

- File-level, message-level, or batch-level business reconciliation is the goal.
- Postgres history/audit tables are the main operational proof.
- Product owners need understandable lineage and counts.

### Option 3: Hybrid

Use source correlation keys for business reconciliation and offsets for operational observability.

Recommended.

```text
Business question:
  Did file A / message M / batch B produce the expected records?
  -> answer using source correlation IDs and reconciliation_log

Operational question:
  Did Kafka/JDBC/S3 sinks consume the topic?
  -> answer using offsets, lag, and connector state
```

Benefits:

- Keeps reconciliation understandable.
- Preserves enough Kafka detail for debugging.
- Avoids making product users reason about partitions.
- Still supports replay and failure investigation.

Costs:

- Requires both metadata fields and offset metrics.
- Requires a clear rule that offsets are not the primary business reconciliation key.

Recommended decision:

```text
Adopt Option 3.
Use source correlation key + _ods_run_id as the primary business lineage contract.
Store partition offsets as operational metadata.
```

## Pattern 1: File Based

File based ingestion has a natural batch identity:

```text
_ods_file_id
```

The file is the unit of lineage, replay, reconciliation, and operational status.

Typical flow:

```text
file
  -> S3 raw
  -> curated
  -> Kafka
  -> Postgres history/audit
  -> Postgres current state, where applicable
```

Core questions:

```text
Was the file received?
How many source rows did it contain?
How many rows failed DQ?
How many rows were published?
How many rows landed in history/audit?
Did the current-state table match latest history?
Where did any failure happen?
```

Primary reconciliation key:

```text
_ods_file_id
```

## Pattern 2: Message Based

Message based ingestion may not have a file. The source message, event, request, or API batch becomes the lineage unit.

Typical flow:

```text
source message / API request / event batch
  -> Kafka or API landing
  -> optional canonicalization
  -> Kafka canonical topic
  -> Postgres history/audit
  -> Postgres current state, where applicable
```

Possible correlation keys:

```text
_ods_source_message_id
_ods_source_event_id
_ods_source_request_id
_ods_source_batch_id
```

For single-record messages, `_ods_source_message_id` or `_ods_source_event_id` is the unit of lineage.

For API pushes or grouped event batches, `_ods_source_batch_id` gives a file-like reconciliation unit.

Core questions:

```text
Was the message/event received?
Was the API request accepted?
How many records were in the batch?
How many records failed validation/canonicalization?
How many records were published?
How many records landed in history/audit?
Did the current-state table match latest history?
Where did any failure happen?
```

The product owner must decide whether message based reconciliation is:

```text
per individual message/event
```

or:

```text
per request/batch/window
```

Recommendation:

```text
Use request/batch/window-level reconciliation for dashboards.
Retain message-level traceability through _ods_source_message_id / _ods_source_event_id.
```

## Required Metadata Contract

Every record produced by the ODS pipeline should carry lineage metadata in the message value.

### File based ingestion

```text
_ods_file_id
_ods_run_id
_ods_domain
_ods_dataset
_ods_business_date
_ods_source_application
_ods_ingested_at
```

### Message based ingestion

```text
_ods_source_message_id
_ods_source_event_id
_ods_source_request_id
_ods_source_batch_id
_ods_run_id
_ods_domain
_ods_dataset
_ods_source_application
_ods_ingested_at
```

Not every message based source needs all four source IDs. The rule is:

```text
the source must provide exactly one stable correlation key,
and optionally a batch/request key when multiple records are submitted together.
```

### Non-canonical to canonical

For non-canonical to canonical pipelines, preserve the original source correlation key and add canonicalization run metadata.

File based:

```text
_ods_file_id
_ods_raw_run_id
_ods_canonicalize_run_id
_ods_domain
_ods_dataset
_ods_business_date
_ods_source_application
_ods_ingested_at
```

Message based:

```text
_ods_source_message_id
_ods_source_event_id
_ods_source_request_id
_ods_source_batch_id
_ods_raw_run_id
_ods_canonicalize_run_id
_ods_domain
_ods_dataset
_ods_source_application
_ods_ingested_at
```

## Unified Reconciliation Shape

Both patterns should use the same reconciliation shape.

For file based:

```text
source_count for _ods_file_id
- dq_fail_count
- canonicalization_dlq_count
= published_count
= history/audit_count
```

For message based:

```text
source_count for _ods_source_batch_id or message window
- validation_fail_count
- canonicalization_dlq_count
= published_count
= history/audit_count
```

The difference is only the source correlation key:

```text
file pattern:    _ods_file_id
message pattern: _ods_source_message_id / _ods_source_batch_id
```

## How To Answer "What Happened To This File?"

Given `_ods_file_id = A`, join or query:

```text
pipeline.file_catalogue
pipeline.run_log
pipeline.run_stage_log
pipeline.reconciliation_log
pipeline.lineage_edge
target history/audit table
current state table, where appropriate
```

Typical query path:

```text
file_catalogue.file_id
  -> run_log.file_id
  -> run_stage_log.run_id
  -> reconciliation_log.run_id
  -> lineage_edge.parent_file_id / child_run_id
  -> target rows where _ods_file_id = file_id
```

This should allow us to answer:

```text
Was the file received?
Was it ingested?
How many source rows did it contain?
How many rows failed DQ?
How many rows were published?
How many rows reached Postgres history/audit?
Did the current-state table reflect the expected latest state?
Where did any failure happen?
```

## How To Answer "What Happened To This Message?"

Given `_ods_source_message_id = M`, join or query:

```text
pipeline.run_log
pipeline.run_stage_log
pipeline.reconciliation_log
pipeline.lineage_edge
target history/audit table
current state table, where appropriate
```

If the message arrived as part of an API batch, start from:

```text
_ods_source_batch_id
```

This should allow us to answer:

```text
Was the message received?
Was it validated?
Was it canonicalized?
Was it published?
Did it reach Postgres history/audit?
Did it affect the current-state table?
Where did any failure happen?
```

## Policy Tables: Current Versus History

The policy model has two different reconciliation surfaces:

```text
policy history / append table
policy current / upsert table
```

These should not be treated the same.

### History / append table

The history table should contain all records pushed for a file/run, assuming it is designed as an append/audit table.

For file-level reconciliation, this is the best Postgres target:

```text
expected history rows for file A
  = published records for file A
  = rows in policy_history where _ods_file_id = A
```

Example:

```text
source_count = 1,000
dq_fail_count = 10
published_count = 990
policy_history rows where _ods_file_id = A = 990
```

That is a clean file-level T2 reconciliation.

### Current / upsert table

The current table represents latest state, not all records pushed by a file.

Therefore this is not always true:

```text
published_count for file A == current table rows where _ods_file_id = A
```

Why not?

File A may insert or update 990 policies. Later file B may update 100 of the same policies. The current table will now show those 100 policies with file B's `_ods_file_id`, not file A's.

So the current table is useful for state reconciliation, not raw file-count reconciliation.

Current table reconciliation should answer:

```text
Does the latest state in the current table match the latest applicable row in history?
```

Suggested check:

```text
For each business key, select the latest valid/effective row from policy_history.
Compare that to policy_current.
```

Example:

```text
policy_history latest row per policy_id
  should equal
policy_current row per policy_id
```

This is a different reconciliation check from file-level T2.

## Recommended Policy Reconciliation Model

Use separate checks.

### File-level T2

Use the history/audit table:

```text
published_count for file A
==
count(*) from policy_history where _ods_file_id = A
```

### Current-state consistency

Use history to derive expected current state:

```text
latest row per policy business key in policy_history
==
row per policy business key in policy_current
```

### Dashboard wording

Avoid saying:

```text
file A reconciles to current table count
```

Prefer:

```text
file A reconciles to history table count
current table reconciles to latest history state
```

## Non-canonical To Canonical Inputs

Non-canonical input may arrive as:

```text
file
event stream
API push
```

The same principle applies to all three:

```text
preserve source correlation ID
preserve ODS run ID
write failed rows/events to DLQ
record counts at each boundary
```

### File input

Use:

```text
_ods_file_id
_ods_run_id
_ods_canonicalize_run_id
```

Reconciliation:

```text
source file rows
- DQ failed rows
- canonicalization DLQ rows
= canonical published rows
= Postgres history/audit rows
```

### Event input

Use:

```text
_ods_source_event_id
_ods_run_id
_ods_canonicalize_run_id
```

Reconciliation may be window-based or event-batch-based:

```text
events consumed for batch/window
- canonicalization DLQ events
= canonical published events
= Postgres history/audit rows
```

### API push

Use:

```text
_ods_source_request_id
_ods_source_batch_id
_ods_run_id
_ods_canonicalize_run_id
```

If API calls contain multiple records, `_ods_source_batch_id` gives a file-like reconciliation key.

## Role Of Kafka Offsets

Kafka offsets should still be captured, but mainly for operations.

Use offsets for:

```text
connector lag
debugging
bounded replay
proving consumer progress
finding records in Kafka tooling
investigating duplicate or missing publish behavior
```

Do not make offsets the main product-facing reconciliation key.

Store offsets as stage metrics:

```json
{
  "topic": "ods.insurance.policy",
  "partitions": [
    {"partition": 0, "start": 120, "end": 370},
    {"partition": 1, "start": 98, "end": 348}
  ]
}
```

This lets engineers debug the Kafka boundary without making product users understand partition math.

## What We Gain By Simplifying This Way

We gain:

```text
simpler mental model
simpler dashboards
clearer product owner story
easier file and message lineage
easier replay reasoning
less dependence on Kafka internals
better support for files, events, and API pushes
```

Product-facing questions:

```text
What happened to file A?
What happened to message M?
What happened to API batch B?
```

Simple answer:

```text
Look up the source correlation key across pipeline tables and history/audit targets.
```

Engineering question:

```text
Did Kafka and sinks move the right offsets?
```

Engineering answer:

```text
Use stored per-partition offset metrics and connector lag.
```

## What We Lose

We lose offsets as the primary business proof.

This means:

```text
Kafka is no longer the main audit ledger.
Postgres history/audit and pipeline control tables become the main audit ledger.
```

This is acceptable if:

```text
records reliably carry ODS metadata
publish jobs record producer counts
history/audit tables retain all pushed records
current-state tables are reconciled separately
```

## Product Owner Decisions

The product owner should decide:

1. Should source correlation ID be the primary lineage key?

   Recommended: yes.

2. Should Kafka offsets be operational metadata rather than reconciliation identity?

   Recommended: yes.

3. Is the policy history table the source of truth for file-level Postgres reconciliation?

   Recommended: yes.

4. Should policy current table reconciliation be defined as "latest history equals current"?

   Recommended: yes.

5. For non-canonical API/event inputs, do we require a file-equivalent batch/request ID?

   Recommended: yes.

6. Do we require ODS lineage metadata fields in every Kafka value and Postgres target table?

   Recommended: yes.

7. For message based ingestion, is reconciliation per individual message or per batch/window?

   Recommended: per batch/window for dashboards, with message-level traceability retained.

## Recommended Direction

Adopt a hybrid model:

```text
Business reconciliation:
  by _ods_file_id for files
  by _ods_source_message_id / _ods_source_batch_id for messages

Operational observability:
  by Kafka topic, partition offsets, and connector lag

Postgres source-unit proof:
  by history/audit tables

Postgres current-state proof:
  current table equals latest state derived from history
```

This keeps the design simpler without giving up lineage, replay, or observability.
