ar# ODS Platform — Reconciliation Design
**Date:** 2026-04-15  
**Status:** Draft  
**Scope:** All four ingestion patterns — S3 (batch files), CDC, API, Event

---

## 1. What Reconciliation Means in a Streaming Platform

In a batch pipeline, reconciliation is straightforward: count the rows in the source file, count the rows in the destination table, compare. A streaming platform is harder because data is never "done". Records arrive continuously, out-of-order, and from multiple sources. There is no natural boundary at which you can say "all data for today has arrived".

Reconciliation for this platform means answering four questions at all times:

1. **Completeness** — did everything published to Kafka arrive from the source? Did everything that should have been published get published?
2. **Correctness** — is the data in Kafka consistent with the data in the source system?
3. **Timeliness** — is data flowing within expected latency bounds? Are consumers keeping up?
4. **Integrity** — are there duplicates, gaps, or out-of-order records?

These questions must be answered differently depending on the source pattern.

---

## 2. Reconciliation by Source Pattern

### 2.1 S3 Batch Files (Current — implemented)

Batch files have a natural boundary: the file. Reconciliation is the simplest of the four patterns.

**What is already implemented:**
- Glue job captures source row count from Parquet/CSV footer at job start
- After Kafka publish, Kafka partition offset delta is compared against source row count
- Mismatch → undelivered records written to DLQ, alarm fires, job fails
- `pipeline.glue_job_log` records `record_count` at each status transition

**What this covers:**
- Completeness at publish time (count reconciliation)
- Integrity of the publish transaction (Kafka transactions prevent partial commits)

**What it does NOT cover:**
- Whether consumers have consumed all published records (consumer lag)
- Whether the source file itself was complete (upstream responsibility)
- Business-level reconciliation — e.g. sum of `premium` in source vs sum of `premium` in Kafka

---

### 2.2 CDC (Change Data Capture)

CDC streams every INSERT, UPDATE, and DELETE from a source database as a continuous stream of change events. Reconciliation is fundamentally different from batch:

- There is no file boundary — the stream is continuous
- Records can arrive out of order within the replication buffer
- The source database has a definitive "current state" that the CDC stream must converge to
- Deletes, updates, and inserts must all be accounted for

**Key reconciliation concepts for CDC:**

**Log Sequence Number (LSN) / Transaction ID tracking**  
Every CDC event carries a position in the source database's write-ahead log (WAL): the LSN (PostgreSQL) or binlog position (MySQL). This is the CDC equivalent of a file row count — it is the ground truth for "how far through the change stream we are".

The reconciliation check is: what is the current LSN on the source database, and what is the last LSN we have published to Kafka? The gap is the replication lag.

```
Source DB WAL position:  LSN 0/4A218B0
Last published to Kafka: LSN 0/4A1FF20
Lag:                     ~120KB of WAL (~500ms at typical write rate)
```

**Row count reconciliation (periodic)**  
Periodically (e.g. hourly), compare the total row count in the source table against the materialised count in the consumer's state store (Kafka Streams KTable or a downstream database). These should converge within the replication lag window.

```sql
-- Source database
SELECT COUNT(*) FROM insurance.policies;
-- → 1,247,893

-- Consumer's materialised view (Kafka Streams state store, synced to RDS)
SELECT COUNT(*) FROM ods_consumer.policies;
-- → 1,247,889

-- Difference: 4 rows in-flight (within replication lag)
```

**Checksum reconciliation (deep, periodic)**  
For critical datasets, compare a row-level checksum between source and consumer. This detects silent data corruption that count reconciliation misses.

```sql
-- Source: MD5 of all key+value concatenations, ordered by primary key
SELECT MD5(STRING_AGG(policy_id || '|' || status || '|' || premium::text, ',' ORDER BY policy_id))
FROM insurance.policies;

-- Consumer state: same computation on materialised table
-- If checksums differ, there is a data integrity issue beyond count mismatch
```

**Initial load reconciliation**  
CDC pipelines typically start with a full snapshot (initial load) followed by the change stream. The initial load count must be verified before the change stream begins, otherwise any record missed in the snapshot is lost permanently.

```
Initial load:    1,247,893 rows snapshotted
LSN at snapshot: 0/48A1200  ← snapshot taken at this point
CDC stream:      starts from LSN 0/48A1200
Total:           1,247,893 + N changes = materialised count
```

---

### 2.3 API-Sourced Data

API ingestion typically polls an external API at intervals (or uses webhooks) and publishes responses to Kafka. Reconciliation challenges:

**Sequence/cursor tracking**  
Most APIs expose a cursor, page token, or `since` parameter for incremental fetching. The reconciliation check is: does the API report any records with `updated_at > last_polled_at` that we have not yet published?

```json
// API response includes pagination metadata
{
  "total_count": 50234,
  "page": 1,
  "page_size": 1000,
  "next_cursor": "eyJpZCI6IjEyMzQ1In0="
}
```

If the pipeline processes 50 pages of 1,000 records each, the reconciliation check is: did we publish exactly 50,000 records matching `total_count`?

**Idempotency key tracking**  
API responses include a unique record identifier (`id`, `uuid`, `reference_number`). The reconciliation store tracks which IDs have been published. A periodic scan compares IDs in the API response window against IDs in Kafka (or the consumer's state).

**Rate limit and pagination gaps**  
If an API call fails mid-pagination (rate limit, timeout), some pages may be re-fetched and some may be skipped. The reconciliation mechanism must detect skipped pages — not just count totals.

---

### 2.4 Event-Sourced Data (Application Events)

Application events are emitted by source systems directly to Kafka (or SNS/SQS → Kafka via routing). Reconciliation is the hardest pattern:

- No persistent source of truth — the source system emits and forgets
- Events can arrive late (network delays, buffering, retries)
- Event order is not guaranteed across partitions
- Duplicates are possible at the producer level

**Sequence number tracking**  
Well-designed event producers include a monotonically increasing sequence number per aggregate (per entity, per partition key). A gap in the sequence — e.g. events 1001, 1002, 1004, 1005 (missing 1003) — indicates a lost event.

```json
{
  "event_id": "evt-1004",
  "aggregate_id": "POL-001",
  "sequence": 1004,
  "event_type": "PolicyRenewed",
  "timestamp": "2026-04-15T09:00:00Z"
}
```

**Heartbeat / sentinel events**  
Source systems can emit periodic heartbeat events (e.g. every 5 minutes) even when no business events occur. If a heartbeat is missed, the consumer knows the producer is silent — either intentionally (no activity) or due to a failure.

**Event count reconciliation (windowed)**  
For each business time window (hourly, daily), compare the expected event volume against the observed volume. Historical patterns establish a baseline: "on a Tuesday morning, PolicyRenewed events average 200/hour". A significant deviation triggers an alert.

---

## 3. Reconciliation Architecture

### 3.1 Reconciliation Tiers

Reconciliation is not a single check — it operates at four tiers with different latencies and coverage.

```
┌─────────────────────────────────────────────────────────────────────┐
│  T0 · Real-time (at publish time)                     <1 minute     │
│  Count check at Kafka publish. Already implemented for S3 pattern.  │
│  For CDC/events: transaction/sequence tracking at producer.         │
└─────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────┐
│  T1 · Near-real-time (consumer lag monitoring)        1–5 minutes   │
│  Kafka consumer group offset vs partition high-water mark.          │
│  Are consumers keeping up? Is any consumer group stalled?           │
└─────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────┐
│  T2 · Periodic aggregated check                       Hourly        │
│  Source count vs Kafka topic record count vs consumer state count.  │
│  Runs as a scheduled Glue or Lambda job.                            │
└─────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────┐
│  T3 · Full business reconciliation                    Daily         │
│  Business-level: sum of values, checksums, complete row comparison. │
│  Source of truth comparison per dataset. SLA-driven.                │
└─────────────────────────────────────────────────────────────────────┘
```

---

### 3.2 T0 — Real-Time Publish Reconciliation

**Already implemented for S3 batch (count reconciliation at publish time).**

For the other patterns, T0 reconciliation looks like:

| Pattern | T0 check | How |
|---|---|---|
| S3 batch | Source row count == Kafka offset delta | Glue job — already implemented |
| CDC | LSN at publish == LSN consumed from WAL | Debezium/DMS offset tracking |
| API | Records fetched == records published | API DAG task-level count check |
| Event | Producer-emitted sequence numbers contiguous | Consumer sequence gap detector |

T0 failures should be treated as hard failures: route to DLQ, emit alarm, do not mark as completed.

---

### 3.3 T1 — Consumer Lag Monitoring

Consumer lag is the gap between the latest record written to a Kafka partition (high-water mark) and the last record a consumer group has committed.

```
Partition 0:  [msg 1][msg 2][msg 3][msg 4][msg 5]  ← high-water mark: offset 5
Consumer A:   committed offset 3
Lag:          2 messages (offsets 4 and 5)
```

**Why lag matters:**  
A consumer with growing lag is falling behind. Left unaddressed, it means data is in Kafka but not yet processed — downstream systems are stale. A stalled consumer (lag growing without bound) is an incident.

**Implementation:**  
MSK exposes consumer group lag as a CloudWatch metric: `SumOffsetLag` per consumer group per topic. Set alarms at two thresholds:

- **Warning**: lag > N records sustained for > 5 minutes — consumer is slow
- **Critical**: lag > M records or lag not shrinking for > 15 minutes — consumer is stalled

The lag thresholds depend on the dataset's expected throughput and consumer SLO. These values must be defined per topic, not globally.

---

### 3.4 T2 — Periodic Aggregated Reconciliation

A scheduled reconciliation job (Glue or Lambda, triggered by MWAA on a cron) runs hourly and compares counts across three planes.

```
Source plane    →    Kafka plane    →    Consumer plane
(source system)      (MSK topic)         (consumer state store)

Count at T:           Topic record count    Consumer materialised count
  S3: file row count  (offset range)        (downstream DB or KTable)
  CDC: table count    
  API: API total      
  Event: N/A (no source of truth)
```

**What the T2 job does:**

1. Query the source system (or `pipeline.glue_job_log` for S3 batch) for expected record counts by business date and dataset
2. Query the Kafka topic for actual record counts in the same time window (using Kafka Admin API to get earliest and latest offsets per partition)
3. Query the consumer state store (if accessible) for their materialised count
4. Write the results to `pipeline.reconciliation_log` in PostgreSQL
5. Emit CloudWatch metrics for any discrepancy beyond tolerance threshold
6. Raise an alarm if discrepancy exceeds tolerance for two consecutive checks

**Tolerance thresholds:**
Not every discrepancy is an incident. CDC replication lag means there will typically be a small gap between source and Kafka counts. Define per-dataset tolerances:

```yaml
# In YAML config per dataset
reconciliation:
  t2_tolerance_records: 100        # allow up to 100 records in-flight
  t2_tolerance_percent: 0.01       # or 0.01% of total, whichever is larger
  t2_check_interval_minutes: 60
  t3_check_schedule: "0 6 * * *"   # daily at 06:00
```

---

### 3.5 T3 — Full Business Reconciliation

T3 is the authoritative daily check. It runs after the business day's data is expected to have settled (accounting for replication lag, late-arriving events, and consumer processing time).

**T3 checks:**

1. **Row count by business date** — total records per dataset per business date in source vs Kafka vs consumer
2. **Aggregate value check** — sum of key numeric fields (e.g. total premium, total claim amount) in source vs consumer. This catches silent data corruption that count reconciliation misses.
3. **Null/missing key check** — are any records in Kafka missing their key fields? These would have produced a `null` message key, bypassing the idempotency layer.
4. **Duplicate key check** — within a business date window, are there duplicate message keys in the topic? Indicates an idempotency failure somewhere in the pipeline.

**T3 reconciliation table:**

```sql
CREATE TABLE pipeline.reconciliation_log (
    id                  BIGSERIAL PRIMARY KEY,
    check_type          VARCHAR NOT NULL,         -- t2_count | t3_count | t3_aggregate | t3_duplicate
    pipeline_type       VARCHAR NOT NULL,         -- publish | cdc | api | event
    domain              VARCHAR NOT NULL,
    dataset             VARCHAR NOT NULL,
    business_date       DATE,
    window_start        TIMESTAMP,
    window_end          TIMESTAMP,
    source_count        BIGINT,
    kafka_count         BIGINT,
    consumer_count      BIGINT,
    source_aggregate    NUMERIC,                  -- e.g. SUM(premium) at source
    consumer_aggregate  NUMERIC,                  -- e.g. SUM(premium) at consumer
    discrepancy_count   BIGINT,                   -- kafka_count - source_count
    discrepancy_pct     NUMERIC(6,4),
    status              VARCHAR NOT NULL,         -- ok | warning | failed
    error_detail        TEXT,
    created_at          TIMESTAMP DEFAULT NOW()
);
```

**Useful queries:**

```sql
-- Any failed reconciliations in the last 24 hours
SELECT dataset, business_date, check_type, source_count, kafka_count, discrepancy_count, error_detail
FROM pipeline.reconciliation_log
WHERE status = 'failed'
  AND created_at >= NOW() - INTERVAL '24 hours'
ORDER BY created_at DESC;

-- Trend of discrepancy for a dataset over time
SELECT business_date, source_count, kafka_count, discrepancy_count, discrepancy_pct
FROM pipeline.reconciliation_log
WHERE dataset = 'policies'
  AND check_type = 't3_count'
ORDER BY business_date DESC
LIMIT 30;

-- Aggregate reconciliation failures (data integrity issues)
SELECT dataset, business_date, source_aggregate, consumer_aggregate,
       source_aggregate - consumer_aggregate AS value_discrepancy
FROM pipeline.reconciliation_log
WHERE check_type = 't3_aggregate'
  AND status != 'ok'
ORDER BY business_date DESC;
```

---

## 4. Reconciliation by Data Operation Type (CDC-specific)

CDC introduces three operation types — INSERT, UPDATE, DELETE — that batch and event patterns do not have. Reconciliation must account for all three.

### 4.1 INSERT Reconciliation

Equivalent to batch: a new row appeared in the source. Verify it arrives in Kafka with the correct key and value.

### 4.2 UPDATE Reconciliation

An update to an existing row produces a CDC event with the `before` and `after` state. Reconciliation must verify:
- The `before` state matches the previously published value for that key
- The `after` state matches the current source database value
- No `before` → `after` transitions were missed (sequence gap = missed update)

A sequence gap means the consumer's materialised state could be stale by N versions, even if the latest version eventually arrives.

### 4.3 DELETE Reconciliation

A deleted row must produce a tombstone message in Kafka (message with the record's key and a `null` value). Verify:
- Every row deleted in the source appears as a tombstone in Kafka
- The tombstone key matches the deleted row's primary key
- Consumer state removes the record (count at consumer should decrease)

**Delete reconciliation gap:**  
If the Kafka topic has a retention policy shorter than the period between T3 reconciliation checks, tombstones may have been compacted away before T3 runs. This must be accounted for in retention configuration — tombstone retention must exceed the T3 check window plus any consumer lag.

```
Tombstone emitted:   2026-04-14 22:00
Topic retention:     7 days minimum (must cover T3 window + max consumer lag)
T3 check:            2026-04-15 06:00  ← tombstone must still be present
```

---

## 5. Handling Late-Arriving Data

Late-arriving data is a fundamental challenge in streaming — a record with a business timestamp of yesterday arrives today. This creates two problems:

1. **Count reconciliation closes too early** — the T3 check for business_date=2026-04-14 runs at 06:00 on 2026-04-15. A record timestamped 2026-04-14 that arrives at 07:00 on 2026-04-15 is missed.

2. **Consumer aggregations are incorrect** — if a consumer aggregates by business date (e.g. daily premium totals), a late-arriving record requires retroactive correction.

### 5.1 Watermarks

A watermark is a threshold that declares "all data with event_time < W is assumed to have arrived". Records arriving after the watermark for their time window are late.

Define per-dataset watermark policies in the YAML config:

```yaml
reconciliation:
  watermark_hours: 4          # wait 4 hours after business_date closes before running T3
  late_arrival_window_hours: 24  # accept late records up to 24h after business_date
  late_arrival_action: reprocess  # reprocess | dlq | discard
```

### 5.2 T3 Reconciliation Timing

T3 should not run at midnight — it should run after the watermark period has elapsed:

```
Business date closes:  2026-04-14 23:59
Watermark:             +4 hours
T3 runs at:            2026-04-15 04:00
```

Records arriving between 2026-04-14 23:59 and 2026-04-15 04:00 with a business timestamp of 2026-04-14 are still counted in the T3 for 2026-04-14.

### 5.3 Retroactive Reconciliation

For records arriving after the watermark has passed (genuinely late), the platform must support retroactive reconciliation: re-running the T3 check for a historical business date when a late record is detected.

This requires:
- The consumer's state store supports retroactive updates (not a batch table that was already snapshotted)
- The reconciliation log supports multiple T3 entries per business_date (keyed on `created_at`, not `business_date`)
- An alert fires when a retroactive correction changes a previously-passed T3 result

---

## 6. Duplicate Detection

Duplicates can enter the platform at multiple points. Each requires a different detection strategy.

| Source | Duplicate type | Detection | Resolution |
|---|---|---|---|
| S3 batch | Same file published twice | `file_state` idempotency guard at DAG entry | Already implemented |
| S3 batch | Same record in different files | Duplicate message key in Kafka topic | T3 duplicate key check |
| CDC | Debezium/DMS reprocesses LSN range on restart | Same LSN published twice | Kafka exactly-once + consumer dedup by LSN |
| API | Pagination overlap — same record on page N and N+1 | Record ID already in `published_ids` table | API DAG dedup check before publish |
| Event | Source application retries on timeout, publishes twice | Same `event_id` in Kafka | Consumer dedup by `event_id`; T3 duplicate check |

**T3 duplicate detection query:**

```sql
-- Detect duplicate message keys within a business date window
-- Requires topic to be materialized to a table (e.g. via Kafka Connect JDBC Sink)
SELECT message_key, COUNT(*) AS occurrences, MIN(offset) AS first_offset, MAX(offset) AS last_offset
FROM kafka_materialised.ods_insurance_policies
WHERE business_date = '2026-04-14'
GROUP BY message_key
HAVING COUNT(*) > 1
ORDER BY occurrences DESC;
```

---

## 7. Sequence Gap Detection (Events and CDC)

A sequence gap means a record was lost between the source and Kafka. For CDC, it means a database transaction was not replicated. For events, it means an application event was dropped.

### 7.1 CDC Sequence Gap Detection

Monitor the gap between the LSN the CDC connector has consumed and the current LSN on the source database:

```
Source DB current LSN:      0/5B3A200
CDC connector committed LSN: 0/5B1F800
Gap:                         ~100KB WAL

Alert threshold:             > 500KB (configurable — dataset write rate dependent)
Critical threshold:          > 2MB (replication is significantly behind)
```

If the CDC connector stops advancing its committed LSN while the source database continues to write, the gap grows unboundedly. This is a connector failure, not just lag.

### 7.2 Event Sequence Gap Detection

For events with per-aggregate sequence numbers:

```python
# Consumer-side gap detector (Kafka Streams or Flink)
# For each aggregate_id, track last_seen_sequence

def process_event(event):
    expected_seq = last_seen[event.aggregate_id] + 1
    if event.sequence > expected_seq:
        gap_count = event.sequence - expected_seq
        emit_metric("event.sequence.gap", gap_count, 
                    tags={"aggregate_id": event.aggregate_id, "dataset": event.dataset})
        # Route gap metadata to reconciliation topic for investigation
    last_seen[event.aggregate_id] = event.sequence
```

Sequence gaps emit a metric and a structured event to `ods.pipeline.reconciliation` (a dedicated topic for reconciliation metadata — see Section 8).

---

## 8. Reconciliation Kafka Topic — `ods.pipeline.reconciliation`

Alongside the existing `ods.pipeline.audit` topic, introduce a dedicated reconciliation topic for structured reconciliation events. This decouples reconciliation monitoring from audit and allows independent consumers (alerting, dashboards, remediation workflows).

**Topic:** `ods.pipeline.reconciliation`

**Event schema:**

```json
{
  "reconciliation_id": "uuid",
  "check_type": "t0_count | t1_lag | t2_count | t3_count | t3_aggregate | t3_duplicate | sequence_gap",
  "pipeline_type": "publish | cdc | api | event",
  "domain": "insurance",
  "dataset": "policies",
  "business_date": "2026-04-14",
  "window_start": "2026-04-14T00:00:00Z",
  "window_end": "2026-04-15T00:00:00Z",
  "source_count": 10000,
  "kafka_count": 9997,
  "discrepancy": 3,
  "discrepancy_pct": 0.03,
  "status": "ok | warning | failed",
  "detail": "3 records missing from partition 2 after publish",
  "timestamp": "2026-04-15T06:00:00Z"
}
```

**Consumers of this topic:**
- CloudWatch Metric Publisher — translate `status=failed` events into CloudWatch metrics for alarming
- Reconciliation Dashboard — real-time view of reconciliation health across all datasets
- Automated Remediation (future) — trigger DLQ replay or resubmission workflows on specific failure types

---

## 9. Reconciliation for Each Pattern — Summary

### 9.1 S3 Batch — Reconciliation Checklist

| Check | Tier | Timing | Status |
|---|---|---|---|
| Source row count == Kafka offset delta | T0 | At publish | Implemented |
| Consumer group lag | T1 | Continuous | Not implemented |
| `glue_job_log` count vs Kafka count | T2 | Hourly | Not implemented |
| Business date count + aggregate sum | T3 | Daily post-watermark | Not implemented |
| Duplicate message key check | T3 | Daily | Not implemented |

### 9.2 CDC — Reconciliation Checklist

| Check | Tier | Timing | Status |
|---|---|---|---|
| LSN commit lag vs source LSN | T0 | Continuous | Not implemented |
| Consumer group lag | T1 | Continuous | Not implemented |
| Source table count vs materialised count | T2 | Hourly | Not implemented |
| Checksum reconciliation (source vs consumer) | T3 | Daily | Not implemented |
| Tombstone coverage for deletes | T3 | Daily | Not implemented |
| Initial snapshot count verification | One-time | At CDC setup | Not implemented |

### 9.3 API — Reconciliation Checklist

| Check | Tier | Timing | Status |
|---|---|---|---|
| API total_count == records published | T0 | Per API run | Not implemented |
| Consumer group lag | T1 | Continuous | Not implemented |
| API cursor/page continuity check | T0 | Per API run | Not implemented |
| API count by window vs Kafka count | T2 | Hourly | Not implemented |
| Duplicate record ID check | T3 | Daily | Not implemented |

### 9.4 Event — Reconciliation Checklist

| Check | Tier | Timing | Status |
|---|---|---|---|
| Sequence number gap detection | T0 | Per event | Not implemented |
| Heartbeat/sentinel event monitoring | T0 | Continuous | Not implemented |
| Consumer group lag | T1 | Continuous | Not implemented |
| Event volume vs historical baseline | T2 | Hourly | Not implemented |
| Duplicate `event_id` check | T3 | Daily | Not implemented |

---

## 10. Open Items and Design Decisions Required

| Item | Question | Impact if unresolved |
|---|---|---|
| CDC connector | Debezium (MSK Connect) or AWS DMS? | Determines what LSN/position metadata is available for reconciliation |
| Consumer state store | Where do consumers materialise their state? (Kafka Streams RocksDB, RDS, DynamoDB?) | T2/T3 cross-plane count comparison requires a queryable consumer count |
| Watermark policy | How long to wait after business date closes before running T3? Dataset-specific or platform-wide? | T3 false positives if watermark is too short |
| Late arrival handling | Reprocess, DLQ, or discard? What is the maximum late arrival window per dataset? | Retroactive reconciliation complexity vs data completeness |
| Sequence number contract | Will source applications emit per-aggregate sequence numbers? Or only event UUIDs? | Without sequences, gap detection is not possible for events |
| T3 aggregate fields | Which fields should be summed in T3 aggregate reconciliation? Must be defined per dataset | Without this, T3 only catches count issues, not value corruption |
| `ods.pipeline.reconciliation` topic owner | Who consumes this topic? Who is responsible for alerting on it? | Topic can exist unmonitored if ownership is not assigned |
| Reconciliation SLOs | What is the acceptable discrepancy before an alert fires? T3 must complete by when each day? | Without SLOs, reconciliation is not operationally actionable |
| Consumer team participation | Do consumer teams report their materialised counts to the platform, or does the platform query consumer state directly? | T2/T3 cross-plane reconciliation requires consumer cooperation or direct access |
| Tombstone retention | MSK topic retention must exceed T3 check window + max consumer lag. Is 7-day retention sufficient? | Short retention means tombstones are compacted before T3 can verify deletes |

---

## 11. Recommended Implementation Order

1. **T1 — Consumer lag alarms** (lowest effort, highest operational value): configure CloudWatch alarms on `SumOffsetLag` per topic per consumer group. This is available today via MSK metrics with no code changes.

2. **T2 count reconciliation for S3 batch** (builds on existing `glue_job_log`): schedule a daily Glue job that queries `glue_job_log` for completed runs and compares `record_count` against Kafka offset deltas for the same business date. Writes results to `pipeline.reconciliation_log`.

3. **T3 count + aggregate for S3 batch**: extend the T2 job to run post-watermark with aggregate field sums. Defines the reconciliation pattern for subsequent patterns.

4. **Sequence gap detection for events**: once the event pattern is designed, implement the per-aggregate sequence tracker as a Kafka Streams application or Flink job. Emits to `ods.pipeline.reconciliation`.

5. **LSN lag monitoring for CDC**: once the CDC connector is selected, configure LSN lag monitoring and integrate with T2 count comparison.

6. **Full T3 for CDC**: checksum reconciliation and delete/tombstone coverage verification. This is the most complex check and should be implemented last.
