# Kafka Sink Pipeline — Step Commentary

---

## Startup — Connector Registration

When a connector is deployed or restarted, it upserts a row in `pipeline.sink_connector_registry`. This is the platform's operational catalogue of active sink connectors — one row per connector instance. It is the first place an engineer should look when investigating connector health, without needing to query the MSK Connect API.

On restart, the row is updated to `status=running` rather than inserted again, ensuring there is always exactly one row per connector.

```sql
-- On connector deploy or restart
INSERT INTO pipeline.sink_connector_registry
    (connector_name, topic, sink_type, target, consumer_group, write_mode, status)
VALUES
    ('ods-iceberg-sink-policies', 'ods.insurance.policies',
     'iceberg', 's3://ods-iceberg-prod/insurance/policies/',
     'ods-iceberg-sink-policies', NULL, 'running')
ON CONFLICT (connector_name) DO UPDATE
    SET status = 'running', updated_at = NOW();
```

---

## Phase 1 · Consume & Deserialise

**❶ ❸ Poll batch from MSK topic**

Each connector maintains its own consumer group (`ods-iceberg-sink-{dataset}` and `ods-jdbc-sink-{dataset}`). They poll the MSK topic independently. This means:

- They can progress at different rates — a slow JDBC write does not block the Iceberg connector
- A failure in one connector does not affect the other
- Each connector's offset position is tracked separately in MSK Connect's internal offset topic

The poll returns up to `batch.size` records, or whatever has accumulated within `poll.interval.ms`. Both values are configured in the MSK Connect connector configuration JSON.

**❷ ❹ Deserialise Avro via Glue Schema Registry**

Each record uses the Confluent wire format: a single magic byte (`0x00`) followed by a 4-byte big-endian schema ID, followed by the Avro binary payload. The connector reads bytes 1–4 to extract the schema ID without deserialising the payload.

The connector looks up the schema ID from its connector-local in-memory cache. If not cached, it calls the Glue Schema Registry. Schemas are cached for the lifetime of the connector — there is no TTL. This means repeated lookups for the same schema version do not call the registry on every record, but a connector restart clears the cache.

The Glue Schema Registry is compatible with the Confluent wire format. The MSK Connect connector must be configured with an Avro converter that reads this format — for example, the AWS Glue Schema Registry Library's `GlueSchemaRegistryKafkaDeserializer`. The specific converter class is set in the connector configuration JSON.

If the deserialised record's fields are incompatible with the current destination table schema (for example, a required column in the destination has been dropped from the Avro schema), the record is routed to the DLQ:

```
ods-dlq-{env}/sink-schema-incompatible/date={date}/topic={topic}/offset={offset}/
```

The incompatible record is not silently dropped. It is recoverable from the DLQ once the schema issue is resolved. The remaining records in the batch continue processing. The connector does not pause for individual schema incompatibility — only write failures cause a pause.

---

## Phase 2 · Iceberg Write

**❺ Buffer and convert records**

Deserialised records are held in an in-memory buffer and converted to Parquet row format. The connector flushes when whichever threshold is reached first:
- `buffer.size.bytes` — total size of buffered data in bytes
- `flush.timeout.ms` — maximum time any record waits in the buffer before being flushed

Tuning these values is a trade-off between write latency (lower thresholds = more frequent, smaller writes) and S3 write efficiency (higher thresholds = fewer, larger Parquet files).

**❻ Write Parquet data files to S3**

When flushing, the connector writes one or more Parquet data files to the Iceberg table's S3 location:
```
ods-iceberg-{env}/{domain}/{dataset}/data/date={date}/{uuid}.parquet
```

Files are partitioned by date, extracted from the record's `business_date` field or the Kafka message timestamp. Parquet column encoding and compression (Snappy by default) are applied at this point.

If the S3 write fails at this stage (unavailable, permissions, capacity), the batch is routed to the DLQ with `reason=s3_write_failure`. The connector pauses and its registry entry is updated to `status=paused`. No Iceberg snapshot is committed — the data files may exist on S3 as orphans, but they are invisible to readers and will be cleaned up by Iceberg's expiry process.

**❼ Commit Iceberg snapshot (atomic metadata update)**

Iceberg's ACID guarantee comes from its atomic snapshot mechanism. After writing data files, the connector performs a three-step metadata commit:

1. **Write manifest file** — lists the new data file paths and their row counts
2. **Write snapshot file** — records the commit timestamp, manifest pointer, and parent snapshot ID
3. **Atomic metadata.json swap** — updates the table's root metadata pointer to the new snapshot

Step 3 is the atomic boundary. Until it completes, no reader can see the new data — the table's current snapshot still points to the previous state. Once it completes, all readers immediately see the new records. There is no window during which a reader sees partial data.

If the metadata.json swap fails, the data files and manifest exist on S3 as orphans. They are never visible to readers. Iceberg's expiry process (`expireSnapshots`) cleans them up on its next run. The connector routes the batch to the DLQ with `reason=snapshot_commit_failure`, pauses, and updates the registry.

**❽ Sync table definition to Glue Data Catalog**

After a successful snapshot commit, the connector syncs the current Iceberg table schema and partition specification to the Glue Data Catalog entry `ods_{domain}.{dataset}`. This keeps Athena's view of the table accurate after any schema evolution. Athena reads from the Glue Data Catalog, not directly from S3 metadata.json.

**❾ Trigger Glue Crawler (async · no wait)**

The Glue Crawler `ods-{dataset}-crawler` is triggered asynchronously to refresh partition metadata in the Glue Data Catalog. The connector does not wait for crawler completion — crawler latency does not block offset commit or the next poll cycle.

**Important:** Glue Crawler failures are not reported to the connector and not retried automatically. Monitor Glue Crawler logs via CloudWatch. If a crawler run fails, partition metadata in Athena becomes stale. Recovery: manually re-trigger the crawler or run `MSCK REPAIR TABLE` in Athena.

**❿ Commit MSK consumer group offsets**

After the Iceberg snapshot is confirmed, the connector commits its consumer group offset to MSK. This is the point of no return for this batch — if the connector restarts after this point, it will not reprocess these records.

**Offset commit ordering is critical:** offset commit happens *after* snapshot commit, not before. If the connector restarts between snapshot commit and offset commit, it will re-read and re-write the same records on the next run. Iceberg handles this safely — re-writing produces a new snapshot that includes the same data files. Subsequent compaction or the Iceberg expiry process will deduplicate. No data is lost or corrupted.

After offset commit, the connector updates its registry row:
```sql
UPDATE pipeline.sink_connector_registry
SET last_offset = 12345,
    last_record_at = NOW(),
    batch_latency_ms = 432,
    status = 'running',
    updated_at = NOW()
WHERE connector_name = 'ods-iceberg-sink-policies';
```

---

## Phase 3 · JDBC Write

**⓫ Batch write to PostgreSQL**

The JDBC connector maps Avro field names to PostgreSQL column names using the field mapping defined in the connector configuration JSON (property: `transforms` or explicit field mapping config, depending on the connector plugin). It then executes a batch write.

Write mode is configured per dataset in the MSK Connect connector configuration JSON and recorded in `pipeline.sink_connector_registry.write_mode`:

- **INSERT** — append-only. Each record becomes a new row. Suitable for immutable event records (e.g. audit log entries, transaction history). Produces duplicate rows if the connector reprocesses the same batch — use only when the consumer guarantees idempotency or deduplication downstream.
- **UPSERT** — merge on primary key. Existing rows are updated; new rows are inserted. Suitable for entity records (e.g. policy status, customer profile) that change over time and where only the latest state is needed.

Write mode cannot be changed after the connector is deployed without a data migration.

**⓬ Commit MSK consumer group offsets**

Offset commit happens after PostgreSQL confirms the write — same ordering principle as the Iceberg connector. If PostgreSQL write fails, the batch is routed to the DLQ, the connector pauses, and the registry is updated with `status=paused`, `error_detail`, and `failure_count + 1`.

---

## Phase 4 · Post-Write

**⓭ ⓮ Emit audit events**

Both connectors emit a structured event to `ods.pipeline.audit` on each batch completion, consistent with the ingestion and publish pipelines. The audit event records:
- `connector_name` and `sink_type`
- `topic`, `consumer_group`, `offset_range` (from offset to offset)
- `record_count` written
- `batch_latency_ms`
- `target` (S3 path or PostgreSQL table name)
- `status` (`completed` or `failed`)

The audit topic is sinked to `ods-audit-sink-{env}` via the Kafka Connect S3 Sink Connector for long-term retention and Athena querying.

---

## Failure Handling

| Failure | Scope | DLQ | Connector | Registry |
|---|---|---|---|---|
| Schema incompatible record | Single record | Record routed to DLQ | Continues | No change |
| S3 data file write failure | Entire batch | Batch routed to DLQ | **Pauses** | status=paused |
| Iceberg snapshot commit failure | Entire batch | Batch routed to DLQ | **Pauses** | status=paused |
| PostgreSQL write failure | Entire batch | Batch routed to DLQ | **Pauses** | status=paused |
| Glue Crawler failure | Partition metadata only | None | Continues | No change |

A paused connector requires an engineer to:
1. Investigate root cause (CloudWatch logs, `pipeline.sink_connector_registry.error_detail`)
2. Fix the underlying issue
3. Manually resume the connector via the MSK Connect API or AWS Console
4. If needed, replay DLQ records back to the source topic or directly re-trigger the connector

---

## PostgreSQL Tables

```sql
-- Catalogue of registered sink connectors
-- One row per connector — upserted on deploy/restart, never deleted
CREATE TABLE pipeline.sink_connector_registry (
    id               SERIAL PRIMARY KEY,
    connector_name   VARCHAR NOT NULL UNIQUE,  -- e.g. ods-iceberg-sink-policies
    topic            VARCHAR NOT NULL,          -- e.g. ods.insurance.policies
    sink_type        VARCHAR NOT NULL,          -- iceberg | jdbc
    target           VARCHAR NOT NULL,          -- S3 path or PostgreSQL table name
    consumer_group   VARCHAR NOT NULL,          -- e.g. ods-iceberg-sink-policies
    write_mode       VARCHAR,                   -- insert | upsert (jdbc only — NULL for iceberg)
    status           VARCHAR NOT NULL,          -- running | paused | failed
    last_offset      BIGINT,                    -- last committed MSK consumer group offset
    last_record_at   TIMESTAMP,                 -- timestamp of last successfully written record
    batch_latency_ms BIGINT,                    -- duration of last completed batch in ms
    failure_count    INT NOT NULL DEFAULT 0,    -- cumulative count of batch failures
    dlq_record_count BIGINT NOT NULL DEFAULT 0, -- cumulative count of records sent to DLQ
    error_detail     TEXT,                      -- populated on status=paused/failed · cleared on resume
    created_at       TIMESTAMP DEFAULT NOW(),
    updated_at       TIMESTAMP DEFAULT NOW()
);
```

### Useful queries

```sql
-- Current status of all sink connectors
SELECT connector_name, sink_type, status, last_offset, last_record_at,
       batch_latency_ms, failure_count, dlq_record_count, updated_at
FROM pipeline.sink_connector_registry
ORDER BY sink_type, connector_name;

-- Connectors that have not received records in the last hour (possible lag or stall)
SELECT connector_name, sink_type, last_record_at,
       NOW() - last_record_at AS time_since_last_record
FROM pipeline.sink_connector_registry
WHERE status = 'running'
  AND last_record_at < NOW() - INTERVAL '1 hour'
ORDER BY last_record_at;

-- All paused or failed connectors
SELECT connector_name, sink_type, status, error_detail, failure_count, updated_at
FROM pipeline.sink_connector_registry
WHERE status IN ('paused', 'failed')
ORDER BY updated_at DESC;

-- Connectors with slow batch latency (> 10 seconds)
SELECT connector_name, sink_type, batch_latency_ms, last_record_at
FROM pipeline.sink_connector_registry
WHERE batch_latency_ms > 10000
ORDER BY batch_latency_ms DESC;

-- Connectors routing records to DLQ (non-zero DLQ count)
SELECT connector_name, sink_type, dlq_record_count, last_record_at
FROM pipeline.sink_connector_registry
WHERE dlq_record_count > 0
ORDER BY dlq_record_count DESC;
```
