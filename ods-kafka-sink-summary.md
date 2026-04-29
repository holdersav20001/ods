# Kafka Sink Pipeline — Plain English Summary

The Kafka sink pipeline reads records from a canonical Kafka topic and writes them into two destinations simultaneously: a long-term queryable Iceberg table stored on S3, and a PostgreSQL table for fast operational lookups. It runs continuously, processing records as they arrive.

---

## What it does

Once data has been published to a Kafka topic by one of the ingestion patterns (S3 batch, CDC, API, or Event), any number of downstream systems can consume it. The Kafka sink pipeline is the platform's own consumer — it takes those records and lands them in persistent storage so that teams can query historical data via Athena and operational systems can look up individual records via SQL.

---

## The two destinations

**Iceberg on S3 — the long-term store**

Apache Iceberg is a table format that sits on top of S3. Records are stored as Parquet files, organised by date and dataset, and registered in the AWS Glue Data Catalog. This makes the data queryable via Athena or any Glue-compatible tool without any additional infrastructure. The Glue Crawler keeps the table's partition metadata up to date as new data arrives.

Iceberg provides ACID guarantees — each batch of records is committed as an atomic snapshot. Readers always see a complete, consistent view of the data, never a partial write. If the pipeline fails mid-write, any partially written files are invisible to readers and are cleaned up automatically.

**PostgreSQL — the operational store**

PostgreSQL provides fast row-level access for operational systems that need to look up individual records by key, run transactional queries, or join data across tables. It is not suitable for full-table scans or analytical queries at scale — that is what Iceberg is for.

The two destinations serve different purposes and are written to independently.

---

## How it works

Two Kafka Connect connectors run on the AWS managed MSK Connect service. Each connector reads from the same Kafka topic independently, using its own consumer group, so they do not interfere with each other and can progress at different rates.

Before writing anything, each connector reads the Avro schema for each record from the Glue Schema Registry. Schemas are cached in memory, so the registry is only called the first time a new schema version is seen. This ensures the data is deserialised correctly and any incompatible schema changes are caught before they reach the destination.

The Iceberg connector collects records into a buffer and flushes them as Parquet files to S3 when either a size or time threshold is reached. Once the files are written, Iceberg atomically commits a snapshot — a single metadata update that makes all new records visible to readers simultaneously. The Glue Data Catalog is then updated and the Glue Crawler is triggered to refresh partition metadata. Only after the snapshot is confirmed does the connector commit its position in the Kafka topic — this ordering ensures that if the connector restarts, it re-reads and safely re-writes the same records rather than losing them.

The JDBC connector writes records directly to PostgreSQL in batches. The write mode — whether records are inserted fresh or upserted against an existing row — is a configuration decision made per dataset. Offset commit to Kafka happens after PostgreSQL confirms the write.

---

## If something goes wrong

Two categories of failure, with different behaviour:

**Schema incompatible record** — a single record whose structure does not match the destination table is routed to the Dead Letter Queue (DLQ). The remaining records in the batch continue. The connector does not pause. The incompatible record can be replayed from the DLQ once the schema issue is resolved.

**Write failure** — if S3 or PostgreSQL cannot be written to, the entire batch is routed to the DLQ and the connector pauses. A paused connector requires an engineer to investigate and manually resume. No records are silently lost.

Every batch completion is recorded in two places:
- The **MSK audit topic** (`ods.pipeline.audit`) — an event stream providing end-to-end traceability, consistent with all other ODS pipelines
- The **connector registry table** in PostgreSQL (`pipeline.sink_connector_registry`) — a per-connector status record showing current state, last processed offset, batch latency, and failure counts

---

## Key guarantees

- **At-least-once delivery with safe reprocessing** — offset commit happens after the Iceberg snapshot is confirmed and after PostgreSQL confirms the write. A connector restart re-reads from the last committed offset; Iceberg's snapshot mechanism and PostgreSQL upsert mode ensure reprocessing is safe and does not corrupt data.
- **Independent sinks** — the two connectors progress independently. A failure in one does not affect the other.
- **Schema safety** — records are validated against the Glue Schema Registry before reaching either destination. Incompatible records are quarantined in the DLQ, not silently dropped or incorrectly written.
- **Resumable** — both connectors track their position in the Kafka topic. A restart does not lose data — processing resumes from the last committed offset.
- **Atomic Iceberg writes** — a partially written batch is never visible to readers. Only committed snapshots are queryable.
