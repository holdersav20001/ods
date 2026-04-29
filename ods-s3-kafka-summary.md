# Publish Pipeline — Plain English Summary

The publish pipeline takes converted data files that have been prepared by the ingestion pipeline and publishes every record in them to a Kafka topic on AWS MSK. Downstream teams then read from those topics in real time. It runs automatically whenever a new file is ready.

---

## What it does

Once the ingestion pipeline has converted a CSV into a Parquet file and placed it in the Curated Zone, the publish pipeline takes over. Its job is to read every row from that file and write each row as a message onto the correct Kafka topic, reliably and exactly once.

---

## How it works

**Triggered automatically**

The moment a file lands in the Curated Zone, AWS fires an event that triggers the publish pipeline. There is no polling and no delay — it starts within seconds of the file arriving.

**Duplicate protection**

The first thing the pipeline does is check whether this file has already been published. If it has — because the event fired twice, or an engineer ran it again manually — it exits immediately without doing any work. This prevents duplicate messages appearing on the Kafka topic.

**Loads the correct configuration**

It loads the configuration for that dataset: which Kafka topic to write to, which fields make up the message key, the schema to validate against, and so on. It locks the exact version of the configuration at this point, so a config change deployed while the job is running cannot affect it mid-flight.

**Validates the schema**

Before publishing anything, the Glue job checks that the file's structure matches the registered schema. If a compatible change is detected — such as a new optional field — it registers the new version automatically and continues. If the change is incompatible — a required field removed or renamed — the entire file is routed to a Dead Letter Queue (DLQ) for an engineer to review, and nothing is published.

**Publishes to Kafka**

Every row in the file is published to the Kafka topic as a message. Each message is given a deterministic key based on the row's identifying fields (for example, a policy number). This means that if the same record is ever published twice, consumers can detect and discard the duplicate.

Publishing uses Kafka transactions, which means either all rows from the file are published or none are. There are no partial publishes.

**Verifies the count**

After publishing, the pipeline counts how many messages were delivered to Kafka and compares it to the number of rows in the source file. If the numbers do not match, the undelivered records are written to the DLQ and an alert fires.

**Updates the audit trail**

Once all rows are confirmed, the pipeline records completion in the database and publishes a summary event to an audit topic, including the record count, schema version, and end-to-end timing.

---

## If something goes wrong

Three things can cause a failure:

- **Schema mismatch** — the file's structure is incompatible with the registered schema. The file goes to the DLQ. Fix the schema, then resubmit.
- **Count mismatch** — fewer messages were confirmed by Kafka than were in the source file. The gap goes to the DLQ. Investigate and replay.

In all cases, the failure is recorded with the reason and an alert is raised. No failure is silent.

---

## What downstream teams see

Downstream teams connect to a Kafka topic such as `ods.insurance.policies` and receive a stream of messages, one per row, in the order they were published. Each message:

- Is in Avro format, validated against a versioned schema
- Has a deterministic key that supports deduplication and compaction
- Carries the business date extracted from the source filename

Teams do not need to know anything about S3, Glue, or the ingestion pipeline. They simply subscribe to their topic and consume.

---

## Key guarantees

- **Exactly-once delivery** — Kafka transactions ensure no partial publishes. Count reconciliation catches any gap.
- **No duplicates** — idempotency guard at entry prevents re-processing the same file. Deterministic message keys allow consumers to deduplicate independently.
- **Schema safety** — no message reaches Kafka without passing schema validation. Breaking changes are blocked before publish.
- **Full audit trail** — every run is logged in the database and an audit event is published, giving a complete history of what was published, when, and how many records.
