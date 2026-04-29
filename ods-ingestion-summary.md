# Ingestion Pipeline — Plain English Summary

The ingestion pipeline moves data files from an internal SFTP server into AWS, cleans and converts them, and hands them off for publishing to Kafka. It runs automatically whenever a new file appears.

---

## What it does

A source system — for example, an actuarial team — drops a CSV file onto a shared SFTP server on a regular schedule. The ingestion pipeline detects that file, copies it safely into AWS, converts it into a more efficient format, and stores it ready for the next stage.

The pipeline is split into two independent parts.

---

## Part 1 — Detect and Transfer

The pipeline checks the SFTP server every five minutes for new files.

When a file is found, the first thing it does is check whether the file is expected. It looks the filename up against an approved list stored in a database. If the file is not on the list — wrong name, wrong path, or something unexpected — it is moved to a quarantine area and an alert is raised. Nothing further happens until an engineer investigates.

If the file is approved, it checks whether it has already been processed. This prevents the same file being processed twice if the sensor fires again or a retry is triggered.

Once both checks pass, it copies the file from the SFTP server into an S3 bucket called the **Raw Zone**. Every file that ever passes through the platform is kept here permanently. This means the platform can always replay historical data without going back to the source system.

After copying, it verifies the file arrived intact by comparing a checksum of the original against the copy. If they do not match, the corrupted copy is quarantined and an alert fires.

Part 1 finishes here. The file is now safely in AWS.

---

## Part 2 — Transform

The moment a file lands in the Raw Zone, AWS automatically notifies Part 2. The two parts are independent — if the transformation fails and needs retrying, it does not touch the SFTP transfer.

Part 2 picks up the configuration for that dataset — things like the expected column structure, file naming pattern, and output location — and passes it to an AWS Glue job to do the actual work.

The Glue job does two things:

**Schema validation** — it checks that the file's columns match what is expected. If a compatible change is detected (such as a new optional column), it registers the updated structure automatically. If the change is incompatible — a column removed, a type changed — the file is routed to a Dead Letter Queue (DLQ) for an engineer to review, and the pipeline stops for that file.

**Conversion** — it converts the CSV into Parquet format. Parquet is a compressed, column-oriented format that is far more efficient for downstream processing and querying than CSV. The converted file is written to a second S3 bucket called the **Curated Zone**, organised by date and dataset.

Once written, the pipeline verifies the number of rows in the output matches the number it read from the input. A mismatch means something was lost in conversion and triggers an alert.

---

## What happens next

The moment a converted file lands in the Curated Zone, it automatically triggers the **Publish Pipeline** (see `ods-s3-kafka-summary.md`), which takes it the rest of the way to Kafka. The two pipelines are joined at the Curated Zone — no manual handoff is needed.

---

## If something goes wrong

Every file is tracked through each stage in a database table. If a file fails at any point — quarantined, checksum mismatch, schema problem, conversion error — its status is recorded with the reason. Engineers can query this table to see exactly what happened to any file and at which stage.

Failed files are not lost. They are routed to the DLQ with full metadata. Once the root cause is fixed, the file can be resubmitted.

---

## Key guarantees

- **No duplicates** — a file that has already been processed is skipped automatically, even if the pipeline is triggered again for the same file.
- **No data loss** — the Raw Zone keeps every original file forever. A replay from any point in history is always possible.
- **No unexpected files** — only files on the approved list are processed. Everything else is quarantined.
- **No silent failures** — every failure writes a record and raises an alert.
