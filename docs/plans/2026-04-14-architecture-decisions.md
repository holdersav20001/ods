# ODS Platform — Architecture & Design Decisions
**Date:** 2026-04-14  
**Status:** Living document — updated as decisions are made or revisited  
**Scope:** S3→Kafka publish pipeline + SFTP→S3 ingestion pipeline (patterns 1 & 2 of 4)

---

## 1. Decisions Made

### 1.1 Orchestration Engine — MWAA (Airflow)

**Decision:** Use AWS Managed Workflows for Apache Airflow (MWAA) for all pipeline orchestration.

**Rationale:**
- Maximum observability — each task in a DAG is individually visible, restartable, and retryable in the MWAA UI
- Retry and alerting built in per task
- `SFTPToS3Operator` is a native Airflow operator — no custom code for file transfer
- Consistent across all 4 ingestion patterns
- Managed service — no cluster to maintain

**Trade-offs:**
- MWAA workers can become a bottleneck at high concurrency — DAG runs queue behind available workers
- MWAA startup latency (cold DAG trigger) is typically 5–15 seconds — acceptable for batch but not sub-second
- Cost: MWAA environment charges regardless of load (minimum ~$0.50/hr base)
- Debugging Airflow task failures requires navigating the MWAA log UI — less ergonomic than Lambda CloudWatch logs

**Alternative considered:** AWS Step Functions — rejected because it lacks the observability and operator ecosystem of Airflow, and Airflow's retry/alerting primitives are superior for data pipelines.

---

### 1.2 Pipeline Trigger — Where EventBridge is Used and Where It Is Not

The platform uses EventBridge only where AWS can emit a native S3 Object Created event. It does not use EventBridge where the event source is outside AWS. Airflow DAGs are always the orchestrator — EventBridge is only the trigger mechanism.

---

**SFTP detection (DAG 1) — Airflow SFTP Sensor, not EventBridge**

EventBridge cannot detect events on an external SFTP server. DAG 1 uses an Airflow SFTP Sensor that polls the SFTP server every 5 minutes. This is an unavoidable polling pattern for any non-AWS source.

The sensor occupies one MWAA worker slot per dataset while waiting. At low dataset counts this is acceptable. If the number of SFTP-sourced datasets grows significantly, consider moving to a push-based SFTP notification mechanism (e.g. SFTP server sends an S3 notification on drop) to eliminate the sensor.

**Alternative considered — Airflow S3KeySensor instead of EventBridge for S3 triggers:**

Rather than EventBridge, an Airflow S3KeySensor could poll S3 for new files every N seconds. This is simpler (one fewer service) but occupies a worker slot continuously while waiting. The decision criteria:

| | S3KeySensor | EventBridge |
|---|---|---|
| Files arrive on predictable schedule | ✅ Suitable | ✅ Suitable |
| Files arrive at unpredictable times | ⚠️ Wastes worker slots | ✅ Better |
| Few datasets | ✅ Suitable | ✅ Suitable |
| Many datasets simultaneously waiting | ❌ Worker slot bottleneck | ✅ Better |
| Fewer moving parts | ✅ Yes | ❌ Extra service to configure |
| Sub-minute latency required | ❌ | ✅ Millisecond trigger |

**Decision: use EventBridge for all S3-triggered DAG runs.** At any realistic dataset count, the worker slot cost of the sensor outweighs the configuration overhead of EventBridge.

---

**Two EventBridge rules in the platform**

| Rule | Fires on | Triggers | Pipeline |
|---|---|---|---|
| `ods-raw-file-rule-{env}` | Object Created in `ods-raw-{env}` | DAG 2 (ETL) | Ingestion |
| `ods-curated-file-rule-{env}` | Object Created in `ods-curated-{env}` | Publish DAG | S3→Kafka |

These two rules are the join points between the three pipeline stages. DAG 1 (Transfer) → S3 Raw → EventBridge → DAG 2 (ETL) → S3 Curated → EventBridge → Publish DAG.

**The Kafka Sink pipeline has no EventBridge rule.** MSK Connect connectors poll Kafka continuously — there is no file event and no DAG involved.

---

**Trade-offs (EventBridge)**
- S3 events can be delivered more than once — idempotency guard at DAG entry is mandatory (see 1.5)
- EventBridge rule misconfiguration (wrong prefix, wrong bucket) silently fails to trigger — a CloudWatch alarm on DAG trigger count is essential
- No native backpressure — if 500 files land simultaneously, 500 DAG runs are triggered; MWAA worker capacity becomes the constraint

---

### 1.3 Schema Registry — AWS Glue Schema Registry (Patterns 1, 3, 4)

**Decision:** Use AWS Glue Schema Registry for Patterns 1, 3, and 4. CDC (Pattern 2) uses JSON without a schema registry — governed by PostgreSQL DDL instead.

**Rationale:**
- Native integration with AWS Glue jobs and MSK
- No additional infrastructure to deploy or manage
- Compatible with the Avro/JSON schema formats used by MSK consumers
- Schema evolution rules (backward/forward/full) configurable per registry

**Why CDC is excluded from the schema registry:**
- CDC events are sourced from PostgreSQL DDL, not from a platform-owned schema. The database table definition is the authoritative schema.
- CDC messages use the Debezium envelope format (`before`, `after`, `op`, `source`) — a widely understood standard that does not benefit from Avro type enforcement.
- Requiring Avro for CDC would rule out AWS DMS and most other CDC tooling without custom conversion layers.
- Schema governance for CDC is enforced at the PostgreSQL DDL level: a table column change is a breaking change and must go through the schema change process.

**Format summary by pattern:**

| Pattern | Format | Registry |
|---|---|---|
| Pattern 1 — S3 Batch | Avro | Glue Schema Registry |
| Pattern 2 — CDC | JSON (Debezium envelope) | None (PostgreSQL DDL governs) |
| Pattern 3 — API | JSON Schema | Glue Schema Registry |
| Pattern 4 — Event | Avro | Glue Schema Registry |

**Trade-offs:**
- Less mature tooling than Confluent Schema Registry — fewer client libraries, less community documentation
- No GUI schema browser in the console (schema browsing via CLI/API only)
- CDC topics have weaker runtime enforcement — consumer defensiveness is required (null-safe field access, unknown field tolerance)
- Migration to Confluent Schema Registry later would require re-serialising all Kafka topics

**Schema evolution strategy (Patterns 1, 3, 4):**
- Compatible changes (new optional field, type widening) → auto-register new schema version, pipeline continues
- Breaking changes (field removal, rename, type narrowing) → route file to DLQ, raise alarm, engineer resolves

**Schema evolution strategy (Pattern 2 — CDC):**
- PostgreSQL DDL changes on a source table are treated as schema changes
- Column additions → compatible (consumers ignore unknown fields)
- Column removals or renames → breaking — consumer sign-off required before DDL change is applied

---

### 1.4 Data Quality — AWS Glue DQDL

**Decision:** Use AWS Glue Data Quality (DQDL) for all data quality checks — hard block and soft warn severity tiers.

**Rationale:**
- Native Glue integration — no additional infrastructure
- DQDL rules stored in versioned YAML config per dataset — config-driven, not hardcoded
- CloudWatch integration for metric emission on rule failures
- DQ results published to `ods-dq-results-{env}` for audit

**Two severity tiers:**
- **Hard block** — dataset-level (e.g. `RowCount >= 1`) fails the entire job; row-level routes failing rows to DLQ, passing rows continue
- **Soft warn** — emits a CloudWatch metric, row continues to Kafka/Curated

**Trade-offs:**
- DQDL is an AWS proprietary language — not portable if the platform migrates off Glue
- Row-level DQ requires loading all data into memory — large files may require Glue worker scaling
- No native DQ dashboard — monitoring relies on CloudWatch metrics and manual S3 DQ result queries

---

### 1.5 Idempotency — Three Layers

**Decision:** Implement three independent idempotency layers.

| Layer | Mechanism | Failure mode covered |
|---|---|---|
| 1 — File level | PostgreSQL `file_state` / `ingestion_file_state` | Prevents duplicate DAG execution per file path |
| 2 — Job level | Kafka transactions (atomic publish per batch) | Prevents partial publish on Glue crash mid-flight |
| 3 — Consumer level | Deterministic message keys (SHA256 of key fields) | Consumer-side deduplication and safe replay |

**Rationale:** Each layer covers a different class of failure. Layer 1 catches duplicates at the orchestration boundary. Layer 2 prevents incomplete writes to Kafka. Layer 3 is the safety net when layers 1 and 2 are bypassed (e.g. manual resubmission, DLQ replay).

**Trade-offs:**
- SHA256 key generation adds per-record CPU overhead in the Glue job — negligible for typical batch sizes but measurable at very high volumes
- Key field changes are a breaking change requiring consumer coordination — key fields must be immutable once in production
- Kafka transactions introduce ~10–20ms overhead per committed batch — acceptable for batch patterns, not for sub-millisecond streaming

---

### 1.6 State Store — PostgreSQL (not DynamoDB)

**Decision:** Use PostgreSQL (`ods_{env}`) as the pipeline state store for idempotency, file tracking, and job audit log.

**Rationale:**
- Atomic writes with SELECT FOR UPDATE prevent concurrent duplicate processing
- SQL is queryable by engineers without specialist tooling — operational visibility is immediate
- Consistent with the existing platform database footprint
- `glue_job_log` (INSERT-only) and `file_state` / `ingestion_file_state` tables serve different purposes and can be queried together with JOINs

**Trade-offs:**
- PostgreSQL is a single point of failure — if RDS is unavailable, all pipelines stall
- Connection pooling must be managed carefully — Glue jobs + DAGs hitting the same DB concurrently
- RDS scaling (read replicas, storage autoscaling) must be planned for before go-live
- Not serverless — base cost even at zero pipeline load

**Alternative considered:** DynamoDB — rejected because SQL is significantly better for operational queries (`SELECT * FROM glue_job_log WHERE status='failed' AND created_at >= CURRENT_DATE` vs DynamoDB scan/filter expressions). The operational burden is lower with PostgreSQL.

---

### 1.7 DLQ — S3 Bucket

**Decision:** Use S3 (`ods-dlq-{env}`) as the Dead Letter Queue, partitioned by failure type, date, topic/dataset, and run_id.

**Rationale:**
- Simple and durable — no TTL, no message size limits
- Partitioned layout enables Athena queries for investigation and replay scripting
- Consistent with the S3-centric platform architecture
- Replay is an engineer-driven process — S3 is a better fit than a Kafka DLQ topic (which would need its own consumer)

**DLQ partition structure:**
```
ods-dlq-{env}/
  schema-incompatible/date={date}/topic|dataset={name}/
  dq-dataset-failure/date={date}/topic|dataset={name}/
  dq-row-failure/date={date}/topic|dataset={name}/run_id={id}/
  count-mismatch/date={date}/topic|dataset={name}/run_id={id}/
```

**Trade-offs:**
- No native alerting on DLQ growth — CloudWatch S3 metrics or Glue job alarms must be configured explicitly
- No automatic retry from DLQ — replay is a manual engineer action (intentional: root cause must be understood first)
- DLQ files can accumulate silently if alarms are not monitored

---

### 1.8 Config Versioning — Pinned S3 Object Version ID

**Decision:** YAML config files stored in `ods-config-{env}` with S3 versioning enabled. The DAG pins the current S3 object version ID at trigger time and passes it to the Glue job.

**Rationale:**
- Config changes deployed mid-flight cannot affect an in-progress job
- Any config version that produced an output can be reconstructed by version ID
- Version ID is recorded in the audit log for full reproducibility

**Trade-offs:**
- Engineers must remember to use versioned config paths, not `latest` — requires discipline or enforcement via CI
- S3 versioning increases storage cost marginally (old config versions accumulate)
- Config rollback requires an engineer to identify the correct version ID — not automated

---

### 1.9 Audit Trail — Kafka Topic `ods.pipeline.audit`

**Decision:** Publish a structured audit event to `ods.pipeline.audit` for every pipeline run (success and failure). Sink this topic to `ods-audit-sink-{env}` via Kafka Connect S3 Sink Connector.

**Rationale:**
- Event-driven audit trail works uniformly across all 4 ingestion patterns
- Multiple consumers can subscribe — operations team, compliance, downstream alerting
- S3 sink provides long-term retention and Athena queryability without database growth
- Complements (not replaces) `pipeline.glue_job_log` — audit topic is the external-facing summary, job log is the internal execution trail

**Trade-offs:**
- If MSK is unavailable, audit events cannot be emitted — pipeline may complete without an audit record (Kafka publish step must not block pipeline success)
- Audit topic schema must be maintained and versioned alongside the pipeline schema
- Kafka Connect S3 Sink Connector is an operational concern not yet configured (see Section 3)

---

### 1.10 Ingestion DAG Split — DAG 1 (Transfer) + DAG 2 (ETL)

**Decision:** Split the ingestion pipeline into two independent DAGs bridged by EventBridge.

```
DAG 1: SFTP → S3 Raw → [done]
EventBridge: S3 Raw Object Created → triggers DAG 2
DAG 2: S3 Raw → Glue ETL → S3 Curated
```

**Rationale:**
- Transfer and ETL have different failure modes and retry behaviours — separating them avoids unnecessary SFTP re-copies on ETL failure
- Enables the platform pattern to apply uniformly: any source that writes to S3 Raw can trigger the same ETL DAG
- DAG 2 is independently retryable without touching the SFTP

**Trade-offs:**
- Two DAGs per dataset doubles the MWAA DAG count — operational overhead of monitoring two DAG histories per file
- EventBridge is now on the critical path — a misconfigured rule silently breaks the DAG 1 → DAG 2 handoff
- End-to-end tracing across two DAG runs requires correlating `run_id` in the job log

---

### 1.11 Glue Job Log — INSERT-Only Audit Table

**Decision:** Glue jobs write to `pipeline.glue_job_log` via JDBC at each status transition. The table is INSERT-only — rows are never updated.

**Status sequences:**
- Publish: `started → schema_validated → dq_passed|dq_warned → publishing → completed|failed`
- Ingestion: `started → schema_validated → dq_passed|dq_warned → converting → completed|failed`

**Rationale:**
- INSERT-only gives a complete execution timeline — every transition is recorded, not just the final state
- Enables time-to-complete analysis per stage (schema validation latency, DQ latency, publish latency)
- `BIGSERIAL` primary key gives a human-readable, chronologically ordered ID for operational use
- `business_date` extracted from filename per YAML pattern — enables cross-run queries by business date regardless of processing date

**Trade-offs:**
- Table grows indefinitely — a retention/archival policy must be defined (not yet decided, see Section 3)
- Glue→PostgreSQL writes add latency to each status transition (JDBC round-trips)
- JDBC connection `ods-postgres-{env}` is shared across all Glue jobs — connection pool sizing matters at scale

---

## 2. Architecture: Strengths and Weaknesses

### 2.1 Strengths

**Event-driven end-to-end**  
The entire pipeline chain is triggered by events (S3 Object Created → EventBridge → DAG). There is no polling anywhere in the critical path. Latency from file landing to Kafka publish is bounded by Glue job execution time, not poll interval.

**Deep operational visibility**  
Every file has a trace: `ingestion_file_state` / `file_state` (coarse), `glue_job_log` (fine-grained, every status transition), `ods.pipeline.audit` (summary event), CloudWatch metrics (dimensional). Engineers can reconstruct exactly what happened to any file without reading logs.

**Multi-layer idempotency**  
Three independent safety layers mean duplicate processing is extremely unlikely to cause duplicate data in Kafka. Each layer is independently testable and independently effective.

**Config-driven, not code-driven**  
Adding a new dataset requires a new YAML config file and a catalogue entry — no new Glue job code. Schema, DQ rules, key fields, topic name, and paths are all config. This significantly lowers the cost of onboarding new datasets.

**Shared platform components**  
Both ingestion and publish pipelines share: PostgreSQL, Schema Registry, Glue Catalog, Crawlers, EventBridge curated rule, DLQ, audit topic, CloudWatch namespace. Adding a third pattern (CDC, API) reuses the same shared layer.

**Permanent raw archive**  
`ods-raw-{env}` retains every CSV file ever ingested. Full replay is always possible from the source — a corrupt or incorrectly processed file can be re-run without coordinating with the upstream SFTP provider.

---

### 2.2 Weaknesses

**MWAA is not serverless**  
MWAA runs continuously regardless of pipeline load. At low throughput, cost per file is high. At high concurrency, worker capacity becomes a hard ceiling — files queue waiting for available workers.

**Glue cold start latency**  
AWS Glue has a startup time of 2–4 minutes for new job runs. For datasets that expect near-real-time processing, this is a significant constraint. Glue streaming (continuous mode) would eliminate this but is architecturally different and not currently planned.

**PostgreSQL as a single point of failure**  
If the RDS instance is unavailable, all pipeline state writes fail and all idempotency checks fail. The entire platform stalls. Multi-AZ RDS mitigates availability risk but does not eliminate it, and failover takes 30–120 seconds during which writes fail.

**No backpressure mechanism**  
If 500 files land in S3 simultaneously, EventBridge fires 500 times and 500 DAG runs are triggered. MWAA workers queue, Glue jobs queue. There is no flow control between the event layer and the execution layer. Burst capacity planning is required.

**Manual DLQ replay**  
DLQ replay is a fully manual process. Engineers must identify the DLQ location, fix root cause, write the fixed file back to the source, and reset PostgreSQL state. There is no automated retry or replay tooling. This will become a bottleneck if DLQ volume is high.

**Ingestion split observability gap**  
A file that fails in DAG 2 leaves DAG 1 with `status=transferred` and DAG 2 with `status=failed`. Correlating these across two DAG histories requires searching by `run_id` — there is no single "end-to-end pipeline status" view for an ingestion file.

**Key field immutability constraint**  
Deterministic message keys depend on key fields defined in YAML config. Changing key fields after go-live is a breaking change that requires coordinating with every Kafka consumer. This is a governance risk if domain teams do not treat key fields with the same rigour as schema fields.

---

## 3. Open Items and Gaps

### 3.1 Infrastructure

| Item | Detail | Owner | Status |
|---|---|---|---|
| SFTP → MWAA network connectivity | MWAA must reach the internal SFTP server. VPC peering, PrivateLink, or transit gateway required | Infrastructure | **Unresolved** |
| RDS sizing and Multi-AZ | PostgreSQL instance sizing, connection pool limits, Multi-AZ configuration for HA | Infrastructure | Not started |
| MSK cluster sizing | Partition count, replication factor, retention, broker sizing | Infrastructure | Not started |
| MWAA worker sizing | Worker count, CPU/memory, max active DAG runs | Infrastructure | Not started |
| IAM roles | Glue execution role, MWAA execution role, EventBridge target role — least-privilege | Security | Not started |
| Secrets management | SFTP credentials, RDS credentials — SSM Parameter Store or Secrets Manager | Security | Not started |
| Kafka Connect S3 Sink Connector | Configuration for draining `ods.pipeline.audit` to `ods-audit-sink-{env}` | Platform | Not started |

---

### 3.2 Design Decisions Not Yet Made

| Decision | Options | Notes |
|---|---|---|
| `glue_job_log` retention policy | Archive to S3 after N days, partition pruning, pg_partman | Table grows indefinitely — must be addressed before go-live |
| DLQ monitoring threshold | Alert when DLQ exceeds N records/bytes | What constitutes a DLQ emergency? |
| Glue job concurrency limit | Max concurrent Glue jobs per dataset, per environment | Without a limit, a burst of files can exhaust Glue DPU quota |
| MWAA DAG concurrency | Max concurrent DAG runs, task parallelism | Needs load-testing to set correctly |
| Schema Registry compatibility mode | Backward / Forward / Full per topic | Currently unspecified — different datasets may need different modes |
| Business date extraction failure | What happens if filename does not match the configured pattern? | Currently undefined — job would fail at filename parsing with no specific error path |
| Config deployment process | How are YAML configs promoted dev → staging → prod? | No CI/CD for config changes defined yet |
| Parquet partitioning depth | `date={date}/dataset={dataset}/` — is file-per-run or append-within-partition intended? | Affects Glue Crawler behaviour and Athena query performance |
| Multi-file batching | What if the upstream sends 5 files per day per dataset? Are they independent runs? | Currently assumed independent — confirm with upstream teams |
| End-to-end SLOs | What is the acceptable latency from file landing on SFTP to records in Kafka? | No SLO defined |

---

### 3.3 Patterns Not Yet Designed

| Pattern | Status | Notes |
|---|---|---|
| CDC → Kafka | In design | Technology evaluated — see `2026-04-16-cdc-technology-evaluation.md`. Format: JSON (Debezium envelope), no schema registry. Recommended connector: MSK Connect + Debezium. Decision D-CDC open. |
| API → Kafka | Not started | Polling or webhook-driven. Rate limiting, pagination, and authentication patterns needed |
| Event → Kafka | Not started | Source likely SNS/SQS or EventBridge — routing and fan-out design needed |

These three patterns are explicitly out of scope for current design but must reuse the shared layer components defined in the S3→Kafka template.

---

## 4. SRE Considerations

### 4.1 SLOs (Not Yet Defined)

No Service Level Objectives have been defined for this platform. The following are recommended starting points:

| SLO | Suggested target | Notes |
|---|---|---|
| File-to-Kafka latency (p95) | < 10 minutes from file landing in S3 Curated | Dominated by Glue startup (~3 min) + job execution |
| File-to-Curated latency (p95) | < 15 minutes from file landing in S3 Raw | Includes Glue ETL startup + conversion |
| Pipeline success rate | > 99.5% of files process without manual intervention | DLQ items count as failures |
| DLQ drain time | < 4 hours from alarm to reprocessing | SRE response time target |

---

### 4.2 On-Call Runbooks Needed

The failure/recovery documents (`2026-04-14-*-failure-and-recovery.md`) are technical references. The following operational runbooks have not yet been written:

- **Runbook: MWAA worker saturation** — how to identify, escalate, and recover when all workers are occupied
- **Runbook: RDS unavailable** — what fails, how pipelines recover when RDS comes back, manual steps
- **Runbook: MSK broker unavailable** — Kafka transaction behaviour, how to identify partially published batches
- **Runbook: Glue DPU quota exhaustion** — what happens when Glue cannot start new jobs, how to triage queued runs
- **Runbook: DLQ growth alarm** — how to triage, prioritise, and replay DLQ records at scale

---

### 4.3 Observability Gaps

**What is covered:**
- Per-alarm CloudWatch alarms for all known failure modes
- Structured audit events in `ods.pipeline.audit`
- Fine-grained job execution log in `pipeline.glue_job_log`
- CloudWatch log groups for Airflow and Glue

**What is missing:**

| Gap | Impact | Recommendation |
|---|---|---|
| End-to-end pipeline latency metric | Cannot track SLO without it | Emit `pipeline.e2e.latency_ms` from DAG using EventBridge trigger timestamp vs completion timestamp |
| Kafka consumer lag monitoring | No visibility into how far behind consumers are | Add MSK consumer lag metrics to CloudWatch dashboard |
| DLQ record count metric | DLQ growth not visible without S3 inventory scan | Glue job should emit `dlq.records.written` metric on each DLQ write |
| MWAA queue depth | No visibility into DAG run backlog | MWAA exposes `QueuedTasks` and `RunningTasks` CloudWatch metrics — add to dashboard |
| PostgreSQL connection pool utilisation | Risk of connection exhaustion at scale | RDS Performance Insights or `pg_stat_activity` polling |
| EventBridge rule invocation failures | Silent failures if rule is misconfigured | Enable EventBridge rule invocation metrics and alert on zero invocations during business hours |
| Cross-DAG tracing (ingestion) | Cannot link DAG 1 and DAG 2 runs for a single file in MWAA UI | Correlate via `run_id` in `glue_job_log` — consider adding `run_id` to DAG 2 trigger payload |

---

### 4.4 Failure Mode Analysis

| Component | Failure | Detection | Impact | Recovery |
|---|---|---|---|---|
| MWAA | All workers occupied | CloudWatch `QueuedTasks` alarm | Files pile up unprocessed, latency grows | Scale MWAA workers; files will process when workers free |
| MWAA | DAG deployment failure | MWAA import error alarm | All DAG runs for affected DAG fail to start | Fix DAG code, redeploy |
| EventBridge | Rule misconfigured | Zero invocation alarm | DAG never triggered — files silently unprocessed | Fix rule, manually trigger backfill |
| Glue | DPU quota exhausted | `GlueJobQueuedJobsExceeded` CloudWatch | Jobs queue and stall — latency grows | Request quota increase; existing queued jobs run when capacity frees |
| PostgreSQL | RDS unavailable | RDS CloudWatch `DatabaseConnections=0` | All pipelines stall at idempotency check | Restore RDS; pipelines auto-recover on retry |
| MSK | Broker unavailable | MSK broker health alarm | Kafka transactions abort; no partial publish | Glue retries transaction; broker recovery unblocks publish |
| S3 | Bucket unavailable (rare) | S3 error rate alarm | Read/write failures in Glue | S3 is 11-nines durable — likely a permissions issue, not outage |
| Schema Registry | Unavailable | Glue job fails at schema fetch | All jobs fail schema validation | Glue retries; Schema Registry is a managed service |

---

### 4.5 Capacity Planning Gaps

The following capacity decisions have not been made and must be addressed before production load:

- **Glue DPU sizing per job** — currently unspecified. Depends on file sizes and row counts per dataset.
- **Glue job concurrency** — how many simultaneous Glue jobs can run before DPU quota is hit?
- **MWAA worker count** — how many concurrent DAG runs are expected at peak? One file arrival per dataset per day is very different from 100 files per dataset per hour.
- **MSK partition count** — too few partitions limits consumer parallelism; too many wastes broker resources. Partition count per topic not yet decided.
- **PostgreSQL IOPS** — `glue_job_log` gets multiple INSERTs per Glue job run. At 100 concurrent jobs, that is ~500–800 INSERTs/second. RDS provisioned IOPS or Aurora may be needed.
- **S3 DLQ and audit sink storage** — no retention or lifecycle policy defined. Unbounded growth if not addressed.

---

## 5. Security Considerations

### 5.1 Decided

- All data at rest in S3 encrypted with SSE-S3 or SSE-KMS (standard AWS default)
- MSK in-transit encryption enabled (TLS)
- PostgreSQL TLS connection from Glue via JDBC

### 5.2 Not Yet Decided / Gaps

| Item | Risk | Recommendation |
|---|---|---|
| SFTP credentials storage | Plaintext credentials in MWAA connection = high risk | Use AWS Secrets Manager; inject into MWAA connection at runtime |
| IAM least-privilege for Glue | Overly broad Glue execution role can access all S3 buckets | Define per-job IAM role with resource-specific S3 permissions |
| IAM least-privilege for MWAA | MWAA execution role must not have write access to production data | Separate execution roles per environment |
| KMS key management | Who owns the KMS keys for S3 buckets? Key rotation policy? | Define key ownership and rotation schedule |
| VPC isolation | Are Glue jobs, MWAA, RDS, and MSK in the same VPC? Are subnets private? | Network topology not yet documented |
| Data classification | Are any datasets PII/sensitive? GDPR implications for raw archive retention? | Dataset classification not done — required before go-live |
| Audit log access control | Who can read `pipeline.glue_job_log`? Does it contain sensitive data? | Apply row-level security or separate read-only role |

---

## 6. Testing Strategy (Not Yet Defined)

No testing strategy has been defined. The following test levels are recommended:

| Test type | What to test | Tooling |
|---|---|---|
| Unit | YAML config parsing, filename date extraction, message key generation | pytest |
| Integration | Glue job end-to-end against a test S3 bucket and test MSK cluster | AWS Glue local testing / moto |
| Contract | Schema evolution rules — assert compatible changes pass, breaking changes fail | Glue Schema Registry API |
| DQ rule validation | Each DQDL rule fires correctly on known-good and known-bad data | Glue Data Quality local evaluation |
| Idempotency | Same file submitted twice produces one Kafka publish | Integration test |
| Failure injection | Force schema failure, DQ failure, count mismatch — verify DLQ write and alarm | Manual or localstack |
| Load / capacity | Burst of N files simultaneously — measure MWAA queue depth and Glue throughput | k6 / custom S3 event generator |

---

## 7. Cost Model (Not Yet Analysed)

No cost estimate has been produced. Key cost drivers:

| Component | Cost driver | Notes |
|---|---|---|
| MWAA | Environment-hours (fixed) + worker-hours (variable) | Minimum ~$400/month for a small environment |
| Glue | DPU-hours per job run | 2 DPUs minimum per job × ~3 min startup = ~0.1 DPU-hour per run |
| MSK | Broker-hours (fixed) + data transfer | Minimum 3-broker cluster, broker size TBD |
| RDS PostgreSQL | Instance-hours + storage | Multi-AZ adds ~2× instance cost |
| S3 | Storage + PUT/GET requests | Raw archive is permanent — storage grows indefinitely |
| EventBridge | Events published (very low cost) | Negligible |
| CloudWatch | Metrics, logs, alarms | Logs ingestion can be significant at high Glue/Airflow verbosity |

A detailed cost model should be produced once Glue job sizes, file volumes, and MWAA worker counts are known.

---

## 8. Document History

| Date | Change |
|---|---|
| 2026-04-14 | Initial version covering S3→Kafka and SFTP ingestion pipelines |
