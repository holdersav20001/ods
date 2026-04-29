# ODS Platform — Kafka Topic Catalogue

**Date:** 2026-04-16
**Status:** Approved
**Scope:** All Kafka topics in the `ods.*` namespace on AWS MSK

> This is the single authoritative reference for all Kafka topics on the ODS platform. Any new topic must be added here when it is created. If a topic exists in MSK that is not listed here, raise a ticket in `ODS-SUPPORT` — it is either undocumented or should not exist.

---

## Topic Naming Convention

All business data topics follow this pattern:

```
ods.{domain}.{dataset}
```

| Segment | Description | Rules |
|---|---|---|
| `ods` | Literal prefix — identifies all ODS platform topics | Fixed; never change |
| `{domain}` | Business domain (e.g. `insurance`, `motor`, `finance`, `customer`) | Lowercase, no hyphens, no dots |
| `{dataset}` | Dataset name within the domain (e.g. `policies`, `claims`, `premiums`) | Lowercase, no hyphens, no dots |

**Examples:**

```
ods.insurance.policies
ods.insurance.claims
ods.motor.vehicles
ods.finance.premiums
ods.customer.profiles
```

Platform-internal topics follow the same prefix with `pipeline` as the domain:

```
ods.pipeline.audit
ods.pipeline.reconciliation
```

---

## Topic Catalogue

### Business Data Topics

One topic exists per dataset. The table below shows the pattern and the known active and planned topics. The example row uses `ods.insurance.policies` as the reference implementation.

| Topic | Status | Cleanup policy | Retention (`retention.ms`) | Partitions | Schema subject | Producers | Consumers | Notes |
|---|---|---|---|---|---|---|---|---|
| `ods.insurance.policies` | Active | `compact,delete` | 604800000 (7 days) | TBD | `insurance-policies` | Publish pipeline (`ods-s3-publish-policies` Glue job) | Open to registered teams | Reference topic; owner: insurance-data-eng |
| `ods.insurance.claims` | Active | `compact,delete` | 604800000 (7 days) | TBD | `insurance-claims` | Publish pipeline | Open to registered teams | Owner: insurance-data-eng |
| `ods.motor.vehicles` | Active | `compact,delete` | 604800000 (7 days) | TBD | `motor-vehicles` | Publish pipeline | Open to registered teams | Owner: motor-data-eng |
| `ods.motor.drivers` | Planned | `compact,delete` | 604800000 (7 days) | TBD | `motor-drivers` | CDC pipeline (not yet implemented) | Open to registered teams | Owner: motor-data-eng |
| `ods.finance.transactions` | Planned | `compact,delete` | 604800000 (7 days) | TBD | `finance-transactions` | API pipeline (not yet implemented) | Open to registered teams | Owner: finance-data-eng |
| `ods.finance.premiums` | Planned | `compact,delete` | 604800000 (7 days) | TBD | `finance-premiums` | Publish pipeline | Open to registered teams | Owner: finance-data-eng |
| `ods.customer.profiles` | Planned | `compact,delete` | 604800000 (7 days) | TBD | `customer-profiles` | CDC pipeline (not yet implemented) | Open to registered teams | Owner: customer-data-eng |
| `ods.customer.interactions` | Planned | `compact,delete` | 604800000 (7 days) | TBD | `customer-interactions` | Event pipeline (not yet implemented) | Open to registered teams | Owner: customer-data-eng |

**Business data topic Kafka config (full):**

```properties
cleanup.policy=compact,delete
retention.ms=604800000          # 7 days
retention.bytes=-1              # no size limit; time governs
min.compaction.lag.ms=432000000 # 5 days — prevents tombstone compaction before consumers process it
delete.retention.ms=86400000    # tombstone visible to consumers for 1 day after compaction eligibility
segment.ms=86400000             # roll segments daily to allow timely deletion
```

See `2026-04-15-data-retention.md §4.3` for the full rationale including the `min.compaction.lag.ms` derivation.

---

### CDC Raw & Staging Topics

These topics are staging/transit topics produced by CDC capture systems (OpenFlow, Debezium) and consumed exclusively by the ECS Kafka Streams canonicalization service. They are **not available for general consumption** and do not carry canonical Avro schemas.

Three topic patterns exist per CDC dataset:

| Pattern | Purpose | Producer | Consumer | Notes |
|---|---|---|---|---|
| `ods.raw.{domain}.{dataset}` | Raw CDC events — non-canonical JSON envelope | CDC capture (OpenFlow · Debezium) | ECS Kafka Streams canonicalization service only | Not registered in Schema Registry. delete cleanup policy. Short retention (24h). See naming note below. |
| `ods.raw.{dataset}.changelog` | Kafka Streams RocksDB state replication | ECS Kafka Streams (managed by Kafka Streams API) | ECS Kafka Streams (on restart — state restoration) | Auto-managed by Kafka Streams. Do not produce or consume manually. compact cleanup policy. |
| `ods.raw.{dataset}.quarantine` | Replayable quarantine — DQ hard-blocks and schema-incompatible records | ECS Kafka Streams canonicalization service | Replay service (future) · Platform team only | Preserves original raw CDC payload + failure context for replay after config/schema fix. delete cleanup policy. 30-day retention. |

**CDC raw topic Kafka config (`ods.raw.{domain}.{dataset}`):**

```properties
cleanup.policy=delete
retention.ms=86400000           # 24 hours — transit topic; canonical topic is the durable record
retention.bytes=-1
segment.ms=3600000              # roll segments hourly for timely deletion
```

**Changelog topic Kafka config (`ods.raw.{dataset}.changelog`):**

```properties
cleanup.policy=compact
retention.ms=-1                 # indefinite — needed for full state restoration
min.compaction.lag.ms=0         # compact aggressively; latest state wins
```

**Quarantine topic Kafka config (`ods.raw.{dataset}.quarantine`):**

```properties
cleanup.policy=delete
retention.ms=2592000000         # 30 days — allows time for config/schema fixes and replay
retention.bytes=-1
segment.ms=86400000
```

> **Naming note — `ods.raw.{domain}.{dataset}` vs `ods.raw.{dataset}.changelog`:** The raw data topic retains the full `{domain}.{dataset}` path (consistent with the canonical topic `ods.{domain}.{dataset}` it feeds). The changelog and quarantine topics omit the domain because they are dataset-scoped internal artefacts — including the domain would make the name unnecessarily long and is not required for routing.

> **`ods.raw.{domain}.{dataset}` is not in Schema Registry.** The CDC envelope schema is source-system-specific (Debezium CDC format or OpenFlow equivalent). Only the downstream canonical topic `ods.{domain}.{dataset}` has a registered Avro schema.

**Consumer group for the canonicalization service:**

```
ods-cdc-canonicalize-{dataset}
```

This consumer group is owned by the platform team and is not available for external registration.

---

### Platform-Internal Topics

These topics are owned by the platform team and are not available for general consumption without explicit approval.

| Topic | Status | Cleanup policy | Retention (`retention.ms`) | Partitions | Schema subject | Producers | Consumers | Notes |
|---|---|---|---|---|---|---|---|---|
| `ods.pipeline.audit` | Active | `delete` | 7776000000 (90 days) | TBD | `pipeline-audit` | All four ingestion pipelines (S3 batch, CDC, API, Event) | Platform team; audit tooling only | JSON payload — no Avro schema enforced. Sinked to `ods-audit-sink-{env}` for 6-year S3 retention. See audit schema below. |
| `ods.pipeline.reconciliation` | Active | `delete` | 2592000000 (30 days) | TBD | `pipeline-reconciliation` | Reconciliation job (Glue/Lambda, MWAA cron); future: consumer-side sequence gap detectors | Platform team; reconciliation dashboard; future automated remediation | Structured reconciliation events. See `2026-04-15-reconciliation-design.md §8`. |

**`ods.pipeline.audit` Kafka config:**

```properties
cleanup.policy=delete
retention.ms=7776000000         # 90 days in Kafka; long-term in S3 audit sink
retention.bytes=-1
segment.ms=86400000
```

**`ods.pipeline.audit` message schema (JSON):**

```json
{
  "run_id": "uuid",
  "source_type": "s3 | sftp | cdc | api | event",
  "source_ref": "s3://ods-curated-prod/insurance/policies/file.parquet",
  "target_topic": "ods.insurance.policies",
  "schema_version": "3",
  "record_count": 10000,
  "status": "success | failed | dlq | quarantine",
  "reason": "schema_incompatible | dq_hard_block | count_mismatch | not_approved | checksum_mismatch | null",
  "timestamp": "2026-04-14T13:00:00Z"
}
```

**`ods.pipeline.reconciliation` Kafka config:**

```properties
cleanup.policy=delete
retention.ms=2592000000         # 30 days
retention.bytes=-1
segment.ms=86400000
```

---

### Non-Kafka Queues Referenced for Completeness

The following are SQS queues, not Kafka topics. They are listed here because they appear in platform diagrams and are referenced in operational runbooks alongside the Kafka topics above.

| Queue | Type | Purpose | Owner |
|---|---|---|---|
| EventBridge DLQ for `ods-curated-file-rule-{env}` | SQS | Catches EventBridge delivery failures when the Kafka publish pipeline DAG cannot be triggered | Platform team |
| EventBridge DLQ for `ods-raw-file-rule-{env}` | SQS | Catches EventBridge delivery failures when the ingestion ETL DAG (DAG 2) cannot be triggered | Platform team |

If an EventBridge rule fires but the target MWAA DAG is unavailable, the event lands in the corresponding SQS DLQ. Check these queues when DAG trigger failures are suspected but no Kafka topic activity is involved.

---

## Topic Creation Command

Use this exact command to create a new business data topic. Do not use the command from the dataset-onboarding documentation — it incorrectly specifies `delete` instead of `compact,delete` and omits the compaction lag settings.

```bash
kafka-topics.sh --create \
  --bootstrap-server $MSK_BOOTSTRAP \
  --topic ods.{domain}.{dataset} \
  --partitions {N} \
  --config cleanup.policy=compact,delete \
  --config retention.ms=604800000 \
  --config min.compaction.lag.ms=432000000 \
  --config delete.retention.ms=86400000
```

Replace `{domain}`, `{dataset}`, and `{N}` with the actual values. The partition count `{N}` must be agreed with the platform team before creation — partition count cannot be decreased after a topic goes live.

After creating the topic:

1. Register it in this catalogue.
2. Register the schema subject in `ods-schema-registry-{env}` (see schema naming below).
3. Add the dataset YAML config to `ods-config-{env}` with owner and key fields defined.
4. Add a row to `pipeline.schema_governance` in PostgreSQL.

---

## Consumer Group Naming

All consumer groups must follow this exact pattern:

```
{team}.{application}.{dataset}
```

| Segment | Description | Example |
|---|---|---|
| `team` | Owning team's short name | `actuarial`, `finance`, `data-science` |
| `application` | The specific application or service | `risk-model`, `reporting-api`, `ml-pipeline` |
| `dataset` | The dataset being consumed — matches the topic's `{dataset}` segment | `policies`, `claims`, `premiums` |

**Good examples:**

```
actuarial.risk-model.policies
finance.reporting-api.premiums
data-science.churn-model.policies
platform.audit-consumer.audit
```

**Do not use:**

```
cg.{team}.{dataset}      # wrong pattern — the cg. prefix is not used on this platform
policies-consumer        # no team identifier
my-consumer              # not identifiable
```

The IAM policy for each consumer is scoped to the specific consumer group name registered with the platform team. A group name that does not match the registered name will be denied by IAM. See `2026-04-15-consumer-onboarding.md §3` for the full policy and rationale.

---

## Schema Subject Naming

Schema subjects in AWS Glue Schema Registry (`ods-schema-registry-{env}`) follow a dot-separated pattern that mirrors the topic name:

```
ods.{domain}.{dataset}
```

This matches the topic name exactly. The mapping from topic name to schema subject is one-to-one and unambiguous.

**Examples:**

| Topic | Schema subject |
|---|---|
| `ods.insurance.policies` | `ods.insurance.policies` |
| `ods.motor.vehicles` | `ods.motor.vehicles` |
| `ods.pipeline.audit` | `ods.pipeline.audit` |

> **Note — discrepancy with schema-governance.md:** The schema governance document (`2026-04-15-schema-governance.md §6.1`) defines the registry subject naming pattern as `{domain}-{dataset}` (hyphen-separated, no `ods.` prefix). The governance registry table in that document also uses hyphen-separated values (e.g. `insurance-policies`). The pattern above (`ods.{domain}.{dataset}`, dot-separated, with prefix) is the canonical pattern for this platform as derived from the topic naming convention. The discrepancy should be resolved in a future revision of the schema governance document. Until that revision is published, treat this catalogue as the authoritative source for schema subject names and verify the actual subject name in `ods-schema-registry-{env}` before raising a support ticket.

`schema-governance.md` remains the authoritative source for all other schema governance rules: compatibility modes, PII tagging, change classification, and the breaking change process.

---

## S3 DLQ Prefixes

These are S3 object prefixes, not Kafka topics. They are listed here because they are the destination for records that fail processing and are referenced during incident response alongside the Kafka topics above.

### Publish pipeline DLQ (`ods-dlq-{env}`)

| Failure type | S3 prefix |
|---|---|
| Schema incompatible | `ods-dlq-{env}/schema-incompatible/date={date}/topic={topic}/` |
| DQ dataset-level failure | `ods-dlq-{env}/dq-dataset-failure/date={date}/topic={topic}/` |
| DQ row-level failure | `ods-dlq-{env}/dq-row-failure/date={date}/topic={topic}/run_id={run_id}/` |
| Count mismatch (post-publish) | `ods-dlq-{env}/count-mismatch/date={date}/topic={topic}/run_id={run_id}/` |
| Publish failed | `ods-dlq-{env}/publish-failed/date={date}/topic={topic}/` |

### Ingestion pipeline DLQ (`ods-dlq-{env}`)

The ingestion pipeline uses `dataset={dataset}` instead of `topic={topic}` because no Kafka topic is involved at the point of failure — the file has not reached the publish pipeline yet.

| Failure type | S3 prefix |
|---|---|
| Schema incompatible | `ods-dlq-{env}/schema-incompatible/date={date}/dataset={dataset}/` |
| DQ dataset-level failure | `ods-dlq-{env}/dq-dataset-failure/date={date}/dataset={dataset}/` |
| DQ row-level failure | `ods-dlq-{env}/dq-row-failure/date={date}/dataset={dataset}/run_id={run_id}/` |
| Count mismatch (post-ETL write) | `ods-dlq-{env}/count-mismatch/date={date}/dataset={dataset}/run_id={run_id}/` |

### CDC canonicalization pipeline DLQ (`ods-dlq-{env}`)

Records routed here are unrecoverable at the time of processing. The original raw CDC payload may be in the quarantine topic if the failure was DQ or schema related.

| Failure type | S3 prefix |
|---|---|
| Field mapping failure (config error · type mismatch) | `ods-dlq-{env}/mapping-failure/date={date}/topic={raw_topic}/` |
| K2 produce failure (after retry exhaustion) | `ods-dlq-{env}/produce-failure/date={date}/topic={canonical_topic}/` |
| Transaction commit failure (after retry exhaustion) | `ods-dlq-{env}/commit-failure/date={date}/topic={canonical_topic}/` |

Quarantine topic (`ods.raw.{dataset}.quarantine`) handles DQ hard-blocks and schema-incompatible records — these are **not** in the S3 DLQ because they are replayable. See CDC Raw & Staging Topics above.

### Quarantine (`ods-quarantine-{env}`)

Files that fail before entering the pipeline proper are not in the DLQ — they go to quarantine:

| Failure type | S3 prefix |
|---|---|
| File not in `file_catalogue` | `ods-quarantine-{env}/not-approved/date={date}/` |
| Checksum mismatch post-transfer | `ods-quarantine-{env}/checksum-mismatch/date={date}/` |

DLQ retention is 90 days active (S3 Standard), then transition to S3 Standard-IA for a further 275 days, then delete. See `2026-04-15-data-retention.md §7` for the full DLQ lifecycle policy.

---

## Related Documents

| Document | Relevance |
|---|---|
| `2026-04-14-s3-kafka-design.md` | Topic definitions, MSK naming conventions, audit topic schema |
| `2026-04-14-ingestion-design.md` | Ingestion pipeline; `ods.pipeline.audit` references for ingestion events |
| `2026-04-15-data-retention.md` | Full Kafka retention policies; S3 lifecycle rules for DLQ and audit sink |
| `2026-04-15-consumer-onboarding.md` | Consumer group naming; IAM access; connecting to MSK; offset management |
| `2026-04-15-reconciliation-design.md` | `ods.pipeline.reconciliation` topic schema and consumer list |
| `2026-04-15-schema-governance.md` | Schema change process; compatibility modes; PII tagging; breaking change coordination |
| `2026-04-16-file-state-machine.md` | PostgreSQL state tables used for idempotency (not Kafka topics, but closely related operationally) |
