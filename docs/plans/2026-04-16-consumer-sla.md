# ODS Platform — Consumer SLA
**Date:** 2026-04-16
**Status:** Draft — latency and success rate commitments pending CTO/Business sign-off (see §9)
**Audience:** Engineering leads and platform architects on downstream consumer teams

---

## 1. What this document is

This document states what the ODS platform formally commits to every team consuming Kafka topics in the `ods.*` namespace. It covers delivery guarantees, topic retention, schema stability, data quality, reconciliation, and escalation. Where a commitment is not yet formally agreed — specifically the latency SLO and pipeline success rate, which are pending CTO and Business sign-off as part of the D5 milestone — this document says so explicitly rather than implying guarantees that have not been made. A consumer team should be able to hand this document to their engineering lead and get a clear picture of what they can rely on when building against ODS topics.

---

## 2. Delivery guarantees

| Guarantee | Committed value | Notes |
|---|---|---|
| End-to-end latency (source arrival to Kafka) | **Proposed target: p95 < 10 minutes** (S3 batch pattern) | Measured from file landing in S3 Raw. Does not include the SFTP poll interval, which adds up to 5 minutes before file arrival in S3 Raw. **Pending D5 sign-off — see §9.** |
| Delivery semantics — producer side | Exactly-once write to Kafka | The Glue publish job uses Kafka transactions with `acks=all`. A record written to a Kafka partition is written exactly once by the platform. |
| Delivery semantics — consumer side | At-least-once consumption | Exactly-once write does not extend to consumer processing. If your consumer restarts after a crash, records between the last committed offset and the crash point will be redelivered. Your processing logic must be idempotent. See `consumer-onboarding.md §8`. |
| Atomic file publishing | All records from a source file are published together or not at all | The platform will never publish a partial file to Kafka. If a publish run fails mid-way, the entire batch is retried from the beginning or routed to the DLQ. |
| Message key stability | Stable across pipeline retries, reruns, and DLQ replay | The message key is a deterministic SHA-256 hash of the dataset's `key_fields` as declared in the YAML config. The same logical record always produces the same key. The key never changes after a topic goes live. |
| Message ordering | Per-partition ordering only | All records with the same message key land on the same partition. You can rely on ordering within a partition for a given entity. Cross-partition ordering is not guaranteed. |
| Pipeline success rate | **Proposed target: > 99.5% of pipeline runs completed without manual intervention**, measured over a rolling 30-day window | **Pending D5 sign-off — see §9.** |

### What the latency measurement covers

For the S3 batch pattern, end-to-end latency is measured from the moment a Parquet file lands in the S3 Raw zone (triggered by the EventBridge `Object Created` event) to the moment the publish job emits `publish.success` in CloudWatch. It does not cover:

- SFTP poll interval (up to 5 minutes before the file lands in S3 Raw)
- The time the source system took to generate and transmit the file
- Consumer processing time after the record is available in Kafka

For CDC, API, and Event patterns, latency definitions are pattern-specific and will be defined in the respective pattern SLA documents as those patterns are formalised.

---

## 3. Topic retention

### 3.1 Retention by topic type

| Topic type | Retention | Cleanup policy | Consumer implication |
|---|---|---|---|
| Business data topics (`ods.{domain}.{dataset}`) | 7 days | `compact,delete` | Do not design a consumer that relies on topic history beyond 7 days. Use the initial load procedure (`consumer-onboarding.md §7`) to bootstrap from a point-in-time S3 Curated snapshot when you first connect or after any extended outage. |
| `ods.pipeline.audit` | 90 days on-topic; 6 years in S3 audit sink (`ods-audit-sink-{env}`) | `delete` | The audit topic is platform-internal. Long-term audit data is available but transitions to cold storage (Glacier) after 90 days. Contact the platform team for historical audit access. |
| `ods.pipeline.reconciliation` | 30 days | `delete` | Platform-internal only. Consumer teams should not subscribe to this topic. |

### 3.2 Tombstone visibility

For business data topics using `compact,delete`, tombstones (null-value messages that signal a logical delete) are guaranteed visible for a minimum of **6 days** after publication. This follows from the configured `min.compaction.lag.ms` of 5 days and `delete.retention.ms` of 1 day — a tombstone will not be removed by the compactor before a consumer with up to 5 days of lag has had the opportunity to read it.

**Consumer risk at high lag:** If your consumer group's committed offset falls more than 5 days behind the partition high-water mark, you risk having compaction remove a tombstone before you read it. The platform monitors consumer lag and will alert at the 4-day mark (§7, T1 monitoring). If you receive a consumer lag alert, treat it as urgent — catching up before the 5-day compaction threshold is your responsibility.

---

## 4. Schema stability commitments

### 4.1 Compatibility mode per ingestion pattern

| Ingestion pattern | Schema compatibility mode | What this means for your consumer code |
|---|---|---|
| S3 batch | `BACKWARD` | New optional fields may be added at any time without notice. Existing fields will not be removed or have their type changed in a way that breaks your existing deserialization code. Write your consumer to tolerate unknown fields — do not fail on fields you did not expect. |
| CDC | `FULL` | Both producers and consumers are protected simultaneously. No field removals and no incompatible type changes in either direction. This is the most stable mode. |
| API | `BACKWARD` | Same as S3 batch — new optional fields may appear; existing fields are stable. |
| Event | `FORWARD` | The producer schema may evolve to add new fields. Your consumer must tolerate unknown fields without failing. Old consumer code continues to work with new messages. |

Compatibility mode is enforced by the AWS Glue Schema Registry. A schema change that violates the declared mode is rejected at registration time and the affected records are routed to the DLQ rather than published.

### 4.2 Breaking changes

A breaking change is any schema change that would cause a correctly-written consumer to fail or silently misprocess data. Examples: removing a field, renaming a field, changing a field's type incompatibly, or tightening a validation constraint.

The platform guarantees that:

- Breaking changes will not be deployed to production without a minimum **30-day notice period** to all registered consumers.
- A new versioned topic (`ods.{domain}.{dataset}.v2`) will be created for the new schema. The original topic will continue to be published to in parallel throughout the notice period.
- All consumers registered in `pipeline.schema_consumers` at the time of the notice will be contacted by email and Slack.

You must be registered as a consumer of a topic to receive breaking change notices. Registration happens at onboarding — see `consumer-onboarding.md §6`. If you are not registered, the platform cannot guarantee you will receive notice.

### 4.3 Key field immutability

The message key fields for each topic are declared in the dataset YAML config and are immutable after a topic goes live. The platform enforces this as a hard rule via CI check — no schema change PR can modify key fields on a live topic. If a key field change is genuinely unavoidable (rare; requires data architecture lead approval), the process creates a new versioned topic with a 90-day transition window. The original topic is never silently modified.

### 4.4 Kafka message headers

Every message published to any `ods.*` business topic carries the following headers. These headers are guaranteed stable — they will not be removed or renamed.

| Header | Value | Description |
|---|---|---|
| `x-ods-run-id` | `run_{domain}_{dataset}_{yyyyMMddTHHmmss}_{random6}` | Unique pipeline run identifier. Use this to trace a message back through the platform audit logs. |
| `x-ods-source-type` | `s3_batch` \| `cdc` \| `api` \| `event` | The ingestion pattern that produced this message. |
| `x-ods-source-ref` | S3 path, DB+LSN, API cursor, or event ID depending on pattern | Identifies the source artifact that produced this record. |
| `x-ods-business-date` | ISO 8601 date string | The business date the data represents — not the date the record was processed. |
| `x-ods-schema-version` | Schema version identifier | The Glue Schema Registry version in force when this message was serialised. Your deserializer resolves this automatically; you do not need to parse it manually during normal operation. |
| `x-ods-pipeline-type` | `publish` | Always `publish` for messages produced by the ODS publish pipeline. |

Full header specification: `data-lineage.md §3.1`.

---

## 5. Data quality commitments

| Guarantee | Detail |
|---|---|
| Schema validation before publish | Every record is validated against the registered Avro schema before it is published to Kafka. Records that fail schema validation are routed to the S3 DLQ (`ods-dlq-{env}`) and never published to the Kafka topic. A CloudWatch alarm fires immediately. |
| Data quality rule enforcement | Every dataset has DQ rules defined in a `.dqdl` file. Hard rule failures block publishing for the affected record — it goes to the DLQ. Soft rule warnings allow publishing but emit a `dq.soft.warning` CloudWatch metric that the platform monitors. |
| No silent data loss | Any record that is not published to Kafka is either in the S3 DLQ (where it is recoverable) or has triggered a CloudWatch alarm. The platform has no discard-and-continue behaviour — every failure is surfaced. |
| Count reconciliation at publish time (T0) | Every S3 batch publish run compares the source Parquet record count against the Kafka partition offset delta after the publish transaction commits. A count mismatch routes the batch to the DLQ and triggers a P1 alarm. No partial batch is silently accepted as complete. |

The platform guarantees these controls are applied. It does not guarantee that the source system's data is semantically correct — schema and DQ validation can only check structural rules, not business logic. If data from the source system is wrong but structurally valid, it will be published. Reconciliation (§7) is the mechanism for detecting upstream issues.

---

## 6. What the platform does not guarantee

The following are explicit non-commitments. They are stated here because they are common consumer assumptions that are incorrect.

**Cross-topic ordering.** There is no ordering guarantee between different topics or between different partitions of the same topic. If your use case requires ordering across two datasets (e.g., policies and claims), you must handle that ordering in your consumer.

**Consumer lag recovery.** The platform monitors consumer lag and will alert when it grows (§7), but the platform will not throttle or pause ingestion to allow a slow consumer to catch up. The pipeline publishes at the rate the source data arrives. If your consumer cannot keep pace, you must scale your consumer.

**Consumer-side processing.** The platform guarantees delivery to Kafka. What your consumer does with messages after consuming them — deserialization, upsert logic, state store management, downstream writes — is outside the platform's scope and your team's responsibility.

**Source data correctness.** The platform validates structure and applies DQ rules, but cannot verify that the upstream source system's data accurately represents reality. A source system that sends structurally correct but semantically wrong data will be published faithfully.

**Infinite replay from Kafka.** Topic retention is 7 days. Do not build a consumer that relies on being able to replay from the beginning of the topic for bootstrapping or disaster recovery. Use the initial load process (`consumer-onboarding.md §7`) instead.

**Sub-second latency.** The S3 batch pattern is batch-driven. p95 latency is targeted at under 10 minutes (pending sign-off). This is not a real-time streaming platform for the S3 pattern.

---

## 7. Reconciliation and correctness checking

The platform runs four tiers of reconciliation. Consumer teams do not need to trigger these — they are automatic. This table documents what the platform checks and what consumer teams should expect when a check fails.

| Tier | Timing | What it checks | What happens on failure | Consumer action required |
|---|---|---|---|---|
| **T0** — Publish-time count check | At every S3 batch publish run | Source Parquet record count equals Kafka partition offset delta after the publish transaction commits | Batch routed to S3 DLQ. P1 CloudWatch alarm fires. Publish job marked failed. No partial batch reaches Kafka. | None. Platform team investigates and replays from DLQ once root cause is resolved. |
| **T1** — Consumer lag monitoring | Continuously (CloudWatch metric, evaluated every 5 minutes) | Consumer group lag against each `ods.*` topic partition | P2 alarm at lag > 50,000 messages; P1 alarm at lag > 200,000 messages | If your consumer group is generating these alarms, your consumer is falling behind. Contact the platform team. Scale your consumer. Do not ignore lag alarms — at 5-day lag you risk missing tombstones (§3.2). |
| **T2** — Periodic aggregated count check | Hourly | Source record counts (from `pipeline.glue_job_log`) compared against Kafka topic record counts for the same business date window | Discrepancy beyond the per-dataset tolerance threshold triggers a CloudWatch alarm and is written to `pipeline.reconciliation_log`. A second consecutive failure triggers a P2 alarm. | None unless the platform team contacts you. T2 failures that persist point to a pipeline issue the platform team investigates. |
| **T3** — Full business reconciliation | Daily (after expected data settlement window) | Business-level aggregate sums (e.g. total premium, total claim amount) at source vs consumer; duplicate key scan; null key check | If T3 fails, the platform team contacts the dataset owner team to investigate whether the discrepancy originates in the source system or the pipeline. | If the platform team contacts you as a consumer about a T3 discrepancy, they may ask for your materialised counts to help isolate where the gap is. |

Reconciliation results (T2 and T3) are written to `pipeline.reconciliation_log` in the platform PostgreSQL database. Contact the platform team if you need access to these results for your own audit or compliance purposes.

---

## 8. Escalation and support

### 8.1 Escalation contacts

```
#ods-platform (Slack)  →  Jira ODS-SUPPORT  →  ODS Platform Tech Lead  →  Aviva Data Engineering Lead
```

For P1 situations (data loss, complete topic outage), contact the ODS Platform Tech Lead directly and raise a P1 Jira ticket simultaneously. Do not wait for the Slack response.

### 8.2 Situation guide

| Situation | What to do |
|---|---|
| Your consumer is not receiving expected messages | Check your consumer group lag in CloudWatch first (`AWS/Kafka SumOffsetLag` for your consumer group). If lag is zero and messages are still not appearing, raise a Jira `ODS-SUPPORT` ticket with: topic name, consumer group, expected business date, and `x-ods-run-id` if you have it. |
| You see a message that appears to be a duplicate | Use the `x-ods-run-id` header to look up the pipeline run. Message keys are deterministic — if two messages share the same key, they represent the same logical record. Apply last-write-wins (upsert on key). If you are seeing two different keys for what you believe is the same entity, raise `ODS-SUPPORT`. |
| You need data older than 7 days | Contact the platform team. Options are: DLQ replay (if the records were routed there), or a point-in-time Parquet snapshot from S3 Curated. The snapshot path follows the initial load procedure in `consumer-onboarding.md §7`. |
| A schema change breaks your consumer | Raise `ODS-SUPPORT` immediately and note the `x-ods-schema-version` header value from the first breaking message. The platform team should have given 30-day notice before this change reached production. If a breaking change was deployed to production without the required notice, that is a platform bug and will be treated as a P1 incident. |
| You receive a consumer lag alert from the platform team | Your consumer group is approaching the 5-day compaction threshold. Scale your consumer or reduce processing time. Respond to the platform team within 4 hours. |
| You want to disconnect from a topic | Follow the consumer offboarding process in `consumer-onboarding.md`. Do not simply stop your consumer and walk away. Deregister your consumer group so the platform team can remove you from lag monitoring and from the breaking change notification list. An unregistered but active consumer group that falls behind will still generate lag alarms that have no owner. |
| You receive an unexpected schema change notice for a topic you consume | If you are not registered as a consumer of that topic, register immediately so the platform has your sign-off requirement on record. Reply to the notice with your team's timeline for testing against the new schema in staging. |

---

## 9. Current commitments status

Not every commitment in this document has been through formal business sign-off. This table is transparent about which are agreed and which are proposed targets the platform is building to.

| Commitment | Status | Pending action |
|---|---|---|
| End-to-end latency SLO (p95 < 10 minutes, S3 batch) | **Proposed** — platform is built to this target; formal SLO not yet agreed | D5: RTO/RPO sign-off by CTO and Business |
| Pipeline success rate (> 99.5%, rolling 30 days) | **Proposed** — platform is built to this target; formal SLO not yet agreed | D5 sign-off |
| Exactly-once write to Kafka (producer side) | **Agreed** — implemented via Kafka transactions + `acks=all` | — |
| Atomic file publishing | **Agreed** — implemented via Kafka transaction boundaries | — |
| Message key stability and immutability | **Agreed** — enforced by CI check on schema changes | — |
| Schema breaking change — 30-day minimum notice | **Agreed** — defined in `schema-governance.md §9.2` | — |
| Schema key field immutability | **Agreed** — enforced by CI check; hard rule with no exceptions | — |
| Topic retention — 7 days for business data topics | **Decided** — implemented as `cleanup.policy=compact,delete`, `retention.ms=604800000` per `data-retention.md §4.3` | Legal/compliance sign-off on overall retention policy still in progress (does not affect the 7-day operational commitment) |
| Tombstone visibility — minimum 6 days | **Agreed** — follows from `min.compaction.lag.ms=432000000` + `delete.retention.ms=86400000` | — |
| Kafka message headers — stable and guaranteed | **Agreed** — headers are part of the platform contract | — |
| Schema validation before publish | **Agreed** — Glue Schema Registry validation is a gate in the publish pipeline | — |
| DQ hard rule enforcement — route to DLQ, never discard | **Agreed** — implemented and monitored via `ods-dlq-records-{env}` alarm | — |
| No silent data loss | **Agreed** — every failure surfaces via DLQ or CloudWatch alarm | — |
| T0 count reconciliation at publish | **Agreed** — implemented for S3 batch pattern | — |
| T1 consumer lag monitoring | **Agreed** — MSK `SumOffsetLag` alarms in place per `observability.md §3.3` | — |
| T2/T3 reconciliation (hourly/daily) | **Designed** — architecture defined in `reconciliation-design.md`; implementation in progress | Platform team implementation milestone |

**Note to reader:** Where a commitment is marked "Proposed", the platform is being built to that target and the engineering team intends to hold to it. However, formal business sign-off has not been obtained, and the value may be adjusted as part of the D5 milestone review. Once D5 is resolved, this table will be updated and the "Proposed" entries will either become "Agreed" or will be revised with the agreed values. Consumers designing critical systems against the latency or success rate targets should flag this dependency with their own engineering lead.

---

## Appendix A — Related documents

| Document | What it covers |
|---|---|
| `consumer-onboarding.md` | Access request, consumer group naming, MSK connection, schema deserialization, offset management, initial load, going-live checklist |
| `schema-governance.md` | Schema change classification, breaking change process, compatibility modes, PII tagging, deprecation and sunset |
| `data-retention.md` | Full retention policy for all platform storage layers; regulatory context |
| `2026-04-14-s3-kafka-design.md` | S3 batch pattern architecture, idempotency design, message key generation, DLQ behaviour |
| `reconciliation-design.md` | Full reconciliation tier design, T0–T3 architecture, per-pattern reconciliation approach |
| `observability.md` | SLO definitions, alarm thresholds, dashboards, consumer lag alarm mapping |

---

## Appendix B — Quick reference for consumer teams

| Item | Platform commitment |
|---|---|
| Message key | SHA-256 of `key_fields` declared in YAML config. Stable forever. Use as your idempotency key. |
| Producer delivery | Exactly-once write. |
| Consumer delivery | At-least-once. You must implement idempotent processing. |
| Latency | Proposed p95 < 10 min (S3 batch, source to Kafka). Pending sign-off. |
| Topic retention | 7 days. Tombstone visibility: minimum 6 days. |
| Schema breaking changes | Minimum 30-day notice. New versioned topic created in parallel. |
| Compatible changes (new optional fields) | No notice required. Your consumer must tolerate unknown fields. |
| No messages appearing | Check your consumer group lag first. Then raise `ODS-SUPPORT`. |
| Need data > 7 days old | Contact platform team for S3 Curated snapshot. |
| Offboarding | Deregister via `consumer-onboarding.md` — do not just stop consuming. |
| Support | `#ods-platform` Slack / Jira `ODS-SUPPORT` |
