# CDC Technology Evaluation: PostgreSQL → Kafka

**Status:** Decision Required (D-CDC)  
**Author:** Platform Architecture Review  
**Date:** 2026-04-16  
**Related:** [Pattern Catalogue](2026-04-16-pattern-catalogue.md) | [Architecture Decisions](2026-04-14-architecture-decisions.md) | [Ingestion Design](2026-04-14-ingestion-design.md)

---

## Context

Pattern 2 (CDC) requires reading change events from a PostgreSQL (RDS) source and publishing them to MSK. The initial assumption was that all ODS patterns would use Avro + Glue Schema Registry for consistency. Two decisions were made that changed the evaluation:

**Decision 1 — OpenFlow is not suitable for this pipeline**

The initial assumption was that Snowflake OpenFlow could act as the CDC pipeline:
- **OpenFlow SPCS (Snowflake-managed)** writes to Snowflake tables only. Kafka is not a supported destination in managed mode.
- **OpenFlow BYOC (your-own-cloud)** can target Kafka but supports **Confluent Schema Registry only** — not AWS Glue Schema Registry. Bridging to Glue requires custom processor code and adds Snowflake as an operational dependency for a pipeline that does not need Snowflake in the data path.
- OpenFlow is therefore ruled out regardless of deployment model.

**Decision 2 — CDC topics will use JSON, not Avro**

MSK is format-agnostic. The ODS platform uses Avro + Glue Schema Registry by default (Patterns 1, 3, 4) because it provides schema enforcement, compact encoding, and schema evolution governance. However, CDC events have a different shape:

- CDC events are sourced from PostgreSQL DDL, not from a platform-owned schema. The source of truth for field structure is the database table definition.
- CDC messages carry a Debezium envelope (`before`, `after`, `op`, `source`, `ts_ms`) that wraps the row payload. This structure is inherently JSON-shaped and does not benefit from Avro's type system in the same way as business-domain schemas.
- Requiring Avro for CDC would rule out AWS DMS (the simplest managed option) and most other CDC tooling without custom conversion layers.
- Many production platforms run Avro for batch/API patterns and JSON for CDC patterns, accepting that CDC governance is handled at the PostgreSQL DDL level rather than a separate schema registry.

**Platform format policy (updated):**

| Pattern | Format | Registry | Rationale |
|---|---|---|---|
| Pattern 1 — S3 Batch | Avro | Glue Schema Registry | Platform-owned schemas; schema evolution governance needed |
| Pattern 2 — CDC | **JSON** | **None** (governed by PostgreSQL DDL) | CDC envelope is JSON-native; source schema governed by DB |
| Pattern 3 — API | JSON Schema | Glue Schema Registry | API contracts are JSON Schema by convention |
| Pattern 4 — Event | Avro | Glue Schema Registry | Event schemas are platform contracts; strict versioning needed |

**Implication for consumers of CDC topics:** consumers must parse JSON defensively. Field structure changes on the source PostgreSQL table constitute breaking changes and must go through the normal schema change process (see `schema-governance.md §3.3`).

This document evaluates all viable CDC connector options under the JSON format decision.

---

## Requirements

| # | Requirement | Priority |
|---|---|---|
| R1 | Read PostgreSQL WAL (logical replication) | Must |
| R2 | Write to AWS MSK (Kafka) | Must |
| R3 | Output JSON format with Debezium CDC envelope | Must |
| R4 | Handle INSERT / UPDATE / DELETE events | Must |
| R5 | Exactly-once or at-least-once delivery with idempotent consumers | Must |
| R6 | AWS-managed or low operational overhead | Should |
| R7 | CloudWatch observability | Should |
| R8 | No Snowflake dependency in the CDC data path | Should |

> **Note:** Avro and Glue Schema Registry are **not** requirements for CDC. JSON is the chosen format. See Context section above for rationale.

---

## Options Evaluated

### Option A — MSK Connect + Debezium PostgreSQL Connector

**What it is:** AWS MSK Connect is a managed Kafka Connect service. The Debezium PostgreSQL connector runs as a plugin and reads PostgreSQL WAL via logical replication.

**How it works:**
```
PostgreSQL WAL (logical replication slot)
    → Debezium PostgreSQL Connector (MSK Connect plugin)
    → MSK topic per table
    → Avro serialised, schema registered in Glue Schema Registry
```

**PostgreSQL requirements:**
- `wal_level = logical` in RDS parameter group
- `rds.logical_replication = 1` (requires instance reboot)
- Replication slot created per connector
- Database user granted `REPLICATION` privilege

**Format and schema registry:**
- Outputs **JSON** using the Debezium CDC envelope (`before`, `after`, `op`, `source`, `ts_ms`) — aligns with platform JSON format decision for CDC
- Avro output is also available if needed in future (via `AvroConverter` + Glue Schema Registry) — no re-engineering required to switch
- Debezium envelope is the de facto standard for CDC events; widely understood by consumers, well-documented, tool-supported

**Operational model:**
- Fully managed by AWS — HA, patching, scaling handled
- Deployed as a MSK Connect custom plugin (Debezium JAR)
- Monitored via CloudWatch; MSK Connect metrics available out of the box

**Estimated cost:** ~£120–160/month per connector instance (1 MCU)

**Fits requirements:** R1 R2 R3 R4 R5 R6 R7 R8 — all met

---

### Option B — AWS DMS (Database Migration Service)

**What it is:** AWS managed service for database replication. Supports PostgreSQL as a CDC source and Kafka (including MSK) as a target.

**How it works:**
```
PostgreSQL WAL
    → DMS Replication Instance
    → MSK topic per table
    → JSON format only
```

**Format and schema registry:**
- **JSON output only** — no native Avro support
- No schema registry integration (none required for CDC under updated format policy)
- DMS CDC JSON payload includes: `metadata.operation` (INSERT/UPDATE/DELETE), `metadata.schema-name`, `metadata.table-name`, `metadata.timestamp`, plus `data` object with column values

**Assessment under JSON format policy:**
- JSON output is now aligned with the platform decision for CDC topics
- No downstream conversion layer required
- Fully managed, minimal operational overhead
- **Limitation**: DMS JSON format is DMS-specific (`"data": {...}` wrapper), not Debezium envelope format. Consumers must be written to the DMS JSON schema. If the connector is later replaced with Debezium, the message format changes — a consumer-breaking change.

**Verdict:** Viable under the JSON format policy. Simpler to operate than Option A but produces a non-standard CDC event format. Acceptable if the team commits to the DMS envelope as the CDC message contract.

---

### Option C — Debezium Standalone (ECS / Fargate)

**What it is:** Self-managed Kafka Connect cluster running the Debezium PostgreSQL connector, deployed on ECS or EC2.

**Format and schema registry:**
- Identical capability to Option A (Avro + Glue Schema Registry)
- Same PostgreSQL WAL requirements

**Trade-offs vs Option A:**

| | MSK Connect (A) | Debezium ECS (C) |
|---|---|---|
| AWS managed | Yes | No |
| Estimated cost | ~£120–160/mo | ~£25–40/mo (Fargate) |
| HA / failover | AWS managed | Self-managed |
| Monitoring | CloudWatch | JMX + custom setup |
| Patching / upgrades | AWS managed | Team responsibility |
| Custom processors | Limited | Full control |

**Verdict:** Viable if cost is a primary driver and the team has Kafka Connect operational expertise. Otherwise Option A is preferred.

---

### Option D — Amazon Managed Service for Apache Flink (formerly KDA)

**What it is:** AWS managed Apache Flink. Flink has a native PostgreSQL CDC source connector that reads WAL and streams change events.

**How it works:**
```
PostgreSQL WAL
    → Flink PostgreSQL CDC source
    → Flink streaming job (transform / route)
    → MSK topic
    → Avro format (via Flink Avro sink)
```

**Glue Schema Registry:**
- Not natively supported in Flink's Avro connectors
- Requires custom implementation using the AWS Glue Schema Registry serialisation libraries
- More complex than Option A

**When to prefer this:**
- If stream processing (filtering, enrichment, joins) is required in the CDC path
- If Flink is already part of the platform

**Verdict:** Over-engineered for a pure CDC pipeline. Adds complexity without benefit over Option A unless stream processing is needed in the CDC path.

---

### Option E — Snowflake OpenFlow SPCS (Snowflake-managed)

**What it is:** OpenFlow hosted in Snowflake's infrastructure. Snowflake manages the NiFi runtime.

**Data path:**
```
PostgreSQL WAL → OpenFlow (SPCS) → Snowflake tables
```

Kafka is **not a supported target** in SPCS mode.

**Verdict:** Does not meet R2. Not suitable for this pipeline.

---

### Option F — Snowflake OpenFlow BYOC (your-own-cloud)

**What it is:** OpenFlow NiFi runtime deployed in your AWS VPC. Full NiFi processor palette available, including `PublishKafkaRecord`.

**Data path:**
```
PostgreSQL WAL → OpenFlow NiFi (BYOC) → MSK → Avro (custom)
```

**Schema registry gap:**
- OpenFlow supports **Confluent Schema Registry only**
- AWS Glue Schema Registry requires custom NiFi processor code (Java) to bridge
- This is feasible but non-standard and maintenance-heavy

**Operational model:**
- Hybrid: Snowflake control plane + your AWS data plane
- You manage the AWS compute (EC2 or ECS) running the NiFi data plane
- Adds Snowflake as an operational dependency even when Snowflake is not in the data path

**Verdict:** Technically feasible but introduces avoidable complexity (Confluent vs Glue schema registry bridging, Snowflake dependency). Not recommended unless the team already operates OpenFlow.

---

### Option G — AWS Kinesis Firehose

**What it is:** A managed data delivery service that buffers and delivers streaming data to S3, Redshift, Snowflake, OpenSearch, and HTTP endpoints.

**Can it read PostgreSQL WAL?** **No.** Firehose is a delivery destination, not a CDC source. It receives data pushed to it.

**Can it output to Kafka/MSK?** **No.** MSK is not a supported Firehose target.

**Verdict:** Not applicable to this use case. Firehose cannot replace any part of a PostgreSQL → Kafka CDC pipeline.

---

## Decision Matrix

_Format column reflects the updated platform decision: JSON is the target format for CDC topics._

| Option | WAL Read | JSON Output | Debezium Envelope | AWS Managed | Ops Effort | Cost/mo | Recommended |
|---|:---:|:---:|:---:|:---:|---|---|:---:|
| A — MSK Connect + Debezium | ✅ | ✅ | ✅ (standard) | ✅ | Minimal | ~£140 | **Yes** |
| B — AWS DMS | ✅ | ✅ | ❌ (DMS format) | ✅ | Minimal | ~£80 | Acceptable |
| C — Debezium Standalone (ECS) | ✅ | ✅ | ✅ (standard) | ❌ | High | ~£30 | If cost constrained |
| D — Amazon Managed Flink | ✅ | ✅ | ✅ (configurable) | ✅ | Medium | Variable | No |
| E — OpenFlow SPCS | ✅ | ❌ (Snowflake only) | ❌ | ✅ | Minimal | — | No |
| F — OpenFlow BYOC | ✅ | ✅ | Configurable | Hybrid | Medium | Variable | No |
| G — Kinesis Firehose | ❌ | N/A | N/A | ✅ | Minimal | — | No |

---

## Recommendation

**Option A — MSK Connect + Debezium PostgreSQL Connector**

Under the JSON format decision, Options A, B, and C all now meet the format requirements. Option A is preferred because:

1. **Debezium envelope is the industry standard** for CDC events — `before`, `after`, `op`, `source`. Consumers built to this contract can be migrated between connectors (MSK Connect → ECS Debezium) without a message format change.
2. **AWS managed** — no cluster to operate, AWS handles HA and patching.
3. **Upgrade path to Avro** — if the platform later decides to bring CDC under schema registry governance, Debezium on MSK Connect can switch to Avro+AvroConverter without changing the pipeline architecture.

**Option B (AWS DMS)** is acceptable if simplicity is the priority and the team accepts the DMS-specific JSON envelope as a long-term consumer contract. The risk is connector lock-in: replacing DMS with Debezium later is a breaking change for all CDC consumers.

**Option C (Debezium ECS)** is viable if cost is the primary constraint and the team has Kafka Connect operational expertise.

---

## What This Means for the CDC Data Path

The CDC pipeline under the recommended architecture:

```
RDS PostgreSQL
    │  (logical replication slot)
    ▼
MSK Connect Worker
    │  Debezium PostgreSQL Connector plugin
    │  JsonConverter (no schema registry)
    ▼
MSK topic: ods.{domain}.{dataset}.cdc
    │  (JSON, Debezium envelope)
    ▼
Downstream consumers
```

**CDC message format (Debezium JSON envelope):**
```json
{
  "before": { "id": 123, "status": "active", ... },
  "after":  { "id": 123, "status": "cancelled", ... },
  "op":     "u",
  "source": {
    "table": "policies",
    "lsn": 12345678,
    "txId": 9876,
    "ts_ms": 1713268800000
  },
  "ts_ms":  1713268800123
}
```

`op` values: `c` (INSERT) / `u` (UPDATE) / `d` (DELETE) / `r` (snapshot read)

`before` is `null` on INSERT; `after` is `null` on DELETE. Both require `REPLICA IDENTITY FULL` on the source table for UPDATE events to carry the full `before` payload.

**Consumer contract:** The message structure above is the stable contract for CDC topic consumers. Changes to PostgreSQL table columns are breaking changes and must follow the schema change process (`schema-governance.md §3.3`).

**Note:** CDC bypasses S3 Curated. Changes are published directly to MSK. This is a design decision recorded in the [Pattern Catalogue](2026-04-16-pattern-catalogue.md).

---

## Prerequisites Before Implementation

1. RDS parameter group: set `rds.logical_replication = 1` (requires reboot — plan a maintenance window)
2. RDS parameter group: `wal_level = logical`, `max_replication_slots ≥ 5`, `max_wal_senders ≥ 5`
3. PostgreSQL user with `REPLICATION` privilege and `SELECT` on target tables
4. `REPLICA IDENTITY FULL` set on each table to capture before-images on UPDATE/DELETE
5. MSK Connect custom plugin: Debezium PostgreSQL connector JAR (no Glue Schema Registry serialiser needed — JSON format)
6. IAM role for MSK Connect with MSK write permissions (no Glue Schema Registry permissions needed for CDC topics)
7. MSK security group allows inbound from MSK Connect worker

---

## Open Decision: D-CDC

| | |
|---|---|
| **Decision** | Which CDC technology to use for Pattern 2 |
| **Options** | A (MSK Connect + Debezium) or C (Debezium ECS) |
| **Default** | Option A unless cost is a primary constraint |
| **Owner** | Platform lead |
| **Gates** | Pattern 2 design, RDS parameter group change (reboot required) |

Once decided, update:
- [`2026-04-16-pattern-catalogue.md`](2026-04-16-pattern-catalogue.md) — Pattern 2 row
- [`2026-04-14-architecture-decisions.md`](2026-04-14-architecture-decisions.md) — add ADR for CDC technology
- [`2026-04-14-ingestion-design.md`](2026-04-14-ingestion-design.md) — CDC section (if exists)
