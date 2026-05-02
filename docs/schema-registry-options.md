# Schema Registry Options For ODS

## Executive Summary

ODS needs schema governance for records published to Kafka and consumed by downstream systems.

Using AWS to hold schemas can make sense if AWS is the strategic platform. The AWS-native option is:

```text
AWS Glue Schema Registry
```

AWS Glue Schema Registry supports:

```text
Avro
JSON Schema
Protobuf
```

So choosing AWS Glue Schema Registry does not mean the ODS must use Avro for every topic. Avro remains a good default for file-to-Kafka-to-JDBC pipelines, but JSON Schema or Protobuf can be used where they fit better.

Recommended principle:

```text
Schema Registry governs Kafka message contracts.
ODS pipeline tables govern operational lineage, reconciliation, and process state.
```

Do not use Schema Registry as a replacement for `dataset_config`, `run_log`, `run_stage_log`, `reconciliation_log`, or lineage tables.

## What Problem Schema Registry Solves

Schema Registry answers:

```text
What shape should messages on this topic have?
Which schema version is valid?
Is a schema change compatible?
Can the producer serialize/validate this record?
Can consumers deserialize this record?
```

It does not answer:

```text
Which file produced this record?
Which run published it?
How many records failed DQ?
Did the JDBC sink write the rows?
What is the lineage between file, topic, and table?
```

Those remain ODS control-plane responsibilities.

## Current Pattern

Today the ODS converts records to Avro before publishing to Kafka:

```text
Parquet / Spark DataFrame
  -> validate against schema
  -> serialize as Avro
  -> publish to Kafka
```

This combines two concerns:

```text
schema validation
serialization format
```

They are related, but not identical.

ODS can validate against a schema without Avro being the only possible wire format.

## Supported Schema Formats

AWS Glue Schema Registry supports:

| Format | Notes |
|---|---|
| Avro | Good default for structured Kafka events and Kafka Connect/JDBC pipelines |
| JSON Schema | Easier to inspect/debug; larger messages; useful for API/event-style payloads |
| Protobuf | Strong contracts and compact messages; more developer ceremony |

## Option 1: Continue With Avro

Flow:

```text
Spark DataFrame
  -> validate against Avro schema
  -> serialize Avro
  -> publish to Kafka
  -> JDBC sink consumes Avro
```

Benefits:

- Good fit for tabular data.
- Compact messages.
- Strong schema evolution support.
- Current pipeline already works this way.
- Good fit for Kafka Connect/JDBC sink patterns.

Costs:

- Harder to inspect messages manually.
- Developers need Avro/schema tooling.
- Source systems may not naturally speak Avro.

Best for:

```text
file based ODS feeds
canonical business topics
high-volume structured data
Kafka Connect sink pipelines
```

## Option 2: Use JSON Schema

Flow:

```text
Spark DataFrame / API payload
  -> validate against JSON Schema
  -> publish JSON
  -> consumers validate/deserialize using Schema Registry
```

Benefits:

- Human-readable messages.
- Easier debugging.
- Natural fit for API and event payloads.
- Avoids Avro conversion for JSON-native sources.

Costs:

- Larger messages.
- Type handling can be less strict than Avro for some cases.
- Kafka Connect and consumers need JSON Schema converter support.
- May require connector configuration changes.

Best for:

```text
message based ingestion
API payloads
lower-volume operational events
topics where inspectability is more important than compactness
```

## Option 3: Use Protobuf

Flow:

```text
producer object
  -> validate against Protobuf schema
  -> serialize Protobuf
  -> publish to Kafka
```

Benefits:

- Compact messages.
- Strong contracts.
- Good fit for service-to-service events.

Costs:

- Requires more developer setup.
- Less natural for ad hoc tabular files.
- Requires generated classes or strong tooling patterns.

Best for:

```text
service-owned APIs/events
high-volume event streams
teams comfortable with Protobuf contracts
```

## Option 4: Spark/DQ Validation Only

Flow:

```text
Spark DataFrame
  -> validate columns/types/DQ rules
  -> publish plain JSON or another format
```

Benefits:

- Simple.
- No registry dependency at publish time.
- Easy local development.

Costs:

- No central schema compatibility governance.
- Consumers have weaker contracts.
- Harder schema evolution control.
- Not recommended for governed ODS Kafka topics.

Best for:

```text
temporary prototypes
internal-only low-risk flows
non-governed diagnostics
```

## Recommended Direction

Use a registry-backed schema for governed Kafka topics.

Recommended default:

```text
File based structured feeds:
  Avro via Schema Registry

Canonical business topics:
  Avro via Schema Registry

API/event message based feeds:
  JSON Schema or Protobuf where appropriate

Prototypes/internal diagnostics:
  Spark/DQ validation only, if explicitly accepted
```

If AWS is the long-term platform, AWS Glue Schema Registry is a sensible target.

## Provider-neutral ODS Design

ODS should not hard-code every pipeline concept to AWS Glue Schema Registry.

Use a provider-neutral configuration model:

```text
schema_registry_provider = aws_glue | confluent | local
schema_id
schema_version
canonical_schema_id
canonical_schema_version
schema_format = avro | json_schema | protobuf
```

`dataset_config` should reference the schema, but not become the schema registry itself.

Example:

```text
dataset_config
  domain
  dataset
  target_topic
  schema_registry_provider
  schema_format
  schema_id
  schema_version
  canonical_topic
  canonical_schema_id
  canonical_schema_version
```

The producer then:

```text
loads dataset config
fetches schema from configured registry
validates / serializes record
publishes to Kafka
records schema version used in run_log
```

## What Stays In ODS Pipeline Tables

Schema Registry should not replace the ODS control plane.

### `pipeline.dataset_config`

Stores:

```text
dataset name
topic names
schema IDs / versions
schema format
target table
DQ config
canonical transform config
```

### `pipeline.run_log`

Stores:

```text
run ID
pipeline type
file/source correlation
schema version used
status
record counts
topic offsets as operational metadata
```

### `pipeline.run_stage_log`

Stores:

```text
stage timings
stage status
input/output refs
record counts
metrics
errors
```

### `pipeline.reconciliation_log`

Stores:

```text
source count
DQ fail count
published count
Postgres count
discrepancy count
status
detail JSON
```

### `pipeline.lineage_edge`

Stores:

```text
relationships between files, runs, topics, and tables
```

## Validation Responsibilities

There are multiple validation layers.

### Schema validation

Checks:

```text
required fields exist
types match
schema version is valid
producer can serialize
consumer can deserialize
```

Owned by:

```text
Schema Registry + producer serializer/validator
```

### Data quality validation

Checks:

```text
business rules
nullability beyond schema
duplicate keys
valid date ranges
allowed values
cross-field rules
```

Owned by:

```text
ODS DQ rules
```

### Reconciliation validation

Checks:

```text
expected count = actual count
DLQ count accounted for
history table received records
current table matches latest history state
```

Owned by:

```text
ODS reconciliation_log
```

## Local Development Consideration

If production uses AWS Glue Schema Registry, local development still needs a practical option.

Options:

```text
run with Confluent Schema Registry locally
mock AWS Glue Schema Registry locally
load schemas from local files for tests
use provider abstraction in code
```

Recommendation:

```text
Keep local tests provider-neutral.
Allow schema files from repo for unit/integration tests.
Use AWS Glue Schema Registry in AWS environments.
```

## Decision Questions

The product/architecture team should decide:

1. Is AWS Glue Schema Registry the strategic production registry?

   Recommended if AWS/MSK/Glue are strategic.

2. Should Avro remain the default for file based and canonical business topics?

   Recommended: yes.

3. Should JSON Schema be allowed for API/event message based topics?

   Recommended: yes, where readability and JSON-native payloads matter.

4. Should ODS keep a provider-neutral schema registry abstraction?

   Recommended: yes.

5. Should ODS control-plane tables remain the source of lineage/reconciliation truth?

   Recommended: yes.

## Recommended Architecture

```text
AWS Glue Schema Registry
  -> owns schemas, schema versions, compatibility

ODS dataset_config
  -> references schema names/versions and topics

ODS producers
  -> fetch schema
  -> validate / serialize
  -> publish records with ODS lineage metadata

ODS run/reconciliation tables
  -> record what happened for each file/message/batch/run
```

This gives AWS-native schema governance without overloading Schema Registry with process, reconciliation, or lineage responsibilities.

