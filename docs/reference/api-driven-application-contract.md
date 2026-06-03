# API-Driven Application Contract

This document is the handoff from this reference repository to the future
API-driven control-plane application repository.

This repo remains the source of truth for:

- Postgres schema and migrations
- `cp.*` write/read functions
- Python wrapper semantics
- workflow/dlq/refeed demos
- dashboard snapshot shape
- regression tests

The future repo should provide the production HTTP/event surface over this
contract. It should not reinvent the lineage model.

## Boundary

This repository:

- owns `cp.run_log`, `cp.run_stage_log`, `cp.output_link`, `cp.input_edge`,
  `cp.dlq`, `ods.target_visibility`, `cp.schema_contract`
- proves the write order
- proves restart/refeed/DLQ behavior
- exposes read-only dashboard/developer functions

Future API repository:

- owns HTTP/event ingestion
- owns auth/authz
- owns Redis or other hot-path infrastructure
- owns OpenLineage event ingestion
- owns production deployment
- calls this repo's database contract

## Core Write API Shape

The API should expose the same concepts as the `control/` wrappers.

### Register File

```http
POST /files/register
```

Request:

```json
{
  "s3_raw_path": "s3://raw/insurance/claim/2026-05-29.json",
  "file_md5": "abc123",
  "business_date": "2026-05-29",
  "domain": "insurance",
  "dataset": "claim",
  "idempotency_key": "insurance:claim:2026-05-29:abc123"
}
```

Maps to:

```python
control.runs.register_file(...)
```

### Start Or Resume Run

```http
POST /runs/start
```

Request:

```json
{
  "workflow_run_id": "uuid-or-orchestrator-execution-id",
  "pipeline_type": "canonicalization",
  "domain": "insurance",
  "dataset": "claim",
  "business_date": "2026-05-29",
  "trigger_type": "airflow",
  "file_id": "optional-uuid",
  "replay_of_run_id": "optional-uuid",
  "orchestrator": {
    "type": "airflow",
    "dag_id": "ods_policy_claims",
    "run_id": "scheduled__2026-05-29",
    "task_id": "canonicalize_claim",
    "try_number": 1,
    "map_index": -1,
    "url": "https://airflow/..."
  },
  "idempotency_key": "airflow:ods_policy_claims:scheduled__2026-05-29:canonicalize_claim:try-1"
}
```

Maps to:

```python
control.runs.start(...)
```

### Start / Finish Stage

```http
POST /runs/{run_id}/stages/start
POST /runs/{run_id}/stages/{stage_log_id}/finish
```

The API should keep the same semantics as `control.stages.stage_scope`: every
stage has status, attempt, counts, metrics, start time, and finish time.

### Write Output Link

```http
POST /outputs
```

Request:

```json
{
  "consumer_run_id": "uuid",
  "edge_type": "curated_to_canonical",
  "target_ref": {
    "path": "s3://silver/insurance/claim/2026-05-29.parquet",
    "content_hash": "sha256-or-md5",
    "version": 1,
    "schema_version": "claim.v1"
  },
  "record_count": 4,
  "inputs": [
    {
      "edge_type": "curated_to_canonical",
      "upstream_output_link_id": "uuid",
      "record_count": 4,
      "source_ref": {
        "input_role": "claim"
      }
    }
  ],
  "transform_version": "silver-v1",
  "idempotency_key": "run:uuid:output:curated_to_canonical:silver-claim"
}
```

Maps to:

```python
control.lineage.write_output_link(...)
```

### Write Output Then Target Rows

```http
POST /targets/{schema}/{table}/write
```

This endpoint writes the output link, input edges, and target rows in one
transaction. It maps to `control.lineage.write_output_then_rows(...)`.

Request:

```json
{
  "consumer_run_id": "uuid",
  "edge_type": "canonical_to_sink",
  "sink_type": "postgres",
  "target_ref": {
    "path": "postgres://ods/policy_claim",
    "content_hash": "postgres-policy-claim-2026-05-29",
    "version": 1
  },
  "record_count": 4,
  "inputs": [
    {
      "edge_type": "canonical_to_sink",
      "upstream_output_link_id": "uuid",
      "record_count": 4
    }
  ],
  "rows": [
    {
      "policy_id": "P001",
      "claim_id": "CL100"
    }
  ],
  "idempotency_key": "run:uuid:sink:ods.policy_claim"
}
```

### Reconcile

```http
POST /reconciliation/sink-link
POST /reconciliation/workflow
```

Maps to:

```python
control.recon.reconcile_sink_link(...)
control.recon.reconcile_workflow(...)
```

### Activate Visibility

```http
POST /target-visibility/activate
```

Request:

```json
{
  "domain": "insurance",
  "dataset": "policy_claim",
  "business_date": "2026-05-29",
  "sink_type": "postgres",
  "target_name": "ods.policy_claim",
  "output_link_id": "uuid",
  "producer_run_id": "uuid",
  "workflow_run_id": "uuid",
  "replacement_scope": "business_key",
  "replacement_key": "P001:CL100",
  "reason": "normal load",
  "idempotency_key": "visibility:insurance:policy_claim:2026-05-29:P001:CL100:uuid"
}
```

Maps to:

```python
control.visibility.activate(...)
```

Activation must happen only after:

- producer run is `succeeded`
- graph-derived reconciliation for the output is `ok`

The database enforces those checks.

### Quarantine DLQ

```http
POST /dlq/quarantine
```

Request:

```json
{
  "run_id": "uuid",
  "stage": "validate_schema",
  "reason": "non-nullable column 'policy_id' is null",
  "source_file_id": "raw-file-uuid",
  "source_ref": {
    "raw_file_id": "raw-file-uuid",
    "raw_path": "s3://raw/insurance/claim/2026-05-29.json",
    "schema_version": "claim.v1"
  },
  "payload_ref": "s3://dlq/insurance/claim/2026-05-29/errors.json",
  "record_count": 1,
  "failed_payload": {
    "claim_id": "CL900",
    "policy_id": null
  },
  "idempotency_key": "dlq:run:uuid:stage:validate_schema:raw-file-uuid:CL900"
}
```

Maps to:

```python
control.dlq.quarantine(...)
```

The API must require `source_file_id`. The SQL function remains backward
compatible, but application code should not create unanchored DLQ lineage.

### Resolve DLQ

```http
POST /dlq/{dlq_id}/resolve
```

Request:

```json
{
  "status": "resolved",
  "resolved_by_run_id": "uuid",
  "resolved_by_output_link_id": "uuid"
}
```

Maps to:

```python
control.dlq.resolve(...)
```

## Read API Shape

The API can wrap the read-only SQL functions directly:

```http
GET /workflows
GET /workflows/{workflow_run_id}
GET /outputs/{output_link_id}/trace
GET /files/{file_id}/impact
GET /targets/{schema}/{table}/rows/{row_id}/trace
GET /airflow/runs/{dag_run_id}?dag_id=...
GET /workflows/{workflow_run_id}/diagnostics
```

Maps to:

- `cp.dashboard_workflows()`
- `cp.dashboard_workflow_detail(...)`
- `cp.dashboard_output_trace(...)`
- `cp.dashboard_file_impact(...)`
- `cp.dashboard_target_row_trace(...)`
- `cp.dashboard_airflow_lookup(...)`
- `cp.developer_diagnostics(...)`

## Idempotency

Every write endpoint should accept `idempotency_key`.

Suggested key ingredients:

- `workflow_run_id`
- orchestrator `dag_id`
- orchestrator `run_id`
- orchestrator `task_id`
- try/attempt
- business date
- domain
- dataset
- operation
- output target path or target table
- replacement key for visibility writes

The API should return the previously-created resource for a repeated key with the
same request body, and reject a repeated key with a different body.

## Redis Boundary

Redis is useful in the future repo, but it should not become lineage truth.

Recommended Redis uses:

- idempotency key cache
- short-lived write locks
- orchestrator run/task lookup cache
- request deduplication
- API response caching for dashboard reads

Postgres remains the source of truth for:

- run state
- stage state
- output links
- input edges
- DLQ lifecycle
- target visibility
- schema contracts
- reconciliation

## OpenLineage Mapping

The API should accept OpenLineage-style events but map them into this contract.

Recommended mapping:

- OpenLineage job/run -> `workflow_run_id` + orchestrator fields
- OpenLineage input dataset -> `input_edge`
- OpenLineage output dataset -> `output_link`
- OpenLineage facets -> `orchestrator_payload`, `target_ref`, `source_ref`, or
  stage metrics depending on facet type
- OpenLineage `COMPLETE` -> finish run and stage
- OpenLineage `FAIL` -> failed run/stage with error

OpenLineage events are a protocol view. The database contract remains the
accounting truth.

## Error Model

Use consistent JSON errors:

```json
{
  "error": {
    "code": "missing_output_link",
    "message": "output_link_id ... not found",
    "hint": "Query /workflows/{workflow_run_id} to list outputs",
    "details": {}
  }
}
```

Map database `P0001` exceptions into 400/404 style API errors with the database
hint preserved.

## First Version Scope

Build first:

1. write endpoints for runs/stages/outputs/sinks/reconciliation/visibility/DLQ
2. read endpoints over the `cp.dashboard_*` and diagnostics functions
3. idempotency key table/cache
4. OpenLineage import adapter
5. auth and tenant boundary

Do not build first:

- new lineage schema
- new target visibility model
- a Redis source of truth
- a dashboard-specific data model that diverges from Postgres
