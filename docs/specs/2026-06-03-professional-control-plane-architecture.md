# Professional ODS Control Plane Architecture

## Purpose

This document describes the direction for turning the current ODS control-plane
prototype into a professional, production-grade metadata and lineage product.

The current design already proves the key ideas:

- `run_log` records work that happened.
- `run_stage_log` records progress inside work.
- `output_link` records exact outputs produced.
- `input_edge` records exact inputs consumed.
- target rows are stamped with ODS identifiers.
- target visibility controls what the business should use.
- refeeds preserve history while making corrected data active.
- Airflow identity can be captured separately from ODS workflow identity.
- dashboard/API functions can explain lineage and diagnostics.

The next step is to harden this into a product that developers can use easily,
support teams can diagnose quickly, and external lineage tools can understand.

## Executive Recommendation

The recommended architecture is:

```text
Data jobs / Airflow / Glue / Spark
        |
        v
ODS Control Plane API
        |
        +--> Postgres: authoritative source of truth
        |
        +--> Redis: live state, cache, locks, pub/sub
        |
        +--> OpenLineage-compatible event export
        |
        +--> Developer SDKs
```

The most important decision is:

```text
Postgres is the source of truth.
Redis is not the source of truth.
```

Redis should help with live operational state, idempotency, caching, locking,
and notifications. It should not be where lineage history lives.

## Design Principles

1. **API-first writes**

   Jobs should not write directly into `cp.run_log`, `cp.output_link`,
   `cp.input_edge`, or target visibility tables. They should call an ODS API.

   The API becomes the place where rules are enforced.

2. **Postgres remains authoritative**

   The control plane is an audit ledger. It must remain queryable and
   reconstructable even if Redis is flushed, redeployed, or unavailable.

3. **Redis is operational, not historical**

   Redis is excellent for current state and speed. It is poor as the permanent
   lineage record.

4. **OpenLineage compatibility, not OpenLineage replacement**

   The control plane should accept and emit OpenLineage-style events, but it
   should keep ODS-specific concepts that OpenLineage does not model directly:

   - business-date active visibility
   - replacement key
   - changed-only refeed
   - row stamping
   - target row history
   - reconciliation status

5. **Developers need five obvious calls**

   If developers need to understand every control table before using the product,
   the product will fail. The API and SDK should hide most table details.

6. **Support needs one-id lookup**

   Support should be able to start from any of these:

   - `workflow_run_id`
   - Airflow `dag_run_id`
   - Airflow `task_id`
   - `run_id`
   - `output_link_id`
   - target table row
   - raw `file_id`

   and navigate to the rest.

## Current Model Strengths

The current design has several strong foundations.

### Exact Output Identity

`output_link` is the right concept. It avoids ambiguity when a task produces
more than one output.

Example:

```text
output_link_id = M300
target         = customer_transaction
edge_type      = merge_to_canonical
```

Downstream jobs consume `M300`, not just "the customer_transaction run".

### Explicit Input Edges

`input_edge` is also the right concept. It records what a task consumed.

Example:

```text
input_edge_id            = E900
output_link_id           = M300
upstream_output_link_id  = C200
input_role               = customer
```

The `input_edge_id` is the row key. The meaningful dependency is:

```text
M300 consumed C200 as customer input
```

### Active Target Visibility

The target visibility table is critical. It separates:

```text
what was produced
```

from:

```text
what the business should use now
```

This is important for refeeds, restarts, and partial failures.

### Row-Level Traceability

Target rows carrying `_ods_output_link_id` is the right approach.

It lets support answer:

```text
This target row came from which output?
That output consumed which inputs?
Those inputs came from which raw files?
```

## Target Architecture

## Component View

```text
                   +----------------------+
                   |      Airflow         |
                   | DAG run / task try   |
                   +----------+-----------+
                              |
                              v
+----------------+    +-------+----------+      +-------------------+
| Glue / Spark   |--->| ODS Control API  |----->| Postgres cp / ods |
| Python jobs    |    | FastAPI          |      | source of truth   |
+----------------+    +-------+----------+      +-------------------+
                              |
                              +-----------> Redis
                              |             live state, locks,
                              |             cache, pub/sub
                              |
                              +-----------> OpenLineage events
                                            external consumers
```

## Write Path

The professional write path should be:

```text
Job starts
  -> POST /runs/start

Stage starts
  -> POST /runs/{run_id}/stages/start

Stage finishes
  -> POST /runs/{run_id}/stages/finish

Output is written
  -> POST /outputs

Target rows are written
  -> POST /targets/{table}/rows or SDK stamps ids before write

Reconciliation passes
  -> POST /reconciliation

Target becomes business-visible
  -> POST /target-visibility/activate

Run finishes
  -> POST /runs/{run_id}/finish
```

The API writes Postgres first. Redis is then updated from the successful
Postgres transaction or from a reliable post-commit event.

## Read Path

The read path should support both:

```text
Postgres direct SQL for auditors and support
```

and:

```text
FastAPI endpoints for applications, dashboards, and developers
```

The functions created in migration `022` are a good start:

```text
cp.dashboard_workflows()
cp.dashboard_workflow_detail(workflow_run_id)
cp.dashboard_output_trace(output_link_id)
cp.developer_diagnostics(workflow_run_id, target_table)
```

The FastAPI layer should call those functions rather than duplicate the SQL.

## Postgres Responsibilities

Postgres should own:

- immutable run history
- stage history
- file catalogue
- output links
- input edges
- target visibility
- row stamping references
- reconciliation history
- DLQ/quarantine records
- refeed and replay relationships
- diagnostics functions
- lineage trace functions

Postgres should be able to answer the full audit question without Redis.

## Redis Responsibilities

Redis should own operational state only.

Recommended Redis use cases:

1. **Live workflow state**

   ```text
   workflow:{workflow_run_id}:state
   run:{run_id}:state
   ```

   Useful for live dashboards and monitoring.

2. **Idempotency keys**

   ```text
   idem:{client}:{idempotency_key}
   ```

   Prevents duplicate API writes when jobs retry.

3. **Locks**

   ```text
   lock:target:{domain}:{dataset}:{business_date}:{replacement_key}
   ```

   Prevents two jobs from activating the same business slice concurrently.

4. **Pub/Sub notifications**

   ```text
   ods.events.workflow
   ods.events.run
   ods.events.visibility
   ```

   Lets dashboards and monitoring tools react without polling heavily.

5. **Short-lived API cache**

   Cache common read endpoints:

   ```text
   GET /workflows
   GET /workflows/{workflow_run_id}
   ```

   Cache must be invalidated by writes or expire quickly.

Redis should not own:

- final lineage history
- active slice truth
- target row history
- refeed supersession history
- audit evidence

## API-First Write Contract

## Required Write APIs

### Start Run

```http
POST /runs/start
```

Purpose:

Create or resume the logical ODS task run.

Example payload:

```json
{
  "workflow_run_id": "2f1e820f-35b8-4a44-82e9-0136deeec2db",
  "pipeline_type": "canonicalization",
  "domain": "insurance",
  "dataset": "claim",
  "business_date": "2026-05-29",
  "trigger_type": "airflow",
  "file_id": "optional-file-id",
  "replay_of_run_id": null,
  "orchestrator": {
    "type": "airflow",
    "dag_id": "ods_policy_claims",
    "run_id": "manual__2026-05-29T00:00:00+00:00",
    "task_id": "canonicalize_claim",
    "try_number": 1,
    "map_index": -1,
    "url": "https://airflow.example/dags/ods_policy_claims/grid"
  },
  "idempotency_key": "ods_policy_claims:2026-05-29:canonicalize_claim:1"
}
```

Returns:

```json
{
  "run_id": "..."
}
```

### Start Stage

```http
POST /runs/{run_id}/stages/start
```

Purpose:

Record that a task entered a named internal stage.

Example:

```json
{
  "stage": "validate_schema",
  "attempt": 1
}
```

### Finish Stage

```http
POST /runs/{run_id}/stages/{stage_log_id}/finish
```

Purpose:

Close a stage with status and counts.

Example:

```json
{
  "status": "succeeded",
  "record_count_in": 1000,
  "record_count_out": 997,
  "metrics": {
    "valid_rows": 997,
    "dlq_rows": 3
  }
}
```

### Create Output

```http
POST /outputs
```

Purpose:

Create an `output_link` and its `input_edge` rows.

Example:

```json
{
  "consumer_run_id": "...",
  "edge_type": "curated_to_canonical",
  "sink_type": "s3",
  "target_ref": {
    "path": "s3://silver/insurance/claim/2026-05-29.parquet",
    "content_hash": "sha256:...",
    "version": 1,
    "layer": "silver",
    "format": "parquet"
  },
  "record_count": 997,
  "inputs": [
    {
      "edge_type": "curated_to_canonical",
      "input_slot": 0,
      "source_file_id": null,
      "upstream_output_link_id": "...",
      "record_count": 1000,
      "source_ref": {
        "input_role": "raw_claim"
      }
    }
  ]
}
```

Rules:

- Every output must have at least one input.
- Raw ingestion outputs use `source_file_id`.
- Downstream outputs use `upstream_output_link_id`.
- `upstream_run_id` alone is not sufficient.
- `target_ref.path`, `target_ref.content_hash`, and `target_ref.version` are required.

### Reconciliation

```http
POST /reconciliation
```

Purpose:

Record whether the output count and target count reconcile.

Example:

```json
{
  "run_id": "...",
  "output_link_id": "...",
  "check_type": "sink_link",
  "source_count": 997
}
```

### Activate Target Visibility

```http
POST /target-visibility/activate
```

Purpose:

Make an output business-visible after target write and reconciliation.

Example:

```json
{
  "output_link_id": "...",
  "domain": "insurance",
  "dataset": "policy_claim",
  "business_date": "2026-05-29",
  "sink_type": "postgres",
  "target_name": "ods.policy_claim",
  "replacement_scope": "business_key",
  "replacement_key": "P001:CL100",
  "reason": "claim refeed corrected amount"
}
```

Rules:

- Only activate after the producing run succeeded.
- Only activate after reconciliation is OK.
- For changed-only refeeds, activate only changed business keys.
- The old active row for the same replacement key becomes inactive.

### Finish Run

```http
POST /runs/{run_id}/finish
```

Purpose:

Mark the ODS run as terminal.

Example:

```json
{
  "status": "succeeded",
  "record_count_out": 997,
  "error": null
}
```

## Idempotency

Every write endpoint should accept:

```text
idempotency_key
```

Recommended key shape:

```text
{orchestrator_type}:{dag_id}:{orchestrator_run_id}:{task_id}:{try_number}:{operation}
```

Examples:

```text
airflow:ods_policy_claims:manual__2026-05-29:canonicalize_claim:1:start_run
airflow:ods_policy_claims:manual__2026-05-29:canonicalize_claim:1:create_output
```

The API should store idempotency state in Redis for fast retry handling and in
Postgres where permanent deduplication is needed.

Response behavior:

```text
first request       -> perform write, return result
same retry          -> return same result
same key different payload -> reject
```

## Logical Task vs Attempt

The current model records `run_log` rows, and Airflow try number is captured in
orchestrator fields. A professional system should make the distinction clearer.

Recommended future model:

```text
task_run
  logical unit of work for a business slice
  e.g. canonicalize claim for 2026-05-29

task_attempt
  one execution attempt of that task
  e.g. Airflow try 1, try 2, manual retry
```

Questions this answers:

```text
Did the logical task complete?
How many attempts happened?
Which attempt wrote the active output?
Which attempt failed?
Which attempt did Airflow show as successful?
```

Lean path:

1. Keep current `run_log` for now.
2. Add explicit attempt identity later if retry reporting becomes confusing.
3. Preserve current `run_id` as the ID attached to `output_link`.

## OpenLineage Compatibility

## Why Align With OpenLineage

OpenLineage gives a common event language for jobs, runs, datasets, and facets.
Aligning with it makes integration easier with lineage tools and external
monitoring.

But ODS has product-specific requirements that OpenLineage does not fully cover.

Therefore the recommendation is:

```text
Accept OpenLineage-like events.
Emit OpenLineage-compatible events.
Keep ODS-native fields for active visibility, refeed, and row traceability.
```

## Mapping

```text
OpenLineage Job
  -> pipeline_type, domain, dataset, orchestrator task identity

OpenLineage Run
  -> cp.run_log row

OpenLineage Input Dataset
  -> cp.input_edge

OpenLineage Output Dataset
  -> cp.output_link

Dataset namespace/name
  -> target_ref.path, schema/table/path metadata

Run facets
  -> orchestrator_payload, stage metrics, retry metadata

Dataset facets
  -> target_ref, source_ref, schema version, content hash
```

## ODS Custom Facets

Recommended custom facets:

```json
{
  "ods_output_link": {
    "output_link_id": "...",
    "edge_type": "canonical_to_sink",
    "record_count": 997,
    "content_hash": "sha256:..."
  },
  "ods_input_edges": [
    {
      "input_edge_id": "...",
      "upstream_output_link_id": "...",
      "input_role": "claim"
    }
  ],
  "ods_target_visibility": {
    "domain": "insurance",
    "dataset": "policy_claim",
    "business_date": "2026-05-29",
    "replacement_scope": "business_key",
    "replacement_key": "P001:CL100",
    "status": "Y"
  },
  "ods_refeed": {
    "is_refeed": true,
    "replaces_output_link_id": "...",
    "reason": "corrected claim amount"
  }
}
```

## Event Ingestion APIs

Two event paths are recommended:

```http
POST /events/openlineage
POST /events/ods
```

`/events/openlineage`:

- accepts OpenLineage-compatible events
- maps them into ODS control-plane rows where possible
- stores unmapped metadata in facets/payloads

`/events/ods`:

- accepts richer native ODS events
- supports active visibility, replacement keys, row stamping, and refeed policy

## Refeed and Restartability Policy

## Restart

Restart means the same logical work was interrupted and must continue or retry.

Examples:

- worker died halfway through a task
- Airflow cleared a task
- Glue job failed after validation but before target write

Restart goals:

- do not expose partial target data
- do not create duplicate active slices
- do not lose failed attempt history
- make retry behavior idempotent

## Refeed

Refeed means new input data was supplied to correct or replace a previous input.

Examples:

- corrected claim file for 2026-05-29
- replacement transaction file
- manually replayed DLQ rows

Refeed goals:

- preserve the original output history
- create new output links for corrected outputs
- update only the intended business scope
- keep unchanged business keys active from the original load

## Replacement Policies

The API should support explicit replacement policies.

```text
slice
  Replace the whole domain/dataset/business_date slice.

business_key
  Replace only rows matching replacement_key.

file
  Replace data derived from one original file_id.

append_only
  Do not supersede anything; add new data only.

manual_approval
  Create pending visibility; require approval before status Y.
```

The selected policy should be visible in `target_visibility` and API responses.

## DLQ and Data Quality

DLQ must be first-class, not an afterthought.

Recommended concepts:

```text
dq_rule_result
  rule name, severity, failed count, sample, metrics

quarantine output_link
  output representing rejected rows

dlq row
  rejected row payload + reason + source file + output link

dlq replay
  corrected DLQ rows re-enter the workflow
```

Expected graph:

```text
raw file
  -> canonicalization
       -> good silver output_link
       -> quarantine output_link
```

Both good rows and rejected rows remain traceable.

Support should be able to answer:

```text
Which rows failed?
Which rule rejected them?
Which raw file did they come from?
Were they later replayed?
Which replay output fixed them?
```

## Developer SDKs

The API should be easy to use from Python, Spark, Glue, and Airflow.

Recommended Python shape:

```python
with ods.task(
    workflow_run_id=workflow_run_id,
    pipeline_type="canonicalization",
    domain="insurance",
    dataset="claim",
    business_date=business_date,
    orchestrator=airflow_context,
) as task:
    with task.stage("validate_schema"):
        validated = validate_schema(raw)

    with task.stage("transform"):
        silver = transform(validated)

    output = task.write_output(
        edge_type="curated_to_canonical",
        target_ref={
            "path": silver_path,
            "content_hash": silver_hash,
            "version": 1,
        },
        inputs=[raw_input],
        record_count=len(silver),
    )
```

The SDK should handle:

- idempotency keys
- retries
- stage start/finish
- API exception translation
- OpenLineage-compatible emission
- row stamping helpers

## Diagnostics and Support

The product should provide diagnostics both as SQL functions and API endpoints.

Existing direction:

```text
cp.developer_diagnostics(workflow_run_id, target_table)
GET /workflows/{workflow_run_id}/diagnostics
```

Diagnostics should detect:

- run started but not finished
- stage started but not finished
- succeeded run with failed/running stage
- run with no stages
- succeeded run with no output
- output with no inputs
- input edge missing `source_file_id` and `upstream_output_link_id`
- downstream edge missing exact `upstream_output_link_id`
- target row missing `_ods_output_link_id`
- target row references nonexistent output link
- target row workflow id disagrees with output producer
- multiple active visibility rows for same business key
- Airflow run missing orchestrator identity
- reconciliation missing or breached

## Operational Alerts

Recommended alert rules:

```text
run_running_too_long
stage_running_too_long
airflow_succeeded_ods_failed
ods_succeeded_airflow_failed
sink_written_not_activated
visibility_conflict
target_rows_missing_output_id
reconciliation_breach
dlq_rate_above_threshold
refeed_deactivated_too_much
output_link_without_inputs
input_edge_without_exact_upstream
```

Alerts should include:

- `workflow_run_id`
- `run_id`
- `orchestrator_dag_id`
- `orchestrator_run_id`
- `orchestrator_task_id`
- `output_link_id`
- domain/dataset/business date
- target table
- recommended diagnostic query/API link

## Security and Governance

## API Security

The API should enforce:

- authentication
- authorization by domain/dataset
- service account identity
- audit logging for every write
- request id / correlation id
- idempotency key validation

## Data Access

Not every user should see raw payloads.

Separate permissions:

```text
read workflow metadata
read lineage graph
read target row payload
read raw file paths
read DLQ payload
activate target visibility
submit refeed
approve refeed
```

## Audit

Every write API call should record:

- caller identity
- request id
- idempotency key
- endpoint
- payload hash
- created/updated ids
- timestamp

This can be a new table:

```text
cp.api_audit_log
```

## Recommended API Read Endpoints

The read API should expose:

```http
GET /workflows
GET /workflows/{workflow_run_id}
GET /workflows/{workflow_run_id}/diagnostics
GET /workflows/{workflow_run_id}/openlineage
GET /runs/{run_id}
GET /runs/{run_id}/inputs
GET /runs/{run_id}/outputs
GET /outputs/{output_link_id}
GET /outputs/{output_link_id}/trace
GET /files/{file_id}/usage
GET /targets/{schema}/{table}/rows/{row_id}/lineage
GET /target-visibility
GET /airflow/{dag_id}/{dag_run_id}
```

## Recommended Write Endpoints

```http
POST /runs/start
POST /runs/{run_id}/finish
POST /runs/{run_id}/stages/start
POST /runs/{run_id}/stages/{stage_log_id}/finish
POST /files/register
POST /outputs
POST /reconciliation
POST /target-visibility/activate
POST /events/openlineage
POST /events/ods
POST /dlq/replay
```

## Data Model Enhancements

Future enhancements worth considering:

1. `cp.task_run`

   Logical task identity separate from attempts.

2. `cp.task_attempt`

   Physical attempt/retry identity.

3. `cp.schema_version`

   Expected schema and validation contract per dataset.

4. `cp.dq_rule_result`

   First-class data quality outcomes.

5. `cp.api_audit_log`

   API write audit trail.

6. `cp.idempotency_log`

   Permanent deduplication record for important writes.

7. `cp.workflow_event`

   Event envelope for OpenLineage/ODS inbound events.

8. `cp.refeed_policy`

   Defines replacement behavior per dataset.

## Phased Roadmap

## Phase 1: Stabilize Database Read Functions

Already started:

- `cp.dashboard_workflows()`
- `cp.dashboard_workflow_detail(...)`
- `cp.dashboard_output_trace(...)`
- `cp.developer_diagnostics(...)`

Next:

- add target row lineage function
- add file usage function
- add Airflow lookup function
- add OpenLineage export function or API endpoint

## Phase 2: Read API

Expose database functions through FastAPI:

- workflow list
- workflow detail
- output trace
- diagnostics
- target row lineage
- OpenLineage export

Keep it read-only first.

## Phase 3: Write API

Move write operations behind FastAPI:

- start run
- stage start/finish
- output creation
- reconciliation
- visibility activation
- run finish

Jobs should stop writing directly to control tables.

## Phase 4: SDKs

Build developer SDKs:

- Python SDK
- Airflow helper
- Glue/Spark helper

Goal:

```text
Developers should not hand-build input_edge/output_link payloads unless they
are doing advanced work.
```

## Phase 5: Redis Operational Layer

Add Redis for:

- idempotency
- locks
- live status
- pub/sub
- read cache

Postgres remains authoritative.

## Phase 6: OpenLineage Integration

Add:

- `POST /events/openlineage`
- OpenLineage event export
- ODS custom facets
- compatibility tests

## Phase 7: Governance and Alerting

Add:

- API auth
- domain/dataset authorization
- audit log
- alert rules
- support runbooks

## What Not To Do

Do not make Redis the permanent lineage store.

Do not let every team invent its own refeed policy.

Do not rely only on Airflow metadata for lineage.

Do not rely on `run_id` alone when the exact `output_link_id` is needed.

Do not activate target visibility before write and reconciliation success.

Do not hide DLQ rows outside the lineage model.

Do not force ODS-specific behavior into pure OpenLineage if it does not fit.

## Opinionated Final Shape

The professional version should feel like this:

```text
Developer:
  I call a few obvious SDK methods.
  I do not need to know every control table.

Support:
  I can paste a workflow id, output id, file id, Airflow run id, or target row
  id and understand what happened.

Business:
  I can trust active target views because partial/failed outputs are not
  activated.

Platform:
  I can interoperate with OpenLineage while keeping ODS-specific controls for
  refeed, restartability, row lineage, and active slices.
```

The control plane should be boring where it matters:

```text
Postgres for truth.
API for enforcement.
Redis for speed.
OpenLineage for interoperability.
SDKs for adoption.
Diagnostics for support.
```

