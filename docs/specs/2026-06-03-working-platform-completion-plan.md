# Working Platform Completion Plan

## Purpose

This document defines what must be completed in this repository before it can be
called a working ODS control-plane platform.

The goal is not to build the future production API service in this repo. The
goal is to finish this repository as a complete, runnable, tested platform
reference that proves:

- restartable workflow control
- exact input/output lineage
- target row traceability
- active/inactive target visibility
- changed-only refeed behavior
- DLQ/data-quality handling
- schema validation
- developer-friendly usage wrappers
- support diagnostics
- clear documentation and runbooks

This document is intended as an implementation brief for Claude Code.

## Repository Boundary

This repository should finish as:

```text
ODS control-plane reference platform
```

It should include:

- Postgres migrations
- control-plane Python wrappers
- workflow harnesses
- demo workflows
- tests
- static dashboard snapshots
- SQL/database functions for support and developers
- documentation

It should not become:

```text
production API service
Redis-backed service
OpenLineage ingestion service
multi-tenant production deployment
```

Those belong in a future repository.

## Non-Goals For This Repo

Do not build the production FastAPI write service in this repo.

Do not add Redis in this repo.

Do not make OpenLineage the source of truth.

Do not replace Postgres control tables with JSON blobs.

Do not remove the existing control-plane relational model.

Do not make Airflow required for local tests.

Do not make the dashboard live-query Postgres. The dashboard can remain
snapshot-based.

## Current Strong Foundations

The repo already has important foundations:

- `cp.run_log`
- `cp.run_stage_log`
- `cp.output_link` view over physical lineage link table
- `cp.input_edge` view over physical lineage edge table
- `cp.file_catalogue`
- `cp.dlq`
- `ods.target_visibility`
- row stamping with ODS identifiers
- control wrappers in Python
- customer/transaction workflow
- policy/claims Airflow-style workflow
- refeed examples
- target visibility examples
- diagnostics/read functions
- dashboard snapshots
- broad pytest coverage

The unfinished work is mostly about making the platform story complete and
usable end-to-end.

## Definition Of Done

The repository is "working platform complete" when a developer can run tests and
demos that prove all of this:

```text
raw file registered
schema validated
good rows written to silver/canonical output
bad rows written to DLQ/quarantine output
DLQ rows remain traceable to raw input
DLQ rows can be replayed/fixed
merge consumes exact upstream output links
sink writes target rows stamped with output ids
aggregate consumes detail output
target visibility activates only after success/reconciliation
refeed replaces only intended business scope
unchanged business keys remain active from original load
diagnostics catch missing stages, missing output links, bad target stamps
developer can use simple wrappers/context managers
support can trace from workflow id, output link id, target row, or file id
```

## Required Completion Areas

## 1. Finalize The Control-Plane Write Contract

The platform needs one documented write contract.

The official sequence should be:

```text
1. register file, if raw file input exists
2. start run
3. start stage
4. application does work
5. finish stage
6. write output_link and input_edge rows
7. write/stamp target rows, if target sink
8. reconcile output or workflow
9. activate target visibility, if business-visible
10. finish run
```

### Required Implementation

Create or update documentation that states exactly which wrapper/function to use
for each step.

Expected mapping:

```text
register file
  control.runs.register_file(...)

start run
  control.runs.start(...)

start stage
  control.stages.start(...)

finish stage
  control.stages.finish(...)

write output
  control.write_output_link(...)
  or control.write_output_then_rows(...)

reconcile
  control.recon.reconcile_sink_link(...)
  control.recon.reconcile_workflow(...)

activate
  control.visibility.activate(...)

finish run
  control.runs.finalise(...)
```

### Acceptance Tests

Add/confirm tests that prove:

- every workflow task has a `run_log` row
- every workflow task has at least one `run_stage_log` row
- every successful task produces an `output_link`
- every output has at least one `input_edge`
- downstream input edges use `upstream_output_link_id`
- target rows are stamped with `_ods_output_link_id`
- business-visible rows have corresponding `target_visibility`

## 2. Finish DLQ And Data Quality

This is the largest missing platform piece.

The platform must prove that bad rows are not lost and are visible in lineage.

## Required DLQ Story

Use a small, explicit data-quality scenario. Prefer adding it to the
policy/claims workflow because claim validation is easy to explain.

## DLQ Implementation For This Repository

In this repository, DLQ should be modelled as a logical quarantine output with
both a physical data location and Postgres control metadata.

Do not use Kafka, Redis, or a separate service for this repo.

Use this model:

```text
physical DLQ data
  S3-style path in target_ref.path
  example: s3://dlq/insurance/claim/2026-05-29/claim-v1-errors.json

control metadata
  cp.dlq row
  quarantine output_link
  input_edge from the same raw file or upstream output
  provenance trace through cp.v_provenance
```

The DLQ output must be a first-class `output_link`.

Example:

```text
output_link
  output_link_id = Q500
  edge_type = quarantine
  target_ref.path = s3://dlq/insurance/claim/2026-05-29/claim-v1-errors.json
  target_ref.content_hash = dlq-content-hash
  target_ref.version = 1
  record_count = 1

input_edge
  output_link_id = Q500
  edge_type = quarantine
  source_file_id = raw claim file id
  record_count = 1

cp.dlq
  source_file_id = raw claim file id
  quarantine_output_link_id = Q500
  failed_payload = original rejected row
  failure_reason = claim_amount must be >= 0
  status = open
```

The original failed payload must not be overwritten. If someone fixes a DLQ row,
record a correction/replay relationship and close or resolve the original DLQ
row. Do not "move" the original DLQ row to another table and lose the failure
history.

Preferred lifecycle:

```text
open
under_review
corrected
replayed
resolved
rejected / ignored
```

Minimal replay model:

```text
correction/replay input
  points to dlq_id or quarantine output_link_id

replay run
  consumes the DLQ/quarantine identity
  writes corrected output_link
  updates cp.dlq.status = resolved
  records resolved_by_run_id and/or resolved_by_output_link_id if columns exist
```

If the existing `cp.dlq` table does not yet have status/resolution columns, add
the smallest useful migration rather than creating a second DLQ table.

Example:

```text
claim file contains 4 rows
3 rows pass validation
1 row fails because claim amount is negative or policy_id is missing
```

Canonicalization should produce:

```text
good output_link
  edge_type = curated_to_canonical
  record_count = 3

quarantine output_link
  edge_type = quarantine
  record_count = 1

cp.dlq row
  failed row payload
  rule/reason
  source file id
  quarantine output link id
```

Reconciliation should prove:

```text
input rows = good rows + dlq rows
```

## Required DLQ Replay Story

Add a replay/fix case:

```text
bad DLQ row is corrected
DLQ replay run consumes the quarantined row
corrected row enters normal lineage
corrected output becomes business-visible where appropriate
```

This does not need to be large. One bad row and one replayed row are enough.

## Required Tests

Tests must prove:

- DLQ rows are written when validation fails
- DLQ row contains enough reason/context to diagnose failure
- quarantine output link exists
- quarantine output is visible in provenance
- good output and quarantine output both trace to the raw source file
- reconciliation accounts for good plus DLQ
- DLQ replay consumes prior DLQ/quarantine identity
- replayed/fixed row traces back to original raw/DLQ context
- bad row is not present in business-visible target before replay/fix

## Suggested Names

Possible test file:

```text
tests/test_policy_claims_dlq_workflow.py
```

Possible workflow helper:

```text
harness/policy_claims_workflow.py
```

Keep it close to the existing policy/claims workflow unless that makes the file
too large. If it becomes too large, create:

```text
harness/policy_claims_dlq_workflow.py
```

## 3. Add Schema Validation Contracts

The platform should be able to say:

```text
This output was validated against schema version X.
```

## Required Minimal Contract

Use `cp.dataset_config` if it already fits. If it does not, add the smallest
useful extension.

Minimum fields/concepts:

```text
domain
dataset
pipeline_type or layer
schema_version
required_columns
nullable_columns
business_key
replacement_scope
replacement_key_template
active flag / effective dates, if needed
```

The exact table shape can be lean. Do not overbuild.

## Validation Behavior

During canonicalization:

```text
read dataset_config
validate required columns
validate nullable rules
validate simple type expectations if available
record schema_version in output metadata
record validation metrics in run_stage_log.metrics
send failed rows to DLQ/quarantine
```

`target_ref` or `transform_version` should include the schema version used.

Example:

```json
{
  "path": "s3://silver/insurance/claim/2026-05-29.parquet",
  "content_hash": "sha256:...",
  "version": 1,
  "schema_version": "claim.v1"
}
```

## Required Tests

Tests must prove:

- missing required column fails validation
- nullable violation goes to DLQ
- valid row passes
- output records schema version
- validation metrics are visible in `run_stage_log.metrics`
- diagnostics can show which schema version was used

## 4. Formalize Replay/Refeed Policy

The behavior exists, but the policy needs to be explicit.

## Required Policies

Support these concepts, at least in config/docs/tests:

```text
slice
  replace whole business_date slice

business_key
  replace only specific business keys

file
  replace rows derived from a specific original file

append_only
  do not supersede; add only

manual_approval
  produce pending visibility, not active visibility
```

For this repo, the minimum implementation should prove:

```text
business_key replacement
slice replacement
append_only or documented non-goal
manual approval as documented future option
```

## Current Behavior To Preserve

Changed-only refeed must continue to work:

```text
Day 2 original load writes multiple business keys.
Day 2 refeed changes only selected keys.
Changed keys: old active row becomes N, corrected row becomes Y.
Unchanged keys: original row remains Y.
```

## Required Tests

Tests must prove:

- exactly one active `Y` row per replacement key
- changed keys are superseded correctly
- unchanged keys are not deactivated by changed-only refeed
- failed refeed does not activate corrected output
- restart before activation leaves previous active output visible
- slice replacement deactivates the whole slice when policy says slice

## 5. Add Developer SDK-Style Wrappers

This repo does not need the future production API, but it does need developer
ergonomics.

Add thin Python context-manager helpers over the existing control wrappers.

## Desired Usage

```python
from control.sdk import task

with task(
    conn,
    workflow_run_id=workflow_run_id,
    pipeline_type="canonicalization",
    domain="insurance",
    dataset="claim",
    business_date=business_date,
    trigger_type="airflow",
    orchestrator=orchestrator,
    commit=False,
) as run:
    with run.stage("validate_schema") as stage:
        valid_rows, bad_rows = validate_claims(rows)
        stage.finish(record_count_in=len(rows), record_count_out=len(valid_rows))

    output = run.write_output(
        edge_type="curated_to_canonical",
        target_ref={
            "path": silver_path,
            "content_hash": silver_hash,
            "version": 1,
            "schema_version": "claim.v1",
        },
        record_count=len(valid_rows),
        inputs=[raw_input],
    )
```

## Required SDK Behavior

The SDK should:

- start the run on enter
- finish the run on successful exit
- mark failed on exception
- start/finish stages through context managers
- allow output creation
- optionally reconcile output
- not hide the underlying ids
- return `run_id`, `stage_log_id`, `output_link_id`

Keep this small. It is a convenience wrapper, not a production service.

## Required Tests

Tests must prove:

- successful task context creates and finishes run
- exception inside task marks run failed
- stage context starts and finishes stage
- exception inside stage marks stage failed or records failure clearly
- output helper creates output link and input edge
- returned ids can be used by trace functions

Suggested file:

```text
tests/test_sdk.py
```

Suggested module:

```text
control/sdk.py
```

## 6. Strengthen Diagnostics

The diagnostics functions are a good start. Expand only where needed.

Current functions to keep:

```text
cp.dashboard_workflows()
cp.dashboard_workflow_detail(workflow_run_id)
cp.dashboard_output_trace(output_link_id)
cp.developer_diagnostics(workflow_run_id, target_table)
```

## Required Additions

Consider adding:

```text
cp.dashboard_file_usage(file_id)
cp.dashboard_target_row_trace(target_schema, target_table, row_id)
cp.dashboard_airflow_lookup(dag_id, dag_run_id)
```

Only add these if they are useful and tested.

## Diagnostics Must Detect

Ensure `cp.developer_diagnostics` detects:

- unfinished run
- unfinished stage
- succeeded run with unfinished stage
- run without stages
- successful run without output
- output without input edges
- input edge missing input identity
- downstream input missing `upstream_output_link_id`
- target row missing ODS ids
- target row output link does not exist
- target row workflow mismatch
- visibility conflict
- Airflow identity missing
- reconciliation missing/breached
- DLQ row missing trace context
- quarantine output missing DLQ rows
- schema validation output missing schema version

## 7. Finish Dashboard As Demonstration Only

The dashboard can remain static/snapshot-based.

It should demonstrate:

- customer/transaction workflow
- policy/claims workflow
- Airflow DAG metadata
- process model
- target row history
- output/input links
- DLQ/quarantine path, if possible
- diagnostics/docs

Do not turn it into a production API dashboard in this repo.

## Required Snapshot Work

Regenerate snapshots after final workflow changes:

```powershell
python -m harness.customer_transaction_workflow --out dashboard/data/demo-workflow.json
python -m harness.policy_claims_workflow --out dashboard/data/policy-claims-workflow.json
```

If a separate DLQ workflow is added:

```powershell
python -m harness.policy_claims_dlq_workflow --out dashboard/data/policy-claims-dlq-workflow.json
```

## 8. Clean Repo Scope

Because the API service will be a future repo, remove or park production API
work from this repo.

If FastAPI files exist from exploration:

```text
api/
tests/test_api.py
fastapi/httpx/uvicorn requirements
```

Decision:

- remove them from final platform repo, or
- keep only if explicitly labelled as a read-only example

Recommended:

```text
Remove FastAPI app from this repo.
Keep database functions.
Future API repo consumes these functions.
```

Do not remove database read functions in migration `022`.

## 9. Update README And Runbook

The README must become platform-facing, not just demo-facing.

It should explain:

- what this repo is
- what this repo is not
- how to start Postgres
- how to apply migrations
- how to run tests
- how to run customer/transaction demo
- how to run policy/claims demo
- how to run DLQ demo, if separate
- how to regenerate dashboard snapshots
- how to open dashboard
- how to use support SQL functions
- how to diagnose workflow issues
- how to trace from target row to raw file
- what future API/Redis/OpenLineage repo will do

Add a short runbook:

```text
Given workflow_run_id, what do I query?
Given output_link_id, what do I query?
Given target row, what do I query?
Given Airflow dag_run_id, what do I query?
Given bad row/DLQ row, what do I query?
```

## Implementation Order

Recommended order for Claude Code:

```text
1. Clean repo scope decision
   - do not build API
   - keep DB functions
   - remove/park FastAPI if present and not wanted

2. Finish DLQ workflow/tests
   - bad row
   - quarantine output
   - cp.dlq
   - replay/fix
   - trace and recon tests

3. Add schema validation contract
   - dataset config/schema version
   - validation metrics
   - DLQ integration

4. Formalize refeed policy
   - business_key behavior
   - slice behavior
   - failed refeed/restart behavior

5. Add SDK context managers
   - task
   - stage
   - write output
   - failure handling

6. Strengthen diagnostics
   - DLQ/schema diagnostics
   - optional file/row/Airflow lookup functions

7. Regenerate snapshots

8. Update README/runbook

9. Run full test suite
```

## Acceptance Criteria

All of the following must be true.

### Tests

```powershell
python -m pytest -q
```

must pass.

### DLQ

There must be tests proving:

```text
bad rows go to cp.dlq
quarantine output exists
DLQ/quarantine traces to raw file
good + bad reconciles to input
DLQ replay/fix is traceable
```

### Schema

There must be tests proving:

```text
schema version is selected
required column failure is caught
valid rows pass
invalid rows go to DLQ
output records schema version
```

### Refeed

There must be tests proving:

```text
changed-only refeed only supersedes changed business keys
unchanged keys remain active
slice replacement can supersede whole slice
failed restart/refeed does not activate partial data
```

### SDK

There must be tests proving:

```text
task context manager creates/finalizes run
stage context manager creates/finalizes stage
exceptions are recorded
output helper creates output/input rows
```

### Documentation

README/docs must make the platform usable without asking the original author.

## Final Positioning

After this work, this repo should be described as:

```text
A working ODS control-plane reference platform built on Postgres.
It demonstrates restartable workflow control, exact lineage, DLQ, refeed,
target visibility, diagnostics, and developer ergonomics.
```

The future repo should be described as:

```text
A production ODS control-plane service with APIs, Redis operational state,
OpenLineage ingestion/export, authentication, and deployment packaging.
```
