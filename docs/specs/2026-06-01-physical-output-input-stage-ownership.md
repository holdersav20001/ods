# Physical Output/Input Rename and Stage Ownership Spec

## Purpose

The current control-plane model is conceptually moving toward:

- `output_link`: what a run or stage produced
- `input_edge`: what that output used as input
- `output_link_id`: the produced output id
- `input_edge_id`: the relationship row id
- `upstream_output_link_id`: the previous output used as input
- `_ods_output_link_id`: the output id stamped on target rows

The current implementation has a confusing compatibility layer:

- `cp.output_link` is a view over physical table `cp.lineage_link`
- `cp.input_edge` is a view over physical table `cp.lineage_edge`
- new column names are view aliases over old physical column names

This spec asks for the next implementation pass: make the physical database, Python API, SQL, tests, docs, and dashboard use the same names. Do not leave the main model as "old physical names with new view aliases."

## Required End State

Physical database objects should use the product terminology directly:

| Current Physical Name | Required Physical Name |
|---|---|
| `cp.lineage_link` | `cp.output_link` |
| `cp.lineage_edge` | `cp.input_edge` |
| `lineage_link_id` | `output_link_id` |
| `lineage_edge_id` | `input_edge_id` |
| `upstream_lineage_link_id` | `upstream_output_link_id` |
| `_ods_lineage_link_id` | `_ods_output_link_id` |
| `p_lineage_link_id` | `p_output_link_id` |
| `lineage_link_id` in reconciliation metrics | `output_link_id` |

The default developer experience should not require remembering that the real table is called `lineage_link`.

## Additional Flexibility Fixes

This implementation should also address the following model inflexibilities. They are not dashboard polish; they affect whether developers can reason about real workflows.

### 1. First-Class Input Role

Multi-input outputs need a queryable role for each input. Today `customer`, `transaction`, `dimension`, `fact`, etc. can be hidden inside `source_ref` JSON.

Add:

```text
cp.input_edge.input_role text nullable
```

Examples:

```text
customer
transaction
detail_slice
reference_data
raw_file
```

Rules:

- `input_role` is optional for single-input flows.
- `input_role` should be populated for merge/join/aggregate flows.
- `source_ref` can still carry rich metadata, but the common role must be a first-class column.
- `cp.v_run_io` should expose `input_role` directly from the column, falling back to JSON only for old rows.

### 2. Stable Task Identity

Runs need a stable logical task identity for restartability and Airflow-style retries.

Add or verify:

```text
cp.run_log.task_key text nullable/required for new code
cp.run_log.attempt integer nullable/required for new code
```

Recommended logical uniqueness:

```text
workflow_run_id
task_key
pipeline_type
domain
dataset
business_date
```

`attempt` should track Airflow/Scheduler retry number. Decide and document whether retry:

- reuses the same `run_id` with incremented attempt metadata, or
- creates attempt rows under one logical task run.

For this implementation, prefer simple restart semantics:

```text
start_or_resume_run(workflow_run_id, task_key, pipeline_type, domain, dataset, business_date)
```

returns the same logical `run_id` for a restarted task unless the caller explicitly starts a new business refeed/workflow.

### 3. Clearer Edge / Transformation Types

The current edge types are too coarse for real workflows. Add or update `cp.edge_type` seed data to cover:

```text
raw_to_bronze
bronze_to_silver
silver_to_silver
silver_merge
silver_to_sink
sink_to_aggregate
aggregate_to_sink
orchestrates
```

If keeping existing values for compatibility, map them clearly:

```text
raw_to_curated      -> raw_to_bronze or raw_to_silver, depending on local naming
curated_to_canonical -> bronze_to_silver / silver_to_silver
merge_to_canonical  -> silver_merge or sink_to_aggregate
canonical_to_sink   -> silver_to_sink / aggregate_to_sink
```

Do not silently overload `merge_to_canonical` for aggregation in new code. Use a clearer edge type or add a first-class `transformation_type`.

### 4. Target Visibility Must Be Exercised

The target-visibility table exists, but the demo/workflow must populate it. The dashboard and tests should prove active-slice behavior rather than only proving lineage history.

Workflow requirements:

- after a successful target write and reconciliation, call activation
- Day 1, Day 2, Day 3 normal loads should each activate their target slices
- Day 2 refeed should deactivate the old Day 2 active slice and activate the corrected one

Expected query:

```sql
SELECT dataset, business_date, status, output_link_id
FROM ods.target_visibility
ORDER BY dataset, business_date, activated_at;
```

There should be `Y` rows for current business-visible slices and `N` rows for superseded refeed slices.

### 5. Standard Target Row Metadata Contract

Every target table written by the product should use a consistent metadata contract.

Required columns:

```text
_ods_output_link_id uuid not null
_ods_workflow_run_id text not null
_ods_loaded_at timestamptz not null default clock_timestamp()
```

Optional columns:

```text
_ods_source_file_id uuid nullable
_ods_active_flag char(1) nullable
```

Decision:

- Prefer active state in `ods.target_visibility`.
- Only add `_ods_active_flag` if a consuming team has a strong row-local filtering requirement.
- Do not make `_ods_active_flag` the only active-slice mechanism.

### 6. Typed `target_ref` Contracts

`target_ref` should stay JSONB, but the database/Python wrapper should validate type-specific minimums.

Required for all outputs:

```text
path
content_hash
version
```

Additional recommended requirements:

```text
kind = s3:
  path, content_hash, version, format

kind = postgres:
  path, content_hash, version, schema, table

kind = kafka:
  path or topic, content_hash, version, topic
```

If `kind` is missing, keep compatibility with the existing minimum contract but emit/use docs that new code should set `kind`.

### 7. Supersession Boundary

Keep lineage immutable. Do not delete or overwrite old output rows during refeed.

Business supersession should primarily live in `ods.target_visibility`:

```text
status = Y/N
superseded_by
replacement_key
```

Do not add `superseded_by_output_link_id` to `cp.output_link` unless the team explicitly wants lineage-level invalidation. If it is added, document that it is different from business active-state supersession.

## Stage Ownership Requirement

The current model attaches outputs and reconciliation mostly to `run_log`. That is acceptable for a simple run with one output, but it gets messy when:

- one run has many stages
- each stage can produce an output
- each stage can require reconciliation
- a developer wants to know which stage read which input and produced which output

Add optional stage-level ownership while keeping run-level ownership.

Required new references:

```text
cp.output_link.producer_stage_log_id nullable FK -> cp.run_stage_log(stage_log_id)
cp.input_edge.consumer_stage_log_id nullable FK -> cp.run_stage_log(stage_log_id)
cp.reconciliation_log.stage_log_id nullable FK -> cp.run_stage_log(stage_log_id)
cp.reconciliation_log.output_link_id nullable FK -> cp.output_link(output_link_id)
```

Rules:

- `consumer_run_id` remains on `cp.output_link` for run-level grouping and compatibility with existing logic.
- `producer_stage_log_id` is optional but should be populated by new code when an output is produced by a known stage.
- `consumer_stage_log_id` is optional but should be populated by new code when an input is consumed by a known stage.
- `reconciliation_log.stage_log_id` is optional but should be populated when a reconciliation belongs to a stage.
- `reconciliation_log.output_link_id` is optional but should be populated when the reconciliation checks one output.

This creates both query paths:

```text
run_log -> output_link -> input_edge
run_log -> run_stage_log -> output_link -> input_edge
run_log -> run_stage_log -> reconciliation_log
```

## Migration Strategy

Prefer an in-place physical rename migration.

For an existing database:

```sql
ALTER TABLE cp.lineage_link RENAME TO output_link;
ALTER TABLE cp.lineage_edge RENAME TO input_edge;

ALTER TABLE cp.output_link RENAME COLUMN lineage_link_id TO output_link_id;

ALTER TABLE cp.input_edge RENAME COLUMN lineage_edge_id TO input_edge_id;
ALTER TABLE cp.input_edge RENAME COLUMN lineage_link_id TO output_link_id;
ALTER TABLE cp.input_edge RENAME COLUMN upstream_lineage_link_id TO upstream_output_link_id;
```

Target tables:

```sql
ALTER TABLE ods.orders RENAME COLUMN _ods_lineage_link_id TO _ods_output_link_id;
ALTER TABLE ods.customer_transaction RENAME COLUMN _ods_lineage_link_id TO _ods_output_link_id;
ALTER TABLE ods.customer_transaction_daily RENAME COLUMN _ods_lineage_link_id TO _ods_output_link_id;
```

If a table already has both `_ods_lineage_link_id` and `_ods_output_link_id`, merge/backfill first, verify equality, then drop the old one:

```sql
UPDATE ods.customer_transaction
   SET _ods_output_link_id = COALESCE(_ods_output_link_id, _ods_lineage_link_id);

-- fail migration if mismatched values exist
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM ods.customer_transaction
        WHERE _ods_lineage_link_id IS NOT NULL
          AND _ods_output_link_id IS NOT NULL
          AND _ods_lineage_link_id <> _ods_output_link_id
    ) THEN
        RAISE EXCEPTION 'cannot drop _ods_lineage_link_id: mismatched _ods_output_link_id values';
    END IF;
END $$;

ALTER TABLE ods.customer_transaction DROP COLUMN _ods_lineage_link_id;
```

Repeat for all target tables that currently carry both names.

## Compatibility Views

The target state should not depend on views with renamed columns.

Remove or replace the current compatibility views:

- remove `cp.output_link` as a view over `cp.lineage_link`
- remove `cp.input_edge` as a view over `cp.lineage_edge`

After the physical rename, `cp.output_link` and `cp.input_edge` should be real tables.

If temporary backward compatibility is necessary, create old-name views over the new physical tables, not the other way around:

```sql
CREATE VIEW cp.lineage_link AS
SELECT
    output_link_id AS lineage_link_id,
    consumer_run_id,
    edge_type,
    sink_type,
    target_ref,
    transform_version,
    record_count,
    created_at
FROM cp.output_link;

CREATE VIEW cp.lineage_edge AS
SELECT
    input_edge_id AS lineage_edge_id,
    output_link_id AS lineage_link_id,
    upstream_run_id,
    upstream_output_link_id AS upstream_lineage_link_id,
    source_file_id,
    input_slot,
    edge_type,
    source_ref,
    record_count
FROM cp.input_edge;
```

These old-name views should be explicitly marked deprecated and should not be used by new code, tests, docs, or dashboard.

## Function and API Renames

Rename SQL functions and Python wrappers so new names are first-class.

Required preferred SQL names:

| Current | Required |
|---|---|
| `cp.write_lineage_link(...)` | `cp.write_output_link(...)` |
| `cp.write_link_then_rows(...)` | `cp.write_output_then_rows(...)` |
| `cp.reconcile_sink_link(...)` | `cp.reconcile_output_link(...)` |
| `cp.activate_target_visibility(... p_lineage_link_id ...)` | `cp.activate_target_visibility(... p_output_link_id ...)` |

Required preferred Python names:

| Current | Required |
|---|---|
| `control.lineage.write_link` | `control.lineage.write_output_link` |
| `control.lineage.write_link_then_rows` | `control.lineage.write_output_then_rows` |
| `control.recon.reconcile_sink_link` | `control.recon.reconcile_output_link` |
| `control.visibility.activate(lineage_link_id=...)` | `control.visibility.activate(output_link_id=...)` |

Old Python aliases may exist temporarily, but tests and docs must use the new names.

## Function Signatures

`write_output_link` should accept:

```python
control.write_output_link(
    conn,
    consumer_run_id=...,
    producer_stage_log_id=...,          # optional
    edge_type=...,
    target_ref=...,
    record_count=...,
    inputs=[
        {
            "consumer_stage_log_id": ...,    # optional
            "source_file_id": ...,           # raw input
            "upstream_output_link_id": ...,  # output input
            "input_slot": ...,
            "edge_type": ...,
            "source_ref": ...,
            "record_count": ...,
        }
    ],
    sink_type=...,
    transform_version=...,
)
```

`write_output_then_rows` should:

- create an `output_link`
- create its `input_edge` rows
- write target rows
- stamp `_ods_output_link_id`
- not stamp `_ods_lineage_link_id`
- optionally stamp `_ods_source_file_id` when row-level raw-file attribution is valid

## `input_reference_id` Is Read-Only Convenience

Do not store `input_reference_id` as a base-table column unless there is a strong reason.

Keep the normalized physical columns:

```text
cp.input_edge.source_file_id
cp.input_edge.upstream_output_link_id
```

Expose developer-friendly views/JSON as:

```text
input_reference_type = raw_file | output_link
input_reference_id = source_file_id OR upstream_output_link_id
```

This avoids duplicate persisted truth while making developer queries easier.

## Developer Views

Update `cp.v_run_io` to use physical new names directly.

Expected shape:

```text
run_id
workflow_run_id
pipeline_type
domain
dataset
business_date

stage_log_id
stage

output_link_id
output_path
output_content_hash
output_record_count

input_edge_id
input_reference_type
input_reference_id
input_path
input_role
source_file_id
upstream_output_link_id
```

For a stage-specific query:

```sql
SELECT *
FROM cp.v_run_io
WHERE run_id = '<run_id>'::uuid
ORDER BY stage, output_created_at, input_slot NULLS LAST;
```

## Dashboard Updates

The dashboard should stop using fallback helpers like:

```js
link.output_link_id || link.lineage_link_id
edge.input_edge_id || edge.lineage_edge_id
edge.upstream_output_link_id || edge.upstream_lineage_link_id
row._ods_output_link_id || row._ods_lineage_link_id
```

After physical rename, the snapshot should contain only:

```text
output_link_id
input_edge_id
upstream_output_link_id
_ods_output_link_id
```

Dashboard labels should use:

- `output_link`
- `input_edge`
- `output_link_id`
- `input_edge_id`
- `upstream_output_link_id`
- `_ods_output_link_id`

Avoid showing `input_edge_id` as if it is the business input. In diagrams, lead with:

```text
input_reference_type
input_reference_id
input_path
```

Then show:

```text
input_edge_id
```

as audit/debug detail.

## Harness Updates

Update `harness/customer_transaction_workflow.py`:

- export `links` from physical `cp.output_link`
- export `edges` from physical `cp.input_edge`
- target tables should contain `_ods_output_link_id` only
- no `_ods_lineage_link_id` mirror
- use `control.write_output_link` / `control.write_output_then_rows`
- pass stage IDs where known

The demo workflow should still produce:

- 3 normal days
- 1 transaction refeed
- customer and transaction canonicalization
- merge
- detail sink
- aggregation
- aggregate sink

## Tests Required

Add or update tests for:

1. Physical table exists:
   - `to_regclass('cp.output_link') = 'cp.output_link'`
   - `to_regclass('cp.input_edge') = 'cp.input_edge'`

2. Old physical tables do not remain as primary tables:
   - if `cp.lineage_link` exists, it must be a deprecated view only
   - if `cp.lineage_edge` exists, it must be a deprecated view only

3. Column names are physically new:
   - `cp.output_link.output_link_id`
   - `cp.input_edge.input_edge_id`
   - `cp.input_edge.output_link_id`
   - `cp.input_edge.upstream_output_link_id`

4. Target rows use only:
   - `_ods_output_link_id`

5. `write_output_link` creates:
   - one `cp.output_link`
   - one or more `cp.input_edge`

6. `write_output_then_rows` stamps:
   - `_ods_output_link_id`
   - not `_ods_lineage_link_id`

7. Stage ownership:
   - an output can be linked to `producer_stage_log_id`
   - an input edge can be linked to `consumer_stage_log_id`
   - reconciliation can be linked to `stage_log_id`
   - reconciliation can be linked to `output_link_id`

8. `cp.v_run_io` shows:
   - output path
   - input reference type/id
   - input path
   - stage when populated

9. `input_role`:
   - merge output has one `input_role='customer'`
   - merge output has one `input_role='transaction'`
   - `cp.v_run_io` exposes both roles

10. Task identity / restart:
   - `start_or_resume_run` returns the same logical run for the same task identity
   - retry/attempt metadata is visible
   - a new refeed workflow creates distinct business outputs without corrupting the old workflow

11. Target visibility:
   - normal target writes activate `Y` slices
   - Day 2 refeed deactivates the old Day 2 slice to `N`
   - only one `Y` row exists per replacement key

12. Target row metadata:
   - every target row has `_ods_output_link_id`
   - every target row has `_ods_workflow_run_id`
   - every target row has `_ods_loaded_at`
   - target rows do not carry `_ods_lineage_link_id`

13. Typed `target_ref`:
   - all outputs require `path`, `content_hash`, `version`
   - new S3 outputs include `kind='s3'` and `format`
   - new Postgres outputs include `kind='postgres'`, `schema`, and `table`

14. Customer/transaction demo still passes:
   - multi-day lineage
   - Day 2 refeed
   - row trace from `_ods_output_link_id`

## Acceptance Criteria

The implementation is done when:

- The physical DB uses `output_link` / `input_edge` names.
- The primary columns use `output_link_id`, `input_edge_id`, `upstream_output_link_id`.
- Target tables use `_ods_output_link_id`, not `_ods_lineage_link_id`.
- Compatibility views, if any, expose old names over new physical tables, not new names over old physical tables.
- Python preferred API uses output/input terminology.
- Stage-level ownership is available and used by new workflow code.
- `input_role` is a first-class column for multi-input outputs.
- `task_key` / attempt semantics are implemented or explicitly preserved if already present.
- Target visibility is populated by the demo and tested for refeed supersession.
- Target row metadata is consistent and no longer carries `_ods_lineage_link_id`.
- `target_ref` has the base contract plus type-specific validation for new code.
- Dashboard and docs no longer explain that `cp.output_link` is a view over `cp.lineage_link`.
- Tests prove physical names, stage ownership, restart/refeed behavior, and row tracing.
