# Airflow Orchestrator Identity and Policy/Claims Workflow Spec

## Purpose

Add first-class Airflow cross-reference fields to the control plane now, then
build a new Airflow-oriented workflow for policy and claims files.

This is needed because support users and downstream monitoring systems must be
able to answer both sides of the question:

```text
What did Airflow think happened?
What did the ODS control plane actually record?
```

Do not rely on naming convention alone. Airflow identity should be queryable
from `cp.run_log` and visible in dashboard/export payloads.

## Required End State

1. `cp.run_log` carries first-class orchestrator identity.
2. `control.runs.start(...)` accepts an optional orchestrator payload.
3. Existing callers keep working.
4. New Airflow policy/claims workflow writes normal control-plane rows:
   `run_log`, `run_stage_log`, `output_link`, `input_edge`, target rows, target
   visibility.
5. Demo data covers 3 business dates and one refeed.
6. Dashboard snapshots expose orchestrator identity.
7. JSON/OpenLineage-style exports include orchestrator identity.

## Non-Goals

- Do not replace the control plane with Airflow metadata.
- Do not make Airflow the lineage source of truth.
- Do not add a workflow table for this change.
- Do not physically rename `lineage_link` / `lineage_edge` in this work unless
  that rename is already in progress elsewhere.
- Do not require Airflow to run for existing harness tests.

## Core Model Decision

Keep these identities separate:

```text
workflow_run_id
  ODS/control-plane workflow execution id.
  Groups related ODS runs for one business execution/refeed.

orchestrator_run_id
  External orchestrator execution id.
  For Airflow this is dag_run_id.
```

Do not overload `workflow_run_id` with Airflow `dag_run_id`. They may be equal
in some demos, but the model should not require it.

## Migration 020: Orchestrator Identity on run_log

Create `db/migrations/020_orchestrator_identity.sql`.

Add nullable columns first so existing data and tests remain compatible:

```sql
ALTER TABLE cp.run_log
  ADD COLUMN IF NOT EXISTS orchestrator_type text,
  ADD COLUMN IF NOT EXISTS orchestrator_dag_id text,
  ADD COLUMN IF NOT EXISTS orchestrator_run_id text,
  ADD COLUMN IF NOT EXISTS orchestrator_task_id text,
  ADD COLUMN IF NOT EXISTS orchestrator_try_number integer,
  ADD COLUMN IF NOT EXISTS orchestrator_map_index integer,
  ADD COLUMN IF NOT EXISTS orchestrator_url text,
  ADD COLUMN IF NOT EXISTS orchestrator_payload jsonb NOT NULL DEFAULT '{}'::jsonb;
```

Recommended indexes:

```sql
CREATE INDEX IF NOT EXISTS ix_run_log_orchestrator_run
  ON cp.run_log (orchestrator_type, orchestrator_dag_id, orchestrator_run_id);

CREATE INDEX IF NOT EXISTS ix_run_log_orchestrator_task
  ON cp.run_log (
    orchestrator_type,
    orchestrator_dag_id,
    orchestrator_run_id,
    orchestrator_task_id
  );
```

Do not create a uniqueness constraint on Airflow identity yet. The current
control-plane restart identity is based on:

```text
workflow_run_id, pipeline_type, domain, dataset, business_date, file_id
```

Airflow attempts should map onto that logical run identity rather than minting
one `run_log` row per retry. Store the latest/current Airflow try number in
`run_log.orchestrator_try_number`. Attempt-level detail remains in
`run_stage_log.attempt`.

## cp.start_run Change

Update `cp.start_run` to accept an optional JSONB orchestrator argument at the
end of the signature.

Target signature:

```sql
CREATE OR REPLACE FUNCTION cp.start_run(
    p_workflow_run_id text,
    p_pipeline_type text,
    p_domain text,
    p_dataset text,
    p_business_date date,
    p_trigger_type text,
    p_file_id uuid DEFAULT NULL,
    p_replay_of_run_id uuid DEFAULT NULL,
    p_orchestrator jsonb DEFAULT '{}'::jsonb
) RETURNS uuid
```

Extract these keys from `p_orchestrator`:

```text
type
dag_id
run_id
task_id
try_number
map_index
url
payload
```

Column mapping:

```text
p_orchestrator.type       -> orchestrator_type
p_orchestrator.dag_id     -> orchestrator_dag_id
p_orchestrator.run_id     -> orchestrator_run_id
p_orchestrator.task_id    -> orchestrator_task_id
p_orchestrator.try_number -> orchestrator_try_number
p_orchestrator.map_index  -> orchestrator_map_index
p_orchestrator.url        -> orchestrator_url
p_orchestrator.payload    -> orchestrator_payload
```

`orchestrator_payload` should preserve the whole supplied JSON object or at
least the nested `payload` object plus useful Airflow context. Prefer preserving
the whole object so future fields are not lost.

On `ON CONFLICT` restart reuse, update:

```text
status = 'running'
finished_at = NULL
error = NULL
orchestrator_* = values from the new call
```

This ensures an Airflow retry updates the logical run with the current
`try_number` and URL.

Keep the existing restart identity behavior from migration `013`.

## Python API Change

Update `control/runs.py`.

Current:

```python
def start(conn, *, workflow_run_id, pipeline_type, domain, dataset, business_date,
          trigger_type, file_id=None, replay_of_run_id=None, commit=True) -> str:
```

Target:

```python
def start(conn, *, workflow_run_id, pipeline_type, domain, dataset, business_date,
          trigger_type, file_id=None, replay_of_run_id=None,
          orchestrator=None, commit=True) -> str:
```

Rules:

- `orchestrator` is optional.
- If omitted, pass `{}` to SQL.
- Use `Jsonb(orchestrator or {})`.
- Existing tests and callers must continue working.

Example Airflow call:

```python
run_id = runs.start(
    conn,
    workflow_run_id=workflow_run_id,
    pipeline_type="canonicalization",
    domain="insurance",
    dataset="policy",
    business_date=business_date,
    trigger_type="airflow",
    orchestrator=airflow_orchestrator_context(context),
    commit=False,
)
```

Helper to add in Airflow workflow code:

```python
def airflow_orchestrator_context(context):
    ti = context["ti"]
    dag_run = context["dag_run"]
    dag = context["dag"]
    return {
        "type": "airflow",
        "dag_id": dag.dag_id,
        "run_id": dag_run.run_id,
        "task_id": ti.task_id,
        "try_number": ti.try_number,
        "map_index": getattr(ti, "map_index", None),
        "url": getattr(ti, "log_url", None),
        "payload": {
            "execution_date": str(getattr(dag_run, "execution_date", "")),
            "logical_date": str(getattr(dag_run, "logical_date", "")),
        },
    }
```

## Snapshot and Dashboard Changes

Update `harness/customer_transaction_workflow.py` snapshot query logic if it is
the shared exporter, or create a shared snapshot exporter, so `runs` include:

```text
orchestrator_type
orchestrator_dag_id
orchestrator_run_id
orchestrator_task_id
orchestrator_try_number
orchestrator_map_index
orchestrator_url
orchestrator_payload
```

Dashboard updates:

- Workflow tab: no required UI change, but JSON and OL exports must include
  orchestrator fields.
- Developer Model tab: task click should show `orchestrator={...}` in
  `runs.start(...)` when present.
- Process Model tab: optional, show orchestrator identity inside the blue task
  card if present.
- OpenLineage export: include an `ods_orchestrator` run facet:

```json
{
  "_producer": "ods-control-plane-dashboard",
  "_schemaURL": "https://example.com/ods-control-plane/facets/1-0-0/OdsOrchestratorFacet.json",
  "type": "airflow",
  "dag_id": "...",
  "run_id": "...",
  "task_id": "...",
  "try_number": 1,
  "map_index": -1,
  "url": "..."
}
```

## New Workflow: Airflow Policy/Claims

Add a new workflow for insurance policy and claim data.

Preferred location:

```text
dags/policy_claims_dag.py
```

If Airflow is not installed in the local dev environment, keep this import-safe:

- Airflow imports should be guarded or isolated.
- Provide a non-Airflow harness runner that uses the same business functions so
  tests can run without Airflow.

Suggested supporting files:

```text
harness/policy_claims_workflow.py
dashboard/data/policy-claims-workflow.json
```

## Policy/Claims Workflow Shape

Normal workflow per business date:

```text
policy raw file              claim raw file
      |                           |
policy ingestion             claim ingestion
      |                           |
policy canonicalization      claim canonicalization
      |                           |
      +--------- merge policy_claim ---------+
                         |                   |
             sink policy_claim        aggregate policy_claim_daily
                         |                   |
                  Postgres detail      sink policy_claim_daily
                                             |
                                      Postgres aggregate
```

Pipeline types and datasets:

| Step | pipeline_type | dataset |
|---|---|---|
| policy raw ingest | `ingestion` | `policy` |
| claim raw ingest | `ingestion` | `claim` |
| policy silver | `canonicalization` | `policy` |
| claim silver | `canonicalization` | `claim` |
| merge | `merge` | `policy_claim` |
| detail sink | `sink` | `policy_claim` |
| aggregate | `aggregation` | `policy_claim_daily` |
| aggregate sink | `sink` | `policy_claim_daily` |

Domain:

```text
insurance
```

## Three-Day Demo Data

Create normal workflows for these business dates:

```text
2026-05-28
2026-05-29
2026-05-30
```

Create one refeed for:

```text
2026-05-29
```

Recommended refeed:

```text
claim refeed
```

Reason:

- It mirrors the existing customer/transaction demo.
- It proves the refeed can rerun one branch.
- It proves merge can reuse the original policy silver output while consuming
  the corrected claim silver output.

Normal Day 2:

```text
policy original -> policy silver
claim original  -> claim silver
merge original policy_claim
sink detail
aggregate
sink aggregate
```

Day 2 refeed:

```text
claim corrected -> claim silver corrected
merge reuses original policy silver + corrected claim silver
sink detail corrected
aggregate corrected
sink aggregate corrected
```

Refeed metadata:

```text
execution_type = refeed
refeed_of_workflow_run_id = original Day 2 workflow_run_id
trigger_type = airflow or replay, depending on implementation
```

If the workflow is truly invoked by Airflow, use:

```text
trigger_type = airflow
orchestrator_type = airflow
```

Do not use `trigger_type = manual` for Airflow-driven runs.

## Input Data Model

Policy raw fields:

```text
policy_id
customer_id
policy_type
effective_date
expiry_date
premium_amount
business_date
```

Claim raw fields:

```text
claim_id
policy_id
claim_date
claim_status
claim_amount
business_date
```

Merged `policy_claim` detail fields:

```text
business_date
policy_id
customer_id
policy_type
premium_amount
claim_id
claim_date
claim_status
claim_amount
```

Aggregate `policy_claim_daily` fields:

```text
business_date
policy_type
claim_count
total_claim_amount
open_claim_count
closed_claim_count
```

All target rows must include the existing ODS stamps:

```text
_ods_workflow_run_id
_ods_lineage_link_id
_ods_output_link_id
_ods_active_flag, if target visibility uses it
```

## Target Tables

Add or ensure demo target tables:

```text
ods.policy_claim
ods.policy_claim_daily
```

If the current target-writing helper is generic, reuse it. If not, add the two
tables in the policy/claims workflow harness setup.

## Edge Types

Use existing edge types where possible:

```text
raw_to_curated
curated_to_canonical
merge_to_canonical
canonical_to_sink
detail_to_aggregate, if already implemented
```

If `detail_to_aggregate` does not exist yet, add it rather than overloading
`merge_to_canonical` for aggregation. The leaner long-term choice is a new
edge type, not a new transformation column.

## Input Roles

If `input_role` exists on `input_edge`, populate it.

Merge input edges:

```text
policy
claim
```

Aggregation input edge:

```text
detail
```

If `input_role` does not exist yet, put the role into `source_ref`:

```json
{"input_role": "policy"}
```

Do not block this workflow on the `input_role` column unless that column is
already being implemented.

## Airflow DAG Requirements

Create an Airflow DAG named:

```text
ods_policy_claims
```

Suggested tasks:

```text
ingest_policy
ingest_claim
canonicalize_policy
canonicalize_claim
merge_policy_claim
sink_policy_claim
aggregate_policy_claim_daily
sink_policy_claim_daily
```

Dependencies:

```text
ingest_policy >> canonicalize_policy
ingest_claim  >> canonicalize_claim
[canonicalize_policy, canonicalize_claim] >> merge_policy_claim
merge_policy_claim >> sink_policy_claim
sink_policy_claim >> aggregate_policy_claim_daily
aggregate_policy_claim_daily >> sink_policy_claim_daily
```

The customer/transaction dashboard taught us an important visual rule:

```text
policy and claim branches are parallel, not serial.
```

The generated snapshot should preserve enough metadata for the dashboard to
show policy and claim side by side.

## Airflow Context and workflow_run_id

For Airflow runs, derive or pass one ODS workflow id per DAG run:

Recommended:

```text
workflow_run_id = deterministic UUID or text derived from dag_id + dag_run_id
```

If using Airflow XCom:

1. First task creates/generates `workflow_run_id`.
2. Downstream tasks read the same `workflow_run_id`.

Do not let each task generate its own workflow id.

For the refeed DAG run:

```text
workflow_run_id = new ODS workflow id for the corrected execution
refeed_of_workflow_run_id = original Day 2 ODS workflow id
```

## Restart and Retry Behavior

Airflow retry of the same task should:

- call `runs.start(...)` again with the same ODS identity fields
- reuse the existing `run_id` where the existing restart identity applies
- update `orchestrator_try_number`
- create/update stage attempts through `stages.stage_scope(..., attempt=try_number)`

The retry should not create duplicate discovered upstream runs for ingestion,
canonicalization, merge, or aggregation.

Sink behavior may still create a new sink run if current model excludes sink
from restart identity. If so, target visibility must prevent partial/stale rows
from becoming business-active.

## Dashboard Expectations

The existing dashboard should be able to load either:

```text
dashboard/data/demo-workflow.json
dashboard/data/policy-claims-workflow.json
```

Accept either approach:

1. Add a dataset selector to the dashboard.
2. Replace the demo snapshot during policy/claims demo.
3. Add a query parameter, for example `?data=policy-claims-workflow.json`.

Minimum requirement:

- The new workflow is viewable in the current dashboard without hand-editing JS.

Workflow tab:

- Show four executions: three normal, one refeed.
- On the refeed card, mark `claim` as changed and `policy_claim` /
  `policy_claim_daily` as impacted.

Process Model:

- Policy and claim branches must render side by side.
- Each task shows inputs, outputs, and stages.
- Stored values remain bold.

Developer Model:

- Task snippets include `orchestrator={...}` for Airflow runs.
- Input clicks show only the input payload.
- Output clicks show `lineage.write_output_link(...)` or
  `lineage.write_output_then_rows(...)`.

OL button:

- Generated OpenLineage-style events include `ods_orchestrator` facet.

## Tests

Add tests for the orchestrator identity change:

1. `runs.start(...)` with no orchestrator still works.
2. `runs.start(...)` with Airflow orchestrator writes all new columns.
3. Re-calling `runs.start(...)` for the same logical run with higher
   `try_number` reuses the run and updates `orchestrator_try_number`.
4. Snapshot export includes orchestrator fields.

Add tests for policy/claims workflow:

1. Three normal workflow executions are produced.
2. One claim refeed is produced for `2026-05-29`.
3. Refeed workflow points to original Day 2 workflow.
4. Refeed merge consumes:
   - original Day 2 policy silver output
   - corrected Day 2 claim silver output
5. Target detail rows for the corrected Day 2 slice are stamped with the refeed
   output link.
6. Aggregate rows trace back to the corrected detail output.
7. Active-slice control marks corrected Day 2 outputs active and original Day 2
   affected target slice inactive.

## Acceptance Criteria

- Migration `020_orchestrator_identity.sql` applies cleanly.
- Existing tests pass.
- Existing customer/transaction demo still works.
- Policy/claims Airflow-oriented workflow produces 3 normal days plus one
  refeed.
- Dashboard can show the policy/claims workflow.
- Refeed is understandable:
  - claim is the changed branch
  - policy is reused from original Day 2
  - policy_claim and policy_claim_daily are impacted/recomputed
- Developer Model shows Airflow orchestrator context in task creation snippets.
- OpenLineage-style export includes orchestrator facet.

## Implementation Order

1. Migration `020_orchestrator_identity.sql`.
2. Update `control/runs.py`.
3. Add tests for orchestrator identity.
4. Update snapshot exporter to include orchestrator fields.
5. Update dashboard JSON/OL/Developer Model for orchestrator fields.
6. Implement policy/claims business workflow functions.
7. Implement Airflow DAG wrapper.
8. Add non-Airflow harness/demo runner for local tests.
9. Add policy/claims dashboard snapshot.
10. Add policy/claims workflow tests.

