# Customer Transaction Lineage Workflow And Dashboard Spec

**Status:** draft  
**Purpose:** define an end-to-end demo workflow that proves multi-input lineage, target row traceability, and dashboard usability before implementation.

## Goal

Build a realistic workflow that starts with two raw files per business date, transforms both into silver outputs, merges them, writes detail rows to Postgres, aggregates those detail rows, and writes aggregate rows to Postgres.

The demo should cover three business dates and one corrected refeed for the middle date. The three-day shape proves that lineage works across slices, not just in one small happy-path day. The refeed proves the control plane can distinguish original data from corrected data for the same business slice.

The workflow must be inspectable through a dashboard with three tabs:

1. Control-table lineage diagram for a selected workflow run.
2. React Flow execution graph showing stages, inputs, and outputs.
3. Target-table row picker that traces a selected Postgres row back to its raw inputs.

## Business Scenario

The business process is customer transaction analytics.

Inputs:

- `customer` raw file
- `transaction` raw file

Outputs:

- `ods.customer_transaction`
- `ods.customer_transaction_daily`

The core lineage requirement is that a row in either Postgres table can be traced back to the raw customer file and the raw transaction file that contributed to it, including distinguishing an original transaction file from a corrected refeed transaction file for the same business date.

## Scenario Scope

The workflow should load three business dates:

| Scenario | Business Date | Customer File | Transaction File | Expected Result |
|---|---|---|---|---|
| Day 1 normal load | `2026-05-28` | original | original | detail + aggregate rows trace to Day 1 raw files |
| Day 2 normal load | `2026-05-29` | original | original | original Day 2 rows trace to original Day 2 raw files |
| Day 3 normal load | `2026-05-30` | original | original | detail + aggregate rows trace to Day 3 raw files |
| Day 2 refeed | `2026-05-29` | reuse original customer | corrected transaction | corrected Day 2 rows trace to original customer + corrected transaction |

The refeed should be for the transaction file only. That gives a clean proof that one input can be corrected while the other input for the same business date remains unchanged.

Expected lineage outcomes:

```text
Day 1 target row
  -> Day 1 customer original raw
  -> Day 1 transaction original raw

Day 2 original target row
  -> Day 2 customer original raw
  -> Day 2 transaction original raw

Day 2 corrected target row
  -> Day 2 customer original raw
  -> Day 2 transaction corrected raw

Day 3 target row
  -> Day 3 customer original raw
  -> Day 3 transaction original raw
```

If target visibility / active-slice semantics are implemented, the Day 2 corrected rows should be active and the Day 2 original rows should be inactive for business-facing views. If target visibility is not implemented yet, both original and corrected rows may remain physically present, but the dashboard must still show which lineage link each row came from.

## Workflow Shape

```text
raw customer file
  -> ingest customer
  -> customer silver

raw transaction file
  -> ingest transaction
  -> transaction silver

customer silver + transaction silver
  -> merge customer_transaction
  -> upsert ods.customer_transaction

ods.customer_transaction / merged output
  -> aggregate customer_transaction_daily
  -> upsert ods.customer_transaction_daily
```

This shape is executed once for each normal business date, then once more for the Day 2 transaction refeed.

## Control-Plane Runs

Each execution creates one shared `workflow_run_id` across all runs in that execution.

For the base demo, there are four executions:

1. Day 1 normal load
2. Day 2 normal load
3. Day 3 normal load
4. Day 2 transaction refeed

The dashboard should be able to inspect one selected `workflow_run_id` and should also be able to show the business-date relationship across the four executions.

Expected logical runs per execution:

| Step | `pipeline_type` | `dataset` | Description |
|---|---|---|---|
| 1 | `ingestion` | `customer` | Register and ingest raw customer file |
| 2 | `ingestion` | `transaction` | Register and ingest raw transaction file |
| 3 | `canonicalization` | `customer` | Transform customer into silver |
| 4 | `canonicalization` | `transaction` | Transform transaction into silver |
| 5 | `merge` | `customer_transaction` | Join customer and transaction silver outputs |
| 6 | `sink` | `customer_transaction` | Upsert merged detail rows to Postgres |
| 7 | `aggregation` | `customer_transaction_daily` | Aggregate detail rows |
| 8 | `sink` | `customer_transaction_daily` | Upsert aggregate rows to Postgres |

Each run should have:

- one `cp.run_log` row
- at least one `cp.run_stage_log` row
- terminal status
- input/output record counts
- reconciliation rows where appropriate

The refeed execution should preserve correction semantics:

- the corrected transaction file gets a new `cp.file_catalogue.file_id`
- the corrected transaction ingestion run is tied to that corrected file
- the corrected transaction silver output has a different `target_ref.content_hash`
- the corrected merge output has a different `target_ref.content_hash`
- the corrected detail sink link has a different `lineage_link_id`
- the corrected aggregate sink link has a different `lineage_link_id`
- the original Day 2 lineage remains queryable for audit

## Input Files

### Customer File

Example payload:

```json
[
  {
    "customer_id": "C001",
    "customer_name": "Ada Lovelace",
    "segment": "premium"
  },
  {
    "customer_id": "C002",
    "customer_name": "Grace Hopper",
    "segment": "standard"
  }
]
```

Control expectations:

- one `cp.file_catalogue` row
- one `raw_to_curated` lineage link
- the lineage edge anchors to `source_file_id`
- `record_count` equals the number of customer rows

### Transaction File

Example payload:

```json
[
  {
    "transaction_id": "T100",
    "customer_id": "C001",
    "amount": 125.50
  },
  {
    "transaction_id": "T101",
    "customer_id": "C001",
    "amount": 74.50
  },
  {
    "transaction_id": "T102",
    "customer_id": "C002",
    "amount": 33.00
  }
]
```

Control expectations:

- one `cp.file_catalogue` row
- one `raw_to_curated` lineage link
- the lineage edge anchors to `source_file_id`
- `record_count` equals the number of transaction rows

### Corrected Transaction Refeed File

The refeed file uses the same business date as Day 2 but different raw content.

Example correction:

```json
[
  {
    "transaction_id": "T100",
    "customer_id": "C001",
    "amount": 125.50
  },
  {
    "transaction_id": "T101",
    "customer_id": "C001",
    "amount": 79.50
  },
  {
    "transaction_id": "T102",
    "customer_id": "C002",
    "amount": 33.00
  }
]
```

The important point is not the specific amount; it is that the corrected file has a different file hash and therefore a distinct raw file identity.

Control expectations:

- same `business_date` as the original Day 2 transaction file
- same `domain` and `dataset`
- different `file_md5`
- different `file_id`
- new ingestion/silver/merge/sink lineage for the corrected chain
- original Day 2 lineage remains available

## Silver Transformations

Each raw file is transformed independently into a silver output.

Customer silver:

```text
customer ingestion run
  -> raw_to_curated link
  -> customer canonicalization run
  -> curated_to_canonical link
```

Transaction silver:

```text
transaction ingestion run
  -> raw_to_curated link
  -> transaction canonicalization run
  -> curated_to_canonical link
```

Each silver lineage link must have:

- `target_ref.path`
- `target_ref.content_hash`
- `target_ref.version`
- `record_count`
- exact `upstream_lineage_link_id`

## Merge

The merge step joins customer silver and transaction silver on `customer_id`.

Input lineage edges:

- customer silver link
- transaction silver link

Output dataset:

```text
customer_transaction
```

Example merged row:

```json
{
  "transaction_id": "T100",
  "customer_id": "C001",
  "customer_name": "Ada Lovelace",
  "segment": "premium",
  "amount": 125.50,
  "business_date": "2026-05-30"
}
```

The merge lineage link should show two upstream edges, one for each silver input.

The merged output must preserve provenance to both raw files. A target row derived from transaction `T100` and customer `C001` should trace back to:

- the raw customer file
- the raw transaction file

For the Day 2 refeed, the corrected merge must consume:

- the Day 2 original customer silver output
- the Day 2 corrected transaction silver output

It must not accidentally consume the Day 1 or Day 3 transaction output, and it must not accidentally consume the Day 2 original transaction output when the corrected transaction output is intended.

## Detail Postgres Sink

Target table:

```text
ods.customer_transaction
```

Required columns:

```sql
row_id BIGSERIAL PRIMARY KEY,
payload JSONB NOT NULL,
_ods_workflow_run_id TEXT,
_ods_lineage_link_id UUID NOT NULL REFERENCES cp.lineage_link(lineage_link_id)
```

The sink writes rows through the sanctioned link-then-rows path.

Lineage shape:

```text
merge_to_canonical link
  -> canonical_to_sink link
  -> ods.customer_transaction rows
```

Clicking a detail row should trace to both raw files.

For Day 2, clicking an original detail row and a corrected detail row should show different transaction raw origins.

## Aggregation

The aggregate step reads the merged/detail output and groups by:

- `business_date`
- `customer_id`

Example aggregate row:

```json
{
  "business_date": "2026-05-30",
  "customer_id": "C001",
  "customer_name": "Ada Lovelace",
  "transaction_count": 2,
  "total_amount": 200.00
}
```

Output dataset:

```text
customer_transaction_daily
```

The aggregation should create its own control-plane run and output link rather than hiding inside the sink step. That keeps aggregate lineage visible as a first-class transformation.

For Day 2, the corrected aggregate should reflect the corrected transaction amount/counts and trace back to the corrected transaction file.

## Aggregate Postgres Sink

Target table:

```text
ods.customer_transaction_daily
```

Required columns:

```sql
row_id BIGSERIAL PRIMARY KEY,
payload JSONB NOT NULL,
_ods_workflow_run_id TEXT,
_ods_lineage_link_id UUID NOT NULL REFERENCES cp.lineage_link(lineage_link_id)
```

Lineage shape:

```text
customer_transaction detail output
  -> aggregation run
  -> aggregate output
  -> canonical_to_sink link
  -> ods.customer_transaction_daily rows
```

Clicking an aggregate row should trace back through the aggregate lineage to the merged detail lineage and then to both raw files.

## Dashboard

### Tab 1: Control Links

Purpose: show the actual control-table lineage for a selected workflow run, with enough context to compare business dates and identify refeed/correction lineage.

Inputs:

- selected `workflow_run_id`
- selected `business_date`
- optional selected `run_id`
- optional focused `lineage_link_id`

Should display:

- runs
- lineage links
- lineage edges
- upstream/downstream relationships
- target refs
- record counts
- raw file anchors
- trace chain for the focused link
- original versus corrected lineage for a refed slice, when available

This tab answers:

```text
What did this workflow produce?
Which upstream outputs did each link consume?
Which raw files contributed to a selected output?
Is this row/output from the original or corrected Day 2 transaction file?
```

### Tab 2: React Flow Run Graph

Purpose: show the workflow visually.

Nodes:

- one node per `cp.run_log` row
- optionally include target table nodes
- each node shows:
  - pipeline type
  - dataset
  - status
  - stage names
  - record count in
  - record count out
  - short run id

Edges:

- derived from `cp.lineage_edge.upstream_run_id`
- labelled with edge type and record count
- must show both customer and transaction feeding the merge
- must show merge feeding the detail sink
- must show detail/aggregate lineage feeding the aggregate sink

The graph should support two useful views:

- selected execution: one `workflow_run_id`
- scenario overview: all three days plus the Day 2 refeed, grouped by business date

This tab answers:

```text
What ran?
What order did it run in?
What did each run read and write?
Which execution is the Day 2 refeed?
```

### Tab 3: Target Rows

Purpose: inspect target-table rows and trace them back to raw.

User flow:

1. User selects `ods.customer_transaction` or `ods.customer_transaction_daily`.
2. User optionally filters by `business_date`.
3. Dashboard lists rows.
4. User clicks a row.
5. Dashboard reads `_ods_lineage_link_id`.
6. Dashboard switches to Tab 1.
7. Tab 1 focuses the corresponding lineage link.
8. Trace panel shows contributing raw files.

For Day 2, this tab should make the original and corrected rows visibly distinguishable. The minimum acceptable distinction is the `_ods_lineage_link_id` and trace output; if target visibility is implemented, it should also show active/inactive status.

This tab answers:

```text
Where did this exact business row come from?
```

## Snapshot Export

The first dashboard version can be a static app backed by a generated JSON snapshot.

Snapshot shape:

```json
{
  "workflow_run_id": "...",
  "generated_at": "...",
  "scenario": {
    "business_dates": ["2026-05-28", "2026-05-29", "2026-05-30"],
    "refeed_business_date": "2026-05-29"
  },
  "executions": [],
  "runs": [],
  "links": [],
  "files": [],
  "tables": {
    "customer_transaction": [],
    "customer_transaction_daily": []
  },
  "traces": {}
}
```

`executions` should include one entry per normal/refeed execution:

- `workflow_run_id`
- `business_date`
- `execution_type`, such as `normal` or `refeed`
- `refeed_of_workflow_run_id`, when applicable
- short description for display

Each run entry should include:

- `run_id`
- `workflow_run_id`
- `pipeline_type`
- `domain`
- `dataset`
- `business_date`
- `trigger_type`
- `status`
- `record_count_in`
- `record_count_out`
- nested `stages`

Each link entry should include:

- `lineage_link_id`
- `consumer_run_id`
- `edge_type`
- `sink_type`
- `target_ref`
- `transform_version`
- `record_count`
- nested `edges`

Each target row in `tables` must include:

- `row_id`
- `payload`
- `_ods_workflow_run_id`
- `_ods_lineage_link_id`
- optional active/inactive visibility fields if target visibility exists

Each trace entry is keyed by `lineage_link_id`.

## Tests

Required tests:

1. Workflow creates customer and transaction raw file registrations for all three normal days.
2. Customer raw produces customer silver lineage for each business date.
3. Transaction raw produces transaction silver lineage for each business date.
4. Each normal merge link has two upstream edges.
5. Detail sink writes expected Postgres rows for all three days.
6. Aggregate sink writes expected Postgres rows for all three days.
7. A Day 1 detail row traces only to Day 1 raw files.
8. A Day 3 detail row traces only to Day 3 raw files.
9. The Day 2 refeed creates a distinct corrected transaction file identity.
10. The Day 2 corrected merge consumes original customer silver plus corrected transaction silver.
11. A Day 2 original detail row traces to original Day 2 transaction raw.
12. A Day 2 corrected detail row traces to corrected Day 2 transaction raw.
13. A Day 2 corrected aggregate row traces back to the corrected transaction raw.
14. Dashboard snapshot includes executions, runs, links, tables, files, and traces.
15. A selected target row has enough data to focus Tab 1 by `_ods_lineage_link_id`.
16. If target visibility exists, Day 2 corrected rows are active and Day 2 original rows are inactive.

## Non-Goals

This demo does not need to implement:

- real Spark jobs
- real S3 IO
- real upsert conflict semantics
- authentication
- live API server
- production-grade dashboard routing
- target active/inactive visibility semantics

The first version can be a static dashboard generated from a Postgres-backed workflow snapshot.

## Recommendation

Build this as a dedicated demo workflow, not by changing the generic fake stages.

Reasons:

- it keeps existing small harness tests clean
- it gives us a business-readable lineage story
- it exercises multi-input lineage, target row tracing, and aggregate lineage in one place
- it becomes a useful regression fixture for future refeed/restart work

## Implementation Split

Claude Code can implement the backend/demo workflow first:

- target tables/migrations for `ods.customer_transaction` and `ods.customer_transaction_daily`
- customer/transaction three-day workflow fixture
- Day 2 transaction refeed fixture
- control-plane writes for all runs, links, edges, stages, reconciliation
- snapshot export JSON
- backend tests listed above

Codex can implement the dashboard afterward:

- static or API-backed UI
- Tab 1 control-link lineage explorer
- Tab 2 React Flow graph
- Tab 3 target row picker
- row click to Tab 1 focused lineage link
- visual differentiation for business date and refeed/corrected lineage
