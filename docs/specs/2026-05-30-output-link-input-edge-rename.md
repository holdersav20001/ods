# Rename lineage_link/lineage_edge to output_link/input_edge

## Purpose

The current control-plane model is correct, but the names are confusing for developers:

- `cp.lineage_link` is not a generic link between two things. It represents a produced output.
- `cp.lineage_edge` is the relationship that says which input(s) were used to produce that output.
- `upstream_lineage_link_id` points to a previous output, not to a previous edge.

Rename the public concepts so the model reads naturally:

- output rows live in `cp.output_link`
- input relationships live in `cp.input_edge`
- downstream target rows point to `_ods_output_link_id`

This is a naming/spec cleanup only. The data model and behavior should remain the same.

## Current Mental Model

Current physical/conceptual names:

```text
cp.lineage_link
  one row per output produced by a run

cp.lineage_edge
  one row per input used to produce that output

cp.lineage_edge.upstream_lineage_link_id
  points to a previous output's lineage_link_id
```

Human-readable model:

```text
output_link = the thing produced
input_edge = what that thing was made from
upstream_output_link_id = previous thing produced
```

## Target Names

Use these names in schema, wrappers, tests, docs, and dashboard labels.

| Current name | Target name |
| --- | --- |
| `cp.lineage_link` | `cp.output_link` |
| `cp.lineage_edge` | `cp.input_edge` |
| `lineage_link_id` | `output_link_id` |
| `lineage_edge_id` | `input_edge_id` |
| `consumer_run_id` | keep as `consumer_run_id` |
| `upstream_lineage_link_id` | `upstream_output_link_id` |
| `_ods_lineage_link_id` | `_ods_output_link_id` |
| `write_lineage_link` | `write_output_link` |
| `write_link_then_rows` | `write_output_then_rows` |
| `run_output_link` | keep as `run_output_link` |
| `activate_target_visibility(... lineage_link_id ...)` | `activate_target_visibility(... output_link_id ...)` |

Keep `edge_type` as-is for now. It describes the production relationship:

- `raw_to_curated`
- `curated_to_canonical`
- `merge_to_canonical`
- `canonical_to_sink`
- `quarantine`
- `replay`
- `orchestrates`

Do not rename `edge_type` in this change.

## Design Rules

### Rule 1: One output, many inputs

One output link can have many input edges.

```text
cp.output_link
  output_link_id = M300
  target_ref = customer_transaction silver

cp.input_edge
  input_edge_id = E1
  output_link_id = M300
  upstream_output_link_id = customer silver output

cp.input_edge
  input_edge_id = E2
  output_link_id = M300
  upstream_output_link_id = transaction silver output
```

### Rule 2: Raw file inputs use source_file_id

For raw file leaves:

```text
cp.input_edge.source_file_id = file_id
cp.input_edge.upstream_output_link_id = NULL
```

For run-to-run inputs:

```text
cp.input_edge.source_file_id = NULL, unless row-level file attribution is valid
cp.input_edge.upstream_output_link_id = previous cp.output_link.output_link_id
```

### Rule 3: Target rows store output identity

Target rows should point to the output link that wrote them:

```text
_ods_output_link_id = cp.output_link.output_link_id
_ods_workflow_run_id = workflow_run_id
```

For backward compatibility during migration, views or temporary compatibility columns may expose `_ods_lineage_link_id`, but new code should use `_ods_output_link_id`.

## API / Wrapper Changes

Rename the Python wrappers in `control/lineage.py` or add new names with backward-compatible aliases.

Preferred new API:

```python
output_link_id = control.write_output_link(
    conn,
    consumer_run_id=run_id,
    edge_type="merge_to_canonical",
    target_ref={...},
    record_count=record_count,
    inputs=[
        {
            "upstream_output_link_id": customer_output_link_id,
            "edge_type": "merge_to_canonical",
            "record_count": customer_count,
        },
        {
            "upstream_output_link_id": transaction_output_link_id,
            "edge_type": "merge_to_canonical",
            "record_count": transaction_count,
        },
    ],
)
```

For file input:

```python
output_link_id = control.write_output_link(
    conn,
    consumer_run_id=run_id,
    edge_type="raw_to_curated",
    target_ref={...},
    record_count=raw_count,
    inputs=[
        {
            "source_file_id": file_id,
            "edge_type": "raw_to_curated",
            "record_count": raw_count,
        }
    ],
)
```

For sink rows:

```python
output_link_id = control.write_output_then_rows(
    conn,
    consumer_run_id=sink_run_id,
    edge_type="canonical_to_sink",
    sink_type="postgres",
    target_ref={...},
    record_count=row_count,
    inputs=[
        {
            "upstream_output_link_id": canonical_output_link_id,
            "edge_type": "canonical_to_sink",
            "record_count": row_count,
        }
    ],
    rows=rows,
)
```

### Backward Compatibility

During transition, keep aliases:

```python
write_lineage_link = write_output_link
write_link_then_rows = write_output_then_rows
```

Aliases may translate old input keys:

```text
upstream_lineage_link_id -> upstream_output_link_id
lineage_link_id -> output_link_id
```

Prefer emitting deprecation warnings in Python wrappers, not in SQL functions.

## Database Migration Approach

Use a staged migration. Do not break existing tests immediately unless all call sites are updated in the same PR.

### Option A: Physical Rename

Rename tables and columns:

```sql
ALTER TABLE cp.lineage_link RENAME TO output_link;
ALTER TABLE cp.lineage_edge RENAME TO input_edge;

ALTER TABLE cp.output_link RENAME COLUMN lineage_link_id TO output_link_id;
ALTER TABLE cp.input_edge RENAME COLUMN lineage_edge_id TO input_edge_id;
ALTER TABLE cp.input_edge RENAME COLUMN lineage_link_id TO output_link_id;
ALTER TABLE cp.input_edge RENAME COLUMN upstream_lineage_link_id TO upstream_output_link_id;
```

Then update functions, views, indexes, FKs, constraints, and tests.

This is the clean end state but has the largest blast radius.

### Option B: Compatibility Views First

Keep physical tables temporarily, add views with new names:

```sql
CREATE VIEW cp.output_link AS
SELECT
    lineage_link_id AS output_link_id,
    consumer_run_id,
    edge_type,
    sink_type,
    target_ref,
    transform_version,
    record_count,
    created_at
FROM cp.lineage_link;

CREATE VIEW cp.input_edge AS
SELECT
    lineage_edge_id AS input_edge_id,
    lineage_link_id AS output_link_id,
    upstream_run_id,
    upstream_lineage_link_id AS upstream_output_link_id,
    source_file_id,
    input_slot,
    edge_type,
    source_ref,
    record_count
FROM cp.lineage_edge;
```

Then add new wrappers and dashboard labels using the new names. Later perform physical rename.

This is safer if many tests and SQL functions still reference old names.

Recommendation: use Option B first unless Claude is also updating every migration, function, test, dashboard, and doc in one pass.

## SQL Function Rename Targets

Current functions to rename or wrap:

| Current SQL function | Target SQL function |
| --- | --- |
| `cp.write_lineage_link` | `cp.write_output_link` |
| `cp.write_link_then_rows` | `cp.write_output_then_rows` |
| `cp.run_output_link` | keep `cp.run_output_link` |
| `cp.activate_target_visibility` param `p_lineage_link_id` | `p_output_link_id` |

`cp.run_output_link` is already a good name because it means “give me the output link for this run.”

## Target Row Columns

Rename target row lineage column:

```text
_ods_lineage_link_id -> _ods_output_link_id
```

Affected areas:

- target table migrations
- `write_output_then_rows`
- row trace SQL
- dashboard target row views
- tests that click/trace rows
- target visibility joins

During transition, either:

1. Keep both columns and write both, or
2. Rename physically and provide compatibility views exposing `_ods_lineage_link_id`.

Recommendation: for a short transition, write both columns if target table compatibility matters.

## Dashboard / Documentation Changes

Update display labels to prefer the new human names:

```text
Output link
Input edge
Consumes upstream output
output_link_id
upstream_output_link_id
source_file_id
```

Avoid leading with raw table names in the teaching diagram. Show table names as secondary details:

```text
Output link
cp.output_link
output_link_id: abc123
target_ref.path: s3://...

Input edge
cp.input_edge
upstream_output_link_id: def456
```

For raw inputs:

```text
Input edge
cp.input_edge
source_file_id: file123
```

## Tests To Update / Add

Update existing tests to assert the new names where appropriate.

Add explicit tests for:

1. `cp.output_link` compatibility view returns the same row as old `cp.lineage_link`.
2. `cp.input_edge` compatibility view returns the same row as old `cp.lineage_edge`.
3. `write_output_link` creates exactly one output row and one or more input rows.
4. `write_output_then_rows` stamps `_ods_output_link_id` onto target rows.
5. Merge output has:
   - one `cp.output_link` row
   - two `cp.input_edge` rows
   - each input edge has an `upstream_output_link_id`
6. Raw output has:
   - one `cp.output_link` row
   - one `cp.input_edge` row
   - input edge has `source_file_id`
7. Row trace works from `_ods_output_link_id` back to raw file.
8. Target visibility activation uses `output_link_id` terminology.

## Acceptance Criteria

- New code and docs use `output_link` / `input_edge` language.
- Existing lineage behavior remains unchanged.
- Existing tests pass or are intentionally updated to the new names.
- Dashboard no longer presents `lineage_link` as the primary teaching term.
- A developer can explain the model as:

```text
output_link = what a run produced
input_edge = what that output was made from
upstream_output_link_id = a previous output used as input
```

## Non-Goals

- Do not redesign lineage traversal.
- Do not change `edge_type` values in this rename.
- Do not change target visibility semantics.
- Do not remove backward compatibility unless all consumers are updated in the same PR.

## Suggested Implementation Order

1. Add compatibility views `cp.output_link` and `cp.input_edge`.
2. Add wrapper aliases `write_output_link` and `write_output_then_rows`.
3. Update dashboard labels and documentation to use new names.
4. Update tests to cover the new names.
5. Update internal code gradually from old names to new names.
6. Decide later whether to physically rename tables/columns or keep compatibility views permanently.

