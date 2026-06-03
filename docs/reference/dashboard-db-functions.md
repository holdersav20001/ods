# Dashboard And Developer SQL Function Contract

The dashboard snapshot exporter and support runbooks rely on these read-only SQL
functions. They are also the recommended interface for developers who cannot use
the dashboard UI.

All functions live in schema `cp`. They validate required ids and raise
developer-readable exceptions with hints when the input cannot be resolved.

## `cp.dashboard_workflows()`

Returns one rollup row per `workflow_run_id`.

Important columns:

- `workflow_run_id`
- `business_date`
- `trigger_type`
- `domain`
- `run_count`
- `stage_count`
- `output_count`
- `input_count`
- `target_visibility_count`
- `succeeded_count`
- `failed_count`
- `running_count`
- `has_replay`
- `orchestrator_type`
- `orchestrator_dag_id`
- `orchestrator_run_id`
- `datasets`
- `pipeline_types`
- `first_started_at`
- `last_finished_at`

Example:

```sql
SELECT workflow_run_id, domain, business_date, run_count, output_count,
       target_visibility_count, orchestrator_type
FROM cp.dashboard_workflows()
ORDER BY first_started_at;
```

## `cp.dashboard_workflow_detail(workflow_run_id text)`

Returns a single `jsonb` document:

```json
{
  "workflow": {},
  "runs": [],
  "output_links": [],
  "target_visibility": []
}
```

Each run contains its `run_stage_log` rows under `stages`. Each output link
contains its `input_edge` rows under `input_edges`.

Example:

```sql
SELECT cp.dashboard_workflow_detail('<workflow_run_id>');
```

Raises if `workflow_run_id` is null or not present in `cp.run_log`.

## `cp.dashboard_output_trace(output_link_id uuid)`

Walks an output back through `input_edge.upstream_output_link_id` until it reaches
raw file leaves (`source_file_id`).

Returns:

- `hop`
- `edge_type`
- `output_link_id`
- `consumer_run_id`
- `pipeline_type`
- `dataset`
- `upstream_run_id`
- `source_file_id`
- `raw_s3_path`
- `is_cycle`

Example:

```sql
SELECT hop, edge_type, dataset, source_file_id, raw_s3_path
FROM cp.dashboard_output_trace('<output_link_id>')
ORDER BY hop;
```

Healthy trace output should reach at least one row where `source_file_id` and
`raw_s3_path` are non-null.

## `cp.dashboard_file_usage(file_id uuid)`

Returns direct input edges that consumed or produced from a raw file id.

Example:

```sql
SELECT input_edge_id, output_link_id, edge_type, workflow_run_id, dataset
FROM cp.dashboard_file_usage('<file_id>');
```

Use this when you want the direct raw-file usage rows.

## `cp.dashboard_file_impact(file_id uuid)`

Returns the downstream closure from a raw file: every output derived from that
file, transitively.

Example:

```sql
SELECT output_link_id, edge_type, workflow_run_id, dataset, target_ref
FROM cp.dashboard_file_impact('<file_id>');
```

Use this when support asks: "If this raw file is wrong, which outputs and target
rows could be affected?"

## `cp.dashboard_target_row_trace(target_schema text, target_table text, row_id bigint)`

Reads the target row's `_ods_output_link_id`, then delegates to
`cp.dashboard_output_trace`.

Example:

```sql
SELECT hop, edge_type, dataset, source_file_id, raw_s3_path
FROM cp.dashboard_target_row_trace('ods', 'policy_claim', 1)
ORDER BY hop;
```

Raises with a clear message if:

- schema/table/row id is missing
- the target table does not exist
- the target table lacks `row_id` or `_ods_output_link_id`
- the row is missing or was not stamped with `_ods_output_link_id`

## `cp.dashboard_airflow_lookup(dag_id text, dag_run_id text)`

Maps Airflow identity back to control-plane runs. `dag_run_id` is required;
`dag_id` may be null.

Example:

```sql
SELECT workflow_run_id, run_id, pipeline_type, dataset, status,
       orchestrator_task_id, started_at, finished_at
FROM cp.dashboard_airflow_lookup(NULL, '<dag_run_id>')
ORDER BY started_at;
```

## `cp.developer_diagnostics(workflow_run_id text, target_table text DEFAULT NULL)`

Returns one row per detected issue:

- `check_name`
- `severity`
- `object_type`
- `object_id`
- `message`
- `details`

Example:

```sql
SELECT check_name, severity, object_type, object_id, message
FROM cp.developer_diagnostics('<workflow_run_id>', 'ods.policy_claim')
ORDER BY severity, check_name;
```

Empty result means healthy.

Current diagnostics cover:

- unfinished runs
- unfinished stages
- succeeded runs without stages
- output-producing succeeded runs without output links
- output links without input edges
- downstream input edges missing `upstream_output_link_id`
- target rows missing ODS ids
- target rows whose `_ods_output_link_id` does not exist
- target rows where `_ods_output_link_id` and `_ods_lineage_link_id` diverge
- target row workflow id mismatches
- active target-visibility conflicts
- DLQ rows missing trace context
- schema-validation outputs missing `schema_version`

## Operational Notes

These functions are intentionally read-only. The future API repository should
call the underlying write wrappers/functions for writes, and these SQL functions
for dashboard/support reads unless it introduces a dedicated read API over the
same contract.
