# Control-Table Python Packages

This guide explains the Python packages used to write ODS control-table data
and how those packages call the Postgres `pipeline.control_*` functions.

## Call Chain

```text
Glue / Airflow / Python job
  -> ods_pipeline
  -> ods_ingestion_control
  -> SELECT pipeline.control_*(...)
  -> pipeline control tables
```

Application jobs should not issue direct `INSERT` or `UPDATE` statements
against control tables. They should use the Python packages, which route writes
through the database-owned functions.

## Package 1: `ods_pipeline`

Location:

```text
ods_pipeline/
```

Purpose:

`ods_pipeline` is the application-facing helper package. Glue jobs, Airflow
DAGs, tests, and operational scripts import this package.

Typical import:

```python
import ods_pipeline
```

Common calls:

```python
conn = ods_pipeline.connect()

ods_pipeline.runs.start(...)
ods_pipeline.stages.start(...)
ods_pipeline.stages.finish(...)
ods_pipeline.files.upsert(...)
ods_pipeline.files.update_catalogue(...)
ods_pipeline.lineage.write_edge(...)
ods_pipeline.reconciliation.write_check(...)
ods_pipeline.runs.update(...)
```

Important modules:

| Module | Owns |
|---|---|
| `ods_pipeline._db` | Connection helpers such as `connect()` and `build_dsn()` |
| `ods_pipeline.runs` | `pipeline.run_log` lifecycle helpers |
| `ods_pipeline.stages` | `pipeline.run_stage_log` stage checkpoints |
| `ods_pipeline.files` | `pipeline.file_catalogue` and `pipeline.file_state` helpers |
| `ods_pipeline.lineage` | `pipeline.lineage_edge` writes |
| `ods_pipeline.reconciliation` | `pipeline.reconciliation_log` writes |
| `ods_pipeline.events` | Queryable/event-stream run events |
| `ods_pipeline.models` | Constants such as `Stage`, `StageEvent`, `RunStatus` |

Example:

```python
ods_pipeline.runs.start(
    conn,
    run_id=run_id,
    pipeline_type="direct_postgres",
    domain="insurance",
    dataset="policies",
    business_date="2026-04-11",
    file_id=file_id,
    parents=[{"run_id": parent_run_id}],
)
```

This is the layer most job code should use.

## Package 2: `ods_ingestion_control`

Location:

```text
ods_ingestion_control/
```

Purpose:

`ods_ingestion_control` is the thin database-function facade. It has a smaller
surface than `ods_pipeline` and maps Python calls directly to SQL function
calls.

Typical import inside helper code:

```python
import ods_ingestion_control as control
```

`ods_pipeline` relies on public module-level names exported by
`ods_ingestion_control/__init__.py`. The package-level import above must expose
these names:

```text
control.finish_stage
control.patch_run
control.record_run_event
control.register_file
control.set_file_state
control.start_run
control.start_stage
control.update_file_catalogue
control.update_run
control.write_lineage_edge
control.write_reconciliation_check
control.write_stage_event
```

Private implementation helpers inside `ods_ingestion_control.control`, such as
`_call` and `_json`, are not part of the contract and are not called by
`ods_pipeline`.

Common calls:

```python
control.start_run(...)
control.update_run(...)
control.patch_run(...)
control.register_file(...)
control.update_file_catalogue(...)
control.set_file_state(...)
control.start_stage(...)
control.finish_stage(...)
control.write_lineage_edge(...)
control.write_reconciliation_check(...)
control.record_run_event(...)
```

Example from the lower-level wrapper:

```python
SELECT pipeline.control_start_run(
    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
)
```

This package owns the `cursor.execute(...)` calls and transaction handling for
the Postgres function API. By default, calls commit their own transaction unless
the caller passes `commit=False`.

## Package 3: Postgres Function API

Location:

```text
db/migrations/33_control_table_functions.sql
```

Purpose:

The Postgres functions own the actual control-table writes and validation.

Examples:

```sql
pipeline.control_start_run(...)
pipeline.control_update_run(...)
pipeline.control_register_file(...)
pipeline.control_update_file_catalogue(...)
pipeline.control_set_file_state(...)
pipeline.control_start_stage(...)
pipeline.control_finish_stage(...)
pipeline.control_write_lineage_edge(...)
pipeline.control_write_reconciliation_check(...)
pipeline.control_record_run_event(...)
```

The functions write to tables such as:

```text
pipeline.run_log
pipeline.run_stage_log
pipeline.file_catalogue
pipeline.file_state
pipeline.lineage_edge
pipeline.reconciliation_log
pipeline.run_events
```

They also enforce validation such as required fields, allowed statuses/states,
non-negative counts, duplicate run metadata checks, and lineage parent
requirements.

## Example End-To-End Call

When a Glue job does this:

```python
ods_pipeline.stages.finish(
    conn,
    run_id=run_id,
    stage=ods_pipeline.Stage.SINK_PG_WAIT,
    status="succeeded",
    event_type=ods_pipeline.StageEvent.COMPLETED,
    attempt_number=attempt,
    record_count_in=target_count,
    record_count_out=loaded_count,
)
```

the flow is:

```text
ods_pipeline.stages.finish()
  -> ods_ingestion_control.finish_stage()
  -> SELECT pipeline.control_finish_stage(...)
  -> INSERT/UPDATE pipeline.run_stage_log
```

## Import Availability

`ods_pipeline` and `ods_ingestion_control` are repo-local packages. They are
made importable by the runtime environment:

- Airflow mounts the repo/package paths into the container.
- Glue-style local containers mount `ods_pipeline` under the Glue user path.
- Tests add the repo root to `sys.path` when needed.

If a standalone script cannot import them, check that the repo root is on
`PYTHONPATH` or that the container has mounted the package directories.

## Which Layer Should Code Use?

Use `ods_pipeline` for normal application jobs.

Use `ods_ingestion_control` only when writing low-level package code or a very
thin operational script that intentionally calls the database-function facade.

Use raw SQL `SELECT pipeline.control_*(...)` for SQL-only/manual operational
work.

Do not write direct `INSERT` or `UPDATE` statements against the control tables
from application code.
