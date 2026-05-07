# Control-plane cookbook — recipe card

> **Style C: keep this tab open while coding.** No narrative, no theory.
> Recipes you copy-paste, table catalogue, common mistakes, decision rules.
> Pair with `control-plane-A-annotated.md` (the explanation) and
> `control-plane-B-sequence.drawio` (the diagram).

---

## Pick a recipe

| You want to … | Use |
|---|---|
| Open a run from inside an HTTP handler / event consumer | [Recipe 1](#recipe-1--open-a-run) |
| Open a run from inside an Airflow task | [Recipe 1b](#recipe-1b--open-a-run-from-airflow) |
| Record a stage start/finish pair around custom work | [Recipe 2](#recipe-2--bracket-a-custom-stage) |
| Close a message/API run with reconciliation | [Recipe 3](#recipe-3--close-a-run) |
| Add a parent edge (multi-hop lineage) | [Recipe 4](#recipe-4--add-a-parent-edge) |
| Mark a stage warned (soft DQ failure) | [Recipe 5](#recipe-5--soft-failure) |
| Mark a stage hard-failed and abort | [Recipe 6](#recipe-6--hard-failure) |
| Write a custom reconciliation check | [Recipe 7](#recipe-7--custom-recon-check) |

---

## Table catalogue

### `pipeline.run_log` — one row per ingestion attempt

| Column | When set | Notes |
|---|---|---|
| `run_id` | by `runs.start` | UUID, fresh per attempt |
| `pipeline_type` | by `runs.start` | `file_pipeline`, `message_api`, `cdc`, `api_pull` |
| `domain`, `dataset`, `business_date` | by `runs.start` | partition keys |
| `status` | by `runs.start` ('running'), `runs.update` ('succeeded'/'failed'/'partial') | `TERMINAL_STATUSES` closes the run |
| `record_count_*` | by `runs.update` | `source / dq_pass / dq_fail / published` |
| `kafka_topic`, `kafka_offset_*` | by `runs.start` / `runs.update` | window the run wrote |
| `parents` | by `runs.start` | jsonb array of edges (also fans out to `lineage_edge`) |

### `pipeline.run_stage_log` — many rows per run

| Column | When set | Notes |
|---|---|---|
| `run_id, stage, event_type, attempt_number` | always | unique key — re-runs increment `attempt_number` |
| `event_type` | always | `stage_started` / `stage_completed` / `stage_failed` / `stage_skipped` / `stage_warned` / `stage_heartbeat` |
| `record_count_in/out` | by `stages.finish` | invariant: `out <= in` |
| `metrics` | by all stage writes | jsonb — free-form, but stick to short keys |
| `output_ref` | optional | S3 URI / Kafka topic-partition-offset / etc — what the stage produced |
| `error` | on `stage_failed` / `stage_warned` | one-line string; long detail goes in `metrics` |

### `pipeline.lineage_edge` — multi-hop run/file ancestry

| Column | Notes |
|---|---|
| `child_run_id` | run that received the data (FK → run_log) |
| `parent_run_id` | run that produced it (FK → run_log, nullable) |
| `parent_file_id` | file that produced it (FK → file_catalogue, nullable) |
| `edge_type` | `raw_to_curated`, `curated_to_kafka`, `curated_to_postgres`, `triggered_by_api_pull`, … |
| `source_ref` | S3 raw URI, topic, etc. |
| `target_ref` | S3 curated URI, Postgres table, etc. |
| `record_count` | rows carried by this hop |

The schema does NOT have `_ods_source_event_id` / `_ods_source_request_id` /
`_ods_change_lsn` / `_ods_file_id` columns. Those correlation IDs live
on `run_log.parents` (jsonb). Query them with the JSONB containment
operator:
```sql
SELECT run_id FROM pipeline.run_log
 WHERE parents @> '[{"_ods_source_event_id": "ev_42abc"}]'::jsonb;
```

### `pipeline.reconciliation_log` — one row per check per run

| Column | Notes |
|---|---|
| `check_type` | stable string — dashboard groups by this. Examples: `message_batch_count`, `t0_offset_count`, `t1_canonical_count`, `recon_t2_postgres_count` |
| `status` | `ok` / `failed` |
| `source_count`, `kafka_count`, `target_count` | filled depending on the check; NULLs allowed |
| `detail` | jsonb — counts breakdown for ops |

---

## Recipe 1 — open a run

**Use case:** HTTP handler / FastAPI event consumer.

```python
from ods_pipeline import messages

correlation = {"_ods_source_event_id": event_id}
# or {"_ods_source_request_id": request_id}
# or {"_ods_source_message_id": message_id}
# or {"_ods_source_batch_id": batch_id}

messages.start_run(
    conn,                                # YOUR transaction; do not commit yet
    run_id=run_id,                       # fresh uuid per attempt
    domain="insurance",
    dataset="event_demo",
    source_application="event_api",      # name of THIS service
    correlation=correlation,
    business_date="2026-05-07",
    kafka_topic="ods.insurance.event_demo",
)
# rows now in: run_log (status=running), run_stage_log (MESSAGE_RECEIVE / stage_started),
# lineage_edge (message_correlation edge -> _ods_source_event_id)
```

## Recipe 1b — open a run from Airflow

**Use case:** Airflow task that owns the file/poll/CDC ingestion.

```python
from ods_pipeline import runs, stages
from ods_pipeline.models import Stage

with conn:                               # one transaction per Airflow task
    runs.start(
        conn,
        run_id=run_id,
        pipeline_type="file_pipeline",   # NOT "message_api" — different lineage join
        domain=domain, dataset=dataset,
        business_date=business_date,
        kafka_topic=target_topic,
        parents=[{                       # edge to the file_catalogue row
            "edge_type": "file_to_run",
            "_ods_file_id": file_id,
        }],
    )
    stages.start(conn, run_id=run_id, stage=Stage.RAW_READ)
    # ... do the work ...
    stages.finish(conn, run_id=run_id, stage=Stage.RAW_READ,
                  status="succeeded", record_count_in=N, record_count_out=N)
```

## Recipe 2 — bracket a custom stage

**Use case:** any time you do meaningful work inside an existing run.

**Recommended (stateless, auto failure-write):**

```python
from ods_pipeline.stages import stage_scope
from ods_pipeline.models import Stage

with stage_scope(conn, run_id=run_id, stage=Stage.SCHEMA_VALIDATE,
                 record_count_in=expected) as s:
    out_count = do_validation(...)
    s.set_result(record_count_out=out_count)
# success → stage_completed row, raise → stage_failed row with error.
# Both branches commit their row independently.
```

**Manual (when you need custom metrics or output_ref):**

```python
from ods_pipeline import stages
from ods_pipeline.models import Stage, StageEvent

attempt = stages.start(conn, run_id=run_id, stage=Stage.CURATED_WRITE,
                       record_count_in=expected)
try:
    out_count = do_write(...)
    stages.finish(
        conn, run_id=run_id, stage=Stage.CURATED_WRITE,
        status="succeeded", event_type=StageEvent.COMPLETED,
        attempt_number=attempt,
        record_count_in=expected, record_count_out=out_count,
        output_ref=f"s3://curated/...",
    )
except Exception as e:
    stages.finish(
        conn, run_id=run_id, stage=Stage.CURATED_WRITE,
        status="failed", event_type=StageEvent.FAILED,
        attempt_number=attempt,
        record_count_in=expected, record_count_out=0,
        error=str(e),
    )
    raise
```

## Recipe 3 — close a run

**Use case:** once Kafka publish + S3 archive succeeded (or failed).

```python
messages.record_result(
    conn,
    run_id=run_id,
    domain=domain, dataset=dataset,
    business_date=business_date,
    source_count=N, published_count=N, archive_count=N,
    kafka_topic=topic,
    archive_ref=f"s3://{bucket}/{key}",   # SET THIS — not optional in practice
)
conn.commit()                             # ONE commit, here, never inside a helper
```

Failure path:
```python
except Exception as exc:
    conn.rollback()
    try:
        messages.record_result(
            conn, run_id=run_id, domain=domain, dataset=dataset,
            business_date=business_date,
            source_count=N, published_count=0, archive_count=N,
            kafka_topic=topic, archive_ref=archive_uri,
            extra_detail={"error": str(exc)},
        )
        conn.commit()
    except Exception:
        conn.rollback()
    raise                                 # bubble up to the framework
```

## Recipe 4 — add a parent edge

**Use case:** downstream run consuming output of an upstream run.

```python
runs.start(
    conn, ...,
    parents=[
        {"edge_type": "run_to_run", "parent_run_id": upstream_run_id},
        # multiple edges OK — fan-out is just a list
        {"edge_type": "file_to_run", "_ods_file_id": file_id},
    ],
)
# `parents` is stored on run_log AND fanned out as one row per edge in lineage_edge.
```

## Recipe 5 — soft failure

**Use case:** DQ rule fired but data still went through.

```python
stages.write(
    conn, run_id=run_id, stage=Stage.DQ_CHECK,
    status="warned", event_type=StageEvent.WARNED,
    record_count_in=N, record_count_out=N,
    metrics={"dq_warning_count": K, "rules_fired": ["null_email", "old_dob"]},
    error=None,                           # warned != failed; error stays NULL
    commit=False,
)
```

The dashboard counts `warned` separately. If you set `validation_fail_count`
on `record_result` the wrapper does this for you — only call `stages.write`
directly when you have a non-standard stage.

## Recipe 6 — hard failure

**Use case:** unrecoverable error inside a stage.

```python
stages.finish(
    conn, run_id=run_id, stage=Stage.CURATED_WRITE,
    status="failed", event_type=StageEvent.FAILED,
    record_count_in=N, record_count_out=0,
    error=str(exc)[:500],                 # one-line message; truncate
    commit=False,
)
runs.update(
    conn, run_id, commit=False,
    status="failed",
    error_summary="curated_write failed",
)
# raise so the outer handler rolls back the open transaction.
raise
```

## Recipe 7 — custom recon check

```python
from ods_pipeline import reconciliation

reconciliation.write_check(
    conn,
    check_type="recon_t2_postgres_count",   # use a stable, dashboard-known name
    run_id=run_id,
    domain=domain, dataset=dataset, business_date=business_date,
    source_count=kafka_count,
    target_count=postgres_row_count,
    status="ok" if kafka_count == postgres_row_count else "failed",
    detail=json.dumps({"diff": kafka_count - postgres_row_count}, sort_keys=True),
    commit=False,
)
```

Then mirror it as a stage row so the lineage viewer can show it:
```python
stages.write(
    conn, run_id=run_id, stage=Stage.RECON_T2,
    status="succeeded" if ok else "failed",
    event_type=StageEvent.COMPLETED if ok else StageEvent.FAILED,
    record_count_in=kafka_count, record_count_out=postgres_row_count,
    commit=False,
)
```

---

## Common mistakes

| Mistake | Why it bites | Fix |
|---|---|---|
| Calling `conn.commit()` inside `start_run` / `record_result` / `stages.*` | Helpers all use `commit=False` by contract. A premature commit half-closes the run; a later failure can't roll it back. | Commit exactly once, in the outermost handler. |
| Reusing `run_id` across retries | `(run_id, stage, event_type, attempt_number)` unique violation on the retry. | Fresh `uuid.uuid4()` per attempt. Tie attempts together via `lineage_edge`. |
| Skipping `archive_ref` on `record_result` | Lineage viewer shows NULL `output_ref` for the archive stage; ops can't find the payload. | Pass the S3 URI. |
| Inventing a new `stage` string | DB CHECK constraint rejects any value not in `Stage.all_values()`. | Add a constant in `ods_pipeline/models.py:Stage` + bump the migration. |
| Writing `lineage_edge` directly | The fan-out from `parents` already does it; you'll create duplicates. | Pass `parents=[…]` to `runs.start`. |
| Using positional args on helpers | All helper signatures are keyword-only after `conn`. Positional usage breaks on the next minor version bump. | Keep keyword form. |
| Logging the payload into `metrics` | jsonb has a 1MB practical ceiling and the dashboard renders it in a tooltip. | Put payloads in S3 (`output_ref`); keep `metrics` to counts and short labels. |
| Forgetting `pipeline_type` | Recon dashboard groups by it; the dataset vanishes from operator view. | Always set it. |

---

## Decision rules — *which helper for which situation?*

```
new run starting?
├── from HTTP / event push? ─► messages.start_run
├── from file_catalogue?    ─► runs.start (parents=file_to_run edge)
├── from upstream run?      ─► runs.start (parents=run_to_run edge)
└── from CDC offset?        ─► runs.start (parents include _ods_change_lsn)

closing a run?
├── one ingest unit, simple count check?  ─► messages.record_result (does everything)
├── multi-stage (file_pipeline)?           ─► call stages.* per stage,
│                                             reconciliation.write_check explicitly,
│                                             runs.update at the end
└── partial success (per-record DQ)?       ─► runs.update(status='partial')
                                             + reconciliation.write_check(status='ok')
                                             + per-stage warned rows
```

---

## How to verify your wiring locally

```bash
# 1. Trigger your code path against the local stack
docker compose up -d postgres localstack kafka
# ... run your handler / DAG ...

# 2. Look at the rows
psql ods_dev -c "
  SELECT run_id, status, record_count_source, record_count_published
    FROM pipeline.run_log
   ORDER BY started_at DESC LIMIT 5"

psql ods_dev -c "
  SELECT stage, event_type, status, record_count_in, record_count_out, error
    FROM pipeline.run_stage_log
   WHERE run_id = '<your-run-id>'
   ORDER BY started_at"

psql ods_dev -c "
  SELECT edge_type, parent_run_id, _ods_source_event_id, _ods_file_id
    FROM pipeline.lineage_edge
   WHERE child_run_id = '<your-run-id>'"

psql ods_dev -c "
  SELECT check_type, status, source_count, target_count, detail
    FROM pipeline.reconciliation_log
   WHERE run_id = '<your-run-id>'"
```

Then point the lineage viewer at it:
```bash
# scripts/lineage_viewer.py — see local-dev.md
python scripts/lineage_viewer.py --run-id <your-run-id>
```

If any of the four queries returns zero rows, your wiring is broken. The
expected pattern for a successful event ingest:

| Table | Rows |
|---|---|
| `run_log` | 1, status=`succeeded` |
| `run_stage_log` | 5–6 (started + finished receive, validate, archive, recon) |
| `lineage_edge` | 1+ (at least the `message_correlation` edge) |
| `reconciliation_log` | 1, status=`ok` |
