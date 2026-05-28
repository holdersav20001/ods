# How ingestion fills the control plane

> **What this doc is:** the one thing to read before adding a new
> ingestion or HTTP surface. Worked example uses `services/event_api`
> (smallest end-to-end ingestion in the repo).
>
> **Pair with:**
> - [`control-plane-sequence.drawio`](control-plane-sequence.drawio) — visual sequence (open in VS Code Draw.io extension)
> - [`ods_pipeline/models.py`](../../ods_pipeline/models.py) — every legal `Stage` / `StageEvent` value (DB constraint)
> - [`tests/integration/test_event_api_live.py`](../../tests/integration/test_event_api_live.py) — proof these rows actually appear

---

## 1 — What gets written

You ingest **one event**. Four tables fill up.

| Table | Question it answers | Rows |
|---|---|---|
| `pipeline.run_log` | Did the run finish? With what counts? | **1** |
| `pipeline.run_stage_log` | What happened inside, in what order? | **5–6** |
| `pipeline.lineage_edge` | What upstream run/file caused this? | 0 (event has no upstream); 1+ for file/api_pull |
| `pipeline.reconciliation_log` | Did source/Kafka/target counts agree? | **1** per check |

After one successful `POST /events` with `event_id='ev_42abc'`:

#### `pipeline.run_log`
```
run_id   | pipeline_type | status    | source | published | kafka_topic
run_001  | message_api   | succeeded | 1      | 1         | ods.insurance.event_demo
orchestrators: [{"_ods_source_event_id": "ev_42abc", "edge_type": "message_correlation"}]
```

#### `pipeline.run_stage_log`
```
run_id  | stage             | event_type      | status     | output_ref
run_001 | message_receive   | stage_started   | running    | NULL
run_001 | message_receive   | stage_completed | succeeded  | NULL
run_001 | message_validate  | stage_completed | succeeded  | NULL
run_001 | message_archive   | stage_completed | succeeded  | s3://ods-event-demo/raw/event/.../ev_42abc.jsonl
run_001 | recon_message     | stage_completed | succeeded  | NULL
```

#### `pipeline.reconciliation_log`
```
run_id  | check_type            | status | source_count | accounted_count
run_001 | message_batch_count   | ok     | 1            | 1
detail: {"source_count":1,"published_count":1,"archive_count":1,"archive_discrepancy":0}
```

#### `pipeline.lineage_edge` — **empty for event pattern**
The event push has no upstream run or file. The correlation lives on
`run_log.orchestrators` (jsonb), not in `lineage_edge`. Find a run by event:
```sql
SELECT run_id FROM pipeline.run_log
 WHERE orchestrators @> '[{"_ods_source_event_id": "ev_42abc"}]'::jsonb;
```

---

## 2 — How those rows appear

Open [`services/event_api/main.py`](../../services/event_api/main.py)
and follow the handler:

```text
POST /events
  │
  ├─ s3.put_object              ← archive object lands first (no DB row yet)
  │
  ├─ messages.start_run(...)    ← writes run_log + first stage_started
  │
  ├─ producer.produce/flush     ← Kafka publish
  │
  └─ messages.record_result(...) ← writes 5 stage rows + recon row + flips run.status
```

Each helper call **commits independently**. Six commits per request,
not one. This is intentional — see §3.

The S3 write isn't recorded *yet* at step 1 by design. The
`stage_archive` row written at the end carries `output_ref=s3://…`,
which is what ties the archive to the run for the lineage viewer.

---

## 3 — Three rules that make it work

### Rule 1: Strict write order

```text
stages → archive_ref → reconciliation → run.status='succeeded'
```

The `runs.update(status='succeeded')` call is **always last**. If a
power cut hits mid-flight you might see partial progress on the
dashboard, but you will *never* see a `succeeded` run without its
matching recon row already there. That invariant is the whole
reason the order exists.

### Rule 2: One `run_id` per attempt, fresh UUID every time

```python
run_id = str(uuid.uuid4())     # ✅ — fresh per attempt
```

Reusing `run_id` across retries collides on the
`(run_id, stage, attempt_number)` unique index. If you must retry,
mint a new UUID and link via `lineage_edge` (see Rule 3).

### Rule 3: Use the helpers, not raw SQL

Every stage row, run row, recon row, and lineage edge has a helper.
Direct `INSERT` bypasses validation and breaks the dashboard.

| If you want to … | Call this | Source |
|---|---|---|
| Open a stage with auto-paired finish | `stages.stage_scope(...)` | [`ods_pipeline/stages.py`](../../ods_pipeline/stages.py) |
| Manual stage open/close (rich outcomes) | `stages.start` + `stages.finish` | same |
| Open a message run | `messages.start_run(...)` | [`ods_pipeline/messages.py`](../../ods_pipeline/messages.py) |
| Close a message run | `messages.record_result(...)` | same |
| Open a file/api_pull/CDC run | `runs.start(orchestrators=[…])` | [`ods_pipeline/runs.py`](../../ods_pipeline/runs.py) |
| Add a lineage edge | `lineage.write_edge(...)` | [`ods_pipeline/lineage.py`](../../ods_pipeline/lineage.py) |
| Write a recon check | `reconciliation.write_check(...)` | [`ods_pipeline/reconciliation.py`](../../ods_pipeline/reconciliation.py) |

---

## 4 — Recipes

### Bracket a stage around custom work

```python
from ods_pipeline.stages import stage_scope
from ods_pipeline.models import Stage

with stage_scope(conn, run_id=run_id, stage=Stage.SCHEMA_VALIDATE,
                 record_count_in=expected) as s:
    out = do_validation(...)
    s.set_result(record_count_out=out)
# success → stage_completed; raise → stage_failed (error captured).
# Both branches commit their row independently.
```

`stage_scope` auto-increments `attempt_number` on retries, so a re-run
lands on attempt 2, 3, … without colliding.

### Link a downstream run to its upstream

```python
from ods_pipeline import runs
runs.start(
    conn, run_id=child_id, pipeline_type="ingestion",
    domain=..., dataset=..., business_date=...,
    orchestrators=[{
        "edge_type": "triggered_by_api_pull",
        "upstream_run_id": api_pull_run_id,
    }],
)
# orchestrators stored on run_log; also fan-out to lineage_edge via write_edge
# inside the DAG that consumes them.
```

Then to find children of a parent:
```sql
SELECT consumer_run_id FROM pipeline.lineage_edge
 WHERE upstream_run_id = 'api_pull_run_007';
```

### Mark a stage as warned (soft DQ failure)

```python
stages.write(
    conn, run_id=run_id, stage=Stage.DQ_CHECK,
    status="warned", event_type=StageEvent.WARNED,
    record_count_in=N, record_count_out=N,
    metrics={"dq_warning_count": K, "rules": ["null_email"]},
)
```

### Custom recon check

```python
from ods_pipeline import reconciliation
reconciliation.write_check(
    conn,
    check_type="recon_t2_postgres_count",     # stable name; dashboard groups by it
    run_id=run_id, domain=domain, dataset=dataset,
    business_date=business_date,
    source_count=accounted_count, target_count=postgres_count,
    status="ok" if accounted_count == postgres_count else "failed",
    detail=json.dumps({"diff": accounted_count - postgres_count}),
)
```

---

## 5 — Counter-examples

```python
# ❌ Flips status BEFORE writing recon — breaks dashboard invariant
runs.update(conn, run_id, status="succeeded")
reconciliation.write_check(conn, status="ok", ...)
# A power cut between the two leaves 'succeeded' with no recon row.
```

```python
# ❌ Reuses run_id with attempt_number=1
run_id = f"event-{event_id}"
stages.start(conn, run_id=rid, stage="message_receive", attempt_number=1)
# Retry collides on the unique index. Mint a fresh UUID, or omit
# attempt_number so stage_scope auto-increments.
```

```python
# ❌ Bare except inside a stage — failure invisible
try:
    publish_to_kafka(...)
except Exception:
    pass            # silent! run stays 'running' forever
# Use stage_scope so failure rows are written automatically:
#   with stage_scope(conn, run_id=rid, stage=Stage.KAFKA_PUBLISH):
#       publish_to_kafka(...)
```

```python
# ❌ Invents a stage string
cur.execute("INSERT INTO pipeline.run_stage_log (..., stage, ...) "
            "VALUES (..., 'my_custom_stage', ...)")
# DB CHECK constraint rejects unknown stages. Add a constant in
# models.py:Stage and bump the migration.
```

```python
# ❌ Direct INSERT into lineage_edge
cur.execute("INSERT INTO pipeline.lineage_edge ...")
# Use lineage.write_edge — keeps edge_type in the supported set and
# populates source_ref / target_ref / record_count for the viewer.
```

---

## 6 — When something dies mid-flight

A worker can die (OOM, k8s eviction, SIGKILL). The handler's `except`
block doesn't run, so no failure rows are written and the run stays
`status='running'`.

[`airflow/dags/dag_run_janitor.py`](../../airflow/dags/dag_run_janitor.py)
runs every minute. It looks for runs where:
- `run_log.status = 'running'`
- `run_log.started_at < NOW() - 5 min`
- No `run_stage_log` activity inside the last 5 min

… and flips them to `failed` with `error_summary='janitor_no_heartbeat'`.

Long-running Glue jobs aren't penalised — the R8 long-running
DockerOperator emits `stage_heartbeat` rows every 30 s, which keeps
the activity timestamp fresh.

---

## 7 — Quick verification queries

After triggering your ingestion:

```sql
-- Did the run finish?
SELECT status, record_count_source, record_count_target, error_summary
  FROM pipeline.run_log WHERE run_id = '<your-run>';

-- What stages ran?
SELECT stage, event_type, status, record_count_out, output_ref, error
  FROM pipeline.run_stage_log WHERE run_id = '<your-run>'
 ORDER BY started_at;

-- Was it reconciled?
SELECT check_type, status, source_count, target_count, detail
  FROM pipeline.reconciliation_log WHERE run_id = '<your-run>';

-- Find by upstream
SELECT run_id FROM pipeline.run_log
 WHERE orchestrators @> '[{"_ods_source_event_id": "ev_42abc"}]'::jsonb;
```

If any of those is empty when you expected rows, the wiring is broken.

---

## 8 — Where to dig deeper

- **The exact code:** [`ods_pipeline/messages.py`](../../ods_pipeline/messages.py) — read `record_result`. Six writes, six commits, strict order. Canonical example.
- **Legal stage / status values:** [`ods_pipeline/models.py`](../../ods_pipeline/models.py) — DB-enforced enum.
- **The visual flow:** [`control-plane-sequence.drawio`](control-plane-sequence.drawio) — open in VS Code's Draw.io extension or upload to draw.io.
- **Worked integration test:** [`tests/integration/test_event_api_live.py`](../../tests/integration/test_event_api_live.py) — drives the full flow against LocalStack + Postgres + Kafka.
