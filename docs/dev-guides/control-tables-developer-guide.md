# Control Tables — Developer Guide

**Audience:** developers writing or reviewing ODS pipeline code.
**Reading time:** 10 minutes.
**Covers:** the direct-Postgres file route end-to-end (other routes share the same vocabulary; see §8).

---

## 1. What is this?

Every file or message the platform processes leaves a paper trail across six tables. The trail answers four questions about any row in any target table:

1. **Where did it come from?** → source file
2. **Who wrote it?** → run
3. **What happened in between?** → stages
4. **Does the count match the source?** → reconciliation

If you can answer those four, you can debug anything. The rest of this guide shows how.

---

## 2. The three UUIDs

The trick to reading any control-table row is to know which UUID it belongs to. There are only three (four if Airflow owns the route).

| Name | Lives in | Born when | Lifetime |
|---|---|---|---|
| `file_id` | `pipeline.file_catalogue.file_id` | Landing task accepts a source file | Forever for that file |
| `ingestion_run_id` | `pipeline.run_log.run_id` (row with `pipeline_type='ingestion'`) | Ingestion task starts | One per ingestion attempt |
| `pg_write_run_id` | `pipeline.run_log.run_id` (row with `pipeline_type='direct_postgres'`) | Load task starts | One per load attempt |
| `route_run_id` *(optional)* | `pipeline.run_log.run_id` (row with `pipeline_type='orchestration'`) | DAG start task | One per DAG run |

**Golden rule:** allocate a `run_id` *when the task actually starts*. Never pre-mint downstream UUIDs.

---

## 3. The six control tables

| Table | Holds | Keyed by |
|---|---|---|
| `pipeline.file_catalogue` | One row per received file. Tracks lifecycle state. | `file_id` |
| `pipeline.run_log` | One row per task run. The "what is running / did run" table. | `run_id` |
| `pipeline.run_stage_log` | Append-only stage timings + counts. Two rows per stage (start, finish). | `run_id, stage, attempt_number` |
| `pipeline.lineage_edge` | One row per data movement (raw→curated, curated→target, etc). | `consumer_run_id, upstream_run_id, source_file_id` |
| `pipeline.reconciliation_log` | Row-count sanity checks. | `run_id, check_type` |
| `ods.*` target tables | The data itself, with `_ods_*` metadata columns linking back. | Business key + `_ods_file_id`, `_ods_run_id` |

`pipeline.file_processing_attempt` is a seventh table used only for retry idempotency — most code doesn't touch it directly.

---

## 4. Cheat sheet — which UUID goes where

Print this. Stick it on your monitor.

```text
file_catalogue.file_id                                = file_id
file_catalogue.last_run_id      after ingestion       = ingestion_run_id
                                after load            = pg_write_run_id

run_log.run_id        (ingestion row)                 = ingestion_run_id
run_log.run_id        (load row)                      = pg_write_run_id
run_log.run_id        (orchestration row)             = route_run_id
run_log.file_id       on every task row               = file_id
run_log.orchestrators on task rows                    = [{run_id: route_run_id,
                                                         edge_type: 'orchestrates'}]

run_stage_log.run_id                                  = the task's own run_id

lineage_edge          raw_to_curated:
    consumer_run_id                                   = ingestion_run_id
    source_file_id                                    = file_id
    upstream_run_id                                   = NULL  (file IS the source)

lineage_edge          curated_to_postgres:
    consumer_run_id                                   = pg_write_run_id
    upstream_run_id                                   = ingestion_run_id
    source_file_id                                    = file_id

reconciliation_log.run_id                             = the task's own run_id

ods.* target row      _ods_file_id                    = file_id
ods.* target row      _ods_run_id                     = pg_write_run_id
                                                       (the load run, NOT
                                                        the ingestion run)
```

---

## 5. The "parent" trap

The single most common confusion. Three different things share the word *parent*:

| Question | Column | Example value |
|---|---|---|
| Who scheduled me? | `run_log.orchestrators[].run_id` | `route_run_id` (the DAG run) |
| Whose **data output** did I read? | `lineage_edge.upstream_run_id` | `ingestion_run_id` (it produced the curated Parquet) |
| Which **file** did I read? | `lineage_edge.source_file_id` | `file_id` (the original CSV) |

**Test:** if I delete the row you're pointing at, do I lose data?
- DAG row → no data lost → it's orchestration → `run_log.orchestrators`
- Ingestion row → curated S3 still exists, but lineage of *who produced it* is lost → data parent → `lineage_edge.upstream_run_id`
- File row → source file location lost → file parent → `lineage_edge.source_file_id`

---

## 6. Worked example — one CSV, one load

Example values used below. The UUIDs are short for readability; in reality they are full UUIDv4 strings.

```text
domain          insurance
dataset         country_codes
business_date   2026-05-21
s3 raw path     s3://ods-raw/insurance/country_codes/date=20260521/country_codes_20260521.csv
s3 curated path s3://ods-curated/insurance/country_codes/date=20260521/
target table    ods.insurance_country_code

file_id           = F
route_run_id      = R   (only if Airflow owns the route)
ingestion_run_id  = I
pg_write_run_id   = P
```

### Step 1 — Landing task registers the file

```python
file_id = ods_pipeline.files.upsert(
    conn,
    domain="insurance",
    dataset="country_codes",
    business_date="2026-05-21",
    sftp_path="/upload/country_codes_20260521.csv",
    s3_raw_path="s3://ods-raw/.../country_codes_20260521.csv",
    state="received",
)
```

State after step 1:

```text
file_catalogue:  file_id=F, state=received, sftp_path=..., s3_raw_path=...
run_log:         (empty)
```

### Step 2 — (Optional) DAG start records the orchestration anchor

Skip this step if no DAG owns the end-to-end route.

```python
route_run_id = uuid4()
ods_pipeline.runs.start(
    conn, run_id=route_run_id, pipeline_type="orchestration",
    domain="insurance", dataset="country_codes",
    business_date="2026-05-21", file_id=file_id,
)
```

State after step 2:

```text
run_log:  R | orchestration | F | running | orchestrators=[]
```

### Step 3 — Ingestion task starts

```python
ingestion_run_id = uuid4()
ods_pipeline.runs.start(
    conn, run_id=ingestion_run_id, pipeline_type="ingestion",
    domain="insurance", dataset="country_codes",
    business_date="2026-05-21", file_id=file_id,
    orchestrators=[{"run_id": route_run_id, "edge_type": "orchestrates"}],
)
```

State after step 3:

```text
run_log:  R | orchestration    | F | running    | orchestrators=[]
          I | ingestion        | F | running    | orchestrators=[{R, orchestrates}]
```

### Step 4 — Ingestion writes stage evidence

For each meaningful step (read raw, validate schema, run DQ, write curated):

```python
attempt = ods_pipeline.stages.start(
    conn, run_id=ingestion_run_id, stage="raw_read",
    input_ref="s3://.../raw/...csv",
)
# ... do the work ...
ods_pipeline.stages.finish(
    conn, run_id=ingestion_run_id, stage="raw_read",
    status="succeeded", event_type="stage_completed",
    attempt_number=attempt, record_count_out=100,
)
```

`run_stage_log` accumulates pairs of rows (one started + one completed) per stage, all carrying `run_id=ingestion_run_id`.

### Step 5 — Ingestion finishes

Four writes mark the ingestion run terminal:

```python
# 1) curated data exists now — flip catalogue
ods_pipeline.files.update_catalogue(
    conn, file_id=file_id, state="curated",
    s3_curated_path="s3://.../curated/.../",
    last_run_id=ingestion_run_id,
)

# 2) record raw→curated data movement
ods_pipeline.lineage.write_edge(
    conn,
    consumer_run_id=ingestion_run_id,
    source_file_id=file_id,           # the file IS the source — no upstream run
    edge_type="raw_to_curated",
    source_ref="s3://.../raw/...csv",
    target_ref="s3://.../curated/.../",
    record_count=100,
)

# 3) row count sanity check
ods_pipeline.reconciliation.write_check(
    conn, check_type="t0_ingestion_count",
    run_id=ingestion_run_id,
    domain="insurance", dataset="country_codes",
    business_date="2026-05-21",
    source_count=100, accounted_count=100, status="ok",
)

# 4) close the run
ods_pipeline.runs.update(
    conn, ingestion_run_id, status="succeeded",
    record_count_source=100, record_count_dq_pass=100,
    record_count_dq_fail=0,
)
```

State after step 5:

```text
file_catalogue:  F | state=curated | s3_curated_path=... | last_run_id=I
lineage_edge:    consumer=I | upstream=NULL | source_file=F | raw_to_curated | 100 rows
recon_log:       run=I | t0_ingestion_count | source=100 | accounted=100 | ok
run_log:         I | ingestion | F | succeeded | record_count_source=100
```

### Step 6 — Load task starts

```python
pg_write_run_id = uuid4()
ods_pipeline.runs.start(
    conn, run_id=pg_write_run_id, pipeline_type="direct_postgres",
    domain="insurance", dataset="country_codes",
    business_date="2026-05-21", file_id=file_id,
    orchestrators=[{"run_id": route_run_id, "edge_type": "orchestrates"}],
)
```

### Step 7 — Load writes target rows + finishes

Stage evidence first (one `postgres_write` pair). Then four wrap-up writes:

```python
# 1) record the data movement — note BOTH parents are set
ods_pipeline.lineage.write_edge(
    conn,
    consumer_run_id=pg_write_run_id,
    upstream_run_id=ingestion_run_id,   # ingestion produced the curated input
    source_file_id=file_id,              # trace back to the source file too
    edge_type="curated_to_postgres",
    source_ref="s3://.../curated/.../",
    target_ref="jdbc:postgresql://.../ods.insurance_country_code",
    record_count=100,
)

# 2) row count check against the target table
ods_pipeline.reconciliation.write_check(
    conn, check_type="direct_postgres_count",
    run_id=pg_write_run_id,
    domain="insurance", dataset="country_codes",
    source_count=100, postgres_count=100, status="ok",
)

# 3) file lifecycle reaches terminal "loaded" state
ods_pipeline.files.update_catalogue(
    conn, file_id=file_id, state="loaded", last_run_id=pg_write_run_id,
)

# 4) close the load run
ods_pipeline.runs.update(
    conn, pg_write_run_id, status="succeeded",
    record_count_source=100, record_count_target=100,
)
```

Target rows in `ods.insurance_country_code`:

```text
country_code | country_name    | _ods_file_id | _ods_run_id
GB           | United Kingdom  | F            | P
US           | United States   | F            | P
```

`_ods_run_id = P` (load run), **never** `I` (ingestion run). The rule: `_ods_run_id` is "who physically wrote this row".

---

## 7. FAQ — the questions everyone asks

**Q. Why is `upstream_run_id` NULL on the raw→curated edge?**
The file itself is the data source. There is no upstream *run* to point to. `source_file_id=file_id` carries the linkage instead. The first edge in any chain looks like this.

**Q. The DAG scheduled me. Is it my "parent"?**
Yes, but in the orchestration sense. Put it in `run_log.orchestrators`. It is **not** the data parent — that belongs in `lineage_edge.upstream_run_id`.

**Q. What is `_ods_run_id` for — ingestion or load?**
Always the run that physically wrote the row to the target table. For the direct-Postgres route that is the load run (`pg_write_run_id`).

**Q. The file came in twice with the same content. What happens?**
`pipeline.file_catalogue` upsert on `(domain, dataset, s3_raw_path)` — same row updated, same `file_id`. The downstream tasks see no change. If processing was already complete, nothing reruns.

**Q. What does `state='loaded'` mean exactly?**
Successful end state for the direct-Postgres route: target rows are visible in `ods.*`. (Older code used `'sunk'` for this; the value is now `'loaded'`.)

**Q. Do I write a `lineage_edge` before or after the data exists?**
**After.** Always. If the work fails after you write the edge, lineage lies. The pattern is: do the work, then record what happened.

**Q. Do I need migration 33's stored procs, or can I write SQL directly?**
Use the Python helpers (`ods_pipeline.runs.start`, etc) — they wrap the stored procs and handle commits, timestamps, and idempotency for you. Direct SQL is reserved for migrations and operator queries.

---

## 8. Sanity queries

File reached final state?

```sql
SELECT state, last_run_id
  FROM pipeline.file_catalogue
 WHERE file_id = '<file_id>';
-- expect: state='loaded', last_run_id=<pg_write_run_id>
```

All runs for this file succeeded?

```sql
SELECT pipeline_type, status, started_at, ended_at
  FROM pipeline.run_log
 WHERE file_id = '<file_id>'
 ORDER BY started_at;
-- expect: orchestration+succeeded, ingestion+succeeded, direct_postgres+succeeded
```

Full lineage chain?

```sql
SELECT edge_type, consumer_run_id, upstream_run_id, source_file_id, record_count
  FROM pipeline.lineage_edge
 WHERE source_file_id = '<file_id>'
    OR consumer_run_id IN (
        SELECT run_id FROM pipeline.run_log WHERE file_id = '<file_id>'
    )
 ORDER BY created_at;
-- expect:
--   raw_to_curated      (consumer=<ingestion>, upstream=NULL,        source_file=<file>)
--   curated_to_postgres (consumer=<load>,      upstream=<ingestion>, source_file=<file>)
```

Reconciliation passed?

```sql
SELECT check_type, source_count, accounted_count, postgres_count, status
  FROM pipeline.reconciliation_log
 WHERE run_id IN (SELECT run_id FROM pipeline.run_log WHERE file_id = '<file_id>')
 ORDER BY created_at;
-- expect: t0_ingestion_count=ok, direct_postgres_count=ok
```

Walk back from a target row to source file?

```sql
SELECT t.country_code,
       fc.s3_raw_path,
       rl.pipeline_type, rl.status
  FROM ods.insurance_country_code t
  JOIN pipeline.file_catalogue  fc ON fc.file_id = t._ods_file_id::uuid
  JOIN pipeline.run_log         rl ON rl.run_id  = t._ods_run_id::uuid
 WHERE t.country_code = 'GB';
-- expect: GB, s3://ods-raw/.../country_codes_20260521.csv, direct_postgres, succeeded
```

---

## 9. Other routes (same vocabulary, different `edge_type`)

The vocabulary above applies to every route. Only the `edge_type` values change:

| Route | edge_type values |
|---|---|
| Direct-Postgres file route | `raw_to_curated`, `curated_to_postgres` |
| Kafka publish route | `raw_to_curated`, `curated_to_kafka` |
| Non-canonical with transform | `raw_to_canonical`, `canonical_to_curated`, then the route's terminal edge |
| API-pull direct-Kafka | `api_to_archive`, `api_to_kafka` |

See [why-control-tables.md](why-control-tables.md) for the design rationale.

---

## 10. Rules to internalise

| Rule | Why |
|---|---|
| One `run_id` per task per attempt. Never pre-allocate. | Restart-safe. Survives DAG-level retries. |
| `file_id` is sticky. Pass it through every task. | One source of truth for the file. |
| Orchestration parent → `run_log.orchestrators`. Data parent → `lineage_edge.upstream_run_id`. | Different questions, different columns. |
| First edge in a chain has `upstream_run_id = NULL`, `source_file_id` set. | The file is the data source — no upstream run exists. |
| `_ods_run_id` on target rows = the load run that wrote them. | "Who physically wrote this row." |
| Write `lineage_edge` AFTER the data exists, not before. | Otherwise lineage lies on failure. |
| Use `ods_pipeline.*` helpers, not raw SQL. | They handle commits, timestamps, idempotency. |

---

# Appendix A — The four enum-like string fields

Four string columns look freeform but are actually controlled vocabularies. Three of them are filtered on by application code, so spelling them wrong silently breaks downstream behaviour. Always pick from the lists below.

## A.1 `run_log.pipeline_type` — what kind of run is this

Identifies the role of a `run_log` row. **Filtered by application code.**

| Value | Written by | Meaning |
|---|---|---|
| `orchestration` | DAG start task (`dag_ingest`, `dag_ingest_direct_postgres`) | Wrapper run for a whole DAG. No data work. |
| `ingestion` | Ingestion Glue/task | Raw → curated. |
| `direct_postgres` | Load task | Curated → Postgres target. |
| `publish` | Kafka publish task | Curated → Kafka topic. |
| `canonicalize` | Non-canonical transform | Raw → canonical schema. |
| `message_api` | Event-API ingest | API/event-driven ingest run. |

**Code that cares:**

- `airflow/dags/dag_recon_t2.py` filters rows `pipeline_type in ("ingestion", "orchestration", "message_api")` to decide which reconciliation to run.
- `ods_pipeline/ingest/api_pull/linkage.py` looks up the orchestration parent via `WHERE pipeline_type='orchestration'`.

If you invent a new value here, **add it to those filters first**.

## A.2 `lineage_edge.edge_type` — what kind of data movement

One per `lineage_edge` row. Describes the physical data hop.

| Value | Written by | Meaning |
|---|---|---|
| `raw_to_curated` | Ingestion finalising | Source file → curated Parquet. |
| `curated_to_postgres` | Direct-Postgres load | Curated → target table. |
| `curated_to_kafka` | Publish job | Curated → Kafka topic. |
| `raw_to_canonical` | Canonicalize job | Raw → canonical Avro. |
| `api_to_kafka` | API-pull direct-Kafka | Polled records → Kafka, no curated stage. |
| `api_to_archive` | API-pull archive writer | Polled records → S3 JSONL archive. |
| `replay` | DLQ ops tooling | Marks a run that replayed a previously-failed run. `upstream_run_id` = the original failed run. |

**Does code care about the string?**
**No application code currently branches on this value.** Dashboards group by it (`scripts/lineage_viewer.py`, `scripts/ops_control_dashboard.py`) and the recon design walks lineage by `edge_type`, but no logic changes its behaviour based on the string. The values exist for **operators reading the table**.

Bottom line: invent new ones if you must, but reuse existing strings for existing semantics. Dashboards group by exact match — typos create new buckets.

## A.3 `run_log.orchestrators[].edge_type` — what kind of orchestration link

This is a **different namespace** from `lineage_edge.edge_type`. Values describe *why this run was scheduled*, not data movement. **Filtered by application code.**

| Value | Used when | Meaning |
|---|---|---|
| `orchestrates` | Any task started by a DAG run | "The DAG scheduled me." Default for task → orchestration link. |
| `triggered_by_api_pull` | `dag_ingest` triggered by `dag_api_pull` | The api_pull watermark sensor walks `orchestrators @> [{edge_type: "triggered_by_api_pull"}]` to find the right dag_ingest run. |
| `triggered_by` | Generic external trigger | Used for replay-from-API or manual replays where the launcher isn't another DAG. |
| `replay_of` | DLQ replay | This run is replaying a failed run; `parents[].run_id` = the failed run id. |

**Code that cares:**

- `ods_pipeline/ingest/api_pull/linkage.py`:
  ```sql
  WHERE pipeline_type='orchestration' AND orchestrators @> '[{"edge_type": "triggered_by_api_pull"}]'::jsonb
  ```
  Misspell `triggered_by_api_pull` and the watermark sensor silently stops promoting. **Critical to get right.**

## A.4 `run_stage_log.event_type` — stage lifecycle event

Two rows per stage (one started, one terminal). The terminal row's `event_type` says how the stage ended. **Filtered by application code.**

| Value | Meaning | Status pair |
|---|---|---|
| `stage_started` | Stage begun, work not yet done | `status='running'` |
| `stage_completed` | Stage finished successfully | `status='succeeded'` |
| `stage_failed` | Stage threw an error | `status='failed'` |
| `stage_skipped` | Stage opted out (e.g., no DQ rules configured) | `status='skipped'` |
| `stage_warned` | Stage produced soft warnings but succeeded overall | `status='warned'` |
| `heartbeat` | Long-running stage still alive | `status='running'` |

Python callers use `ods_pipeline.models.StageEvent` constants — `StageEvent.COMPLETED`, `StageEvent.FAILED`, etc. — instead of bare strings. Prefer the constant.

**Code that cares:**

- `ods_pipeline/runs.py` `finalise()` validates that every opened stage closed by checking:
  ```sql
  WHERE event_type NOT IN ('stage_completed','stage_failed','stage_skipped','stage_warned')
  ```
  If a stage was opened with `stage_started` and never paired with a terminal event, finalise refuses to mark the run succeeded.
- `dag_ingest.py:447` queries `WHERE event_type='stage_completed'` to find which stages of a previous attempt already finished (for restart logic).
- `db/migrations/33_control_table_functions.sql` `control_start_stage` looks up the last `event_type='stage_started'` row to allocate the next `attempt_number`.

If you add a new value here, you also need to decide:
- Is it terminal? If yes, add it to the `NOT IN` list in `finalise()`.
- Is it `stage_started`-like? If yes, the attempt-number logic in `control_start_stage` needs to account for it.

## A.5 Summary — does the code care?

| Field | Code cares? | If you misspell, what breaks |
|---|---|---|
| `run_log.pipeline_type` | **Yes** | Reconciliation skips the row; api_pull linkage can't find the orchestration parent. |
| `lineage_edge.edge_type` | **No** (operator-facing) | Dashboards show a new bucket; nothing breaks functionally. |
| `run_log.orchestrators[].edge_type` | **Yes** (`triggered_by_api_pull` only) | Watermark sensor silently stops promoting cursors. |
| `run_stage_log.event_type` | **Yes** | `finalise()` blocks the run; restart logic miscounts. |

**Rule of thumb:** if it ends up on `run_log` or `run_stage_log`, treat it as a controlled vocabulary and reuse the existing constants. `lineage_edge.edge_type` is the only freeform one.
