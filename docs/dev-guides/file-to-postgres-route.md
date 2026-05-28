# File → Postgres Route — Developer Guide

**Scope:** the single route from an SFTP-dropped file to rows in a Postgres target table. Nothing else.

**Reading time:** 8 minutes.

```text
SFTP drop
   │
   ▼
S3 raw           (immutable copy of the source file)
   │
   ▼
S3 curated       (cleaned, schema-validated, DQ-checked Parquet)
   │
   ▼
ods.<table>      (target rows in Postgres)
```

Each arrow is one task. Each task writes one or more control-table rows so the route is debuggable, replayable, and provably complete.

---

## 1. The three UUIDs

| Name | Lives in | Born when | Lifetime |
|---|---|---|---|
| `file_id` | `pipeline.file_catalogue.file_id` | Landing task accepts the file | Forever for that file |
| `ingestion_run_id` | `pipeline.run_log.run_id` (row with `pipeline_type='ingestion'`) | Ingestion task starts | One per ingestion attempt |
| `pg_write_run_id` | `pipeline.run_log.run_id` (row with `pipeline_type='direct_postgres'`) | Load task starts | One per load attempt |

Optional fourth, only if a DAG owns the end-to-end route:

| Name | Lives in | Born when |
|---|---|---|
| `route_run_id` | `pipeline.run_log.run_id` (row with `pipeline_type='orchestration'`) | DAG start task |

**Rule:** allocate a `run_id` *when the task starts*. Never pre-mint downstream UUIDs.

---

## 2. The five control tables this route touches

| Table | Holds | Keyed by |
|---|---|---|
| `pipeline.file_catalogue` | One row per received file with lifecycle state. | `file_id` |
| `pipeline.run_log` | One row per task run. | `run_id` |
| `pipeline.run_stage_log` | Two rows per stage (start, finish). | `run_id, stage, attempt_number` |
| `pipeline.lineage_edge` | One row per data movement (raw→curated, curated→postgres). | `consumer_run_id, upstream_run_id, source_file_id` |
| `pipeline.reconciliation_log` | Row-count sanity checks. | `run_id, check_type` |

Plus the target table `ods.<your_table>` with `_ods_file_id` and `_ods_run_id` metadata columns linking back.

`pipeline.file_processing_attempt` exists for retry idempotency but most code paths don't touch it directly.

---

## 3. Cheat sheet — which UUID goes where

```text
file_catalogue.file_id                                  = file_id
file_catalogue.last_run_id   after ingestion succeeds   = ingestion_run_id
                             after load succeeds        = pg_write_run_id

run_log.run_id        (orchestration row, optional)     = route_run_id
run_log.run_id        (ingestion row)                   = ingestion_run_id
run_log.run_id        (direct_postgres row)             = pg_write_run_id
run_log.file_id       on every task row                 = file_id
run_log.orchestrators on task rows                      = [{run_id: route_run_id,
                                                           edge_type: 'orchestrates'}]

run_stage_log.run_id                                    = the task's own run_id

lineage_edge   raw_to_curated:
    consumer_run_id                                     = ingestion_run_id
    source_file_id                                      = file_id
    upstream_run_id                                     = NULL  (file IS source)

lineage_edge   curated_to_postgres:
    consumer_run_id                                     = pg_write_run_id
    upstream_run_id                                     = ingestion_run_id
    source_file_id                                      = file_id

reconciliation_log.run_id                               = the task's own run_id

target row    _ods_file_id                              = file_id
target row    _ods_run_id                               = pg_write_run_id
                                                         (load run, NOT ingestion)
```

---

## 4. The "parent" trap

The single biggest confusion. Three columns share the word *parent*. Three different questions:

| Question | Column | Typical value |
|---|---|---|
| Who scheduled me? | `run_log.orchestrators[].run_id` | `route_run_id` |
| Whose **data output** did I read? | `lineage_edge.upstream_run_id` | `ingestion_run_id` |
| Which **file** did I read? | `lineage_edge.source_file_id` | `file_id` |

**Sanity test:** if the row you're pointing at vanished, would data be lost?
- DAG row gone → ingestion still has the raw file → DAG was **orchestration parent** → goes in `orchestrators`
- Ingestion row gone → curated Parquet still exists, but lineage of who made it is lost → **data parent** → goes in `upstream_run_id`
- File row gone → source file untraceable → **file parent** → goes in `source_file_id`

---

## 5. Worked example — one CSV, one load

```text
domain          insurance
dataset         country_codes
business_date   2026-05-21
s3 raw path     s3://ods-raw/insurance/country_codes/date=20260521/country_codes_20260521.csv
s3 curated path s3://ods-curated/insurance/country_codes/date=20260521/
target table    ods.insurance_country_code

file_id           = F
route_run_id      = R   (optional; only if a DAG owns the route)
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

State after:

```text
file_catalogue:  F | received | sftp_path=... | s3_raw_path=... | last_run_id=NULL
run_log:         (empty)
```

### Step 2 — DAG start (optional)

```python
route_run_id = uuid4()
ods_pipeline.runs.start(
    conn, run_id=route_run_id, pipeline_type="orchestration",
    domain="insurance", dataset="country_codes",
    business_date="2026-05-21", file_id=file_id,
)
```

State after:

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

State after:

```text
run_log:  R | orchestration | F | running    | orchestrators=[]
          I | ingestion     | F | running    | orchestrators=[{R, orchestrates}]
```

### Step 4 — Ingestion writes stage evidence

For each stage (`raw_read`, `schema_validate`, `dq_check`, `curated_write`):

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

`run_stage_log` accumulates two rows per stage, all carrying `run_id=ingestion_run_id`.

### Step 5 — Ingestion finishes

Four writes mark the ingestion run terminal:

```python
# (1) curated data now exists — flip catalogue
ods_pipeline.files.update_catalogue(
    conn, file_id=file_id, state="curated",
    s3_curated_path="s3://.../curated/.../",
    last_run_id=ingestion_run_id,
)

# (2) record raw → curated data movement
ods_pipeline.lineage.write_edge(
    conn,
    consumer_run_id=ingestion_run_id,
    source_file_id=file_id,           # file IS the source — no upstream run
    edge_type="raw_to_curated",
    source_ref="s3://.../raw/...csv",
    target_ref="s3://.../curated/.../",
    record_count=100,
)

# (3) row count sanity check
ods_pipeline.reconciliation.write_check(
    conn, check_type="t0_ingestion_count",
    run_id=ingestion_run_id,
    domain="insurance", dataset="country_codes",
    business_date="2026-05-21",
    source_count=100, accounted_count=100, status="ok",
)

# (4) close the run
ods_pipeline.runs.update(
    conn, ingestion_run_id, status="succeeded",
    record_count_source=100, record_count_dq_pass=100,
    record_count_dq_fail=0,
)
```

State after:

```text
file_catalogue:  F | curated | s3_curated_path=... | last_run_id=I
lineage_edge:    consumer=I | upstream=NULL | source_file=F | raw_to_curated | 100
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

### Step 7 — Load writes target + finishes

Stage evidence first (one `postgres_write` start + finish pair). Then four wrap-up writes:

```python
# (1) record curated → postgres data movement (BOTH parents now set)
ods_pipeline.lineage.write_edge(
    conn,
    consumer_run_id=pg_write_run_id,
    upstream_run_id=ingestion_run_id,   # ingestion produced the curated input
    source_file_id=file_id,              # carry through file linkage
    edge_type="curated_to_postgres",
    source_ref="s3://.../curated/.../",
    target_ref="jdbc:postgresql://.../ods.insurance_country_code",
    record_count=100,
)

# (2) row count check against the actual target table
ods_pipeline.reconciliation.write_check(
    conn, check_type="direct_postgres_count",
    run_id=pg_write_run_id,
    domain="insurance", dataset="country_codes",
    source_count=100, postgres_count=100, status="ok",
)

# (3) file lifecycle reaches terminal "loaded"
ods_pipeline.files.update_catalogue(
    conn, file_id=file_id, state="loaded", last_run_id=pg_write_run_id,
)

# (4) close the load run
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

## 6. FAQ — the questions that come up

**Q. Why is `upstream_run_id` NULL on the raw→curated edge?**
The file is the data source. There is no upstream *run* to point to. `source_file_id=file_id` carries the linkage. The first edge in any chain looks like this.

**Q. The DAG scheduled me. Is it my "parent"?**
Yes, in the orchestration sense. Put it in `run_log.orchestrators`. It is **not** the data parent — that belongs in `lineage_edge.upstream_run_id`.

**Q. What is `_ods_run_id` for — ingestion or load?**
Always the run that physically wrote the row to the target table. For this route that is `pg_write_run_id`.

**Q. Same file dropped twice with identical content. What happens?**
`pipeline.file_catalogue` upserts on `(domain, dataset, s3_raw_path)` — same row, same `file_id`. If processing was already complete (`state='loaded'`), the downstream tasks see no new work and exit.

**Q. What does `state='loaded'` mean exactly?**
Terminal success state for this route: target rows are visible in `ods.*`. (Older code used `'sunk'`; the value is now `'loaded'`.)

**Q. Do I write `lineage_edge` before or after the data exists?**
**After.** Always. If the work fails after the edge is written, lineage lies. The pattern is: do the work, then record what happened.

**Q. Do I write SQL directly, or use helpers?**
Use `ods_pipeline.*` helpers. They wrap the stored procs and handle commits, timestamps, idempotency. Direct SQL is reserved for migrations and operator queries.

**Q. What if the source columns don't match the target table schema?**
Configure `is_canonical=false` and a `transform_yaml_path` on the dataset config. The transform runs between curated and load — same control-table writes, same flow, with a `transform` stage in `run_stage_log`. No extra runs.

---

## 7. Debug queries

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

Full lineage chain for one file?

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

Which stages opened but never closed (broken / stuck runs)?

```sql
SELECT run_id, stage, attempt_number, started_at
  FROM pipeline.run_stage_log s1
 WHERE event_type = 'stage_started'
   AND NOT EXISTS (
        SELECT 1 FROM pipeline.run_stage_log s2
         WHERE s2.run_id = s1.run_id
           AND s2.stage = s1.stage
           AND s2.attempt_number = s1.attempt_number
           AND s2.event_type IN ('stage_completed','stage_failed',
                                 'stage_skipped','stage_warned')
       );
```

---

## 8. Rules to internalise

| Rule | Why |
|---|---|
| One `run_id` per task per attempt. Never pre-allocate. | Restart-safe. Survives DAG retries. |
| `file_id` is sticky — pass through every task. | One source of truth for the file. |
| Orchestration parent → `run_log.orchestrators`. Data parent → `lineage_edge.upstream_run_id`. They are different columns answering different questions. | Avoids the "parent" trap. |
| First edge in a chain: `upstream_run_id = NULL`, `source_file_id` set. | The file is the data source — no upstream run exists. |
| `_ods_run_id` on target rows = the load run, never ingestion. | "Who physically wrote this row." |
| Write `lineage_edge` AFTER the data exists, not before. | Otherwise lineage lies on failure. |
| Use `ods_pipeline.*` helpers, not raw SQL. | They handle commits, timestamps, idempotency. |

---

## 9. What this route does NOT do

This guide deliberately stops at the Postgres target. If your dataset also needs any of these, see the general guide:

- **Kafka publish** (`curated_to_kafka` edges)
- **API-pull sources** (`api_to_archive`, `api_to_kafka` edges)
- **Non-canonical → canonical transforms with separate canonicalize runs**
- **Multi-source gold tables** (one load run consumes multiple files)

The vocabulary is identical — the same UUIDs, the same six tables, the same parent rules — only the `edge_type` values change. See [control-tables-developer-guide.md](control-tables-developer-guide.md) Appendix A for the full type catalog.
