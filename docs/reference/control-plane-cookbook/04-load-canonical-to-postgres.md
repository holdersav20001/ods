# Building block 4 — Load canonical parquet into the dataset's Postgres target

> Part of the **control-plane cookbook**: one recipe per *distinct chunk of work*. The
> single-file and multi-file workflows are just these blocks composed in different orders.
> This block is the terminal leg for `delivery=direct_postgres` datasets — it runs once after
> canonicalize (single-file) or once after merge (multi-file).

---

## Use case (your words)

> "I have canonical parquet. Load it into the dataset's Postgres target table — upsert or append
> per config — stamping every row with its lineage id so each Postgres row traces back to the
> canonical input (and onward to raw). Re-running must be safe (no double-load)."

---

## You provide (inputs)

| Input | Where from | Example |
|-------|-----------|---------|
| `run_id` | minted by the orchestrator (Airflow task) | uuid |
| `domain`, `dataset` | the dataset you're loading | `insurance`, `party` |
| `s3_input_path` | the **canonical** parquet (or curated, for non-canonicalized datasets) | `s3://ods-canonical-local/insurance/party/date=2026-06-01/` |
| `file_id` | optional; the source file's catalogue id | uuid |
| `upstream_run_id` | optional; the canonicalize (or merge) run | uuid |

Everything else — `postgres_target_table`, `write_mode`, `key_fields`, `is_canonical`,
`transform_yaml_path`, `recon_tolerance_records` — comes from **`dataset_config`**. You do not pass it.
`load_dataset_config(conn, domain, dataset)` reads it. This is the real sequence from
`glue/jobs/ods_postgres_write.py` (`run(...)`).

## You produce (outputs)

- Rows in the dataset's Postgres target table (`dataset_config.postgres_target_table`, e.g. `ods.party`),
  **every one stamped with `_ods_lineage_link_id`** so it dereferences to this write event and onward
  through `lineage_edge` to the canonical input and the raw file.
- A complete control-plane trail (below) ending in `run_log.status = 'succeeded'`.
- Rows that violate a Postgres constraint go to the DLQ (see Failure handling).

---

## Steps + control-table interactions

Order matters. Each step says **what you do** and **what control record it touches** (and the API).

**0. Connect + resolve config.**
`conn = ods_pipeline.connect()`, then `config = load_dataset_config(conn, domain, dataset)`. Pull
`target = config["postgres_target_table"]` (must be `schema.table`, e.g. `ods.party`),
`write_mode = config.get("write_mode") or "upsert"` (`append` | `upsert`), `key_fields` (the upsert key),
`is_canonical`, and `transform_yaml_path`.

**1. Open the run.**
`ods_pipeline.runs.start(conn, run_id=…, pipeline_type="direct_postgres", domain=…, dataset=…, business_date=None, file_id=…, orchestrators=[{"run_id": upstream_run_id, "edge_type": "orchestrates"}])`
→ **`run_log`** row, `status='running'` (idempotent on `run_id`). The orchestration parent is recorded
here as an `orchestrates` edge — it is **not** the data-lineage upstream (that is discovered in step 5).

**2. Stage CURATED_READ — read canonical parquet → DataFrame.**
`ods_pipeline.stages.start(... stage=Stage.CURATED_READ ...)` → **`run_stage_log`** (`stage_started`).
Read `df = spark.read.parquet(s3a_path)`. For `is_canonical=false` datasets, the transform mapping at
`transform_yaml_path` is compiled to a single `selectExpr` projection (source-shape → canonical-shape;
ODS metadata appended as passthrough in the same projection — no shuffle, no row-aligned join).
Then **strip the upstream lineage handle and stamp this write's**: drop any curated `_ods_run_id` /
`_ods_file_id` (legacy, removed by migration 36), mint `lineage_link_id = uuid4()`, and
`df = df.withColumn("_ods_lineage_link_id", F.lit(lineage_link_id))`. `curated_count = df.count()`.
`stages.finish(... CURATED_READ, status="succeeded", record_count_out=curated_count)` → **`run_stage_log`**.

> The `lineage_link_id` is **minted here, before the rows are written**, and the same value is both
> stamped on every row and (in step 5) used as the `lineage_link` PK. That is the design intent. The
> bug is the *commit ordering*, not the id minting — see step 3 and the failure section.

**3. Stage SINK_PG_WAIT — write the rows.**
`stages.start(... stage=Stage.SINK_PG_WAIT ...)` → **`run_stage_log`**. Then
`_dispatch_write(df, write_mode=…, target=…, key_fields=…, run_id=…)`:
- **upsert** (`_write_upsert`): Spark JDBC writes the stamped rows to a per-run stage table
  `<table>_stage_<run_id_short>` (`mode("overwrite")`), then one psycopg2 transaction runs
  `INSERT INTO <target> (...) SELECT ... FROM <stage> ON CONFLICT (<key_fields>) DO UPDATE SET ...`,
  drops the stage, and **commits**. UUID columns (incl. `_ods_lineage_link_id`) are cast `::uuid` on the SELECT.
- **append** (`_write_append`): same stage-then-`INSERT...SELECT` shape but no `ON CONFLICT`; straight insert, commit.

  **The target rows are committed here (`conn.commit()` inside `_write_upsert`/`_write_append`).**

**4. Reconciliation check.**
`postgres_count = _postgres_count_for_link(conn, target, lineage_link_id)` —
`SELECT COUNT(*) FROM <target> WHERE _ods_lineage_link_id = <id>`. Compare to `curated_count` within
`recon_tolerance_records`. `ods_pipeline.reconciliation.write_check(... check_type="direct_postgres_count", source_count=curated_count, postgres_count=…, status="ok"|"failed")`
→ **`reconciliation_log`** (1).

**5. Lineage — discover upstream, then write the link.**
Discover the **data-lineage** upstream (not the orchestration parent): use `--upstream_run_id` *if it
exists in `run_log`*, else the most recent succeeded `canonicalize` run for `file_id`, else the most
recent succeeded `ingestion` run. Pick the edge verb from the layer actually read:
`'canonical_to_postgres'` if `/canonical/` is in `s3_input_path`, else `'curated_to_postgres'`
(`ods_postgres_write.py:538-542`).
`ods_pipeline.lineage.write_link(conn, lineage_link_id=<the minted id>, consumer_run_id=run_id, edge_type=edge_verb, target_ref="jdbc:postgresql://.../<target>", contributions=[{input_slot:"main", upstream_run_id:<canon/merge run>, source_file_id:file_id, source_ref:s3_input_path, record_count:curated_count}])`
→ **`lineage_link`** (1) + **`lineage_edge`** (1, `input_slot='main'`), committed.

**6. Finish stage + catalogue + status.**
`SINK_PG_WAIT` finished (`run_stage_log`); if `file_id`, `files.update_catalogue(... state="loaded")`;
`runs.update(conn, run_id, status="succeeded"|"failed", record_count_source=curated_count, record_count_target=postgres_count)` → **`run_log`**.

> ### REQUIRED order vs what the code does today (read this)
> The correct, crash-safe order is:
> **write_link (commit) → THEN commit the target rows → THEN status=succeeded.**
> Every target row carries `_ods_lineage_link_id`, so the `lineage_link` row it points at must already
> exist and be committed before the rows are durable.
>
> **The code currently does this backwards.** In `glue/jobs/ods_postgres_write.py`, `_dispatch_write`
> at lines 473-479 calls `_write_upsert`/`_write_append`, which **commit the target rows** at
> `_write_upsert` line 247 (`conn.commit()`) / `_write_append` line 175. Only afterward does
> `lineage.write_link(...)` run at lines 544-558. So the rows are committed **before** the lineage_link
> exists — the same inversion flagged in `ods_merge.py` (rows committed at `:215`, `write_link` at `:341`).
> A crash in that window leaves Postgres rows whose `_ods_lineage_link_id` points at a `lineage_link`
> row that never got written: dangling lineage. **Fix:** call `write_link` and commit it first, then
> write/commit the rows, then flip status. Until then this ordering is a known, documented gap.

---

## Control records written (summary)

| Table | Rows | When |
|-------|------|------|
| `run_log` | 1 | step 1 (`running`) → step 6 (`succeeded`/`failed`) |
| `run_stage_log` | 2 | one per stage: CURATED_READ, SINK_PG_WAIT |
| `lineage_link` | 1 | step 5, `edge_type='canonical_to_postgres'` (or `curated_to_postgres`) |
| `lineage_edge` | 1 | step 5, `input_slot='main'`, `upstream_run_id`=canon/merge run |
| `reconciliation_log` | 1 | step 4, `check_type='direct_postgres_count'` |
| `ods.<target>` (e.g. `ods.party`) | N | step 3, every row stamped `_ods_lineage_link_id` |
| *DLQ* | 0..N | step 3, rows rejected by Postgres constraints |

---

## Idempotency / re-run (read before you schedule retries)

- **upsert** is **safe to re-run.** The stage-and-merge keys on `dataset_config.key_fields` via
  `ON CONFLICT (...) DO UPDATE`; re-running with the same business data overwrites the same keys.
  Note that each re-run mints a *new* `lineage_link_id` and re-stamps the rows it touches, so the
  newest write event owns those rows' lineage — that is correct.
- **append is NOT idempotent.** A straight insert on retry **double-loads**. If a dataset uses
  `write_mode=append`, you **must** guard the re-run yourself — e.g. delete-by-`business_date`-then-insert,
  or dedupe by run before insert. There is no built-in guard in the append path today. Treat append
  datasets as at-most-once only if the orchestrator guarantees exactly-once delivery; otherwise add the guard.

---

## Failure handling — as designed + the gaps

1. **Row-level constraint failure (type / NOT NULL / PK/unique).** Rows that violate the target table's
   constraints should go to the **DLQ** with enough context to replay: `run_id`, `file_id`,
   `s3_input_path`, target table, the offending row, and the constraint that rejected it. The good rows
   still load; the run records the rejected count.
2. **Whole-write failure.** On any exception, `run(...)` sets `run_log.status='failed'` with an
   `error_summary`. Fix the data/config and re-run (upsert is safe; append needs the guard above).

> **DLQ design note (honest gap):** there is **no first-class DLQ table** in the control plane today —
> only `ops/dlq.py` whole-run replay. The stage-and-merge path currently runs the merge in one psycopg2
> transaction, so a single bad row aborts the whole batch (all-or-nothing) rather than quarantining the
> offender. For a real row-level DLQ you need a durable DLQ sink (a table, or an S3 DLQ prefix + a `dlq`
> row keyed by `run_id`) **and a drain/replay path**. This is the "DLQ + replay" building block (separate recipe).

---

## Reconciliation

Invariant: **`canonical_count == rows_written`** (upsert: inserted+updated; append: inserted).
Implemented as: compare the canonical parquet row count (`curated_count`) to the post-load count in the
`ods.*` target for *this write event* — `COUNT(*) WHERE _ods_lineage_link_id = <id>` — within
`recon_tolerance_records`. Recorded in **`reconciliation_log`** (`check_type='direct_postgres_count'`,
`source_count=curated_count`, `postgres_count`, `status`). A breach beyond tolerance fails the run
(`status='failed'`), not just logs.

---

## Done when

- `run_log.status='succeeded'`, `record_count_source` (=canonical count) and `record_count_target`
  (=rows in target for this link) populated.
- Exactly **one** `lineage_link` (`canonical_to_postgres` or `curated_to_postgres`) + its
  `lineage_edge` (`input_slot='main'`, upstream = canon/merge run) exist **before** the rows are
  committed and **before** the success status. *(Today the code commits rows first — see the ordering
  callout; this is the target invariant, not the current behaviour.)*
- Target rows present in `ods.<target>` and **every one carries `_ods_lineage_link_id`** (NOT NULL,
  migration 36 / `db/migrations/38_party_direct_pg_target.sql`).
- Counts reconcile (`reconciliation_log.status='ok'`).

> **FK gap (honest note):** `_ods_lineage_link_id` is `NOT NULL` but has **no foreign key** to
> `pipeline.lineage_link(lineage_link_id)` — see migration 36 §5 and `db/migrations/38_party_direct_pg_target.sql`
> line 17 (`_ods_lineage_link_id uuid NOT NULL`, no `REFERENCES`). The link from a target row back to its
> lineage is enforced **only by convention** today. **Recommend** adding the FK
> (`REFERENCES pipeline.lineage_link(lineage_link_id) ON DELETE RESTRICT`) once the write-ordering above
> is fixed — with the current inverted commit order the FK would (correctly) reject the rows, which is
> exactly why the ordering must be fixed first.

---

## Copy-paste skeleton (real API)

```python
import uuid
import ods_pipeline
from pyspark.sql import functions as F
from utils import load_dataset_config
Stage, StageEvent = ods_pipeline.Stage, ods_pipeline.StageEvent

conn = ods_pipeline.connect()
cfg = load_dataset_config(conn, domain, dataset)
target      = cfg["postgres_target_table"]          # e.g. "ods.party"
write_mode  = (cfg.get("write_mode") or "upsert").lower()
key_fields  = cfg.get("key_fields") or []

ods_pipeline.runs.start(conn, run_id=run_id, pipeline_type="direct_postgres",
                        domain=domain, dataset=dataset, business_date=None, file_id=file_id,
                        orchestrators=[{"run_id": upstream_run_id, "edge_type": "orchestrates"}] if upstream_run_id else None)
try:
    ods_pipeline.stages.start(conn, run_id=run_id, stage=Stage.CURATED_READ, input_ref=s3_input_path)
    df = spark.read.parquet(s3_input_path.replace("s3://", "s3a://"))
    # (is_canonical=false -> compile transform_yaml_path to a single selectExpr projection)
    for legacy in ("_ods_run_id", "_ods_file_id"):
        if legacy in df.columns: df = df.drop(legacy)
    lineage_link_id = str(uuid.uuid4())                       # mint BEFORE stamping
    df = df.withColumn("_ods_lineage_link_id", F.lit(lineage_link_id))
    n = df.count()
    ods_pipeline.stages.finish(conn, run_id=run_id, stage=Stage.CURATED_READ,
                               status="succeeded", event_type=StageEvent.COMPLETED, record_count_out=n)

    # REQUIRED order: write_link (commit) BEFORE the rows. (Real code does this AFTER — known bug.)
    edge_verb = "canonical_to_postgres" if "/canonical/" in s3_input_path.lower() else "curated_to_postgres"
    upstream = discover_upstream(conn, upstream_run_id, file_id)   # canon -> ingest fallback
    ods_pipeline.lineage.write_link(conn, lineage_link_id=lineage_link_id, consumer_run_id=run_id,
        edge_type=edge_verb, target_ref=f"jdbc:postgresql://.../{target}", record_count=n,
        contributions=[{"input_slot": "main", "upstream_run_id": upstream,
                        "source_file_id": file_id, "source_ref": s3_input_path, "record_count": n}])

    ods_pipeline.stages.start(conn, run_id=run_id, stage=Stage.SINK_PG_WAIT, input_ref=s3_input_path)
    _dispatch_write(df, write_mode=write_mode, target=target, key_fields=list(key_fields), run_id=run_id)  # commits rows

    pg_n = _postgres_count_for_link(conn, target, lineage_link_id)
    ok = abs(n - pg_n) <= int(cfg.get("recon_tolerance_records") or 0)
    ods_pipeline.reconciliation.write_check(conn, check_type="direct_postgres_count", run_id=run_id,
        domain=domain, dataset=dataset, business_date=None,
        source_count=n, postgres_count=pg_n, status="ok" if ok else "failed")
    ods_pipeline.stages.write(conn, run_id=run_id, stage=Stage.SINK_PG_WAIT,
        event_type=StageEvent.COMPLETED if ok else StageEvent.FAILED, status="succeeded" if ok else "failed",
        record_count_in=n, record_count_out=pg_n)
    if file_id:
        ods_pipeline.files.update_catalogue(conn, file_id, state="loaded", last_run_id=run_id)
    ods_pipeline.runs.update(conn, run_id, status="succeeded" if ok else "failed",
        record_count_source=n, record_count_target=pg_n)
except Exception as exc:
    ods_pipeline.runs.update(conn, run_id, status="failed", error_summary=f"direct_postgres write failed: {exc}")
    raise
```
