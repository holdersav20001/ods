# Building block 2 — Canonicalize curated parquet to canonical parquet

> Part of the **control-plane cookbook**: one recipe per *distinct chunk of work*. The
> single-file and multi-file workflows are just these blocks composed in different orders.
> This block runs once per dataset after its inputs are curated — it sits between the ingest
> block (block 1) and the postgres-load block, and consumes the curated parquet block 1 produced.

---

## Use case (your words)

> "I have curated parquet. Apply the dataset's `transform.yaml` (rename / cast / drop) to produce
> CANONICAL parquet, with rows that fail transformation quarantined to the DLQ, and the control
> plane recording it so the canonical output is traceable to the curated input."

---

## You provide (inputs)

| Input | Where from | Example |
|-------|-----------|---------|
| `run_id` | minted by the orchestrator (Airflow task) | uuid |
| `domain`, `dataset` | the dataset you're canonicalizing | `insurance`, `policies` |
| `s3_curated_path` | the curated parquet the ingest run wrote | `s3://ods-curated-local/curated/insurance/policies/date=20260601/` |
| `business_date` | `YYYY-MM-DD` | `2026-06-01` |
| `upstream_run_id` | **required**; the ingest `run_id` that produced the curated parquet | uuid |
| `airflow_dag_id`, `airflow_run_id` | optional; orchestration context | str |

The transform rules are **not** passed — they come from `datasets/<domain>/<dataset>/transform.yaml`,
loaded by `_load_transform(domain, dataset)`. The canonical output prefix is derived, not passed
(`_canonical_s3_path` → `s3a://$ODS_CANONICAL_BUCKET/canonical/<domain>/<dataset>/date=<bd_compact>/`).

The `transform.yaml` shape:

```yaml
transform_version: 1
is_canonical: true
renames:           # dict old → new
  old_col: new_col
casts:             # dict col → any Spark SQL type expression
  premium_amount: decimal(10,2)
drops:             # list of columns to remove
  - tmp_internal_col
```

## You produce (outputs)

- Canonical parquet at `s3://$ODS_CANONICAL_BUCKET/canonical/<domain>/<dataset>/date=<bd_compact>/`,
  every row stamped with `_ods_lineage_link_id` so it is forensically traceable to the curated input.
- A control-plane trail (below) ending in `run_log.status = 'succeeded'` with `record_count_target` set.
- (Once built) any rows that failed transformation quarantined in the DLQ.

---

## Steps + control-table interactions

Order matters. Each step says **what you do** and **what control record it touches** (and the API).
This is the real sequence from `glue/jobs/ods_canonicalize_file.py::run(...)`.

**0. Connect + discover the upstream file.**
`pg = ods_pipeline.connect()`. Then read the ingest run's `file_id` so the canonical lineage edge can
point at the original source file:
`SELECT file_id FROM pipeline.run_log WHERE run_id = <upstream_run_id>` → `upstream_file_id`. This is
the orchestration link back to block 1 — the canonicalize run inherits the ingest run's `file_id`.

**1. Load the transform.**
`transform = _load_transform(domain, dataset)` reads `datasets/<domain>/<dataset>/transform.yaml`
(resolved via `ODS_DATASETS_ROOT` or `<repo>/datasets`). Missing file → `FileNotFoundError`, no run is
opened. Returns the normalized dict: `renames`, `casts`, `drops`, `is_canonical`, `transform_version`,
`raw_path` (the yaml path, recorded for provenance).

**2. Open the run.**
`control_start_run(pg, run_id=…, pipeline_type="canonicalize", domain=…, dataset=…, business_date=…,
file_id=upstream_file_id, runtime_context={transform_yaml, transform_version, is_canonical,
airflow_dag_id, airflow_run_id, started_at})` → **`run_log`** row, `status='running'`. Then
`pg.commit()` — the run is durable before any Spark work starts.

> Note: this job uses `control_start_run` / `control_patch_run` from `ods_ingestion_control`, the
> thin control-plane shim — **not** the higher-level `ods_pipeline.runs.start` helper that block 1
> uses. Same `run_log` table, same `running → succeeded/failed` lifecycle.

**3. Read curated parquet → DataFrame.**
`df = spark.read.parquet(s3a_in)` (the input path with `s3://` rewritten to `s3a://`).
`source_count = df.count()` — this is the curated row count, the left side of the recon invariant.
*No stage row is written today* (see Gaps below).

**4. Apply the transform.**
`df, actions = apply_transform(df, transform)` applies **renames → drops → casts, in that order**.
Each applied op is appended to `actions` (e.g. `{"op":"cast","column":"premium_amount","type":"decimal(10,2)"}`)
and later stored in `runtime_context.actions_applied` for provenance. Casts that can't parse a value
yield Spark `NULL` rather than throwing — see Failure handling.

**5. Stamp the lineage handle + write canonical parquet.**
- Mint `lineage_link_id = uuid4()` *up front* and stamp every row:
  `df = df.withColumn("_ods_lineage_link_id", F.lit(lineage_link_id))`.
- `df.write.mode("overwrite").parquet(s3a_out)` to the canonical prefix.
- `target_count = df.count()` — the canonical row count, the right side of the recon invariant.

**6. Lineage — write the link BEFORE flipping status.**
`ods_pipeline.lineage.write_link(pg, lineage_link_id=…, consumer_run_id=run_id,
edge_type="curated_to_canonical", target_ref=<canonical s3:// uri>, record_count=target_count,
contributions=[{upstream_run_id, source_file_id:upstream_file_id, source_ref:s3_curated_path,
input_slot:"canonical", record_count:source_count, edge_type:"curated_to_canonical"}])`
→ exactly **one `lineage_link`** + **one `lineage_edge`** (`input_slot='canonical'`, upstream = the
ingest run). The pre-minted `lineage_link_id` is reused so the value stamped on every parquet row
matches the link row exactly.

**7. Flip status to succeeded — AFTER lineage.**
`control_patch_run(pg, run_id=…, fields={status:"succeeded", record_count_source:source_count,
record_count_target:target_count, runtime_context:{…, actions_applied, output_path, lineage_link_id}})`
→ **`run_log`** patch. Then `pg.commit()`. *Status flips only after the lineage row exists* — the
**lineage-before-status invariant**, identical to block 1 step 8.

On any exception, the `except` block patches `run_log.status='failed'` with a truncated
`error_summary`, commits, and re-raises. `finally` stops Spark and closes the connection.

---

## Control records written (summary)

| Table | Rows | When |
|-------|------|------|
| `run_log` | 1 | step 2 (`running`) → step 7 (`succeeded`/`failed`), `pipeline_type='canonicalize'` |
| `run_stage_log` | **0 today** (should be 1 per stage: READ, TRANSFORM, CANONICAL_WRITE) | — *to build* |
| `lineage_link` | 1 | step 6, `edge_type='curated_to_canonical'` |
| `lineage_edge` | 1 | step 6, `input_slot='canonical'`, upstream = ingest `run_id` + `source_file_id` |
| `reconciliation_log` | **0 today** (should be 1, `check_type='canonicalize'`) | — *to build* |
| *DLQ* | **0 today** (should be 0..N failed-transform rows) | — *to build* |

> The job does **not** touch `file_catalogue` — that's block 1's responsibility. It does inherit the
> ingest run's `file_id` (step 0) so the lineage edge can name the original source file.

---

## Failure handling

There are two distinct kinds, mirroring block 1.

1. **Job-level failure** — bad input path, unreadable parquet, `transform.yaml` missing, or the write
   failing. The `except` block sets `run_log.status='failed'` with an `error_summary`, commits, and
   re-raises. *All-or-nothing*: no canonical parquet is left behind that the control plane considers
   valid, because status never reached `succeeded` and **no lineage link was written** (the write is
   ordered after a clean write/count). Fix the cause and re-run with the same `run_id` semantics.

2. **Row-level transform failure** — e.g. a `cast` to `decimal(10,2)` on a value Spark can't parse.
   Spark's `cast` is lenient: it produces `NULL` rather than raising. So **today these rows silently
   pass through as NULLs** and inflate neither a failure count nor the DLQ. The *intended* design:
   detect rows where a non-null source value became null after a declared cast (or otherwise violates
   the transform contract), split them out, and quarantine to the DLQ with `run_id`, the failing
   `column`/`cast` expression, and `source_ref` (the curated path). Then:
   - **partial success** — if *some* rows transform cleanly: write those to canonical, quarantine the
     rest, run **succeeds** with `record_count_target < record_count_source` and a positive transform-
     failure count.
   - **fail-all** — if *every* row fails (e.g. a cast that can never parse the whole column): nothing
     usable is produced → mark the run **failed**, write no lineage link.

> **DLQ design note (to build — honest gap):** there is **no first-class DLQ table** in the control
> plane today, and this job does not split or quarantine any rows — only `ops/dlq.py` exists, and it
> does **whole-run replay**, not row-level capture. For row-level transform failures to be anything
> but silent NULLs you need: (a) a durable DLQ (one table, or an S3 DLQ prefix + a `dlq` row keyed by
> `run_id` + failing column/cast + `source_ref`), and (b) a drain/replay path. Until that exists,
> lenient casts mask data loss. This is the shared "DLQ + replay" building block (separate recipe).

---

## Reconciliation

This block's invariant: **`curated_count == canonical_count + transform_failures`** (within
`recon_tolerance_*`). With `curated_count = source_count` and `canonical_count = target_count`.

> **Gap:** the job records `record_count_source` and `record_count_target` on `run_log`, but does
> **not** write a `reconciliation_log` row today, and (per Failure handling) `transform_failures` is
> not computed — lenient casts make it effectively 0. The *intended* write: a `reconciliation_log`
> row with `check_type='canonicalize'`, `source_count=source_count`,
> `accounted_count=target_count + transform_failures`, and the discrepancy. A breach beyond tolerance
> should **fail the run**, not just log. Mark this **to build** alongside the DLQ.

---

## Done when

- `run_log.status='succeeded'`, with `record_count_source` and `record_count_target` populated.
- Exactly **one** `lineage_link` (`curated_to_canonical`) + its `lineage_edge`
  (`input_slot='canonical'`, upstream = the ingest run) exist **before** the success status.
- Canonical parquet exists at `s3://.../canonical/<domain>/<dataset>/date=<bd_compact>/`, every row
  carrying `_ods_lineage_link_id` equal to the `lineage_link_id` on the link row.
- (Once built) any failed-transform rows are in the DLQ and accounted for in the
  `reconciliation_log` invariant.

---

## Copy-paste skeleton (real API)

```python
import uuid
import ods_pipeline
from ods_ingestion_control import start_run as control_start_run, patch_run as control_patch_run
from glue.jobs.ods_canonicalize_file import _load_transform, apply_transform, _canonical_s3_path

pg = ods_pipeline.connect()

# 0. discover the upstream file_id from the ingest run
with pg.cursor() as cur:
    cur.execute("SELECT file_id::text FROM pipeline.run_log WHERE run_id = %s::uuid", (upstream_run_id,))
    row = cur.fetchone()
upstream_file_id = row[0] if row and row[0] else None

# 1. load the transform
transform = _load_transform(domain, dataset)

# 2. open the run (running) and commit
control_start_run(pg, run_id=run_id, pipeline_type="canonicalize", domain=domain, dataset=dataset,
                  business_date=business_date, file_id=upstream_file_id,
                  runtime_context={"transform_yaml": transform["raw_path"],
                                   "transform_version": transform["transform_version"],
                                   "is_canonical": transform["is_canonical"]})
pg.commit()

try:
    # 3. read curated parquet
    df = spark.read.parquet(s3_curated_path.replace("s3://", "s3a://"))
    source_count = df.count()
    # 4. apply renames -> drops -> casts
    df, actions = apply_transform(df, transform)
    # 5. stamp lineage handle + write canonical parquet
    lineage_link_id = str(uuid.uuid4())
    df = df.withColumn("_ods_lineage_link_id", F.lit(lineage_link_id))
    s3a_out = _canonical_s3_path(domain, dataset, business_date)
    df.write.mode("overwrite").parquet(s3a_out)
    target_count = df.count()
    # 6. lineage FIRST: 1 link + 1 contribution (curated -> canonical)
    ods_pipeline.lineage.write_link(pg, lineage_link_id=lineage_link_id, consumer_run_id=run_id,
        edge_type="curated_to_canonical", target_ref=s3a_out.replace("s3a://", "s3://"),
        record_count=target_count,
        contributions=[{"upstream_run_id": upstream_run_id, "source_file_id": upstream_file_id,
                        "source_ref": s3_curated_path, "input_slot": "canonical",
                        "record_count": source_count, "edge_type": "curated_to_canonical"}])
    # 7. status LAST: succeeded + counts, then commit
    control_patch_run(pg, run_id=run_id, fields={"status": "succeeded",
        "record_count_source": source_count, "record_count_target": target_count,
        "runtime_context": {"actions_applied": actions,
                            "output_path": s3a_out.replace("s3a://", "s3://"),
                            "lineage_link_id": lineage_link_id}})
    pg.commit()
except Exception as exc:
    control_patch_run(pg, run_id=run_id, fields={"status": "failed", "error_summary": str(exc)[:500]})
    pg.commit()
    raise
finally:
    spark.stop(); pg.close()
```
