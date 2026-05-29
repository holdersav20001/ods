# Building block 1 — Ingest a raw file to validated curated parquet

> Part of the **control-plane cookbook**: one recipe per *distinct chunk of work*. The
> single-file and multi-file workflows are just these blocks composed in different orders.
> This block appears once in the single-file flow and once per input file in the multi-file flow.

---

## Use case (your words)

> "I have a raw file (CSV/JSONL) sitting in S3. I need to read it, validate it against the
> registered schema, drop/quarantine bad rows, and write it out as curated parquet — and I
> need the control plane to record what happened so the row is traceable and re-runnable."

---

## You provide (inputs)

| Input | Where from | Example |
|-------|-----------|---------|
| `run_id` | minted by the orchestrator (Airflow task) | uuid |
| `domain`, `dataset` | the dataset you're ingesting | `insurance`, `party` |
| `s3_input_path` | the raw object | `s3://ods-raw-local/insurance/party/2026-06-01/party_20260601.csv` |
| `file_id` | optional; created if absent | uuid |
| `upstream_run_id` | optional; the orchestration parent | uuid |

Everything else (schema_id, raw_format, dq_rules, curated path) comes from **`dataset_config`** —
you do not pass it. `load_dataset_config(conn, domain, dataset)` reads it.

## You produce (outputs)

- Curated parquet at the dataset's `s3_curated_path/date=<business_date>/`.
- A complete control-plane trail (below) ending in `run_log.status = 'succeeded'`.
- Quarantined bad rows in the DLQ (see Failure handling).

---

## Steps + control-table interactions

Order matters. Each step says **what you do** and **what control record it touches** (and the API).
This is the real sequence from `glue/jobs/ingestion/pipeline.py`.

**0. Connect + idempotency guard.**
`conn = ods_pipeline.connect()`. Then `registration.already_completed(conn, s3_input_path)` —
if this file already reached `completed` in **`file_catalogue`**, short-circuit: open a run, mark it
`succeeded` with "already completed", write one `skipped` stage row, return. *This is what makes an
Airflow retry safe* — you never re-ingest the same file.

**1. Resolve business_date + register the file.**
- `reading.resolve_business_date(conn, config=…, s3_input_path=…, file_id=…)` → `YYYY-MM-DD`
  (parsed from the filename via `dataset_config.filename_pattern`).
- `registration.register(conn, run_id=…, domain=…, dataset=…, business_date=…, s3_input_path=…, file_id=…)`
  → **`file_catalogue`** row (via `control_register_file`, idempotent on `(domain,dataset,s3_raw_path)`).
  Returns `(file_id, md5, size)`. The `file_md5` is your dedupe key.

**2. Open the run.**
`ods_pipeline.runs.start(conn, run_id=…, pipeline_type="ingestion", domain=…, dataset=…, business_date=…, config_version_id=…, file_id=…, orchestrators=[{"run_id": upstream_run_id, "edge_type": "orchestrates"}])`
→ **`run_log`** row, `status='running'` (via `control_start_run`, idempotent on `run_id`).

**3. Stage RAW_READ — read raw → DataFrame.**
Wrap in `with stage_scope(conn, run_id=…, stage=Stage.RAW_READ, input_ref=s3_input_path) as s:` —
the context manager writes `stage_started` on entry and `stage_succeeded`/`stage_failed` on exit to
**`run_stage_log`**. Inside: `df = reading.read_raw(spark, s3_input_path, raw_format)`,
`source_count = df.count()`, then `runs.update(conn, run_id, record_count_source=source_count)`
(**`run_log`** patch) and `s.set_result(record_count_out=source_count)`.

**4. Stage SCHEMA_VALIDATE — validate columns against the registry.**
`with stage_scope(... stage=Stage.SCHEMA_VALIDATE ...) as s:` →
`validation.validate_columns(df_columns=df.columns, schema_id=config["schema_id"], schema_version=config["schema_version"])`.
This GETs the Avro subject `schema_id` from Schema Registry and asserts every required (non-`_ods_`)
field is present. Missing columns raise `SchemaValidationError` → the stage writes `stage_failed`
and the run fails (see Failure handling). A 404 (unregistered subject) is tolerated.
`s.set_result(metrics=metrics)`.

**5. Stage DQ_CHECK — quarantine bad rows (row-level DLQ).**
`with stage_scope(... stage=Stage.DQ_CHECK ...) as s:` → `quality.evaluate(df, config_dq_rules=config["dq_rules"], …)`
splits the DataFrame into **passing** and **failing** by the dataset's `dq_rules`.
- Failing rows → **DLQ** (see Failure handling). `failing_count` recorded.
- `runs.update(conn, run_id, record_count_dq_pass=…, record_count_dq_fail=…)` (**`run_log`**).
- If all rows fail → `DQAllRowsFailed` → run fails.

**6. Enrich (no stage row).** `curating.enrich_with_metadata(passing_df, file_id, run_id, domain, dataset, business_date)`
stamps the `_ods_*` columns. Pure transform — nothing written to the control plane.

**7. Stage CURATED_WRITE — write the parquet.**
`with stage_scope(... stage=Stage.CURATED_WRITE ...) as s:` →
`curated_uri, written_count = curating.write_and_verify(passing_df, …, expected_count=source_count-failing_count)`.
Writes curated parquet and verifies the row count. `s.set_result(output_ref=curated_uri, record_count_out=written_count)`.

**8. Finalise — lineage, then status, in that order.**
`finalising.finalise_success(conn, run_id=…, …, curated_uri=…, source_count=…, written_count=…, failing_count=…)` does, **in this order**:
1. `write_link(edge_type="raw_to_curated", consumer_run_id=run_id, target_ref=curated_uri, contributions=[{input_slot:"main", source_file_id:file_id, source_ref:s3_input_path, record_count:written_count}])`
   → **`lineage_link`** (1) + **`lineage_edge`** (1), committed.
2. `mark_curated` → **`file_catalogue.state`** = `curated`.
3. `reconciliation.write_check` → **`reconciliation_log`** (see Reconciliation).
4. `runs.update(status="succeeded", record_count_target=written_count)` → **`run_log`**. *Status flips only
   after the lineage row exists* — this is the invariant.
5. `mark_completed` → **`file_catalogue.state`** = `completed`.

On any exception, `finalising.finalise_failure(...)` sets `run_log.status='failed'` and
`file_catalogue.state='failed'` instead.

---

## Control records written (summary)

| Table | Rows | When |
|-------|------|------|
| `file_catalogue` | 1 (upsert) | step 1; state advances `received→curated→completed` |
| `run_log` | 1 | step 2 (`running`) → step 8 (`succeeded`/`failed`) |
| `run_stage_log` | 4 (+1 skip path) | one per stage: RAW_READ, SCHEMA_VALIDATE, DQ_CHECK, CURATED_WRITE |
| `lineage_link` | 1 | step 8, `edge_type='raw_to_curated'` |
| `lineage_edge` | 1 | step 8, `input_slot='main'`, points at the raw `file_id` |
| `reconciliation_log` | 1 | step 8 |
| *DLQ* | 0..N | step 5, the failing rows |

---

## Failure handling — two distinct kinds

1. **Structural failure (schema)** — columns missing vs the registered schema. *All-or-nothing*: the
   SCHEMA_VALIDATE stage writes `stage_failed`, `finalise_failure` runs, `run_log.status='failed'`,
   `file_catalogue.state='failed'`. The whole file is rejected; **fix the file or schema and re-run**
   (the idempotency guard won't block it because it never reached `completed`).
2. **Row-level failure (DQ)** — individual bad rows. Good rows proceed to curated; **bad rows go to the
   DLQ** with enough context to replay: `run_id`, `file_id`, `s3_input_path`, the failed rule, and the
   row. The run still **succeeds** (partial), with `record_count_dq_fail > 0`.

> **DLQ design note (decide before implementing):** today the pipeline computes `failing_count` and
> can quarantine rows, but there is **no first-class DLQ table** in the control plane — only
> `ops/dlq.py` replay for whole-run replays. For these workflows you need a durable DLQ
> (one table, or an S3 DLQ prefix + a `dlq` row keyed by `run_id`) **and a drain/replay path**, else
> DLQ is a graveyard. This is building block "DLQ + replay" (separate recipe).

---

## Reconciliation

This block's reconciliation invariant: **`source_count == written_count + failing_count`** (within
`recon_tolerance_*`). Recorded in `reconciliation_log` at step 8 (`check_type='ingest'`,
`source_count`, `accounted_count=written_count+failing_count`, discrepancy). A breach beyond tolerance
should fail the run, not just log.

---

## Done when

- `run_log.status='succeeded'`, `record_count_source` / `record_count_dq_pass` / `record_count_dq_fail` / `record_count_target` populated.
- Exactly one `lineage_link` (`raw_to_curated`) + its `lineage_edge` exist **before** the success status.
- `file_catalogue.state='completed'`.
- Curated parquet exists at `curated_uri` with `written_count` rows.
- Any bad rows are in the DLQ, accounted for in reconciliation.

---

## Copy-paste skeleton (real API)

```python
import ods_pipeline
from ods_pipeline.stages import stage_scope
Stage, StageEvent = ods_pipeline.Stage, ods_pipeline.StageEvent

conn = ods_pipeline.connect()
if registration.already_completed(conn, s3_input_path):
    ...  # open run, mark succeeded "already completed", write skipped stage, return

business_date = reading.resolve_business_date(conn, config=cfg, s3_input_path=s3_input_path, file_id=file_id)
file_id, md5, _ = registration.register(conn, run_id=run_id, domain=domain, dataset=dataset,
                                        business_date=business_date, s3_input_path=s3_input_path, file_id=file_id)
ods_pipeline.runs.start(conn, run_id=run_id, pipeline_type="ingestion", domain=domain, dataset=dataset,
                        business_date=business_date, file_id=file_id,
                        orchestrators=[{"run_id": upstream_run_id, "edge_type": "orchestrates"}] if upstream_run_id else None)
try:
    with stage_scope(conn, run_id=run_id, stage=Stage.RAW_READ, input_ref=s3_input_path) as s:
        df = reading.read_raw(spark, s3_input_path, raw_format); n = df.count()
        ods_pipeline.runs.update(conn, run_id, record_count_source=n); s.set_result(record_count_out=n)
    with stage_scope(conn, run_id=run_id, stage=Stage.SCHEMA_VALIDATE, input_ref=s3_input_path) as s:
        s.set_result(metrics=validation.validate_columns(df_columns=df.columns,
                     schema_id=cfg["schema_id"], schema_version=cfg["schema_version"]))
    with stage_scope(conn, run_id=run_id, stage=Stage.DQ_CHECK, input_ref=s3_input_path, record_count_in=n) as s:
        out = quality.evaluate(df, config_dq_rules=cfg["dq_rules"], source_count=n, ...)
        # out.failing rows -> DLQ
        ods_pipeline.runs.update(conn, run_id, record_count_dq_pass=n-out.failing_count, record_count_dq_fail=out.failing_count)
        s.set_result(record_count_out=n-out.failing_count)
    good = curating.enrich_with_metadata(out.passing_df, file_id=file_id, run_id=run_id, domain=domain, dataset=dataset, business_date=business_date)
    with stage_scope(conn, run_id=run_id, stage=Stage.CURATED_WRITE, input_ref=s3_input_path, record_count_in=n) as s:
        curated_uri, written = curating.write_and_verify(good, ..., expected_count=n-out.failing_count)
        s.set_result(output_ref=curated_uri, record_count_out=written)
    finalising.finalise_success(conn, run_id=run_id, ..., curated_uri=curated_uri,
                                source_count=n, written_count=written, failing_count=out.failing_count)
except Exception as exc:
    finalising.finalise_failure(conn, run_id=run_id, ..., error_summary=str(exc)[:500])
```
