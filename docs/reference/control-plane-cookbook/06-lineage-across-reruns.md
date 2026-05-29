# Building block 6 — Lineage that survives re-runs and any trigger

> Cross-cutting recipe. Not a pipeline stage — a *rule every block follows* so that lineage stays
> correct no matter how a job was launched (Airflow task, manual clear, backfill, standalone re-run).

---

## Use case (your words)

> "I had to re-run a stage — I cleared an Airflow task, or I ran one job on its own. How do I guarantee
> the lineage chain stays intact, and that when I'm investigating an issue I can actually reconstruct
> what happened from the control plane?"

---

## The core principle

**Lineage must not depend on the orchestrator handing run-ids around.** If a job only knows its upstream
because Airflow passed `--upstream_run_id` via XCom, then a manual re-run (or a different trigger) that
doesn't pass the right id silently breaks the chain. (This is exactly what the PARTY run showed:
`run_log.orchestrators` had a throwaway parent on ingest and nothing on canonicalize.)

So: **anchor lineage on stable, data-derived keys, and have each job *discover* its upstream from the
control plane.**

---

## The rules

**1. Anchor on `file_id` + `business_date` + `dataset`.**
These survive any trigger and group every run — including re-runs — for one unit of work. `file_catalogue`
is the root anchor. All runs for a file share its `file_id`.

**2. Each job discovers its upstream — it does not trust the passed param.**
Use the passed `--upstream_run_id` only as a *hint*; the source of truth is a control-plane lookup:
```python
ingest_run = ods_pipeline.runs.latest_succeeded_run(
    conn, file_id=file_id, pipeline_type="ingestion"   # the producing stage you depend on
)
if ingest_run is None:
    raise RuntimeError(f"no succeeded ingestion run for file_id={file_id}; cannot establish data parent")
```
`latest_succeeded_run(conn, *, file_id, pipeline_type)` returns the most recent succeeded run of that
type for the file (`ods_pipeline/runs.py:115`). A standalone re-run then re-links correctly because it
*finds* its real parent instead of trusting whatever id (if any) was passed.

> Pattern guard: prefer `--upstream_run_id` **only if it exists in `run_log`**, else discover. That's what
> `ods_postgres_write.py:513-531` already does. Every job should match it.

**3. `lineage_edge.upstream_run_id` is the single source of run parentage.**
`run_log.orchestrators` is **not** used for the data-flow chain (it is being dropped — see the architecture
review). The chain you walk for "where did this come from" is always `lineage_link` → `lineage_edge`.

**4. A re-run mints a NEW `run_id` — tag it.**
Re-runs don't overwrite; they create a new run. To record "this is a reprocess of run X", write a
`lineage_edge` with `edge_type='replay'` from the new run to the original (the `ops/dlq.py` replay
primitive already does this). Now "is this a re-run, and of what?" is queryable.

**5. Make re-runs idempotent at the sink.**
Ingest short-circuits on an already-`completed` file (`file_catalogue.state`). Postgres load must upsert on
`key_fields` (safe) or, for append, delete-by-`business_date`-then-insert — so a re-run doesn't double-load.

---

## Investigating "what happened" — what to query (and what NOT to)

| Question | Query | Don't use |
|----------|-------|-----------|
| Every run for this file | `run_log WHERE file_id = …` | — |
| The data-flow chain (→ raw) | walk `lineage_link`/`lineage_edge` (see `ops/lineage_dashboard/party_lineage_view.sql`) | `run_log.orchestrators` |
| What a specific run did per stage | `run_stage_log WHERE run_id = …` | — |
| Is this run a re-run of another | `lineage_edge WHERE edge_type='replay' AND consumer_run_id = …` | — |
| Where bad rows went | DLQ (see [block 05](05-dlq-and-replay.md)) | — |

`file_id` + `lineage_edge` + `run_stage_log` reconstruct any incident. `orchestrators` does not — it is
inconsistently populated and being removed.

---

## What exists today vs to build

- 🟢 `latest_succeeded_run` exists; `ods_postgres_write.py` already discovers its upstream.
- 🟠 **`ods_canonicalize_file.py` trusts the passed `--upstream_run_id`** (no discovery) — it must resolve
  `file_id` (it isn't passed one today) and discover its ingest parent. *Fix in the plan.*
- 🟠 **`ods_postgres_write.py` records `business_date=None`** — the pg run can't be filtered by date. *Fix.*
- 🟠 **`run_log.orchestrators` to be dropped** — but it also carries orchestration-only edges (api_pull
  trigger, `raw_to_canonical`) that have no `lineage_edge` home; those must be rehomed first. *Phased in the plan.*

---

## Done when

- Any stage re-run **standalone** (no correct `--upstream_run_id`) still writes a correct
  `lineage_edge` to its real upstream, discovered from the control plane.
- Every run carries `file_id` **and** `business_date`, so all runs for a unit of work group cleanly.
- A re-run is identifiable via a `replay` edge to the original.
- An investigator reconstructs the full chain from `file_id` + `lineage_edge` + `run_stage_log` alone.
