# Control-Plane Write Contract

The platform has **one** documented write contract: the ordered sequence every
workflow task follows to record a run, its lineage, its target rows, its
reconciliation, and (when business-visible) its active slice. This reference maps
each step to the **actual** function/wrapper to call in `control/`. It is the
authoritative description of the write API — where the planning spec's mapping
was aspirational (e.g. stage lifecycle), this document reflects the code as it
is. The full platform README/runbook is a separate document.

All wrappers take a live psycopg `conn` as the first positional argument and a
`commit=True` keyword (pass `commit=False` to compose many writes in one
transaction — every demo/test does). They never mint a `workflow_run_id`: the
composer mints it once and threads it to every run.

---

## The official 10-step sequence

```text
1. register file          if a raw file input exists
2. start run
3. start stage
4. application does work
5. finish stage
6. write output_link + input_edge rows
7. write / stamp target rows   if a target sink
8. reconcile output or workflow
9. activate target visibility  if business-visible
10. finish run
```

Steps 3–5 repeat per stage. Steps 6–9 repeat per produced output. Conditional
steps are marked below.

---

## Step → actual wrapper mapping (low-level)

| # | Step | Wrapper to call | Notes |
|---|------|-----------------|-------|
| 1 | Register file *(conditional: only if raw file input)* | `control.runs.register_file(conn, *, s3_raw_path, file_md5, business_date, domain, dataset)` → `file_id` | Idempotent on `(file_md5, business_date)`. Skip for runs whose input is an upstream output, not a raw file (merge/aggregate hops). |
| 2 | Start run | `control.runs.start(conn, *, workflow_run_id, pipeline_type, domain, dataset, business_date, trigger_type, file_id=None, replay_of_run_id=None, orchestrator=None)` → `run_id` | `file_id` links the run to its registered raw file. `orchestrator` carries external (Airflow) identity. |
| 3 | Start stage | `control.stages.stage_scope(conn, run_id, stage, attempt=1)` context manager | **There is no `stages.start` / `stages.finish`.** The context manager opens the stage on `__enter__`. |
| 4 | Application does work | *(your code)* | Set `st.record_in`, `st.record_out`, `st.metrics` on the yielded handle. |
| 5 | Finish stage | (automatic on `stage_scope` exit) | Clean exit → finished `succeeded`; an exception → finished `failed` and re-raised. |
| 6 | Write output_link + input_edge | `control.lineage.write_output_link(conn, *, consumer_run_id, edge_type, target_ref, record_count, inputs, sink_type=None, transform_version=None)` → `output_link_id` | `inputs` is a list of input-edge dicts. Downstream (run-to-run) inputs use the new-name key `upstream_output_link_id`; raw-file leaves use `source_file_id`. `target_ref` MUST be a dict with non-empty `path`, non-empty `content_hash`, and a `version` key. |
| 7 | Write / stamp target rows *(conditional: only if target sink)* | `control.lineage.write_output_then_rows(conn, *, consumer_run_id, edge_type, target_ref, record_count, inputs, rows, sink_type=None, transform_version=None, source_file_id=None)` → `output_link_id` | Use **instead of** `write_output_link` for the sink hop: writes the link + edges **and** the target rows in one transaction, stamping each row with `_ods_lineage_link_id` (FK) and, where the table has it, `_ods_output_link_id`. Pass `source_file_id` only when the rows map cleanly to one source file. |
| 8 | Reconcile | `control.recon.reconcile_sink_link(conn, *, lineage_link_id, source_count)` (per-output) and/or `control.recon.reconcile_workflow(conn, *, workflow_run_id)` (cross-hop) | `reconcile_sink_link` derives the accounted count from the actual stamped rows for **one** output (correct under Decision-#6 fan-out). `reconcile_workflow` compares raw-in vs sink+dlq-out across the whole workflow. `control.recon.write_check(...)` records a caller-supplied two-number check. Must run in the same transaction as the row write. |
| 9 | Activate target visibility *(conditional: only if business-visible)* | `control.visibility.activate(conn, *, domain, dataset, business_date, sink_type, target_name, file_id, output_link_id, producer_run_id, workflow_run_id, replacement_scope="slice", replacement_key=None, supersede=True, reason=None)` → `visibility_id` | Identify the output with the new-name kwarg `output_link_id`. RAISES if the producer run is not `succeeded` or reconciliation is not `ok` — visibility activates only after success. `replacement_scope` selects the refeed policy (`slice` / `business_key` / `file` / `append_only`). |
| 10 | Finish run | `control.runs.finalise(conn, run_id, *, status, record_count_out=None)` | Stamps `finished_at`. On failure call `control.runs.patch(conn, run_id, status="failed", error=...)`. |

### Lineage read helpers (for downstream hops)

A downstream run names its **exact** upstream output before step 6:

- `control.runs.latest_succeeded_run(conn, *, domain, dataset, business_date, pipeline_type)` — the single newest succeeded run for a slice.
- `control.runs.succeeded_runs(conn, *, domain, dataset, business_date, pipeline_type)` — all succeeded runs, newest-first (the 1:N merge fan-in primitive).
- `control.runs.run_output_link(conn, *, run_id, edge_type, target_path=None, content_hash=None)` → `output_link_id` — the output a run produced for an edge type; RAISES on absence/ambiguity (no silent stale pick).
- `control.runs.run_record_count(conn, *, run_id)` — that run's `record_count_out` (the per-slot count for merge inputs).

### Other surfaces

- DLQ: `control.dlq.quarantine(...)` writes a quarantine output_link + `cp.dlq` row; `control.dlq.resolve(...)` closes/replays it.
- Schema: `control.schema.get_contract(...)` / `control.schema.validate_rows(...)` partition rows into good/bad before steps 6–7.

---

## The ergonomic path: `control.sdk.task(...)`

`control.sdk` is a thin context-manager layer over the same wrappers (it calls
them, does not replace them). `task(...)` does step 2 on enter and step 10 on
exit (`succeeded`, or `failed` + error on exception); `run.stage(...)` wraps
`stages.stage_scope`; `run.write_output(...)` wraps `lineage.write_output_link`;
`run.reconcile_sink_link(...)` wraps `recon.reconcile_sink_link`. Ids stay
accessible (`run.run_id`, `stage.stage_log_id`, the returned `output_link_id`,
`run.last_output_link_id`). The `commit` flag is passed straight through.

```python
from control import runs
from control.sdk import task

# Step 1 (conditional) — register the raw file first.
file_id = runs.register_file(
    conn, s3_raw_path="s3://raw/orders/2026-05-29/orders.csv",
    file_md5=md5, business_date="2026-05-29", domain="sales", dataset="orders",
    commit=False)

# Steps 2 + 10 — start/finalise via the context manager.
with task(conn, workflow_run_id=wfid, pipeline_type="ingestion",
          domain="sales", dataset="orders", business_date="2026-05-29",
          trigger_type="manual", file_id=file_id, commit=False) as run:

    # Steps 3–5 — stage lifecycle (finished on exit).
    with run.stage("validate_schema") as stage:
        good, bad = schema.validate_rows(conn, ...)   # step 4: do the work
        stage.finish(record_count_in=len(rows), record_count_out=len(good))

    # Step 6 — record the produced output + its input edges.
    output_link_id = run.write_output(
        edge_type="raw_to_curated",
        target_ref={"path": "s3://curated/orders/...", "content_hash": h, "version": 1},
        record_count=len(good),
        inputs=[{"edge_type": "raw_to_curated",
                 "source_file_id": file_id,
                 "source_ref": {"path": "s3://raw/orders/2026-05-29/orders.csv"},
                 "record_count": len(good)}])
    # also available as run.last_output_link_id
```

For the sink hop, call `lineage.write_output_then_rows(...)` (step 7) to write
the link **and** stamp the target rows, then `run.reconcile_sink_link(output_link_id, source_count=...)`
(step 8) and `visibility.activate(...)` (step 9, if business-visible) before the
`task` block exits and finalises the run.

A downstream run's `inputs` point at upstream outputs by id:

```python
inputs=[{"edge_type": "merge_to_canonical",
         "upstream_output_link_id": upstream_link_id,   # NEW-name key
         "input_slot": 0,
         "record_count": runs.run_record_count(conn, run_id=upstream_run_id)}]
```

---

## Invariants this contract guarantees

These hold for **any** compliant workflow and are enforced by the cross-cutting
acceptance tests in `tests/test_write_contract.py`:

- every run has a `cp.run_log` row with a terminal status;
- every run has ≥1 `cp.run_stage_log` row (step 3 is not optional);
- every successful run of an output-producing `pipeline_type` (ingestion,
  canonicalization, merge, sink, aggregation) has ≥1 `output_link`;
- every `output_link` has ≥1 `input_edge`;
- every run-to-run (downstream) `input_edge` carries a non-null
  `upstream_output_link_id`; file-leaf edges instead carry `source_file_id`;
- target rows are stamped with `_ods_output_link_id`;
- business-visible rows have a corresponding `ods.target_visibility` row (only
  for workflows that perform step 9 — e.g. the policy/claims demo; the
  customer/transaction demo intentionally does not activate visibility).
