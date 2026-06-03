# Support Runbook

This runbook answers the five questions a support engineer asks when triaging
the ODS control plane, with copy-paste queries against the **real** functions and
columns. All functions are read-only.

**How to connect.** Postgres is the container `avivaods-postgres-1` on TCP
`localhost:5440` (db `ods_cp`, user/pass `ods`/`ods`). **`docker exec` is
unavailable on this host** — connect over TCP with psycopg or any client. From
the repo, the simplest connection is:

```python
from control.db import connect          # reads ODS_CP_* env (defaults to 5440/ods_cp)
with connect() as conn:
    rows = conn.execute("SELECT * FROM cp.dashboard_workflows()").fetchall()
    for r in rows:
        print(r)
```

Every query below is plain SQL; run it through that `conn.execute(...)` (or any
TCP client pointed at `localhost:5440`).

Start broad: list all workflows and their health counts.

```sql
SELECT workflow_run_id, domain, business_date, run_count, stage_count,
       output_count, input_count, target_visibility_count
FROM cp.dashboard_workflows()
ORDER BY business_date, workflow_run_id;
```

---

## 1. Given a `workflow_run_id`, what do I query?

**Full picture (one JSON document):**

```sql
SELECT cp.dashboard_workflow_detail('<workflow_run_id>');
```

Returns a single `jsonb` with keys `workflow` (the rollup row from
`cp.dashboard_workflows()`), `runs`, and (per run) its stages, outputs, inputs,
plus the workflow's `target_visibility` rows. This is the one call that shows
everything that happened under that execution. It **raises** with a hint if the
`workflow_run_id` does not exist (query `cp.dashboard_workflows()` to list them).

**Health check — is anything wrong?**

```sql
SELECT check_name, severity, object_type, object_id, message
FROM cp.developer_diagnostics('<workflow_run_id>')
ORDER BY severity, check_name;
```

Empty result = healthy. Otherwise each row names a defect (`unfinished_run`,
`unfinished_stage`, `run_without_stages`, missing output link, bad target stamp,
`dlq_row_missing_trace_context`, `schema_validation_output_missing_schema_version`,
etc.). Pass a second arg (`'<schema>.<table>'`) to also validate row stamping on
that target table.

**Raw tables, if you want to drill manually:**

```sql
-- the runs in this execution
SELECT run_id, pipeline_type, domain, dataset, business_date,
       status, trigger_type, record_count_in, record_count_out,
       error, started_at, finished_at
FROM cp.run_log
WHERE workflow_run_id = '<workflow_run_id>'
ORDER BY started_at;

-- the stages (restart checkpoints) for those runs
SELECT s.run_id, s.stage, s.attempt, s.status,
       s.record_count_in, s.record_count_out, s.started_at, s.finished_at
FROM cp.run_stage_log s
JOIN cp.run_log r ON r.run_id = s.run_id
WHERE r.workflow_run_id = '<workflow_run_id>'
ORDER BY s.started_at, s.stage, s.attempt;
```

**How to read it:** a healthy run has `status='succeeded'` and a
non-NULL `finished_at`; every stage should be terminal (`succeeded`/`failed`)
with a `finished_at`. A run `status='failed'` carries the reason in `error`.

---

## 2. Given an `output_link_id`, what do I query?

This is the id a run produced (`cp.output_link.output_link_id`), and the value a
target row carries as `_ods_output_link_id`. Walk it back to raw:

```sql
SELECT hop, edge_type, output_link_id, consumer_run_id, pipeline_type,
       dataset, upstream_run_id, source_file_id, raw_s3_path, is_cycle
FROM cp.dashboard_output_trace('<output_link_id>')
ORDER BY hop;
```

Returns one row **per provenance hop**, ordered from the link's own edge (hop 1)
down to the raw leaf. The terminal hop has a non-NULL `source_file_id` /
`raw_s3_path` — the registered raw file this output ultimately derives from.
`is_cycle=true` flags a guarded cycle (should not occur in healthy data).

**Lower-level equivalents** (same link-to-link provenance walk over
`cp.v_provenance`):

```sql
-- the reusable SQL the platform ships, parameterised by the link id:
--   control/queries/trace_row.sql   (psycopg named param %(link_id)s)
-- python:
--   cur.execute(open('control/queries/trace_row.sql').read(),
--               {"link_id": output_link_id})
```

`cp.v_provenance` is the recursive view that walks only provenance edges
(link -> link via `upstream_lineage_link_id`, terminating at the raw file via
`source_file_id`). `cp.dashboard_output_trace` is the supported, hint-validated
wrapper over it — prefer it.

**How to read it:** follow `hop` upward to see each transform
(`raw_to_curated` -> `curated_to_canonical` -> `merge` -> ...). If the chain
does not reach a `source_file_id`, lineage is incomplete — run
`cp.developer_diagnostics` on the producing run's `workflow_run_id`.

---

## 3. Given a target row, what do I query?

You have a row in an `ods.*` target table (e.g. `ods.customer_transaction`,
`row_id = 1`) and want its full provenance:

```sql
SELECT hop, edge_type, output_link_id, dataset,
       source_file_id, raw_s3_path, is_cycle
FROM cp.dashboard_target_row_trace('ods', 'customer_transaction', 1)
ORDER BY hop;
```

This reads the row's `_ods_output_link_id`, then delegates to the same
provenance walk as Question 2 — so the result shape is identical (one row per
hop, raw file at the terminal hop). It **raises** with a hint if the table does
not exist, or if the row has no `_ods_output_link_id` (an unstamped row — a bug
the diagnostics would flag).

**Inspect the row's stamps directly first** if you only need the ids:

```sql
SELECT row_id, _ods_workflow_run_id, _ods_output_link_id, _ods_lineage_link_id
FROM ods.customer_transaction
WHERE row_id = 1;
```

**How to read it:** `_ods_output_link_id` is the bridge — it equals the
producing `output_link_id` (Question 2) and `_ods_lineage_link_id` (physical
name). `_ods_workflow_run_id` is the execution (Question 1). From those three you
can pivot to workflow detail, output trace, or the raw file.

---

## 4. Given an Airflow `dag_run_id`, what do I query?

Runs record the external orchestrator's identity in `cp.run_log.orchestrator_*`
columns (migration 020). Map a dag_run to its control-plane runs:

```sql
SELECT workflow_run_id, run_id, pipeline_type, dataset, status,
       orchestrator_type, orchestrator_dag_id, orchestrator_run_id,
       orchestrator_task_id, started_at, finished_at
FROM cp.dashboard_airflow_lookup('<dag_id>', '<dag_run_id>')
ORDER BY started_at;
```

`dag_id` is **optional** (pass `NULL` to match any dag); `dag_run_id` is the
discriminating key (= `cp.run_log.orchestrator_run_id`) and is **required** — the
function raises with a hint if it is NULL. Returns every run whose
`orchestrator_run_id` matches, with its `workflow_run_id`.

**Then pivot:** take a returned `workflow_run_id` into Question 1
(`cp.dashboard_workflow_detail` / `cp.developer_diagnostics`) for the full story.

**How to read it:** one Airflow dag_run typically maps to several runs (one per
task/pipeline). `status='failed'` rows point you at the failing task; the
`workflow_run_id` ties them together for restartability analysis.

---

## 5. Given a bad row / DLQ row, what do I query?

Bad rows are quarantined to `cp.dlq` with a first-class quarantine output_link
and the rejected payload preserved verbatim.

**List open / problem DLQ rows:**

```sql
SELECT dlq_id, run_id, stage, status, reason, record_count,
       quarantine_output_link_id, resolved_by_run_id,
       resolved_by_output_link_id, created_at
FROM cp.dlq
WHERE status IN ('open', 'under_review', 'corrected')   -- not yet resolved
ORDER BY created_at;
```

**Inspect one DLQ row's failure and its raw provenance:**

```sql
-- the preserved rejected payload + reason (history is never overwritten):
SELECT dlq_id, status, reason, failed_payload, source_ref, payload_ref
FROM cp.dlq
WHERE dlq_id = '<dlq_id>';

-- trace the quarantine output back to the raw file it came from:
SELECT hop, edge_type, dataset, source_file_id, raw_s3_path
FROM cp.dashboard_output_trace(
       (SELECT quarantine_output_link_id FROM cp.dlq WHERE dlq_id = '<dlq_id>'))
ORDER BY hop;
```

`status` is one of `open / under_review / corrected / replayed / resolved /
rejected` (enforced by a CHECK). `failed_payload` is the actual rejected
row(s); `reason` explains why (e.g. `non-nullable column 'policy_id' is null`).
`quarantine_output_link_id` is the lineage anchor, so a bad row traces to raw
exactly like a good one.

**Replay / resolve (how the fix is recorded):**

```text
control.dlq.replay(conn, original_run_id=..., pipeline_type=..., domain=...,
                   dataset=..., business_date=...)
        -> mints a NEW workflow_run_id, starts a run with trigger_type='replay'
           and replay_of_run_id = original_run_id.

control.dlq.resolve(conn, dlq_id=..., status='resolved',
                    resolved_by_run_id=..., resolved_by_output_link_id=...)
        -> flips status and records the corrected run/output; never touches
           failed_payload or reason (failure history is preserved).
```

After resolution the DLQ row shows `status='resolved'` with non-NULL
`resolved_by_run_id` / `resolved_by_output_link_id` pointing at the corrected
run and the corrected output. See `harness/policy_claims_dlq_workflow.py` for the
full quarantine -> replay -> resolve story end to end.

**How to read it:** a clean DLQ has no `open`/`under_review` rows. A row stuck
`open` with a null `quarantine_output_link_id` or null `reason` is itself a
defect — `cp.developer_diagnostics` flags it as `dlq_row_missing_trace_context`.

---

## Function reference (all read-only)

| Function | Signature | Returns |
|---|---|---|
| `cp.dashboard_workflows` | `()` | one rollup row per `workflow_run_id` |
| `cp.dashboard_workflow_detail` | `(workflow_run_id text)` | `jsonb` (workflow + runs + stages + io + visibility) |
| `cp.dashboard_output_trace` | `(output_link_id uuid)` | provenance hops to raw |
| `cp.dashboard_file_usage` | `(file_id uuid)` | every run/output/edge using a raw file |
| `cp.dashboard_target_row_trace` | `(target_schema text, target_table text, row_id bigint)` | provenance hops to raw (via the row's `_ods_output_link_id`) |
| `cp.dashboard_airflow_lookup` | `(dag_id text, dag_run_id text)` | runs matching an Airflow dag_run |
| `cp.developer_diagnostics` | `(workflow_run_id text, target_table text DEFAULT NULL)` | one row per detected defect |

See also `docs/reference/control-plane-write-contract.md` (write side) and
`docs/reference/refeed-replacement-policy.md` (refeed/replay semantics).
