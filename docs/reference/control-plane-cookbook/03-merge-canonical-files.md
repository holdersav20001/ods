# Building block 3 — Merge N canonical files into one canonical dataset

> Part of the **control-plane cookbook**: one recipe per *distinct chunk of work*. The
> single-file and multi-file workflows are just these blocks composed in different orders.
> This block appears once per merge point in a multi-input flow: it consumes the canonical
> output of N upstream ingest+canonicalize chains (blocks 1+2, one per slot) and folds them
> into a single canonical dataset.

---

## Use case (your words)

> "I have N canonical files (one per input slot — e.g. `core` + `enrichment`), each produced
> by its own ingest+canonicalize chain. I need to MERGE them into a single canonical dataset
> (join/combine by key), write the merged canonical output, and record provenance so the merged
> output traces back to ALL N inputs."

This is the **1:N lineage case**. Blocks 1 and 2 each produce *one* `lineage_link` with *one*
`lineage_edge` (single input → single output). A merge produces **one `lineage_link` with N
`lineage_edge` rows — one edge per input slot**. The `input_slot` column is what distinguishes
the edges (`core`, `enrichment`, …). That fan-in is the entire point of this block; everything
else (run_log, stages, recon) is the same shape as blocks 1/2.

> **Divergence from the current code — read this first.** The shipped `glue/jobs/ods_merge.py`
> merges slot staging tables straight into the **wide Postgres target** `ods.policies_enriched`
> with `edge_type='merge_to_postgres'`. This block describes the *design* you asked for:
> merge → **canonical file** (not Postgres). That needs an `edge_type` like **`merge_to_canonical`**,
> which is **not in the current taxonomy** (`lineage.py` lists `raw_to_curated`,
> `curated_to_kafka`, `curated_to_postgres`; the merge job adds `merge_to_postgres` /
> `slot_to_merged`). **FLAG:** `merge_to_canonical` would be a new edge_type to add to the
> taxonomy. The Postgres load of the merged canonical file is the **separate block 04**.

---

## You provide (inputs)

| Input | Where from | Example |
|-------|-----------|---------|
| `merge_run_id` | minted by the orchestrator (deterministic uuid5 so retries are idempotent) | uuid |
| `domain` | the merge domain | `insurance` |
| `dataset` | the **merged** dataset name | `policies_enriched` |
| `business_date` | the date all slots must share | `2026-06-01` |

Everything else — which datasets play which slot, where each slot's canonical output lives, the
join key, the join type — comes from **`dataset_config`**. You do not pass the slot list. The
merge job reads it: each slot dataset's config row carries `dataset_config.slot_name` (the role it
plays in *this* merge) and `merge_dataset` (which merge it feeds). `_load_slot_defs(conn, domain,
dataset)` returns the slot definitions ordered by `slot_name`.

> **`dataset_config.slot_name` vs `lineage_edge.input_slot`.** They are deliberately the same
> string at runtime but live in different places for different reasons. `slot_name` is *config* —
> it declares "dataset `policies_core` is the `core` slot of merge `policies_enriched`". It is set
> once, by whoever registers the dataset. `lineage_edge.input_slot` is *provenance* — it records,
> per merge run, "this edge is the `core` contribution to this merged output". The merge copies
> `slot_name` into `input_slot` when it writes the edges. Config says what *should* feed the merge;
> the edge records what *did*.

## You produce (outputs)

- A merged **canonical file** at the merged dataset's `s3_canonical_path/date=<business_date>/`
  (in the current Postgres-target code this is the row set in `ods.policies_enriched` instead).
- A complete control-plane trail (below) ending in `run_log.status='succeeded'`.
- **One `lineage_link`** for the merge write, with **N `lineage_edge` rows** (one per slot),
  written **before** the success status flips.
- Unmatched / conflicting rows in the DLQ (see Failure handling).

---

## Steps + control-table interactions

Order matters. Each step says **what you do** and **what control record it touches** (and the API).
This is the real sequence from `glue/jobs/ods_merge.py`, retargeted to a canonical-file write.

**0. Connect + idempotency guard.**
`pg = _get_pg_conn()`. Then `SELECT status FROM pipeline.run_log WHERE run_id=merge_run_id` — if it
already reached `succeeded`, short-circuit and return. Because the orchestrator mints
`merge_run_id` deterministically (uuid5 of `domain/dataset/business_date`), an Airflow retry of the
merge task reuses the same id and is a no-op. *This is what makes the merge retry-safe.*

**1. Merge-readiness barrier — wait until all N slots are ready.**
A merge must not start until **every** slot has a `succeeded` canonicalize run for this
`business_date`. There is no polling inside the job; the **barrier is the Airflow DAG**: the merge
task `depends_on` the canonicalize task of every slot within the same DAG run, so it cannot be
scheduled until all upstream slot tasks for that `business_date` have succeeded. Slots that arrive
in *different* DAG runs (late files) do not satisfy this barrier automatically — see the
late-arriving policy below.

**2. Open the merge run.**
`upsert_run_header(pg_dsn, run_id=merge_run_id, pipeline_type="merge", domain=…, dataset=…, business_date=…)`
→ **`run_log`** row, `status='running'`, `pipeline_type='merge'` (idempotent on `run_id`).

**3. Stage MERGE_READ — resolve each slot's upstream canonical run.**
`write_stage_row(... stage="merge_read", status="running")` → **`run_stage_log`**. Then for each
slot in `_load_slot_defs`:
- read the slot's canonical rows for this `business_date` (`_read_staging` in the PG code; the
  canonical-file design reads `s3_canonical_path/date=<business_date>/` for that slot);
- resolve the **upstream run that produced that canonical output** — the most recent `succeeded`
  `pipeline_type='canonicalize'` run for `(domain, slot.dataset, business_date)`, falling back to
  the `stage` run if no canonicalize run exists;
- record per-slot `{upstream_run_id, file_id, source_ref, count}` in `slot_meta`.

**business_date alignment is the invariant here:** every slot is read for the *same*
`business_date`, and the upstream-run lookup is filtered on that same `business_date`. The merge
**never** combines `core` from `2026-06-01` with `enrichment` from `2026-06-02`. Close the stage:
`write_stage_row(... stage="merge_read", status="succeeded", record_count_in=Σ slot counts)`.

**4. Stage MERGE_WRITE — join the slots, mint the link id, write the canonical output.**
`write_stage_row(... stage="merge_write", status="running")`. Join the slots by key
(`_merge_slots` joins on `policy_id`). **Mint `lineage_link_id = uuid4()` BEFORE writing rows** so
every output row can be stamped with the same `_ods_lineage_link_id` handle. Write the merged
canonical file (in the PG code, `_write_wide` deletes-then-inserts `ods.policies_enriched` for the
`business_date`, stamping `_ods_lineage_link_id`). Close the stage with `output_ref` = the merged
canonical URI and `record_count_out=written`.

**5. Stage LINEAGE — one link, N edges.**
Build a `contributions` list with **one entry per slot**, then call `write_link` once:

```python
contributions = [
    {"upstream_run_id": core_canon_run,   "source_file_id": core_file_id,
     "source_ref": core_canonical_uri,   "input_slot": "core",       "record_count": Nc},
    {"upstream_run_id": enrich_canon_run, "source_file_id": enrich_file_id,
     "source_ref": enrich_canonical_uri, "input_slot": "enrichment", "record_count": Ne},
]
ods_pipeline.lineage.write_link(
    pg,
    lineage_link_id=lineage_link_id,        # the id minted in step 4
    consumer_run_id=merge_run_id,
    edge_type="merge_to_canonical",          # NEW edge_type (current code uses merge_to_postgres)
    target_ref=merged_canonical_uri,
    record_count=Nmerged,
    contributions=contributions,
)
```

`write_link` runs all inserts in one transaction and writes **1 `lineage_link`** + **N
`lineage_edge`** rows that all share the `lineage_link_id`; the per-slot `input_slot` is what makes
each edge distinct. (The current job sets each contribution's per-edge `edge_type` to
`slot_to_merged` and the link's `edge_type` to `merge_to_postgres`; the canonical design uses
`merge_to_canonical`.) `write_link` requires ≥1 contribution — if a slot is genuinely empty, an
explicit `input_slot="empty"` placeholder edge is written so the link stays discoverable.

**6. Finalise — status last.**
`update_run_fields(pg_dsn, merge_run_id, status="succeeded", record_count_target=written)` →
**`run_log`**. Optionally write `reconciliation_log` (see Reconciliation) **before** this. *The
status flips only after the `lineage_link` + N edges exist* — same invariant as block 1.

On any exception, the `except` block sets `run_log.status='failed'` with `error_summary` and writes
a `stage_failed` row for the stage that was in flight.

---

## Control records written (summary)

| Table | Rows | When |
|-------|------|------|
| `run_log` | 1 | step 2 (`running`, `pipeline_type='merge'`) → step 6 (`succeeded`/`failed`) |
| `run_stage_log` | 3 | one per stage: MERGE_READ, MERGE_WRITE, LINEAGE |
| `lineage_link` | **1** | step 5, `edge_type='merge_to_canonical'` |
| `lineage_edge` | **N** | step 5, one per `input_slot` (`core`, `enrichment`, …) |
| `reconciliation_log` | 1 | step 6 |
| *DLQ* | 0..N | step 4, unmatched / conflicting rows |

The N-edge fan-in is the only structural difference from blocks 1/2, which write exactly one edge.

---

## Failure handling — as designed + flagged gaps

1. **Slot-not-ready (structural).** A required slot has no `succeeded` canonicalize run for this
   `business_date`. *All-or-nothing*: the readiness barrier (step 1) means the merge task is not
   even scheduled. If reached defensively (e.g. config drift), MERGE_READ writes `stage_failed`,
   `run_log.status='failed'`, and the merge is re-run once the slot lands.
2. **Row-level merge rejects (DLQ).** Unmatched or conflicting keys during the join — e.g. an inner
   join drops a key present in only one slot, or two slots disagree on a value that must be unique.
   These rows should go to the **DLQ** with enough context to triage and replay: `run_id` (the
   merge run), **which slot** the row came from (or "both, conflict"), and the **join key**. The
   merge run can still **succeed** (partial), with the reject count carried in reconciliation.

> **DLQ design note (decide before implementing).** Same gap as block 1: there is **no first-class
> DLQ table** in the control plane today — only `ops/dlq.py` whole-run replay. For merge rejects you
> need a durable, row-level DLQ. As-designed:
> - On reject, write a row to a `merge_dlq` table (or an S3 DLQ prefix + a `dlq` row keyed by the
>   merge `run_id`) carrying `run_id`, `business_date`, `input_slot`, the join `key`, the reject
>   reason (`unmatched` / `conflict`), and the raw row payload.
> - **Replay path:** a drain job re-reads the DLQ rows for a `run_id`, re-attempts the merge for
>   just those keys (after the missing slot lands or the conflict is resolved), and on success
>   marks the DLQ rows drained — so the DLQ is not a graveyard.
> This is the "DLQ + replay" building block (separate recipe). Flagged as forward-looking; not yet
> implemented.

---

## Reconciliation

Merge reconciliation is **join-semantics dependent** — state the invariant for the join you chose
and record it in `reconciliation_log` at step 6 (`check_type='merge'`, with per-slot
`source_count`s, `accounted_count`, and discrepancy):

- **Inner join:** `merged_count <= min(slot counts)` (only keys present in *every* slot survive).
- **Left join (core-driven):** `merged_count == core_count` (every core key is kept; missing
  enrichment is null-filled).
- **Full-outer join (the current `_merge_slots`):** `merged_count == |union of keys|`, and
  `merged_count + dlq_count` accounts for every distinct input key.

Whichever join the merged dataset declares, the recon invariant must hold within
`recon_tolerance_*`. A breach beyond tolerance should **fail the run**, not just log.

> **Late / partial-arrival policy — explicit decision point.** What happens if `enrichment` is
> missing or arrives after `core` for a `business_date`? Pick one and encode it in
> `dataset_config`; do not leave it implicit:
> - **Block (strict):** the merge does not run until all N slots are ready (the default — enforced
>   by the readiness barrier). Best when downstream needs all columns.
> - **Proceed-with-present (degrade):** merge the slots that are ready now (left/outer join,
>   null-fill the absent slot) and mark the run partial. Use only if downstream tolerates nulls.
> - **Re-merge (catch-up):** run the merge now with what is present, and when the late slot lands,
>   run a **new merge run** (new `merge_run_id`) for the same `business_date` that supersedes the
>   first. The new run writes a fresh `lineage_link` + N edges; the merged output is replaced
>   (the PG code already deletes-then-inserts by `business_date`, which makes re-merge idempotent
>   on the target). This is the recommended policy when late enrichment is common.

---

## Done when

- The merge run `run_log.status='succeeded'`, `pipeline_type='merge'`, with `record_count_target`
  set to the merged row count.
- **Exactly one `lineage_link` (`merge_to_canonical`) with N `lineage_edge` rows — one per
  `input_slot`** — exist **before** the success status flips.
- The merged canonical file exists at its `s3_canonical_path/date=<business_date>/` with
  `record_count_target` rows (current code: the `business_date` partition of
  `ods.policies_enriched`).
- All slots used the **same `business_date`**.
- Any unmatched / conflicting rows are in the DLQ and accounted for in `reconciliation_log`.

---

## Copy-paste skeleton (real API)

```python
import uuid
import ods_pipeline
from utils import upsert_run_header, update_run_fields, write_stage_row

pg, pg_dsn = _get_pg_conn(), _pg_dsn()

# 0. Idempotency guard (deterministic merge_run_id from the orchestrator)
with pg.cursor() as cur:
    cur.execute("SELECT status FROM pipeline.run_log WHERE run_id=%s", (merge_run_id,))
    row = cur.fetchone()
if row and row[0] == "succeeded":
    return 0

# 2. Open the merge run
upsert_run_header(pg_dsn, run_id=merge_run_id, pipeline_type="merge",
                  domain=domain, dataset=dataset, business_date=business_date)
stage = "merge_read"
try:
    # 1+3. Readiness is enforced by the Airflow DAG; here we resolve each slot's
    #      upstream canonical run for THIS business_date.
    slot_defs = _load_slot_defs(pg, domain, dataset)        # uses dataset_config.slot_name
    write_stage_row(pg_dsn, run_id=merge_run_id, stage="merge_read", status="running")
    slot_data, slot_meta = {}, {}
    for slot in slot_defs:
        sname = slot["slot_name"]
        rows = _read_canonical(slot, business_date)         # same business_date for every slot
        canon_run_id, file_id = _latest_canonicalize_run(pg, domain, slot["dataset"], business_date)
        slot_data[sname] = rows
        slot_meta[sname] = {"upstream_run_id": canon_run_id, "file_id": file_id,
                            "source_ref": _canonical_uri(slot, business_date), "count": len(rows)}
    write_stage_row(pg_dsn, run_id=merge_run_id, stage="merge_read", status="succeeded",
                    record_count_in=sum(m["count"] for m in slot_meta.values()))

    # 4. Join + write the merged canonical file. Mint the link id FIRST.
    stage = "merge_write"
    write_stage_row(pg_dsn, run_id=merge_run_id, stage="merge_write", status="running")
    merged = _merge_slots(slot_data.get("core", []), slot_data.get("enrichment", []))
    lineage_link_id = str(uuid.uuid4())
    merged_uri, written = _write_canonical(merged, lineage_link_id, business_date)  # stamps _ods_lineage_link_id
    write_stage_row(pg_dsn, run_id=merge_run_id, stage="merge_write", status="succeeded",
                    output_ref=merged_uri, record_count_out=written)

    # 5. ONE lineage_link + N lineage_edge (one per slot). THE 1:N case.
    stage = "lineage"
    contributions = [
        {"upstream_run_id": m["upstream_run_id"], "source_file_id": m["file_id"],
         "source_ref": m["source_ref"], "input_slot": sname, "record_count": m["count"],
         "edge_type": "merge_to_canonical"}
        for sname, m in slot_meta.items() if m["upstream_run_id"]
    ]
    ods_pipeline.lineage.write_link(
        pg, lineage_link_id=lineage_link_id, consumer_run_id=merge_run_id,
        edge_type="merge_to_canonical",          # NEW edge_type (current code: merge_to_postgres)
        target_ref=merged_uri, record_count=written, contributions=contributions)

    # 6. reconciliation_log (join-semantics invariant) THEN status.
    _write_recon(pg_dsn, merge_run_id, slot_meta, written)
    update_run_fields(pg_dsn, merge_run_id, status="succeeded", record_count_target=written)
    pg.close(); return 0
except Exception as exc:
    update_run_fields(pg_dsn, merge_run_id, status="failed", error_summary=str(exc)[:500])
    write_stage_row(pg_dsn, run_id=merge_run_id, stage=stage, status="failed", error=str(exc))
    pg.close(); return 1
```
