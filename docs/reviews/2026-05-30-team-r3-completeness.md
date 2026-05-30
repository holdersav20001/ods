# R3 — Completeness & DB-as-lineage-authority review

**Reviewer:** `completeness-attacker` (lineage-review team)
**Branch:** `feat/p9-lineage-team-review`  **DB:** `ods_cp` (001–012 applied)
**Probe:** `tests/test_team_r3.py` (namespace `p9r3_*` / `CONFIRMED_*`,`SOUND_*`)
**Result:** 9/9 pass; full suite 188 pass (deterministic on rerun).

## Verdict

The **trace-to-raw promise holds for every row produced through the sanctioned
client** (single ingest, N-way merge, fan-out postgres+kafka, DLQ, replay all
reach a raw `file_catalogue` row — `SOUND_*` probes confirm). **But the DB is
NOT a self-defending lineage authority.** The integrity that makes those traces
sound is enforced *only inside* `cp.write_lineage_link`; the lineage TABLES are
writable directly and several invariants the function guarantees are **not**
backed by the schema. Anything with table-level INSERT can forge, orphan, or
over-claim lineage, and the application role can do exactly that today.

## CONFIRMED holes

### C1 — Codex P2: direct INSERT bypasses the edge-type smuggling guard (HIGH)
`test_CONFIRMED_direct_insert_bypasses_smuggling_guard`

The guard that an edge's `edge_type` must match its parent link's `edge_type`
lives **only** in `cp.write_lineage_link` (012). The table CHECKs do not encode
it: `edge_must_anchor` and `raw_edge_requires_source_file` are satisfied by any
`raw_to_curated` edge that simply names a `source_file_id`. Proven live:

- `SELECT cp.write_lineage_link(... raw_to_curated edge under a
  curated_to_canonical link ...)` → **RaiseException** (guard fires). ✔
- `INSERT INTO cp.lineage_edge (edge_type='raw_to_curated', source_file_id=…)`
  under that same canonical link → **succeeds**, and the mismatched edge is
  **live in `cp.v_provenance`**. ✘

### C2 — No structural backstop: no trigger, no privilege restriction (HIGH)
`test_CONFIRMED_no_trigger_and_app_role_can_write_lineage_edge`

`cp.lineage_edge` has **zero non-internal triggers**. The application role
(`ods`) holds direct `INSERT/UPDATE/DELETE` on the lineage tables — and on this
instance it is **superuser and table owner**. So there is no layer beneath the
function that re-checks edge integrity. The function is advisory, not
authoritative.

### C3 — Forged edge makes trace-to-raw OVER-CLAIM a false raw (HIGH)
`test_CONFIRMED_forged_edge_makes_trace_overclaim_a_false_raw`

Building on C1: a forged `raw_to_curated` edge naming an **unrelated** raw file,
direct-inserted under a real canonical link, makes `trace_row.sql` return **both
the legit raw and the forged raw**. Post-P5 link→link adjacency prevents
*sibling* over-claim within sanctioned writes, but it cannot stop a directly
forged leaf — the canonical now "derives from" a file it never touched.

### C4 — Orphan link / orphan sink row dead-end (MED)
`test_CONFIRMED_orphan_link_zero_edges_dead_ends`,
`test_CONFIRMED_orphan_sink_row_traces_to_no_raw`

`cp.write_lineage_link` requires ≥1 edge, but the table permits a
**zero-edge `lineage_link`** by direct insert: it is absent from `v_provenance`
and `trace_row.sql` returns nothing. Worse, a **committed `ods.orders` row**
under such a 0-edge `canonical_to_sink` link satisfies the FK on
`_ods_lineage_link_id` yet **traces to no raw** — a real, queryable row that is
invisible to lineage.

### C5 — A4-S5 carried forward: wholly-failed upstream loses rows silently (MED)
`test_CONFIRMED_wholly_failed_upstream_loses_rows_with_no_breach`

60 raw rows arrive across two ingest runs; one upstream fails downstream so only
30 reach canonical/sink. `cp.reconcile_sink` is **per-run** and DB-derived: the
sink run only knows its own 30 upstream rows, so recon reports `ok / disc=0`.
The 30 lost rows produce **no breach, no DLQ row, and no trace**. The gap *is*
computable from lineage (`Σ raw_to_curated.record_count = 60` vs
`Σ curated_to_canonical.record_count = 30`), and the failed upstream's curated
link is **consumed by nothing** — but **no automated cross-hop check computes
this**. Detection from lineage is *possible*; it is *not performed*.

## SOUND (regression guards — the promise holds for sanctioned writes)

- `SOUND_single_ingest_traces_to_raw`
- `SOUND_fanout_each_sink_traces_to_raw` (postgres + kafka each reach raw)
- `SOUND_merge_traces_every_parent_to_raw` (N-way merge reaches all N raw files)

## Recommendations (priority order)

1. **Make the DB the authority (closes C1–C4).** Either:
   - **Trigger** `BEFORE INSERT/UPDATE ON cp.lineage_edge` that RAISES when
     `NEW.edge_type` ≠ the parent link's `edge_type`, *except* the recognised
     annotations (`replay`, and link-level `quarantine`/`orchestrates`). The
     trigger must catch: (a) edge_type smuggling (C1/C3), and pairs naturally
     with a constraint-trigger / deferred check that (b) every non-annotation
     `lineage_link` has ≥1 anchoring edge (C4). A forged-leaf check should also
     reject a `raw_to_curated` edge under a non-`raw_to_curated` link.
   - **OR** revoke direct DML on `cp.lineage_edge`/`cp.lineage_link` from the
     app role and route all writes through `SECURITY DEFINER` `cp.*` functions
     (and stop running the app as superuser/owner). This is the cleaner
     "functions are the only mutators" posture.
2. **Cross-hop reconciliation (closes C5).** Add a workflow-scoped check that
   compares `Σ raw_to_curated.record_count` against downstream
   canonical/sink accounted counts per `workflow_run_id`; breach when an
   arrived upstream produced no consuming downstream link. This is the only
   layer that can catch a wholly-failed upstream end-to-end.

## Scope / hygiene

Audit-only. Added `tests/test_team_r3.py` + this doc. All probes use the
rollback `conn` fixture (nothing committed); the schema was never dropped or
altered. The single intermittent `test_replay_traces_to_raw` failure observed
once under full-suite ordering is a **pre-existing discovery tie-break flake**
(newest-succeeded-run ordering by `finished_at` with equal timestamps), not
caused by these probes — it passes in isolation and on every rerun, and the
full suite is 188-green deterministically.
