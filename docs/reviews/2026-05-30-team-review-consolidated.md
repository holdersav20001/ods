# Lineage antagonistic team review — consolidated findings

**Date:** 2026-05-30 · **Focus:** can we always answer "where did this data come from?" under **restart** (Airflow
clear-task, same `workflow_run_id`) and **refeed** (new file, new `workflow_run_id`, replay)?
**Team:** restart-attacker (R1), refeed-attacker (R2), completeness-attacker (R3), integrity-attacker (R4),
each deriving from spec decision #5, distrusting the 148-test suite. Plus Codex pass-3 (P1–P4).
**Probes:** `tests/test_team_r1…r4.py`. Per-lens docs: `2026-05-30-team-r1…r4-*.md`.

## What HELD (proven sound by adversarial probes)
- Trace-to-raw holds for **every sanctioned-client row** — single, N-way merge, fan-out (pg+kafka), DLQ, replay. (R3)
- **Refeed/replay is correct:** correction history is navigable (origin + `replay`/`replay_of_run_id` + superseded
  original), and there is **zero cross-contamination** (old→old raw, new→new raw) across single/double/replay-of-
  replay/no-op/DLQ-drain. (R2) The link→link model (P5) genuinely isolates corrections.

## THEME A — RESTART-A-TASK breaks lineage (CRITICAL, the headline) — R1
The harness never modeled restart; its semantics were unverified. They are broken:
- **A1 (CRIT):** `cp.start_run` has no `ON CONFLICT`. An Airflow clear-task mints a **NEW `run_id`** under the same
  `workflow_run_id`. Decision-#5 idempotency is file-grain + link-grain, but **discovery is run-grain** and the
  5-part link key leads with `consumer_run_id` → on a real restart the new run_id makes **link dedup never engage**.
  The spec's "unchanged task → same link, counts stable" is **structurally unreachable on restart.**
- **A2 (CRIT):** **merge double-counts one physical file** — `succeeded_runs` returns both the original and the
  restarted ingest run → 2 slots × 5 rows = 10; both edges trace to the same `file_id`.
- **A3 (HIGH):** `latest_succeeded_run` silently re-binds canonicalize to the restart run — provenance depends on
  restart timing (and on a discovery tie-break that **flaked once** under equal `finished_at`, R3 note).
- **A-fix (R1's synthesis):** make run discovery **`workflow_run_id`-aware** — one live run per
  `(workflow_run_id, pipeline_type, slice)`, newest attempt supersedes. Closes A1–A3 together. Then MODEL restart in
  the harness and test it.

## THEME B — Exact output selection (Codex P1) — R1-C4, R2-C1
`cp.run_output_link`'s **path branch** (mig 010) has NO ambiguity guard (only the no-path branch got F1's
`count>1 ⇒ RAISE`). The hardened uniqueness is on `(run, edge_type, content_hash)` not path, so one run can hold two
links at the SAME path with different content_hash (in-place correction / changed content). The path-only selector
returns ONE (the STALE/original) with no raise → a consumer wires to pre-correction bytes. Latent under the default
composer (replay uses a distinct `-replay` path); live for any same-path correction.
→ Fix: mirror F1's ambiguity guard on the path branch AND key on content_hash (exact output identity, not path).

## THEME C — DB is NOT the lineage authority (Codex P2) — R3-C1/C2/C3/C4
Integrity lives in `cp.write_lineage_link`; the tables don't enforce it. App role `ods` has full DML (and is
owner/superuser). Proven bypasses via direct `INSERT`:
- **C1:** a `raw_to_curated` edge under a `curated_to_canonical` link (smuggling) — function RAISES, **table accepts**,
  edge appears in `v_provenance`.
- **C3:** a forged `raw_to_curated` edge naming an UNRELATED file under a canonical link → `trace_row.sql`
  **over-claims a false raw**.
- **C4:** a zero-edge `lineage_link` AND a committed `ods.orders` row under it → the real row **traces to NO raw**
  (FK satisfied, lineage blind).
→ Fix: a `BEFORE INSERT` trigger on `cp.lineage_edge` (edge_type == parent link_type except `replay`/`quarantine`/
  `orchestrates` annotations; anchoring) + a link-has-≥1-edge guard; and/or revoke direct DML, route writes through
  `SECURITY DEFINER cp.*` functions, stop running the app as owner/superuser.

## THEME D — Data-integrity constraints (Codex P3) — R4 (13 gaps)
The DB accepts nonsense that corrupts lineage/recon:
- **D1 (HIGH):** **negative `record_count` on all 9 count columns** (link, edge, run_log in/out, stage in/out, dlq,
  recon source/accounted) → one `-1` corrupts every SUM.
- **D2 (HIGH):** **recon can lie** — `reconciliation_log` accepts `status='ok'` with `discrepancy=500`, and
  `discrepancy` need not equal `source−accounted`. The integrity layer has no integrity at the DB level.
- **D3 (MED):** `target_ref` `version: null` and `''` pass `target_ref_contract` (only `? 'version'`).
- **D4 (MED):** free-text `status='banana'`, `trigger_type='banana'` (spec enumerates 4), `attempt<=0`,
  `input_slot<0`.
- **D5 (MED):** nothing enforces a link's `record_count == SUM(its edges.record_count)` (core merge invariant) —
  needs a deferred constraint trigger (cross-row).
→ Fix: CHECKs for #1/#3/#4, a derived/enforced discrepancy+status for #2, a deferred trigger for #5.

## THEME E — Per-output recon (Codex P4) — R1-C5
`cp.reconcile_sink` is run-scoped: a CORRECT 2-sink fan-out of 5 rows counts 10 → false `double_count` (disc −5).
→ Fix: add `cp.reconcile_sink_link(lineage_link_id, source_count)` for per-output checks; keep run-scoped as a sum.

## THEME F — Cross-hop end-to-end recon (A4-S5) — R3-C5
Wholly-failed upstream: 60 raw arrive, 30 reach sink → 30 lost rows produce **no breach, no DLQ, no trace**.
`reconcile_sink` is per-run so it sees self-consistent 30==30. The loss IS computable from lineage sums
(Σ `raw_to_curated.record_count` 60 vs downstream accounted 30) but no automated cross-hop check exists.
→ Fix: a workflow-scoped cross-hop reconciliation. (Larger feature.)

## THEME G — Changed-content boundary: stale link, no supersession/orphan recon — R1-C6
Mid-run clear-task with changed upstream content forms a NEW link (content_hash differs) and leaves the prior link
**discoverable and stale**; changed-content sink restart **doubles** target rows (new link, row-guard misses). There
is NO `superseded_at`/`is_valid` flag and NO orphan recon — the spec's "recon flags the orphan" is unimplemented.
→ Fix: supersession marker on `lineage_link` + an orphan/stale-link recon check (tied to Theme A).

## Triage
- **CRITICAL:** A (restart double-count/ambiguity).
- **HIGH:** B (stale output selection), C (DB bypassable), D1/D2 (negative counts, recon-can-lie).
- **MEDIUM:** D3/D4/D5, E (per-output recon), F (cross-hop), G (supersession/orphan recon).

## Meta
Fourth independent pass; still finding CRITICAL gaps — the restart case, which the spec *specifies* but the build
never modeled or tested. Confirms the lesson at scale: **unmodeled operational cases are unverified, and
self-authored suites don't surface them.** The DB-authority finding (C) is the structural fix: enforce invariants
where no caller can bypass, rather than trusting the function layer.
