# Full platform audit — consolidated findings (A1–A5)

**Date:** 2026-06-03 · 5 adversarial lenses (A1 SQL/migrations, A2 lineage, A3 tests, A4 workflows, A5 diagnostics/dashboard/docs). Per-lens detail: `2026-06-03-audit-a1…a5-*.md`.

## Headline pattern
The "incomplete refeed-aggregate input edge" bug was fixed **only in the DLQ workflow** — it remains in `customer_transaction_workflow.py` AND `policy_claims_workflow.py`. Fixing the *class* (every refeed aggregate names all contributing detail outputs) is the priority.

## Fix list (triaged)
- **F1 (HIGH, A2-H1+A4-02):** port the `_aggregate_from_detail` multi-input fix (DLQ workflow) to `customer_transaction_workflow.py` + `policy_claims_workflow.py` refeed-aggregate paths — name ALL contributing detail outputs with per-edge record_counts.
- **F2 (HIGH, A2-H2+A5-1):** `cp.quarantine` must stamp `source_file_id` on the quarantine edge (raw id currently only in `source_ref` JSON) so the quarantine output traces to raw (spec "quarantine traces to raw"; runbook Q5).
- **F3 (HIGH, A4-01):** `customer_transaction_workflow` must call `visibility.activate` (business_key, changed-only) — currently 0 `target_visibility` rows for `sales`; write-contract step 9 skipped.
- **F4 (P1, A1-F1):** `cp.get_schema_contract` "latest" sort `regexp_replace(...,'\D','')::bigint` concatenates digits (`v1.10`→110). Replace with a correct version order (split numeric parts, or `effective_from DESC NULLS LAST, created_at DESC`).
- **F5 (P2, A1-F3):** `developer_diagnostics.active_visibility_conflict` GROUP BY omits `sink_type,target_name` (the `uq_target_visibility_active` columns) → false positives. Match the index.
- **F6 (P2, A1-F4+A4-03):** `reconcile_workflow` — order terminal run by `seq` (post-028), filter `dlq.record_count` by unresolved status (no double-count after replay), include `detail_to_aggregate` in sink_out; AND call it from the workflows so cross-hop recon actually runs.
- **F7 (MED, A4-06):** `control.schema.validate_rows` — empty `required_columns` accepts everything (incl `{}`); add a non-empty-contract guard + (optional) reject unknown/extra and simple type mismatches.
- **F8 (P2, A1-F2):** dual `_ods_lineage_link_id`/`_ods_output_link_id` (shelved-rename residue) — reconcilers read old, diagnostics read new. DECISION: rename shelved → add a CHECK guaranteeing the two columns stay equal (or one null) + document the shelving; do NOT un-shelve.
- **F9 (docs, A5-2+A5-3):** README — `control.stages.start/finish` don't exist (use `stage_scope`/SDK); migrations say "001–027" but ship 029.
- **F10 (LOW, A3):** strengthen the 8 weak tests — esp. `test_quarantine_recon_input_equals_good_plus_dlq` (pure `3+1==4` tautology, recon never invoked); `..._tiebreak_is_nondeterministic` (asserts a Postgres truism); `succeeded_runs_ordered_by_clock` (asserts membership, not order).

## Not fixing (noise / by-design)
A5-4 dormant-but-structurally-sound `schema_validation_output_missing_schema_version` (no demo candidate); A5-5 superseded 027 body (live fn from 029 is clean); A5-6 cosmetic duplicate hops in raw `v_provenance` (hidden by DISTINCT in the trace fn); A1-P3 silent no-ops on bad id / sink dedup / register_file path discard (low-value, document if needed).

## What the audit verified CORRECT
DLQ replay provenance + lifecycle (the prior fix genuinely works — both detail inputs named, traces to raw); changed-only refeed visibility (changed keys superseded, unchanged stay active, no double-active); domain-scoped resets; discovery determinism (seq tiebreak); DAG import-safety + one-wfid-per-run; 35/40 test files genuinely adversarial; no diagnostic false-positives on healthy demos; no test hides a live bug; `node --check` clean; all 3 snapshots load.
