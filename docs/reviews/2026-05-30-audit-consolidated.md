# Adversarial audit — consolidated findings (A1–A4)

**Date:** 2026-05-30 · **Trigger:** the fan-out dedup bug proved gates can pass for the wrong reason; this
is the independent re-audit of the whole build by four auditors (A1 identity/keys, A2 wrong-reason tests,
A3 spec contradictions, A4 provenance stress), each deriving expectations from the spec, not the tests.
Source docs: `2026-05-30-audit-a1.md … a4.md`. Probe tests: `tests/test_audit_a1…a4.py`.

## The headline (3 of 4 auditors converged on it independently)

**F1 — `cp.run_output_link` re-introduces the exact C1 over-claim that P5 existed to kill — relocated to the
discovery layer. (HIGH)** *(A1-1, A3-C1, A4-S1.)* It returns ONE link per `(run_id, edge_type)` via
`ORDER BY created_at DESC, lineage_link_id DESC LIMIT 1`. `created_at` defaults to `now()` (transaction-stable),
so two outputs of one run tie → the winner is a **random `gen_random_uuid()`**. The moment a run legitimately
emits ≥2 outputs of one edge_type (which the spec's C1 / decision-#6 / fan-out explicitly allow), a downstream
consumer's `upstream_lineage_link_id` — and its whole trace-to-raw — is non-deterministic: over-claims the
UUID-picked sibling, under-reaches the intended output. **My P5 fix moved the C1 bug from the write key to the
discovery selector.** Latent today only because no current fake consumes a multi-output edge_type — i.e. it
"passes for the wrong reason," the same failure mode as the original bug.
→ Fix: discovery must key on OUTPUT IDENTITY (target path/content_hash), not `(run, edge_type)`. Minimum-safe:
`run_output_link` must RAISE on >1 match (fail loud, not random-pick); full fix adds a target/output disambiguator.

## CONFIRMED defects

**F2 — `fake_merge` binds per-slot counts to the WRONG upstream. (HIGH — real bug, was green)** *(A2.)*
`composers.py:78` passes `slot_counts` in file order; `runs.succeeded_runs()` returns newest-first
(`runs.py:90`). Zipped positionally → slot 0 names the 30-row upstream but carries count 10; slot 2 names the
10-row upstream but carries 30. Total (60) and recon (60==60) still balance, so `test_lineage_merge.py`
(multiset/sum asserts only) stayed green. **This is the exact concern waved off at P3c build time as "multiset
assertions handle it" — A2 proved it's a real per-slot misattribution.**
→ Fix: bind each slot's count to its discovered upstream explicitly (zip by resolved upstream, or pass
`{upstream_run_id: count}`), and add a per-slot binding assertion.

**F3 — `trace_row.sql` has its own UNGUARDED recursion → hangs on a cyclic edge. (MED-HIGH)** *(A4-S7a.)*
`v_provenance` got the CYCLE clause (mig 006), but the gate-4 debug deliverable `control/queries/trace_row.sql`
runs a separate `WITH RECURSIVE` with no guard → `QueryCanceled`/hang on a forged cycle. The 009 CHECK forbids
null upstream links but not a cycle.
→ Fix: add the same CYCLE guard to `trace_row.sql`.

**F4 — `COALESCE('')` in the dedup key collapses two hashless+pathless outputs; 2nd edge set dropped. (MED)**
*(A1-2.)* Two distinct outputs that both lack path+content_hash map to the same key → 2nd silently `DO NOTHING`.
→ Fix: require non-empty path or content_hash for any non-quarantine link (CHECK), or reject empty-keyed links.

**F5 — Idempotent-replay branch ignores a CHANGED edge set and discards it silently. (MED)** *(A1-4.)*
When `write_lineage_link` hits the conflict, it returns the existing link WITHOUT checking the supplied edges
match what's stored — a different edge set is silently lost (no error).
→ Fix: on conflict, verify the incoming edge set matches the stored one; raise on mismatch (or document as a
hard idempotency precondition and test it).

**F6 — `register_file` dedups on `(file_md5, business_date)` only — ignores domain/dataset. (MED)** *(A1-3, A3-C3.)*
Same content (md5) for two different datasets on one date → 2nd registration silently returns the FIRST file_id
(wrong dataset). Plausible for shared/identical reference files.
→ **DECISION NEEDED:** add `domain, dataset` to the dedup key (treat as per-dataset files) vs keep md5-global
(content-addressed, dataset-agnostic). Spec never states the file grain.

**F7 — Recon cannot detect a true imbalance: `accounted_count` is caller-supplied, never derived from the
graph/target rows. (MED — system-level)** *(A2, A4-S5.)* Every recon test feeds self-consistent fakes
(`source == good+dlq` by construction), so the layer is unfalsifiable; a wholly-failed upstream's lost rows are
reconciled by nothing.
→ **DECISION NEEDED (scope):** derive `accounted_count` from actual `ods.<dataset>` rows / lineage edge sums
(makes recon real but is a feature), vs keep arithmetic recon + document the boundary.

## SUSPECTED / lower

- **F8 (SUSPECTED):** `cp.lineage_edge` has NO uniqueness constraint — all integrity rests on one function with
  no DB backstop; a direct/buggy writer could dup edges. *(A1.)* → consider a natural-key unique constraint.
- **F9 (doc, MED):** spec decision-#5 still quotes the OLD 3-part dedup key — the original bug verbatim, one
  paragraph from the correct 5-part key. *(A3-C2.)* → fix the spec text.
- **F10 (MED, NOTE):** discovery `finished_at`-tie fallback to random `run_id` can re-canonicalize a stale ingest
  on a clock tie — same root cause as F1. *(A4-S2b.)*
- **Stylistic/doc-drift:** v_provenance hidden recursion key; write-event vs output term mixing; `orchestrates`
  conflated as trigger_type vs edge_type; "trace every row to raw" vs write-event-grain caveat. *(A3 S-1..3, C-4.)*

## What HELD (regression-guarded by the audit probes)
Refeed no-contamination (A4-S2), double-replay idempotency (S3), concurrent writers (S4), DLQ forced-imbalance
→ breach/double_count caught (S6), 5-hop deep chain fully traces (S7b), merge correctly excludes a failed slot
(S5), the original P5 fan-out fix is genuinely sound (A3), all 12 function signatures match the appendix
(P5 drift closed, A3).

## Meta-lesson
The audit's top finding is that **my P5 fix re-introduced the very class it fixed** (write-side → discovery-side).
Confirms the retrospective: a fix authored and tested by one mind inherits the same blind spot. The independent
multi-auditor pass is what caught it — and three auditors converging on F1 independently is the strongest signal
in this report.
