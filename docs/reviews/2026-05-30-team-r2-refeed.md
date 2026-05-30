# R2 — Refeed/Replay Lineage Review (refeed-attacker)

**Date:** 2026-05-30
**Branch:** `feat/p9-lineage-team-review`  **DB:** `ods_cp` (schema 001–012)
**Probe suite:** `tests/test_team_r2.py` (9 probes, all green; namespace `p9r2_*`,
committing connection, slice-scoped cleanup in `finally`, zero leaked rows)
**Posture:** assume you cannot answer *"where did this row come from, and what did
it correct?"* until proven. Audit only — no fixes, no migration/harness edits.

## Decision-#5 rule under test

A corrected/late/reprocessed file is a **NEW execution**: the composer mints a
**NEW** `workflow_run_id`, `trigger_type='replay'`, `replay_of_run_id` → the
original run. The **full provenance chain is re-written** PLUS a `replay`
annotation edge, so the refed row still traces to raw (X5) and a
new execution SUPERSEDES the prior output. Boundary: **new bytes ⇒ refeed under a
new `workflow_run_id`.**

## Headline

- **The correction story is navigable** — a replayed row answers all three
  questions (origin / that-it-was-a-correction / what-it-superseded), and
  cross-contamination is fully prevented. Strong on the core promise.
- **ONE CONFIRMED defect (HIGH):** the **refeed dual of the Codex P1 restart
  bug**. `cp.run_output_link`'s **path branch has no ambiguity guard**, so when a
  correction writes a second link at the **same path** with a **new
  content_hash**, a downstream consumer that names its upstream **by path** can
  wire to **stale pre-correction content**.

---

## CONFIRMED

### C1 (HIGH) — `cp.run_output_link` path selector ignores content_hash → stale wiring after an in-place correction
**Probe:** `test_CONFIRMED_5b_run_output_link_path_selector_ignores_content_hash`
(Codex P1 cross-check / probe #5).

`cp.run_output_link(run, edge_type, p_target_path)` (mig 010) selects on
`(consumer_run_id, edge_type, target_ref->>'path')` and **returns the first
match** with **no count/ambiguity check on the path branch** — only the *no-path*
branch got the F1 `v_n > 1 ⇒ RAISE` guard:

```sql
IF p_target_path IS NOT NULL THEN
  SELECT lineage_link_id INTO v_link FROM cp.lineage_link
   WHERE consumer_run_id=p_run_id AND edge_type=p_edge_type
     AND target_ref->>'path'=p_target_path;          -- no LIMIT, no count guard
  ...RETURN v_link; END IF;
```

The hardened uniqueness index is on
`(consumer_run_id, edge_type, (target_ref->>'content_hash'))` (mig 001) — **not
path**. So ONE run may legitimately hold **two `curated_to_canonical` links at
the same path with different content_hashes** (an original write + an in-place
correction). The path-only selector cannot distinguish them.

**Evidence (probe output):** constructed two links at one path via the sanctioned
`lineage.write_link` (`HASH-ORIGINAL`, `HASH-CORRECTED`); `n_at_path=2`;
`selector raised=False`; the selector returned the link carrying
**`hash=HASH-ORIGINAL`** — the **stale** one. A consumer that disambiguates its
upstream by `target_path` (the harness's own fan-out selector contract) wires to
pre-correction content and **does not raise**.

**Why this is the refeed dual of Codex P1:** path is treated as a content
identity on the *input side* (the upstream disambiguator), but it is not one. The
restart bug was the same shape on restart; here it is reached when a correction
reuses a path. The default `replay_single_file` composer **avoids** it by writing
the corrected canonical to a **distinct** path (`{bd}-replay.parquet`) — see SOUND
S5 — so this is latent under the default wiring but live for any caller that
corrects in place at a stable path.

**Fix (audit recommendation, not applied):** mirror the F1 guard on the path
branch of `cp.run_output_link` — `count(*)` the matches and `RAISE 'ambiguous'`
when `> 1`; better, key the disambiguator on **content_hash** (the actual output
identity), since path is explicitly *not* unique in the schema. Until then,
in-place corrections at a stable path are not safely addressable by path.

---

## SOUND (attacks attempted; the system held — kept as regression guards)

### S1 — A replayed row answers origin / correction / superseded (probe #1)
`test_SOUND_replayed_row_answers_origin_correction_and_superseded`.
A replayed sink row (a) traces to the **corrected** raw (and *not* the old raw)
via its own re-written chain; (b) its canon run carries
`trigger_type='replay'` + `replay_of_run_id=`original, and exactly one `replay`
provenance edge; (c) that `replay` edge names the original run, whose own output
still independently traces to the old raw. All three questions answerable.

### S1b — Correction history reachable from the row (probe #1, headline)
`test_correction_history_reachability_from_a_replayed_row`.
`trace_row.sql` does **not** recurse `replay` edges (they carry no
`upstream_lineage_link_id`), so the trace-to-raw chain alone does not surface the
superseded run *on that walk*. **But** the correction history **is** reachable via
the `replay` edge (`lineage_edge.edge_type='replay'` → original run) and via
`run_log.replay_of_run_id`. Verdict: reachable, by an intentional side channel —
**not a gap**, but noted: a single "row → full correction history" query requires
joining `replay_of_run_id` / the `replay` edge, it is not one recursive walk.

### S2 — No cross-contamination (probe #2, isolation core promise)
`test_SOUND_no_cross_contamination_old_traces_old_new_traces_new`.
After a refeed with a different corrected md5, the OLD sink row traces to the OLD
raw **only** and the NEW to the NEW **only**. The 009 link→link walk
(`upstream_lineage_link_id`) gives each chain its own exact-output adjacency; no
discovery pulls a sibling chain. Isolation held.

### S3 — Same-md5 (no-op) refeed is coherent, not a duplicate (probe #3)
`test_same_md5_replay_builds_coherent_chain_to_the_same_raw`.
`cp.register_file` dedups on `(file_md5, business_date, domain, dataset)` (mig
010 F6). A replay of an unchanged file reuses the **same `file_id`**; the new
chain traces to exactly **one** raw (the shared file); `file_catalogue` holds one
row. No ambiguous multi-raw chain.

### S4 — Replay-of-replay forms a navigable correction chain (probe #4)
`test_replay_of_replay_forms_navigable_correction_chain` +
`test_double_replay_idempotent_counts_stable`.
O ← R1 ← R2 via `replay_of_run_id` is fully walkable and correctly ordered; R2's
row traces to **its own** corrected raw only (not R1's, not O's). Two identical
replays keep counts stable (record_count rows per sink link, original untouched)
and dedup to one file row.

### S5 — Default refeed sink discovers the replay canonical, not the original (probe #5a)
`test_SOUND_5a_default_refeed_sink_discovers_replay_canonical_not_original`.
`latest_succeeded_run` (ordered `finished_at DESC`) returns the replay canon run;
the replay sink's `canonical_to_sink` edge names the **replay** canon output. The
original and replay canonical links sit at **distinct paths**, so the path-only
selector cannot conflate them — this is *why* C1 is latent rather than live under
the default composer.

### S6 — DLQ drain refeed traces to raw AND keeps its quarantine origin (probe #6)
`test_dlq_drain_refeed_traces_to_raw_and_keeps_quarantine_origin`.
A quarantined batch (`fake_fail` → `quarantine` link/edge), when replayed
(`dlq_drain`/replay flavour), produces a drained row that traces to raw via its
own chain (not orphaned); the `replay` edge points back at the quarantined run,
which **still** carries its `quarantine` edge. DLQ origin remains discoverable.

---

## Cross-checks for `team-lead`

- **Can you find the correction history?** YES — origin (corrected raw via the
  re-written chain), the fact-of-correction (`trigger_type`/`replay_of_run_id` +
  the `replay` edge), and what-it-superseded (the `replay` edge → original run,
  whose output is intact) are all reachable. Caveat: it takes the `replay`
  edge / `replay_of_run_id` side channel, not the single trace-to-raw walk.
- **Cross-contamination:** none — old↔new chains are fully isolated (link→link
  adjacency, 009).
- **Codex P1 cross-check:** **CONFIRMED** for in-place same-path corrections —
  `run_output_link` path branch returns stale content with no ambiguity guard.
  **Mitigated by default** because `replay_single_file` writes the corrected
  canonical to a distinct `-replay` path.

## Severity roll-up

| ID | Severity | Status | One-liner |
|----|----------|--------|-----------|
| C1 | HIGH | CONFIRMED | `run_output_link` path branch lacks the F1 ambiguity guard → stale wiring on same-path corrections (latent under default composer) |
| S1b | INFO | SOUND (noted) | Correction history needs the `replay` edge / `replay_of_run_id` side channel, not one trace-to-raw walk |
| S1–S6 | — | SOUND | origin/correction/superseded answerable; isolation, no-op dedup, replay-of-replay, default discovery, DLQ drain all hold |
