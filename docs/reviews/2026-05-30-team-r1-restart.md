# Team R1 — Restart-attacker findings (Airflow clear-task / retry)

**Reviewer:** `restart-attacker` (lineage-review team)
**Date:** 2026-05-30
**Branch:** `feat/p9-lineage-team-review`
**Spec under test:** `docs/specs/2026-05-29-control-plane-design-v2.md`, **decision #5** (restart-a-task) + **boundary rule** + decision #6 (one link per output).
**Probes:** `tests/test_team_r1.py` (8 probes, all green; modelled with the REAL client `control/*`, no harness fakes touched, schema never dropped, all writes `commit=False`/rollback-isolated).

## Headline

**Restart-a-task double-counts and ambiguates lineage. CONFIRMED.** The control plane has **no notion of run identity scoped to `workflow_run_id`**. `cp.start_run` mints a new `run_id` on *every* call (no `ON CONFLICT`), so an Airflow clear-task that re-runs a task produces a **second succeeded run** for the slice under the same `workflow_run_id`. Decision #5's idempotency guarantees are scoped to **files** (`register_file`) and **links** (`write_lineage_link`) — neither covers the **run grain** that the discovery primitives (`latest_succeeded_run`, `succeeded_runs`) operate on. Consequences:

- the merge hop **double-counts one physical file** (two discovered slots);
- the canonicalize hop **silently re-binds** to whichever run finished last;
- worse than expected: **link dedup never even engages on a real restart**, because the link `ON CONFLICT` key is `consumer_run_id`-scoped and the restart's `run_id` is new — so the spec's "an unchanged task produces the same link (counts stable)" is **false** for any real clear-task.

**Codex P1 (run_output_link path selector ignores content_hash): CONFIRMED.**
**Codex P4 (reconcile_sink run-scoped, false double_count on fan-out): CONFIRMED.**

**CONFIRMED defects: 6** (5 CONFIRMED probes + the strengthened link-dedup finding inside C-1). 1 SOUND result (same-bytes sink restart is genuinely idempotent).

---

## C-1 (CRITICAL) — Restart mints a duplicate run; link idempotency never engages

**Rule violated:** decision #5 — "re-runs under the SAME `workflow_run_id`… idempotent… re-running an unchanged task produces the **same link** (counts stable)."

**CONFIRMED** by `test_CONFIRMED_restart_ingest_duplicates_succeeded_run_for_one_slice`.

`cp.start_run` (002) has **no `ON CONFLICT`** — every call mints a fresh `run_id`. An Airflow clear-task on the ingest task therefore yields TWO `run_log` rows with the **same `workflow_run_id`** and the **same `file_id`**, both `status='succeeded'`. `register_file` dedups (file-grain key), but:

- **`write_lineage_link` does NOT dedup across the restart.** Its 5-part `ON CONFLICT` key (010) leads with `consumer_run_id`. The restart run has a *new* `consumer_run_id`, so the insert never conflicts → a **brand-new link** is minted. The spec's "same link, counts stable" only holds for an in-place re-call **under one run_id**, which a real Airflow clear-task never is. This is a stronger defect than mere run duplication: the documented idempotency mechanism is structurally unreachable on the very operation it is specified for.

**Fix:** give the run grain a restart-aware identity. Either (a) make `start_run` upsert on `(workflow_run_id, pipeline_type, domain, dataset, business_date, attempt)` returning the existing `run_id` for a clear-task, or (b) make discovery (`latest_succeeded_run`/`succeeded_runs`) **collapse to one run per `(workflow_run_id, pipeline_type, slice)`** (newest attempt wins), so a restart supersedes rather than accretes. Option (b) is the smaller change and aligns discovery with decision #5's "same workflow_run_id ⇒ restart."

---

## C-2 (CRITICAL) — Restart makes the merge hop double-count one physical file

**Rule violated:** decision #5 (idempotent restart) + decision #6 (counts must reflect real outputs).

**CONFIRMED** by `test_CONFIRMED_restart_makes_merge_double_count_one_physical_file`.

The merge hop discovers its upstreams via `cp.succeeded_runs` (exactly what `harness.fakes.fake_merge` does). After one ingest restart, `succeeded_runs` returns **both** ingestion runs for the slice. Merge folds each in as a slot, so a single 5-row physical file contributes **2 slots × 5 = 10** to the merged `canonical` link's `record_count`. Both merge edges trace, via their upstream `raw_to_curated` links, back to the **same one `file_id`** → provenance shows two parallel raw-to-curated inputs for one file. **Lineage is duplicated and the count is inflated 2×.**

**Fix:** as C-1(b) — discovery must return at most one run per `(workflow_run_id, slice)`; the restart attempt supersedes the prior one. Until then, merge over a restarted slice is unsound.

---

## C-3 (HIGH) — Restart ambiguates `latest_succeeded_run` binding (non-deterministic provenance)

**Rule violated:** decision #5 (same `workflow_run_id` ⇒ deterministic restart) + principle #4 (discovery, not trust).

**CONFIRMED** by `test_CONFIRMED_restart_ambiguates_latest_succeeded_run_binding`.

`cp.latest_succeeded_run` orders `finished_at DESC, run_id DESC`. A restart produces a newer run, so the canonicalize hop **silently binds to the restart run, not the original** — even when both produced identical bytes. The function cannot express "the run for THIS `workflow_run_id`," so which run a downstream binds to depends on **restart timing**, the exact ambiguity decision #5's same-id rule was meant to eliminate.

**Fix:** add a `workflow_run_id`-scoped discovery variant (`latest_succeeded_run_in_run(workflow_run_id, …)`) for intra-run recovery, and have the harness hops use it; keep the slice-wide variant for cross-run (replay) cases.

---

## C-4 (HIGH) — `run_output_link(target_path)` is not an exact selector  *(Codex P1 — CONFIRMED)*

**Rule violated:** decision #5 boundary rule + decision #6 (output identity = path **and** content_hash).

**CONFIRMED** by `test_CONFIRMED_run_output_link_path_not_exact_silent_stale_pick`. (Teammate R2's `test_team_r2.py::test_5b_run_output_link_path_selector_ignores_content_hash` independently reproduces the same hole.)

`cp.run_output_link` (010) with a `p_target_path` matches `WHERE … target_ref->>'path' = p_target_path` with **no `content_hash` predicate**. The mid-run-changed-content case (decision #5 boundary) leaves **two links at the same path** (old + new `content_hash`) — both legal under the 5-part unique key. A plpgsql `SELECT … INTO` over two matching rows returns **one arbitrarily and does NOT error**. So while path-*less* ambiguity correctly `RAISE`s "ambiguous", path-*ful* ambiguity-on-content **silently** resolves. **A consumer wiring its upstream by path can be handed the stale bytes** with no signal.

**Fix:** make `target_path` an exact selector — accept an optional `p_content_hash`, and when a path resolves to >1 link, `RAISE` "ambiguous — pass content_hash" (mirroring the path-less branch) rather than `SELECT INTO` a silent winner.

---

## C-5 (HIGH) — `reconcile_sink` is run-scoped, not per-output → false `double_count` on fan-out  *(Codex P4 — CONFIRMED)*

**Rule violated:** decision #6 — "one link per output… fan-out of identical bytes mints two links"; recon must not breach a *correct* fan-out.

**CONFIRMED** by `test_CONFIRMED_reconcile_sink_run_scoped_false_double_count_on_fanout`.

`cp.reconcile_sink` (011) derives `accounted` by counting **all** `ods.<dataset>` rows joined to **any** `canonical_to_sink` link of the run. Decision #6 *mandates* fan-out: one sink run writes K links (e.g. postgres + kafka), each stamping its own rows. A 5-row upstream fanned to 2 sinks stamps **10 rows**, so `reconcile_sink(run, 5)` computes `accounted=10, discrepancy=-5, status='double_count'` — **a false breach on a correct fan-out.**

**Fix (recommended):** add a **per-output** check `cp.reconcile_sink_link(p_lineage_link_id, p_source_count)` that counts only rows stamped with *that* link, and call it once per `canonical_to_sink` link. The run-scoped check should be retired or reserved for single-output runs.

---

## C-6 (HIGH) — Changed-content restart doubles target rows; old link orphaned, unflagged

**Rule violated:** decision #5 boundary rule — "clear-task **must clear downstream too**… **recon flags the orphan** if it doesn't."

**CONFIRMED** by `test_CONFIRMED_restart_sink_changed_content_doubles_rows` and `test_CONFIRMED_changed_content_leaves_discoverable_stale_link_unflagged`.

`write_link_then_rows` (008) has a row-idempotency guard keyed on `_ods_lineage_link_id`, which is keyed on the link, which is keyed on `content_hash`. So:

- **same bytes** → same link → guard holds → no doubling. **SOUND** (`test_SOUND_restart_sink_same_content_does_not_double_rows`).
- **changed bytes** (the boundary case) → new link → guard misses → the new rows are stamped **in addition** to the old → target holds **10 rows for a 5-row source**; the old link's rows are **not superseded**; `reconcile_sink` reports `double_count`.

And when only the *upstream* content changes mid-run without clearing downstream: both old and new curated links coexist at one path, the **stale link is still discoverable** (`run_output_link` by path — see C-4), and there is **no `superseded_at`/`is_valid` flag on `cp.lineage_link` and no recon check that marks the orphan**. The spec's "recon flags the orphan" requirement is **not implemented** — the orphan silently rots.

**Fix:** implement the spec's stated guard rail — either a cascade so clear-task supersedes downstream links (add `superseded_at`/`superseded_by_link_id` to `lineage_link`) **or** a recon check that flags any consumer with >1 live link at one `(edge_type, path)`. Without one, the boundary case is a silent data-quality hole.

---

## SOUND result (restart genuinely idempotent here)

- **Same-bytes sink restart does NOT double rows** — `test_SOUND_restart_sink_same_content_does_not_double_rows`. The link 5-part key + the `write_link_then_rows` row-guard combine to make an in-place sink re-run idempotent at row grain, and rows remain traceable to raw via the curated upstream link. This is the one place decision #5's idempotency holds as written — but note it holds **only** because the sink restart re-uses the same `consumer_run_id` in this scenario; a sink restart that minted a new `run_id` (C-1) would mint a new link and break even this.

---

## Cross-cutting root cause

Every CRITICAL/HIGH above traces to one gap: **decision #5 specifies idempotency at file and link grain, but the operation it describes (Airflow clear-task) changes the RUN grain, and discovery is run-grain.** The fix that closes C-1/C-2/C-3 at once is to make run discovery `workflow_run_id`-aware (one live run per `(workflow_run_id, pipeline_type, slice)`); C-4/C-5/C-6 are independent hardening of the selector, recon, and supersede/orphan handling.
