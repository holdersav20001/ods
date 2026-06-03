# Audit A3 — Test Quality (passes-for-the-wrong-reason + coverage gaps)

Date: 2026-06-03
Lens: A3 — do GREEN tests mean CORRECT code? Hunt for self-confirming asserts,
tautologies, would-pass-if-broken, recon fed self-consistent fakes, unscoped
queries, missing negatives, and assertions weakened to match buggy behaviour.
Scope: all 40 `tests/test_*.py` (12,230 lines). Read-only — DOCUMENT, no fixes.

## Headline

**The suite is overwhelmingly sound and unusually self-aware.** The original
fan-out failure mode (assert the happy shape the harness hand-built) has been
systematically hunted down by the `test_audit_a*` / `test_team_r*` probe files,
which encode the *requirement's hardest case* and assert against **real workflow
output / DB-derived state**, not hand-shaped fixtures. Trace-to-raw, changed-only
refeed, discovery determinism/tiebreak, write-contract, schema validation,
graph-derived recon, DLQ lifecycle, and complete-input-edges are each guarded by
at least one test that **would fail under a plausible mutation**.

The single structural weakness is **arithmetic reconciliation** (`recon.write_check`
/ `cp.write_reconciliation_check`): it compares two CALLER-supplied numbers, so
every test that drives it with `accounted = good + dlq` is **balanced by
construction and unfalsifiable for real row loss**. This is a KNOWN, DOCUMENTED
limitation — the codebase added graph-derived recon (`reconcile_sink`,
`reconcile_sink_link`, `reconcile_workflow`) precisely to close it, and those ARE
tested adversarially (delete real rows → breach). So the weak `write_check` tests
are testing the function's *arithmetic contract*, not pretending to detect loss —
but several have names/docstrings ("recon catches imbalance", "input == good+dlq")
that **overstate** what they prove.

**No test was found that hides a currently-live bug.** Two probe files
deliberately assert KNOWN GAPS as the current (wrong) behaviour
(`test_team_r4`, by design); these are documented findings, not silent passes.

Counts: **~well over 300 assertions across 40 files. Sound: 35 files.
Weak/wrong-reason or notable caveat: 5 files (8 specific tests). Genuine
coverage gaps: 3 (2 self-flagged in-suite).**

---

## Per-area findings

### Reconciliation — the one real soft spot

| Test / area | Verdict | Sev | Why / mutation not caught | Strengthening |
|---|---|---|---|---|
| `test_recon.py::test_recon_balance_regimes` (+ breach/double_count/balanced_via_fake_fail) | weak / wrong-reason | MED | Drives `recon.write_check(source, accounted=good+dlq)` and asserts `disc = source-accounted` & status-from-sign. The test HANDS the SQL both numbers, so it only proves arithmetic on its own inputs. `fake_fail` is balanced *by construction* (`source=good+bad, accounted=good+bad`). A mutation that lost rows in a real sink is invisible here. | These test the arithmetic contract only — keep, but rename/redocument to stop implying loss-detection. The real-loss guard lives in `test_graph_recon` / `test_team_r3` — fine division of labour. |
| `test_dlq_lifecycle.py::test_quarantine_recon_input_equals_good_plus_dlq` | weak / tautology | MED | Hand-inserts a good link (rc=3) + a dlq row (rc=1), then asserts `3 + 1 == 4`. The `4` is a hardcoded constant the test itself chose; **no recon function is invoked**. Proves `3+1=4`, not that recon accounts for good+dlq vs a real source. Would pass if recon were entirely broken. | Drive a real stage with 4 raw rows → assert the recon_log row's source/accounted, or delete this in favour of `test_audit_a4::test_s6`. |
| `test_policy_claims_dlq_workflow.py::test_06_recon_input_equals_good_plus_dlq` | sound-ish (minor caveat) | LOW | Reads the recon_log row the *workflow* wrote (real output, not hand-inserted) and asserts `source==4, accounted==4, ok`. Better than the lifecycle version, but still `write_check`-style (balanced by construction); the `4` is the demo's known input. Cannot catch real loss — but does not claim to. | Acceptable; the loss path is covered by `test_team_r3` workflow recon. |
| `test_contract.py::test_write_reconciliation_check` (parametrized) | weak / wrong-reason | LOW | Same arithmetic-only pattern as `test_recon`. It is explicitly a function-contract test; graph recon contract tested separately (`test_reconcile_sink_link_per_output_roundtrip`, `test_reconcile_workflow_cross_hop_roundtrip`). | None needed beyond a docstring note. |
| `test_sink_dlq_replay.py::test_dlq_in_graph_and_recon` | sound (recon half is weak) | LOW | The `metrics["good"]+metrics["dlq"]==source==accounted` assert is balanced-by-construction; BUT the quarantine-link-in-`v_provenance` half is a real observation. | Keep; recon half is decorative. |
| **GAP: no `write_check` test drives a true UNBALANCED stage** | gap | LOW | All breach/double_count cases are produced by passing a mismatched `accounted` directly. No fake stage actually drops a row WITHOUT DLQ-ing it and lets `write_check` notice. (Graph recon covers real loss, so impact is low.) | One fake stage that loses a row silently → assert `write_check` breaches. |

**Reconciliation contrast (the GOOD model):** `test_graph_recon.py::test_graph_recon_forced_real_loss_breaches` writes 10 real `ods.orders` rows, **DELETEs 3**, then reconciles `source=10` → asserts `accounted==7, breach`. This FAILS if the SQL trusts a supplied number. `test_graph_recon_accounted_independent_of_caller` reconciles with `source=999` and proves accounted is still DB-derived (8). `test_team_r3::test_FIXED_wholly_failed_upstream_loses_rows_breaches_via_workflow_recon` and `test_audit_a4::test_s6` prove cross-hop / per-output loss is caught. These are exemplary.

### Discovery determinism & tiebreak — SOUND

| Test | Verdict | Notes |
|---|---|---|
| `test_discovery_tiebreak.py` (all 7) | SOUND | Forces the exact adversarial tie (`UPDATE ... finished_at = now()` on both runs) so only the `seq` tiebreak can decide; proves determinism across 10 repeats; `test_distinct_finished_at_still_orders_by_finished_at` proves seq does NOT override finished_at. Would fail under a broken ORDER BY. |
| `test_discovery_determinism.py::test_succeeded_runs_ordered_by_clock` | weak (name/assert mismatch) | LOW. Name + docstring promise "ordered / strictly increasing", but the only assert is `set(ids) <= got` — a **membership** check, not ordering. A mutation returning runs in arbitrary/reverse order passes. (Ordering IS proven in `test_discovery_tiebreak::test_succeeded_runs_newest_first...`, so net coverage is fine.) Add an order assert here too. |
| `test_audit_a2.py::test_audit_discovery_picks_the_NEWEST_not_just_a_member` | SOUND | Closes the exact gap above at the audit layer. |

### Changed-only refeed / replacement policy — SOUND (model tests)

| Test | Verdict | Notes |
|---|---|---|
| `test_refeed_policy.py::test_business_key_changed_only_supersedes_only_changed_keys` | SOUND | THE critical invariant. Activates K1+K2, refeeds ONLY K1, asserts K1→N/corrected→Y **and K2 stays Y**. Fails if a refeed wrongly wiped the whole slice (the bug class). |
| `test_refeed_policy.py` slice / file / append_only / failed-refeed / restart-before-activate | SOUND | Real negatives (failed run raises "must be succeeded"; append_only keeps both Y). |
| `test_refeed_policy.py::test_manual_approval_...` (xfail strict, raises=CheckViolation) | SOUND | Strict xfail with specific exception — fails loudly the day 'P' status is added. Correct way to mark a future gap. |
| `test_customer_transaction_workflow.py::test_13b_refeed_target_upsert_only_writes_changed_rows` | SOUND | Changed-only at the row level. |
| `test_policy_claims_dlq_workflow.py::test_16/17/18/19` | SOUND | Replay recomputes affected aggregate; stale superseded; **unchanged key stays active**; recomputed aggregate traces to ALL contributing details (normal + replay). |

### Trace-to-raw / lineage / fan-out / merge-slot — SOUND

| Test / area | Verdict | Notes |
|---|---|---|
| `test_lineage_single.py::test_discovery_selects_latest_ingest_no_upstream_param` | SOUND | Asserts `canon.upstream == latest_succeeded_run()` (proves it SELECTS) + static signature proof no upstream is passed + `test_discovery_raises_without_ingest` negative. |
| `test_lineage_merge.py` (slot binding) | SOUND | 3 distinct upstreams, distinct slots 0/1/2, edge-sum==link rc, **each slot traces to its OWN raw file**, multiset of counts matches files. Directly guards the F2 slot-misattribution bug. |
| `test_lineage_hardening.py::test_downstream_edge_specificity` / `test_trace_row_sql_no_sibling_overclaim` | SOUND | Asserts the walk reaches the named output's raw (`file_a`) AND **`file_b NOT in reached`** — proves no sibling over-claim. |
| `test_audit_a2.py::test_audit_merge_total_balances_even_when_slots_misattributed` | SOUND (meta) | Deliberately shows total-recon stays `ok` under misattribution — documents WHY total-balance recon is insufficient and the per-slot test is needed. Excellent self-awareness. |
| `test_sink_dlq_replay.py::test_replay_traces_to_raw` (X5) | SOUND | Replayed row traces via its OWN re-written chain; asserts original file NOT the sole raw leaf. |
| `test_team_r1/r2/r3` | SOUND | Restart-identity (reuse run, no double-count), refeed no-cross-contamination (old→old raw, new→new raw), completeness/DB-authority (direct-insert smuggle & forged edge RAISE). |

### DLQ lifecycle & schema validation — SOUND

| Test | Verdict | Notes |
|---|---|---|
| `test_dlq_lifecycle.py` (status enum reject, unknown-id raise, payload preserved across resolve, terminal-requires-traceable-ref) | SOUND | Strong negatives. (Only `test_quarantine_recon_input_equals_good_plus_dlq` is weak — see recon table.) |
| `test_schema_contract.py` (validate_rows) | SOUND | Positive AND negative: missing-required bad, non-nullable-null bad, nullable-null good, mixed batch split. |
| `test_diagnostics.py` (all ~30) | SOUND (model file) | Every diagnostic check has a test that INJECTS the anomaly and asserts detection, plus exemption tests (quarantine/replay edges NOT flagged) and a clean-workflow baseline. The right way to avoid positive-only testing. |
| `test_edge_validity.py` / `test_constraints.py` / `test_adapter_contract.py` | SOUND | Heavy negative/constraint coverage; `test_bad_sink_job_is_caught_by_the_contract` proves the contract CATCHES wrong ordering (not just confirms right). |

### Write contract — SOUND

`test_write_contract.py` runs the real demos and queries the actual control tables
scoped to this run's `workflow_run_id`s, with NOT-EXISTS orphan checks. Invariant 5's
`(upstream_output_link_id is None) == (source_file_id is None)` XOR-anchor check is a
genuine adversarial guard against dangling edges. Would fail if the workflow produced
unstamped rows or orphan edges.

---

## Specific weak / wrong-reason tests (the short list)

| Test | Classification | Sev | Mutation that escapes |
|---|---|---|---|
| `test_dlq_lifecycle.py::test_quarantine_recon_input_equals_good_plus_dlq` | tautology (3+1==4 on hand-inserted rows; no recon invoked) | MED | Break recon entirely → still green. |
| `test_recon.py::*` + `test_contract.py::test_write_reconciliation_check` + `fake_fail`-balanced asserts | self-consistent fakes (`accounted=good+dlq` by construction) | MED | Any real row-loss path — unfalsifiable by these. |
| `test_audit_a1.py::test_probe_CONFIRMED_run_output_link_tiebreak_is_nondeterministic` | precondition-only / near-tautology | LOW | Only asserts `now() <> clock_timestamp()` (a Postgres truism). Does NOT exercise `run_output_link`; named as if it proves nondeterminism. Would stay green even if the tiebreak were fixed. The actual behaviour is guarded elsewhere (now RAISES on ambiguity), so it is a vestigial probe. |
| `test_discovery_determinism.py::test_succeeded_runs_ordered_by_clock` | name/assert mismatch (asserts membership, not order) | LOW | A reordering mutation passes. Ordering covered in `test_discovery_tiebreak`. |
| `test_audit_a4.py::test_s7a` (`assert rows is not None` for trace_row.sql cursor) | weak half | LOW | A cursor result is never None; the real cycle-flag assert (`any(r[2] ...)`) on the adjacent line carries the test. |

## Coverage gaps

1. **No `write_check` test drives a truly unbalanced *stage*** (silent row drop without DLQ). All breach/double_count cases pass a mismatched `accounted` by hand. LOW impact — graph/workflow recon covers real loss. (Self-noted gap.)
2. **`test_team_r4.py::test_r4_link_count_vs_edge_sum_unenforced`** — asserts `_accepts(...)` is TRUE: the DB accepts `link.record_count != SUM(edges)`. A **known, unfixed integrity gap**, asserted as current behaviour (by design, will flip when fixed). MED data-integrity gap.
3. **`test_target_visibility.py::test_file_scope_replacement_skipped`** (`@pytest.mark.skip`) — file-scope replacement marked deferred, BUT `test_refeed_policy.py::test_file_scope_supersedes_only_that_file` actually exercises and passes file-scope. The skip is **stale / redundant**, not a real gap. LOW.

## Notes for the reader

- **xfail/skip audit** (only 4 total): `test_refeed_policy` manual_approval xfail (strict, correct); `test_customer_transaction_workflow` visibility skip (declared non-goal, correct); `test_target_visibility` file-scope skip (stale — covered elsewhere); `test_team_r2` conditional skip when the DB blocks the two-links-one-path setup (defensible). None hide a real gap silently.
- **Stale-comment contradiction (not a test defect):** `test_idempotency.py::test_replay_same_correction_twice...`'s docstring references a sibling `test_link_then_rows_rows_are_NOT_idempotent` claiming rows double on retry — but the actual sibling `test_link_then_rows_rows_are_idempotent` asserts rows do NOT double (migration 008). The docstring predates the fix; the test itself is sound and adversarial (calls the write twice, asserts no doubling).
- **Global-query robustness:** demo-driven tests consistently scope to this run's `workflow_run_id`s (or use the rollback `conn` fixture), so committed sibling/demo data does not false-pass them. `test_dashboard_developer_functions` hardcodes demo counts (8/8/8/9) but filters `WHERE workflow_run_id = <this demo's id>`, so the counts are the deterministic demo shape, not global — not fragile.
