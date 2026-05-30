# R4 — Data-Integrity Sweep (DB-authority constraint completeness)

**Team:** lineage-review · **Agent:** integrity-attacker · **Date:** 2026-05-30
**DB:** `ods_cp`, schema 001–012 (APPLIED) · **Probes:** `tests/test_team_r4.py` (`p9r4_*` / `test_r4_*`, 14 passed)

## Mission
Find every nonsense value the DB **accepts** via a direct INSERT/UPDATE that would corrupt
lineage or reconciliation. The DB is meant to be the authority; the sanctioned `cp.*`
functions are not the only writers we must trust.

## Method
Enumerated all CHECK constraints live in schema `cp`, then fired single-statement probes
under savepoints on the rolled-back `conn` fixture. **The ONLY CHECK constraints that exist:**

| Table | Constraint |
|---|---|
| `lineage_link` | `sink_type_iff_sink`, `target_ref_contract` |
| `lineage_edge` | `raw_edge_requires_source_file`, `edge_must_anchor`, `upstream_link_required_for_run_edges` |

There is **no** CHECK on any count column, any `status`, `trigger_type`, `business_date`,
`attempt`, `input_slot`, or any recon discrepancy/status consistency.

---

## CONFIRMED gaps (empirically accepted by `ods_cp`)

| # | Column / constraint | Nonsense ACCEPTED | Corruption risk | Sev | Proposed CHECK / trigger |
|---|---|---|---|---|---|
| 1a | `lineage_link.record_count` | `-1` | Negative in SUM-based merge totals masks N missing rows | HIGH | `CHECK (record_count >= 0)` |
| 1b | `lineage_edge.record_count` | `-1` | Corrupts per-edge SUM / graph recon | HIGH | `CHECK (record_count >= 0)` |
| 1c/1d | `run_log.record_count_in/out` | `-5` | Negative throughput counts; in/out reconciliation lies | HIGH | `CHECK (record_count_in IS NULL OR record_count_in >= 0)` (+ `_out`) |
| 1e/1f | `run_stage_log.record_count_in/out` | `-5` | Stage-level count corruption | HIGH | `CHECK (record_count_in IS NULL OR record_count_in >= 0)` (+ `_out`) |
| 1g | `dlq.record_count` | `-1` | DLQ total in source==good+dlq recon identity goes negative | HIGH | `CHECK (record_count >= 0)` |
| 1h/1i | `reconciliation_log.source_count / accounted_count` | `-1` | The recon table itself stores impossible counts | HIGH | `CHECK (source_count >= 0)`, `CHECK (accounted_count >= 0)` |
| 2 | `lineage_link.target_ref_contract` | `{"version": null}` and `{"version": ""}` | Version meant to defeat overwrite-mutability (decision #4); null/empty is useless — two outputs share one identity | HIGH | tighten to `coalesce(target_ref->>'version','') <> ''` |
| 3a/3b/3c | `run_log.status`, `run_stage_log.status`, `reconciliation_log.status` | `'banana'` | Garbage status is invisible to status-based recon/orchestration filters | MED | `CHECK status IN (...)` per table, or FK to a status lookup |
| 3d | `run_log.trigger_type` | `'banana'` | Spec enumerates `airflow\|manual\|replay\|dlq_drain`; free text breaks trigger routing | MED | `CHECK (trigger_type IN ('airflow','manual','replay','dlq_drain'))` |
| 4b | `run_stage_log.attempt` | `0`, `-3` | Retry accounting is 1-based; attempt<=0 is meaningless | MED | `CHECK (attempt >= 1)` |
| 4c | `lineage_edge.input_slot` | `-1` | Merge slot binding is 0-based; a negative slot can never bind a declared input | MED | `CHECK (input_slot >= 0)` |
| 5a | `reconciliation_log` (status vs discrepancy) | `status='ok'` with `discrepancy=500` | A real breach reported as clean — the recon table lies about its own verdict | **CRITICAL** | `CHECK ((status='ok') = (discrepancy = 0))` (or derive status) |
| 5b | `reconciliation_log` (discrepancy arithmetic) | `discrepancy=0` while `source=10, accounted=3` | The certified arithmetic is wrong; recon is unfalsifiable | **CRITICAL** | `CHECK (discrepancy = source_count - accounted_count)` |
| 6 | `lineage_link.record_count` vs `SUM(lineage_edge.record_count)` | link header `10` while edges sum to `999` | Core merge invariant; lineage SUMs disagree with the link total | HIGH | deferred constraint trigger on `lineage_edge`: per-link `SUM(edge.record_count) = link.record_count` at COMMIT |

**Control (constraint works as far as it goes):** a `target_ref` *missing* the `version`
key is correctly **REJECTED** by `target_ref_contract` — proving gap #2 is specifically the
key-existence-only check (`? 'version'`), not a broken constraint.

---

## SUSPECTED (flagged, not asserted as hard corruption)

| # | Column | Accepted | Note | Sev |
|---|---|---|---|---|
| 4a | `run_log.business_date` | `'2999-12-31'` | Far-future date accepted. Only a bug if forward-fill is disallowed; likely a function-level policy, not a hard DB constraint. | LOW |

---

## Authority analysis — constraint vs function-only

For each gap: is it caught by a sanctioned `cp.*` function (so the blessed path is safe) but
NOT by a constraint (so a direct INSERT corrupts)?

- **Gaps 1, 4b, 4c (negatives / non-positive):** function-only at best. `cp.write_lineage_link`,
  `cp.finish_stage`, `cp.start_stage` etc. pass caller-supplied counts straight through with no
  validation. A direct INSERT — or a caller handing a negative — corrupts. **These NEED a CHECK**
  (cheap, single-column, non-breaking — all existing fakes are >= 0).
- **Gap 2 (version):** the constraint exists but is too weak; **tighten the CHECK**. Python
  wrappers (`control/lineage.py`) validate shape, but the DB is the backstop and currently isn't.
- **Gaps 3a–3d (status/trigger_type):** pure free TEXT, no function gate. **NEED a CHECK or lookup FK.**
- **Gaps 5a/5b (recon self-consistency):** the most important. `cp.write_reconciliation_check`
  computes discrepancy from two caller numbers (already flagged in 011 as unfalsifiable for the
  *value*); here the **table-level** relationship between status, discrepancy, and source/accounted
  is unconstrained, so a direct INSERT can make the recon table contradict itself. **NEED CHECK(s)**
  or generated columns so status/discrepancy are derived, not asserted.
- **Gap 6 (link == SUM edges):** cross-row/cross-table, so it cannot be a row CHECK. Acceptable as
  function-only ONLY if every writer goes through `cp.write_lineage_link` AND that function enforces
  it; today a direct INSERT bypasses it. **NEEDS a deferred constraint trigger** to be truly DB-authoritative.

## Headline
**13 CONFIRMED gaps, 1 SUSPECTED.** Negative `record_count` is accepted on **all 9 count columns**
(`lineage_link`, `lineage_edge`, `run_log`×2, `run_stage_log`×2, `dlq`, `reconciliation_log` source+accounted),
silently corrupting SUM-based recon. The two CRITICAL gaps are in `reconciliation_log` itself: a row can
say `status='ok'` while `discrepancy<>0`, and `discrepancy` need not equal `source_count - accounted_count` —
the system's own truth check can lie.

## Proposed DB constraints (audit only — NOT applied)
1. `record_count >= 0` on `lineage_link`, `lineage_edge`, `dlq`.
2. `record_count_in/out IS NULL OR >= 0` on `run_log`, `run_stage_log`.
3. `source_count >= 0`, `accounted_count >= 0` on `reconciliation_log`.
4. `target_ref_contract`: `coalesce(target_ref->>'version','') <> ''`.
5. `status IN (...)` (or lookup FK) on `run_log`, `run_stage_log`, `reconciliation_log`.
6. `trigger_type IN ('airflow','manual','replay','dlq_drain')` on `run_log`.
7. `attempt >= 1` on `run_stage_log`; `input_slot >= 0` on `lineage_edge`.
8. `discrepancy = source_count - accounted_count` and `(status='ok') = (discrepancy = 0)` on `reconciliation_log`.
9. Deferred constraint trigger: per-link `SUM(lineage_edge.record_count) = lineage_link.record_count`.
