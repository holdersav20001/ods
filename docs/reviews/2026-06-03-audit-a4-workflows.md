# Adversarial Audit A4 — Workflows / SDK / Control Wrappers

Date: 2026-06-03
Scope: `control/` (runs, stages, lineage, recon, visibility, dlq, schema, sdk),
`harness/customer_transaction_workflow.py`, `harness/policy_claims_workflow.py`,
`harness/policy_claims_dlq_workflow.py`, `dags/policy_claims_dag.py`.
Method: EXPECTED derived from `docs/reference/control-plane-write-contract.md` +
`docs/reference/refeed-replacement-policy.md`. Each finding PROVEN with a
code citation and/or a read-only query against the committed demo DB
(`localhost:5440/ods_cp`). Read-only; nothing fixed.

Lens: write-contract violations, gate bypasses, "numbers right / lineage or
state wrong" bugs, changed-only refeed correctness, SDK delegation, wrapper
correctness, reset domain-scoping, orchestrator identity, DAG import-safety.

---

## Headline

**6 CONFIRMED findings. No activation gate was found to be bypassable in the
SQL primitive itself** (the recon/succeeded gate lives in
`cp.activate_target_visibility` and the live DB shows zero double-active rows and
zero recon breaches). The most serious issues are **omission/incompleteness**,
not bypass:

1. **A4-01 (HIGH, contract violation):** the customer/transaction workflow
   (`sales` domain) writes two business-visible Postgres sinks but **NEVER calls
   `visibility.activate` (write-contract step 9)**. Live DB: `sales` has 8 sink
   runs / 31 sink rows and **0 `ods.target_visibility` rows**. The whole demo's
   "which output is active now" truth is missing; a refeed there silently
   supersedes nothing.
2. **A4-02 (HIGH, lineage-wrong / numbers-right):** the policy/claims (non-DLQ)
   **refeed aggregate has incomplete provenance**. Its single
   `detail_to_aggregate` input edge names only the refeed detail sink (2 changed
   rows) yet declares `record_count=4`; the 2 unchanged rows' producing output is
   not referenced. Recon still reports `ok` (caller two-number check). The DLQ
   workflow does this correctly (two edges, `detail_original`+`detail_corrected`)
   — proving the non-DLQ path is the defect.

The remaining findings are MEDIUM/LOW (cross-hop recon never invoked,
try_number never refreshed, SDK recon-gate ordering, schema validate edge cases).

---

## CONFIRMED FINDINGS

### A4-01 — Customer/transaction workflow skips write-contract step 9 (activation) entirely
- Severity: **HIGH** — write-contract violation (business-visible sink, no activation).
- Component: `harness/customer_transaction_workflow.py`.
- Evidence (code): the module imports only
  `from control import lineage, recon, runs, stages`
  (`harness/customer_transaction_workflow.py:36`) — **no `visibility` import**.
  Grep for `visibility|activate` in the file returns only the `reset_demo_state`
  DELETE of `ods.target_visibility` (lines 125, 136-137); there is no
  `visibility.activate` call anywhere. `_sink_rows` (lines 442-496) writes rows
  + `reconcile_sink_link` + `finalise`, then returns — step 9 is never executed
  for `ods.customer_transaction` or `ods.customer_transaction_daily`.
- Evidence (DB, read-only):
  `SELECT domain,count(*) FROM ods.target_visibility GROUP BY 1` →
  `insurance=21, insurance_dlq=7` and **no `sales` row**;
  `cp.run_log` shows `sales` has 8 `pipeline_type='sink'` runs over the two
  datasets and `ods.customer_transaction` has 20 rows /
  `ods.customer_transaction_daily` 11 rows. So 31 business-visible sink rows are
  active in the data layer with **no business-truth (visibility) record at all**.
- Why it matters: the write contract (step 9) requires activation for any
  business-visible sink, and the refeed-policy doc's whole supersession model
  operates on `ods.target_visibility`. This demo's "Day-2 transaction refeed"
  therefore proves lineage but **does not prove the active-slice/refeed-truth
  story** — its corrected sink rows never supersede the originals in
  business-truth (there is no business-truth). The customer demo's refeed is a
  lineage-only refeed, not a visibility refeed.
- CONFIRMED.
- Fix (documented, not applied): activate one visibility row per output
  (slice-scope for the detail/aggregate, or business_key like the policy demo)
  after each successful sink + recon-ok, exactly as
  `policy_claims_workflow._activate_business_keys` does.

### A4-02 — Policy/claims (non-DLQ) refeed aggregate: incomplete provenance, record_count mismatch
- Severity: **HIGH** — "numbers right / lineage wrong".
- Component: `harness/policy_claims_workflow.py`, `refeed_execution` →
  `_aggregate_from_detail`.
- EXPECTED (refeed-policy doc, `business_key` scope + completeness): a recomputed
  aggregate must trace to EVERY contributing detail output. For Day-2 `auto`, the
  recomputed bucket is built from CL100(corrected)+CL101(unchanged)+CL102
  (corrected): 3 rows from two distinct detail outputs (refeed sink + original
  sink).
- Evidence (code): the refeed aggregate run is built by
  `_aggregate_from_detail(..., detail_sink=detail_sink, detail_rows=detail_rows,
  ...)` (`harness/policy_claims_workflow.py:867-871`), where `detail_sink` is the
  **refeed** sink that wrote only `changed_detail_rows` (line 848-854,
  `rows=changed_detail_rows`) and `detail_rows` is the FULL corrected day. The
  aggregate writes ONE `detail_to_aggregate` input edge naming
  `detail_sink["link_id"]` with `record_count=len(detail_rows)` (the FULL count)
  — `harness/policy_claims_workflow.py:651-657`. The original Day-2 detail sink
  (which still holds the unchanged rows' provenance) is never added as an input.
- Evidence (DB, read-only) — the committed refeed run set
  (`orchestrator_run_id LIKE '%refeed%'`):
  - refeed detail sink `record_count_out=2`, rows stamped=2 (changed-only).
  - refeed aggregate's `detail_to_aggregate` edge:
    `record_count=4, input_slot=0, upstream dataset=policy_claim,
    upstream record_count_out=2, upstream_is_refeed=true`.
  - i.e. the edge declares 4 but the sole named upstream produced 2; the other 2
    rows' output is dropped from provenance.
  - recon for that aggregate run: `aggregate_policy_claim_daily source=4
    accounted=4 status=ok` — a caller two-number check that cannot see the
    missing edge.
- Contrast (proves it is fixable + that the DLQ path is correct): the DLQ
  workflow's `_recompute_affected_aggregates`
  (`harness/policy_claims_dlq_workflow.py:835-916`) adds one
  `detail_to_aggregate` edge per contributing active detail output. Live DB
  `insurance_dlq` aggregate edges:
  `(rc=3,slot=0,role=detail) | (rc=2,slot=0,role=detail_original) |
  (rc=1,slot=1,role=detail_corrected)` — complete provenance.
- Why it matters: a trace-to-raw of the corrected `2026-05-29:auto` aggregate row
  reaches only the corrected claim's detail sink, NOT the unchanged auto claim
  (CL101) that is also counted in `claim_count`. The aggregate's count is
  correct but its lineage under-reports where it came from, and its declared
  edge `record_count` (4) disagrees with the named output (2).
- CONFIRMED.
- Fix: mirror the DLQ workflow — recompute affected aggregate keys from the full
  current detail set and emit one `detail_to_aggregate` edge per contributing
  detail output (original sink for unchanged rows + refeed sink for changed rows),
  with per-edge record_counts that sum to the true input.

### A4-03 — Cross-hop reconciliation (`reconcile_workflow`) is implemented but never called
- Severity: **MEDIUM** — DoD gap / latent gate not exercised.
- Component: `control/recon.py:53-66` (`reconcile_workflow`), all three harness
  workflows.
- EXPECTED: the write-contract step 8 lists `reconcile_workflow(workflow_run_id)`
  as the cross-hop raw-in-vs-sink+dlq-out check; its docstring states it is the
  only check that catches a wholly-failed upstream silently dropping rows.
- Evidence (code): no harness calls `recon.reconcile_workflow` (only
  `recon.write_check` and `recon.reconcile_sink_link` appear in the three
  workflows). Evidence (DB):
  `SELECT count(*) FROM cp.reconciliation_log WHERE check_type='workflow'` → **0**.
- Why it matters: every committed workflow relies solely on per-output sink_link
  recon + caller-supplied write_checks. The per-run/per-output checks are
  self-consistent by construction (each writes `source_count=len(rows),
  accounted=len(rows)`), so the only check that could catch cross-hop loss is
  never run. This is the mechanism that would otherwise have flagged A4-02's
  dropped-provenance refeed at the workflow level.
- CONFIRMED.
- Fix: call `recon.reconcile_workflow(conn, workflow_run_id=...)` at the end of
  each execution (and assert `ok`).

### A4-04 — Orchestrator `try_number` is hardcoded; restart identity never refreshes
- Severity: **MEDIUM** — orchestrator identity / restart-identity incorrectness.
- Component: `harness/policy_claims_workflow.py:_orchestrator` (default
  `try_number=1`, line 159) and `policy_claims_dlq_workflow.py:_orchestrator`
  (line 125). `runs.start` threads `orchestrator` straight through
  (`control/runs.py:31-46`).
- EXPECTED (audit brief + migration 020): an Airflow run carries identity and a
  restart refreshes `try_number`.
- Evidence (DB, read-only):
  `SELECT domain, orchestrator_try_number, count(*) FROM cp.run_log
   WHERE domain IN ('insurance','insurance_dlq') GROUP BY 1,2` →
  `insurance try=1 (30 runs)`, `insurance_dlq try=1 (13 runs)` —
  **every run has try_number=1; distinct try_number count = 1.** All 43 runs
  carry `orchestrator_type='airflow'` (good), but the try number never advances.
- Nuance: the harness simulates fresh runs only, so try=1 is expected for the
  harness path. The DAG's `_OrchestratorBinding._live` (dags/policy_claims_dag.py
  :162-171) DOES forward the live Airflow `ti.try_number` via
  `airflow_orchestrator_context` (line 139). So the bug is scoped to the harness
  simulation: it has no codepath that re-runs a failed run with an incremented
  try, so restart-identity is asserted nowhere. SUSPECTED for the real DAG
  (untestable here — Airflow not installed), CONFIRMED for the harness (no
  restart path exercises try_number > 1).
- Fix: add a restart/retry fixture that re-invokes `runs.start` for the same
  logical task with `try_number=n+1` (cp.start_run restart-reuse) and assert the
  run_log row's `orchestrator_try_number` advances.

### A4-05 — SDK `task` finalises `succeeded` without verifying recon; gate relies on caller ordering
- Severity: **MEDIUM** — gate ordering relies on convention, not enforcement.
- Component: `control/sdk.py:146-174` (`task`), `control/stages.py:27-52`.
- Observation 1 (recon not gated by SDK): `task`'s clean-exit path calls
  `runs.finalise(..., status="succeeded")` unconditionally
  (`control/sdk.py:173-174`). It does not check that any `reconcile_*` ran or
  returned `ok`. The succeeded+recon-ok gate is enforced only later, inside
  `cp.activate_target_visibility` when (and only when) the caller calls
  `visibility.activate`. So the SDK itself will happily mark a run `succeeded`
  with no recon at all; A4-01 is exactly that situation in raw-wrapper form. This
  is by design per the doc (recon is a separate step), but it means **"succeeded"
  is not evidence of reconciliation** — only activation is. Anyone reading
  `run_log.status='succeeded'` as "reconciled" is wrong.
- Observation 2 (exception path double-write, benign): on exception inside a
  stage, `stages.stage_scope` writes `finish_stage('failed', ...)` and (if
  `commit`) commits, then re-raises (`control/stages.py:35-43`); the exception
  then propagates to `task.__exit__`, which patches the RUN `failed` and re-raises
  (`control/sdk.py:167-172`). With `commit=False` (every demo/test), neither the
  failed stage nor the failed run is committed inside the wrappers, so the caller
  must roll back; the failure rows are lost unless the caller commits them. This
  is acceptable for the transactional demos but means **a failed stage's
  diagnostic row is not durable under the `commit=False` composition pattern** —
  it is rolled back with everything else.
- CONFIRMED (code-level; both are real behaviours, severity is "trap" not
  "broken").
- Fix: document that `succeeded` ≠ reconciled, and consider a `task(...,
  require_recon=True)` option that asserts a recon row exists before finalising;
  for failed-run durability, finalise/patch on a side connection or commit the
  failure rows before rollback.

### A4-06 — `schema.validate_rows` does not flag extra/unknown columns or type mismatches; empty contract passes everything
- Severity: **LOW/MEDIUM** — validation edge cases (silent acceptance).
- Component: `control/schema.py:36-62`.
- EXPECTED (audit brief): handle extra columns, type mismatches, empty rows.
- Evidence (code): `validate_rows` only checks (a) required column present and
  (b) required non-nullable column not None (`control/schema.py:49-57`). It does
  NOT check:
  - **extra/unknown columns** — a row with arbitrary extra keys passes; there is
    no "unexpected column" rejection.
  - **type mismatches** — a `claim_amount` of `"five"` or a `policy_id` of `123`
    passes (only None-ness is checked).
  - **empty `required`** — if a contract has empty/NULL `required_columns`
    (`contract.get("required_columns") or []`, line 44), every row is GOOD
    including `{}` — an empty row passes validation.
  These are silent-accept paths: bad data flows to the GOOD output and is
  activated, never quarantined.
- Live impact: the DLQ demo only exercises the missing-required-column path
  (`CL900 policy_id=None`), so the type/extra-column gaps are untested by any
  committed run.
- CONFIRMED (code-level).
- Fix: extend the contract/validator to reject unknown columns when the contract
  is closed, and add type checks keyed on a contract column-type map; treat an
  empty `required_columns` as a misconfiguration (raise), not "accept all".

---

## CHECKED AND FOUND CORRECT (negative results — no finding)

- **Changed-only refeed (policy/claims detail + aggregate visibility):** CORRECT.
  Live DB Day-2 (`2026-05-29`) detail visibility: 4 active keys (CL100,101,102,103);
  only the CHANGED keys `P001:CL100` and `P002:CL102` have a superseded (`N`,
  `superseded_by` set) prior row, unchanged CL101/CL103 keep their original `Y`.
  Aggregate: `2026-05-29:auto` went `N`→`Y` (changed), `2026-05-29:home` stayed
  `Y` (unchanged). No blanket slice deactivation.
- **No double-active per key:** `GROUP BY (domain,dataset,business_date,sink_type,
  target_name,replacement_scope,replacement_key) HAVING count(*)>1 WHERE status=Y`
  → **0 rows**. The partial unique index holds across all three demos.
- **Activation gate (succeeded + recon-ok):** enforced in
  `cp.activate_target_visibility` (per visibility.py:44-46 docstring + spec); no
  recon breach exists in the DB (`reconciliation_log.status<>'ok'` → 0 rows), so
  no stale/breached activation could be observed. The Python wrapper correctly
  rejects both-ids-conflict and missing-id (`control/visibility.py:48-55`).
- **DLQ replay correctness:** `_recompute_affected_aggregates` recomputes the
  affected key from the FULL current detail set and emits complete provenance
  (two detail_to_aggregate edges, confirmed in DB). The quarantine→corrected→
  resolved lifecycle and the `replay` annotation edge (is_provenance=false leaf)
  are wired through sanctioned wrappers; the bad row is never activated.
- **`reset_demo_state` domain-scoping:** all three resets scope every DELETE to
  `domain IN (this domain's run set)` and delete child→parent (recon→dlq→edge→
  link→stage→run→file) with target rows cleared first; no global TRUNCATE. The
  DLQ workflow deliberately uses a distinct domain (`insurance_dlq`) so the
  `insurance` reset cannot FK-violate its committed rows. Live DB shows the three
  domains (`sales`, `insurance`, `insurance_dlq`) coexisting (30/30/13 runs) —
  consistent with non-overlapping scoped resets.
- **DAG import-safety + single workflow_run_id:** Airflow imports are guarded
  (`dags/policy_claims_dag.py:47-55`); `derive_workflow_run_id` is a deterministic
  uuid5(dag_id,dag_run_id) minted once by `ingest_policy` and pushed via XCom
  (`XCOM_WORKFLOW_RUN_ID`), with downstream tasks pulling it (one id per DAG run).
  `_latest_hop` recovers run/link ids from the control plane between separate task
  processes (read-only). Import-safe and correctly threaded.
- **Lineage input-key translation / target_ref contract:** `_translate_inputs`
  raises on new+old key conflict; `_validate_target_ref` enforces non-empty
  path/content_hash + present version client-side (`control/lineage.py:31-76`).
- **`run_output_link` discovery:** raises on absence/ambiguity (no silent stale
  pick) per `control/runs.py:81-105`; harnesses pass upstreams explicitly rather
  than discovering, avoiding cross-execution ambiguity.

---

## Notes on method / limits
- `docker exec` is broken; all DB evidence is read-only `SELECT`s against the
  committed demo data on `localhost:5440/ods_cp` via psycopg (autocommit reads,
  no writes/commits of new state).
- The planned spec path `docs/specs/working-platform-completion-plan.md` does not
  exist; the live spec is `docs/specs/2026-06-03-working-platform-completion-
  plan.md` (referenced by the DLQ workflow header). The 10-step contract was
  taken from `docs/reference/control-plane-write-contract.md` as authoritative.
- A4-04 (real-DAG try_number) is SUSPECTED-only because Airflow is not installed
  here; the harness portion is CONFIRMED.
