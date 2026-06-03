# Adversarial Audit A5 — Diagnostics, Dashboard, Docs-vs-Reality

- **Date:** 2026-06-03
- **Lens:** A5 — diagnostics that don't detect what they claim; dashboard render/data
  bugs; documentation that contradicts the code.
- **Mode:** READ-ONLY. DB queried over TCP `localhost:5440` (`ods_cp`, `ods/ods`) via
  psycopg3; every mutation used to PROVE a check was executed inside a transaction and
  **rolled back** — nothing committed. Code cited by `file:line`.
- **Scope:** migrations 022/027/029 (diagnostics + read functions), `dashboard/`
  (`app.js`, `metadata-flow.js`, `index.html`, 3 snapshots), and the 4 reference docs
  (`README.md`, `control-plane-write-contract.md`, `refeed-replacement-policy.md`,
  `support-runbook.md`).

## Headline

**6 CONFIRMED findings.** Two are doc-vs-reality lies in `README.md`: it tells you to
call **`control.stages.start(...)` / `control.stages.finish(...)`**, functions that
**do not exist** (only `stage_scope` does), and it claims the migration set is
**"001 through 027"** when the repo ships through **029**. The most consequential
runtime finding is in the **support runbook**: its Question-5 query promises to "trace
the quarantine output back to the raw file it came from … a bad row traces to raw
exactly like a good one," but `cp.dashboard_output_trace(quarantine_output_link_id)`
**stops at hop 1 with `source_file_id = NULL`** — the quarantine edge carries its raw
identity in `source_ref` JSONB, not in the `source_file_id` column the trace surfaces,
so the documented support step silently fails to reach raw. No diagnostic produces a
false positive on the healthy demo, and every new 027 check that *has* matching demo
data was proven to fire; one new check (`schema_validation_output_missing_schema_version`)
is structurally sound but **dormant on the committed demo** (its three conditions can
never co-occur there).

All 8 read/diagnostic functions exist with the documented signatures; all 5 runbook
questions execute and return the documented shape (except the Q5 raw-trace claim above);
`node --check` passes on both dashboard JS files; all 3 snapshots load and the DLQ
banner renders "resolved (replayed)".

---

## CONFIRMED findings

### A5-1 (HIGH) — Runbook Q5 "trace the quarantine output back to raw" does not reach raw
- **Item:** `docs/reference/support-runbook.md` §5 ("Given a bad row / DLQ row…").
- **Doc claim:** the query
  `SELECT hop, edge_type, dataset, source_file_id, raw_s3_path FROM
  cp.dashboard_output_trace((SELECT quarantine_output_link_id FROM cp.dlq WHERE …))`
  with prose "trace the quarantine output back to the raw file it came from" and
  "`quarantine_output_link_id` is the lineage anchor, so a bad row traces to raw exactly
  like a good one."
- **Reality (CONFIRMED, query run read-only):** for the single committed DLQ row
  (`quarantine_output_link_id = ef35976e-…`) the trace returns exactly **one row**:
  `hop=1, edge_type=quarantine, source_file_id=NULL, raw_s3_path=NULL`. The underlying
  `cp.lineage_edge` for that link has `source_file_id=NULL`, `upstream_lineage_link_id=NULL`,
  and stores the raw identity only in `source_ref = {"raw_file_id":"b8099101-…",
  "raw_path":"s3://raw/insurance_dlq/claim_dlq/2026-05-29.json", …}`. By contrast a *good*
  row (`cp.dashboard_target_row_trace('ods','customer_transaction',1)`) terminates at
  `hop=4 raw_to_curated` with `source_file_id`/`raw_s3_path` populated — so the
  "exactly like a good one" claim is false for the quarantine path.
- **Code:** `db/migrations/022_dashboard_developer_functions.sql:236-302`
  (`dashboard_output_trace` selects `c.source_file_id`/`fc.s3_raw_path` only; the
  quarantine edge has neither). `register_file`/quarantine writer stamps `source_ref`,
  not `source_file_id`, for quarantine edges.
- **Fix:** either (a) reword the runbook to say the quarantine raw file is in
  `cp.dlq.source_ref->>'raw_file_id'` / `payload_ref` (and show that query), or
  (b) extend `dashboard_output_trace` to surface `source_ref->>'raw_file_id'` as a
  fallback raw anchor for quarantine edges. Audit-only — do NOT fix.

### A5-2 (HIGH) — README write-contract brief cites non-existent `control.stages.start/finish`
- **Item:** `README.md:203,205` (the "In brief" 10-step list).
- **Doc claim:** `3. start stage   control.stages.start(...)` and
  `5. finish stage  control.stages.finish(...)`.
- **Reality (CONFIRMED):** `control/stages.py` defines **only** `def stage_scope(conn,
  run_id, stage, attempt=1, *, commit=True)` (`control/stages.py:28`). There is no
  `start` and no `finish`. The authoritative `control-plane-write-contract.md` (step 3)
  explicitly says **"There is no `stages.start` / `stages.finish`. The context manager
  opens the stage on `__enter__`."** — so the README contradicts its own sibling doc and
  the code. A reader copying the README brief calls a `AttributeError`.
- **Fix:** README brief should read `control.stages.stage_scope(...)` (context manager)
  for steps 3+5. Audit-only.

### A5-3 (MED) — README undercounts the migration set ("001 through 027" / "001-027")
- **Item:** `README.md:108` ("applies the ordered migrations in `db/migrations/`
  (001 through 027)") and `README.md:269` ("`db/migrations/  001-027 ordered Postgres
  migrations`").
- **Reality (CONFIRMED):** `db/migrations/` contains through **029**
  (`028_discovery_tiebreak.sql`, `029_dlq_diagnostics_fixes.sql`). 029 is the migration
  that actually makes the diagnostics correct (P1a/P2a fixes); omitting it from the
  README count is materially misleading about which DB state the docs describe.
- **Fix:** update both references to "001 through 029" / "001-029". Audit-only.

### A5-4 (MED) — `schema_validation_output_missing_schema_version` is DORMANT on the committed demo
- **Item:** `db/migrations/029_dlq_diagnostics_fixes.sql:413-444` (authoritative copy;
  also `027_diagnostics.sql:432-463`). Listed in the migration banner as one of the
  four NEW checks the diagnostics "Must Detect."
- **Status:** **SUSPECTED weakness (intentional per scope note), PROVEN dormant.** The
  check fires only when a `curated_to_canonical` output (a) lacks `schema_version`,
  (b) has a `cp.schema_contract` for its `(domain,dataset)`, AND (c) has a *sibling*
  canonical output in the SAME workflow that DOES carry `schema_version`.
- **Evidence (queries run read-only):** across all 10 demo workflows, **no workflow
  satisfies (a)+(c) together**. The only two canonical outputs that carry
  `schema_version` are in `(insurance_dlq, claim_dlq)`, which has **no** `schema_contract`
  (fails (b) as a candidate and is the wrong dataset for the sibling test). The only
  contract-backed candidate datasets — `(insurance, claim)` — have **no** schema_version
  sibling anywhere in their workflows. Per-workflow feasibility check returned
  `BOTH=false` for every workflow → "schema_validation check CAN fire on demo: **False**."
- **Proof the predicate is NOT contradictory:** injecting a `schema_version` onto a
  sibling canonical output in workflow `2d00345a` (which has the `insurance/claim`
  candidate) made the check fire once, flagging the `insurance/claim` canonical link
  `10b304e3` — then rolled back. So the logic is correct; it simply has no live demo
  data that exercises it. The migration documents this as a deliberate "practical scope
  note" (`027_diagnostics.sql:416-431`).
- **Fix:** add a demo workflow that stamps `schema_version` on one canonical output of a
  contract-backed dataset and omits it on a sibling, so the check has a real positive
  example; or note in the spec that this detector is provably-firing-by-construction
  only. Audit-only.

### A5-5 (LOW) — 027 copy of `developer_diagnostics` ships the two bugs its own banner admits
- **Item:** `db/migrations/027_diagnostics.sql` lines 223 and 528-530.
- **Status:** CONFIRMED-as-superseded (no runtime impact — 029 wins). The applied 027
  body contains: (1) `input_edge_without_input_identifier` exempting **only**
  `'orchestrates'` (line 223), which would false-positive on a sanctioned
  `quarantine`/`replay` edge; and (2) `target_row_missing_ods_ids` with the contradictory
  predicate `t._ods_workflow_run_id = $1 AND (... t._ods_workflow_run_id IS NULL ...)`
  (lines 528-530), a check that can never return the null-workflow anomaly it claims to
  detect. The live function is the **029** redefinition (proven: `pg_proc` shows one
  `cp.developer_diagnostics`; live runs are clean and the fixed predicates fire when an
  anomaly is injected). Recorded because the 027 file remains in the applied migration
  history with the defective bodies as historical fact, exactly as its own banner
  (`027_diagnostics.sql:46-56`) warns.
- **Fix:** none needed (029 supersedes). Documented for completeness. Audit-only.

### A5-6 (LOW) — `v_provenance` emits the quarantine link multiple times (de-duped downstream)
- **Item:** `cp.v_provenance` / `db/migrations/022_…:236-302`.
- **Status:** CONFIRMED low-severity. Selecting the quarantine link from `cp.v_provenance`
  returned the **same row 6 times**. `dashboard_output_trace` masks this with
  `SELECT DISTINCT`, so the supported wrapper shows one row — but any caller reading
  `cp.v_provenance` directly (the runbook's "lower-level equivalents" pointer in §2) gets
  duplicate hops for a multi-edge link. Not a correctness bug in the dashboard functions;
  a rough edge in the underlying view.
- **Fix:** investigate the `v_provenance` join fan-out for links with multiple edges;
  no dashboard fix required. Audit-only.

---

## Verified GOOD (no finding — proves absence of the hunted bugs)

- **No diagnostic false positive on healthy demo.** `cp.developer_diagnostics(wf, NULL)`
  over all 10 committed workflows returned **0 rows** (clean). Target-row checks against
  `ods.policy_claim`/`ods.customer_transaction` likewise clean.
- **New 027/029 checks PROVEN to fire** (inject-then-rollback): `unfinished_stage`,
  `dlq_row_missing_trace_context`, and `quarantine_output_without_dlq_rows` each returned
  the expected anomaly when their condition was injected.
- **029 P2a fix verified.** A row with a valid link but null `_ods_workflow_run_id` is now
  attributable; total-orphan rows are caught table-wide by `target_row_orphan_no_ods_ids`.
- **All 8 functions exist with documented signatures** (`pg_proc` dump matches the
  runbook "Function reference" table exactly).
- **All 5 runbook questions execute** read-only and return the documented column shape:
  Q1 `dashboard_workflow_detail`+`developer_diagnostics`, Q2 `dashboard_output_trace`
  (reaches raw at terminal hop), Q3 `dashboard_target_row_trace` (reaches raw),
  Q4 `dashboard_airflow_lookup` (8 rows for a real dag_run), Q5 DLQ list/inspect.
  Only the Q5 *quarantine raw-trace* sub-claim is wrong (A5-1).
- **`dashboard_file_usage` vs `dashboard_file_impact` differ correctly** — file usage
  returns 1 direct ingest edge; file impact returns 6 downstream outputs for the same raw
  file (P2b downstream-closure view is real).
- **All control wrappers cited by `control-plane-write-contract.md` exist** —
  `register_file`, `start`, `finalise`, `patch`, `stage_scope`, `write_output_link`,
  `write_output_then_rows`, `reconcile_sink_link`, `reconcile_workflow`, `recon.write_check`,
  `visibility.activate`, `dlq.replay` — and the write-contract doc correctly notes the
  `stage_scope` (not `start`/`finish`) reality.
- **Dashboard:** `node --check` passes on `app.js` and `metadata-flow.js`. The `?data=`
  selector + header dropdown list all 3 snapshots; each fetches with `cache:no-store` and
  falls back to a "snapshot not found" message. All 3 snapshot JSONs carry the keys the
  JS reads (`executions/runs/links/tables/files/traces/scenario`). The DLQ snapshot has a
  `quarantine` link + a succeeded replay run (`replay_of_run_id` set), so `dlqSummary`
  computes `status="resolved"` and `DlqBanner` renders "resolved (replayed)"; the
  quarantine output renders as a red DLQ card (`app.js:3576-3584`).
- **No stale `|| old_name` leftover.** The id helpers
  `outputLinkId/inputEdgeId/upstreamOutputLinkId/rowOutputLinkId` (`app.js:2100-2114`)
  prefer the developer/view name (`output_link_id`) and fall back to the physical name
  (`lineage_link_id`). The committed snapshots emit **only** `lineage_link_id`, so the
  fallback is **load-bearing, not stale** — removing it would break every snapshot.
  This is correct defensive code.

---

## Method note
EXPECTED derived from the migration banners, the runbook/write-contract docs, and the
spec. Every CONFIRMED finding was proven by running the documented query read-only
against the committed demo data, or by `file:line` citation. Mutations used to prove a
check fires were issued inside an explicit transaction and `ROLLBACK`-ed; the connection
was opened `read_only` for all pure reads. `--drop`/apply was never run.
