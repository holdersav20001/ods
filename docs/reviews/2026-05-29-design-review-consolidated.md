# Control-Plane Design — Consolidated Review (4 lenses)

**Date:** 2026-05-29 · **Reviewers:** Software Architect, Senior Developer, Senior QA, Data-Lineage expert (parallel, adversarial, pre-build).
**Reviewed:** `docs/specs/2026-05-29-control-plane-design.md` + README + reference docs.

## Consolidated verdict

**Concept is sound — the schema shape and the id-for-grouping / edges-for-provenance split are the strong parts, and the redundancies from the old design are correctly cut. But the spec is NOT build-ready as written.** Do **not** start Phase 1 until the CRITICAL + HIGH items below are folded into a spec **v2**. Three issues are *expensive-after-build* (schema/atomicity/edge_type) and one is a *hard blocker* (no function signatures exist).

Per-lens bottom line: Architect = "build with HIGH+ changes"; Senior Dev = "not buildable as written (no signatures)"; QA = "not certifiable until failure/negative tests added"; Lineage = "not provably complete — 4 fixes first."

---

## CRITICAL (fix before any code)

**X1 — Link-before-rows is convention-only; a link with zero edges passes the FK.** *(Architect C2 + QA C1 + Lineage C1 — all three.)* The FK only rejects a dangling pointer at insert; it does **not** guarantee the link (and its edges) were committed first, and a `lineage_link` with zero `lineage_edge` rows is trace-to-raw failure that nothing catches.
→ **Resolution:** (a) `write_link` writes the link **+ all N edges in ONE transaction** (the one place atomicity is non-negotiable); (b) provide a single sanctioned sink primitive `write_link_then_rows(...)` — the only path allowed to stamp/commit target rows; (c) recon check **"every lineage_link has ≥1 edge"**; (d) test: abort between link-commit and row-commit → zero rows without a link.

**X2 — No function signatures exist in the authoritative spec; the cookbook (declared non-authoritative) carries the only concrete shapes and contradicts the new design.** *(Senior Dev C1/C2 — the #1 build blocker.)* Names clash (`write_lineage_link` vs `lineage.write_link`+`write_edge`; `start_run` vs `control_start_run`). You cannot write the migrations or client without inventing every signature.
→ **Resolution:** add a **function-signature appendix** to spec v2 (name, args+types, return, ON CONFLICT key, commit responsibility) for all `cp.*` functions; declare `write_lineage_link` supersedes per-edge `write_edge`; one verb set.

**X3 — `workflow_run_id NOT NULL` everywhere is unsafe.** *(Architect C1 + Senior Dev H1.)* Standalone/manual/replay/DLQ-drain runs have no Airflow `dag_run_id`; `NOT NULL` forces a synthesized value or breaks them — and a per-stage-minted id silently corrupts the grouping key the design depends on.
→ **Resolution:** client mints a documented synthetic id when none is supplied (`manual:<uuid>` / `replay:<orig_run_id>`); composer mints **once** and threads it (required arg to every stage, never defaulted per-stage). Keep `NOT NULL` on `run_log`; decide the format **now**.

**X4 — The fake-stage harness can bypass the logic under test / give false confidence.** *(Senior Dev H4 + QA C2.)* If fakes hand-build edges/link_ids, pass upstream ids directly, mint their own workflow_run_id, or insert target rows before the link commits, the tests validate the harness, not the client.
→ **Resolution:** pin **harness MUST-NOT rules** in the spec (no hand-built edges/ids; upstream only via discovery; one workflow_run_id from the composer; no row insert before link commit). Add one **adapter-contract test** (mock client, assert the real job's call-sequence: `write_link` before row-write) and an explicit "what fakes cannot prove" coverage-boundary section.

**X5 — Replayed rows can't be traced to raw.** *(Lineage C2.)* A replay writes only a `replay` edge to the *failed* run, which has no provenance chain — walking upstream dead-ends.
→ **Resolution:** a replay writes the **normal provenance chain** (`raw_to_curated`…) **plus** the `replay` annotation; add a test "trace-to-raw succeeds for a replayed row."

---

## HIGH

**H-edge — `edge_type`: use a lookup table, not CHECK; commit to `canonical_to_sink`; fan-out needs N links/run.** *(Architect H1/H2/H3 + Lineage H1.)* CHECK forces a migration per new sink; the taxonomy is already in flux. Fan-out to 2 sinks = two terminal links under one run, which the stated "1:1 with run" framing forbids.
→ `cp.edge_type` lookup table (+ `is_provenance` flag); commit to `canonical_to_sink` + a `sink_type` column **on `lineage_link`** (queryable "which rows went to Kafka"); drop "1:1 with run" — state **"1 link per write-event, N per run"**; add a fan-out test.

**H-dlq — DLQ is invisible to the lineage graph.** *(Lineage H3.)* Quarantined rows tie to `run_id` only; a provenance walk sees the happy path only.
→ Add `edge_type='quarantine'` (target_ref = DLQ payload) so the graph is rows-complete.

**H-recon — recon tests pass vacuously; negatives + fan-out untested.** *(QA C3 + Lineage H1.)* `source==good+dlq` fed self-consistent fakes proves nothing.
→ Add: `good+dlq<source` → breach; `>source` → double-count; "every link ≥1 edge"; fan-out `child.record_count==parent.record_count` per sink link.

**H-trigger — how does a run record an orchestration *trigger* edge now that `orchestrators` is gone?** *(Senior Dev C3 + Architect M3 + Lineage L1.)* `lineage_edge` expects `upstream_run_id`/`source_file_id`; a trigger has neither, and `orchestrates` edges pollute trace-to-raw.
→ Specify how `start_run` records a trigger; mark orchestration edges `is_provenance=false`; ship the trace view filtering them.

**H-discovery — `latest_succeeded_run` signature + business_date are undefined but load-bearing.** *(Senior Dev H2/H3 + QA L2.)* The re-run test only works if canon *discovers* (ignores passed id). business_date must be required `date` everywhere (the old `=None` bug).
→ Define `latest_succeeded_run(domain,dataset,business_date,pipeline_type)` + tie-break; forbid harness passing upstream into the link path; `business_date` required typed everywhere.

**H-contract — contract test must be exhaustive, not "executes."** *(QA H4 + Senior Dev M4.)* Enumerate functions dynamically from `pg_proc` (schema `cp`), fail if any is un-asserted, and assert returned columns vs the table — else it won't catch the next drift (the `run_events` class).

**H-fk — FK tests are one-directional.** *(QA H3.)* Test dangling rejection on all FKs (consumer/upstream/source_file/link/dlq.run/dlq.replay), not just the target row.

---

## MEDIUM

- **Drop `consumer_run_id` from `lineage_edge`** — derive via `lineage_link_id` FK (one source of truth). *(Architect M1.)*
- **Mutable `target_ref`** — re-run overwrites canonical parquet, so an old link points at different bytes. Add a content hash/version token to `target_ref`. *(Lineage M2.)*
- **Idempotent replay** — replay twice → stable row + link counts; original recon unchanged. *(QA H1.)*
- **Migration runner unspecified** — pick ordered `NNN_*.sql` + a tiny psql runner; document the apply-to-`ods_cp` step. *(Senior Dev M1.)*
- **Merge recon can't fit scalar `source_count`** (per-slot) — store per-slot in `metrics` JSON. *(Senior Dev M5.)*
- **N-edge merge test** — assert distinct `input_slot`, distinct non-null `upstream_run_id`, `SUM(edge.record_count)==link.record_count`. *(QA M1.)*
- **Concurrency test** — two concurrent writers under one workflow_run_id; no lost/dup links. *(QA M3.)*
- **Transform as provenance** — promote `transform_version` to a first-class column on the canonical link; treat silent cast-to-NULL as a DQ/quarantine event, not a silent mutation. *(Lineage M1.)*
- **Merge readiness** — explicitly out-of-scope (orchestrator's job); `fake_merge` asserts caller-supplied slots. *(Senior Dev H5.)*

## LOW
NOT-NULL rejection test for `workflow_run_id` *(QA H2)*; `edge_type` bad-value rejection test *(QA M4)*; DLQ `replayed_at`/`replay_run_id` bookkeeping + double-drain guard *(QA M5)*; partial/failed-midway orphan detection *(QA L1)*; pin `file_id` UUID end-to-end + jsonb types for config *(Senior Dev L2/L3)*; pytest isolation (transactional rollback per test) *(Senior Dev L4)*; verify PG port 5440 mapping *(Senior Dev L1)*; OpenLineage interop as a later secondary feed *(Lineage M2)*.

---

## Decisions the user should make (the genuine forks)

1. **`workflow_run_id` synthetic-id format** for non-Airflow runs (`manual:<uuid>` / `replay:<orig>`)? *(X3)*
2. **`canonical_to_sink` + `sink_type` on the link** (recommended) vs keep `canonical_to_postgres` for now? *(H-edge)*
3. **DLQ as a `quarantine` lineage edge** (rows-complete graph) — in, or keep DLQ separate from the graph? *(H-dlq)*
4. **Content hash/version on `target_ref`** to defeat overwrite-mutability — in scope now, or defer? *(Lineage M2)*

## Recommended next step
**New session's FIRST task = revise the spec to v2** incorporating CRITICAL + HIGH (with the 4 decisions above resolved), THEN start Phase 1 (migrations + functions + exhaustive contract test). The schema, atomicity, edge_type taxonomy, and workflow_run_id format are all cheap to fix in v2 and expensive to change after tables exist.
