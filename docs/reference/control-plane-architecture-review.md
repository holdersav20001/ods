# ODS Control-Plane — Architecture Review

**Date:** 2026-05-29
**Scope:** the `pipeline.*` control schema, its stored functions, the per-write-commit model, and the lineage model. Grounded in `db/migrations/*.sql`, `ods_ingestion_control/`, `ods_pipeline/`, and the glue jobs.

---

## 1. The concept in three lines

- **`dataset_config`** registers a dataset (YAML → DB); every job reads it to decide route, schema, write mode, target.
- **`file_catalogue` → `run_log` → `run_stage_log`** record *what physical file arrived*, *each task execution*, and *each stage within it*.
- **`lineage_link` (bundle) + `lineage_edge` (per input)** record provenance; every `ods.*` row carries `_ods_lineage_link_id`, so any row traces back to its raw file.

The defining design choice: a **stateless control plane** — each fact is committed on its own write (no cross-stage transaction, no 2PC), ordering is enforced by the Python call sequence, and a **janitor** reaps runs that die mid-flight.

---

## 2. Pros (what's genuinely good)

1. **Config-driven onboarding.** A new dataset is a YAML file synced into `dataset_config` — no code change (PARTY proved this: dataset + migration + config, zero job edits). `delivery`/`source_type`/`write_mode`/`key_fields`/`schema_id` fan out to every stage.
2. **Crash-tolerant by construction.** Per-write commits mean no long-held locks, no distributed transaction across Spark/docker steps that can each die independently. Every durable fact is immediately durable. This is the right instinct for the environment.
3. **Clean, uniform lineage model.** `lineage_link` 1:N `lineage_edge` handles single-source *and* multi-source merge with one shape (`input_slot` tags each contribution). Row-level provenance back to raw works end-to-end (verified live). This is the strongest part of the design.
4. **Idempotent writers.** `ON CONFLICT DO NOTHING` (start_run, register_file), `attempt_number` auto-increment, `FOR UPDATE SKIP LOCKED` on stage close — safe retries and concurrent runs.
5. **Honest separation of concerns.** Physical file (`file_catalogue`) vs execution (`run_log`) vs provenance (`lineage_*`) vs verification (`reconciliation_log`) are distinct tables with distinct lifecycles.
6. **Invariants centralized in the DB.** `control_require_text`, the `control_patch_run` key whitelist, discrepancy math — enforced in plpgsql regardless of caller language. `SECURITY DEFINER` + fixed `search_path`.
7. **Clean teardown.** `run_log → run_stage_log / lineage_link / lineage_edge` cascade on delete.

---

## 3. Cons / risks (the real weaknesses)

1. **The headline property is convention, not guarantee.** "Lineage rows exist before the consumer rows; a run reaches `succeeded` only after proof exists" is enforced *only by Python call ordering*. It is already violated: `ods_merge.py` commits target rows (`:215`) **before** committing the lineage bundle (`:341`); the earlier code review found the same inversion in `ods_postgres_write.py`. A crash in that window leaves `ods.*` rows whose `_ods_lineage_link_id` points at a `lineage_link` that does not exist. **The system's central promise — trustworthy lineage — is not actually guaranteed.**
2. **No referential integrity on the link.** `_ods_lineage_link_id` was added `NOT NULL` to every target table (migration 36 §5) but with **no FK** to `lineage_link`. The exact pointer forensics depend on is not enforced by the DB.
3. **Stored-function ↔ schema drift.** `control_record_run_event` still INSERTs `record_count_published`, a column **renamed away in migration 35** (`run_events`). It fires a runtime error on every run — silently swallowed (best-effort), so `run_events` rows are lost without anyone noticing. `CREATE OR REPLACE` across dozens of migrations makes this class of bug easy and invisible. No test catches it.
4. **Too many overlapping "state" surfaces.** `file_catalogue.state`, `file_processing_attempt.status`, `run_log.status`, `run_stage_log.status`, and `run_events.status` all describe progress. Reconciling them is constant mental overhead and a source of drift.
5. **`run_events` is a weak, redundant audit copy.** String `run_id` (no FK), duplicates `run_log` columns (file_id, s3 paths, counts, status), best-effort and silently lossy. Unclear it earns its keep next to `run_log`.
6. **Per-write commit = no logical atomicity.** Partial state is *normal* and correctness leans entirely on (a) the janitor reaping orphans and (b) idempotency being correct in every writer. Both are single points of systemic fragility.
7. **Two representations of run parentage.** `run_log.orchestrators` (JSONB `[{run_id, edge_type}]`) and `lineage_edge.upstream_run_id` both encode "which run fed this one."
8. **Business logic in plpgsql.** Whitelists, count math, discrepancy calc live in stored functions — harder to unit-test and version than app code, and they drift from the Python client (exactly what bit #3).

---

## 4. Simplifications (cut / merge)

1. **Drop or absorb `run_events`.** If the only consumer is the dashboard/audit, query `run_log` directly. Removes the entire class of bug #3 and one status surface (#4). If a Kafka-event mirror is genuinely needed, give it an FK and generate it from `run_log`, don't hand-maintain a parallel function.
2. **One file-state machine.** Merge `file_processing_attempt` into `file_catalogue.state`. The migration-34 rename already admitted the confusion — finish it.
3. **One status vocabulary.** Define a single canonical run/file lifecycle; derive or drop the duplicates. Today the same concept is spelled differently in 3–4 tables.
4. **Pick one parentage representation.** `lineage_edge.upstream_run_id` already subsumes `run_log.orchestrators` for provenance; keep `orchestrators` only if it carries orchestration-only edges that are deliberately *not* lineage (document that distinction or drop it).
5. **Split the stored functions by purpose.** Keep the *idempotent writers* as DB functions (where `ON CONFLICT` atomicity is the point: start_run, register_file, write_link). Move *pure validation* (patch whitelist, count math) into the Python client where it is testable — or commit fully to DB-side and **generate** the functions from one source so they cannot drift from the schema.

---

## 5. Improvements (prioritized)

**P0 — Make the invariant real, not conventional**
- Add the FK `_ods_lineage_link_id → pipeline.lineage_link(lineage_link_id)` (start `DEFERRABLE INITIALLY DEFERRED` if the writers need the rows in the same tx). This makes "link before rows" a DB law, not a comment.
- **And** fix write ordering everywhere: commit `write_link` **before** committing target rows in `ods_postgres_write.py` and `ods_merge.py`, mirroring the correct `finalising.py` order.
- Wire `runs.finalise()` (the existing invariant guard in `ods_pipeline/runs.py:160`) into **every** success path. Today it exists but the hot path calls `runs.update(status='succeeded')` directly, so the guard never runs.

**P0 — Fix the `run_events` drift**
- Reissue `control_record_run_event` to target `record_count_target` (new migration), or drop `run_events` per simplification #1.
- Add a **function-schema contract test**: after applying all migrations to a scratch DB, invoke every `control_*` function with representative args and assert it executes. This would have caught the drift the day migration 35 landed.

**P1**
- **SQL injection** in `ods_canonicalize_slot.py` (earlier review): regex-validate `business_date` (`^\d{4}-\d{2}-\d{2}$`) and allowlist/quote the staging table identifier.
- **Reconcile the state machines** (simplifications #2/#3).
- **Constrain `edge_type`.** ~11 free-text edge_type strings across the codebase invite typos. Make it a `CHECK` constraint or a lookup table so a bad edge_type fails at write time, not silently in a lineage walk.

**P2**
- **Date ingestion gap.** `inferSchema=true` infers date columns as INT96 timestamps; Spark's default `int96RebaseModeInWrite=EXCEPTION` breaks ingestion on pre-1900 dates. Set it to `CORRECTED` in the ingestion Spark session.
- **Ship the lineage views.** `v_lineage_link_sources` already flattens edges; add a documented top-down view (the `party_lineage_view.sql` shape) as a DB view for analysts/dashboard so nobody hand-writes the 3-hop walk.

---

## 6. Actionable bug list (with refs)

| ID | Severity | Where | Issue |
|----|----------|-------|-------|
| B1 | High | `33_control_table_functions.sql:1062` vs `35_clarity_renames.sql:56` | `control_record_run_event` writes dropped column `record_count_published`; silent `run_events` loss |
| B2 | High | `ods_merge.py:215` vs `:341` (and `ods_postgres_write.py` per earlier review) | Target rows committed before lineage_link → orphan-link crash window; inverts the invariant |
| B3 | Medium | migration 36 §5 / `38_party_direct_pg_target.sql:17` | No FK `_ods_lineage_link_id → lineage_link`; dangling provenance allowed |
| B4 | Medium | `ods_pipeline/runs.py:160` | `finalise()` invariant guard exists but is not on the success hot path |
| B5 | Critical | `ods_canonicalize_slot.py` (~205) | SQL injection via interpolated `business_date`/table name |
| B6 | Low | `glue/jobs/ingestion/reading.py:119` | `inferSchema` + default INT96 rebase breaks pre-1900 dates |

---

## 7. Bottom line

The shape is sound and the lineage model is a real asset — config-driven, crash-tolerant, row-level provenance that works. The gap between **stated** and **enforced** is the thing to fix: the control plane *promises* trustworthy lineage but enforces it by convention, and that convention is already broken in two writers and undermined by a missing FK and an un-wired guard. Close that gap (FK + ordering + guard + a function-schema test), collapse the redundant status/event surfaces, and the design is genuinely strong.
