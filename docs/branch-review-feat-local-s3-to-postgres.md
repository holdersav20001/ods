# Branch Review — `feat/local-s3-to-postgres`

**Date:** 2026-05-02
**Scope:** `main..HEAD` — 52 files, ~6,213 insertions
**Reviewers:** Code Reviewer, Software Architect, SRE, Test Analyst (deep), Test Analyst (gap)

---

## Executive Summary

Branch ships substantial control-plane refactor (`ods_pipeline/*`), risk canonicalization pipeline, lineage contract, file-level rollback, dual-sink history pattern, and ~5k LOC of new tests. Architecture direction is sound; control-plane extraction is clean. **However, 8 blocker-level issues prevent safe merge**: SQL identifier injection (×2), lost-update race, non-atomic multi-row writes, non-deterministic publish reconciliation, no Kafka transactional producer, DLQ helper-only (no writer/retry), no exactly-once semantics. Test suite is broad but shallow — multiple façade-only tests and zero CI workflows committed.

---

## 1. Blockers (must fix pre-merge)

| # | File:Line | Issue | Fix |
|---|-----------|-------|-----|
| B1 | `glue/jobs/utils.py:59-67` | SQL identifier injection in `write_job_log` — `cols` joined into f-string, no whitelist | `psycopg2.sql.Identifier` + whitelist |
| B2 | `ods_pipeline/runs.py:122-135` | Same identifier risk in `runs.update`. Whitelist exists but no `Identifier` quoting | `Identifier` + `c.isidentifier()` assert |
| B3 | `ods_pipeline/stages.py:104-155` | Lost-update race — `UPDATE … WHERE id=(SELECT … LIMIT 1)` no `FOR UPDATE`. Append-fallback in separate tx | `SELECT FOR UPDATE SKIP LOCKED` + single tx |
| B4 | `ods_pipeline/messages.py:123-208` | `record_result` writes 6+ rows non-atomically. Multiple `commit()` mid-flow | Pass explicit tx OR `ON CONFLICT DO NOTHING` on `(run_id, stage, attempt, event_type)` |
| B5 | `glue/jobs/ods_s3_publish.py:393-468` | T0 recon `published = end-start` non-deterministic with concurrent producers | Track delivered offsets per partition via `_deliver_cb` |
| B6 | `glue/jobs/ods_s3_publish.py:384-452` | No transactional Kafka producer + no `producer.close()`. Retry duplicates downstream | `transactional.id` per `(run_id, partition)`, `init/begin/commit_transaction`, `close()` in finally |
| B7 | `ods_pipeline/dlq.py` | DLQ is helper only — no writer, retry, backoff, max-attempts. Connect sinks lack `errors.tolerance`/`deadletterqueue` | Implement DLQ writer + Connect `errors.tolerance=all`, `errors.deadletterqueue.topic.name=ods.dlq.<sink>`, `errors.retry.timeout=600000` |
| B8 | `ods_pipeline/offsets.py` + publish | No exactly-once. `offsets.py` only records ranges, never drives commits | Idempotent + transactional producer scoped per `run_id`; persist last-committed offset before claiming success |

---

## 2. High-Severity Findings

### Code Quality

- **`set_file_state` ON CONFLICT clobbers terminal state** (`utils.py:70-91`). Stale `processing` write overwrites `completed`/`failed`. Combined with `current_state == "completed"` short-circuit (`publish.py:259`), idempotency claim of step 2 evaporates under concurrency.
  *Fix:* `ON CONFLICT DO UPDATE … WHERE pipeline.file_state.status NOT IN ('completed','failed')`
- **`runs.start` idempotency racy** (`ods_pipeline/runs.py:54-103`) — INSERT-then-SELECT window. Use `RETURNING xmax = 0 AS inserted` for atomic insert detection
- **`ods_s3_publish` business_date file_id fallback wrong file** (`ods_s3_publish.py:316-329`) — `ORDER BY first_seen_at DESC LIMIT 1` mis-attaches lineage on re-delivery. Make `--file_id` mandatory
- **AWS credentials default `"test"` outside local** (`ods_s3_publish.py:54-62`) — silently authenticates as `test` in non-local envs. DSN strings leak to Airflow logs via broad re-raise

### Reliability

- **Schema Registry SPOF** — no HA, no client cache TTL, no `auto.register=false`. Outage = full pipeline stop, silent DLQ growth
- **Run-event producer fire-and-forget** (`run_event_producer.py`) — SR down → lineage silently disappears from Kafka while Postgres ledger advances. Buffer-to-disk + outbox replay
- **Glue OOM has no checkpointing** — partition replays from offset 0 (ties back to B6)
- **Reconciliation no tolerance band** — single dup triggers MISMATCH; no Connect lag check before recon. Add `tolerance_pct` (e.g. 0.01%); poll Connect REST + consumer-group lag with timeout
- **Stage ledger writes not transactional with side-effects** (`stages.py`) — produce → ledger write in separate connections. Crash between = ghost duplicates. Outbox pattern required

---

## 3. Architecture Review

### Strengths

1. **Clean control-plane extraction** — `ods_pipeline/*` consolidates 3 prior copies of upsert SQL behind typed `Stage`/`StageEvent` API
2. **Normalised `lineage_edge`** — file-scoped, multi-parent, column-aware. Replaces date-scoped joins
3. **Logical rollback via `state` + view** — single source of truth, O(1) reversible. Avoids soft-delete-flag anti-pattern
4. **Reconciliation as first-class stage** — visible in same model as functional stages, auditable
5. **Declarative canonicalize** — `canonicalize.py` (pure, lazy) + `ods_canonicalize.py` (runtime) cleanly separate mapping from execution. Templatable

### Top 5 Architectural Risks

| # | Risk | Mitigation |
|---|------|------------|
| A1 | Boundary leak — `matches_context` correlation duplicated in `canonicalize.py`. CDC/Event patterns will silently drop records (no `file_id`/`run_id`) | Move correlation predicates into `ods_pipeline.messages` keyed by pattern type |
| A2 | Dual-sink-on-single-topic — history sink lag → current state without matching history row. No cross-sink parity check | Add `reconciliation.write_check(check_type='dual_sink_parity')` comparing `MAX(_ods_offset)` per partition across both targets |
| A3 | Lineage contract has no closure invariant | Finalise-time check: fail run if `published > 0 AND no lineage_edge`, or open stages exist |
| A4 | Risk pipeline coherence by convention only — fork risk before pattern #2 (CDC/API/Event) | **Build `ods_pipeline.patterns.IngestionPattern` template now** (name, stages[], topics[], sinks[], recon_checks[]) |
| A5 | Rollback views invisible to Kafka consumers — canonical topic still has bad rows | Emit supersede event on `pipeline.file_state` topic; consumers filter |

### Refactor Priority
- **P1:** `ods_pipeline.patterns` module (blocks pattern #2)
- **P1:** Dual-sink parity recon check
- **P2:** Lineage closure invariant in `runs.finalise()`
- **P2:** Centralise correlation predicates
- **P3:** Supersede event topic

---

## 4. SRE Review

### Failure Modes Not Covered
- Kafka broker down → no graceful degradation
- Postgres down → no retry on `runs.finish`
- S3 throttle → no backoff
- Glue OOM → no checkpoint resume
- Connect sink lag → recon races
- Schema Registry unavailable → silent DLQ growth, no fallback

### Observability Gaps
- Grep for `prometheus|statsd|alert|burn` returns nothing
- `scripts/ops_control_dashboard.py` (untracked) = Postgres viewer, not alerting
- No PagerDuty hook, no thresholds, no runbooks per stage

### Candidate SLOs (30-day window)

| SLO | SLI | Target | Burn alert |
|---|---|---|---|
| Ingest freshness | `count(file_published_within_15m_of_arrival) / count(files)` | 99.5% | 14.4× over 5m/1h |
| Publish completeness | `count(runs where reconciliation=MATCH) / count(terminal runs)` | 99.9% | 6× over 30m/6h |
| Sink convergence | `count(messages applied to PG within 5m of Kafka commit) / count(messages)` | 99.5% | 14.4× over 5m/1h |

---

## 5. Test Suite Review

### Inventory
27 test files, 5,082 LOC. Unit: 15. Integration: 12.

### Broken / Dead Tests
- `tests/dags/test_dag_integrity.py` — imports `dag2_etl_trigger`, `dag_publish` (deleted modules)
- `tests/unit/test_t0_check.py:16` — fixture broken: `run_id = ANY(%s)` uuid=text. Tests likely erroring silently
- `tests/unit/test_dq.py` — `pytest.importorskip("pyspark")` → silent skip in CI
- `tests/integration/test_write_modes.py` — 2 intermittent failures (`test_run_events_contain_file_id`, `test_lineage_edges_written`)

### Façade-Only Tests (false confidence)
- `test_recon_t2.py` — monkeypatches `_reconcile_*` to lambdas, asserts call order only. Zero SQL exercised
- `test_connect_admin_offsets.py` — monkeypatches `get_offsets`, never tests HTTP layer
- `test_dlq.py`, `test_offsets.py`, `test_message_api_control_plane.py` — pure-function smoke only

### Coverage Map (worst gaps)

| Module | % | Gap |
|--------|---|-----|
| `ods_pipeline/events.py` | 0% | all |
| `airflow/dags/dag_ingest.py` | 0% | every branch (no integrity test) |
| `airflow/dags/dag_drop_to_raw,multi_file.py` | 0% | all |
| `glue/jobs/ods_merge,stage,dq.py` | 5% | all (dq via skipped test) |
| `glue/jobs/ods_canonicalize.py` (448 LOC) | 15% | DLQ, registry, write path |
| `airflow/dags/dag_recon_t2.py` | 20% | façade only — real SQL unverified |
| `ods_pipeline/messages.py` | 25% | record_* SQL, reconcile_batch |
| `glue/jobs/ods_s3_publish.py` | 25% | producer error handling |
| `ods_pipeline/files.py` | 30% | state machine, md5 collision |

### Infrastructure Issues
- `conftest.py` = 15 lines, session pg, **no isolation/truncation/rollback**
- Hardcoded ports/hosts: `5440`, `2222`, `8083`, `8081`, container `avivaods-broker-1`
- 11 integration files duplicate `pg_conn` fixture ≥6×
- No `pytest.ini`/`pyproject.toml` test config — markers undefined
- **Zero CI workflows committed** (no `.github/workflows/`)
- Flake: `time.sleep(1)` polling 60s, docker-exec shellouts racing consumer
- Order-dependent: `test_write_modes.py` day1/day2 share DB state

### Top 10 Tests To Add

| # | File | Assert | Why |
|---|------|--------|-----|
| 1 | `tests/unit/test_offsets_property.py` | Hypothesis: `delta_count >= 0`, range invariants | Free coverage of reset/empty/giant |
| 2 | `tests/unit/test_runs_validation.py` | Garbage field rejected; double-finish idempotent; ended_at not on non-terminal | Zero negative coverage today |
| 3 | `tests/unit/test_stages_upsert.py` | COALESCE upsert verified; new attempt = new row | `stages.py:116` unverified |
| 4 | `tests/unit/test_files_state_machine.py` | Allowed transitions; bad state raises; idempotent set_state | `files.py` 0% unit coverage |
| 5 | `tests/unit/test_avro_compat.py` | fastavro BACKWARD compat vs fixture-snapshot | Guards canonical contract |
| 6 | `tests/unit/test_jdbc_sink_alignment.py` | Diff connector json `pk.fields`/`fields.whitelist` vs migration DDL | Sink↔DDL drift bug class |
| 7 | `tests/unit/test_dag_recon_t2_sql.py` | Real SQL via `pytest-postgresql` | Replaces façade-only test |
| 8 | `tests/unit/test_register_schemas_idempotent.py` | `responses` fake SR; 409 idempotency + 5xx bubble | `register_schemas.py` mostly untested |
| 9 | `tests/integration/test_failure_injection.py` | toxiproxy drop SR/PG/Kafka mid-run; assert failed + DLQ | No failure-injection coverage |
| 10 | `tests/unit/test_dlq_pii_redaction.py` | Property: ssn/password/token never verbatim in DLQ | Security regression guard |

### Infrastructure Refactors
1. New `tests/integration/conftest.py` — consolidate fixtures, **per-test schema** via `pytest-postgresql`
2. `tests/_helpers.py` — single `wait_for(predicate, timeout, interval)`. Kill all `time.sleep` polling
3. **testcontainers-python** for Kafka+SR+PG. Drop docker-exec shellouts
4. Fake SR via `responses`/`pytest-httpserver` for unit-tier
5. Fix `test_t0_check.py` uuid cast (`run_id::uuid = ANY(%s::uuid[])`)
6. Delete or rewrite `test_dag_integrity.py` to current DAGs
7. `pyproject.toml [tool.pytest.ini_options]`: markers `unit/integration/slow/requires_spark`, `--strict-markers`, default `-m "not integration and not requires_spark"`

### CI Workflows (all missing)
- `.github/workflows/unit.yml` — every PR, no docker
- `.github/workflows/integration.yml` — main + `[ci-int]` label
- `.github/workflows/schema-compat.yml` — Avro compat
- `.github/workflows/migrations.yml` — fresh PG + test_migrations

### Coverage + Gates
- `pytest-cov` (installed) → target ≥75% line on `ods_pipeline/*`. Fail PR if module drops >2pp
- pre-commit: `ruff`, `black`, `sqlfluff` on migrations, `yamllint` on connectors, `avro-tools validate`
- CODEOWNERS gate: any new `ods_pipeline/*` fn requires test

---

## 6. Missing Test Categories

| Category | Status |
|----------|--------|
| Property-based (Hypothesis) | None — offsets/dlq/canonicalize are ideal |
| Contract (Avro compat, sink↔DDL) | None |
| Chaos / fault injection | None |
| Performance baselines | None |
| Concurrency (md5 race, double-start) | None |
| Security (SQL injection, secret leakage) | None |

---

## 7. Recommended Execution Order

| Phase | Work | Why |
|-------|------|-----|
| **0 — Pre-merge** | Fix B1–B8 blockers | Security + correctness gate |
| **1 — Stabilise tests** | Fix `test_t0_check` fixture, delete dead `test_dag_integrity`, add `pyproject.toml` markers, conftest consolidation | Restore signal integrity |
| **2 — CI** | Add unit + integration workflows | Stop relying on green-on-laptop |
| **3 — High-ROI tests** | Tests #1, #5, #6 (property + contract) | Catch drift, free coverage |
| **4 — Reliability** | Connect `errors.tolerance` + DLQ writer + transactional producer (B7/B8) | Closes data-loss/dup bug class |
| **5 — Architecture** | `ods_pipeline.patterns` template (A4) | Unblocks pattern #2 (CDC) |
| **6 — Observability** | Prometheus metrics + 3 SLOs | Get on-call signal |
| **7 — Failure injection** | toxiproxy tier (test #9) | Validate B7/B8 + recon |
| **8 — Cross-sink parity** | Dual-sink recon check (A2) | Closes silent divergence |

---

## 8. Operator Tooling & Platform Roadmap (peer review, verified)

Independent reviewer flagged operator-facing gaps. Claims verified against branch state on 2026-05-02:

| Claim | Verified | Evidence |
|-------|----------|----------|
| No pytest markers / fast subset | ✓ | No `pyproject.toml` test config; markers undefined (matches §5) |
| DLQ contract present | ✓ | `ods_pipeline/dlq.py` exists |
| Offsets in stage metrics JSON, no normalised table | ✓ | `offsets.py` is helpers only; grep `run_kafka_offsets` → 0 hits |
| No replay/rerun CLI | ✓ | grep `replay\|rerun` in `ods_pipeline/` → none. Only `scripts/capture_evidence.py` |
| No OpenLineage proof | ✓ | grep → only docs (`architecture-decision-pack.md`, `plans/2026-04-15-data-lineage.md`); no listener code |
| Reconciliation dashboard thin | ✓ | `scripts/ops_control_dashboard.py` + `pipeline_dashboard.py` + `lineage_viewer.py` exist but not exposing T0/T1/T2 trends, DLQ-adjusted counts, "what to rerun" hints |

### 8.1 Pytest markers + faster CI subsets [P1, links §5]
Add `pyproject.toml [tool.pytest.ini_options]` markers: `unit, integration, e2e, slow, negative`. Default `-m "not slow and not e2e"` for daily; separate `slow`/`e2e` jobs in CI. Already covered in §5 — promote to highest priority for dev velocity.

### 8.2 DLQ dashboard + replay tooling [P1]
Operators need:
- DLQ counts by `domain/dataset/run_id/stage`
- Latest errors with payload preview
- Selective replay after fix
- Replay attempts recorded in `run_log` + `lineage_edge` (parent_run_id = original failed run, edge_type = `replay`)

**Build:** extend `scripts/ops_control_dashboard.py` with DLQ panel; new `python -m ods_pipeline.ops dlq {list|show|replay}` CLI. Replay must reuse existing `runs.start` + `lineage.write_edge` so evidence chain stays intact.

### 8.3 Normalised Kafka offset table [P1]
Today: offset ranges live inside `run_stage_log.metrics` JSONB. Hard to query, no index, can't aggregate.

**Add:** `db/migrations/19_run_kafka_offsets.sql`
```sql
CREATE TABLE pipeline.run_kafka_offsets (
  run_id        uuid    NOT NULL REFERENCES pipeline.run_log(run_id),
  stage         text    NOT NULL,
  topic         text    NOT NULL,
  partition     int     NOT NULL,
  offset_start  bigint  NOT NULL,
  offset_end    bigint  NOT NULL,
  record_count  bigint  GENERATED ALWAYS AS (offset_end - offset_start) STORED,
  recorded_at   timestamptz DEFAULT now(),
  PRIMARY KEY (run_id, stage, topic, partition)
);
CREATE INDEX ON pipeline.run_kafka_offsets (topic, partition, recorded_at DESC);
```
Backfill writer in `ods_pipeline.offsets.persist_ranges(conn, run_id, stage, topic, ranges)`. Keep JSONB as transitional; deprecate after 2 sprints. **Synergy with B5/B6** — easier to verify exactly-once when offsets are queryable.

### 8.4 Message/API demo pipeline [P2]
Build small FastAPI endpoint that proves message/API pattern parallel to file-based risk:
- POST `/events` → write `pipeline.file_catalogue` row (synthetic file_id), archive payload to S3 JSONL, publish raw Kafka, canonicalize via existing `ods_canonicalize.py`, write recon
- Reuses `ods_pipeline.runs/stages/messages/lineage` — proves boundary cleanliness (links **A1** in §3)
- Becomes first concrete instance of the **`ods_pipeline.patterns.IngestionPattern`** template (A4)

**Path:** `services/event_api/main.py` + `tests/integration/test_event_api_e2e.py`

### 8.5 Reconciliation dashboard improvements [P2]
Add SQL views:
- `ods.v_recon_latest_failed` — most recent failed check per `(domain, dataset)`
- `ods.v_recon_t0_t1_t2_trend` — daily counts MATCH/MISMATCH per check_type
- `ods.v_recon_dlq_adjusted` — `record_count_published + dlq_count = record_count_source`?
- `ods.v_current_history_consistency` — current row count vs latest history row per business_date
- `ods.v_rerun_candidates` — runs in `failed` whose upstream parents are `succeeded` (safe to rerun)

Wire into `scripts/ops_control_dashboard.py`.

### 8.6 Run restart/replay CLI [P2, depends on 8.2 + 8.5]
```
python -m ods_pipeline.ops replay --file-id <uuid>
python -m ods_pipeline.ops rerun --run-id <uuid>
```
Behaviour:
- Look up original run, dataset config, DAG entry point
- Trigger Airflow DAG via REST with original parameters
- Create new `run_log` row, link via `lineage_edge(parent_run_id=<old>, edge_type='replay')`
- Preserve old run's evidence — never mutate failed records

### 8.7 OpenLineage / Spark listener proof [P3]
Add example: `glue/jobs/listeners/openlineage_capture.py` + sample capture file in `docs/evidence/openlineage_sample.json`. Map to existing `lineage_edge` / `run_stage_log` columns. **Decision:** keep ODS tables as business truth, OpenLineage as supplementary observability. Document mapping in `docs/openlineage-mapping.md`.

### 8.8 History/current reconciliation hardening [P3, depends on 8.5]
Beyond counts:
- Generate expected latest state from history (`SELECT DISTINCT ON (key) … ORDER BY business_date DESC`)
- Row-value comparison vs current table (not just count match)
- Mismatch samples written to `s3://<bucket>/recon/mismatches/<run_id>/`
- Surface mismatch keys in dashboard with drill-through

**Implementation:** new `ods_pipeline.reconciliation.compare_history_vs_current(conn, dataset, business_date)`.

### Recommended Sequence (peer-reviewer order, retained)
**Do first: 8.1, 8.2, 8.3.** Highest immediate operator value, both file + message/API patterns benefit, no large new moving parts.
- **8.1** unblocks fast PR feedback loop
- **8.2** turns DLQ from helper into operational surface
- **8.3** makes B5/B6 exactly-once work verifiable

Then **8.4** to validate pattern template (A4), **8.5/8.6** to close the operator loop, **8.7/8.8** as enhancement.

### Updated Phase Plan (merge with §7)
| Phase | Add |
|-------|-----|
| 1 — Stabilise tests | + pytest markers (8.1) |
| 2 — CI | (unchanged) |
| 3 — High-ROI tests | (unchanged) |
| **3.5 — Operator unblock** | **DLQ tooling (8.2) + offset table (8.3)** |
| 4 — Reliability | (unchanged) — leverages 8.3 for verification |
| 5 — Architecture | + Message/API demo (8.4) as first `IngestionPattern` instance |
| 6 — Observability | + Recon views (8.5) + replay CLI (8.6) |
| 7 — Failure injection | (unchanged) |
| 8 — Cross-sink parity | (unchanged) |
| **9 — Lineage enhancement** | **OpenLineage proof (8.7) + history/current row-value recon (8.8)** |

---

## 9. Files Referenced

**Production**
- `ods_pipeline/{dlq,messages,offsets,reconciliation,runs,stages,files,lineage,models,_db,events,metadata}.py`
- `glue/jobs/{ods_ingestion,ods_s3_publish,ods_canonicalize,canonicalize,utils,ods_merge,ods_stage,dq}.py`
- `airflow/dags/{dag_ingest,dag_recon_t2,dag_drop_to_raw,dag_multi_file,dag_config_sync}.py`
- `airflow/dags/common/{run_event_producer,connect_admin,kafka_admin,connector_provisioner,recon,yaml_loader}.py`
- `schemas/insurance/risk_*.avsc`, `scripts/register_schemas.py`
- `db/migrations/*` (14 files)
- `docker/connect-config/jdbc-sink-*.json`

**Tests** — all 27 files under `tests/`

**Docs**
- `docs/architecture-decision-pack.md`
- `docs/non-canonical-canonical-pipeline.md`
- `docs/file-id-reconciliation-and-lineage-options.md`
- `patterns/*.docx` (uncommitted)
