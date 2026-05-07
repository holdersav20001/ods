# P0/P1/P2 Implementation Plan — `feat/local-s3-to-postgres`

> **For agentic workers:** Use `superpowers:subagent-driven-development`. Each task gated by 3-agent consensus (Senior Dev implements, Adversarial Reviewer + Architect must both ack before next task starts). Steps use checkbox `- [ ]` syntax.

**Goal:** Land all P0 (security/correctness blockers), P1 (architecture + operator unblock), and P2 (platform completeness) items from `docs/branch-review-feat-local-s3-to-postgres.md`.

**Architecture:** Iterative, dependency-ordered, single shared branch (`feat/local-s3-to-postgres`). Each task closes one identified risk. Tests written first per TDD where practical; production code follows.

**Tech Stack:** Python 3.11, psycopg2, confluent-kafka, Avro, FastAPI, pytest, pytest-postgresql, Hypothesis, testcontainers, Postgres 15, Kafka, Schema Registry, Airflow.

**Consensus Rule:** Every task ends with PR-style checkpoint. Senior Dev posts diff + test output. Adversarial Reviewer challenges (security, correctness, edge cases). Architect validates fit (boundaries, contracts, future patterns). All three must ack with the literal token `ACK` before next task starts. Disagreement → revise → re-review.

**Branch:** `feat/local-s3-to-postgres` (current). Commits per task with conventional commit prefix.

---

## Dependency Graph

```
P0 chain (security/correctness, blocks everything):
  T1 (B1+B2 SQL identifier injection)
  T2 (B3 lost-update race in stages)
  T3 (B4 atomic record_result)
  T4 (B5+B6 transactional Kafka producer + per-msg offset tracking)
  T5 (B7 DLQ writer + Connect errors.tolerance)
  T6 (B8 exactly-once offset commit)

P1 chain (depends on P0 done):
  T7  (8.1 pytest markers + pyproject.toml)
  T8  (8.3 normalised run_kafka_offsets table) [depends T4, T6]
  T9  (8.2 DLQ tooling: list/show/replay) [depends T5]
  T10 (A1 centralise correlation predicates)
  T11 (A2 dual-sink parity recon check) [depends T8]
  T12 (A4 ods_pipeline.patterns.IngestionPattern template) [depends T10]

P2 chain (depends on P1 done):
  T13 (A3 lineage closure invariant)
  T14 (8.5 reconciliation dashboard SQL views) [depends T11]
  T15 (8.6 replay/rerun CLI) [depends T9, T13]
  T16 (8.4 Message/API demo pipeline) [depends T12]
```

---

# PART A — P0 (Security & Correctness Blockers)

## Task 1 — Fix SQL identifier injection (B1, B2)

**Files:**
- Modify: `glue/jobs/utils.py:59-67` (`write_job_log`)
- Modify: `ods_pipeline/runs.py:122-135` (`update`)
- Create: `tests/unit/test_sql_injection_guard.py`

**Acceptance:**
- All dynamic identifiers use `psycopg2.sql.Identifier`
- Whitelist enforced via `ALLOWED_*_FIELDS` set; unknown keys raise `ValueError`
- Negative tests cover: keys with quotes, semicolons, spaces, sql keywords
- No regression — existing `test_run_log_helpers.py` passes

- [ ] **Step 1.1:** Write `tests/unit/test_sql_injection_guard.py` with cases:
  - `runs.update(conn, rid, **{"value); DROP TABLE pipeline.run_log; --": 1})` raises `ValueError`
  - `write_job_log(conn, **{"foo bar": 1})` raises `ValueError`
  - Whitelisted fields work as before
- [ ] **Step 1.2:** Run new test, confirm FAIL
- [ ] **Step 1.3:** Refactor `runs.update` — use `sql.SQL("UPDATE pipeline.run_log SET {sets} WHERE run_id=%s").format(sets=sql.SQL(', ').join(sql.Composed([sql.Identifier(c), sql.SQL('=%s')]) for c in cols))`. Assert each `c.isidentifier() and c in ALLOWED_RUN_FIELDS`
- [ ] **Step 1.4:** Apply same pattern to `write_job_log`. Add `ALLOWED_JOB_LOG_FIELDS` constant
- [ ] **Step 1.5:** Run full unit suite, confirm green
- [ ] **Step 1.6:** Commit: `fix(security): parameterise dynamic identifiers in runs.update + write_job_log (B1, B2)`
- [ ] **Step 1.7:** **CONSENSUS GATE** — post diff + test output. Wait for Reviewer + Architect ACK.

---

## Task 2 — Fix lost-update race in stages.finish (B3)

**Files:**
- Modify: `ods_pipeline/stages.py:104-155` (`finish`)
- Create: `tests/unit/test_stages_concurrency.py`

**Acceptance:**
- `SELECT … FOR UPDATE SKIP LOCKED` used to claim row
- UPDATE + fallback INSERT in single transaction (single `conn.commit()` at end)
- Concurrent `finish` calls from two connections → exactly one wins, the other appends
- No silent fallthrough where neither branch executes

- [ ] **Step 2.1:** Write concurrency test using two `psycopg2.connect()` connections + threading. Assert: after both call `finish` for same `(run_id, stage, attempt)`, row count is exactly 2 (one updated, one appended) AND no row is missing terminal status
- [ ] **Step 2.2:** Run test, confirm FAIL (current code may double-update or lose one)
- [ ] **Step 2.3:** Refactor `finish` — single tx: `BEGIN`, `SELECT … FOR UPDATE SKIP LOCKED`, conditional UPDATE-or-INSERT, `COMMIT`. Remove intermediate `conn.commit()`
- [ ] **Step 2.4:** Run test, confirm PASS. Run full unit suite
- [ ] **Step 2.5:** Commit: `fix(stages): single-tx FOR UPDATE SKIP LOCKED in finish (B3)`
- [ ] **Step 2.6:** **CONSENSUS GATE**

---

## Task 3 — Atomic record_result (B4)

**Files:**
- Modify: `ods_pipeline/messages.py:123-208` (`record_result`)
- Modify: callers of `record_result` (`grep -rn record_result`)
- Create: `tests/unit/test_messages_atomicity.py`

**Acceptance:**
- `record_result` accepts an externally-managed connection and **does not commit**
- Caller wraps the entire flow in single tx; commits once at end
- On simulated mid-flow exception (raise after stage write, before recon write), DB has neither stage nor recon row (rollback)
- Stage rows have `ON CONFLICT (run_id, stage, attempt_number, event_type) DO NOTHING` for idempotent retries

- [ ] **Step 3.1:** Add migration `db/migrations/19_stage_idempotency.sql` adding the unique constraint
- [ ] **Step 3.2:** Write atomicity test: monkeypatch `reconciliation.write_check` to raise after `stages.write`. Assert no partial rows
- [ ] **Step 3.3:** Refactor `record_result` to remove internal `conn.commit()` calls; require caller to commit. Update docstring
- [ ] **Step 3.4:** Update all callers (Glue jobs, DAG tasks) to use `with conn: ...` context for transactional commit/rollback
- [ ] **Step 3.5:** Add `ON CONFLICT DO NOTHING` to stage INSERT
- [ ] **Step 3.6:** Run integration tests `test_run_events.py`, `test_negative_paths.py` — confirm green
- [ ] **Step 3.7:** Commit: `fix(messages): atomic record_result + idempotent stage inserts (B4)`
- [ ] **Step 3.8:** **CONSENSUS GATE**

---

## Task 4 — Transactional Kafka producer + per-msg offset tracking (B5, B6)

**Files:**
- Modify: `glue/jobs/ods_s3_publish.py:384-468` (publish loop + T0 recon)
- Modify: `ods_pipeline/offsets.py` (add `record_delivered_offset` helper)
- Create: `tests/unit/test_publish_transactional.py`
- Create: `tests/integration/test_publish_idempotent.py`

**Acceptance:**
- Producer config: `enable.idempotence=true`, `transactional.id=f"ods-publish-{run_id}-{partition}"`, `acks=all`, `max.in.flight.requests.per.connection=5`
- `init_transactions()` → `begin_transaction()` → loop `produce()` → `commit_transaction()`. On any exception → `abort_transaction()`. `producer.close()` in `finally`
- Delivered offsets captured per partition via `_deliver_cb` and stored in a dict; T0 recon compares `len(rows) == sum(len(per_partition_offsets))`
- Republishing same input file produces zero net new records (test with two-back-to-back runs of same file)

- [ ] **Step 4.1:** Write integration test: publish file → record `_ods_count_topic`. Re-run same publish (same `run_id`+`file_id`). Assert topic count unchanged
- [ ] **Step 4.2:** Run, confirm FAIL (current code duplicates)
- [ ] **Step 4.3:** Add `OffsetTracker` class in `ods_pipeline/offsets.py` with `on_delivery(err, msg)` callback. Store `(partition, offset)` per delivered msg
- [ ] **Step 4.4:** Refactor `ods_s3_publish.py` producer block: transactional config, `init/begin/commit/abort_transaction` lifecycle, `OffsetTracker` for callback. T0 recon uses tracker counts not offset deltas. `producer.close()` in finally
- [ ] **Step 4.5:** Run tests, confirm PASS
- [ ] **Step 4.6:** Commit: `fix(publish): transactional Kafka producer + per-msg offset tracking (B5, B6)`
- [ ] **Step 4.7:** **CONSENSUS GATE**

---

## Task 5 — DLQ writer + Connect errors.tolerance (B7)

**Files:**
- Modify: `ods_pipeline/dlq.py` (add `DlqWriter` class)
- Modify: `docker/connect-config/jdbc-sink-*.json` (add error config)
- Create: `tests/unit/test_dlq_writer.py`
- Create: `tests/integration/test_dlq_roundtrip.py`

**Acceptance:**
- `DlqWriter.write(envelope, attempt)` uploads to `s3://<dlq-bucket>/<domain>/<dataset>/<yyyy>/<mm>/<dd>/<run_id>/<attempt>.json`
- Retry policy: `max_attempts=5`, exponential backoff `1s, 2s, 4s, 8s, 16s`
- Connect sinks have `errors.tolerance=all`, `errors.deadletterqueue.topic.name=ods.dlq.<sink-name>`, `errors.deadletterqueue.context.headers.enable=true`, `errors.retry.timeout=600000`
- Integration test: inject malformed Avro → assert lands in S3 DLQ + Connect DLQ topic, sink continues

- [ ] **Step 5.1:** Write `DlqWriter` class with boto3 (or moto for unit). Test: 5 retries with backoff, S3 key correct
- [ ] **Step 5.2:** Update all 3 jdbc-sink JSON configs with error.* fields
- [ ] **Step 5.3:** Integration test: produce poison record to canonical topic → assert sink unchanged + DLQ topic has the record + connector status `RUNNING`
- [ ] **Step 5.4:** Wire `DlqWriter` into Glue job DLQ branch (`ods_canonicalize.py`)
- [ ] **Step 5.5:** Run all DLQ tests, confirm green
- [ ] **Step 5.6:** Commit: `feat(dlq): writer with retry + Connect errors.tolerance (B7)`
- [ ] **Step 5.7:** **CONSENSUS GATE**

---

## Task 6 — Exactly-once offset commit (B8)

**Files:**
- Modify: `ods_pipeline/offsets.py` (add `commit_to_postgres` helper)
- Modify: `glue/jobs/ods_s3_publish.py` (call after transactional commit)
- Create: `tests/unit/test_offsets_commit.py`

**Acceptance:**
- Offsets persisted to `pipeline.run_kafka_offsets` (preview — actual table arrives in T8) **inside the same transaction** as the run-status update
- On crash between Kafka `commit_transaction` and PG commit, replay detects the mismatch via `runs.start` idempotency + offset comparison and skips republish
- Crash test: kill process between Kafka commit and PG update → restart → no duplicate publish

- [ ] **Step 6.1:** Write a stub table migration `db/migrations/20_run_kafka_offsets_stub.sql` (full table in T8)
- [ ] **Step 6.2:** Write crash-simulation test using `multiprocessing` + signal — fork publisher, kill after Kafka commit, restart, assert no dupes
- [ ] **Step 6.3:** Implement `offsets.commit_to_postgres(conn, run_id, stage, ranges)`; call after `producer.commit_transaction()`, inside `with conn:`
- [ ] **Step 6.4:** Add resume-time check in `runs.start`: if `(run_id, stage)` already has offset rows, treat as idempotent retry (return existing run, do not republish)
- [ ] **Step 6.5:** Run tests, confirm green
- [ ] **Step 6.6:** Commit: `fix(offsets): persist atomically with run status; exactly-once on retry (B8)`
- [ ] **Step 6.7:** **CONSENSUS GATE**

---

# PART B — P1 (Architecture + Operator Unblock)

## Task 7 — pytest markers + pyproject.toml (8.1)

**Files:**
- Create or modify: `pyproject.toml`
- Modify: every test file — add `@pytest.mark.unit` / `integration` / `e2e` / `slow` / `negative` decorator
- Delete: `tests/dags/test_dag_integrity.py` (imports nonexistent modules) OR rewrite to current DAGs
- Fix: `tests/unit/test_t0_check.py:16` uuid cast bug
- Modify: `tests/conftest.py` — add per-test schema isolation

**Acceptance:**
- `pytest -m "not slow and not e2e"` runs in <60s on dev laptop
- All tests have at least one marker; `--strict-markers` enabled
- Unit tier passes without docker-compose
- `test_t0_check.py` no longer errors in cleanup

- [ ] **Step 7.1:** Add `pyproject.toml` `[tool.pytest.ini_options]` block with markers + `--strict-markers` + default `-m "not slow and not e2e and not integration"`
- [ ] **Step 7.2:** Categorise all 27 test files (unit vs integration vs e2e vs slow). Apply markers
- [ ] **Step 7.3:** Fix `test_t0_check.py:16` cast: `run_id::uuid = ANY(%s::uuid[])`
- [ ] **Step 7.4:** Delete `tests/dags/test_dag_integrity.py`. Replace with `test_dag_imports.py` listing current DAG modules
- [ ] **Step 7.5:** Refactor `tests/conftest.py` — add `pg_schema` fixture creating per-test schema, `SET search_path`, drop on teardown
- [ ] **Step 7.6:** Run `pytest -m unit`, confirm green and <60s
- [ ] **Step 7.7:** Commit: `test: add markers, fix t0_check cleanup, per-test schema isolation (8.1)`
- [ ] **Step 7.8:** **CONSENSUS GATE**

---

## Task 8 — Normalised run_kafka_offsets table (8.3)

**Files:**
- Create: `db/migrations/21_run_kafka_offsets_full.sql`
- Modify: `ods_pipeline/offsets.py` — full reader/writer
- Modify: `ods_pipeline/stages.py` — keep JSONB write transitional, add deprecation comment
- Create: `tests/unit/test_run_kafka_offsets.py`

**Acceptance:**
- Table exists with PK `(run_id, stage, topic, partition)`, generated `record_count`, `recorded_at` default
- Index `(topic, partition, recorded_at DESC)` for dashboards
- `offsets.persist_ranges(conn, run_id, stage, topic, ranges)` writes batch
- `offsets.read_ranges(conn, run_id, stage)` returns dict
- Backfill script in `scripts/backfill_run_kafka_offsets.py` populates from existing JSONB

- [ ] **Step 8.1:** Write migration matching schema in §8.3 of review doc
- [ ] **Step 8.2:** Write unit test for `persist_ranges` round-trip + uniqueness
- [ ] **Step 8.3:** Implement `persist_ranges` and `read_ranges`
- [ ] **Step 8.4:** Wire into `ods_s3_publish.py` (replace T6 stub call)
- [ ] **Step 8.5:** Write `scripts/backfill_run_kafka_offsets.py` reading `run_stage_log.metrics.offsets`
- [ ] **Step 8.6:** Run on local DB, verify backfilled rows match JSONB source
- [ ] **Step 8.7:** Commit: `feat(offsets): normalised run_kafka_offsets table + backfill (8.3)`
- [ ] **Step 8.8:** **CONSENSUS GATE**

---

## Task 9 — DLQ tooling: list / show / replay (8.2)

**Files:**
- Create: `ods_pipeline/ops/__init__.py`
- Create: `ods_pipeline/ops/dlq.py` (CLI subcommands)
- Modify: `scripts/ops_control_dashboard.py` (add DLQ panel)
- Create: `tests/unit/test_ops_dlq_cli.py`

**Acceptance:**
- `python -m ods_pipeline.ops dlq list [--domain --dataset --since]` prints counts table
- `python -m ods_pipeline.ops dlq show <s3_key>` prints envelope JSON
- `python -m ods_pipeline.ops dlq replay <s3_key> [--dry-run]` re-publishes to source topic + records replay attempt in `lineage_edge` (`edge_type='replay'`, `parent_run_id=<original failed run>`)
- Dashboard shows: counts by domain/dataset/run/stage, latest 20 errors, click-through to envelope
- Replay records new run via `runs.start`, links to original via lineage

- [ ] **Step 9.1:** Write CLI tests using `click.testing.CliRunner` — list, show, replay (with mock S3 + Kafka)
- [ ] **Step 9.2:** Implement `ods_pipeline/ops/dlq.py` with click commands
- [ ] **Step 9.3:** Add DLQ panel to `scripts/ops_control_dashboard.py` (likely Streamlit/FastAPI based on existing code)
- [ ] **Step 9.4:** Integration test: drop bad file → DLQ records → `replay` CLI → assert success path
- [ ] **Step 9.5:** Commit: `feat(ops): DLQ list/show/replay CLI + dashboard panel (8.2)`
- [ ] **Step 9.6:** **CONSENSUS GATE**

---

## Task 10 — Centralise correlation predicates (A1)

**Files:**
- Modify: `ods_pipeline/messages.py` — add `correlate(message, context, pattern_type)` function
- Modify: `glue/jobs/canonicalize.py` — replace `matches_context` with import from `ods_pipeline.messages`
- Create: `tests/unit/test_messages_correlate.py`

**Acceptance:**
- Single function handles correlation for all pattern types: `file`, `cdc`, `api`, `event`
- `file`: matches by `_ods_file_id`
- `cdc`: matches by `_ods_change_lsn` (or equivalent)
- `api`: matches by `_ods_request_id`
- `event`: matches by `_ods_event_id`
- `glue/jobs/canonicalize.py:matches_context` deleted; imports new function
- Existing canonical pipeline tests still pass

- [ ] **Step 10.1:** Define `PatternType` enum in `ods_pipeline/models.py`. Define correlation field per type
- [ ] **Step 10.2:** Write parametrized test covering all 4 pattern types
- [ ] **Step 10.3:** Implement `correlate(message, context, pattern_type)` returning bool
- [ ] **Step 10.4:** Refactor `canonicalize.py` to use it. Delete `matches_context`
- [ ] **Step 10.5:** Run canonical pipeline tests, confirm green
- [ ] **Step 10.6:** Commit: `refactor(correlation): centralise predicates by pattern type (A1)`
- [ ] **Step 10.7:** **CONSENSUS GATE**

---

## Task 11 — Dual-sink parity recon check (A2)

**Files:**
- Modify: `ods_pipeline/reconciliation.py` — add `check_dual_sink_parity(conn, dataset, business_date)`
- Modify: `airflow/dags/dag_recon_t2.py` — invoke per dataset that has dual sinks
- Create: `db/migrations/22_v_dual_sink_parity.sql` (SQL view)
- Create: `tests/unit/test_dual_sink_parity.py`

**Acceptance:**
- For each `(topic, partition)`, compute `MAX(_ods_offset)` in current sink table and history sink table
- Write recon row with `check_type='dual_sink_parity'`, status MATCH/MISMATCH/PENDING (PENDING if either sink lags <threshold)
- Threshold configurable per dataset (default 60s lag tolerance)
- View `ods.v_dual_sink_parity` exposes latest result per dataset

- [ ] **Step 11.1:** Write SQL view migration
- [ ] **Step 11.2:** Write unit test seeding fake sink tables, assert MATCH/MISMATCH/PENDING per scenario
- [ ] **Step 11.3:** Implement `check_dual_sink_parity` returning recon record
- [ ] **Step 11.4:** Wire into `dag_recon_t2.py`
- [ ] **Step 11.5:** Run integration `test_write_modes.py` — confirm new check appears
- [ ] **Step 11.6:** Commit: `feat(recon): dual-sink parity check (A2)`
- [ ] **Step 11.7:** **CONSENSUS GATE**

---

## Task 12 — IngestionPattern template (A4)

**Files:**
- Create: `ods_pipeline/patterns/__init__.py`
- Create: `ods_pipeline/patterns/base.py` — `IngestionPattern` dataclass
- Create: `ods_pipeline/patterns/file.py` — concrete file-based pattern (refactor risk + policies into this)
- Modify: `airflow/dags/dag_ingest.py` — DAG factory consumes `IngestionPattern`
- Modify: `glue/jobs/ods_canonicalize.py` — entrypoint consumes pattern
- Create: `tests/unit/test_patterns_base.py`

**Acceptance:**
- `IngestionPattern` dataclass: `name`, `pattern_type`, `stages: list[Stage]`, `topics: list[str]`, `sinks: list[str]`, `recon_checks: list[str]`, `correlation_field`
- File pattern (`patterns/file.py`) registers risk + policies via existing yaml configs
- DAG + Glue entrypoint take a pattern name, look up registry, drive execution
- `tests/integration/test_canonical_pipeline.py` still green using new path
- Adding a new pattern requires only: new pattern subclass + yaml config — no DAG/Glue changes

- [ ] **Step 12.1:** Define `IngestionPattern` dataclass + registry (`PATTERNS: dict[str, IngestionPattern]`)
- [ ] **Step 12.2:** Write test asserting registration + lookup + invariants
- [ ] **Step 12.3:** Implement `patterns/file.py` consuming existing `patterns/insurance/{policies,risk}.yaml`
- [ ] **Step 12.4:** Refactor `dag_ingest.py` to use registry
- [ ] **Step 12.5:** Refactor `ods_canonicalize.py` entrypoint
- [ ] **Step 12.6:** Run full integration suite, confirm green
- [ ] **Step 12.7:** Commit: `refactor(patterns): IngestionPattern template + file pattern (A4)`
- [ ] **Step 12.8:** **CONSENSUS GATE**

---

# PART C — P2 (Platform Completeness)

## Task 13 — Lineage closure invariant (A3)

**Files:**
- Modify: `ods_pipeline/runs.py` — add `finalise(conn, run_id)` doing invariant check
- Modify: callers of `runs.update(... status='succeeded')` to call `finalise` first
- Create: `tests/unit/test_runs_finalise.py`

**Acceptance:**
- `finalise` raises `LineageInvariantError` if:
  - `published_count > 0 AND no lineage_edge with child_run_id=run_id`
  - any open (non-terminal) stage exists for run_id
- On error, run marked `failed` with `error_summary='lineage invariant violated: <details>'`
- Existing successful runs continue to succeed (no false positives)

- [ ] **Step 13.1:** Write tests for both invariants + happy path
- [ ] **Step 13.2:** Implement `finalise`
- [ ] **Step 13.3:** Update callers
- [ ] **Step 13.4:** Run integration suite, confirm no regression
- [ ] **Step 13.5:** Commit: `feat(runs): finalise enforces lineage closure invariant (A3)`
- [ ] **Step 13.6:** **CONSENSUS GATE**

---

## Task 14 — Reconciliation dashboard SQL views (8.5)

**Files:**
- Create: `db/migrations/23_recon_dashboard_views.sql`
- Modify: `scripts/ops_control_dashboard.py` (add panels)
- Create: `tests/integration/test_recon_views.py`

**Acceptance:**
- 5 views created:
  - `ods.v_recon_latest_failed`
  - `ods.v_recon_t0_t1_t2_trend`
  - `ods.v_recon_dlq_adjusted`
  - `ods.v_current_history_consistency`
  - `ods.v_rerun_candidates`
- Dashboard exposes each view as a panel with date filters
- Integration test seeds runs + recon rows + DLQ rows, asserts each view returns expected shape

- [ ] **Step 14.1:** Draft SQL for each view (assert against the data model in `db/migrations/`)
- [ ] **Step 14.2:** Write integration test seeding scenarios
- [ ] **Step 14.3:** Run, confirm shapes
- [ ] **Step 14.4:** Wire into dashboard
- [ ] **Step 14.5:** Commit: `feat(dashboard): 5 recon views + dashboard panels (8.5)`
- [ ] **Step 14.6:** **CONSENSUS GATE**

---

## Task 15 — replay/rerun CLI (8.6)

**Files:**
- Modify: `ods_pipeline/ops/__init__.py`
- Create: `ods_pipeline/ops/runs.py` (`replay`, `rerun` subcommands)
- Create: `tests/unit/test_ops_runs_cli.py`

**Acceptance:**
- `python -m ods_pipeline.ops replay --file-id <uuid>` — looks up file, original run, dataset config, triggers DAG via Airflow REST. Creates new run linked via `lineage_edge(edge_type='replay', parent_run_id=<old>)`
- `python -m ods_pipeline.ops rerun --run-id <uuid>` — same but keyed by run_id
- Old run record + evidence preserved (no mutation)
- `--dry-run` prints intended action without executing

- [ ] **Step 15.1:** Write CLI tests with mocked Airflow REST client
- [ ] **Step 15.2:** Implement `replay` and `rerun`
- [ ] **Step 15.3:** Integration test: fail a run, replay it, assert new run succeeds + lineage edge written
- [ ] **Step 15.4:** Commit: `feat(ops): replay + rerun CLI (8.6)`
- [ ] **Step 15.5:** **CONSENSUS GATE**

---

## Task 16 — Message/API demo pipeline (8.4)

**Files:**
- Create: `services/event_api/__init__.py`
- Create: `services/event_api/main.py` (FastAPI app)
- Create: `services/event_api/Dockerfile`
- Modify: `docker-compose.yml` — add `event-api` service
- Create: `patterns/insurance/event_demo.yaml`
- Create: `tests/integration/test_event_api_e2e.py`

**Acceptance:**
- `POST /events` accepts JSON event payload
- Pipeline: write `pipeline.file_catalogue` synthetic row → archive payload to `s3://<bucket>/raw/event/...jsonl` → publish to raw Kafka → canonicalize via existing `ods_canonicalize.py` → write recon
- Reuses `ods_pipeline.runs/stages/messages/lineage` — proves boundary cleanliness
- First concrete instance of `IngestionPattern` template (T12) for non-file pattern
- E2E test posts event → asserts row in canonical sink table within 30s

- [ ] **Step 16.1:** Define `event_demo.yaml` pattern config
- [ ] **Step 16.2:** Write FastAPI service consuming `IngestionPattern` from registry
- [ ] **Step 16.3:** Add docker-compose service + healthcheck
- [ ] **Step 16.4:** Write E2E test
- [ ] **Step 16.5:** Run E2E, confirm green
- [ ] **Step 16.6:** Commit: `feat(event-api): Message/API ingestion pattern demo (8.4)`
- [ ] **Step 16.7:** **CONSENSUS GATE**

---

# Closeout

## Task 17 — Update review doc + memory

**Files:**
- Modify: `docs/branch-review-feat-local-s3-to-postgres.md` — mark P0/P1/P2 items closed with commit refs
- Memory: project memory entry for completed implementation cycle

- [ ] Mark each item closed with `[x]` + commit hash
- [ ] Save memory: project entry "P0/P1/P2 implementation cycle completed 2026-05-XX"
- [ ] Final commit: `docs: mark P0/P1/P2 review items as completed`

---

## Self-Review Notes

- **Spec coverage:** All 16 P0/P1/P2 items mapped to a task. B1+B2 merged into T1 (same fix pattern). A5 (supersede event topic, P3) deliberately excluded. 8.7 (OpenLineage) and 8.8 (history row-value recon) deliberately excluded as P3.
- **Type consistency:** `IngestionPattern` (T12) used by `event_api` (T16), `dag_ingest` (T12), `ods_canonicalize` (T12). `OffsetTracker` (T4) used by T6 and T8. `correlate` (T10) used by T12.
- **Dependencies:** T8 depends on T4+T6 (offset persistence); T9 depends on T5 (DLQ writer); T11 depends on T8 (offset table); T12 depends on T10 (correlation); T15 depends on T9+T13; T16 depends on T12.
- **Risks:** T4 + T6 are the highest-complexity tasks; if Kafka transactions misbehave in test env, fallback is at-least-once + dedup keys. T16 introduces new service — kept minimal; full ops hardening is out of scope.

---

## Execution Mode

**Sub-skill:** `superpowers:subagent-driven-development`

**Team:** 3-agent — Senior Developer (implements), Adversarial Code Reviewer (challenges), Architect (validates fit)

**Gate:** Each task ends with consensus checkpoint. All three must post `ACK` before next task starts.
