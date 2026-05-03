# API Pull Backlog

Slice 1 proved the core API pull contract: schema, pattern registration,
poller, JSONL archive, watermark pending/commit, lineage, reconciliation,
DAG wiring, and JSONL ingestion.

The remaining P0 E2E is now complete for the canonical API pull demo
(`patterns/insurance/api_pull_demo.yaml`, `is_canonical=true`):
`tests/integration/test_api_pull_e2e_live.py` proves stub API -> S3 JSONL
archive -> Glue JSONL ingestion -> curated Parquet -> Kafka Avro publish
-> `jdbc-sink-api-pull-demo` -> Postgres rows, plus API archive
reconciliation, T0 publish reconciliation, offset persistence, lineage,
and watermark promotion.

Non-canonical API-pull canonicalize coverage remains future work for the
first real non-canonical API source.

## Tracking Summary

| # | Item | Priority | Owner | Status |
|---|------|----------|-------|--------|
| 1 | Full E2E: dag_api_pull -> dag_ingest -> Glue -> Kafka -> optional canonicalize -> JDBC -> recon | P0 | TBD | Done for the canonical API pull demo. Non-canonical API-pull canonicalize coverage remains future work when a real non-canonical API source is onboarded. |
| 2 | Dashboard API Pull view | P1 | TBD | Deferred |
| 3 | Failure/recovery + DAG smoke integration tests | P0 | TBD | Done: `test_api_pull_failure_recovery_live.py` + `test_dag_api_pull_smoke.py`. |
| 4 | Onboarding / runbook docs | P1 | TBD | Done: `docs/api-pull-onboarding.md` + `docs/api-pull-runbook.md`. |
| 5 | Additional cursor styles: etag, offset, full_replace | P2 | TBD | Deferred, driven by real source requirements. |
| 6 | Tighten finalise_watermark -> dag_ingest run linkage | P0 | TBD | Done: deterministic `parent_run_id = uuid5("api_pull:" + api_pull_run_id)` pre-minted by `dag_api_pull` and consumed by `dag_ingest.init_run`; `ingest_status_for_api_pull_run` matches by exact PK + verifies the `triggered_by_api_pull` edge. |

P1 should land before first non-demo dataset onboards. P2 is
opportunistic and driven by real source requirements.

## 1. Full E2E Test

**Goal**: prove the live path stub API -> dag_api_pull -> dag_ingest ->
Kafka raw -> optional canonicalize -> JDBC sink -> reconciliation,
against the docker-compose stack, not a pytest-only mocked path.

**Status**: complete for `patterns/insurance/api_pull_demo.yaml`, which
is configured as canonical and therefore follows the direct archive ->
curated -> Kafka -> JDBC path.

**Acceptance covered**:
- Stub source returns JSON records.
- API pull archives gzipped JSONL to LocalStack S3, registers
  `file_catalogue`, writes lineage, writes `api_pull_archive_count`, and
  stages a pending watermark.
- Glue ingestion reads JSONL via Spark, validates schema, runs DQ, and
  writes curated Parquet.
- Glue publish writes Avro messages to `ods.insurance.api_pull_demo`,
  persists per-partition offsets, and writes `t0_publish_count`.
- `jdbc-sink-api-pull-demo` consumes the topic and upserts three rows
  into `ods.insurance_api_pull_demo`.
- `api_pull_watermark.committed_cursor_value` advances only after the
  triggered `dag_ingest` parent run succeeds.
- Re-running with the committed cursor archives zero new records and
  does not advance the cursor.

**Files**:
- `tests/integration/test_api_pull_e2e_live.py`
- `schemas/insurance/api_pull_demo.avsc`
- `docker/connect-config/jdbc-sink-api-pull-demo.json`
- `db/migrations/25_api_pull_demo_sink.sql`

## 2. Dashboard API Pull View

**Goal**: operator visibility for API pull alongside the existing
file-pattern dashboard.

**Panels**:
- Active API pull datasets from `dataset_config WHERE source_type='api_pull'`.
- Latest pull status per dataset from `run_log WHERE pipeline_type='api_pull'`.
- Watermark / cursor from `api_pull_watermark`.
- Pull lag from `now() - api_pull_watermark.updated_at`.
- Archive URI + file_id joined from stages / `file_catalogue`.
- Fetched vs archived reconciliation from `api_pull_archive_count`.
- Failed pulls from failed `run_log` and `run_stage_log`.
- DLQ for api_pull when added.
- Replay candidates: failed API pull runs and failed downstream
  `dag_ingest` runs joined by lineage / file id.

**Filters**: `domain`, `dataset`, `source_application`, `status`,
`business_date`, `run_id`, `file_id`, and cursor/window.

**Files**: extend `scripts/ops_control_dashboard.py` or a sibling
FastAPI dashboard module.

## 3. Failure / Recovery + DAG Smoke Tests

**Status**: done.

**Cases covered**:
- API 5xx after retries -> `raw_poll` failed, run failed, no archive,
  no `dag_ingest` trigger, no watermark advance.
- Archive succeeds, downstream `dag_ingest` fails -> pending cursor
  cleared by `finalise_watermark`, committed cursor unchanged, next poll
  re-issues the same window.
- Archive succeeds, downstream `dag_ingest` succeeds -> pending promoted
  to committed, `last_successful_run_id` set, `api_pull.completed`
  event emitted.
- 304 / no-changes -> no archive, no trigger, watermark unchanged.
- Concurrent DAG runs for same dataset -> `try_lock` returns false on
  the second invocation.
- DAG import smoke and trigger-conf conformance.

**Files**:
- `tests/integration/test_api_pull_failure_recovery_live.py`
- `tests/integration/test_dag_api_pull_smoke.py`

## 4. Onboarding / Runbook Docs

**Status**: done.

**Files**:
- `docs/api-pull-onboarding.md`
- `docs/api-pull-runbook.md`

## 5. Additional Cursor Strategies

`etag`, `offset`, and `full_replace` should each be one new cursor class
in `ods_pipeline/ingest/api_pull/cursors.py` plus one branch in
`build_cursor`. The poller body should not change. Build these only when
a real source requires them.

## 6. Watermark -> dag_ingest Linkage

**Status**: done.

`dag_api_pull` now pre-mints a deterministic downstream parent run id
from the API pull run id. `dag_ingest.init_run` consumes that id, and
`ingest_status_for_api_pull_run` matches by exact primary key while also
verifying the `triggered_by_api_pull` parent edge. The edge-only fallback
returns `None` on multiple matches, so ambiguous replays do not promote a
cursor.
