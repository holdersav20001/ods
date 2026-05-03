# API Pull — Deferred Backlog

Slice 1 (in feat/local-s3-to-postgres) proves the core API pull contract:
schema, pattern registration, poller, JSONL archive, watermark
pending/commit, lineage, recon, dag wiring, JSONL ingestion path. The
items below are tracked but not in slice 1.

## Tracking summary

| # | Item | Priority | Owner | Status |
|---|------|----------|-------|--------|
| 1 | Full E2E: dag_api_pull → dag_ingest → Glue → Kafka → canonicalize → JDBC → recon | P0 | TBD | **Partially done** — stub → archive → Glue JSONL ingestion → curated parquet covered by `tests/integration/test_api_pull_e2e_live.py`. Kafka publish + canonicalize + JDBC sink + cross-stage recon still deferred (shares code with file pattern; covered by `test_run_events.py` for the file path). |
| 2 | Dashboard API Pull view | P1 | TBD | Deferred |
| 3 | Failure/recovery + DAG smoke integration tests | P0 | TBD | **Done** — `test_api_pull_failure_recovery_live.py` + `test_dag_api_pull_smoke.py`. |
| 4 | Onboarding / runbook docs | P1 | TBD | **Done** — `docs/api-pull-onboarding.md` + `docs/api-pull-runbook.md`. |
| 5 | Additional cursor styles: etag, offset, full_replace | P2 | TBD | Deferred — driven by real source requirements. |
| 6 | Tighten finalise_watermark → dag_ingest run linkage (no replay/race promotion) | P0 | TBD | **Done** — explicit `triggered_by_api_pull` edge in `run_log.parents`; lookup via JSONB containment in `ods_pipeline.ingest.api_pull.linkage`. Proven by `test_api_pull_watermark_linkage.py`. |

P0 must land in Slice 2. P1 should land before first non-demo dataset
onboards. P2 is opportunistic — driven by real source requirements.

## 1. Full E2E test

**Goal**: prove the live path stub API → dag_api_pull → dag_ingest →
Kafka raw → optional canonicalize → JDBC sink → recon, against the
docker-compose stack, not a pytest-only mocked path.

**Acceptance**:
- Stub source (in-process FastAPI or new compose `stub_api` service)
  returns paged JSONL records.
- `dag_api_pull` DAG run creates `pipeline.run_log (pipeline_type='api_pull')`,
  archives gzipped JSONL to LocalStack S3, registers `file_catalogue`,
  and triggers `dag_ingest`.
- `dag_ingest` reads JSONL via Spark, publishes to raw Kafka, JDBC sink
  consumes offsets, and the canonical/postgres counts match fetched
  records.
- T0/T1/T2 reconciliation rows are `ok`.
- `api_pull_watermark.committed_cursor_value` advances only after the
  triggered `dag_ingest` parent run succeeds.
- Re-running with the committed cursor archives zero new records and
  does not trigger `dag_ingest`.

**Files**: `tests/integration/test_api_pull_e2e_live.py`,
optional `docker-compose.override.yml` adding a stub_api service.

## 2. Dashboard API Pull view

**Goal**: operator visibility for API pull alongside the existing
file-pattern dashboard.

**Panels**:
- Active API pull datasets (`dataset_config WHERE source_type='api_pull'`)
- Latest pull status per dataset (`run_log WHERE pipeline_type='api_pull'`)
- Watermark / cursor: `api_pull_watermark` (committed, pending, locked, last_successful_run_id)
- Pull lag: `now() - api_pull_watermark.updated_at` and time since last successful run
- Archive URI + file_id: `run_stage_log` for `message_archive` joined to `file_catalogue`
- Fetched vs archived reconciliation: `reconciliation_log WHERE check_type='api_pull_archive_count'`
- Failed pulls: failed `run_log` and failed `run_stage_log` for API stages
- DLQ for api_pull (when added; not in slice 1)
- Replay candidates: failed API pull runs and failed downstream `dag_ingest` runs joined by `file_id`

**Filters**: `domain`, `dataset`, `source_application`, `status`,
`business_date`, `run_id`, `file_id`, `cursor/window`.

**Files**: extend `scripts/ops_control_dashboard.py` (or sibling
FastAPI dashboard module).

## 3. Failure / recovery + DAG smoke tests

**Goal**: prove correctness on the unhappy paths described in the
design doc.

**Cases**:
- API 5xx after retries → `raw_poll` failed, run failed, no archive,
  no `dag_ingest` trigger, no watermark advance.
- Archive succeeds, downstream `dag_ingest` fails → pending cursor
  cleared by `finalise_watermark`, committed cursor unchanged, next
  poll re-issues the same window.
- Archive succeeds, downstream `dag_ingest` succeeds → pending promoted
  to committed, `last_successful_run_id` set, `api_pull.completed`
  event emitted.
- 304 / no-changes → `raw_poll` skipped, run succeeded, no archive,
  no trigger, watermark unchanged.
- Concurrent DAG runs for same dataset → `try_lock` returns False on
  the second invocation, second invocation returns None.
- DAG import smoke: `dag_api_pull` imports cleanly under Airflow,
  `list_active_api_datasets` returns rows, `poll_one` produces a valid
  `dag_ingest` trigger conf.

**Files**:
- `tests/integration/test_api_pull_failure_recovery_live.py`
- `tests/integration/test_api_pull_no_changes_live.py`
- `tests/integration/test_dag_api_pull_smoke.py`

## 4. Onboarding / runbook docs

**Goal**: an operator can add a new API pull source and recover stuck
state without reading the source.

**Sections**:
- How to add a new dataset YAML (location, required fields, secret_ref
  conventions, permitted cursor styles).
- How to register the secret in the Airflow Secrets Backend / env.
- How to verify config sync via `dag_config_sync`.
- How to operate the dashboard panels (what each means, what
  thresholds mean).
- How to replay a failed window (re-trigger `dag_ingest` for an
  existing `file_id`; cursor is left committed if the original ingest
  succeeded; otherwise pending is cleared and the next poll re-issues).
- How to recover a stuck `pending_cursor_value` (sensor timeout,
  manual `clear_pending` / `promote` SQL, restart guidance).
- Security expectations for tokens (never in YAML/source_config, only
  via secret_ref pointing at env/Secrets Backend; rotation runbook).

**Files**: `docs/api-pull-onboarding.md`, `docs/api-pull-runbook.md`.

## 5. Additional cursor strategies

`etag`, `offset`, `full_replace` — each is one new class in
`ods_pipeline/ingest/api_pull/cursors.py` and one branch in
`build_cursor`. The poller body never changes. Driven by real source
requirements; do not pre-build.

## 6. Tighten finalise_watermark → dag_ingest run linkage

**Risk** (called out by code review of slice 1):
`finalise_watermark` currently locates the downstream parent run via
"latest `s3_batch` row in `pipeline.run_log` for this `file_id`". A
manual replay of the same `file_id` after the `api_pull` run finished
could in principle observe a status that does not belong to the
`dag_ingest` execution actually launched by `dag_api_pull`, and
promote/clear the wrong cursor.

In slice 1 this is mitigated by `max_active_runs=1` on `dag_api_pull`
and by the lock in `api_pull_watermark`, but it is not proven.

**Goal**: make the linkage explicit so the promoted watermark provably
belongs to the exact `dag_ingest` parent run launched by `poll_one`.

**Acceptance**:
- `poll_one` records the triggered Airflow `dag_run.run_id` (or the
  `dag_ingest` parent `run_id` if a synchronous trigger pattern is
  adopted) on `pipeline.api_pull_watermark.pending_run_id` or in a
  separate column / `run_log.parents` edge.
- `finalise_watermark` looks up the downstream run by that explicit
  identifier, not by `file_id` LIMIT 1.
- An integration test replays an old `file_id` and proves the cursor
  is not promoted by the replay.

**Files**:
`airflow/dags/dag_api_pull.py`,
`ods_pipeline/ingest/api_pull/watermark.py`,
optional new column or use of `parents`.
