# API Pull Ingestion Design And Implementation Plan

## Purpose

Add an **API pull** ingestion pattern to Aviva ODS.

The platform already supports:

- **File-based ingestion**: source file lands, is registered in `pipeline.file_catalogue`, and flows through `dag_ingest`.
- **Event/API push ingestion**: source systems push events into the platform.
- **CDC ingestion**: change events flow through the canonical pipeline.

The missing pattern is:

```text
Airflow schedule -> pull external HTTP API -> archive response to S3
                 -> register logical batch in file_catalogue
                 -> reuse dag_ingest -> Kafka -> optional canonicalize -> JDBC sink
```

The goal is to reuse the existing file pipeline where that is sensible, while still treating API pull correctly as a **cursor/window-based source**, not a supplier-uploaded file.

## Recommended Decision

Implement API pull as a **logical archived batch**.

The API poller writes the pulled records to S3 as gzipped JSONL, registers that archive as a row in `pipeline.file_catalogue`, and triggers the existing `dag_ingest` flow.

This keeps the design simple:

- No new Kafka publishing path is needed for v1.
- Existing file lineage can be reused.
- Existing replay/rerun tooling can work from the archived S3 object.
- Existing canonicalization and JDBC sink logic remain the downstream path.
- Existing `file_id` lineage remains available, but it represents an API archive batch rather than a supplier file.

The main additions are:

- API source configuration.
- API watermark/cursor state.
- JSONL raw input support in ingestion.
- API pull run/stage/reconciliation records.
- Dashboard panels for API pull visibility.

## High-Level Flow

```text
dag_api_pull
  -> list active source_type='api_pull' datasets
  -> lock/read committed API watermark
  -> call external API page by page
  -> write gzipped JSONL archive to S3
  -> register archive in pipeline.file_catalogue
  -> write API pull run/stage/reconciliation records
  -> trigger dag_ingest with file_id
  -> dag_ingest reads JSONL and writes curated Parquet
  -> publish curated records to raw Kafka topic
  -> optional canonicalize raw topic to canonical topic
  -> wait for JDBC sink
  -> finalise run
  -> commit API watermark only after downstream success
```

## Why Not A Separate API-To-Kafka Pipeline?

A first-class API-to-Kafka pipeline is possible, but it would duplicate a lot of existing behaviour:

- Kafka publish and offset tracking.
- Canonicalize branching.
- Sink waits.
- Replay handling.
- File-level lineage-style browsing.
- Reconciliation from source batch to Kafka.

For v1, API pull should create a durable S3 archive and reuse the existing ingestion path. If later we need low-latency or streaming API pulls, we can add a direct API-to-Kafka path after the control-plane contract is proven.

## Source Identity

For file pipelines:

```text
_ods_file_id = supplier file identity
```

For API pull pipelines:

```text
_ods_file_id = archived API batch identity
```

That distinction should be visible in documentation and dashboards.

The API records should also carry API-specific metadata:

```text
_ods_run_id
_ods_file_id
_ods_source_application
_ods_domain
_ods_dataset
_ods_business_date
_ods_ingested_at
_ods_source_request_id
_ods_source_record_id
_ods_source_cursor
_ods_source_event_time
_ods_archive_uri
```

`_ods_source_request_id` already matches the existing `PatternType.API` correlation field.

## S3 Archive Format

Use gzipped JSONL:

```text
s3://ods-raw-local/api_pull/{domain}/{dataset}/date={business_date}/run_id={run_id}.jsonl.gz
```

Each line should be a single source record envelope:

```json
{
  "_ods_run_id": "uuid",
  "_ods_source_request_id": "uuid-or-request-id",
  "_ods_source_application": "source-system-name",
  "_ods_domain": "insurance",
  "_ods_dataset": "api_pull_demo",
  "_ods_business_date": "2026-05-03",
  "_ods_ingested_at": "2026-05-03T12:34:56Z",
  "_ods_source_cursor": "2026-05-03T12:00:00Z",
  "_ods_archive_uri": "s3://...",
  "payload": {
    "source_field": "value"
  }
}
```

The ingestion job may either flatten `payload` during JSONL read or canonicalization can map from nested `payload` fields. The preferred v1 approach is to write flat records when the API response shape is already tabular, and nested `payload` only when preserving complex source shape matters.

## Dataset Configuration

Add a YAML pattern, for example:

```yaml
domain: insurance
dataset: api_pull_demo
source_type: api_pull
pattern_type: api
write_mode: upsert
key_fields: [request_id]

source:
  application: demo_api
  url: ${API_PULL_DEMO_URL}
  method: GET
  raw_format: jsonl
  auth:
    type: bearer
    secret_ref: API_PULL_DEMO_TOKEN
  cursor:
    style: since_timestamp
    request_param: updated_since
    response_field: updated_at
    initial: "2026-01-01T00:00:00Z"
  page:
    style: link_header
  timeout_seconds: 30
  retries: 3

target_topic: ods.insurance.api_pull_demo
canonical_topic: ods.insurance.api_pull_demo.canonical
postgres_target_table: ods.insurance_api_pull_demo
schema_id: ods.insurance.api_pull_demo-value
canonical_schema_id: ods.insurance.api_pull_demo.canonical-value
schema_version: 1
data_classification: Internal
dq_rules:
  hard_blocks: []
recon_tolerance_records: 0
recon_tolerance_pct: 0
```

`yaml_loader.py` should sync the standard dataset-level fields into `pipeline.dataset_config`. The nested `source` block can either remain YAML-only for v1 or be stored in a JSONB column if operational querying is needed.

Recommended v1 addition:

```sql
ALTER TABLE pipeline.dataset_config
    ADD COLUMN IF NOT EXISTS raw_format VARCHAR DEFAULT 'csv' NOT NULL,
    ADD COLUMN IF NOT EXISTS source_config JSONB DEFAULT '{}'::jsonb NOT NULL;
```

If avoiding `dataset_config` schema changes is preferred, `raw_format` and `source` can stay in YAML and be loaded by the API pull DAG. The ingestion job still needs to know the raw format, either from `dataset_config` or from DAG conf.

## Watermark Table

Add a dedicated API watermark table. Watermark state is operational state and should not live only in `run_log.metrics`.

Suggested migration:

```sql
CREATE TABLE IF NOT EXISTS pipeline.api_pull_watermark (
    domain VARCHAR NOT NULL,
    dataset VARCHAR NOT NULL,
    source_application VARCHAR NOT NULL,
    cursor_type VARCHAR NOT NULL,
    committed_cursor_value TEXT NULL,
    pending_cursor_value TEXT NULL,
    pending_run_id UUID NULL,
    last_successful_run_id UUID NULL,
    locked_at TIMESTAMP NULL,
    updated_at TIMESTAMP DEFAULT now() NOT NULL,
    PRIMARY KEY (domain, dataset, source_application)
);

CREATE INDEX IF NOT EXISTS idx_api_pull_watermark_pending
    ON pipeline.api_pull_watermark (pending_run_id)
    WHERE pending_run_id IS NOT NULL;
```

### Cursor Commit Rule

Do not advance `committed_cursor_value` immediately after a successful HTTP call.

Safe sequence:

```text
1. Read committed cursor.
2. Pull API records.
3. Write S3 archive.
4. Register file_catalogue.
5. Store pending cursor against api_pull run.
6. Trigger dag_ingest.
7. When downstream flow succeeds, promote pending cursor to committed cursor.
```

This prevents data loss if the API call succeeds but the downstream ingestion fails.

## Supported Cursor Styles

Implement cursor strategies behind a small protocol/factory.

| Style | Stored Watermark | Paging Behaviour | Notes |
|---|---|---|---|
| `since_timestamp` | max source update timestamp | link header or next token | Best default for APIs with `updated_at` |
| `offset` | numeric offset | increment offset until empty page | Simple, but weaker if source data changes while paging |
| `etag` | last ETag | `If-None-Match`, 304 means no work | Good for full snapshots |
| `full_replace` | last successful run ID/time | single full pull | Good for small reference data |

Each strategy should return:

```text
records
new_cursor_value
source_request_id
page_count
http_status_summary
```

## Authentication

For v1, implement bearer-token auth:

```text
source.auth.type = bearer
source.auth.secret_ref = API_PULL_DEMO_TOKEN
```

The DAG/runtime reads the token from environment or Airflow Secrets Backend and adds:

```text
Authorization: Bearer <token>
```

Future auth types can be added without changing the poller:

- `none`
- `basic`
- `mtls`
- OAuth client credentials

## Pipeline Table Contract

### `pipeline.run_log`

Create an API pull run:

```text
pipeline_type = api_pull
domain
dataset
business_date
status = running | succeeded | failed | partial
parents = optional retry/replay metadata
```

The downstream `dag_ingest` will create the usual parent/ingestion/publish/canonicalize child runs for the archived batch.

### `pipeline.run_stage_log`

Recommended stages:

```text
raw_poll
message_archive
recon_message
finalise
```

Add `Stage.RAW_POLL` if it does not exist.

The existing stages continue downstream:

```text
raw_read
schema_validate
dq_check
curated_write
curated_read
kafka_publish
kafka_consume
canonical_transform
recon_t0
recon_t1
sink_pg_wait
finalise
```

### `pipeline.file_catalogue`

Register the S3 JSONL archive:

```text
domain
dataset
business_date
s3_raw_path
file_md5
file_size_bytes
source_row_count
state = received or registered
last_run_id = api_pull_run_id
```

For API pull, `s3_raw_path` points to the archived JSONL batch.

### `pipeline.reconciliation_log`

Add API-window reconciliation:

```text
check_type = api_pull_archive_count
source_count = records fetched from API
kafka_count = null
postgres_count = null
discrepancy_count = archived_count - fetched_count
status = ok | failed
detail = cursor/window/page metadata
```

Downstream checks remain:

```text
t0_publish_count
t1_canonical_count
t2_sink_count
dual_sink_parity where applicable
current_history_row_value where applicable
```

### `pipeline.lineage_edge`

Write an API-to-archive edge:

```text
child_run_id = api_pull_run_id
parent_run_id = null
parent_file_id = null
edge_type = api_to_archive
source_ref = https://source-api.example/items?updated_since=...
target_ref = s3://ods-raw-local/api_pull/...
record_count = fetched_count
```

Then `dag_ingest` writes the existing file/archive-to-curated and curated-to-Kafka lineage.

### `pipeline.run_events`

Emit best-effort events such as:

```text
api_pull.started
api_pull.archived
api_pull.skipped_no_changes
api_pull.failed
api_pull.completed
```

Dashboards should use `run_log` and `run_stage_log` as the authoritative source. `run_events` remains useful for event-stream diagnostics.

## DAG Design

New file:

```text
airflow/dags/dag_api_pull.py
```

Suggested shape:

```python
@dag(schedule="*/15 * * * *", catchup=False, max_active_runs=1, tags=["ods", "api"])
def dag_api_pull():
    @task
    def list_active_api_datasets() -> list[dict]:
        # SELECT domain, dataset, source_config, target_topic, ...
        # FROM pipeline.dataset_config
        # WHERE active = TRUE AND source_type = 'api_pull'
        ...

    @task
    def poll_one(cfg: dict) -> dict | None:
        # start api_pull run
        # lock/read watermark
        # call poll_and_archive
        # upsert file_catalogue
        # write stages/recon/lineage
        # return dag_ingest conf or None when no records/304
        ...

    pulled = poll_one.expand(cfg=list_active_api_datasets())

    TriggerDagRunOperator.partial(
        task_id="trigger_ingest",
        trigger_dag_id="dag_ingest",
    ).expand(conf=pulled)
```

If a pull returns no records, the DAG should mark the API pull run as succeeded/skipped and not trigger `dag_ingest`.

## Poller Package

New package:

```text
ods_pipeline/ingest/api_pull/
  __init__.py
  poller.py
  auth.py
  cursors.py
  watermark.py
```

Single public entrypoint:

```python
def poll_and_archive(
    *,
    dataset_config,
    s3_client,
    watermark_store,
    secrets,
    run_id,
    business_date,
) -> ArchivedBatch:
    ...
```

Return object:

```python
@dataclass
class ArchivedBatch:
    domain: str
    dataset: str
    business_date: str
    source_application: str
    s3_uri: str
    file_md5: str
    file_size_bytes: int
    record_count: int
    old_cursor_value: str | None
    new_cursor_value: str | None
    source_request_id: str
    page_count: int
    no_changes: bool = False
```

## Ingestion Job Change

`glue/jobs/ods_ingestion.py` currently reads CSV only.

Add raw-format branching:

```python
if raw_format == "csv":
    df = spark.read.option("inferSchema", "true").option("header", "true").csv(s3a_path)
elif raw_format == "jsonl":
    df = spark.read.json(s3a_path)
else:
    raise ValueError(f"Unsupported raw_format={raw_format}")
```

The raw format should come from `dataset_config.raw_format` or from DAG conf.

For JSONL archives, DQ and schema validation still apply after the read.

## Dashboard Changes

Add an **API Pull** view to the FastAPI dashboard.

Minimum panels:

| Panel | Source |
|---|---|
| Active API pull datasets | `dataset_config WHERE source_type='api_pull'` |
| Last pull status | `run_log WHERE pipeline_type='api_pull'` |
| Cursor/watermark | `api_pull_watermark` |
| Pull lag | `api_pull_watermark.updated_at` and latest successful run |
| Fetched vs archived counts | `reconciliation_log WHERE check_type='api_pull_archive_count'` |
| Failed pulls | failed `run_log` / failed `run_stage_log` for API stages |
| Archive URI | `run_stage_log.output_ref` for `message_archive` |
| Downstream status | join API archive `file_id` to downstream `dag_ingest` runs |
| Replay candidates | failed API pull runs and failed downstream runs |

Dashboard filters should include:

```text
domain
dataset
source_application
status
business_date
cursor/window
run_id
file_id
```

## Failure And Recovery

### API returns 5xx or times out

Retry according to config. If retries fail:

- Mark `raw_poll` failed.
- Mark API pull run failed.
- Do not write archive.
- Do not advance watermark.

### API returns 304 / no changes

- Mark `raw_poll` succeeded/skipped.
- Mark run succeeded.
- Do not trigger `dag_ingest`.
- Do not change committed cursor unless the cursor strategy explicitly requires it.

### Archive succeeds but downstream ingest fails

- Keep pending cursor.
- Do not promote to committed cursor.
- Operator can rerun downstream from the same `file_id`.

### Publish/canonicalize/sink fails

- Existing downstream failure handling applies.
- API cursor remains uncommitted until the whole chain succeeds.

### Duplicate pull/replay

- S3 archive path includes `run_id`, so archives are immutable.
- `file_catalogue` deduplicates by `domain`, `dataset`, `s3_raw_path`.
- Kafka/Postgres idempotency still depends on keys and existing replay safeguards.

## Implementation Todo

### Phase 1 - Design Scaffolding

- [ ] Add `docs/api-pull-ingestion-design.md`.
- [ ] Add `Stage.RAW_POLL` to `ods_pipeline.models.Stage`.
- [ ] Add `ods_pipeline/patterns/api_pull.py`.
- [ ] Import API pull pattern in `ods_pipeline/patterns/__init__.py`.
- [ ] Add `patterns/insurance/api_pull_demo.yaml`.

### Phase 2 - Schema And Config

- [ ] Add migration for `pipeline.api_pull_watermark`.
- [ ] Decide whether to add `dataset_config.raw_format` and `dataset_config.source_config`.
- [ ] Update `airflow/dags/common/yaml_loader.py` to sync API pull config.
- [ ] Add config validation for required API fields.

### Phase 3 - Poller Package

- [ ] Create `ods_pipeline/ingest/api_pull/__init__.py`.
- [ ] Create `poller.py` with `poll_and_archive`.
- [ ] Create `auth.py` with bearer-token provider.
- [ ] Create `cursors.py` with `since_timestamp`, `offset`, `etag`, and `full_replace`.
- [ ] Create `watermark.py` with lock/read/pending/commit helpers.
- [ ] Write gzipped JSONL archives to S3.
- [ ] Calculate MD5 over archived bytes.
- [ ] Return `ArchivedBatch` metadata.

### Phase 4 - Airflow

- [ ] Add `airflow/dags/dag_api_pull.py`.
- [ ] List active `source_type='api_pull'` datasets.
- [ ] Start and close API pull runs through `ods_pipeline.runs`.
- [ ] Write API pull stages through `ods_pipeline.stages`.
- [ ] Write `api_pull_archive_count` reconciliation.
- [ ] Write API-to-archive lineage edge.
- [ ] Register archive in `file_catalogue`.
- [ ] Trigger `dag_ingest` only when records were archived.
- [ ] Add a finalizer that commits watermark only after downstream success.

### Phase 5 - JSONL Ingestion

- [ ] Add raw-format support to `glue/jobs/ods_ingestion.py`.
- [ ] Pass `raw_format` through `dag_ingest` context.
- [ ] Ensure JSONL records retain ODS metadata fields.
- [ ] Ensure schema validation supports JSONL-derived DataFrames.
- [ ] Ensure DQ failures still go to DLQ.

### Phase 6 - Dashboard

- [ ] Add API pull datasets panel.
- [ ] Add watermark/cursor panel.
- [ ] Add pull lag metric.
- [ ] Add fetched vs archived reconciliation panel.
- [ ] Add failed API pulls panel.
- [ ] Add archive URI and file_id linkage.
- [ ] Add filters for source application, cursor/window, run_id, and file_id.

### Phase 7 - Documentation

- [ ] Document how to onboard a new API pull dataset.
- [ ] Document supported cursor strategies.
- [ ] Document replay and watermark recovery.
- [ ] Document dashboard operating procedure.
- [ ] Document security expectations for API tokens.

## Unit Tests

Prefer real local components where practical. Avoid relying on mocked tests for behavioural proof.

### Pattern Tests

File:

```text
tests/unit/test_pattern_api_pull.py
```

Coverage:

- API pull pattern is registered.
- Pattern type is `PatternType.API`.
- Correlation field is `_ods_source_request_id`.
- Stages include `raw_poll`, `message_archive`, `recon_message`, and `finalise`.
- Topics and sinks are present.
- YAML path exists.

### Cursor Strategy Tests

File:

```text
tests/unit/test_api_pull_cursors.py
```

Coverage:

- `since_timestamp` advances to max response timestamp.
- `offset` advances by returned page size.
- `etag` handles 200 and 304.
- `full_replace` returns one-shot cursor metadata.
- Invalid cursor style raises a clear error.

These can be deterministic unit tests without network.

### Auth Tests

File:

```text
tests/unit/test_api_pull_auth.py
```

Coverage:

- Bearer token is loaded from environment/secret provider.
- Missing token raises a clear error.
- Authorization header shape is correct.
- `none` auth sets no auth header.

### Archive Format Tests

File:

```text
tests/unit/test_api_pull_archive_format.py
```

Coverage:

- Records are written as gzipped JSONL.
- One source record equals one line.
- MD5 is stable for the same byte content.
- Archive envelope includes required ODS metadata.

### Watermark Tests

File:

```text
tests/unit/test_api_pull_watermark.py
```

Coverage:

- Pending cursor is stored separately from committed cursor.
- Committed cursor is unchanged after failed run.
- Pending cursor is promoted only for successful run.
- Locking prevents concurrent pull for same domain/dataset/source application.

## Integration Tests

These should use the local docker-compose stack where possible.

### Live API Pull Archive Test

File:

```text
tests/integration/test_api_pull_archive_live.py
```

Setup:

- Start a tiny local FastAPI source app in the test process or as a compose service.
- Use real HTTP calls.
- Use LocalStack S3.
- Use real Postgres.

Assertions:

- API pull run is created in `run_log`.
- `raw_poll` and `message_archive` stages are written.
- S3 JSONL archive exists.
- `file_catalogue` row exists with MD5 and source row count.
- `api_pull_archive_count` reconciliation is `ok`.
- Watermark pending cursor is stored.

### Live End-To-End API Pull Test

File:

```text
tests/integration/test_api_pull_e2e_live.py
```

Flow:

```text
stub API -> dag_api_pull -> S3 archive -> file_catalogue -> dag_ingest
         -> raw Kafka -> optional canonical Kafka -> JDBC sink -> recon
```

Assertions:

- API pull run succeeds.
- Downstream `dag_ingest` runs succeed.
- Raw Kafka messages match archive record count.
- Canonical Kafka messages match expected count when configured.
- Postgres target rows match expected count.
- T0/T1/T2 reconciliation rows are `ok`.
- Watermark committed cursor advances only after downstream success.
- Lineage contains API-to-archive and archive-to-Kafka path.

### No-Changes Test

File:

```text
tests/integration/test_api_pull_no_changes_live.py
```

Assertions:

- API returns 304 or empty response.
- No S3 archive is written.
- No `file_catalogue` row is created.
- `dag_ingest` is not triggered.
- Run is marked succeeded/skipped.
- Watermark remains correct.

### Failure/Recovery Test

File:

```text
tests/integration/test_api_pull_failure_recovery_live.py
```

Assertions:

- API 5xx after retries marks run failed.
- No committed cursor advance.
- Archive success followed by downstream failure leaves pending cursor uncommitted.
- Re-running processes the same cursor window safely.

### DAG Import/Smoke Test

File:

```text
tests/integration/test_dag_api_pull.py
```

Assertions:

- Airflow imports `dag_api_pull` without errors.
- DAG can list configured API pull datasets.
- DAG emits valid `dag_ingest` trigger conf for archived records.

## E2E Acceptance Criteria

The feature is acceptable when:

- A developer can add a YAML config for an API source.
- Airflow can pull records from a real HTTP endpoint.
- Pulled records are archived to S3 as JSONL.
- The archive is visible in `file_catalogue`.
- Existing `dag_ingest` can process the archive.
- Records reach Kafka.
- Canonicalization works when configured.
- JDBC sink receives records when configured.
- Reconciliation proves:
  - fetched equals archived,
  - archived equals raw Kafka published,
  - raw equals canonical where applicable,
  - canonical equals sink/history where applicable.
- Cursor advances only after successful downstream completion.
- Failed pulls and failed downstream runs are visible in the dashboard.
- Replay/rerun can recover without losing records.

## Open Decisions

| Decision | Default Recommendation |
|---|---|
| Store source config in `dataset_config.source_config` or YAML-only? | Use `source_config JSONB` if dashboards/operators need visibility; otherwise YAML-only is acceptable for v1 |
| Commit cursor in `dag_api_pull` or `dag_ingest.finalise`? | Commit after downstream success; implementation can use a callback/finalizer |
| Treat API archive as file? | Yes, but label it clearly as a logical API batch |
| Flatten JSONL or preserve nested payload? | Flatten for tabular APIs; preserve nested payload for complex event-like APIs |
| First cursor style to implement? | `since_timestamp`, then add `etag`, `offset`, `full_replace` |
| First test dataset? | `insurance.api_pull_demo` |

## Risks

| Risk | Mitigation |
|---|---|
| Cursor advances before downstream success | Use pending vs committed cursor |
| JSONL archive cannot be ingested by current Glue job | Add raw-format support before E2E |
| API source changes while paging | Prefer cursor/token APIs over offset where possible |
| Duplicate pulls publish duplicate downstream rows | Use stable keys, upsert semantics, and existing replay safeguards |
| Operators cannot tell where API pull failed | Add dashboard API Pull view |
| API auth secrets leak into logs | Never log token values; use secret refs only |

## First Implementation Slice

The smallest useful slice is:

```text
1. Add API pull pattern registration.
2. Add watermark table.
3. Add poller with since_timestamp + bearer auth.
4. Archive JSONL to S3 and register file_catalogue.
5. Add JSONL support to ods_ingestion.py.
6. Trigger existing dag_ingest.
7. Add dashboard visibility for latest pulls and watermarks.
8. Add live E2E test with local FastAPI source.
```

This proves the full idea without building every cursor/auth mode immediately.
