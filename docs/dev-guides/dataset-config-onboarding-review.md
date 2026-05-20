# Dataset config onboarding - review draft

> Draft for review. This turns the control-table population story into a
> checklist for adding or changing a dataset.

## What you are populating

You are populating `pipeline.dataset_config` indirectly.

Write YAML, run `dag_config_sync`, then verify the row in Postgres. The runtime
tables are populated later by the DAGs when data actually moves.

## Folder convention to settle

Current code:

```text
dag_config_sync reads DATASETS_DIR
default DATASETS_DIR = /opt/airflow/datasets
docker-compose mounts ./datasets to /opt/airflow/datasets
docker-compose also mounts ./patterns to /opt/airflow/patterns
```

Some existing docs and examples put API/direct-postgres YAML under `patterns/`.
That is useful for transform/pattern examples, but it is not the default sync
folder.

For review, choose one of these:

| Option | Change needed |
|---|---|
| Make `datasets/` the official source for synced dataset config | Move or copy synced dataset YAML into `datasets/`; keep `patterns/` for transform examples. |
| Make `patterns/` the official source | Change `DATASETS_DIR` default or `docker-compose.yml` so `dag_config_sync` scans `patterns/`. |
| Scan both | Change `dag_config_sync` to scan both mounted folders and document duplicate-name behavior. |

My recommendation: use `datasets/` for rows that must sync into
`pipeline.dataset_config`, and use `patterns/` for reusable pattern or transform
definitions.

## Required fields by route

### File to Kafka/Postgres sink

Use this when a file lands on SFTP and should flow through Kafka and the JDBC
sink.

```yaml
domain: insurance
dataset: policies
source_type: s3_batch
delivery: file_pipeline
write_mode: upsert
filename_pattern: '^policies_(?P<bd>\d{8})\.csv$'
key_fields: [policy_id]
target_topic: ods.insurance.policies
postgres_target_table: ods.insurance_policy
s3_curated_path: s3://ods-curated-local/insurance/policies/
schema_id: ods.insurance.policies-value
schema_version: 1
data_classification: Internal
recon_tolerance_records: 0
recon_tolerance_pct: 0
schema_def:
  fields:
    - {name: policy_id, type: string}
dq_rules:
  hard_blocks: []
```

### File direct to Postgres

Use this for a batch file that should skip Kafka.

```yaml
domain: insurance
dataset: file_direct_pg_upsert_demo
source_type: s3_batch
delivery: direct_postgres
write_mode: upsert
filename_pattern: '^country_codes_(?P<bd>\d{8})\.csv$'
key_fields: [country_code]
postgres_target_table: ods.insurance_file_direct_pg_upsert_demo
s3_curated_path: s3://ods-curated-local/insurance/file_direct_pg_upsert_demo/
schema_id: ods.insurance.file_direct_pg_upsert_demo-value
schema_version: 1
data_classification: Internal
recon_tolerance_records: 0
recon_tolerance_pct: 0
schema_def:
  fields:
    - {name: country_code, type: string}
    - {name: country_name, type: string}
dq_rules:
  hard_blocks: []
```

Do not set `target_topic` or `canonical_topic` for `delivery: direct_postgres`.

### API pull through file pipeline

Use this when Airflow polls an API, archives the response, and then reuses the
file pipeline.

```yaml
domain: insurance
dataset: api_pull_demo
source_type: api_pull
pattern_type: api
delivery: file_pipeline
write_mode: upsert
key_fields: [request_id]
target_topic: ods.insurance.api_pull_demo
canonical_topic: ods.insurance.api_pull_demo.canonical
postgres_target_table: ods.insurance_api_pull_demo
s3_curated_path: s3://ods-curated-local/insurance/api_pull_demo/
schema_id: ods.insurance.api_pull_demo-value
canonical_schema_id: ods.insurance.api_pull_demo.canonical-value
schema_version: 1
data_classification: Internal
raw_format: jsonl
is_canonical: true
recon_tolerance_records: 0
recon_tolerance_pct: 0
dq_rules:
  hard_blocks: []
schema_def:
  fields:
    - {name: request_id, type: string, required: true}
source:
  application: demo_api
  url: ${API_PULL_DEMO_URL}
  method: GET
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
transform:
  fields:
    - target: request_id
      source: _ods_source_request_id
      required: true
  required:
    - request_id
```

Never put real tokens, passwords, API keys, or client secrets in YAML. Use
`secret_ref`.

## Sync checklist

1. Add or edit the YAML in the agreed sync folder.
2. Add any required schema files under `schemas/`.
3. Add the Postgres target table migration if the target does not exist.
4. Make the secret available to Airflow if the dataset uses API auth.
5. Trigger `dag_config_sync`.
6. Check the Airflow task output for validation errors.
7. Verify the row:

```sql
SELECT domain,
       dataset,
       source_type,
       delivery,
       target_topic,
       postgres_target_table,
       active,
       config_version_id
  FROM pipeline.dataset_config
 WHERE domain = '<domain>'
   AND dataset = '<dataset>';
```

8. For API pull datasets, also verify:

```sql
SELECT raw_format,
       source_config->>'application' AS application,
       source_config->'auth'->>'secret_ref' AS secret_ref,
       source_config->'cursor'->>'style' AS cursor_style
  FROM pipeline.dataset_config
 WHERE domain = '<domain>'
   AND dataset = '<dataset>';
```

## Runtime verification

After data runs, verify runtime evidence separately:

```sql
SELECT file_id, state, s3_raw_path, s3_curated_path, last_run_id
  FROM pipeline.file_catalogue
 WHERE domain = '<domain>'
   AND dataset = '<dataset>'
 ORDER BY state_updated_at DESC
 LIMIT 10;
```

```sql
SELECT run_id, pipeline_type, status, started_at, ended_at, error_summary
  FROM pipeline.run_log
 WHERE domain = '<domain>'
   AND dataset = '<dataset>'
 ORDER BY started_at DESC
 LIMIT 10;
```

```sql
SELECT stage, event_type, status, record_count_in, record_count_out, error
  FROM pipeline.run_stage_log
 WHERE run_id = '<run_id>'
 ORDER BY started_at;
```

```sql
SELECT check_type, status, source_count, kafka_count, postgres_count, detail
  FROM pipeline.reconciliation_log
 WHERE domain = '<domain>'
   AND dataset = '<dataset>'
 ORDER BY created_at DESC
 LIMIT 10;
```

## Things this guide should not cover

Keep these in separate docs:

| Topic | Reason |
|---|---|
| Manual incident repair SQL | High-risk operational work; should have an explicit runbook. |
| Deep API pull design | Too much detail for onboarding. |
| Schema registry governance | Shared platform concern, not just dataset config. |
| Dashboard implementation | Runtime evidence consumer, not part of population. |
