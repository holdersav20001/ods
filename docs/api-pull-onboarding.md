# Onboarding a new API pull dataset

This guide walks through adding a new ``source_type=api_pull`` dataset to
the ODS pipeline. It assumes you have read
[docs/api-pull-ingestion-design.md](api-pull-ingestion-design.md) for
context on the overall flow.

## At a glance

```
new YAML in patterns/<domain>/<dataset>.yaml
        |
        v
dag_config_sync syncs to pipeline.dataset_config
        |
        v
secret_ref env var available to Airflow workers
        |
        v
dag_api_pull picks up the dataset on its next schedule
        |
        v
S3 archive -> file_catalogue -> dag_ingest -> Kafka -> sink -> recon
```

## Step 1 — Write the YAML

Add a file at ``patterns/<domain>/<dataset>.yaml``. See
``patterns/insurance/api_pull_demo.yaml`` for a complete reference.

Required top-level fields:

| Field | Purpose |
|---|---|
| `domain` | Logical domain (e.g. `insurance`). Pairs with `dataset` for PK. |
| `dataset` | Dataset name. |
| `source_type` | Must be `api_pull`. Picked up by `dag_api_pull`. |
| `pattern_type` | Must be `api`. Drives correlation field selection. |
| `key_fields` | Business key — list of column names. |
| `target_topic` | Raw Kafka topic. Convention: `ods.<domain>.<dataset>`. |
| `postgres_target_table` | Final JDBC sink table — schema-qualified. |
| `s3_curated_path` | `s3://ods-curated-<env>/<domain>/<dataset>/`. |
| `schema_id`, `canonical_schema_id` | Avro schema subjects (if registered). |
| `schema_version` | Schema version pin. |
| `data_classification` | `Internal`/`Restricted`/etc. — drives access controls. |
| `raw_format` | Set to `jsonl` for api_pull. |
| `dq_rules` | DQ rules block; `hard_blocks: []` for no hard blocks. |
| `recon_tolerance_records` / `recon_tolerance_pct` | Variance allowed before recon fails. |

Required `source` block (api_pull-specific — synced into
``dataset_config.source_config`` JSONB):

```yaml
source:
  application: "<source-system-name>"      # e.g. "policy-admin-api"
  url: "${MY_API_URL}"                      # env-substituted at poll time
  method: GET
  auth:
    type: bearer                            # slice 1 supports: none, bearer
    secret_ref: MY_API_TOKEN                # env var name — NEVER the token itself
  cursor:
    style: since_timestamp                  # slice 1: since_timestamp only
    request_param: updated_since            # query parameter sent to source
    response_field: updated_at              # field used to compute new watermark
    initial: "2026-01-01T00:00:00Z"         # used until first successful poll
  page:
    style: link_header                      # 'link_header' | 'none' (slice 1)
  timeout_seconds: 30
  retries: 3
```

Required `transform` block (mapping envelope ➜ canonical):

```yaml
transform:
  fields:
    - target: <key_field>
      source: _ods_source_request_id        # API pull correlation field
      required: true
    - target: <other_field>
      source: payload.<source_field>
      type: string
  required:
    - <key_field>
```

Required `schema_def` block (used by Glue schema validation if registry
returns 404):

```yaml
schema_def:
  fields:
    - name: <key_field>
      type: string
      required: true
```

## Step 2 — Configure the secret

Bearer tokens MUST come from env / Airflow Secrets Backend, not the
YAML. The YAML only carries `secret_ref` (the env var name).

Local development:

```bash
docker exec -it avivaods-airflow-scheduler-1 \
    bash -lc 'export MY_API_TOKEN=...' # or via .env mounted to the container
```

Production: register the secret in your Airflow Secrets Backend so
``MY_API_TOKEN`` is injected as an env var on workers.

`yaml_loader._scrub_secrets` strips ``token``, ``password``,
``client_secret`` and ``api_key`` from the YAML before persisting to
``dataset_config.source_config``, but **never put a real secret in
the YAML in the first place** — only ``secret_ref``.

## Step 3 — Sync the YAML to dataset_config

Trigger ``dag_config_sync`` (manual). This calls
``yaml_loader.sync_to_db`` which:

1. Computes a SHA-256 hash of the YAML; skips the row if unchanged.
2. Upserts ``pipeline.dataset_config`` with the standard columns plus
   ``raw_format`` and ``source_config`` (secrets scrubbed).
3. Increments ``config_version_id`` on every change.

Verify:

```sql
SELECT domain, dataset, source_type, raw_format,
       source_config->'cursor'->>'style' AS cursor_style,
       source_config->'auth'->>'secret_ref' AS secret_ref,
       active, config_version_id
  FROM pipeline.dataset_config
 WHERE domain='<domain>' AND dataset='<dataset>';
```

## Step 4 — Provision the rest of the pipeline

These are file-pattern-equivalent steps; they don't change for api_pull:

- **Avro schema**: register ``schema_id`` and ``canonical_schema_id``
  in Schema Registry (if you want strict schema validation rather than
  the 404 pass-through).
- **Postgres target table**: create ``postgres_target_table`` with the
  expected columns plus the ODS metadata columns
  (``_ods_run_id``, ``_ods_business_date``, ``_ods_ingested_at``,
  ``_ods_source_request_id``).
- **Kafka topics**: create raw + canonical topics.
- **JDBC sink connector**: provision the connector; the lookup name in
  ``dag_ingest.wait_sinks`` is
  ``f"jdbc-sink-{domain}-{dataset}".replace("_", "-")``.
- **S3 buckets**: ``ods-raw-<env>`` and ``ods-curated-<env>`` must
  exist.

## Step 5 — Verify

`dag_api_pull` runs every 15 minutes on its default schedule. After
the first run:

```sql
-- watermark advanced after the first successful E2E
SELECT * FROM pipeline.api_pull_watermark
 WHERE domain='<domain>' AND dataset='<dataset>';

-- archives registered
SELECT file_id, state, source_row_count, file_md5
  FROM pipeline.file_catalogue
 WHERE domain='<domain>' AND dataset='<dataset>'
 ORDER BY state_updated_at DESC LIMIT 5;

-- runs for both api_pull and downstream s3_batch
SELECT pipeline_type, status, started_at, ended_at
  FROM pipeline.run_log
 WHERE domain='<domain>' AND dataset='<dataset>'
 ORDER BY started_at DESC LIMIT 10;

-- recon row per archive batch
SELECT status, source_count, detail
  FROM pipeline.reconciliation_log
 WHERE check_type='api_pull_archive_count'
   AND domain='<domain>' AND dataset='<dataset>'
 ORDER BY created_at DESC LIMIT 5;
```
