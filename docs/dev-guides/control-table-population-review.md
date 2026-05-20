# Control table population - review draft

> Draft for review. This is intended to simplify the existing control-plane
> documentation, not replace it until the team agrees the wording is right.

## Short version

The control tables make sense if you split them into two groups:

| Group | Tables | Who populates them |
|---|---|---|
| Dataset configuration | `pipeline.dataset_config` | Humans write YAML; `dag_config_sync` syncs it into Postgres. |
| Runtime evidence | `pipeline.file_catalogue`, `pipeline.run_log`, `pipeline.run_stage_log`, `pipeline.lineage_edge`, `pipeline.reconciliation_log` | The platform writes these while files, API pulls, or events are processed. |

Do not manually insert runtime evidence rows during normal onboarding. Add or
change dataset YAML, run config sync, then let the DAGs and helper modules write
the audit/state rows.

## The main idea

`pipeline.dataset_config` answers: "What is this dataset and how should the
platform process it?"

The runtime tables answer: "What actually happened for a specific file,
request, event, or run?"

Keeping those questions separate removes most of the apparent complexity.

## Configuration flow

For a normal dataset onboarding:

```text
YAML file
  -> dag_config_sync
  -> airflow.dags.common.yaml_loader.sync_to_db
  -> pipeline.dataset_config
  -> downstream DAGs route and execute from that row
```

The sync code validates the YAML before it writes the row. It catches structural
mistakes such as:

| Rule | Why it exists |
|---|---|
| `source_type=s3_batch` needs `filename_pattern` | File scanning needs to know which files belong to the dataset. |
| Kafka deliveries need `target_topic` | Publish and sink waits need a topic to observe. |
| `delivery=direct_postgres` must not set Kafka topics | That route deliberately skips Kafka. |
| `write_mode=upsert` needs `key_fields` | The Postgres merge needs a business key. |
| API pull with bearer auth needs `secret_ref` | Secrets should be resolved at runtime, not stored in YAML or Postgres. |

## Runtime flow

Runtime rows are written by DAG tasks and helper modules as stateless durable
facts:

| Table | Meaning |
|---|---|
| `pipeline.file_catalogue` | A physical file or archive was discovered and registered. |
| `pipeline.run_log` | A pipeline attempt exists and has a current or final status. |
| `pipeline.run_stage_log` | Individual stages started, completed, warned, skipped, or failed. |
| `pipeline.lineage_edge` | One run/file caused another run. |
| `pipeline.reconciliation_log` | Source, Kafka, archive, or target counts were compared. |

The helpers intentionally commit each meaningful transition as its own recovery
point. A restarted worker does not need in-memory state from the old process; it
can inspect Postgres to decide whether work is still running, already completed,
safe to retry, or stale enough for the janitor to fail.

The important success invariant is:

```text
stage evidence -> archive/output evidence -> reconciliation evidence -> run status succeeded
```

`succeeded` is written last. A run may be `running`, `failed`, or `partial` with
incomplete evidence, especially after a crash. It should not be marked
`succeeded` until the required evidence rows have already been committed.

## What to populate manually

Usually, only these things are manual:

| Thing | Where |
|---|---|
| Dataset config YAML | `datasets/<domain>/<dataset>.yaml` or the agreed sync folder. |
| Avro/schema files | `schemas/<domain>/...` |
| Target table migrations | `db/migrations/...` |
| Secrets | Environment variables or Airflow Secrets Backend. |

Everything below should normally be written by the platform:

| Do not manually populate | Writer |
|---|---|
| `pipeline.file_catalogue` | `dag_drop_to_raw` or API archive registration. |
| `pipeline.run_log` | `ods_pipeline.runs` helpers. |
| `pipeline.run_stage_log` | `ods_pipeline.stages` helpers. |
| `pipeline.lineage_edge` | `ods_pipeline.lineage` helpers. |
| `pipeline.reconciliation_log` | `ods_pipeline.reconciliation` helpers. |

Manual repair may still be needed during incidents, but that should be covered
by a separate runbook, not the onboarding guide.

## Current sources of confusion

These are the items I would resolve before calling the docs final:

| Issue | Why it is confusing | Proposed resolution |
|---|---|---|
| "Control plane" means both config and runtime evidence | Readers cannot tell whether they are meant to populate YAML or audit tables. | Use "dataset config" for YAML/`dataset_config`; use "runtime evidence" for run/stage/recon/lineage tables. |
| API pull guide says new YAML goes under `patterns/` | `dag_config_sync` currently scans `DATASETS_DIR`, defaulting to `/opt/airflow/datasets`. | Pick one convention and make code/docs agree. |
| `dataset_config` contains many unrelated fields | It is doing routing, schema, DQ, sink, API source, and versioning. | Keep it for now, but document field groups rather than one long flat list. |
| Runtime helpers use stateless durable writes | Independent commits are the mechanism, but restartability is the architectural goal. | Document the stateless contract once: each meaningful transition is a committed recovery point, and `succeeded` is written only after required evidence exists. |
| `messages.py` has a stale transaction comment | It says callers must use `commit=False`, while the current helper uses stateless durable writes. | Remove or rewrite the stale comment. |

## Review questions

1. Should the official sync source be `datasets/`, `patterns/`, or both?
2. Should event pattern YAML be synced into `dataset_config`, or should event config stay separate?
3. Is `dataset_config` still the right long-term home for API `source_config`, or should that move to a pattern-specific config table later?
4. Do operators need a separate incident runbook for manual table repairs?
