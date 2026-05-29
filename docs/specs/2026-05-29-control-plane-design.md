# ODS Control Plane — fresh design (Spark-free)

**Date:** 2026-05-29
**Status:** design, pending approval
**Location:** `C:\Users\Holde\development\ODS` (new repo)

## Purpose

A clean, Spark-free implementation of the ODS **control plane** — the tables, functions, and Python
client that record *what ran*, *what it produced*, *where data came from*, and *what failed* — so any row
is traceable to raw and any incident is reconstructable. The data plane (Spark reads/writes, S3, Kafka) is
**out of scope**: stages are simulated by a Python harness that calls the real control client with
synthetic counts and string refs.

This is a fresh rebuild informed by the review of the existing `aviva ODS` control plane — it keeps what
worked and fixes the diagnosed problems at the schema level instead of patching.

## Principles

1. **Two complementary keys.** `workflow_run_id` (= Airflow `dag_run_id`) **groups** every record of one
   pipeline execution → one-filter investigation. `lineage_edge` carries **provenance/direction** and
   spans DAG runs (multi-file merge, late data, replay) — which the id cannot. Keep both.
2. **Stateless writes.** Per-write commit, ordering enforced by call sequence. No cross-stage transaction.
3. **Lineage before consumer rows.** A write event's `lineage_link` (+edges) is committed *before* the
   rows it produced — enforced in the client and by FK.
4. **Spark-free testing.** Fakes call the real client; only Postgres is required.

## What changes vs the old design (fixed at the root)

- **`workflow_run_id` is native and `NOT NULL`** on every control table + stamped on target rows.
- **No `orchestrators` column.** `lineage_edge.upstream_run_id` is the sole run-parentage source.
  Orchestration-only links (e.g. api-pull trigger) are `lineage_edge` rows with an orchestration edge_type.
- **No `run_events` table.** Run history lives in `run_log` + `run_stage_log`; emit to Kafka later only if a
  consumer exists.
- **`_ods_lineage_link_id` is a real FK** to `lineage_link`.
- **`edge_type` is constrained** (CHECK or lookup table) — no free-text typos.
- **DLQ is native** from day 1 (`dlq` table + replay), not bolted on.
- **Uniform bookkeeping:** every stage writes `run_stage_log` + a `reconciliation_log` row.

## Schema (Postgres, database `ods_cp` on the running instance, port 5440)

All tables in schema `cp`. Every table has `workflow_run_id TEXT NOT NULL`.

| Table | Purpose | Key columns |
|-------|---------|-------------|
| `cp.dataset_config` | dataset registry | `domain,dataset` UNIQUE; `key_fields`, `write_mode`, `sink_type`, `sink_config`, `dq_rules` |
| `cp.file_catalogue` | physical file registry / state | `file_id` PK; `s3_raw_path`, `file_md5`, `business_date`, `state` |
| `cp.run_log` | one row per task execution | `run_id` PK; `workflow_run_id`, `pipeline_type`, `domain`, `dataset`, `business_date`, `file_id`→file_catalogue, `status`, counts |
| `cp.run_stage_log` | per stage+attempt (append) | `run_id`→run_log; `stage`, `attempt`, `status`, `record_count_in/out`, `metrics` |
| `cp.lineage_link` | the write-event bundle | `lineage_link_id` PK; `consumer_run_id`→run_log; `edge_type` (CHECK), `target_ref`, `record_count` |
| `cp.lineage_edge` | per-input contribution (1:N under a link) | `lineage_link_id`→lineage_link; `consumer_run_id`, `upstream_run_id`→run_log, `source_file_id`→file_catalogue, `input_slot`, `edge_type` (CHECK), `source_ref`, `record_count` |
| `cp.reconciliation_log` | count checks per hop | `run_id`→run_log; `check_type`, `source_count`, `accounted_count`, `discrepancy`, `status` |
| `cp.dlq` | quarantined failures + replay | `dlq_id` PK; `run_id`→run_log, `stage`, `reason`, `source_ref`, `payload_ref`, `record_count`, `replayed_at`, `replay_run_id`→run_log |

Target tables (simulated): `ods.<dataset>` carry `_ods_workflow_run_id` and `_ods_lineage_link_id UUID NOT NULL REFERENCES cp.lineage_link`.

`edge_type` allowed set: `raw_to_curated`, `curated_to_canonical`, `merge_to_canonical`, `canonical_to_sink`, `replay`, `orchestrates` (extensible via the lookup/CHECK).

## Control functions (plpgsql, `cp.*`)

`start_run`, `patch_run` (whitelisted keys), `register_file`, `start_stage`/`finish_stage`,
`write_lineage_link` (link + N edges, returns link_id), `write_reconciliation_check`, `quarantine` (dlq row).
All take `workflow_run_id`. A **contract test** invokes every function against the migrated schema (prevents
function↔column drift — the bug that broke the old `run_events`).

## Python client (`control/`)

Thin typed wrappers over `SELECT cp.<fn>(...)`, per-write commit (`commit=True` default), mirroring the
proven `ods_ingestion_control`/`ods_pipeline` shape: `runs.start/patch/finalise/latest_succeeded_run`,
`lineage.write_link`, `stages.stage_scope`, `recon.write_check`, `dlq.quarantine/replay`.

## Harness (`harness/`) — the Spark-free stages

Each stage is a Python function that calls the **real** client with synthetic data (row counts, fake
`s3://…` string refs):

- `fake_ingest(file)` → register_file, start_run(ingestion), stage rows, write_link `raw_to_curated`, recon, finalise.
- `fake_canonicalize(...)` → discovers ingest upstream, write_link `curated_to_canonical`, ...
- `fake_merge(slots)` → ONE link with **N edges** (`merge_to_canonical`, input_slot per slot).
- `fake_sink(...)` → write_link `canonical_to_sink` **before** stamping/committing target rows; FK enforced.
- `fake_fail(...)` → routes to `dlq.quarantine`; `replay` re-runs + writes a `replay` edge.

Composers: `run_single_file(...)` and `run_multi_file(...)` reproduce the two workflows end-to-end, all
sharing one `workflow_run_id`.

## Tests (`tests/`)

Against `ods_cp` (pytest). Assert:
- single-file: one `workflow_run_id` groups all runs; trace one `ods` row → raw via `lineage_edge`.
- multi-file: merge link has N edges; provenance spans the per-file runs.
- re-run: canon with a bogus upstream still links correctly (discovery); replay writes a `replay` edge.
- write-ordering: link exists before target rows; FK rejects a dangling `_ods_lineage_link_id`.
- DLQ: failure quarantined; `source == good + dlq` reconciles.
- contract: every `cp.*` function executes against the live schema.

## Out of scope

Spark, S3/LocalStack, Kafka, real sink drivers (Postgres/Snowflake mechanics), the aviva ODS glue jobs.
A later thin integration test confirms the real Spark jobs call this client with the right args.

## Build order (phased)

1. Migrations (schema + functions) + apply to `ods_cp` + contract test.
2. Python client.
3. Harness (fakes + composers).
4. Tests (single-file, multi-file, re-run, DLQ, ordering, FK).
