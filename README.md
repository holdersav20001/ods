# ODS Control Plane (Spark-free)

A clean, **Spark-free** implementation of the ODS control plane — the Postgres tables, plpgsql functions,
and Python client that record *what ran*, *what it produced*, *where data came from*, and *what failed*,
so any row is traceable to raw and any incident is reconstructable.

> **New session / new contributor: read this file, then `docs/specs/2026-05-29-control-plane-design.md`
> (the authoritative design). Everything you need is in this repo — no prior chat context required.**

## What this is (and isn't)

- **Is:** the control plane — `run_log`, `run_stage_log`, `lineage_link` + `lineage_edge`,
  `reconciliation_log`, `file_catalogue`, `dlq`, `dataset_config`, the `cp.*` functions, the Python
  client, and a **harness** that simulates pipeline stages by calling the real client with synthetic
  counts and string refs.
- **Isn't:** the data plane. **No Spark, no S3, no Kafka.** Stages are faked. The lineage logic is pure
  Postgres + Python, so it's tested with Postgres alone — fast, deterministic, CI-friendly.

## Core idea (read this before the schema)

Two complementary keys:
- **`workflow_run_id`** (= Airflow `dag_run_id`) **groups** every record of one pipeline execution →
  one-filter investigation. It does *not* span executions.
- **`lineage_edge`** carries **provenance/direction** and **spans** DAG runs (multi-file merge, late data,
  re-runs/replay). A re-run is a *new* `workflow_run_id`; the `lineage_edge` chain (+ a `replay` edge) is
  what ties it back to what it reprocessed.

So: **id for grouping, edges for provenance.** Both are kept; neither replaces the other.

## Status

- ✅ Design spec written: `docs/specs/2026-05-29-control-plane-design.md`.
- ⚠️ **Reviewed by 4 lenses (architect, senior dev, QA, lineage) → NOT build-ready as written.**
  See `docs/reviews/2026-05-29-design-review-consolidated.md` (5 CRITICAL + ~8 HIGH, converged).
- 👉 **FIRST task for the new session: revise the spec to v2** addressing the CRITICAL+HIGH items
  (function-signature appendix, atomic link+edges / write-ordering primitive, `workflow_run_id`
  synthetic-id format, `edge_type` lookup table + `canonical_to_sink`, replay-traces-to-raw,
  DLQ-as-edge, harness MUST-NOT rules, exhaustive contract test). **Then** Phase 1.
- ❓ 4 decisions to make first — see the review's "Decisions the user should make".

## Setup (do once, before Phase 1)

Postgres is reused from the running `avivaods-postgres-1` instance, in an **isolated database** so it
never collides with the existing `pipeline` schema:

```bash
# create the isolated DB (one time)
docker exec -i avivaods-postgres-1 psql -U ods -d postgres -c "CREATE DATABASE ods_cp;"
# connection used by migrations + client + tests:
#   host=localhost  port=5440  db=ods_cp  user=ods  password=ods
# all control tables live in schema  cp.*
```

If `avivaods-postgres-1` isn't running, start that stack's Postgres, or stand up any Postgres 15+ and
point the connection at it.

## Build order (phased — see spec §"Build order")

1. **Migrations** — `db/migrations/`: schema (`cp.*`) + `cp.*` functions, applied to `ods_cp`, plus a
   **contract test** that invokes every function against the migrated schema (guards function↔column drift).
2. **Python client** — `control/`: thin per-write-commit wrappers (`runs`, `lineage`, `stages`, `recon`,
   `dlq`) mirroring the proven `ods_ingestion_control`/`ods_pipeline` shape.
3. **Harness** — `harness/`: `fake_ingest`/`fake_canonicalize`/`fake_merge`/`fake_sink`/`fake_fail` calling
   the **real** client; composers `run_single_file` + `run_multi_file` (both share one `workflow_run_id`).
4. **Tests** — `tests/`: single-file, multi-file (N-edge merge), re-run discovery, replay edge,
   write-ordering (link before rows), FK rejects dangling link id, DLQ quarantine+replay, recon, contract.

## Why these decisions (context, not required reading)

This is a fresh rebuild informed by a full review of the existing `aviva ODS` control plane. The reasoning
lives in `docs/reference/` (copied in so this repo stands alone):

- `control-plane-architecture-review.md` — pros/cons, the bugs, the P0s. Explains **why no `orchestrators`,
  why no `run_events`, why the link FK, why `workflow_run_id`.**
- `control-plane-cookbook/` — the per-use-case recipes (ingest, canonicalize, merge=1:N lineage, sink,
  DLQ+replay, lineage-across-reruns). The conceptual building blocks. *Note: these reference the old aviva
  paths/jobs; the `docs/specs` design is authoritative for THIS repo.*

Key fixes baked into the design (so a re-run is reconstructable): jobs **discover** their upstream
(`latest_succeeded_run`) instead of trusting a passed id; anchor on `file_id`+`business_date`+`dataset`;
a re-run writes a `replay` edge; `lineage_edge` is the sole parentage; `business_date` recorded on every run.

## Out of scope

Spark, S3/LocalStack, Kafka, real sink drivers, the aviva ODS glue jobs. A later thin integration test
confirms the real Spark jobs call this client correctly.
