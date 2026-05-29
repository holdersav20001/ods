# ODS Control Plane — fresh design **v2** (Spark-free)

**Date:** 2026-05-29
**Status:** design, build-ready (supersedes `2026-05-29-control-plane-design.md`)
**Location:** `C:\Users\Holde\development\ODS`
**Incorporates:** the 4-lens consolidated review (`docs/reviews/2026-05-29-design-review-consolidated.md`) —
all 5 CRITICAL + 8 HIGH + the MEDIUM items, with the 4 user decisions resolved.

## User decisions (resolved — drive the changes below)

1. **`workflow_run_id` = bare UUID** for non-Airflow runs. Origin lives in two columns on `run_log`:
   `trigger_type ('airflow'|'manual'|'replay'|'dlq_drain')` and `replay_of_run_id` (nullable FK). **No
   prefix-string parsing.** **Column type is `TEXT NOT NULL`, not `uuid`** — Airflow `dag_run_id` is a
   string (`scheduled__2026-05-29T00:00:00`), so the column must hold both that and a stringified `uuid4`
   for non-Airflow runs (principle #1: `workflow_run_id` *is* the `dag_run_id`). *(resolves X3)*
2. **`canonical_to_sink` + `sink_type` on `lineage_link`** (`postgres`|`kafka`|`s3`|…). One generic sink
   edge_type; new sinks need **no migration**. *(resolves H-edge)*
3. **DLQ is a lineage edge** — `edge_type='quarantine'`, `is_provenance=true`, `target_ref` = DLQ payload.
   Provenance walk is rows-complete. *(resolves H-dlq)*
4. **`target_ref` carries a content hash + version now** — `{path, content_hash, version}` jsonb. Old links
   stay pinned to the exact bytes they logged; re-run overwrite can't silently re-point them. *(resolves Lineage M2)*
5. **Two *separate* restartability use cases — do not conflate.**
   - **Restart-a-task** (Airflow clear-task / retry): operator clears a failed or stale task in the Airflow
     UI; Airflow re-runs **from that task forward** under the **same `dag_run_id`** = **same `workflow_run_id`**.
     `trigger_type` stays `'airflow'`. No new chain. Re-execution is **idempotent**: `register_file` dedups on
     `(file_md5, business_date)`; `write_lineage_link` dedups on `(consumer_run_id, edge_type,
     target_ref->>'content_hash')` → re-running an unchanged task produces the **same link** (counts stable).
     This is intra-run recovery — the normal "restart from any task" Airflow gives you.
   - **Replay / refeed** (new or corrected file arrived): a **new** file (late data, vendor correction,
     reprocessing) → composer mints a **new `workflow_run_id`**, `trigger_type='replay'` (or `'manual'`/
     `'dlq_drain'`), `replay_of_run_id` → the original run. **Full provenance chain is re-written** plus a
     `replay` edge, so the refed row still traces to raw (X5). This is a *new execution that supersedes prior
     output*, not a re-run of the old one.

   **Boundary rule:** same input bytes + same `workflow_run_id` ⇒ restart-task (idempotent dedup). New bytes
   ⇒ refeed under a new `workflow_run_id`. **Mid-run clear-task with *changed* upstream content** (rare —
   parquet rewritten under a live run) produces a **new link** (content_hash differs, `ON CONFLICT` misses);
   the prior link is **not** auto-superseded. Operational requirement: an Airflow clear-task **must clear
   downstream tasks too**, so the changed content propagates and no stale canonical/sink link lingers. Recon
   flags the orphan if it doesn't.

---

## Purpose

A clean, Spark-free implementation of the ODS **control plane** — the tables, functions, and Python client
that record *what ran*, *what it produced*, *where data came from*, and *what failed* — so any row is
traceable to raw and any incident is reconstructable. The data plane (Spark, S3, Kafka) is **out of scope**:
stages are simulated by a Python harness that calls the **real** control client with synthetic counts and
string refs. Lineage logic is pure Postgres + Python → tested with Postgres alone.

## Principles

1. **Two complementary keys.** `workflow_run_id` **groups** every record of one pipeline execution
   (one-filter investigation; does not span executions). `lineage_edge` carries **provenance/direction**
   and **spans** DAG runs (merge, late data, replay). id for grouping, edges for provenance. Keep both.
2. **Atomic write-event.** A `lineage_link` and **all its edges** are written in **one transaction** — the
   one place atomicity is non-negotiable (X1). Everything else is per-write commit.
3. **Lineage before consumer rows.** Target rows are stamped/committed **only** through the sanctioned sink
   primitive, which writes the link+edges first. Enforced in client *and* by FK (X1).
4. **Discovery, not trust.** A stage **discovers** its upstream via `latest_succeeded_run(...)`; it never
   trusts a passed-in upstream id (H-discovery). Anchor on `file_id` + `business_date` + `dataset`.
5. **Spark-free testing.** Fakes call the real client; only Postgres is required.

---

## Schema (Postgres, database `ods_cp`, port 5440, schema `cp`)

Every control table has `workflow_run_id TEXT NOT NULL` (holds Airflow string `dag_run_id` **or** a
stringified `uuid4` for non-Airflow runs). `business_date DATE NOT NULL` wherever a run/file
appears — never defaulted, never nullable (kills the old `=None` bug).

| Table | Purpose | Key columns |
|-------|---------|-------------|
| `cp.edge_type` | **lookup** for edge taxonomy (X2/H-edge) | `edge_type TEXT PK`, `is_provenance BOOLEAN NOT NULL` |
| `cp.dataset_config` | dataset registry | `domain,dataset` UNIQUE; `key_fields jsonb`, `write_mode`, `sink_type`, `sink_config jsonb`, `dq_rules jsonb` |
| `cp.file_catalogue` | physical file registry / state | `file_id UUID PK`; `s3_raw_path`, `file_md5`, `business_date DATE NOT NULL`, `state` |
| `cp.run_log` | one row per task execution | `run_id UUID PK`; `workflow_run_id TEXT NOT NULL`, **`trigger_type`**, **`replay_of_run_id UUID→run_log`**, `pipeline_type`, `domain`, `dataset`, `business_date DATE NOT NULL`, `file_id→file_catalogue`, `status`, counts |
| `cp.run_stage_log` | per stage+attempt (append) | `run_id→run_log`; `stage`, `attempt`, `status`, `record_count_in/out`, `metrics jsonb` |
| `cp.lineage_link` | the write-event bundle ("1 link per write-event, **N per run**") | `lineage_link_id UUID PK`; `consumer_run_id UUID→run_log`, `edge_type→cp.edge_type`, **`sink_type`** (null unless `canonical_to_sink`), `target_ref jsonb {path,content_hash,version}`, **`transform_version`**, `record_count` |
| `cp.lineage_edge` | per-input contribution (1:N under a link) | `lineage_link_id→lineage_link`, `upstream_run_id UUID→run_log`, `source_file_id→file_catalogue`, `input_slot`, `edge_type→cp.edge_type`, `source_ref jsonb`, `record_count` — **no `consumer_run_id`** (derive via the link FK; M1) |
| `cp.reconciliation_log` | count checks per hop | `run_id→run_log`; `check_type`, `source_count`, `accounted_count`, `discrepancy`, `status`, `metrics jsonb` (per-slot merge counts; M5) |
| `cp.dlq` | quarantined failures + replay | `dlq_id UUID PK`; `run_id→run_log`, `stage`, `reason`, `source_ref jsonb`, `payload_ref`, `record_count`, `replayed_at`, `replay_run_id UUID→run_log` |

Target tables (simulated): `ods.<dataset>` carry `_ods_workflow_run_id TEXT` and
`_ods_lineage_link_id UUID NOT NULL REFERENCES cp.lineage_link`.

**`cp.edge_type` seed rows:**

| edge_type | is_provenance |
|-----------|---------------|
| `raw_to_curated` | true |
| `curated_to_canonical` | true |
| `merge_to_canonical` | true |
| `canonical_to_sink` | true |
| `quarantine` | true |
| `replay` | true (annotation; chain still written — X5) |
| `orchestrates` | **false** (trigger edge; excluded from trace-to-raw — H-trigger) |

**Trace-to-raw view** `cp.v_provenance` walks `lineage_edge`→`lineage_link` filtering
`edge_type.is_provenance = true`, so `orchestrates` triggers never pollute a provenance walk (H-trigger).

---

## Function-signature appendix (authoritative — X2)

**One verb set.** `write_lineage_link` writes the link **+ all N edges in one transaction** and supersedes
any per-edge `write_edge`. Each row = name, args(types), returns, ON CONFLICT key, commit responsibility.

```
cp.start_run(
    p_workflow_run_id  text,           -- Airflow dag_run_id OR stringified uuid4
    p_pipeline_type    text,
    p_domain           text,
    p_dataset          text,
    p_business_date    date,            -- NOT NULL
    p_trigger_type     text,            -- 'airflow'|'manual'|'replay'|'dlq_drain'
    p_file_id          uuid   = null,
    p_replay_of_run_id uuid   = null
) RETURNS uuid                          -- run_id
  -- ON CONFLICT: none (new run_id each call). Commits its own row.
  -- If p_trigger_type='orchestrates', also writes an orchestration lineage_edge
  --   (is_provenance=false) recording the trigger; no upstream_run_id/source_file_id required.

cp.patch_run(p_run_id uuid, p_patch jsonb) RETURNS void
  -- whitelisted keys only {status,record_count_in,record_count_out,error}. Own commit.

cp.register_file(
    p_workflow_run_id text, p_s3_raw_path text, p_file_md5 text,
    p_business_date date, p_domain text, p_dataset text
) RETURNS uuid                          -- file_id
  -- ON CONFLICT (file_md5, business_date) DO UPDATE state -> returns existing file_id (idempotent).

cp.start_stage(p_run_id uuid, p_stage text, p_attempt int) RETURNS bigint   -- stage_log_id
cp.finish_stage(p_stage_log_id bigint, p_status text,
                p_in bigint, p_out bigint, p_metrics jsonb) RETURNS void

cp.write_lineage_link(
    p_consumer_run_id   uuid,
    p_edge_type         text,           -- FK cp.edge_type
    p_target_ref        jsonb,          -- {path, content_hash, version}
    p_record_count      bigint,
    p_edges             jsonb,          -- [{upstream_run_id, source_file_id, input_slot, edge_type, source_ref, record_count}, ...]
    p_sink_type         text = null,    -- required iff edge_type='canonical_to_sink'
    p_transform_version text = null
) RETURNS uuid                          -- lineage_link_id
  -- ATOMIC: link + all N edges in ONE transaction (X1). Raises if p_edges is empty
  --   ("every link has >=1 edge" — enforced at write, re-checked by recon).
  -- ON CONFLICT (consumer_run_id, edge_type, target_ref->>'content_hash') DO NOTHING
  --   -> returns existing link_id (idempotent replay — QA H1).

cp.write_link_then_rows(
    p_consumer_run_id uuid, p_edge_type text, p_target_ref jsonb,
    p_record_count bigint, p_edges jsonb, p_rows jsonb,
    p_sink_type text = null, p_transform_version text = null
) RETURNS uuid                          -- lineage_link_id
  -- The ONLY sanctioned path to stamp/commit target rows. Calls write_lineage_link,
  --   THEN stamps _ods_lineage_link_id on rows, all ordered so no row exists without a committed link.

cp.write_reconciliation_check(
    p_run_id uuid, p_check_type text, p_source_count bigint,
    p_accounted_count bigint, p_metrics jsonb = null
) RETURNS void                          -- status/discrepancy computed; own commit.

cp.quarantine(
    p_run_id uuid, p_stage text, p_reason text,
    p_source_ref jsonb, p_payload_ref text, p_record_count bigint
) RETURNS uuid                          -- dlq_id; also writes a 'quarantine' lineage_edge (decision 3).

cp.latest_succeeded_run(
    p_domain text, p_dataset text, p_business_date date, p_pipeline_type text
) RETURNS uuid                          -- run_id of newest status='succeeded' match
  -- tie-break: ORDER BY finished_at DESC, run_id DESC LIMIT 1. THIS is how a stage discovers upstream.
```

A **contract test** (see Tests) enumerates `pg_proc` in schema `cp` and fails if any function is
un-asserted or its returned columns drift from the table definitions (H-contract).

---

## Python client (`control/`)

Thin typed wrappers over `SELECT cp.<fn>(...)`, per-write commit (`commit=True` default), mirroring the
proven `ods_ingestion_control`/`ods_pipeline` shape:

- `runs.start / patch / finalise / latest_succeeded_run`
- `lineage.write_link(...)` (link + N edges, atomic) and `lineage.write_link_then_rows(...)` (sink)
- `stages.stage_scope(...)` (context manager → start_stage/finish_stage)
- `recon.write_check(...)`
- `dlq.quarantine(...) / replay(...)`

`workflow_run_id` is **minted once by the composer** (a `uuid4`) and passed as a **required arg** to every
stage — never defaulted or minted per-stage (X3/X4).

---

## Harness (`harness/`) — the Spark-free stages

Each stage calls the **real** client with synthetic data (row counts, fake `s3://…` refs, fake hashes):

- `fake_ingest(file, workflow_run_id)` → register_file, start_run(ingestion), stages, `write_link raw_to_curated`, recon, finalise.
- `fake_canonicalize(..., workflow_run_id)` → **discovers** ingest upstream via `latest_succeeded_run`, `write_link curated_to_canonical` (carries `transform_version`), recon.
- `fake_merge(slots, workflow_run_id)` → ONE link with **N edges** (`merge_to_canonical`, distinct `input_slot`, distinct discovered `upstream_run_id`); per-slot counts in recon `metrics`.
- `fake_sink(..., workflow_run_id)` → `write_link_then_rows canonical_to_sink` with `sink_type` — link committed **before** rows; FK enforces.
- `fake_fail(...)` → `dlq.quarantine` (writes `quarantine` edge). `replay` mints a **new** `workflow_run_id`, `trigger_type='replay'`, `replay_of_run_id=<orig>`, and **re-writes the full provenance chain** (`raw_to_curated`…) **plus** a `replay` edge — so a replayed row still traces to raw (X5).

Composers `run_single_file(...)` and `run_multi_file(...)` mint **one** `workflow_run_id` and thread it.

### Harness MUST-NOT rules (pinned — X4)

The harness exists to exercise the client, not substitute for it. Fakes **MUST NOT**:
- hand-build `lineage_edge`/`lineage_link` rows or `lineage_link_id`s (only via `write_lineage_link`);
- pass an upstream `run_id` into the link path — upstream comes **only** from `latest_succeeded_run`;
- mint their own `workflow_run_id` — it arrives from the composer;
- insert/stamp a target row before the link commits — only `write_link_then_rows`.

**Coverage boundary ("what fakes cannot prove"):** fakes do not prove the *real* Spark jobs call the client
correctly (sequence/args) — that is the job of the **adapter-contract test** (mock client, assert the real
job's call-sequence: `write_link` before any row-write) and a later thin integration test. State this
boundary explicitly in `tests/README`.

---

## Tests (`tests/`, pytest, transactional rollback per test — L4)

**Happy-path / structure**
- single-file: one `workflow_run_id` groups all runs; trace one `ods` row → raw via `v_provenance`.
- multi-file merge: link has N edges; assert distinct `input_slot`, distinct non-null `upstream_run_id`, `SUM(edge.record_count) == link.record_count` (M1).
- **fan-out**: one run writes **two** `canonical_to_sink` links (postgres + kafka); query "rows that went to kafka" via `sink_type`; assert `child.record_count == parent.record_count` per sink link (H-edge/H-recon).

**Atomicity / ordering / FK**
- write-ordering: abort between link-commit and row-commit → **zero rows without a link** (X1).
- FK rejection on **every** FK: dangling `consumer_run_id`, `upstream_run_id`, `source_file_id`, `_ods_lineage_link_id`, `dlq.run_id`, `dlq.replay_run_id` (H-fk).
- `write_lineage_link` with empty edges → **raises** ("every link ≥1 edge").

**Discovery / replay**
- re-run: canon given a **bogus** upstream id still links correctly because it discovers (H-discovery).
- replay traces to raw: replayed row walks `v_provenance` → raw, with a `replay` edge present (X5).
- idempotent replay: replay twice → stable row + link counts; original recon unchanged (QA H1).

**Recon (non-vacuous — H-recon)**
- `good + dlq == source` reconciles; `good + dlq < source` → **breach**; `> source` → **double-count**.
- "every `lineage_link` has ≥1 edge" recon check.

**DLQ / quarantine**
- failure quarantined; `quarantine` edge present so provenance walk is rows-complete (decision 3).
- double-drain guard; `replayed_at`/`replay_run_id` bookkeeping (QA M5).

**Contract / concurrency / misc**
- contract: enumerate `cp.*` from `pg_proc`, fail if any un-asserted; assert returned columns vs tables (H-contract).
- concurrency: two writers under one `workflow_run_id` → no lost/dup links (M3).
- NOT-NULL rejection for `workflow_run_id` (QA H2); bad `edge_type` rejected by FK (QA M4).
- transform: silent cast-to-NULL is treated as a **quarantine** event, not a silent mutation (Lineage M1).

---

## Migration runner (M1)

Ordered `db/migrations/NNN_*.sql` applied by a tiny psql runner to `ods_cp`:
```bash
for f in db/migrations/[0-9]*.sql; do
  docker exec -i avivaods-postgres-1 psql -U ods -d ods_cp -v ON_ERROR_STOP=1 -f - < "$f"
done
```
`001_schema.sql` (tables + `cp.edge_type` seed + `v_provenance`), `002_functions.sql` (all `cp.*`),
`003_targets.sql` (sample `ods.<dataset>`). Contract test runs after apply.

## Out of scope

Spark, S3/LocalStack, Kafka, real sink drivers, the aviva ODS glue jobs. Merge **readiness** is the
orchestrator's job — `fake_merge` asserts caller-supplied slots, does not decide completeness (H5). A later
thin integration test confirms the real Spark jobs call this client with the right args.

## Build order (phased)

1. **Migrations** — `001`–`003` + apply to `ods_cp` + **exhaustive contract test** (gates the rest).
2. **Python client** — `control/` wrappers, composer mints+threads `workflow_run_id`.
3. **Harness** — fakes + composers, MUST-NOT rules honoured; adapter-contract test.
4. **Tests** — the full matrix above.
