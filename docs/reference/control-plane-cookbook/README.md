# Control-plane cookbook

How to use the `pipeline.*` control tables to build a pipeline, **one distinct chunk of work at a time**.
Both real workflows are just these blocks composed in different orders — so you learn the blocks once
and reuse them.

Each recipe follows the same shape: **Use case → You provide → You produce → Steps + control-table
interactions → Control records written → Failure handling (incl. DLQ) → Reconciliation → Done when →
Copy-paste skeleton.** Each step states *what you do* and *which control record it writes/reads* via the
real API, in the order that satisfies the lineage-before-status invariant.

## The blocks

| # | Block | Use case | Lineage edge written |
|---|-------|----------|----------------------|
| [01](01-ingest-raw-to-parquet.md) | Ingest raw → curated parquet | read raw, schema-validate, DQ, write curated parquet | `raw_to_curated` (1 edge) |
| [02](02-canonicalize-parquet.md) | Curated → canonical parquet | apply `transform.yaml` (rename/cast/drop) | `curated_to_canonical` (1 edge) |
| [03](03-merge-canonical-files.md) | Merge N canonical files → canonical | combine slots (core + enrichment) into one | `merge_to_canonical` (**N edges, 1 link** — the 1:N case) |
| [04](04-load-canonical-to-postgres.md) | Canonical → Postgres | upsert/append into `ods.*`, stamp lineage id | `canonical_to_postgres` (1 edge) |
| [05](05-dlq-and-replay.md) | DLQ + replay (cross-cutting) | quarantine bad data anywhere, drain + replay | `replay` (on re-run) |

## How the two workflows compose the blocks

**Single-file** (one file → Postgres):
```
[01 ingest] → [02 canonicalize] → [04 postgres]
```

**Multi-file** (two files merged → Postgres):
```
file10:  [01 ingest] → [02 canonicalize] ┐
file11:  [01 ingest] → [02 canonicalize] ┤→ [03 merge] → [04 postgres]
```

Block **05 (DLQ)** is not a stage — every block above routes its failures through it.

## Reading order

Start at [01](01-ingest-raw-to-parquet.md) — it explains the run/stage/lineage/finalise pattern in full.
02 and 04 are variations on it; 03 is the 1:N merge case; 05 is the cross-cutting failure path.

---

## Cross-cutting gaps surfaced while writing these (real, grounded)

These came out of checking each recipe against the actual jobs. They are the platform's current
inconsistencies — fix candidates, not doc errors. Each block flags its own; collected here so they
aren't lost.

1. **Bookkeeping is inconsistent across blocks.** The **ingest** job (block 01) writes the full trail:
   `run_stage_log` per stage **and** a `reconciliation_log` row. The **canonicalize** job (block 02)
   writes **neither** — no stage rows, no recon. Merge/postgres are in between. A pipeline-wide
   standard (every block writes stage rows + a recon check) would make the control plane uniform and
   the cookbook honest by default rather than by exception.

2. **Write-ordering invariant is inverted in two writers.** Blocks 03 and 04: target rows are committed
   **before** `write_link` commits the `lineage_link` (`ods_postgres_write.py` `_write_upsert:247`/
   `_write_append:175` vs `write_link` at `544-558`; `ods_merge.py:215` vs `:341`). A crash in between
   leaves `ods.*` rows whose `_ods_lineage_link_id` points at a link that doesn't exist. Correct order:
   `write_link` (commit) → commit rows → status succeeded.

3. **No FK on `_ods_lineage_link_id`.** The column is `NOT NULL` on every target table but has no
   `REFERENCES pipeline.lineage_link`. Provenance is enforced by convention only. (Add the FK *after*
   fixing #2 — today the inverted order would make the FK reject rows.)

4. **DLQ is half-built.** Only **DQ** failing rows are persisted today — `quality.evaluate` writes them
   to `s3://ods-dlq-<env>/…/run_id=…/`, but with **no control-plane row** (just `run_log.record_count_dq_fail`).
   The other four failure modes (schema, transform, merge, Postgres constraint) persist **nothing** at
   row level — they fail the whole run. Block 05 proposes the first-class `pipeline.dlq` table + the
   drain/replay path to close this. `Stage.DLQ_WRITE` already exists as an enum value with no call site.

5. **Silent transform data loss.** Block 02: Spark `cast` turns unparseable values into `NULL` rather
   than failing, so "failed transformations" pass through silently as NULLs — there is no transform DLQ.
   Decide: strict cast (reject → DLQ) vs lenient (NULL), and make it explicit.

6. **`merge_to_canonical` edge_type does not exist yet.** The current merge job goes straight to Postgres
   (`merge_to_postgres`). The workflows' merge-to-canonical-file step (block 03) needs this edge_type
   added to the taxonomy.

7. **Merge readiness is unspecified.** Block 03: how the merge knows all slots are present, the
   `business_date` alignment invariant, and the late/partial-arrival policy (block / proceed / re-merge)
   are design decisions the current code does not encode.

> Items 2–4 are the P0s from the control-plane architecture review
> (`docs/dev-guides/control-plane-architecture-review.md`).
