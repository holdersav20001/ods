# Multi-File Staging + Merge Pipeline Design

**Date:** 2026-04-29  
**Status:** Implemented (Option A)

---

## Problem

A single wide Postgres table must be populated from multiple CSV files that arrive at different times. Each file owns a disjoint set of columns for the same logical key (`policy_id`). Requirements: full per-column lineage, safe rerun of any individual slot, observable merge state, crash-safe restarts.

---

## Option A — Staging Tables + Merge Job (CHOSEN)

Each file (slot) writes to a narrow staging table. A merge job runs once all required slots are staged for a given `business_date`, producing the wide target row via a full-outer join.

```
File 1 (core)        → S3 raw → ods_stage.py → pipeline.slot_staging_core
File 2 (enrichment)  → S3 raw → ods_stage.py → pipeline.slot_staging_enrichment
                                                         ↓
                                               ods_merge.py (full-outer join)
                                                         ↓
                                               ods.policies_enriched
                                                         ↓
                                         pipeline.merge_contribution_log
                                         (per-slot, per-column lineage)
```

**Lineage chain:**
```
ods.policies_enriched._ods_merge_run_id
  → pipeline.merge_run_log.merge_run_id
  → pipeline.merge_contribution_log (slot_name, slot_run_id, columns_written, s3_raw_path)
  → pipeline.run_log (per-slot run with file_id)
  → pipeline.file_catalogue (file_md5, sftp_path, s3_raw_path)
```

**Pros:**
- Complete slot isolation — rerunning core doesn't touch enrichment rows
- Crash-safe — DELETE-before-INSERT on both staging and target is idempotent
- Observable — `run_log` entry per slot, `merge_run_log` per merge, `merge_contribution_log` per column group
- Column-level attribution built in at merge time
- Fits existing batch pattern exactly

**Cons:**
- Requires readiness gate (all slots must be staged before merge triggers)
- Extra staging tables per slot
- Merge must be re-triggered after any slot rerun

---

## Option B — UPSERT with JSONB Audit Column

Single wide target table. Each file UPSERTs only its columns via `INSERT ... ON CONFLICT (policy_id) DO UPDATE SET col1=EXCLUDED.col1, ...`. A `_ods_sources JSONB` column accumulates `{run_id, slot, columns, arrived_at}` entries.

**Pros:** No staging tables; no coordination (each slot fires independently).

**Cons:**
- Race condition if both slots arrive simultaneously (last write wins on shared columns)
- Rerunning slot 1 risks overwriting slot 2's columns if ON CONFLICT logic is wrong
- Column-level lineage requires JSONB functions, not a JOIN
- Partial rows visible to consumers between slot 1 and slot 2 arrival
- JSONB grows unbounded; old entries require manual pruning

---

## Option C — Event Sourcing + Materialised View

Each slot file appends Kafka events tagged with `slot`. A stream processor (Kafka Streams / ksqlDB) maintains a materialised view that merges latest event per `(policy_id, slot)`. Postgres wide table is populated from the stream.

**Pros:** Real-time merge; natural immutable audit log; pure event-driven.

**Cons:**
- Adds stream processor not in current stack (significant new dependency)
- Rerun semantics require replaying Kafka — complex with Schema Registry + offset tracking
- T2 reconciliation must query stream state, not just Postgres
- Out of scope for batch ODS pattern

---

## Implementation Detail (Option A)

### Slot Configuration (`pipeline.dataset_config` additions)

| Column | Type | Purpose |
|--------|------|---------|
| `slot_name` | VARCHAR | e.g. `core`, `enrichment`; NULL for non-slot datasets |
| `merge_dataset` | VARCHAR | Name of the merge target dataset this slot feeds |
| `staging_table` | VARCHAR | Postgres table where this slot's rows are staged |

### New Tables

| Table | Purpose |
|-------|---------|
| `pipeline.slot_staging_core` | Staged rows from core CSV (keyed policy_id + business_date) |
| `pipeline.slot_staging_enrichment` | Staged rows from enrichment CSV |
| `pipeline.merge_run_log` | One row per merge execution |
| `pipeline.merge_contribution_log` | Per-slot lineage row per merge (columns_written, s3_raw_path) |
| `ods.policies_enriched` | Wide target table with `_ods_merge_run_id`, `_ods_run_id_core`, `_ods_run_id_enrich` |

### Rerun Semantics

- **Slot rerun:** `ods_stage.py` DELETEs staging rows for `_ods_business_date` before inserting, so reruns replace that slot cleanly. Other slots untouched.
- **Merge rerun:** `ods_merge.py` DELETEs target rows for `_ods_business_date` before inserting. Idempotency check (status=succeeded for same bd) prevents duplicate merges.
- **Deterministic merge_run_id:** Derived as `uuid5(NAMESPACE_OID, "{domain}/{dataset}/{business_date}")` — same business_date always maps to same `merge_run_id`, enabling safe DAG retries.

### Readiness Gate

`check_all_slots_ready` in `dag_multi_file.py` queries `pipeline.run_log` for `status='succeeded' AND pipeline_type='stage'` rows for all slot datasets that feed the merge target. Only when all slots are present does the merge trigger.
