# lineage_link + Autonomous Tasks — File-Batch Routes

**Scope:** file-batch routes only (`source_type='s3_batch'`). API-pull and event-driven routes are unchanged for now.

**Reason for change:** the existing `lineage_edge` table is the right shape, but two real-world cases were uncomfortable:

1. A stage that **merges multiple raw files** into one target write — the load run writes N edges that all share `consumer_run_id`. From a target row, finding "all the source files that fed this row" needs a scatter-gather query and `_ods_file_id` is forced to pick one file (lossy).
2. The current `init_run` task **pre-allocates UUIDs for every downstream task at DAG start**. That couples DAG topology to the control plane: add a new stage and `init_run` must change; non-Airflow orchestrators (EventBridge, Step Functions, manual cron) have no equivalent. Failures mid-route leave orphan run rows.

This guide describes the new pattern that fixes both.

---

## 1. `lineage_link` — one row per consumer write event

```text
lineage_link (the bundle)
    lineage_link_id   UUID PK
    consumer_run_id   the run that did the write
    edge_type         raw_to_curated, curated_to_postgres, silver_to_gold, ...
    target_ref        where the write went (S3 path, JDBC URI, ...)
    record_count      total rows written
    created_at        timestamp

lineage_edge (the contributions, one per source)
    + lineage_link_id (NEW: FK back to the bundle)
    + slot_name       (NEW: optional role: core, enrichment, lookup, ...)
    consumer_run_id, upstream_run_id, source_file_id,
    edge_type, source_ref, target_ref, record_count
```

**Target rows in `ods.*` gain a single column:**

```text
_ods_lineage_link_id   UUID    -- single-column handle to the write event
```

Old `_ods_file_id` / `_ods_run_id` columns stay (back-compat) but are no longer the primary lineage handle.

---

## 2. Write pattern

Every consumer write goes through one helper call:

```python
ods_pipeline.lineage.write_link(
    conn,
    consumer_run_id=run_id,
    edge_type="curated_to_postgres",
    target_ref="jdbc:postgresql://.../ods.insurance_policy",
    record_count=98,
    contributions=[
        {
            "upstream_run_id": ingest_run_for_file1,
            "source_file_id":  file1_id,
            "source_ref":      "s3://ods-curated/.../file1/",
            "slot_name":       None,
            "record_count":    50,
        },
        {
            "upstream_run_id": ingest_run_for_file2,
            "source_file_id":  file2_id,
            "source_ref":      "s3://ods-curated/.../file2/",
            "slot_name":       None,
            "record_count":    48,
        },
    ],
)
# Returns lineage_link_id — stamp it on every target row written by this event.
```

The helper:
1. Mints a new `lineage_link_id` (UUID4).
2. INSERTs the `lineage_link` row.
3. INSERTs N `lineage_edge` rows, all sharing the link_id.
4. Returns the `lineage_link_id`.

All N+1 INSERTs happen in one transaction.

Single-source case: `contributions=[{...one entry...}]`. Same code path. Same shape.

---

## 3. Trace from a target row

A single JOIN reaches every contributor:

```sql
SELECT le.source_file_id,
       le.upstream_run_id,
       le.source_ref,
       le.slot_name,
       le.record_count
  FROM ods.insurance_policy t
  JOIN pipeline.lineage_edge le
    ON le.lineage_link_id = t._ods_lineage_link_id::uuid
 WHERE t.policy_id = 'POL-001';
```

Single-source row → 1 result.
Multi-source row → N results.

Or use the helper view:

```sql
SELECT *
  FROM pipeline.v_lineage_link_sources
 WHERE lineage_link_id = (
   SELECT _ods_lineage_link_id::uuid
     FROM ods.insurance_policy
    WHERE policy_id = 'POL-001'
 );
```

---

## 4. Autonomous task pattern (replaces `init_run` pre-allocation)

### What `init_run` used to do

```python
@task
def init_run():
    parent_run_id = uuid4()       # pre-mints orchestration UUID
    ingest_run_id = uuid4()       # pre-mints ingestion UUID
    pg_write_run_id = uuid4()     # pre-mints load UUID
    # writes all 3 to run_log, passes via XCom
```

### What each autonomous task does instead

```python
def my_task(conn, *, file_id, dag_run_id):
    # 1. Mint MY OWN run_id at the moment I start
    my_run_id = uuid4()

    # 2. Discover my upstream from CONTROL TABLES (not from upstream task's XCom)
    upstream_ingest_run = lookup_latest_ingest_run_for_file(conn, file_id)

    # 3. Register my run
    ods_pipeline.runs.start(
        conn, run_id=my_run_id, pipeline_type="direct_postgres",
        file_id=file_id,
        # orchestrators carries the Airflow dag_run_id, not pre-allocated parent UUIDs
        orchestrators=[{"dag_run_id": dag_run_id, "edge_type": "scheduled_by"}],
    )

    # 4. Do work, stage rows etc

    # 5. Atomic finalise: lineage_link + lineage_edge + status + file state
    link_id = ods_pipeline.lineage.write_link(
        conn,
        consumer_run_id=my_run_id,
        edge_type="curated_to_postgres",
        target_ref="...",
        record_count=N,
        contributions=[
            {"upstream_run_id": upstream_ingest_run, "source_file_id": file_id,
             "source_ref": curated_path, "record_count": N},
        ],
    )
    write_target_rows(stamp=_ods_lineage_link_id := link_id)
    ods_pipeline.runs.update(conn, my_run_id, status="succeeded",
                             record_count_source=N, record_count_target=N)
```

### Key properties

| Property | Why it matters |
|---|---|
| Task mints own `run_id` when work starts | No orphan rows from skipped tasks. No pre-allocation. |
| Task discovers upstream from control tables, not XCom | Works under Airflow, EventBridge, Step Functions, cron, manual. |
| Task records its own lineage at the end | Stateless. Survives orchestrator restart. |
| `orchestrators[]` carries `dag_run_id`, not a pre-minted parent UUID | Airflow gives `dag_run_id` for free; no extra control-plane wiring. |
| No required handoff between tasks | Add or remove tasks without touching `init_run`. |

### Upstream lookup helper

The "find my upstream" lookup is the only new helper needed. For the direct-Postgres load task:

```python
def lookup_latest_succeeded_ingest_run(conn, file_id):
    """Return the most recent succeeded ingestion run_id for this file.

    Used by autonomous load tasks to discover their data parent without
    requiring the upstream task to pass it via XCom.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT run_id FROM pipeline.run_log
             WHERE file_id = %s
               AND pipeline_type = 'ingestion'
               AND status = 'succeeded'
             ORDER BY ended_at DESC LIMIT 1
            """,
            (file_id,),
        )
        row = cur.fetchone()
        return row[0] if row else None
```

For multi-source loads, the load task takes a list of `file_id`s in its event payload / CLI arg and does the lookup per file.

---

## 5. Airflow vs EventBridge vs cron — same pattern

```
ANY trigger
   │
   ▼
Task starts
   │
   ▼
mints run_id          ─── self-contained
discovers upstream    ─── via control tables, not XCom
does work
writes lineage_link   ─── atomic
closes run
   │
   ▼
ANY next trigger
```

Airflow: triggered by DagRun scheduler. `dag_run_id` recorded in `orchestrators[]`.
EventBridge: triggered by event rule. `event_id` (or rule ARN) recorded in `orchestrators[]`.
Cron: triggered by scheduler. `cron_schedule_id` (or `now()`) recorded.

The control-plane writes are identical in all three cases.

---

## 6. What's NOT in this scope

| Out of scope | Why |
|---|---|
| API-pull routes | Watermark + cursor logic is route-specific. Folded in a later migration. |
| Event-driven routes | Same. |

Everything else IS in scope:

* File-batch target tables are **TRUNCATEd** (clean slate, no backfill).
* Legacy `_ods_run_id` and `_ods_file_id` columns are **DROPPED** from file-batch target tables. `_ods_lineage_link_id` (NOT NULL) is the sole lineage handle.
* `dag_ingest_direct_postgres` is rewritten — no `init_run` pre-allocation. Each task mints its own `run_id` and discovers upstream from control tables.
* `pipeline.merge_run_log` and `pipeline.merge_contribution_log` are **dropped**. `lineage_link` replaces both.
* Multi-source `policies_enriched` columns `_ods_run_id_core`, `_ods_run_id_enrich`, `_ods_merge_run_id` are **dropped**. A single `_ods_lineage_link_id` replaces them; `slot_name` on each contributing edge carries the role.

---

## 7. Effort summary

| Item | Where |
|---|---|
| Migration 36 | `db/migrations/36_lineage_link_file_batch.sql` |
| Helper | `ods_pipeline.lineage.write_link` (+ control-plane shim) |
| Helper | `ods_pipeline.runs.lookup_latest_succeeded_ingest_run` |
| File-batch load jobs | `glue/jobs/ods_postgres_write.py`, `glue/jobs/ods_merge.py` — switch to autonomous pattern + `write_link` |
| Tests | unit + integration cover single-source + multi-source paths |
| Docs | this file + update `file-to-postgres-route.md`, `file-to-postgres-flow.drawio` |

Big-picture: one new table, one new column on `lineage_edge`, one new column per file-batch target table, one new helper, one new pattern for tasks. Multi-source becomes a free side-effect, not a special case.
