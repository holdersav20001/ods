# Building block 5 — Dead-letter queue (DLQ) and replay

> Part of the **control-plane cookbook**. Unlike blocks 01–04 this is not a *pipeline stage* — it is a
> **cross-cutting capability** every block leans on. Any block (schema validation, DQ, canonical
> transform, merge, or a Postgres constraint) can fail; this recipe says where the bad data lands,
> how it stays tied to the run that produced it, and how you drain/replay it after a fix.

> ⚠️ **Read this first — EXISTS vs TO BUILD.** This document is *document-as-designed*. Parts of the
> DLQ exist today; the durable, queryable, row-level DLQ does **not**. Every section below is tagged:
>
> - 🟢 **EXISTS** — implemented today, with file/line references.
> - 🟠 **TO BUILD** — proposed design. Do **not** assume these tables/paths exist. Build them.
>
> Do not mistake a 🟠 block for current behaviour.

---

## Use case (your words)

> "When data fails at ANY stage — schema validation, DQ rules, canonical transform, merge conflict,
> or a Postgres constraint — the bad data must land somewhere durable, be inspectable, be tied to the
> run that produced it, and be REPLAYABLE after a fix. I need to know how to quarantine bad data and
> how to drain/replay the DLQ using the control plane."

---

## What exists today (ground truth)

Before the design, here is exactly what is real in the repo right now. Read these to confirm:

| Capability | Status | Where |
|------------|--------|-------|
| DQ failing rows written to durable storage | 🟢 EXISTS (S3 only) | `glue/jobs/ingestion/quality.py` |
| `record_count_dq_fail` count on the run | 🟢 EXISTS | `run_log`, set in `glue/jobs/ingestion/pipeline.py` (`runs.update`) |
| Whole-run / whole-envelope replay + `replay` lineage edge | 🟢 EXISTS | `ods_pipeline/ops/dlq.py` (`_DlqOps.replay`, ~L118–156) |
| Whole-file failure status | 🟢 EXISTS | `run_log.status='failed'` + `file_catalogue.state='failed'` (`finalise_failure`) |
| `dlq_write` stage value (for `run_stage_log.stage`) | 🟢 EXISTS (defined, under-used) | `ods_pipeline/models.py` `Stage.DLQ_WRITE` |
| **First-class, queryable, row-level DLQ control table** | 🟠 **TO BUILD** | proposed `pipeline.dlq` (below) |
| **A drain path that marks rows replayed** | 🟠 **TO BUILD** | proposed `dlq drain` (below) |

> 🟢 **EXISTS — what `quality.py` actually does today.** The DQ stage *does* persist failing rows, not
> just count them. `quality.evaluate(...)` splits the DataFrame and, when `failing_count > 0`, writes:
>
> ```python
> failing_df.write.mode("overwrite").parquet(
>     _dlq_path(domain, dataset, business_date, run_id)
> )
> # s3a://ods-dlq-<env>/<domain>/<dataset>/date=<business_date>/run_id=<run_id>/failed.csv
> ```
>
> So the *rows* exist on S3 and are partitioned by `run_id` — good. **But there is no control-plane
> row** describing that write: nothing in Postgres records "run X quarantined N rows at S3 location Y
> for reason Z". You can only discover it by listing S3 (`ops/dlq.py list` walks the bucket) or by
> reading `record_count_dq_fail` and *inferring* the path. The other failure modes (schema, transform,
> merge, Postgres) write **nothing durable at the row level at all** — they fail the whole run. That
> gap is what the 🟠 design below closes.

---

## You provide (inputs)

### When quarantining (flow A) — the block already has these in scope
| Input | Where from |
|-------|-----------|
| `run_id` | the failing run (minted by the orchestrator) |
| `domain`, `dataset`, `business_date` | the run being processed |
| `stage` | which block failed: `schema` / `dq` / `transform` / `merge` / `postgres` |
| `reason` | the exception / rule / constraint that rejected the data |
| the failing rows | the DataFrame slice (DQ) or the rejected payload (merge/PG) |

### When draining/replaying (flow B)
| Input | Where from |
|-------|-----------|
| `domain`, `dataset`, `business_date` (or a specific `dlq_id`) | operator selects what to replay |
| fix applied | corrected source file, fixed schema, relaxed/fixed DQ rule, fixed constraint |
| `target` | the originating block to re-run (ingestion / canonicalize / merge / publish) |

---

## You produce (outputs)

- 🟠 A durable, queryable **`pipeline.dlq`** row per quarantine event, tied to its `run_id`, naming
  the S3 payload location, the stage, the reason, and the record count.
- 🟢 The quarantined rows themselves on S3 (DQ does this today; other stages must start doing it).
- 🟢 On replay: a new `run_log` row and a `lineage_edge` with `edge_type='replay'` pointing at the
  original failed run (the existing `ops/dlq.py` pattern).
- 🟠 The original DLQ row stamped with `replayed_at` + `replay_run_id` so it drops out of the queue.

---

## 🟠 TO BUILD — the `pipeline.dlq` table

> 🟠 **This table does not exist yet.** Proposed shape. Either a real table **or** an S3 DLQ prefix
> (which already exists per `quality.py`) **plus** this control row keyed by `run_id`. The S3 prefix
> is the payload; this row is the *index* that makes it discoverable and replayable.

```sql
CREATE TABLE pipeline.dlq (
    dlq_id         uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id         uuid NOT NULL REFERENCES pipeline.run_log(run_id),
    domain         text NOT NULL,
    dataset        text NOT NULL,
    business_date  date,
    stage          text NOT NULL,          -- schema | dq | transform | merge | postgres
    reason         text NOT NULL,          -- rule id, exception class, constraint name
    source_ref     text,                   -- the input that produced the bad rows (raw S3 path, topic+offset)
    payload_ref    text,                   -- S3 location of the quarantined rows (the failed.csv/parquet)
    payload_inline jsonb,                  -- OPTIONAL: inline payload for tiny single-record failures
    record_count   integer NOT NULL,       -- how many rows landed here
    created_at     timestamptz NOT NULL DEFAULT now(),
    replayed_at    timestamptz,            -- NULL until drained
    replay_run_id  uuid REFERENCES pipeline.run_log(run_id)  -- NULL until drained
);

CREATE INDEX dlq_open_idx ON pipeline.dlq (domain, dataset, business_date)
    WHERE replayed_at IS NULL;   -- "the open queue" is the cheap query
```

Notes on the shape:
- `stage` deliberately uses the **coarse block name** (`schema`/`dq`/`transform`/`merge`/`postgres`),
  not the fine-grained `run_stage_log.stage` value, because a replay re-runs a *block*, not a stage.
- Use `payload_ref` for the normal (bulk row) case — it points at the S3 location each block writes.
  Reserve `payload_inline` for single-record failures (a merge conflict on one PK, a single PG
  constraint violation) where standing up an S3 object is overkill.
- `replayed_at IS NULL` defines "the open queue". Replayed rows stay for audit, not reprocessing.

---

## Flow A — QUARANTINE on failure (control records written)

The principle: **every block, on rejecting data, writes (1) the rows somewhere durable and (2) one
`pipeline.dlq` row tying those rows to its `run_id`.** Then it does whatever its own block already
does to its run status.

> 🟠 Helper to build: `ods_pipeline.dlq_control.quarantine(conn, run_id, domain, dataset,
> business_date, stage, reason, source_ref, payload_ref, record_count)` → inserts one `pipeline.dlq`
> row and returns its `dlq_id`. Per the *stateless control-plane* rule, this is its own committed
> write — not folded into a larger transaction. Optionally pair it with a `run_stage_log` row using
> the **already-defined** `Stage.DLQ_WRITE` (🟢 the value exists; the call site does not).

Per block:

| Block | When it quarantines | Rows go to | `pipeline.dlq` row | Run outcome |
|-------|--------------------|-----------|--------------------|-------------|
| **01 ingest — schema** | required columns missing | 🟠 whole raw file → DLQ prefix (today: nothing; whole run just fails) | 🟠 `stage='schema'`, `record_count=source_count`, `payload_ref=<raw path>` | run **fails** (all-or-nothing) |
| **01 ingest — DQ** | some rows break `dq_rules` | 🟢 `failing_df.write...parquet(_dlq_path(...))` (S3, today) | 🟠 `stage='dq'`, `record_count=failing_count`, `payload_ref=s3a://ods-dlq-<env>/.../run_id=<run_id>/failed.csv` | run **succeeds** (partial) |
| **02 canonicalize — transform** | raw-shape → canonical-shape transform throws/drops | 🟠 offending records → DLQ prefix | 🟠 `stage='transform'`, `source_ref=<topic+offset or curated path>` | run **fails** (or partial if per-record) |
| **03/04 merge** | merge conflict (e.g. duplicate/contradicting PK) | 🟠 conflicting records → DLQ prefix or `payload_inline` | 🟠 `stage='merge'`, `record_count=<conflicts>` | run **fails** for the conflicting slice |
| **postgres sink** | a constraint rejects the write (FK / NOT NULL / unique) | 🟠 rejected rows → DLQ prefix or `payload_inline` | 🟠 `stage='postgres'`, `reason=<constraint name>` | run **fails** |

> 🟢 **EXISTS today** only the green cells: the DQ S3 write and `record_count_dq_fail` on `run_log`,
> plus `finalise_failure` flipping `run_log.status='failed'` + `file_catalogue.state='failed'` for the
> all-or-nothing cases. Everything orange is the work.

---

## Flow B — DRAIN / REPLAY (the path and its lineage)

This is the half that turns the DLQ from a graveyard into a queue. The shape **reuses the existing
`ops/dlq.py` replay pattern** — open a fresh run, link it to the original via a `replay` lineage edge,
re-run the work — and extends it to mark the `pipeline.dlq` row drained.

> 🟢 **EXISTS — the replay primitive.** `ods_pipeline/ops/dlq.py` `_DlqOps.replay(s3_uri, ...)`
> (~L118–156) already: mints a `replay_run_id`, calls `runs.start(..., pipeline_type="dlq_replay",
> orchestrators=[original_run])`, then `lineage.write_edge(conn, consumer_run_id=replay_run_id,
> upstream_run_id=original_run, edge_type="replay", source_ref=..., target_ref=..., record_count=...)`,
> then re-produces the payload. Today it operates on a **single S3 envelope** and republishes to a
> Kafka topic. It does **not** touch a `pipeline.dlq` row (there isn't one).

Steps:

**1. Select the open queue.** 🟠
`SELECT * FROM pipeline.dlq WHERE replayed_at IS NULL AND domain=… AND dataset=… AND business_date=…`
(the partial index `dlq_open_idx` makes this cheap). Equivalent CLI to build:
`dlq list --domain insurance --dataset party --open`. (🟢 `ops/dlq.py list` exists but lists S3 keys,
not control rows — extend it to read the table.)

**2. Fix the source.** Operator action, outside the control plane: correct the raw file, fix the
schema in the registry, fix/relax the DQ rule in `dataset_config`, resolve the merge conflict, or fix
the data violating the PG constraint. The DLQ row's `reason` + `payload_ref` tell you exactly what
broke and gives you the rows to inspect (`dlq show <dlq_id>`).

**3. Re-run the originating block.** 🟠 Open a new run and re-invoke the *same block* the row's
`stage` names (ingestion / canonicalize / merge / publish), feeding it the fixed source. Reuse the
`ops/dlq.py` pattern:
```python
replay_run_id = str(uuid.uuid4())
runs.start(conn, run_id=replay_run_id, pipeline_type="dlq_replay",
           domain=domain, dataset=dataset, business_date=business_date,
           orchestrators=[original_run_id])
lineage.write_edge(conn, consumer_run_id=replay_run_id, upstream_run_id=original_run_id,
                   edge_type="replay", source_ref=dlq_row.payload_ref,
                   target_ref=<block output>, record_count=dlq_row.record_count)   # 🟢 existing API
# ... run the originating block normally under replay_run_id ...
```

**4. Mark the row drained.** 🟠 On success, stamp the DLQ row — its own committed write:
```sql
UPDATE pipeline.dlq SET replayed_at = now(), replay_run_id = :replay_run_id WHERE dlq_id = :dlq_id;
```
Now it leaves the open queue but remains for audit. The **`replay` lineage edge is the durable proof**
that the fixed run descends from the failed one — the original evidence is never mutated.

> **Ordering (stateless control-plane rule).** Write the `replay` lineage edge **before** stamping
> `replayed_at`, exactly as block 01 writes lineage before flipping run status. If the process dies
> between the two, the row is still "open" and the replay is idempotently safe to retry.

---

## Control records written (summary)

| Table | Rows | When | Status |
|-------|------|------|--------|
| `pipeline.dlq` | 1 per quarantine event | flow A, any failing block | 🟠 TO BUILD |
| *S3 DLQ prefix* (`ods-dlq-<env>/...`) | the payload | flow A | 🟢 EXISTS for DQ; 🟠 for other stages |
| `run_log` | patched (`record_count_dq_fail`) / `status='failed'` | flow A | 🟢 EXISTS |
| `file_catalogue` | `state='failed'` (whole-file) | flow A | 🟢 EXISTS |
| `run_stage_log` | 1 (`stage='dlq_write'`) | flow A, optional | 🟢 value exists; 🟠 call site |
| `run_log` (replay) | 1 (`pipeline_type='dlq_replay'`, `running`→`succeeded`) | flow B | 🟢 EXISTS |
| `lineage_edge` | 1 (`edge_type='replay'`, `upstream_run_id=<failed run>`) | flow B | 🟢 EXISTS |
| `pipeline.dlq` (drain) | patched (`replayed_at`, `replay_run_id`) | flow B | 🟠 TO BUILD |

---

## Reconciliation

The DLQ is the durable record of the `failing_count` already tracked in `run_log`, so it slots
straight into the cookbook's reconciliation invariant. Per block:

```
source_count == good_count + dlq_count
```

- For **block 01**, `good_count = written_count`, `dlq_count = failing_count`, restating the existing
  invariant `source_count == written_count + failing_count` (🟢 recorded in `reconciliation_log` at
  finalise). The 🟠 difference: `dlq_count` should be derivable from `SUM(record_count)` of the
  `pipeline.dlq` rows for that `run_id`, **not** only from `record_count_dq_fail`. They must agree —
  that equality is the recon check on the DLQ itself.
- For every other block (transform, merge, postgres), the same identity holds once those blocks start
  writing `pipeline.dlq` rows: a row that vanished from the output and produced no DLQ row is an
  **unaccounted-for loss** and should breach reconciliation, not pass silently.

A replayed row does **not** change history: the original run's recon stays as it was; the replay run
gets its **own** `reconciliation_log` entry. Nothing is rewritten.

---

## Done when

- Every failure path — schema, DQ, transform, merge, Postgres — produces a **durable DLQ record tied
  to its `run_id`** (🟢 DQ-on-S3 today; 🟠 a `pipeline.dlq` row + payload for all five).
- Replay **re-runs the originating block** under a new `run_id` and records a `lineage_edge` with
  `edge_type='replay'` pointing at the original failed run (🟢 primitive exists; 🟠 wired to drain a
  `pipeline.dlq` row and stamp `replayed_at`/`replay_run_id`).
- Reconciliation **accounts for every row**: `good + DLQ = source`, per block, with `dlq_count`
  cross-checked against the `pipeline.dlq` rows for the run.
- The open queue (`replayed_at IS NULL`) is empty for a dataset/business_date once all fixes are
  drained.

---

## Copy-paste skeleton

> 🟠 Mostly **TO BUILD**. The `lineage.write_edge` / `runs.start` calls are 🟢 real APIs (see
> `ops/dlq.py`); `pipeline.dlq` and the `dlq_control` helper are the proposed pieces.

```python
import uuid
import ods_pipeline
from ods_pipeline import lineage, runs

# ── Flow A: quarantine (called from the failing block) ──────────────────────
def quarantine(conn, *, run_id, domain, dataset, business_date,
               stage, reason, source_ref, payload_ref, record_count):
    """🟠 TO BUILD — one committed DLQ write, stateless control-plane style."""
    dlq_id = str(uuid.uuid4())
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO pipeline.dlq
               (dlq_id, run_id, domain, dataset, business_date, stage,
                reason, source_ref, payload_ref, record_count)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (dlq_id, run_id, domain, dataset, business_date, stage,
             reason, source_ref, payload_ref, record_count),
        )
    conn.commit()
    return dlq_id

# In block 01's DQ stage, right after quality.evaluate writes failed.csv to S3:
#   quarantine(conn, run_id=run_id, domain=domain, dataset=dataset,
#              business_date=business_date, stage="dq", reason="dq_rules",
#              source_ref=s3_input_path, payload_ref=dlq_s3_path,
#              record_count=outcome.failing_count)

# ── Flow B: drain / replay ──────────────────────────────────────────────────
def drain_one(conn, dlq_row, *, rerun_block):
    """🟠 TO BUILD — reuses the 🟢 ops/dlq.py replay pattern."""
    replay_run_id = str(uuid.uuid4())
    runs.start(conn, run_id=replay_run_id, pipeline_type="dlq_replay",
               domain=dlq_row["domain"], dataset=dlq_row["dataset"],
               business_date=dlq_row["business_date"],
               orchestrators=[dlq_row["run_id"]])
    # 🟢 existing API — write lineage BEFORE stamping the row (ordering invariant)
    lineage.write_edge(conn, consumer_run_id=replay_run_id,
                       upstream_run_id=dlq_row["run_id"], edge_type="replay",
                       source_ref=dlq_row["payload_ref"], target_ref=None,
                       record_count=dlq_row["record_count"])
    rerun_block(run_id=replay_run_id, source_ref=dlq_row["payload_ref"])  # the fixed re-run
    with conn.cursor() as cur:                                            # 🟠 mark drained
        cur.execute(
            "UPDATE pipeline.dlq SET replayed_at = now(), replay_run_id = %s "
            "WHERE dlq_id = %s",
            (replay_run_id, dlq_row["dlq_id"]),
        )
    conn.commit()
    return replay_run_id
```
