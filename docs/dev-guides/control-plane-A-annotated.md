# Control-plane walkthrough — annotated event-pattern source

> **Style A: line-by-line code commentary on a real ingestion.** Read this
> when adding a new ingestion pattern or HTTP surface. Pair with
> [`control-plane-B-sequence.drawio`](control-plane-B-sequence.drawio) for
> the visual shape, and [`control-plane-C-cookbook.md`](control-plane-C-cookbook.md)
> while coding.
>
> **Audience:** any developer touching ingestion code for the first time.
> **Pattern chosen:** event API ([`services/event_api/main.py`](../../services/event_api/main.py)).
> Smallest end-to-end ingestion in the repo: one HTTP request, one Kafka
> message, one full set of control-plane writes. Generalises to
> file_pipeline, api_pull, and CDC.

---

## What you should be able to answer after reading this

1. Which row appears in `pipeline.run_log` when a request arrives, and what fields it carries.
2. Which rows appear in `pipeline.run_stage_log`, in what order, and what `event_type` each carries.
3. **What a `lineage_edge` row is for, what fields it links, and how the lineage viewer uses it.**
4. Why `record_result` does not commit, and what happens if you forget to.
5. Where reconciliation is written and what makes it pass vs fail.

---

## The four control-plane tables in one breath

| Table | Question it answers | Rows per request |
|---|---|---|
| `pipeline.run_log` | "Did the run finish? With what counts?" | 1 |
| `pipeline.run_stage_log` | "What happened inside the run, in what order?" | 5–6 |
| `pipeline.lineage_edge` | "What triggered this run? What did it produce?" | 1+ (one per parent edge) |
| `pipeline.reconciliation_log` | "Did source / Kafka / target counts match?" | 1 per check_type |

You touch all four every ingestion. The helpers in
[`ods_pipeline/messages.py`](../../ods_pipeline/messages.py) bundle the
writes so you don't have to remember each table by hand.

---

## What `lineage_edge` is for — explained with the actual schema

A run row tells you **what** happened. A lineage edge tells you **what
caused it** or **what it consumed**. The lineage viewer and the recon
"did downstream finish?" query both walk these edges.

The actual schema (`db/migrations/14_lineage_and_events.sql`):

| Column | Type | Notes |
|---|---|---|
| `lineage_edge_id` | BIGSERIAL PK | |
| `child_run_id` | UUID, FK → run_log | run that received |
| `parent_run_id` | UUID, FK → run_log (nullable) | run that produced |
| `parent_file_id` | UUID, FK → file_catalogue (nullable) | file that produced |
| `edge_type` | VARCHAR | `raw_to_curated`, `curated_to_kafka`, `curated_to_postgres`, `triggered_by_api_pull`, … |
| `source_ref` | TEXT | S3 raw URI / topic / etc |
| `target_ref` | TEXT | S3 curated URI / Postgres table / etc |
| `record_count` | BIGINT | how many records this hop carried |

Concrete row shapes (from real DAGs):

```sql
-- 1. "This curated-write run consumed file f_99 from raw"
INSERT INTO pipeline.lineage_edge
    (child_run_id, parent_file_id, edge_type, source_ref, target_ref, record_count)
VALUES
    ('run_002', 'f_99', 'raw_to_curated',
     's3://ods-raw/.../f_99.csv', 's3://ods-curated/.../insurance/policies/...', 1234);

-- 2. "This downstream run was triggered by api_pull run_007"
INSERT INTO pipeline.lineage_edge
    (child_run_id, parent_run_id, edge_type)
VALUES
    ('run_009', 'run_007', 'triggered_by_api_pull');

-- 3. "Publish run wrote topic+offset window from curated"
INSERT INTO pipeline.lineage_edge
    (child_run_id, parent_run_id, edge_type, source_ref, target_ref, record_count)
VALUES
    ('run_010', 'run_005', 'curated_to_kafka',
     's3://ods-curated/...', 'ods.insurance.event_demo', 200);
```

**Where message-correlation lives.** The schema has NO
`_ods_source_event_id` / `_ods_source_request_id` / `_ods_change_lsn`
columns — those correlation IDs are stored on `pipeline.run_log.parents`
as JSONB instead. Lineage edges are only about run↔run and file↔run
hops. To find a run by event_id you query the JSONB:

```sql
-- "Customer reports event ev_42abc got lost — which run handled it?"
SELECT run_id
  FROM pipeline.run_log
 WHERE parents @> '[{"_ods_source_event_id": "ev_42abc"}]'::jsonb;
```

**Five concrete operator questions answered by edges:**

1. *"This api_pull run_007 finished — did the downstream ingestion fire?"*
   →  `SELECT child_run_id FROM lineage_edge WHERE parent_run_id='run_007'`
2. *"File f_99 arrived hours ago — has any pipeline picked it up?"*
   →  `SELECT child_run_id FROM lineage_edge WHERE parent_file_id='f_99'`
3. *"How many records flowed across each hop for this run chain?"*
   →  recursive CTE walking `parent_run_id` and summing `record_count`
4. *"What S3 path did the curated-write produce?"*
   →  `SELECT target_ref FROM lineage_edge WHERE child_run_id=… AND edge_type='raw_to_curated'`
5. *"Which Kafka topic did publish run_005 write to?"*
   →  `SELECT target_ref FROM lineage_edge WHERE parent_run_id='run_005' AND edge_type='curated_to_kafka'`

**The rule:** every run that consumes a parent (file or upstream run)
should write at least one `lineage_edge` row naming the parent. Use
`ods_pipeline.lineage.write_edge`; don't INSERT directly.

---

## The annotated source — `services/event_api/main.py`

Line numbers stable as of `1525046`; if they drift, follow function names.

### 1. Generate IDs and archive to S3

```python
def ingest_event(envelope, conn=Depends(_get_pg)):
    event_id = envelope.event_id or str(uuid.uuid4())
    run_id   = str(uuid.uuid4())
    archive_key = f"raw/event/{envelope.domain}/{envelope.dataset}/{...}/{event_id}.jsonl"
    s3_client.put_object(Bucket=archive_bucket, Key=archive_key, Body=...)
```

* `event_id` is the **business identity** of what arrived. Trust the
  caller's value if supplied; mint UUID otherwise. This becomes the
  `_ods_source_event_id` referenced by every downstream lineage edge.
* `run_id` is the **internal identity** for this attempt. Always fresh
  (no reuse on retries — collisions would violate the `(run_id, stage,
  event_type, attempt_number)` unique index on `run_stage_log`).
* The S3 write happens **before** any DB row. If the process dies after
  the put, no `run_log` row exists, the caller sees a 5xx, and the S3
  object is orphaned-but-unreferenced — garbage-collectable.
* If the put itself fails, raise 502. Nothing to roll back yet.

> **Is the S3 write recorded?** Not at this moment, by design — the
> control-plane row that ties this `archive_key` to the run gets written
> in step 5 (`stages.write(MESSAGE_ARCHIVE)` with `output_ref=s3://…`).
> For the api_pull pattern there's an additional `file_catalogue` row
> upserted at the same point. The S3 write **must not** be recorded
> before it succeeds — that's why the put precedes the DB insert.

### 2. Open the run — `messages.start_run`

```python
from ods_pipeline import messages
messages.start_run(
    conn,
    run_id=run_id,
    domain=envelope.domain,
    dataset=envelope.dataset,
    source_application="event_api",
    correlation={"_ods_source_event_id": event_id},
    business_date=envelope.business_date,
    kafka_topic=pattern.topics[0],
)
```

One Python call, two INSERTs, **two independent commits** under the
stateless contract:

| Helper inside | Table written | Purpose |
|---|---|---|
| `runs.start(...)` | `pipeline.run_log` | new run, `status='running'`, `pipeline_type='message_api'`. The `parents=[{"edge_type": "message_correlation", "_ods_source_event_id": event_id, …}]` payload is stored on the `parents` JSONB column. **Commits.** |
| `stages.start(MESSAGE_RECEIVE)` | `pipeline.run_stage_log` | first stage, `event_type='stage_started'`. **Commits.** |

The message-correlation IDs (`_ods_source_event_id` etc.) live on
`run_log.parents`, NOT in `lineage_edge` — see the schema note above.
For the event pattern there's no upstream `run_id` or `file_id`, so no
`lineage_edge` row is written. (Other patterns — file_pipeline, api_pull
— DO write `lineage_edge` rows for `file_to_run` / `triggered_by_api_pull`
hops via `lineage.write_edge`.)

**Live progress.** After this call returns, the operator dashboard
already shows the run as `running` with the receive stage `started`. If
the worker dies before the next step, those rows are durable; the
heartbeat janitor (`dag_run_janitor`, runs every minute) closes them
when no further stage activity appears within ~5 minutes.

### 3. Publish to Kafka

```python
producer = producer_factory()
producer.produce(topic=pattern.topics[0], value=json.dumps({
    "_ods_source_event_id": event_id,
    "_ods_run_id": run_id,
    "_ods_domain": envelope.domain,
    "_ods_dataset": envelope.dataset,
    "_ods_business_date": envelope.business_date,
    "payload": envelope.payload,
}, default=str).encode("utf-8"))
flush_result = producer.flush()
if isinstance(flush_result, int) and flush_result:
    raise RuntimeError(f"{flush_result} Kafka message(s) not delivered")
```

* The payload's envelope fields (`_ods_run_id`, `_ods_source_event_id`,
  `_ods_business_date`) are mandatory — the canonicalize Spark stage and
  recon T0 join on them.
* `flush()` blocks until every produced message is acked (or producer
  internal timeout fires). Non-zero return = failure path below.

### 4. Close the run — `messages.record_result`

```python
messages.record_result(
    conn,
    run_id=run_id,
    domain=envelope.domain, dataset=envelope.dataset,
    business_date=envelope.business_date,
    source_count=1, published_count=1, archive_count=1,
    kafka_topic=pattern.topics[0],
    archive_ref=f"s3://{archive_bucket}/{archive_key}",
)
# No conn.commit() here — every helper call below is commit=True.
```

Six writes, **six independent commits**, in this order:

| # | Write | Table | `event_type` / status |
|---|---|---|---|
| 1 | `stages.finish(MESSAGE_RECEIVE)` | `run_stage_log` | `stage_completed` |
| 2 | `stages.write(MESSAGE_VALIDATE)` | `run_stage_log` | `stage_completed` (or `stage_warned` if `validation_fail_count > 0`) |
| 3 | `stages.write(MESSAGE_ARCHIVE)` | `run_stage_log` | `stage_completed`; **`output_ref=s3://…/event_id.jsonl`** — this is the row that records the S3 write |
| 4 | `reconciliation.write_check(...)` | `reconciliation_log` | `status='ok'` iff `published_count == accepted_count` AND `archive_count == source_count` |
| 5 | `stages.write(RECON_MESSAGE)` | `run_stage_log` | mirrors the recon outcome on the run's stage timeline |
| 6 | `runs.update(...)` | `run_log` | flips run to `'succeeded'` / `'failed'`; **runs LAST** so `succeeded` always implies the proof rows already landed |

**Why this order is the safety net.** With atomic transactions, ordering
didn't matter — the commit was the safety net. With stateless commits,
the safety net IS the order. If a power cut hits mid-call:

* After write 3 only — operator dashboard shows three completed stages,
  no recon row, run still `running`. Janitor reaps it later as failed.
* After write 4 only — recon row exists with whatever status the count
  comparison produced; run still `running`. Same janitor reap.
* After write 6 only — `succeeded` is durable; recon, archive, and stage
  rows all already landed. The dashboard invariant
  ("succeeded ⇒ recon row present") holds.

### 5. The failure path

```python
except Exception as exc:
    try:
        messages.record_result(conn, ...,
                               published_count=0,
                               extra_detail={"error": str(exc)})
    except Exception:
        # Heartbeat janitor reaps the orphan run if even the failure
        # write fails (DB momentarily unreachable).
        pass
    raise HTTPException(status_code=502, detail=...)
```

* No rollback dance. Each control-plane row that landed before the
  failure stays — the dashboard reflects how far the run got.
* `record_result` re-runs with `published_count=0` — writes the
  remaining stages + a failed recon row + flips run.status to `failed`.
  Counts mismatch → recon `status='failed'` → run `status='failed'`.
* If even that fails (transient DB error), the inner `except` is a
  no-op. The run stays `running`; the janitor closes it within
  the next 5 minutes.

---

## Concrete example — what's actually in the DB after one successful request

Setup: client `POST /events` with payload `{"customer_id": 7, "amount": 19.99}`.
Handler mints `run_id='run_001'`, sees no client `event_id` so mints
`event_id='ev_42abc'`. Archive key
`raw/event/insurance/event_demo/2026-05-07/ev_42abc.jsonl`.

After `conn.commit()` the four tables look like this (only relevant
columns shown):

#### `pipeline.run_log` — 1 row

| run_id | pipeline_type | domain | dataset | business_date | status | record_count_source | record_count_published | kafka_topic | started_at | ended_at |
|---|---|---|---|---|---|---|---|---|---|---|
| `run_001` | `message_api` | `insurance` | `event_demo` | `2026-05-07` | `succeeded` | 1 | 1 | `ods.insurance.event_demo` | `2026-05-07 09:33:14` | `2026-05-07 09:33:14` |

#### `pipeline.run_stage_log` — 6 rows

| run_id | stage | event_type | status | attempt | record_count_in | record_count_out | output_ref |
|---|---|---|---|---|---|---|---|
| `run_001` | `message_receive` | `stage_started` | `running` | 1 | NULL | NULL | NULL |
| `run_001` | `message_receive` | `stage_completed` | `succeeded` | 1 | 1 | 1 | NULL |
| `run_001` | `message_validate` | `stage_completed` | `succeeded` | 1 | 1 | 1 | NULL |
| `run_001` | `message_archive` | `stage_completed` | `succeeded` | 1 | 1 | 1 | **`s3://ods-event-demo/raw/event/insurance/event_demo/2026-05-07/ev_42abc.jsonl`** |
| `run_001` | `recon_message` | `stage_completed` | `succeeded` | 1 | 1 | 1 | NULL |

(Plus the implicit run-end via `runs.update`.)

The bolded `output_ref` is the answer to *"is the S3 write recorded?"*.
For api_pull a `file_catalogue` row would also exist with the same URI.

#### `pipeline.lineage_edge` — 0 rows

The event pattern has no upstream run or file, so no edge is written.
The correlation lives on `run_log.parents`:

```sql
SELECT run_id
  FROM pipeline.run_log
 WHERE parents @> '[{"_ods_source_event_id": "ev_42abc"}]'::jsonb;
-- → run_001
```

Compare with the file pattern, which DOES write a `lineage_edge` row
when ingesting a `file_catalogue` entry:

```sql
SELECT child_run_id, edge_type, source_ref, target_ref, record_count
  FROM pipeline.lineage_edge
 WHERE parent_file_id = 'f_99';
-- → run_002, raw_to_curated, s3://raw/.../f_99.csv, s3://curated/..., 1234
```

#### `pipeline.reconciliation_log` — 1 row

| run_id | check_type | status | source_count | kafka_count | target_count | detail |
|---|---|---|---|---|---|---|
| `run_001` | `message_batch_count` | `ok` | 1 | 1 | NULL | `{"source_count":1,"published_count":1,"accepted_count":1,"archive_count":1,"archive_discrepancy":0,"validation_fail_count":0,"dlq_count":0}` |

Operator dashboard groups by `check_type`; this row keeps the
`message_batch_count` panel green.

---

## How to apply this to a new pattern

When you add (say) the SFTP-poll pattern next quarter:

1. **Pick the correlation field.** SFTP files reuse `_ods_file_id` —
   it's already in `metadata.MESSAGE_CORRELATION_FIELDS`. Pass it via
   `correlation={"_ods_file_id": file_id}` (HTTP) or
   `parents=[{"edge_type": "file_to_run", "_ods_file_id": file_id}]`
   (Airflow).
2. **Decide the stages.** Open
   [`ods_pipeline/models.py`](../../ods_pipeline/models.py): `Stage` is
   the canonical enum; the DB CHECK constraint enforces it. Add a new
   constant if none fits (e.g. `Stage.SFTP_POLL`); update the migration
   that constrains the column.
3. **Wrap stage writes.** Don't call
   `cur.execute("INSERT INTO run_stage_log ...")` directly — use
   `stages.start` / `stages.finish` / `stages.write`. They handle
   attempt numbers, timestamps, metric JSON encoding.
4. **One commit per ingest unit.** Whether the unit is "one file", "one
   poll tick", or "one HTTP request", every control-plane write must
   share that transaction.
5. **Mirror the recon rule.** Pick a count invariant (`source_count ==
   published_count`, sum-of-MD5s, or whatever fits) and write it via
   `reconciliation.write_check` with a stable `check_type` so the
   dashboard can group it.

---

## Things that look optional but aren't

* **`source_application`** — seeds the lineage edge.
* **`pipeline_type`** — recon dashboard groups by it; mis-set and your
  dataset disappears from operator view.
* **`business_date`** — partition for archive S3 paths AND the join key
  in reconciliation. Derive deterministically (UTC date of arrival)
  if the source doesn't carry one.
* **`archive_ref`** on `record_result` — without it, the archive stage
  row has `output_ref=NULL`. Operators investigating a bad payload then
  have to grep S3 by filename. Always set the URI.
* **The same `run_id` everywhere** — Kafka envelope `_ods_run_id`,
  `run_log.run_id`, every `run_stage_log.run_id`. Spark canonicalize
  joins on it.

---

## Counter-examples — code that compiles and tests pass but is wrong

```python
# WRONG — flips run.status='succeeded' BEFORE writing recon row
runs.update(conn, run_id, status='succeeded')   # don't run this first!
reconciliation.write_check(conn, status='ok', ...)
# A power-cut between the two leaves a 'succeeded' run with no recon
# row. The dashboard rule "succeeded ⇒ recon present" silently breaks.
# Always write recon first; flip status last.
```

```python
# WRONG — invents a stage string
with conn.cursor() as cur:
    cur.execute(
        "INSERT INTO pipeline.run_stage_log (run_id, stage, event_type, started_at) "
        "VALUES (%s, 'my_custom_stage', 'stage_started', NOW())",
        (run_id,),
    )
# rejected by the CHECK constraint. Use Stage.* and stages.start().
```

```python
# WRONG — reuses run_id across retries with attempt_number=1
run_id = f"event-{event_id}"         # don't pin run_id to event_id!
stages.start(conn, run_id=rid, stage='message_receive', attempt_number=1)
# A retry collides on the (run_id, stage, attempt_number) unique index
# (migration 19). Mint a fresh UUID per attempt; or omit attempt_number
# so stages.start auto-increments via next_attempt_number().
```

```python
# WRONG — bare exception inside a stage block, no failure recorded
try:
    publish_to_kafka(...)
except Exception:
    pass                              # silent! run stays 'running' forever
# Use stage_scope so the failure stage row + run.status flip are written
# automatically:
#     with stage_scope(conn, run_id=rid, stage=Stage.KAFKA_PUBLISH):
#         publish_to_kafka(...)
```

```python
# WRONG — INSERT directly into lineage_edge bypassing the helper
with conn.cursor() as cur:
    cur.execute(
        "INSERT INTO pipeline.lineage_edge (child_run_id, parent_run_id, "
        "edge_type) VALUES (%s, %s, %s)",
        (rid, parent_rid, 'raw_to_curated'),
    )
# Use lineage.write_edge so the edge_type stays in the supported set and
# the source_ref / target_ref / record_count columns get populated. The
# viewer expects them — direct INSERT creates degraded edges.
```

---

## Where to look next

* [`ods_pipeline/messages.py`](../../ods_pipeline/messages.py) — helper
  source. `record_result` is the canonical example of composing
  `stages.write` / `reconciliation.write_check` / `runs.update` under
  one transaction.
* [`ods_pipeline/models.py`](../../ods_pipeline/models.py) — every legal
  value for `Stage`, `StageEvent`, `RunStatus`, plus
  `MESSAGE_CORRELATION_FIELDS`. Treat it as the contract.
* [`tests/integration/test_event_api_live.py`](../../tests/integration/test_event_api_live.py)
  — the whole flow against LocalStack + Postgres + Kafka. Source-of-truth
  for what rows look like after success.
* [`control-plane-B-sequence.drawio`](control-plane-B-sequence.drawio) —
  the same flow as a sequence diagram.
* [`control-plane-C-cookbook.md`](control-plane-C-cookbook.md) — recipes
  to keep open while coding.
