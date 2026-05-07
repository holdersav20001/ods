# Control-plane sequence — diagram-first walkthrough

> **Style B: visual-first.** Open
> [`control-plane-B-sequence.drawio`](control-plane-B-sequence.drawio) in
> draw.io / VS Code Draw.io extension while reading this page.
>
> Pair with `control-plane-A-annotated.md` (line-by-line code) and
> `control-plane-C-cookbook.md` (recipe card).

---

## Why this exists

When a new dev asks *"what actually happens when a request hits our event
API?"* the honest answer is **eight database rows in three different tables
plus one S3 object plus one Kafka message — all under one transaction**.

That's hard to explain in prose. The diagram gives you the temporal shape;
this page annotates each step so you can read them together.

---

## How to read the diagram

* **Five lifelines (top of the page, left → right):**
  1. **HTTP Client** — orange. The untrusted edge.
  2. **`event_api.ingest_event`** — green. The FastAPI handler at
     [`services/event_api/main.py`](../../services/event_api/main.py).
  3. **S3** — yellow. The archive bucket; the only durable raw store.
  4. **Postgres** — blue. The control-plane: `run_log`, `run_stage_log`,
     `lineage_edge`, `reconciliation_log`.
  5. **Kafka** — purple. The pipeline boundary — once a message is acked
     here, the file_pipeline DAGs take over.
* **Numbered arrows** are messages between actors. Each arrow has a one-line
  label (the call) and a small italic note (the rule or invariant).
* **Blue boxes hanging off the Postgres lifeline** show *which row(s)*
  each call writes. They are not separate steps — they are the contents of
  the call.
* **The thick green arrow at step 6** is the only commit. Everything before
  it is in-flight; everything after it is durable.
* **The pink band at the bottom** describes the failure path; it is not a
  separate sequence, just a collapsed callout because the failure path
  reuses the same arrows.

---

## Step-by-step commentary

### Step 1 — HTTP request arrives

Client `POST /events` with a payload and (optionally) an `event_id`. The
handler mints a fresh `run_id` (UUID) regardless of whether `event_id` was
supplied. **Trust the caller's `event_id`** — it's their business identity
— but always **own your own `run_id`** so retries do not collide.

### Step 2 — Archive to S3 first

```text
s3://<bucket>/raw/event/<domain>/<dataset>/<business_date>/<event_id>.jsonl
```

This is the only step that runs *outside* the database transaction.
Reasoning: if S3 is down we want to fail fast and return 502 before
opening a `run_log` row. If the process dies after S3 succeeds but before
Postgres opens the run, the archive object is orphaned — that's
acceptable; a janitor can sweep it later.

The reverse order (DB row first, then S3) is wrong: a crash there leaves
a `run_log` row referring to an archive URI that does not exist.

### Step 3 — `messages.start_run` opens the ledger

One Python call, **three INSERT statements**, all in the same open
transaction:

| Insert | Table | Purpose |
|---|---|---|
| run row | `pipeline.run_log` | `status='running'`, `pipeline_type='message_api'`, count fields NULL |
| edge row | `pipeline.lineage_edge` | `edge_type='message_correlation'`, ties this `run_id` to the upstream `_ods_source_event_id` |
| stage row | `pipeline.run_stage_log` | `stage='message_receive'`, `event_type='stage_started'` |

**Why bundle the three?** Because a developer who writes "open a run"
should not be responsible for remembering all three writes. The wrapper
exists exactly so you cannot forget the lineage edge — that edge is what
makes the dashboard able to answer *"which event triggered this run?"*.

**No commit yet.** All three rows are pending.

### Step 4 — Kafka publish

The Kafka payload includes envelope fields `_ods_run_id`,
`_ods_source_event_id`, `_ods_domain`, `_ods_dataset`,
`_ods_business_date`. These are mandatory — the Spark canonicalize stage
and recon T0 join on them.

`flush()` blocks until every message is acknowledged or the producer's
internal timeout fires. A non-zero return is treated as a publish
failure; we follow the failure path (pink band) instead of the success
arrows.

The dashed grey "ack" arrow shows the broker confirming receipt. We don't
write anything to Postgres in response — the success/failure of the
publish is captured by the next step.

### Step 5 — `messages.record_result` closes the ledger

One Python call, **six writes**, all `commit=False`:

1. `stages.finish(MESSAGE_RECEIVE)` — completes the stage row started in
   step 3. `event_type` flips to `stage_completed`.
2. `stages.write(MESSAGE_VALIDATE)` — separate stage row for validation.
   `status='succeeded'` if `validation_fail_count==0`, else
   `status='warned'`.
3. `stages.write(MESSAGE_ARCHIVE)` — stage row for the S3 write done in
   step 2. `output_ref` carries the S3 URI; this is what makes the
   archive findable from the lineage viewer.
4. `reconciliation.write_check(message_batch_count)` — one row in
   `reconciliation_log`. `status='ok'` only if
   `published_count == source_count - validation_fail_count - dlq_count`
   AND `archive_count == source_count`.
5. `stages.write(RECON_MESSAGE)` — stage row mirroring the recon outcome,
   so the run timeline shows recon as a stage event.
6. `runs.update(run_log)` — flips run row to `status='succeeded'` (or
   `'failed'`) and fills the `record_count_*` columns.

**Why six?** Each row answers a different operator question:

| Question | Answered by row |
|---|---|
| "Did the run finish?" | `run_log.status` |
| "What happened in receive?" | `run_stage_log` (MESSAGE_RECEIVE) |
| "Was anything filtered by validation?" | `run_stage_log` (MESSAGE_VALIDATE) |
| "Where is the raw payload?" | `run_stage_log` (MESSAGE_ARCHIVE).`output_ref` |
| "Do source / Kafka / archive counts agree?" | `reconciliation_log` |
| "When did recon evaluate?" | `run_stage_log` (RECON_MESSAGE) |
| "Which event triggered this?" | `lineage_edge` (written in step 3) |

### Step 6 — `conn.commit()` (atomic boundary)

One commit. Eight+ rows become durable atomically. This is the only place
in the whole flow where the handler calls `commit`.

A failure anywhere from step 3 to step 5 → `conn.rollback()` and zero
rows persist (S3 object is orphaned, but never referenced).

### Step 7 — Response

`200 OK` with `{run_id, event_id, archive_uri, accepted: true}`. The
`run_id` lets the client correlate later support tickets to a specific
ledger row.

---

## Failure path (pink band)

If step 4 or step 5 raises:

1. `conn.rollback()` — drops all step-3 rows.
2. Re-run `messages.record_result` with `published_count=0` and
   `extra_detail={"error": "..."}`. This writes a *closed, failed* run
   row plus a failed recon row. The lineage edge is re-written too.
3. `conn.commit()`.
4. Raise `HTTPException(502)`.

End state: one `run_log` row with `status='failed'`, full stage timeline
showing where it failed, recon row showing the count mismatch. Operators
can find it on the dashboard immediately. The S3 archive remains so the
payload is not lost.

---

## Where to look next

* **For exact code:** [`control-plane-A-annotated.md`](control-plane-A-annotated.md)
* **For copy-paste recipes:** [`control-plane-C-cookbook.md`](control-plane-C-cookbook.md)
* **For the Stage / StageEvent enum (the legal values):**
  [`ods_pipeline/models.py`](../../ods_pipeline/models.py)
* **For the helper source:** [`ods_pipeline/messages.py`](../../ods_pipeline/messages.py)
* **For the integration-level proof of these rows:**
  [`tests/integration/test_event_api_live.py`](../../tests/integration/test_event_api_live.py)
