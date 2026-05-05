# API Pull — Low-Latency Direct-Kafka Design

Backlog item 7. Alternative shape for `api_pull` datasets that need
sub-minute latency. Mirrors the existing `event` push pattern: poller
publishes per record / per page directly to Kafka, a Kafka Connect S3
sink writes the archive in parallel.

This document is a **design**, not a build plan. Implementation gated
on a concrete dataset that needs sub-minute latency.

## Context

Today's `api_pull` (slice 1) is the file-pipeline shape:

```
API → S3 raw archive (JSONL.gz) → Glue Parquet → raw Kafka → … → Postgres
```

The Glue subprocess + curated Parquet step adds ~60–120 s per batch.
For most batch-style insurance feeds that's fine; for near-real-time
sources (claims first-notification, fraud signals, low-latency pricing
feeds) it is not.

Goals for the direct-Kafka shape:

- End-to-end latency from source emit to Postgres < 30 s p95.
- Reuse every control-plane primitive we already have (run_log, stages,
  reconciliation_log, lineage_edge, watermark) — no new tables for the
  pattern itself.
- Keep replay safe (no record loss on poller crash, sink crash, or
  cursor commit race).
- Stay decoupled from `dag_ingest`. `dag_ingest` keeps its file-pattern
  contract; the direct-Kafka path is a separate runner.

## Non-goals

- Streaming joins, windowed aggregation, exactly-once semantics across
  multiple sources. Direct-Kafka is still per-record append; downstream
  sinks own dedup.
- Replacing the existing `api_pull` shape. Both ship side-by-side; YAML
  picks which.
- Removing Avro / Schema Registry. Schema-on-write is preserved.

## Architecture

```
┌──────────────┐   poll               ┌──────────────────────────────────┐
│ External API │ ───────────────────► │ ods_pipeline.ingest.api_pull_kafka│
└──────────────┘                      │   .stream_and_publish             │
                                      │ • bearer auth                     │
                                      │ • cursor (since_timestamp / etag) │
                                      │ • per-record envelope             │
                                      │ • Avro encode                     │
                                      └─────────────┬────────────────────┘
                                                    │ produce (idempotent)
                                                    ▼
                                            ┌──────────────────┐
                                            │ raw Kafka topic  │
                                            │ ods.<dom>.<ds>   │
                                            └─────┬────────┬───┘
                                                  │        │
                          Kafka Connect S3 sink   │        │  optional Glue
                          (Parquet or JSONL)      ▼        ▼  canonicalize
                                  ┌──────────────────┐    ┌──────────────────┐
                                  │ S3 ARCHIVE       │    │ canonical Kafka  │
                                  │ s3://…/api_pull/ │    │ ods.<dom>.<ds>-  │
                                  │   <ds>/year=…/   │    │ canonical        │
                                  └──────────────────┘    └────────┬─────────┘
                                                                   │
                                                          Kafka Connect JDBC
                                                                   ▼
                                                            ┌──────────┐
                                                            │ Postgres │
                                                            └──────────┘
```

The poller is the only new component. Two existing Kafka Connect sinks
(S3 + JDBC) do the rest.

## YAML config (delta from current `api_pull`)

```yaml
domain: insurance
dataset: api_pull_lowlat_demo
source_type: api_pull
pattern_type: api
delivery: direct_kafka                # NEW. opts: file_pipeline | direct_kafka
                                      # default = file_pipeline (preserves slice 1)

is_canonical: true
target_topic: ods.insurance.api_pull_lowlat_demo
schema_id: ods.insurance.api_pull_lowlat_demo-value
postgres_target_table: ods.insurance_api_pull_lowlat_demo

source:
  application: lowlat_api
  url: ${API_PULL_LOWLAT_URL}
  auth:
    type: bearer
    secret_ref: API_PULL_LOWLAT_TOKEN
  cursor:
    style: since_timestamp
    request_param: updated_since
    response_field: updated_at
    initial: "2026-05-01T00:00:00Z"
  page:
    style: link_header
  poll_interval_seconds: 5            # was: 15-minute Airflow schedule
  batch_max_records: 500              # cap per produce flush
  produce_acks: all                   # required for idempotent producer
  retries: 3
```

Two changes:

- `delivery: direct_kafka` — picked up by the new dispatcher in
  `dag_api_pull` to route to the new runner instead of the file
  pipeline.
- `poll_interval_seconds` — finer-grained than Airflow's minute
  granularity. The runner is a long-lived loop, not a per-tick task.

## Components

### 1. Long-running poller — `ods_pipeline.ingest.api_pull_kafka.runner`

A continuously-running Python process per dataset, NOT a per-schedule
Airflow task. Two deployment options:

- **(a)** Airflow `LongRunningOperator` / `KubernetesPodOperator` with
  `task_concurrency=1, max_active_tis_per_dag=1` and a heartbeat. Same
  scheduler. Good for environments without an extra orchestrator.
- **(b)** Standalone systemd / k8s Deployment. Better operational fit
  for sub-second polling but adds an out-of-Airflow runtime.

Pick **(a)** first; revisit if the dataset count grows past ~10.

Loop body (pseudo):

```python
def run_dataset(cfg):
    cursor   = build_cursor(cfg.source, watermark.read_committed())
    auth     = build_auth(cfg.source.auth)
    session  = make_session(auth, cfg.source.timeout_seconds, retries)
    producer = make_idempotent_avro_producer(
        topic            = cfg.target_topic,
        schema_id        = cfg.schema_id,
        bootstrap        = KAFKA,
        registry_url     = REGISTRY,
        enable_idempotence = True,
    )
    while not stop:
        request = cursor.initial_request()
        run_id  = uuid.uuid4()
        runs.start(run_id, pipeline_type="api_pull_lowlat", ...)
        stages.start(run_id, "raw_poll")
        try:
            for page in walk_pages(session, cursor):
                envelopes = [envelope(record, run_id, ...) for record in page]
                start_offsets, end_offsets = produce_batch(
                    producer, envelopes,
                    transactional_id=f"api_pull:{cfg.dataset}:{run_id}",
                )
                stages.write(run_id, "kafka_publish",
                             metrics={"offset_start_by_partition": start_offsets,
                                      "offset_end_by_partition":   end_offsets})
            new_cursor = cursor.advance(all_records)
            watermark.record_pending(run_id, new_cursor)
            reconciliation.write_check(
                check_type   = "api_pull_publish_count",
                source_count = len(all_records),
                kafka_count  = len(all_records),     # produce ACKed
                status       = "ok",
            )
            stages.finish(run_id, "raw_poll", "succeeded")
            runs.update(run_id, status="succeeded",
                        record_count_published=len(all_records))
        except Exception as exc:
            stages.finish(run_id, "raw_poll", "failed", error=str(exc))
            runs.update(run_id, status="failed", error_summary=str(exc))
            # do not advance watermark; loop continues
        sleep(cfg.source.poll_interval_seconds)
```

Key invariants:

- **Idempotent producer** (`enable.idempotence=true`, `acks=all`,
  `max.in.flight.requests.per.connection ≤ 5`) prevents per-record
  duplicates from broker retries.
- **Transactional producer** (`transactional.id` per `(dataset,
  run_id)`) makes the per-page produce atomic; either all records on
  the page land on the topic or none do. Optional but recommended for
  the recon contract below.
- **Per-record envelope** carries the same ODS metadata block the file
  pipeline writes, so consumers see identical correlation fields:
  `_ods_run_id`, `_ods_source_request_id`, `_ods_source_application`,
  `_ods_business_date`, `_ods_ingested_at`, `_ods_source_cursor`,
  `_ods_archive_s3_uri` (set when the S3 sink later writes; null at
  produce time — see "S3 archive" below).

### 2. S3 archive sink — Kafka Connect

Add a Connect S3 sink config per direct-Kafka dataset:

```json
{
  "name": "s3-sink-api-pull-lowlat-demo",
  "config": {
    "connector.class": "io.confluent.connect.s3.S3SinkConnector",
    "topics": "ods.insurance.api_pull_lowlat_demo",
    "s3.bucket.name": "ods-archive-local",
    "topics.dir": "api_pull",
    "path.format": "'year'=YYYY/'month'=MM/'day'=dd/'hour'=HH",
    "partitioner.class": "io.confluent.connect.storage.partitioner.TimeBasedPartitioner",
    "format.class": "io.confluent.connect.s3.format.parquet.ParquetFormat",
    "rotate.interval.ms": "60000",
    "flush.size": "1000",
    "schema.compatibility": "BACKWARD"
  }
}
```

The sink decides archive cadence (rotate every 1 min OR every 1000
records). Replay reads from S3 by time window, NOT by `file_id` —
identity moves from "supplier file" to "Kafka offset window".

### 3. JDBC sink — unchanged

Same JDBC sink as today, pointed at the dataset's raw or canonical
Kafka topic per `is_canonical`.

### 4. Watermark — unchanged shape, new commit signal

Reuses `pipeline.api_pull_watermark`. The behaviour change:

- `record_pending(run_id, cursor)` — called after producer flush returns
  end-offsets for every partition (i.e. broker has ACKed all records).
- `promote(run_id)` — called once **both** sinks have caught up to the
  produce end-offsets. Two checks, both via existing
  `connect_admin.wait_until_offset_consumed`:
  1. `s3-sink-<dataset>` consumer offsets ≥ produce end-offsets, and
  2. `jdbc-sink-<dataset>` consumer offsets ≥ produce end-offsets.
- `clear_pending(run_id)` — called on either sink failing for that
  run's offset window.

This swaps the slice-1 commit signal ("dag_ingest parent run
succeeded") for ("both downstream sinks consumed our offsets"). Same
two-phase contract; different oracle.

## Reconciliation contract

| Check | When | Source count | Compared to | Status condition |
|---|---|---|---|---|
| `api_pull_publish_count` | After producer flush | records fetched | producer end_offset − start_offset | discrepancy == 0 |
| `t0_publish_count` | (alias) | same as above | same | unchanged from existing |
| `archive_offset_lag` | When promote runs | s3 sink committed offsets | publish end_offsets | sink ≥ publish |
| `t2_sink_count` | When promote runs | jdbc sink committed offsets | publish end_offsets | sink ≥ publish |
| `t2_sink_count` (row check) | Hourly via `dag_recon_t2` | publish kafka offset delta | postgres rows tagged with run_id | within tolerance |

`api_pull_archive_count` is dropped — there is no pre-Kafka archive in
this shape.

## Lineage

```
api_to_kafka          (parent_run_id = api_pull_run_id, target = topic)
kafka_to_s3_archive   (child_run_id = api_pull_run_id, target = s3 sink path, lag offsets)
kafka_to_postgres     (child_run_id = api_pull_run_id, target = postgres table, lag offsets)
```

`file_id` is **not** assigned in this shape. Lineage primary key is
`(topic, start_offset, end_offset)`. If the dashboard or downstream
tooling requires `file_id`, mint a synthetic one:

```python
synthetic_file_id = uuid5(NAMESPACE, f"{topic}|{run_id}|{end_offsets_hash}")
```

Stored in `file_catalogue.file_id` with `state='kafka_published'` and
`s3_raw_path = NULL`. This keeps existing recon / dashboard joins
working without a schema change, but the meaning is "logical Kafka
batch", not "supplier file". Documented next to the column.

## Failure & recovery

| Failure | Today (file pipeline) | Direct Kafka |
|---|---|---|
| Source 5xx | run failed, no archive, no cursor advance | run failed, no produce, no cursor advance — same |
| Producer flush fails mid-page | n/a | transactional abort discards page; retry next loop iteration; no partial publish |
| S3 sink stuck | n/a (no S3 sink) | promote() blocks on offset lag; pending cursor stays pending; alarm on lag |
| JDBC sink stuck | promote blocks on `wait_until_offset_consumed` (existing) | same |
| Poller crashes mid-flush | n/a | idempotent producer + transactional id => broker discards uncommitted txn on next start; cursor is from `read_committed`, so window re-issues |
| Cursor commit race | impossible (PK linkage) | impossible (run_id-keyed pending row, same as today) |

Operational alarm to add: `s3_sink_lag_seconds` per dataset. Default
threshold 5 min. If S3 sink falls behind, archive cadence is wrong;
JDBC sink may still be promoting.

## Replay

Replay shape changes. Two modes:

- **(a) Replay from S3 archive**: read the parquet file(s) for a time
  window, re-publish to Kafka via a one-shot Glue job that reuses the
  producer logic. Same replay semantics as today's `dag_ingest` replay
  but the source is the S3 archive, not the upstream API.
- **(b) Replay from Kafka**: reset a fresh consumer-group offset on the
  raw topic and let the JDBC sink re-consume. Bounded by Kafka
  retention. No re-poll of the source.

For "lost a record entirely" scenarios neither (a) nor (b) helps — you
must re-poll with a rolled-back cursor. Procedure:

```sql
UPDATE pipeline.api_pull_watermark
   SET committed_cursor_value = '<earlier value>',
       pending_cursor_value   = NULL,
       pending_run_id         = NULL,
       updated_at             = NOW()
 WHERE domain=$1 AND dataset=$2 AND source_application=$3;
```

Source must accept the older cursor value. Re-poll re-publishes. Sinks
upsert by key (canonical) or append (history) — append datasets in this
mode require dedup downstream.

## Schema Registry

No change. The poller registers its raw subject (and canonical subject
when `is_canonical=false`) the same way `scripts/register_schemas.py`
already does.

## Migration plan from current `api_pull`

Per-dataset, opt-in via YAML:

1. Add `delivery: direct_kafka` to a dataset's YAML.
2. Add the dataset's `s3-sink-<ds>` Connect config; provision via
   `dag_config_sync` or manual `curl` to Connect.
3. Add the dataset's `jdbc-sink-<ds>` Connect config (existing pattern).
4. Add the dataset's target Postgres table migration (existing pattern).
5. Register Avro subject(s).
6. Switch `dag_api_pull` dispatcher: if
   `dataset_config.source_config.delivery == 'direct_kafka'`, route to
   the new long-running runner instead of `poll_one + dag_ingest`.
   Otherwise unchanged.

`dag_ingest` is untouched. The file pipeline keeps working for every
existing dataset.

## Tests required before ship

- Unit: producer envelope shape + transactional id derivation +
  idempotent producer config.
- Unit: cursor advance after partial-page flush failure (cursor must
  not advance).
- Integration (live): poller → real Kafka → real S3 sink → real JDBC
  sink → Postgres rows match. Equivalent of
  `test_api_pull_e2e_live.py` for the new shape.
- Integration: kill the poller mid-flush; restart; assert no duplicate
  records on the topic (idempotent + transactional).
- Integration: stop the S3 sink connector; assert promote blocks;
  pending cursor stays; restart; promote unblocks.
- Integration: stop the JDBC sink connector; same as S3 case.
- Integration: replay from S3 archive into Kafka via the one-shot
  replay job; assert idempotent at JDBC sink.

## Open decisions

| Decision | Default if not pushed back |
|---|---|
| Long-running runtime: Airflow vs standalone | Airflow `LongRunningOperator` (cuts new infra) |
| `file_id` synthetic in `file_catalogue` | yes — keeps dashboards / lineage joins working without schema change |
| Single-record produce vs micro-batch | micro-batch capped by `batch_max_records`; lower CPU, transactional cleaner |
| Archive format | Parquet via S3 sink (compresses better, fits curated convention) |
| Per-record S3 archive (legacy event-pattern style) | NO — relies on Connect S3 sink. Avoids another writer in the poller. |

## Out of scope (this design)

- Backfill from a cold start of a sub-minute source. (Probably
  reuse the existing `api_pull` file-pipeline shape for the initial
  bulk window, then flip the dataset to `direct_kafka` at cutover.)
- Multi-tenant rate limiting (per-source token bucket). Add when we
  have ≥3 direct-Kafka datasets sharing one source.
- Exactly-once across the source, Kafka, and Postgres. Source replay
  semantics dominate; we offer at-least-once with idempotent sinks.
