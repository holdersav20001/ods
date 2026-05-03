# API pull runbook

Operator playbook for the api_pull pattern. Pair with
[docs/api-pull-onboarding.md](api-pull-onboarding.md) for first-time
dataset setup.

## Where things live

| Concern | Source of truth |
|---|---|
| Active datasets | `pipeline.dataset_config WHERE source_type='api_pull' AND active=TRUE` |
| Source URL / cursor / auth ref | `pipeline.dataset_config.source_config` JSONB |
| Cursor state | `pipeline.api_pull_watermark` |
| Archived batches | `pipeline.file_catalogue WHERE dataset=...` (rows where `s3_raw_path` starts with `s3://ods-raw-*/api_pull/...`) |
| Run lifecycle | `pipeline.run_log` (`pipeline_type='api_pull'` + linked `s3_batch` parent) |
| Stage progression | `pipeline.run_stage_log` (stages: `raw_poll`, `message_archive`, `recon_message`, `finalise`, then file-pattern downstream) |
| Recon | `pipeline.reconciliation_log WHERE check_type='api_pull_archive_count'` |
| Lineage | `pipeline.lineage_edge WHERE edge_type IN ('api_to_archive','raw_to_curated','curated_to_kafka')` |
| Run events | `pipeline.run_events WHERE event_type LIKE 'api_pull.%'` |

## Common operations

### Inspect the most recent poll for a dataset

```sql
SELECT r.run_id, r.status, r.started_at, r.ended_at,
       r.record_count_source, r.record_count_published,
       w.committed_cursor_value, w.pending_cursor_value
  FROM pipeline.run_log r
  LEFT JOIN pipeline.api_pull_watermark w
    ON w.domain=r.domain AND w.dataset=r.dataset
 WHERE r.domain=$1 AND r.dataset=$2
   AND r.pipeline_type='api_pull'
 ORDER BY r.started_at DESC
 LIMIT 5;
```

### Find the archive S3 URI + downstream linkage for an api_pull run

```sql
SELECT s.output_ref AS archive_uri,
       fc.file_id, fc.state, fc.s3_curated_path,
       sb.run_id::text AS dag_ingest_run, sb.status AS dag_ingest_status
  FROM pipeline.run_stage_log s
  JOIN pipeline.file_catalogue fc ON fc.s3_raw_path = s.output_ref
  LEFT JOIN pipeline.run_log sb
    ON sb.pipeline_type='s3_batch'
   AND sb.parents @> jsonb_build_array(jsonb_build_object(
         'run_id', $1::text, 'edge_type', 'triggered_by_api_pull'))
 WHERE s.run_id=$1::uuid AND s.stage='message_archive';
```

### How the linkage prevents wrong-cursor promotion

`dag_api_pull` pre-mints the dag_ingest parent run_id deterministically
from the api_pull run_id:

```python
parent_run_id = uuid.uuid5(uuid.NAMESPACE_OID,
                           f"api_pull:{api_pull_run_id}")
```

It passes that as ``parent_run_id`` in the dag_ingest trigger conf, AND
records ``triggered_by_api_pull`` as a parent edge in
``run_log.parents``. ``finalise_watermark`` then looks up
the downstream run by primary key (`run_log.run_id = parent_run_id`)
**and** verifies the parent edge — so:

- A "latest by file_id" replay cannot match (PK does not match).
- TriggerDagRunOperator retries that mint a different PK cannot
  promote the wrong row (PK does not match).
- A spurious row inserted at the same PK without the edge is rejected
  by the edge check.

If the caller supplies no ``expected_parent_run_id`` (legacy / test
paths), the lookup falls back to JSONB containment on the edge alone
and returns ``None`` if more than one row matches — "ambiguous, do not
promote".

### Replay a failed pull

A pull's archive is immutable on S3 (path keyed by ``run_id``).
Recovery options depend on which step failed:

| Failure | Recovery |
|---|---|
| Source 5xx after retries | No archive written, no cursor advance. Wait for next schedule, or trigger `dag_api_pull` manually once source is healthy. |
| Bearer token missing/invalid | Fix `secret_ref` env var. Re-run as above. |
| Archive ok, downstream `dag_ingest` failed | `finalise_watermark` clears the pending cursor. The same window will be re-polled and re-archived on the next schedule (the source typically returns the same records, so the result is idempotent). To recover faster, manually trigger `dag_ingest` for the existing `file_id` (see "Manual replay of an existing file_id" below). |
| Pending cursor stuck (`finalise_watermark` timeout) | See "Recover a stuck pending watermark" below. |

### Manual replay of an existing file_id

Use this to re-run `dag_ingest` against an already-archived batch
without re-polling the source:

```bash
docker exec avivaods-airflow-scheduler-1 \
    airflow dags trigger \
    --conf '{
      "file_id": "<uuid>",
      "domain": "<domain>",
      "dataset": "<dataset>",
      "business_date": "<YYYY-MM-DD>",
      "replay_of_run_id": "<previous_dag_ingest_run_id>",
      "replay_request_id": "<ticket-or-incident-id>"
    }' \
    dag_ingest
```

The `replay_of_run_id` adds a `replay` parent edge in `run_log.parents`
(distinct from `triggered_by_api_pull`). This means the api_pull
watermark sensor will NOT observe this manual replay — the pending
cursor lifecycle stays driven by the original poll.

### Recover a stuck pending watermark

Symptom: ``pipeline.api_pull_watermark.pending_cursor_value`` is set and
``pending_run_id`` does not match any in-flight run. The
``finalise_watermark`` sensor likely timed out
(``API_PULL_DOWNSTREAM_TIMEOUT_SECONDS``, default 1800s) without the
downstream `dag_ingest` either succeeding or failing.

Decision tree:

1. Check the downstream run carrying the api_pull edge:

   ```sql
   SELECT run_id::text, status, started_at, ended_at, error_summary
     FROM pipeline.run_log
    WHERE pipeline_type='s3_batch'
      AND parents @> jsonb_build_array(jsonb_build_object(
            'run_id', '<pending_run_id>'::text,
            'edge_type', 'triggered_by_api_pull'));
   ```

2. If status is `succeeded`, manually promote:

   ```sql
   UPDATE pipeline.api_pull_watermark
      SET committed_cursor_value=pending_cursor_value,
          last_successful_run_id=pending_run_id,
          pending_cursor_value=NULL,
          pending_run_id=NULL,
          updated_at=NOW()
    WHERE domain=$1 AND dataset=$2 AND source_application=$3
      AND pending_run_id::text=$4;
   ```

3. If status is `failed`/`partial`, manually clear:

   ```sql
   UPDATE pipeline.api_pull_watermark
      SET pending_cursor_value=NULL,
          pending_run_id=NULL,
          updated_at=NOW()
    WHERE domain=$1 AND dataset=$2 AND source_application=$3
      AND pending_run_id::text=$4;
   ```

4. If no row exists, the trigger never ran (Airflow trigger failure).
   Clear pending as in (3) and trigger `dag_api_pull` manually.

5. If the watermark `locked_at` is non-null but no DAG run is in
   flight, force-unlock:

   ```sql
   UPDATE pipeline.api_pull_watermark
      SET locked_at=NULL, updated_at=NOW()
    WHERE domain=$1 AND dataset=$2 AND source_application=$3;
   ```

   This is safe because each poll generates a new ``api_pull_run_id``
   and the lock guard is per-dataset, not per-run.

### Force a fresh full pull (drop all watermark state)

Only do this if you have confirmed the source is willing to re-emit
all historical records and the downstream sinks are upsert-keyed.

```sql
DELETE FROM pipeline.api_pull_watermark
 WHERE domain=$1 AND dataset=$2 AND source_application=$3;
```

The next `dag_api_pull` schedule will re-create the row with NULL
cursors and start from the configured `cursor.initial`.

### Pause a dataset

Set ``active=FALSE`` in dataset_config; ``dag_api_pull`` skips inactive
rows.

```sql
UPDATE pipeline.dataset_config SET active=FALSE
 WHERE domain=$1 AND dataset=$2;
```

## Cursor strategy behaviour (slice 1)

| Style | Stored watermark | Page traversal | Notes |
|---|---|---|---|
| `since_timestamp` | `max(response_field)` over fetched records | RFC 5988 `Link: rel=next` if `page.style=link_header`; one-shot if `none` | If the page is empty the cursor is left unchanged so the next poll re-issues the same window. |

Other styles (`etag`, `offset`, `full_replace`) are tracked in
[docs/api-pull-backlog.md](api-pull-backlog.md) item 5.

## Known limits

- Slice 1 verifies the path up to and including Glue JSONL ingestion +
  curated parquet write. Kafka publish, canonicalize and JDBC sink
  use the same code as the file pattern; their integration is tracked
  by [docs/api-pull-backlog.md](api-pull-backlog.md) item 1.
- Dashboard panels for api_pull are tracked by item 2.
- 304 Not Modified is honoured by the poller but slice 1 does not yet
  emit `If-None-Match` headers (etag cursor strategy is item 5).

## Security checklist

- Never put tokens in the YAML or in ``dataset_config.source_config``.
  ``yaml_loader._scrub_secrets`` strips obvious key names before
  persisting, but the right place for secrets is the env / Airflow
  Secrets Backend referenced by `secret_ref`.
- Never log token values in poller / DAG output. The ``BearerAuth``
  provider stores the token as an instance attribute and only writes
  ``Authorization: Bearer ***`` shape to the session header — do not
  add token-content logging.
- Rotate tokens by updating the env var and restarting Airflow workers
  (or re-loading the Secrets Backend). No code or config change needed
  on rotation.
