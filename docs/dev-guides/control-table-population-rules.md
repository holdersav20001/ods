# Control-table population rules

This is the contract every pipeline step MUST follow when writing to the
`pipeline.*` control tables. The dashboard at `ops/lineage_dashboard/`
visualises these rows directly — if a step ignores the rules, its
artefacts will be mislabelled (e.g. curated parquet shown as "raw file")
or invisible.

## 1. One write event per write

Every time a step writes data to a new artefact (an S3 prefix, a
Postgres table, a Kafka topic) it MUST record exactly **one** row in
`pipeline.lineage_link` and one or more rows in `pipeline.lineage_edge`.

Use the helper:

```python
import ods_pipeline

ods_pipeline.lineage.write_link(
    conn,
    lineage_link_id=str(uuid.uuid4()),    # mint BEFORE stamping rows
    consumer_run_id=<this_run_id>,        # the run doing the write
    edge_type="<verb>_to_<noun>",         # see §3
    target_ref="<output URI>",            # see §4
    record_count=<rows_written>,
    contributions=[
        {
            "upstream_run_id":  <previous_run_id_or_None>,
            "source_file_id":   <file_catalogue_id_or_None>,
            "source_ref":       "<input URI>",       # see §4
            "input_slot":        "<slot_or_'main'>",  # see §5
            "record_count":     <rows_read>,
            "edge_type":        "<same as parent>",
        },
        # ... one entry per source contribution
    ],
)
```

The bundle (`lineage_link` + N `lineage_edge` rows) is one transaction.
Never leave a lineage_edge orphaned.

## 2. Stamp every output row with `_ods_lineage_link_id`

For Postgres targets and S3 parquet outputs, stamp the lineage_link_id
on every row before the write so a single row can be traced back to its
write event without joining on file_id / run_id.

```python
lineage_link_id = str(uuid.uuid4())
df = df.withColumn("_ods_lineage_link_id", F.lit(lineage_link_id))
df.write....
ods_pipeline.lineage.write_link(... lineage_link_id=lineage_link_id ...)
```

Mint the id **before** stamping so the same id ends up on the rows and
in the bundle.

## 3. `edge_type` naming

`edge_type` is a short verb-phrase describing the write event. Follow
this pattern:

    <source_layer>_to_<target_layer>

Layers in the file-batch route:

| Layer        | Where the artefacts live                            |
|--------------|------------------------------------------------------|
| `raw`        | `s3://<raw-bucket>/<domain>/<dataset>/...`           |
| `curated`    | `s3://<curated-bucket>/<domain>/<dataset>/date=.../` |
| `staging`    | `pipeline.slot_staging_<slot>` (Postgres)            |
| `canonical`  | `s3://<curated-bucket>/canonical/<domain>/<ds>/...`  |
| `merged`     | the wide table written by `ods_merge`                |
| `postgres`   | any `ods.*` Postgres target                          |

Examples:

- `raw_to_curated`           — ingestion stage
- `curated_to_canonical`     — ods_canonicalize_file (direct-PG route)
- `staging_to_canonical`     — ods_canonicalize_slot (merge route)
- `canonical_to_merged`      — ods_merge contributions
- `merge_to_postgres`        — ods_merge writing to `ods.policies_enriched`
- `curated_to_postgres`      — ods_postgres_write (direct-PG)

If your step writes to a new layer, pick a new noun and document it
above. Do NOT reuse `raw_to_curated` for a curated→canonical step.

## 4. `source_ref` and `target_ref`

Both are free-text URIs describing WHERE the bytes live.

- **`target_ref`** = the URI of THIS write event's OUTPUT.  Required.
  - S3 prefix:    `s3://bucket/path/date=YYYYMMDD/`
  - Postgres:     `jdbc:postgresql://<host>/<schema>.<table>`
  - Kafka topic:  `kafka://<cluster>/<topic>`

- **`source_ref`** = the URI of the contribution's INPUT. Required for
  every `lineage_edge` row.
  - For ingestion: the raw CSV/JSONL path on S3 or SFTP.
  - For canonicalize: the curated parquet prefix (NOT the raw csv).
  - For merge: the canonical parquet prefix per slot (NOT the slot
    staging table).
  - For postgres_write: the canonical parquet prefix (or curated parquet
    when no canonicalize step ran).
  - For staging-table reads: `postgres://pipeline.slot_staging_<slot>`.

The dashboard classifies nodes by URI prefix (see
`ops/lineage_dashboard/api/main.py::_artefact_kind`). Mis-pointing
`source_ref` at the raw csv when the step actually read curated parquet
is what makes the dashboard show a "raw file" box where it should say
"curated".

## 5. `input_slot`

Per-edge tag describing the role of this contribution within the link.

- Single-source writes: `"main"`.
- Multi-source writes (e.g. `ods_merge`): one of the slot names from
  `dataset_config.input_slot`, e.g. `"core"`, `"enrichment"`.
- Defensive / empty links (no actual data): `"empty"`.

## 6. `pipeline_type` on `run_log`

The `pipeline_type` column drives which YAML configs the dashboard
surfaces and which control-table rows are visible. Use:

| pipeline_type     | Stage                                              |
|-------------------|----------------------------------------------------|
| `orchestration`   | The DAG/route-level scheduling row.                |
| `ingestion`       | Raw → curated parquet (ods_ingestion).             |
| `stage`           | Raw → slot_staging Postgres table (ods_stage).     |
| `canonicalize`    | Curated/staging → canonical parquet.               |
| `merge`           | Per-slot canonical parquets → merged wide table.   |
| `direct_postgres` | Curated/canonical parquet → ods.* Postgres target. |
| `load`            | Generic Postgres write (alias of direct_postgres). |

Any new pipeline_type MUST be added to the dashboard's
`configsForRun()` in
`ops/lineage_dashboard/web/src/NodeDetails.jsx` so the right YAML
badges show up.

## 7. Always pre-mint `lineage_link_id`

Never let `write_link()` mint the id for you on file-batch routes —
mint it yourself with `str(uuid.uuid4())` and stamp it on the output
rows BEFORE calling `write_link`. Otherwise rows land without a
traceable handle if the write_link call fails.

## 8. Use `run_log.runtime_context` for declarative metadata

Anything non-schema (transform yaml path, applied actions, output S3
URI, slot name, airflow run id) goes into `runtime_context` JSONB on
the run_log row. The dashboard renders it in the run detail panel.
Don't invent new columns.

## 9. Cascade is on; tests can wipe by run_log

`pipeline.lineage_link.consumer_run_id`, `lineage_edge.consumer_run_id`,
`lineage_edge.upstream_run_id`, `lineage_edge.lineage_link_id`, and
`run_stage_log.run_id` all CASCADE on delete from run_log. A test or
dev clean-up can safely `DELETE FROM pipeline.run_log WHERE ...` and
the lineage tables will follow.

## 10. Reference implementations

The canonical examples of each rule in action:

- Single-file ingest → curated:  `glue/jobs/ingestion/finalising.py`
- File-batch canonicalize:       `glue/jobs/ods_canonicalize_file.py`
- Per-slot canonicalize:         `glue/jobs/ods_canonicalize_slot.py`
- Multi-source merge:            `glue/jobs/ods_merge.py`
- Postgres direct-write:         `glue/jobs/ods_postgres_write.py`

Read those before writing a new step. If your step doesn't fit, raise
a PR that documents the new rule HERE.
