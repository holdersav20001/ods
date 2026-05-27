# Why Control Tables?

People will reasonably ask whether CloudWatch, Spark plans, Airflow logs, or JDBC/Postgres metrics already provide this information.

The short answer:

```text
CloudWatch, Spark, Airflow, and JDBC metrics tell us what the compute did.
Control tables tell us what business data moved, why, under which config, and how to prove it later.
```

They overlap slightly, but they are not substitutes.

## What The Existing Tools Give Us

| Tool | Good for | Limitation |
|---|---|---|
| CloudWatch | Job logs, errors, stack traces, container metrics, timestamps. | Logs are not a durable data-movement ledger. They are hard to join to a target row or source file. |
| Spark UI / Spark plan | Physical execution, shuffles, tasks, partitions, performance debugging. | It explains execution mechanics, not business lineage or reconciliation. |
| Airflow | DAG/task status, scheduling, retries, dependencies. | It knows orchestration state, not necessarily which business rows/files reached each target. |
| JDBC/Postgres metrics | Database writes, SQL failures, connection/load issues. | It does not explain source file, config version, upstream lineage, or cross-stage reconciliation by itself. |

These tools are operational telemetry. We still need them.

Control tables are different: they are data-movement evidence.

## What Control Tables Provide

Control tables answer questions the other tools cannot answer cleanly as one joined story:

```text
Which exact source file produced this Postgres row?
Which run wrote it?
Which config version was active?
Was the row count reconciled?
Was the file already processed before?
Where is the raw copy?
Where is the silver copy?
Which transform/load path was used?
What other inputs contributed to this gold row?
```

CloudWatch might show:

```text
job X ran at 10:03 and wrote 100 rows
```

Control tables can show:

```text
file country_codes_20260521.csv
  -> file_id
  -> raw S3 path
  -> silver S3 path
  -> ingestion run
  -> direct_postgres run
  -> target table rows via _ods_run_id
  -> lineage edges
  -> reconciliation checks
  -> config_version_id
```

That is the key difference.

## The Real Value

The value is not "more logging". The value is a queryable, durable data-movement ledger.

Control tables provide:

| Capability | What it gives us |
|---|---|
| Auditability | Prove where a target row came from. |
| Reconciliation | Prove row counts matched between stages. |
| Restart safety | Know what has already completed and what can be resumed. |
| Supportability | Answer incidents with SQL instead of searching logs. |
| Lineage | Walk from gold/Postgres back to silver/raw/source file. |
| Config traceability | Know which dataset config version produced a run. |
| Cross-tool consistency | Join file, run, stage, lineage, and reconciliation evidence in one place. |

## Example Question

If a user asks:

```text
Why is this row in Postgres?
```

The answer should not require manually reading CloudWatch logs, Spark plans, Airflow task attempts, and database history.

With control tables and target metadata, we can query:

```text
Postgres target row
  -> _ods_run_id
  -> pipeline.run_log
  -> pipeline.reconciliation_log
  -> pipeline.lineage_edge
  -> pipeline.file_catalogue
  -> S3 raw / S3 silver
```

That is the evidence chain.

## Runtime Context Metadata

We should keep our own `pipeline.run_log.run_id` as the pipeline identity. That
is the ID target rows, stage records, reconciliation checks, and lineage edges
can safely point at.

Platform IDs from Glue, Lambda, MWAA/Airflow, Spark, and CloudWatch are still
useful, but they are runtime correlation metadata rather than the lineage key.
Store them on the run as optional JSON:

```sql
pipeline.run_log.runtime_context jsonb
```

Example:

```json
{
  "platform": "glue",
  "glue_job_name": "ods_postgres_write",
  "glue_job_run_id": "jr_123456789",
  "spark_app_id": "application_1716290000000_0001",
  "airflow_dag_id": "dag_ingest_direct_postgres",
  "airflow_run_id": "manual__2026-05-23T09:15:00+00:00",
  "cloudwatch_log_group": "/aws-glue/jobs/output",
  "cloudwatch_log_stream": "jr_123456789"
}
```

This gives us both sides:

```text
_ods_run_id / pipeline.run_log.run_id
  -> durable data-movement identity

pipeline.run_log.runtime_context
  -> jump-off point to CloudWatch, Glue, Spark, Lambda, or Airflow evidence
```

`runtime_context` should not replace `run_id` or `file_id`. It makes external
logs easier to find, while the control tables remain the durable ledger for what
business data moved.

## Is There Another Way?

Yes. There are alternatives.

| Alternative | Trade-off |
|---|---|
| Use only CloudWatch/Airflow/Spark logs | Lower upfront effort, but weak audit, lineage, and SQL investigation. |
| Use OpenLineage, Marquez, DataHub, or Atlas | Strong lineage capability, but a larger platform commitment. Operational run/file/reconciliation state may still be needed. |
| Store only table metadata in Postgres/S3 | Useful for row tagging, but not enough for retries, reconciliation, or multi-step evidence. |
| Emit structured events to an observability platform | Useful if a mature platform already exists, but we still need a durable query model for audit and support. |
| Use lightweight Postgres control tables | Simple, queryable, pragmatic, and close to the data movement process. |

## Position

Control tables should not be presented as a replacement for CloudWatch, Spark UI, Airflow, or database metrics.

They serve a different purpose:

```text
CloudWatch can prove a job ran.
The control tables prove what data that job moved.
```

The best framing is:

```text
CloudWatch/Spark/Airflow are operational observability.
Control tables are data-movement evidence.
We need both.
```

## Design Principle

For every material data movement, the platform should leave durable evidence that can be queried later:

```text
source file or input
  -> run
  -> stage evidence
  -> output
  -> reconciliation
  -> lineage
```

That evidence should be available without replaying logs or reconstructing events from multiple tools.
