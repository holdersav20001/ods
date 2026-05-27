# Control Table Functions

Project code should not write directly to the Postgres control tables. Use the
database-owned `pipeline.control_*` functions via the `ods_ingestion_control`
Python package.

Example:

```python
import psycopg2
import ods_ingestion_control as control

with psycopg2.connect(dsn) as conn:
    control.start_run(
        conn,
        run_id=run_id,
        pipeline_type="ingestion",
        domain="insurance",
        dataset="policies",
        business_date="2026-05-23",
    )

    attempt = control.start_stage(conn, run_id=run_id, stage="raw_read")
    control.finish_stage(
        conn,
        run_id=run_id,
        stage="raw_read",
        status="succeeded",
        attempt_number=attempt,
        record_count_out=100,
    )
```

The package only calls `SELECT pipeline.control_*`. The functions own the table
mutation logic for `pipeline.run_log`, `pipeline.run_stage_log`,
`pipeline.file_catalogue`, `pipeline.file_processing_attempt`, `pipeline.lineage_edge`,
`pipeline.reconciliation_log`, and `pipeline.run_events`.

Grant projects `EXECUTE` on the functions rather than broad write access to the
tables whenever the deployment model allows it.
