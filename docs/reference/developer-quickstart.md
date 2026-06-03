# Developer Quickstart

This repository is the Postgres control-plane reference implementation. Use it to
prove the database contract, wrapper behavior, demos, diagnostics, and dashboard
snapshots before moving the write API into a separate repository.

## 1. Start Postgres

The default local database is:

```text
host=localhost
port=5440
dbname=ods_cp
user=ods
password=ods
```

If the container is stopped:

```powershell
docker start avivaods-postgres-1
```

Use TCP (`localhost:5440`). On this host, `docker exec ... psql` can fail, so the
repo uses psycopg over TCP.

## 2. Apply Migrations From Empty

```powershell
python -m db.apply --drop
```

This drops `cp` and `ods`, then applies all ordered migrations in
`db/migrations/` (`001` through `033`). Use this before release checks and before
regenerating committed dashboard snapshots.

## 3. Run The Demos

```powershell
python -m harness.customer_transaction_workflow --out dashboard/data/demo-workflow.json
python -m harness.policy_claims_workflow --out dashboard/data/policy-claims-workflow.json
python -m harness.policy_claims_dlq_workflow --out dashboard/data/policy-claims-dlq-workflow.json
```

Each harness resets only its own domain:

- `sales`
- `insurance`
- `insurance_dlq`

That means all three demos can coexist in one database after a clean run.

## 4. Open The Snapshot Dashboard

```powershell
python -m http.server 8099 --directory dashboard
```

Open:

```text
http://localhost:8099/index.html
http://localhost:8099/index.html?data=policy-claims-workflow.json
http://localhost:8099/index.html?data=policy-claims-dlq-workflow.json
```

The dashboard is static. It reads JSON files from `dashboard/data/`; it does not
query Postgres live.

## 5. Run Tests

```powershell
python -m pytest -q
```

Current expected result:

```text
389 passed, 1 skipped, 1 xfailed
```

The expected failure is:

```text
tests/test_refeed_policy.py::test_manual_approval_pending_is_future_option
```

Manual approval pending status `P` is documented as a future option and is not
implemented in this reference release.

## 6. Inspect Workflows Without The Dashboard

List workflows:

```sql
SELECT workflow_run_id, domain, business_date, trigger_type,
       run_count, stage_count, output_count, input_count,
       target_visibility_count, orchestrator_type
FROM cp.dashboard_workflows()
ORDER BY first_started_at;
```

Open one workflow as JSON:

```sql
SELECT cp.dashboard_workflow_detail('<workflow_run_id>');
```

Run diagnostics:

```sql
SELECT check_name, severity, object_type, object_id, message
FROM cp.developer_diagnostics('<workflow_run_id>')
ORDER BY severity, check_name;
```

Empty diagnostics means the workflow is healthy.

## 7. Trace Data

Trace an output:

```sql
SELECT hop, edge_type, dataset, source_file_id, raw_s3_path
FROM cp.dashboard_output_trace('<output_link_id>')
ORDER BY hop;
```

Trace a target row:

```sql
SELECT hop, edge_type, dataset, source_file_id, raw_s3_path
FROM cp.dashboard_target_row_trace('ods', 'customer_transaction', 1)
ORDER BY hop;
```

Trace a raw file downstream:

```sql
SELECT output_link_id, edge_type, workflow_run_id, dataset, target_ref
FROM cp.dashboard_file_impact('<file_id>');
```

## 8. Release Check

Before handing this repo to the API project, run:

```powershell
python -m db.apply --drop
python -m harness.customer_transaction_workflow --out dashboard/data/demo-workflow.json
python -m harness.policy_claims_workflow --out dashboard/data/policy-claims-workflow.json
python -m harness.policy_claims_dlq_workflow --out dashboard/data/policy-claims-dlq-workflow.json
python -m pytest -q
```

Then verify:

```sql
SELECT status, count(*) FROM cp.run_log GROUP BY status;
SELECT status, count(*) FROM cp.reconciliation_log WHERE check_type='workflow' GROUP BY status;
SELECT domain, dataset, status, count(*) FROM ods.target_visibility GROUP BY domain, dataset, status;
```

Expected:

- all runs are `succeeded`
- workflow reconciliation rows are `ok`
- no duplicate active target-visibility rows
- quarantine edges have `source_file_id`
- `_ods_lineage_link_id` and `_ods_output_link_id` match on demo targets
