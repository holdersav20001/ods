# ODS Lineage Dashboard

Static React dashboard for the customer/transaction lineage demo snapshot.

## Run

```powershell
python -m http.server 8099 --directory dashboard
```

Open:

```text
http://localhost:8099/index.html
```

## Regenerate Data

From the repository root:

```powershell
python -m harness.customer_transaction_workflow --out dashboard/data/demo-workflow.json
```

The generator resets generated demo/control rows by default before writing a
fresh snapshot. Use `--no-reset` only when you intentionally want to append.

## Snapshot

By default the dashboard reads:

```text
dashboard/data/demo-workflow.json
```

### Switching snapshots

The dashboard can load either known snapshot without editing JS:

- Header **snapshot** dropdown (top of the page), or
- the `?data=` query param, e.g.
  `http://localhost:8099/index.html?data=policy-claims-workflow.json`.

Known snapshots:

```text
dashboard/data/demo-workflow.json               (default; customer/transaction)
dashboard/data/policy-claims-workflow.json      (insurance policy/claims, Airflow)
dashboard/data/policy-claims-dlq-workflow.json  (insurance DLQ quarantine + replay)
```

Regenerate the policy/claims snapshots from the repo root:

```powershell
python -m harness.policy_claims_workflow --out dashboard/data/policy-claims-workflow.json
python -m harness.policy_claims_dlq_workflow --out dashboard/data/policy-claims-dlq-workflow.json
```

### DLQ / quarantine + replay

The DLQ snapshot demonstrates the quarantine path. When it is loaded, the
**Workflow Diagram** tab shows:

- a red **DLQ / quarantine** banner summarising the quarantined records
  (`scenario.quarantined_claim_id`), the stage that failed
  (`scenario.dlq_stage`), the `dlq_id` / DLQ S3 target, and whether a
  `trigger_type='replay'` execution resolved it (open → resolved), and
- the `edge_type='quarantine'` output link rendered as a red **DLQ** card in
  the producing run's column.

The status (open vs resolved) is derived entirely from the snapshot — a
quarantine output link plus a succeeded replay run — with no live query.

Expected sections:

- `executions`: three normal loads plus one Day 2 transaction refeed
- `runs`: `run_log` rows with nested stages
- `links`: output links with nested input edges
- `tables`: target rows from `ods.customer_transaction` and
  `ods.customer_transaction_daily`
- `files`: raw file catalogue rows used by the workflows
- `traces`: raw-file trace rows keyed by output link id

## Tabs

- **Workflows:** workflow-level metrics, process links, JSON, and OL export.
- **Metadata Map:** high-level metadata relationships.
- **Control Links:** run/output/input inspection and raw-file trace.
- **Run Flow:** React Flow graph of run inputs and outputs.
- **Workflow Diagram:** top-to-bottom workflow flow.
- **Process Model:** task, stage, input, and output cards.
- **Developer Model:** clickable cards showing API calls/payloads.
- **Target Rows:** row-level history and output-link traceability.
- **Templates:** implementation examples.
- **Documentation:** common questions from the design discussion, plus a
  read-side **Diagnostics & docs** panel listing the support SQL functions
  (`cp.dashboard_workflows`, `cp.developer_diagnostics`,
  `cp.dashboard_output_trace`, `cp.dashboard_file_usage`,
  `cp.dashboard_target_row_trace`, `cp.dashboard_airflow_lookup`) and pointers
  to the write-contract and runbook docs. Static text only — no live query.

## Changed-Only Refeed

The demo refeed processes the corrected transaction file, but the target upsert
writes only rows whose payload changed. Unchanged target rows keep their
original `_ods_output_link_id`; changed rows show a superseded output and a
latest output in row history.

## Orchestrator identity

Airflow-driven runs carry orchestrator identity (`orchestrator_type/dag_id/run_id/
task_id/try_number/map_index/url`). When present the dashboard surfaces it as:

- an `orchestrator={...}` argument in the Developer Model `runs.start(...)` snippet, and
- an `ods_orchestrator` run facet in the OpenLineage (OL) export.

Non-orchestrated runs are unaffected (no extra argument, no facet).

## Naming

- **Output link:** what a run produced.
- **Input edge:** what an output was made from.
- **upstream_output_link_id:** a previous output consumed as an input.
- **_ods_output_link_id:** the output id stamped on a target row.
