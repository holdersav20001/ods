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

The dashboard reads:

```text
dashboard/data/demo-workflow.json
```

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
- **Documentation:** common questions from the design discussion.

## Changed-Only Refeed

The demo refeed processes the corrected transaction file, but the target upsert
writes only rows whose payload changed. Unchanged target rows keep their
original `_ods_output_link_id`; changed rows show a superseded output and a
latest output in row history.

## Naming

- **Output link:** what a run produced.
- **Input edge:** what an output was made from.
- **upstream_output_link_id:** a previous output consumed as an input.
- **_ods_output_link_id:** the output id stamped on a target row.
