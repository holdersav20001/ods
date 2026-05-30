# ODS Lineage Dashboard

Static dashboard for the customer/transaction lineage demo snapshot.

## Run

```powershell
python -m http.server 8099 --directory dashboard
```

Open:

```text
http://localhost:8099
```

## Data

The dashboard reads:

```text
dashboard/data/demo-workflow.json
```

Naming (see `docs/specs/2026-05-30-output-link-input-edge-rename.md`):

- **Output link** — what a run produced (`output_link_id`; view `cp.output_link`,
  physical table `cp.lineage_link`).
- **Input edge** — what that output was made from (view `cp.input_edge`, physical
  table `cp.lineage_edge`).
- **upstream_output_link_id** — a previous output used as input (physical column
  `upstream_lineage_link_id`).

Expected snapshot sections:

- `executions`: three normal loads plus the Day 2 transaction refeed
- `runs`: control-plane run rows with nested stages
- `links`: output links with their nested input edges (snapshot keys remain the
  physical `lineage_link_id` / `lineage_edge_id` / `upstream_lineage_link_id`)
- `tables`: target rows from `ods.customer_transaction` and `ods.customer_transaction_daily`,
  each carrying both `_ods_lineage_link_id` and the new-name mirror `_ods_output_link_id`
- `traces`: trace rows keyed by output link id

## Tabs

- **Control Links:** inspect one execution/run, focus an output link, and see raw-file trace.
- **Run Flow:** React Flow graph for the whole scenario or selected execution.
- **Target Rows:** pick a table/date, click a row, and jump back to its focused output link.
