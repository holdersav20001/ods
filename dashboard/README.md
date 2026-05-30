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

Expected snapshot sections:

- `executions`: three normal loads plus the Day 2 transaction refeed
- `runs`: control-plane run rows with nested stages
- `links`: lineage links with nested edges
- `tables`: target rows from `ods.customer_transaction` and `ods.customer_transaction_daily`
- `traces`: trace rows keyed by `_ods_lineage_link_id`

## Tabs

- **Control Links:** inspect one execution/run, focus a link, and see raw-file trace.
- **Run Flow:** React Flow graph for the whole scenario or selected execution.
- **Target Rows:** pick a table/date, click a row, and jump back to its focused lineage link.
