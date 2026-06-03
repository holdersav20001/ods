# ODS Control-Plane Reference Platform

A working, Spark-free **ODS control-plane reference platform** on Postgres. It is
a complete, runnable, tested implementation of the metadata layer an
Airflow/Glue/Spark estate would call into to record **what ran, what it
produced, what it consumed, and what is business-active right now** — with exact
row-level traceability.

It proves, end to end and under test:

- **restartable workflow control** — runs and stages tracked in `cp.run_log` /
  `cp.run_stage_log`, grouped by `workflow_run_id`.
- **exact input/output lineage** — `cp.output_link` (what a run produced) and
  `cp.input_edge` (what that output consumed), walked link-to-link.
- **target row traceability** — every `ods.*` row is stamped with
  `_ods_output_link_id` / `_ods_workflow_run_id`, so any row traces back to the
  raw file it came from.
- **active/inactive target visibility** — `ods.target_visibility` records which
  output is business-active, activated only after a run **succeeds** and its
  reconciliation is **ok**.
- **changed-only refeed** — a corrected file replaces only the intended business
  scope; unchanged business keys keep their original output (lineage is never
  rewritten — it is immutable audit truth).
- **DLQ / data-quality handling** — bad rows are quarantined to `cp.dlq` with a
  first-class quarantine output_link, stay traceable to the raw input, and can
  be replayed/resolved.
- **schema validation** — rows are validated against `cp.schema_contract`.
- **developer ergonomics** — thin Python wrappers in `control/` and a context-
  manager SDK in `control/sdk.py`.
- **support diagnostics** — read-only SQL functions to trace and diagnose
  (`cp.dashboard_*`, `cp.developer_diagnostics`).

## What This Repo Is NOT

This is a **reference platform**, not the production service. It deliberately
does **not** include (these belong to a future repository):

- the production FastAPI write **API service**,
- a **Redis**-backed service,
- an **OpenLineage** ingestion service as the source of truth,
- a multi-tenant production deployment.

The dashboard is intentionally **snapshot-based** (it does not live-query
Postgres). Postgres control tables remain the source of truth; the OpenLineage
export is a *derived* view. See `docs/specs/2026-06-03-working-platform-completion-plan.md`
("Repository Boundary" / "Non-Goals") for the boundary.

### The future API / Redis / OpenLineage repo

A separate, future repository will host the production write **API** (the
control-plane write contract served over HTTP), a **Redis**-backed hot path for
identity/locking, and **OpenLineage** ingestion. That repo will *consume* this
platform's contract — the ordered write sequence documented in
`docs/reference/control-plane-write-contract.md` — rather than reinvent it. This
repo exists to make that contract concrete, correct, and tested first.

---

## Quick Start

```powershell
# 1. Postgres must be running (see "Start Postgres" below)
# 2. Create the schema + apply all migrations
python -m db.apply --drop
# 3. Run the tests
python -m pytest -q
# 4. Run a demo and regenerate its dashboard snapshot
python -m harness.customer_transaction_workflow --out dashboard/data/demo-workflow.json
# 5. Open the dashboard
python -m http.server 8099 --directory dashboard
#    then browse http://localhost:8099/index.html
```

## Start Postgres

The control plane runs against a Postgres 15+ database. In this environment it
is the docker container **`avivaods-postgres-1`**, exposed on TCP port **5440**
(container 5432 -> host 5440). Default connection:

```text
host=localhost   port=5440   dbname=ods_cp   user=ods   password=ods
```

Start the container if it is not already up:

```powershell
docker start avivaods-postgres-1
```

> **`docker exec` may be unavailable on this host.** Do **not** rely on
> `docker exec ... psql`. Everything below talks to Postgres over **TCP on
> `localhost:5440`** (psycopg / `python -m db.apply`), which is the supported
> path. The container only needs to be *running and port-mapped*.

Override the connection with environment variables if needed:

```powershell
$env:ODS_CP_HOST="localhost"
$env:ODS_CP_PORT="5440"
$env:ODS_CP_DB="ods_cp"
$env:ODS_CP_USER="ods"
$env:ODS_CP_PASSWORD="ods"
```

## Apply Migrations

`db/apply.py` connects over TCP and applies the ordered migrations in
`db/migrations/` (001 through 027). Because `docker exec` is unavailable, this is
**the** way to (re)create the schema:

```powershell
# Drop and recreate ods_cp from scratch, then apply 001-027.
python -m db.apply --drop
```

Run `python -m db.apply --drop` for a reliably-clean database before running the
full test suite or regenerating snapshots.

## Tests

```powershell
python -m pytest -q
```

Expected: **341 passed, 2 skipped, 1 xfailed**. Run `python -m db.apply --drop`
first for a deterministic, clean database.

Useful focused suites:

```powershell
python -m pytest tests\test_customer_transaction_workflow.py -q
python -m pytest tests\test_dlq_lifecycle.py tests\test_diagnostics.py -q
python -m pytest tests\test_refeed_policy.py tests\test_target_visibility.py -q
```

---

## Demos

Each demo is a self-contained, domain-scoped workflow harness. Running a harness
**resets only its own domain's** generated rows (so all three demos can coexist
in one database), drives the full control-plane write contract through the
`control/` wrappers, and writes a dashboard snapshot to the `--out` path.

| Demo | Command | Snapshot | Story |
|---|---|---|---|
| Customer / Transaction | `python -m harness.customer_transaction_workflow --out dashboard/data/demo-workflow.json` | `demo-workflow.json` | 3 normal days + a Day-2 transaction **refeed** (changed-only) |
| Insurance Policy / Claims | `python -m harness.policy_claims_workflow --out dashboard/data/policy-claims-workflow.json` | `policy-claims-workflow.json` | Airflow-orchestrated runs + refeed; `orchestrator_*` identity recorded |
| Policy / Claims **DLQ** | `python -m harness.policy_claims_dlq_workflow --out dashboard/data/policy-claims-dlq-workflow.json` | `policy-claims-dlq-workflow.json` | schema-validation, **quarantine** a bad row, then **replay/resolve** it |

The `--out` path defaults to the snapshot in the table, so the bare command is
enough. Pass `--no-reset` to append instead of resetting the domain.

### Regenerate all dashboard snapshots

```powershell
python -m db.apply --drop
python -m harness.customer_transaction_workflow --out dashboard/data/demo-workflow.json
python -m harness.policy_claims_workflow         --out dashboard/data/policy-claims-workflow.json
python -m harness.policy_claims_dlq_workflow     --out dashboard/data/policy-claims-dlq-workflow.json
```

## Dashboard

The dashboard is a **static** React app (no build step, no server-side DB
queries). Serve the `dashboard/` folder and open `index.html`:

```powershell
python -m http.server 8099 --directory dashboard
```

```text
http://localhost:8099/index.html
```

It loads one of the three committed snapshots. Select a snapshot with the header
dropdown or the **`?data=`** query parameter:

```text
http://localhost:8099/index.html?data=demo-workflow.json
http://localhost:8099/index.html?data=policy-claims-workflow.json
http://localhost:8099/index.html?data=policy-claims-dlq-workflow.json
```

Tabs: **Workflow Overview** (runs grouped by `workflow_run_id` with counts),
**Target Row History** (business-key history, changed-only refeed), **Process
Model** (tasks, stages, input edges, output links — parallel branches), and
**Developer Model** (each card shows the `control/` API call a developer would
write, including `orchestrator={...}` for Airflow runs). An **OL** button emits
OpenLineage-style events (a derived view; Postgres remains source of truth).

---

## The Write Contract (how a workflow records itself)

Every workflow task follows one ordered write sequence. The authoritative
mapping of each step to the exact `control/` wrapper is
**`docs/reference/control-plane-write-contract.md`**. In brief:

```text
1. register file        control.runs.register_file(...)
2. start run            control.runs.start(...)
3. start stage          control.stages.start(...)
4. application work
5. finish stage         control.stages.finish(...)
6. write output+inputs  control.lineage.write_output_link(...)  (or write_output_then_rows)
7. stamp target rows    (included in write_output_then_rows when there is a sink)
8. reconcile            control.recon.*
9. activate visibility  control.visibility.activate(...)        (if business-visible)
10. finish run          control.runs.finalise(...)
```

`control/sdk.py` provides context managers (`run(...)`, `stage(...)`) that wrap
steps 2/3/5/10 and mark runs/stages failed on exception. See also
`docs/reference/refeed-replacement-policy.md` for how a refeed decides which
prior active output it replaces.

## Support & Diagnostics SQL Functions

All read-only. These power both the dashboard and a developer/support engineer
working directly against Postgres (over TCP — `docker exec` is unavailable).
Validated and round-tripped in `tests/` (migrations 022 and 027).

| Function | Purpose | Example |
|---|---|---|
| `cp.dashboard_workflows()` | One row per `workflow_run_id` with run/stage/output/input/visibility counts. | `SELECT * FROM cp.dashboard_workflows();` |
| `cp.dashboard_workflow_detail(workflow_run_id text)` | Full JSON for one workflow: its runs, stages, outputs, inputs, target visibility. | `SELECT cp.dashboard_workflow_detail('<wfid>');` |
| `cp.dashboard_output_trace(output_link_id uuid)` | Walk an output link back to its raw source file(s), one row per provenance hop. | `SELECT * FROM cp.dashboard_output_trace('<output_link_id>');` |
| `cp.dashboard_file_usage(file_id uuid)` | Every run/output/edge that consumed or produced from a raw file. | `SELECT * FROM cp.dashboard_file_usage('<file_id>');` |
| `cp.dashboard_target_row_trace(target_schema text, target_table text, row_id bigint)` | Resolve a target row to its output link, then walk provenance to raw. | `SELECT * FROM cp.dashboard_target_row_trace('ods','customer_transaction', 1);` |
| `cp.dashboard_airflow_lookup(dag_id text, dag_run_id text)` | Runs whose `orchestrator_*` identity matches an Airflow dag_run (`dag_id` may be NULL). | `SELECT * FROM cp.dashboard_airflow_lookup(NULL, '<dag_run_id>');` |
| `cp.developer_diagnostics(workflow_run_id text, target_table text DEFAULT NULL)` | Detect problems in a workflow: unfinished run/stage, missing stages, missing output links, bad target stamps, DLQ trace gaps, missing schema_version. | `SELECT * FROM cp.developer_diagnostics('<wfid>');` |

### Diagnose a workflow

`cp.developer_diagnostics('<workflow_run_id>')` returns
`(check_name, severity, object_type, object_id, message, details)` rows — one per
detected issue. Empty result = healthy. Optionally pass a target table to also
validate row stamping on it:

```sql
SELECT check_name, severity, message
FROM cp.developer_diagnostics('<workflow_run_id>', 'ods.customer_transaction');
```

### Trace a target row back to raw

```sql
-- hop / edge_type / output_link_id / ... / raw_s3_path, ordered curated -> raw
SELECT hop, edge_type, dataset, source_file_id, raw_s3_path
FROM cp.dashboard_target_row_trace('ods', 'customer_transaction', 1)
ORDER BY hop;
```

The last hop carries the non-NULL `source_file_id` / `raw_s3_path` — the
registered raw file the row ultimately came from.

> **Full support playbook:** `docs/reference/support-runbook.md` answers the five
> common support questions (given a `workflow_run_id`, an `output_link_id`, a
> target row, an Airflow `dag_run_id`, or a bad/DLQ row) with copy-paste queries.

---

## Repository Layout

```text
control/        thin Python wrappers + SDK over the cp.* write functions
control/queries/ reusable SQL (e.g. trace_row.sql — provenance walk to raw)
db/migrations/  001-027 ordered Postgres migrations (schema + functions)
db/apply.py     migration applier (TCP; python -m db.apply --drop)
harness/        the three demo workflows + shared snapshot exporter
dashboard/      static React dashboard + committed snapshots in dashboard/data/
tests/          pytest suite (run python -m db.apply --drop first)
docs/           specs, reviews, and reference docs (below)
```

## Documentation

- `docs/reference/control-plane-write-contract.md` — the authoritative 10-step
  write contract and its exact wrapper mapping.
- `docs/reference/support-runbook.md` — the support runbook (five questions).
- `docs/reference/refeed-replacement-policy.md` — refeed/replay replacement
  policy (business-truth vs immutable lineage).
- `docs/reference/control-plane-cookbook/` — task-oriented how-tos.
- `docs/reference/control-plane-architecture-review.md` — architecture review.
- `docs/specs/2026-06-03-working-platform-completion-plan.md` — the completion
  plan / definition of done this repo implements.

---

## Design / History (secondary)

The original design framing — `workflow_run_id` groups one execution for
investigation; `output_link` + `input_edge` form the lineage graph across files,
tasks, days, and refeeds; the platform models the metadata layer that a real
Spark/Glue/Airflow estate would call into — remains accurate and is preserved as
the platform's core narrative above. The product naming (vs the physical column
names) is:

| Product name | Meaning | Physical compatibility |
|---|---|---|
| `output_link` | the output produced by a run | formerly `lineage_link` |
| `output_link_id` | id of the produced output | formerly `lineage_link_id` |
| `input_edge` | one input consumed by an output | formerly `lineage_edge` |
| `input_edge_id` | id of the relationship row | formerly `lineage_edge_id` |
| `upstream_output_link_id` | previous output used as input | formerly `upstream_lineage_link_id` |

This repository is not Spark, S3, Kafka, or a production scheduler. It is the
metadata/control-plane layer those systems would call. The full phase history and
design rationale live in `docs/specs/` and `docs/reference/`.
