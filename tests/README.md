# ODS Control-Plane Test Suite — coverage boundary

This suite proves the **control plane** (the `cp.*` Postgres functions + the
`control/` Python client) behaves correctly, and that the **harness** (`harness/`
fake Spark-free stages + composers) drives that client through the full
lineage / recon / DLQ / replay lifecycle.

## What the tests DO prove

- **Schema + contract** (`test_contract.py`): every `cp.*` table/function exists
  with the expected shape; column-drift guards.
- **Client round-trips** (`test_client.py`): every wrapper writes/reads the
  expected rows.
- **Lineage to raw** (`test_lineage_single/merge.py`, `test_sink_dlq_replay.py`):
  ingest → canonicalize → merge → sink links trace back to the raw file through
  `cp.v_provenance`; discovery (`latest_succeeded_run` / `succeeded_runs`) selects
  the right upstream; replay re-writes its own chain (X5).
- **Regression matrix** (P4):
  - `test_recon.py` — recon `ok` / `breach` / `double_count` regimes.
  - `test_constraints.py` — EVERY foreign key, the `edge_type` FK, and the
    `workflow_run_id` NOT NULL are rejected for dangling/invalid references.
  - `test_idempotency.py` — re-running the SAME replay correction is a no-op
    (stable link/edge/row counts; original recon unchanged).
  - `test_concurrency.py` — two concurrent writers under one `workflow_run_id`
    both persist, no lost/duplicate links, no deadlock.
  - `test_trigger_edge.py` — an `orchestrates` trigger edge exists in
    `cp.lineage_edge` but is EXCLUDED from `cp.v_provenance` (never pollutes
    trace-to-raw).
  - `test_transform_dq.py` — a transform DQ failure (cast-to-NULL) is
    QUARANTINED (visible in the graph), never silently written as NULL; recon
    stays balanced (good + dlq == source).
  - `test_discovery_determinism.py` — the `007` `clock_timestamp()` fix makes
    discovery deterministic even within a single transaction.

## What the fakes / this suite CANNOT prove

The harness exercises the **client** with synthetic data. It deliberately does
**not** run Spark. Therefore this suite **cannot** prove:

1. **That the real Spark jobs call the client correctly** — i.e. that the
   production ingest / canonicalize / merge / sink jobs invoke the `control/`
   wrappers in the right **sequence** with the right **arguments**. The fakes
   *are* a correct caller by construction; a real job could call the client in
   the wrong order (e.g. write target rows before the lineage link) and the
   harness would never notice, because the harness *is not the real job*.

2. **Real data transformation correctness** — the fakes pass synthetic row
   counts and fake hashes; no actual parsing, casting, dedup, or merge logic
   runs. Whether a real transform produces the right rows is out of scope.

3. **Real I/O / object-store / Spark-cluster behaviour** — no S3, no parquet, no
   cluster scheduling is touched.

### How the boundary is partially closed

- `test_adapter_contract.py` closes part of gap (1): it is a **mock-client**
  test pinning the one ordering contract a real sink job MUST follow —
  `write_link` / `write_link_then_rows` is called **before** any target-row
  write ("Postgres write is last"). This is the single place mocks are correct,
  because the property under test is a **call sequence**, not DB behaviour. A
  real Spark adapter is conformant iff it passes this same assertion.

- **Fully** closing gap (1) — plus gaps (2) and (3) — requires a later
  **integration test** that runs the actual Spark jobs against a real (or
  containerised) stack and asserts the same lineage / recon invariants this
  suite asserts against the fakes. That integration test is out of scope for the
  control-plane build; this README marks the boundary.
