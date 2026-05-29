# ODS Control Plane Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the Spark-free ODS control plane (Postgres `cp.*` schema + functions, Python client, fake-stage harness, full test matrix) so any target row traces to raw and any incident is reconstructable.

**Architecture:** Postgres-only lineage. `workflow_run_id` (TEXT) groups one execution; `lineage_edge` carries provenance and spans runs. A link + all its edges are written in one transaction; target rows are stamped only via a sanctioned sink primitive that writes the link first. Stages discover upstream via `latest_succeeded_run` — never trust a passed id. Built component-by-component, each hop gated by an evidence-based lineage sign-off before the next.

**Tech Stack:** Postgres 15+ (db `ods_cp`, schema `cp`, port 5440, in `avivaods-postgres-1`), plpgsql, Python 3.11 + psycopg, pytest.

**Authoritative spec:** `docs/specs/2026-05-29-control-plane-design-v2.md`. **Review of record:** `docs/reviews/2026-05-29-design-review-consolidated.md`.

---

## Build sequence & gates (read first)

Sequential — components are a dependency chain. Each phase has a hard exit gate; do not start the next until the gate passes with evidence (command output pasted, not asserted).

| Phase | Component | Owner agent | Exit gate (evidence required) |
|-------|-----------|-------------|-------------------------------|
| P1 | Migrations + `cp.*` functions + contract test | `database-optimizer` | Contract test green; every `pg_proc` fn in schema `cp` asserted; returned columns match tables |
| P2 | Python client `control/` | `senior-developer` | Per-fn round-trip unit tests green; `workflow_run_id` minted once by composer |
| P3a | Harness ingest hop (`raw_to_curated`) | general + TDD | **LINEAGE GATE A** |
| P3b | Harness canonicalize hop | general + TDD | **LINEAGE GATE B** |
| P3c | Harness merge hop (N-edge) | general + TDD | **LINEAGE GATE C** |
| P3d | Harness sink hop (`canonical_to_sink`) — **postgres write LAST** | general + TDD | **LINEAGE GATE D** |
| P4 | Full regression matrix + concurrency + idempotency | `code-reviewer` review | All spec tests green; `security-engineer` pass |

**Between every phase:** `code-reviewer` on the diff; `security-engineer` on anything touching SQL string-building.

### LINEAGE GATE — falsifiable checklist (run by a dedicated lineage-reviewer agent, evidence-based; Reality-Checker posture: "NEEDS WORK" until query output proves otherwise)

A hop passes ONLY when the reviewer pastes query output proving ALL of:
1. **Trace-to-raw total:** for *every* target/curated row produced this hop, `cp.v_provenance` returns a `raw_to_curated` ancestor. Zero orphans. (`SELECT count(*) FROM <rows> r WHERE NOT EXISTS (provenance-to-raw)` = 0.)
2. **No empty links:** `SELECT count(*) FROM cp.lineage_link l WHERE NOT EXISTS (SELECT 1 FROM cp.lineage_edge e WHERE e.lineage_link_id = l.lineage_link_id)` = 0.
3. **Provenance excludes triggers:** `v_provenance` rows all have `is_provenance = true`; no `orchestrates` edge appears in a trace.
4. **One-query debug:** given one target row's `_ods_lineage_link_id`, a single query reconstructs the full chain file→ingest→canon→(merge)→sink. This query is a **deliverable** committed at `control/queries/trace_row.sql`.
5. **(3c only)** merge: distinct `input_slot`, distinct non-null `upstream_run_id`, `SUM(edge.record_count) == link.record_count`.
6. **(3d only)** replay traces to raw; DLQ rows appear in graph via `quarantine` edge; fan-out: two `canonical_to_sink` links, `child.record_count == parent.record_count` per `sink_type`.

---

## File Structure

```
db/
  migrations/
    001_schema.sql        # cp.* tables + cp.edge_type seed + cp.v_provenance
    002_functions.sql     # all cp.* plpgsql functions (signature appendix)
    003_targets.sql       # sample ods.<dataset> target table
  apply.sh                # ordered psql runner -> ods_cp
control/
  __init__.py
  db.py                   # connection factory (host/port/db/user from env)
  runs.py                 # start / patch / finalise / latest_succeeded_run
  lineage.py              # write_link / write_link_then_rows
  stages.py               # stage_scope context manager
  recon.py                # write_check
  dlq.py                  # quarantine / replay
  queries/trace_row.sql   # one-query debug deliverable (Gate item 4)
harness/
  fakes.py                # fake_ingest / fake_canonicalize / fake_merge / fake_sink / fake_fail
  composers.py            # run_single_file / run_multi_file (mint ONE workflow_run_id)
tests/
  conftest.py             # transactional-rollback fixture per test
  test_contract.py        # P1 exhaustive contract test
  test_client.py          # P2 per-fn round-trip
  test_lineage_single.py  # P3a/b
  test_lineage_merge.py   # P3c
  test_sink_dlq_replay.py # P3d
  test_recon.py           # P4 negatives + fan-out
  test_concurrency.py     # P4
  README.md               # coverage boundary: "what fakes cannot prove"
```

---

## PHASE 1 — Migrations + functions + contract test

**Owner:** `database-optimizer`. Foundation; nothing else builds until the contract test is green.

### Task 1: Repo + DB scaffolding

**Files:**
- Create: `db/apply.sh`, `tests/conftest.py`, `control/db.py`

- [ ] **Step 1: Confirm the isolated DB exists**

Run:
```bash
docker exec -i avivaods-postgres-1 psql -U ods -d postgres -c "SELECT 1 FROM pg_database WHERE datname='ods_cp';"
```
Expected: one row. If empty:
```bash
docker exec -i avivaods-postgres-1 psql -U ods -d postgres -c "CREATE DATABASE ods_cp;"
```

- [ ] **Step 2: Write the migration runner**

`db/apply.sh`:
```bash
#!/usr/bin/env bash
set -euo pipefail
for f in "$(dirname "$0")"/migrations/[0-9]*.sql; do
  echo ">> applying $f"
  docker exec -i avivaods-postgres-1 psql -U ods -d ods_cp -v ON_ERROR_STOP=1 -f - < "$f"
done
echo ">> migrations applied"
```

- [ ] **Step 3: Write the connection factory**

`control/db.py`:
```python
import os
import psycopg

def connect():
    return psycopg.connect(
        host=os.getenv("ODS_CP_HOST", "localhost"),
        port=int(os.getenv("ODS_CP_PORT", "5440")),
        dbname=os.getenv("ODS_CP_DB", "ods_cp"),
        user=os.getenv("ODS_CP_USER", "ods"),
        password=os.getenv("ODS_CP_PASSWORD", "ods"),
    )
```

- [ ] **Step 4: Write the per-test rollback fixture**

`tests/conftest.py`:
```python
import pytest
from control.db import connect

@pytest.fixture
def conn():
    c = connect()
    c.autocommit = False
    try:
        yield c
    finally:
        c.rollback()   # every test isolated; no committed state leaks
        c.close()
```

- [ ] **Step 5: Commit**

```bash
git add db/apply.sh control/db.py tests/conftest.py
git commit -m "chore: db scaffolding — migration runner, conn factory, rollback fixture"
```

### Task 2: Schema migration (`001_schema.sql`)

**Files:**
- Create: `db/migrations/001_schema.sql`
- Test: `tests/test_contract.py` (schema-presence assertions added here)

- [ ] **Step 1: Write the failing schema-presence test**

`tests/test_contract.py`:
```python
EXPECTED_TABLES = {
    "edge_type", "dataset_config", "file_catalogue", "run_log",
    "run_stage_log", "lineage_link", "lineage_edge",
    "reconciliation_log", "dlq",
}

def test_all_cp_tables_exist(conn):
    rows = conn.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema='cp'"
    ).fetchall()
    present = {r[0] for r in rows}
    assert EXPECTED_TABLES <= present, EXPECTED_TABLES - present
```

- [ ] **Step 2: Run it — verify it fails**

Run: `pytest tests/test_contract.py::test_all_cp_tables_exist -v`
Expected: FAIL (schema `cp` empty / tables missing).

- [ ] **Step 3: Write `001_schema.sql`**

```sql
CREATE SCHEMA IF NOT EXISTS cp;
CREATE EXTENSION IF NOT EXISTS pgcrypto;   -- gen_random_uuid()

CREATE TABLE cp.edge_type (
    edge_type     TEXT PRIMARY KEY,
    is_provenance BOOLEAN NOT NULL
);
INSERT INTO cp.edge_type(edge_type, is_provenance) VALUES
    ('raw_to_curated', true), ('curated_to_canonical', true),
    ('merge_to_canonical', true), ('canonical_to_sink', true),
    ('quarantine', true), ('replay', true), ('orchestrates', false);

CREATE TABLE cp.dataset_config (
    domain      TEXT NOT NULL, dataset TEXT NOT NULL,
    key_fields  JSONB NOT NULL, write_mode TEXT NOT NULL,
    sink_type   TEXT, sink_config JSONB, dq_rules JSONB,
    UNIQUE (domain, dataset)
);

CREATE TABLE cp.file_catalogue (
    file_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    s3_raw_path   TEXT NOT NULL, file_md5 TEXT NOT NULL,
    business_date DATE NOT NULL, state TEXT NOT NULL DEFAULT 'registered',
    domain TEXT, dataset TEXT,
    UNIQUE (file_md5, business_date)
);

CREATE TABLE cp.run_log (
    run_id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_run_id  TEXT NOT NULL,                 -- Airflow dag_run_id OR stringified uuid4
    trigger_type     TEXT NOT NULL,                 -- airflow|manual|replay|dlq_drain
    replay_of_run_id UUID REFERENCES cp.run_log(run_id),
    pipeline_type    TEXT NOT NULL,
    domain           TEXT NOT NULL, dataset TEXT NOT NULL,
    business_date    DATE NOT NULL,
    file_id          UUID REFERENCES cp.file_catalogue(file_id),
    status           TEXT NOT NULL DEFAULT 'running',
    record_count_in  BIGINT, record_count_out BIGINT,
    error            TEXT,
    started_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at      TIMESTAMPTZ
);

CREATE TABLE cp.run_stage_log (
    stage_log_id    BIGSERIAL PRIMARY KEY,
    run_id          UUID NOT NULL REFERENCES cp.run_log(run_id),
    stage           TEXT NOT NULL, attempt INT NOT NULL DEFAULT 1,
    status          TEXT NOT NULL,
    record_count_in BIGINT, record_count_out BIGINT, metrics JSONB,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ
);

CREATE TABLE cp.lineage_link (
    lineage_link_id   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    consumer_run_id   UUID NOT NULL REFERENCES cp.run_log(run_id),
    edge_type         TEXT NOT NULL REFERENCES cp.edge_type(edge_type),
    sink_type         TEXT,                          -- non-null iff edge_type='canonical_to_sink'
    target_ref        JSONB NOT NULL,                -- {path, content_hash, version}
    transform_version TEXT,
    record_count      BIGINT NOT NULL,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT sink_type_iff_sink CHECK (
        (edge_type = 'canonical_to_sink') = (sink_type IS NOT NULL)
    )
);
-- Expression uniqueness must be a UNIQUE INDEX (Postgres forbids expressions in a
-- table-level UNIQUE constraint). write_lineage_link's ON CONFLICT infers against this.
CREATE UNIQUE INDEX uq_lineage_link_target
    ON cp.lineage_link (consumer_run_id, edge_type, (target_ref->>'content_hash'));

CREATE TABLE cp.lineage_edge (
    lineage_edge_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    lineage_link_id UUID NOT NULL REFERENCES cp.lineage_link(lineage_link_id),
    upstream_run_id UUID REFERENCES cp.run_log(run_id),
    source_file_id  UUID REFERENCES cp.file_catalogue(file_id),
    input_slot      INT NOT NULL DEFAULT 0,
    edge_type       TEXT NOT NULL REFERENCES cp.edge_type(edge_type),
    source_ref      JSONB,
    record_count    BIGINT NOT NULL
);

CREATE TABLE cp.reconciliation_log (
    recon_id        BIGSERIAL PRIMARY KEY,
    run_id          UUID NOT NULL REFERENCES cp.run_log(run_id),
    check_type      TEXT NOT NULL,
    source_count    BIGINT NOT NULL, accounted_count BIGINT NOT NULL,
    discrepancy     BIGINT NOT NULL, status TEXT NOT NULL,
    metrics         JSONB, created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE cp.dlq (
    dlq_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id        UUID NOT NULL REFERENCES cp.run_log(run_id),
    stage         TEXT NOT NULL, reason TEXT NOT NULL,
    source_ref    JSONB, payload_ref TEXT, record_count BIGINT NOT NULL,
    replayed_at   TIMESTAMPTZ, replay_run_id UUID REFERENCES cp.run_log(run_id),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Provenance walk: edges only, provenance edge_types only (excludes 'orchestrates').
CREATE VIEW cp.v_provenance AS
WITH RECURSIVE walk AS (
    SELECT l.lineage_link_id, e.lineage_edge_id, e.upstream_run_id,
           e.source_file_id, e.edge_type, l.consumer_run_id
    FROM cp.lineage_link l
    JOIN cp.lineage_edge e ON e.lineage_link_id = l.lineage_link_id
    JOIN cp.edge_type t    ON t.edge_type = e.edge_type AND t.is_provenance
  UNION ALL
    SELECT pl.lineage_link_id, pe.lineage_edge_id, pe.upstream_run_id,
           pe.source_file_id, pe.edge_type, pl.consumer_run_id
    FROM walk w
    JOIN cp.lineage_link pl ON pl.consumer_run_id = w.upstream_run_id
    JOIN cp.lineage_edge pe ON pe.lineage_link_id = pl.lineage_link_id
    JOIN cp.edge_type t     ON t.edge_type = pe.edge_type AND t.is_provenance
)
SELECT * FROM walk;
```

- [ ] **Step 4: Apply and re-run the test**

Run: `bash db/apply.sh && pytest tests/test_contract.py::test_all_cp_tables_exist -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add db/migrations/001_schema.sql tests/test_contract.py
git commit -m "feat(db): cp schema, edge_type lookup, v_provenance view"
```

### Task 3: Functions migration (`002_functions.sql`) — TDD per function

For **each** function in the spec appendix (`start_run`, `patch_run`, `register_file`, `start_stage`, `finish_stage`, `write_lineage_link`, `write_link_then_rows`, `write_reconciliation_check`, `quarantine`, `latest_succeeded_run`) repeat the red→green→commit cycle. The atomic `write_lineage_link` is shown in full as the reference pattern; the rest follow the same structure from the appendix signatures.

- [ ] **Step 1: Write the failing test for `write_lineage_link` atomicity + empty-edge reject**

Add to `tests/test_contract.py`:
```python
import json, uuid

def _mk_run(conn):
    return conn.execute(
        "SELECT cp.start_run(%s,'ingestion','sales','orders',%s,'manual')",
        [str(uuid.uuid4()), "2026-05-29"],
    ).fetchone()[0]

def test_write_lineage_link_rejects_empty_edges(conn):
    run = _mk_run(conn)
    with pytest.raises(Exception):
        conn.execute(
            "SELECT cp.write_lineage_link(%s,'raw_to_curated',%s,10,%s)",
            [run, json.dumps({"path":"s3://x","content_hash":"h1","version":1}), json.dumps([])],
        )

def test_write_lineage_link_writes_link_and_edges_atomically(conn):
    run = _mk_run(conn)
    edges = [{"upstream_run_id":None,"source_file_id":None,"input_slot":0,
              "edge_type":"raw_to_curated","source_ref":{"p":"s3://raw"},"record_count":10}]
    link = conn.execute(
        "SELECT cp.write_lineage_link(%s,'raw_to_curated',%s,10,%s)",
        [run, json.dumps({"path":"s3://x","content_hash":"h1","version":1}), json.dumps(edges)],
    ).fetchone()[0]
    n = conn.execute("SELECT count(*) FROM cp.lineage_edge WHERE lineage_link_id=%s",[link]).fetchone()[0]
    assert n == 1
```

- [ ] **Step 2: Run — verify fail** (`function cp.write_lineage_link does not exist`).

- [ ] **Step 3: Write the function in `002_functions.sql`** (reference pattern; write the other 9 from their appendix signatures in the same file before applying)

```sql
CREATE OR REPLACE FUNCTION cp.write_lineage_link(
    p_consumer_run_id   uuid,
    p_edge_type         text,
    p_target_ref        jsonb,
    p_record_count      bigint,
    p_edges             jsonb,
    p_sink_type         text DEFAULT NULL,
    p_transform_version text DEFAULT NULL
) RETURNS uuid
LANGUAGE plpgsql AS $$
DECLARE
    v_link uuid;
    v_edge jsonb;
BEGIN
    IF jsonb_array_length(coalesce(p_edges,'[]'::jsonb)) = 0 THEN
        RAISE EXCEPTION 'write_lineage_link: at least one edge required (link %, type %)',
            p_consumer_run_id, p_edge_type;
    END IF;

    INSERT INTO cp.lineage_link
        (consumer_run_id, edge_type, sink_type, target_ref, transform_version, record_count)
    VALUES
        (p_consumer_run_id, p_edge_type, p_sink_type, p_target_ref, p_transform_version, p_record_count)
    ON CONFLICT (consumer_run_id, edge_type, (target_ref->>'content_hash')) DO NOTHING
    RETURNING lineage_link_id INTO v_link;

    IF v_link IS NULL THEN          -- idempotent replay: link already exists, reuse it
        SELECT lineage_link_id INTO v_link FROM cp.lineage_link
        WHERE consumer_run_id = p_consumer_run_id AND edge_type = p_edge_type
          AND target_ref->>'content_hash' = p_target_ref->>'content_hash';
        RETURN v_link;              -- edges already written under it
    END IF;

    FOR v_edge IN SELECT * FROM jsonb_array_elements(p_edges) LOOP
        INSERT INTO cp.lineage_edge
            (lineage_link_id, upstream_run_id, source_file_id, input_slot,
             edge_type, source_ref, record_count)
        VALUES
            (v_link,
             nullif(v_edge->>'upstream_run_id','')::uuid,
             nullif(v_edge->>'source_file_id','')::uuid,
             coalesce((v_edge->>'input_slot')::int, 0),
             coalesce(v_edge->>'edge_type', p_edge_type),
             v_edge->'source_ref',
             (v_edge->>'record_count')::bigint);
    END LOOP;
    RETURN v_link;   -- link + N edges in ONE transaction (X1)
END $$;
```
*(Write `start_run`, `patch_run`, `register_file`, `start_stage`, `finish_stage`, `write_link_then_rows`, `write_reconciliation_check`, `quarantine`, `latest_succeeded_run` per the v2 appendix signatures in this same file. `latest_succeeded_run` tie-break: `ORDER BY finished_at DESC, run_id DESC LIMIT 1`. `quarantine` also inserts a `quarantine` edge. `start_run` with `trigger_type='orchestrates'` also writes an `is_provenance=false` trigger edge.)*

- [ ] **Step 4: Apply and re-run** — `bash db/apply.sh && pytest tests/test_contract.py -v` → PASS.

- [ ] **Step 5: Commit** — `git commit -m "feat(db): cp.* functions — atomic write_lineage_link + appendix"`.

### Task 4: Exhaustive contract test (the H-contract gate)

**Files:** Modify `tests/test_contract.py`

- [ ] **Step 1: Write the failing exhaustiveness test**

```python
def test_every_cp_function_is_asserted(conn):
    fns = {r[0] for r in conn.execute(
        "SELECT proname FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace "
        "WHERE n.nspname='cp'").fetchall()}
    # ASSERTED is maintained by hand as each fn gets a round-trip test below.
    ASSERTED = {
        "start_run","patch_run","register_file","start_stage","finish_stage",
        "write_lineage_link","write_link_then_rows","write_reconciliation_check",
        "quarantine","latest_succeeded_run",
    }
    missing = fns - ASSERTED
    assert not missing, f"cp functions with no contract assertion: {missing}"
```

- [ ] **Step 2: Run** — passes once all 10 exist and are listed; fails the day someone adds an unasserted `cp.*` fn (catches the old `run_events` drift class).

- [ ] **Step 3: Add a returned-column drift check** — for `start_run`/`write_lineage_link`, assert the returned id resolves to a row whose columns match the table definition (query `information_schema.columns` vs the insert).

- [ ] **Step 4: Commit** — `git commit -m "test(db): exhaustive cp.* contract + column-drift guard"`.

> **P1 EXIT GATE:** `pytest tests/test_contract.py -v` all green; `test_every_cp_function_is_asserted` passes. `database-optimizer` confirms indexes on FK columns + `(domain,dataset,business_date,pipeline_type,status)` for `latest_succeeded_run`. `code-reviewer` on the diff. Then P2.

---

## PHASE 2 — Python client (`control/`)

**Owner:** `senior-developer`. Thin typed wrappers, per-write commit (`commit=True` default). Composer mints ONE `workflow_run_id` (`str(uuid4())`) and passes it as a **required arg** to every stage.

Per-module TDD (red→green→commit), each a round-trip test through the live DB:
- [ ] `runs.start / patch / finalise / latest_succeeded_run` — assert `latest_succeeded_run` returns the newest succeeded run and **ignores** a bogus passed id (discovery).
- [ ] `lineage.write_link` — round-trips link + edges; `lineage.write_link_then_rows` — stamps `_ods_lineage_link_id` and proves no row exists before the link commit (two-commit ordering test).
- [ ] `stages.stage_scope` context manager — start on enter, finish on exit, finish with `failed` on exception.
- [ ] `recon.write_check` — computes discrepancy/status.
- [ ] `dlq.quarantine / replay` — replay mints new `workflow_run_id`, sets `trigger_type='replay'`, `replay_of_run_id`.

> **P2 EXIT GATE:** `pytest tests/test_client.py -v` green; ordering test proves link-before-rows; `latest_succeeded_run` discovery test green. `code-reviewer` + `security-engineer` (no f-string SQL; params only). Then P3a.

*(Full bite-sized TDD steps with complete code for each module are authored at the start of P2, following the Task-3 pattern — held until P1's applied schema is the source of truth, to avoid signature drift.)*

---

## PHASE 3 — Harness, hop by hop (each lineage-gated)

**Owner:** general + TDD. Honour the **harness MUST-NOT rules** (v2 spec): no hand-built edges/ids; upstream only via `latest_succeeded_run`; one `workflow_run_id` from the composer; no row insert before link commit.

- [ ] **P3a ingest** `fake_ingest(file, workflow_run_id)` → register_file, start_run(ingestion), stage_scope, `write_link raw_to_curated`, recon, finalise. **→ LINEAGE GATE A** (checklist items 1–4).
- [ ] **P3b canonicalize** `fake_canonicalize(..., workflow_run_id)` → discovers ingest via `latest_succeeded_run`, `write_link curated_to_canonical` with `transform_version`. **→ LINEAGE GATE B**. Add the re-run test: bogus upstream id still links via discovery.
- [ ] **P3c merge** `fake_merge(slots, workflow_run_id)` → ONE link, N edges, distinct `input_slot`, per-slot counts in recon `metrics`. **→ LINEAGE GATE C** (items 1–5).
- [ ] **P3d sink — postgres write LAST** `fake_sink(..., workflow_run_id)` → `write_link_then_rows canonical_to_sink` with `sink_type`. `fake_fail` → `dlq.quarantine`; `replay` re-writes full provenance chain + `replay` edge. **→ LINEAGE GATE D** (items 1–4 + 6: replay-to-raw, DLQ-in-graph, fan-out two sink links).

Composers `run_single_file` / `run_multi_file` mint one `workflow_run_id` and thread it. Write `tests/README.md` coverage boundary ("what fakes cannot prove") + the **adapter-contract test** (mock client, assert real-job call sequence: `write_link` before any row-write).

> **By P3d, lineage is already signed off on every upstream hop (A–C) before the first postgres write — your invariant, enforced by the gate order.**

---

## PHASE 4 — Regression matrix, concurrency, idempotency

**Owner:** build + `code-reviewer` + `security-engineer`. Add the remaining spec tests:
- [ ] recon negatives: `good+dlq<source`→breach; `>source`→double-count; "every link ≥1 edge"; fan-out per-sink count equality.
- [ ] FK rejection on **every** FK (consumer/upstream/source_file/`_ods_lineage_link_id`/`dlq.run_id`/`dlq.replay_run_id`).
- [ ] idempotent replay/refeed: new `workflow_run_id` per refeed; replay twice → stable counts; original recon unchanged.
- [ ] **restart-task (Airflow clear-task): same `workflow_run_id`, unchanged input → re-run stage → exactly 1 link (dedup on `(consumer_run_id, edge_type, content_hash)`), counts stable, recon unchanged.**
- [ ] **restart-task with changed upstream content under same `workflow_run_id` → new link (content_hash differs); assert prior link NOT auto-superseded and recon flags the orphan unless downstream also re-run.**
- [ ] concurrency: two writers under one `workflow_run_id` → no lost/dup links.
- [ ] NOT-NULL reject for `workflow_run_id`; bad `edge_type` rejected by FK.
- [ ] transform cast-to-NULL treated as a `quarantine` event, not silent mutation.

> **P4 EXIT GATE:** full `pytest -v` green; lineage-reviewer signs the final trace-to-raw across single + multi-file + replay; `security-engineer` clean. Done.

---

## Self-review notes (author check, done)

- **Spec coverage:** every CRITICAL/HIGH maps to a task — X1→Task 3 + P2 ordering + P3d; X2→Task 3+4; X3→schema `trigger_type`/`replay_of_run_id` (Task 2) + P2; X4→P3 MUST-NOT + adapter-contract; X5→P3d replay; H-edge→`edge_type` table + `sink_type` (Task 2) + P3d fan-out; H-dlq→`quarantine` edge (Task 3 `quarantine` fn + P3d); H-recon→P4; H-trigger→`v_provenance` + `orchestrates` (Task 2/3); H-discovery→`latest_succeeded_run` (Task 3) + P2/P3b; H-contract→Task 4; H-fk→P4.
- **Decisions baked:** TEXT `workflow_run_id`, `canonical_to_sink`+`sink_type`, `quarantine` provenance edge, `target_ref` hash/version — all in `001_schema.sql`.
- **Type consistency:** `write_lineage_link` / `write_link_then_rows` / `latest_succeeded_run` names identical across plan and spec appendix.
- **Known expansion:** P2–P4 carry task lists + gates + representative code, not full per-step code; expanded to Task-3-style bite-sized TDD at each phase start against the *applied* schema to prevent signature drift. P1 is fully bite-sized and immediately executable.
