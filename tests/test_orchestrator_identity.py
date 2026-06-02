"""Orchestrator-identity foundation (migration 020 + 021).

Spec: docs/specs/2026-06-02-airflow-orchestrator-policy-claims-workflow.md
      ("Migration 020", "cp.start_run Change", "Python API Change", "Tests").

What this proves:
  1. runs.start(...) with NO orchestrator still works; orchestrator_payload
     defaults to {} and the 7 scalar orchestrator_* columns are NULL.
  2. runs.start(..., trigger_type='airflow', orchestrator={full airflow dict})
     writes all 8 orchestrator columns correctly (the whole dict is preserved
     in orchestrator_payload).
  3. Restart: two start() calls for the SAME logical identity (same
     workflow_run_id + pipeline_type + domain + dataset + business_date +
     file_id, pipeline_type != 'sink') reuse the SAME run_id; the second call's
     try_number/url REFRESH the row and status is reset to 'running'.
  4. detail_to_aggregate (migration 021) is a registered is_provenance
     edge_type, and the upstream_link_required_for_run_edges CHECK now also
     governs it: an edge WITHOUT upstream_lineage_link_id is REJECTED, WITH it
     (under a detail_to_aggregate link) is ACCEPTED.
"""
import json
from uuid import uuid4

import pytest
import psycopg

from control import runs

BD = "2026-05-29"


def _airflow_orchestrator(try_number=1, map_index=-1, run_id="manual__2026-05-29T00:00:00"):
    """A full Airflow orchestrator context dict as the DAG helper would build it."""
    return {
        "type": "airflow",
        "dag_id": "ods_customer_transaction",
        "run_id": run_id,
        "task_id": "canonicalize",
        "try_number": try_number,
        "map_index": map_index,
        "url": f"http://airflow/log?try={try_number}",
        "payload": {
            "execution_date": "2026-05-29T00:00:00+00:00",
            "operator": "PythonOperator",
        },
    }


def _cols(conn, run_id):
    return conn.execute(
        "SELECT orchestrator_type, orchestrator_dag_id, orchestrator_run_id, "
        "orchestrator_task_id, orchestrator_try_number, orchestrator_map_index, "
        "orchestrator_url, orchestrator_payload, status "
        "FROM cp.run_log WHERE run_id=%s",
        (run_id,),
    ).fetchone()


def test_start_no_orchestrator_defaults(conn):
    """No orchestrator: payload defaults to {}, the 7 scalar cols are NULL."""
    run_id = runs.start(
        conn, workflow_run_id=str(uuid4()), pipeline_type="ingestion",
        domain="sales", dataset="orders", business_date=BD,
        trigger_type="manual", commit=False,
    )
    (otype, dag, orid, task, tryn, mapi, url, payload, status) = _cols(conn, run_id)
    assert (otype, dag, orid, task, tryn, mapi, url) == (None,) * 7
    assert payload == {}
    assert status == "running"


def test_start_with_airflow_orchestrator_writes_all_columns(conn):
    """A full airflow dict populates all 8 orchestrator columns; payload whole."""
    orch = _airflow_orchestrator(try_number=1, map_index=-1)
    run_id = runs.start(
        conn, workflow_run_id=str(uuid4()), pipeline_type="canonicalize",
        domain="sales", dataset="orders", business_date=BD,
        trigger_type="airflow", orchestrator=orch, commit=False,
    )
    (otype, dag, orid, task, tryn, mapi, url, payload, _status) = _cols(conn, run_id)
    assert otype == "airflow"
    assert dag == "ods_customer_transaction"
    assert orid == orch["run_id"]
    assert task == "canonicalize"
    assert tryn == 1
    assert mapi == -1
    assert url == "http://airflow/log?try=1"
    # the WHOLE object is preserved (so future fields are not lost).
    assert payload == orch


def test_restart_reuses_run_and_refreshes_orchestrator(conn):
    """Airflow clear-task retry: SAME logical identity (non-sink) -> SAME run_id;
    try_number 1 -> 2 refreshed, status back to 'running'."""
    wfid = str(uuid4())
    fid = conn.execute(
        "SELECT cp.register_file(%s,%s,%s,'sales','orders')",
        (f"s3://raw/{uuid4()}.csv", uuid4().hex, BD),
    ).fetchone()[0]

    def _call(try_number):
        return runs.start(
            conn, workflow_run_id=wfid, pipeline_type="ingestion",
            domain="sales", dataset="orders", business_date=BD,
            trigger_type="airflow", file_id=fid,
            orchestrator=_airflow_orchestrator(try_number=try_number),
            commit=False,
        )

    run1 = _call(1)
    # first attempt finished, then a clear-task re-runs it (try 2)
    conn.execute("SELECT cp.patch_run(%s,%s)", (run1, json.dumps({"status": "succeeded"})))
    run2 = _call(2)

    assert run2 == run1, "restart must reuse the same run_id"
    (_otype, _dag, _orid, _task, tryn, _mapi, _url, _payload, status) = _cols(conn, run2)
    assert tryn == 2, "orchestrator_try_number must refresh on restart"
    assert status == "running", "restart resets status to running"
    # exactly one run_log row for this logical identity
    n = conn.execute(
        "SELECT count(*) FROM cp.run_log WHERE workflow_run_id=%s AND pipeline_type='ingestion'",
        (wfid,),
    ).fetchone()[0]
    assert n == 1


def test_run_log_exposes_orchestrator_columns(conn):
    """Round-trip: the orchestrator columns are queryable on cp.run_log."""
    orch = _airflow_orchestrator(try_number=3, map_index=5)
    run_id = runs.start(
        conn, workflow_run_id=str(uuid4()), pipeline_type="merge",
        domain="sales", dataset="orders", business_date=BD,
        trigger_type="airflow", orchestrator=orch, commit=False,
    )
    cols = {r[0] for r in conn.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema='cp' AND table_name='run_log'").fetchall()}
    expected = {
        "orchestrator_type", "orchestrator_dag_id", "orchestrator_run_id",
        "orchestrator_task_id", "orchestrator_try_number",
        "orchestrator_map_index", "orchestrator_url", "orchestrator_payload",
    }
    assert expected <= cols, f"run_log missing orchestrator columns: {expected - cols}"
    row = conn.execute(
        "SELECT orchestrator_try_number, orchestrator_map_index, "
        "orchestrator_payload->'payload'->>'operator' "
        "FROM cp.run_log WHERE run_id=%s", (run_id,)).fetchone()
    assert row == (3, 5, "PythonOperator")


# ---- migration 021: detail_to_aggregate edge_type ---------------------------

def test_detail_to_aggregate_is_registered_provenance_edge(conn):
    row = conn.execute(
        "SELECT is_provenance FROM cp.edge_type WHERE edge_type='detail_to_aggregate'"
    ).fetchone()
    assert row is not None, "detail_to_aggregate edge_type not registered"
    assert row[0] is True


def test_detail_to_aggregate_requires_upstream_link(conn):
    """The 021-extended upstream_link_required_for_run_edges CHECK governs
    detail_to_aggregate: an edge WITHOUT upstream_lineage_link_id is REJECTED;
    WITH it (under a detail_to_aggregate link) it is ACCEPTED."""
    # an upstream output link to anchor the aggregate edge
    up_run = conn.execute(
        "SELECT cp.start_run(%s,'canonicalize','sales','orders',%s,'manual')",
        (str(uuid4()), BD),
    ).fetchone()[0]
    fid = conn.execute(
        "SELECT cp.register_file(%s,%s,%s,'sales','orders')",
        (f"s3://raw/{uuid4()}.csv", uuid4().hex, BD),
    ).fetchone()[0]
    up_link = conn.execute(
        "SELECT cp.write_lineage_link(%s,'raw_to_curated',%s,%s,%s)",
        (up_run, json.dumps({"path": "s3://detail", "content_hash": "d1", "version": 1}),
         3, json.dumps([{"edge_type": "raw_to_curated", "source_file_id": str(fid),
                         "source_ref": {}, "record_count": 3}])),
    ).fetchone()[0]

    agg_run = conn.execute(
        "SELECT cp.start_run(%s,'aggregate','sales','orders',%s,'manual')",
        (str(uuid4()), BD),
    ).fetchone()[0]
    agg_link = conn.execute(
        "INSERT INTO cp.lineage_link (consumer_run_id, edge_type, target_ref, record_count) "
        "VALUES (%s,'detail_to_aggregate',%s,%s) RETURNING lineage_link_id",
        (agg_run, json.dumps({"path": "s3://agg", "content_hash": "a1", "version": 1}), 1),
    ).fetchone()[0]

    # WITHOUT upstream_lineage_link_id -> REJECTED by the CHECK.
    conn.execute("SAVEPOINT no_link")
    with pytest.raises(psycopg.errors.CheckViolation):
        conn.execute(
            "INSERT INTO cp.lineage_edge (lineage_link_id, edge_type, source_ref, record_count) "
            "VALUES (%s,'detail_to_aggregate',%s,%s)",
            (agg_link, json.dumps({}), 3),
        )
    conn.execute("ROLLBACK TO SAVEPOINT no_link")

    # WITH upstream_lineage_link_id -> ACCEPTED.
    eid = conn.execute(
        "INSERT INTO cp.lineage_edge (lineage_link_id, edge_type, "
        "upstream_run_id, upstream_lineage_link_id, source_ref, record_count) "
        "VALUES (%s,'detail_to_aggregate',%s,%s,%s,%s) RETURNING lineage_edge_id",
        (agg_link, up_run, up_link, json.dumps({}), 3),
    ).fetchone()[0]
    assert eid is not None
