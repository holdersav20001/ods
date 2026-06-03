"""Database functions for dashboard/support consumers.

These tests cover migration 022. The functions are intentionally database-side
so developers and support teams can query the same information as the dashboard
without using the dashboard UI.
"""
import datetime as dt
from uuid import uuid4

import pytest
from psycopg.types.json import Jsonb

from harness.policy_claims_workflow import run_demo


def test_dashboard_workflow_functions_return_demo_shape(conn):
    demo = run_demo(conn, commit=False)
    workflow_run_id = demo["day1"]["workflow_run_id"]

    summary = conn.execute(
        """
        SELECT run_count, stage_count, output_count, input_count,
               orchestrator_type, orchestrator_dag_id, datasets, pipeline_types
        FROM cp.dashboard_workflows()
        WHERE workflow_run_id = %s
        """,
        [workflow_run_id],
    ).fetchone()

    assert summary is not None
    assert summary[0] == 8
    assert summary[1] == 8
    assert summary[2] == 8
    assert summary[3] == 9
    assert summary[4] == "airflow"
    assert summary[5] == "ods_policy_claims"
    assert set(summary[6]) == {"claim", "policy", "policy_claim", "policy_claim_daily"}
    assert {"ingestion", "canonicalization", "merge", "sink", "aggregation"} <= set(summary[7])

    detail = conn.execute(
        "SELECT cp.dashboard_workflow_detail(%s)", [workflow_run_id]
    ).fetchone()[0]
    assert detail["workflow"]["workflow_run_id"] == workflow_run_id
    assert len(detail["runs"]) == 8
    assert len(detail["output_links"]) == 8
    assert len(detail["target_visibility"]) == 6
    assert all("stages" in run for run in detail["runs"])
    assert all("input_edges" in link for link in detail["output_links"])


def test_dashboard_output_trace_walks_to_raw_files(conn):
    demo = run_demo(conn, commit=False)
    workflow_run_id = demo["day1"]["workflow_run_id"]

    detail_sink_link = conn.execute(
        """
        SELECT ol.output_link_id
        FROM cp.run_log r
        JOIN cp.output_link ol ON ol.consumer_run_id = r.run_id
        WHERE r.workflow_run_id = %s
          AND r.pipeline_type = 'sink'
          AND r.dataset = 'policy_claim'
        """,
        [workflow_run_id],
    ).fetchone()[0]

    trace = conn.execute(
        "SELECT * FROM cp.dashboard_output_trace(%s)", [detail_sink_link]
    ).fetchall()
    raw_paths = {row[8] for row in trace if row[8] is not None}

    assert len(trace) >= 4
    assert any("/policy/" in path for path in raw_paths)
    assert any("/claim/" in path for path in raw_paths)
    assert {row[1] for row in trace} >= {
        "canonical_to_sink",
        "merge_to_canonical",
        "curated_to_canonical",
        "raw_to_curated",
    }


def test_developer_diagnostics_clean_workflow_has_no_findings(conn):
    demo = run_demo(conn, commit=False)
    workflow_run_id = demo["day1"]["workflow_run_id"]

    findings = conn.execute(
        "SELECT * FROM cp.developer_diagnostics(%s, %s)",
        [workflow_run_id, "ods.policy_claim"],
    ).fetchall()

    assert findings == []


def test_developer_diagnostics_catches_unfinished_run_and_stage(conn):
    workflow_run_id = str(uuid4())
    business_date = dt.date(2026, 6, 2)

    unfinished_run = conn.execute(
        "SELECT cp.start_run(%s,%s,%s,%s,%s,%s)",
        [workflow_run_id, "ingestion", "diag", "orders", business_date, "manual"],
    ).fetchone()[0]
    succeeded_run = conn.execute(
        "SELECT cp.start_run(%s,%s,%s,%s,%s,%s)",
        [workflow_run_id, "canonicalization", "diag", "orders", business_date, "manual"],
    ).fetchone()[0]
    conn.execute("SELECT cp.start_stage(%s,%s,%s)", [succeeded_run, "transform", 1])
    conn.execute(
        "SELECT cp.patch_run(%s,%s)",
        [succeeded_run, Jsonb({"status": "succeeded", "record_count_out": 10})],
    )

    findings = conn.execute(
        """
        SELECT check_name, severity, object_type, object_id
        FROM cp.developer_diagnostics(%s)
        ORDER BY check_name
        """,
        [workflow_run_id],
    ).fetchall()
    check_names = {row[0] for row in findings}

    assert "unfinished_run" in check_names
    assert "run_without_stages" in check_names
    assert "stage_not_closed_on_succeeded_run" in check_names
    assert "succeeded_run_without_output_link" in check_names
    assert str(unfinished_run) in {row[3] for row in findings}


def test_dashboard_functions_raise_clear_exceptions(conn):
    missing_workflow = str(uuid4())

    conn.execute("SAVEPOINT missing_workflow")
    with pytest.raises(Exception, match="workflow_run_id .* not found"):
        conn.execute("SELECT cp.dashboard_workflow_detail(%s)", [missing_workflow]).fetchone()
    conn.execute("ROLLBACK TO SAVEPOINT missing_workflow")

    workflow_run_id = str(uuid4())
    conn.execute(
        "SELECT cp.start_run(%s,%s,%s,%s,%s,%s)",
        [workflow_run_id, "ingestion", "diag", "orders", dt.date(2026, 6, 2), "manual"],
    )

    conn.execute("SAVEPOINT missing_table")
    with pytest.raises(Exception, match="target table ods.no_such_table does not exist"):
        conn.execute(
            "SELECT * FROM cp.developer_diagnostics(%s,%s)",
            [workflow_run_id, "ods.no_such_table"],
        ).fetchall()
    conn.execute("ROLLBACK TO SAVEPOINT missing_table")
