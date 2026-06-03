"""Migration 027 — strengthened cp.developer_diagnostics anomaly detection.

Spec: docs/specs/2026-06-03-working-platform-completion-plan.md area 6
      ("Strengthen Diagnostics" — the "Diagnostics Must Detect" list and the
      optional "Required Additions" lookup functions).

These tests prove that for EACH anomaly in the spec list we can CONSTRUCT the
bad state and cp.developer_diagnostics(workflow_run_id[, target_table]) reports
it (the matching issue_type / check_name appears), and that a CLEAN demo
workflow reports NO issues.

Building a bad state sometimes requires bypassing the sanctioned wrappers with
direct SQL inserts — that is the only way to materialise the anomaly the
detector must catch, and is acceptable IN TESTS. Every test uses the `conn`
rollback fixture (commit=False) and scopes assertions to its OWN
workflow_run_id, so it is robust to committed demo data.
"""
import datetime as dt
import json
from uuid import uuid4

import pytest

from harness.policy_claims_workflow import run_demo

BD = dt.date(2026, 6, 2)


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
def _diagnose(conn, workflow_run_id, target_table=None):
    """Return the set of check_name values for a workflow (optionally a table)."""
    if target_table is None:
        rows = conn.execute(
            "SELECT check_name, severity, object_type, object_id, message "
            "FROM cp.developer_diagnostics(%s) ORDER BY check_name",
            [workflow_run_id],
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT check_name, severity, object_type, object_id, message "
            "FROM cp.developer_diagnostics(%s, %s) ORDER BY check_name",
            [workflow_run_id, target_table],
        ).fetchall()
    return rows


def _checks(rows):
    return {r[0] for r in rows}


def _new_run(conn, *, status="succeeded", pipeline="ingestion", dataset="orders",
             trigger="manual", finished=True, domain="sales", wf=None,
             orchestrator_type=None):
    """Insert a cp.run_log row directly and return (run_id, workflow_run_id)."""
    wf = wf or str(uuid4())
    finished_at = "now()" if finished else "NULL"
    run_id = conn.execute(
        f"""
        INSERT INTO cp.run_log
            (workflow_run_id, pipeline_type, domain, dataset, business_date,
             trigger_type, status, started_at, finished_at, orchestrator_type)
        VALUES (%s,%s,%s,%s,%s,%s,%s, now(), {finished_at}, %s)
        RETURNING run_id
        """,
        [wf, pipeline, domain, dataset, BD, trigger, status, orchestrator_type],
    ).fetchone()[0]
    return run_id, wf


def _output_link(conn, run_id, *, edge_type="curated_to_canonical", sink_type=None,
                 target_ref=None, record_count=1):
    target_ref = target_ref or {"path": "s3://x/y", "content_hash": "h", "version": 1}
    return conn.execute(
        """
        INSERT INTO cp.lineage_link
            (consumer_run_id, edge_type, sink_type, target_ref, record_count)
        VALUES (%s,%s,%s,%s,%s) RETURNING lineage_link_id
        """,
        [run_id, edge_type, sink_type, json.dumps(target_ref), record_count],
    ).fetchone()[0]


def _input_edge(conn, link_id, *, edge_type="curated_to_canonical",
                source_file_id=None, upstream_run_id=None,
                upstream_link_id=None, record_count=1):
    return conn.execute(
        """
        INSERT INTO cp.lineage_edge
            (lineage_link_id, edge_type, source_file_id, upstream_run_id,
             upstream_lineage_link_id, record_count)
        VALUES (%s,%s,%s,%s,%s,%s) RETURNING lineage_edge_id
        """,
        [link_id, edge_type, source_file_id, upstream_run_id,
         upstream_link_id, record_count],
    ).fetchone()[0]


def _raw_file(conn, *, domain="sales", dataset="orders"):
    return conn.execute(
        """
        INSERT INTO cp.file_catalogue
            (s3_raw_path, file_md5, business_date, domain, dataset)
        VALUES (%s,%s,%s,%s,%s) RETURNING file_id
        """,
        [f"s3://raw/{dataset}/{uuid4().hex}.csv", "md5-" + uuid4().hex, BD,
         domain, dataset],
    ).fetchone()[0]


# --------------------------------------------------------------------------- #
# clean baseline
# --------------------------------------------------------------------------- #
def test_clean_demo_workflow_has_no_findings(conn):
    demo = run_demo(conn, commit=False)
    wf = demo["day1"]["workflow_run_id"]
    rows = _diagnose(conn, wf, "ods.policy_claim")
    assert rows == [], f"clean workflow reported issues: {_checks(rows)}"


# --------------------------------------------------------------------------- #
# run / stage anomalies
# --------------------------------------------------------------------------- #
def test_unfinished_run(conn):
    _, wf = _new_run(conn, status="running", finished=False)
    assert "unfinished_run" in _checks(_diagnose(conn, wf))


def test_unfinished_stage(conn):
    run_id, wf = _new_run(conn, status="running", finished=False)
    conn.execute(
        "INSERT INTO cp.run_stage_log (run_id, stage, attempt, status, started_at) "
        "VALUES (%s,'curate',1,'running', now())",
        [run_id],
    )
    assert "unfinished_stage" in _checks(_diagnose(conn, wf))


def test_succeeded_run_with_unfinished_stage(conn):
    run_id, wf = _new_run(conn, status="succeeded", finished=True)
    conn.execute(
        "INSERT INTO cp.run_stage_log (run_id, stage, attempt, status, started_at) "
        "VALUES (%s,'curate',1,'running', now())",
        [run_id],
    )
    # The 022-era check name is preserved.
    assert "stage_not_closed_on_succeeded_run" in _checks(_diagnose(conn, wf))


def test_run_without_stages(conn):
    _, wf = _new_run(conn, status="succeeded", finished=True)
    # No stage rows, no output link either -> both warnings present.
    assert "run_without_stages" in _checks(_diagnose(conn, wf))


def test_succeeded_run_without_output_link(conn):
    _, wf = _new_run(conn, status="succeeded", finished=True)
    assert "succeeded_run_without_output_link" in _checks(_diagnose(conn, wf))


# --------------------------------------------------------------------------- #
# lineage anomalies
# --------------------------------------------------------------------------- #
def test_output_link_without_input_edge(conn):
    run_id, wf = _new_run(conn, status="succeeded", finished=True)
    _output_link(conn, run_id)  # no input edge
    assert "output_link_without_input_edge" in _checks(_diagnose(conn, wf))


def test_input_edge_missing_input_identity(conn):
    run_id, wf = _new_run(conn, status="succeeded", finished=True)
    link = _output_link(conn, run_id)
    # 'replay' is allowed to anchor to nothing by the table CHECK, so use it to
    # materialise an edge with neither source_file_id nor upstream_output_link_id.
    _input_edge(conn, link, edge_type="replay")
    assert "input_edge_without_input_identifier" in _checks(_diagnose(conn, wf))


def test_downstream_input_missing_upstream_output_link(conn):
    run_id, wf = _new_run(conn, status="succeeded", finished=True)
    link = _output_link(conn, run_id)
    upstream_run, _ = _new_run(conn, status="succeeded", finished=True, wf=wf,
                               pipeline="canonicalization", dataset="orders")
    # downstream edge names a run but not the exact upstream output link.
    _input_edge(conn, link, edge_type="replay", upstream_run_id=upstream_run)
    assert "downstream_edge_missing_upstream_output_link" in _checks(_diagnose(conn, wf))


# --------------------------------------------------------------------------- #
# target-row anomalies (require a target_table argument)
# --------------------------------------------------------------------------- #
def test_target_row_missing_ods_ids(conn):
    demo = run_demo(conn, commit=False)
    wf = demo["day1"]["workflow_run_id"]
    # Null out an ODS id on one row of this workflow's target.
    conn.execute(
        "UPDATE ods.policy_claim SET _ods_output_link_id = NULL "
        "WHERE row_id = (SELECT min(row_id) FROM ods.policy_claim "
        "                WHERE _ods_workflow_run_id = %s)",
        [wf],
    )
    assert "target_row_missing_ods_ids" in _checks(_diagnose(conn, wf, "ods.policy_claim"))


def test_target_row_output_link_does_not_exist(conn):
    demo = run_demo(conn, commit=False)
    wf = demo["day1"]["workflow_run_id"]
    conn.execute(
        "UPDATE ods.policy_claim SET _ods_output_link_id = %s "
        "WHERE row_id = (SELECT min(row_id) FROM ods.policy_claim "
        "                WHERE _ods_workflow_run_id = %s)",
        [str(uuid4()), wf],
    )
    assert "target_row_broken_output_link" in _checks(_diagnose(conn, wf, "ods.policy_claim"))


def test_target_row_workflow_mismatch(conn):
    demo = run_demo(conn, commit=False)
    wf = demo["day1"]["workflow_run_id"]
    other = demo["day2"]["workflow_run_id"]
    # Re-stamp a day1 row with a DIFFERENT workflow id while its output_link
    # still belongs to day1 -> mismatch.
    conn.execute(
        "UPDATE ods.policy_claim SET _ods_workflow_run_id = %s "
        "WHERE row_id = (SELECT min(row_id) FROM ods.policy_claim "
        "                WHERE _ods_workflow_run_id = %s)",
        [other, wf],
    )
    assert "target_row_workflow_mismatch" in _checks(_diagnose(conn, other, "ods.policy_claim"))


# --------------------------------------------------------------------------- #
# visibility / orchestrator / recon anomalies
# --------------------------------------------------------------------------- #
def test_visibility_conflict(conn):
    run_id, wf = _new_run(conn, status="succeeded", finished=True,
                          dataset="orders", domain="sales")
    link_a = _output_link(conn, run_id, edge_type="canonical_to_sink",
                          sink_type="postgres")
    link_b = _output_link(conn, run_id, edge_type="canonical_to_sink",
                          sink_type="postgres",
                          target_ref={"path": "p2", "content_hash": "h2", "version": 1})
    # The active-slice unique index keys on target_name too, so the two active
    # rows differ ONLY by target_name. The diagnostic groups by replacement_key
    # (not target_name), so they still form a conflict for the SAME key.
    for link, target_name in ((link_a, "ods.orders_a"), (link_b, "ods.orders_b")):
        conn.execute(
            """
            INSERT INTO ods.target_visibility
                (domain, dataset, business_date, sink_type, target_name,
                 lineage_link_id, producer_run_id, workflow_run_id,
                 replacement_scope, replacement_key, status)
            VALUES (%s,%s,%s,'postgres',%s,%s,%s,%s,
                    'business_date','sales/orders/2026-06-02','Y')
            """,
            ["sales", "orders", BD, target_name, link, run_id, wf],
        )
    assert "active_visibility_conflict" in _checks(_diagnose(conn, wf))


def test_airflow_identity_missing(conn):
    _, wf = _new_run(conn, status="succeeded", finished=True,
                     trigger="airflow", orchestrator_type=None)
    assert "airflow_identity_missing" in _checks(_diagnose(conn, wf))


def test_reconciliation_missing_or_breached(conn):
    # A sink run with no reconciliation row.
    _, wf = _new_run(conn, status="succeeded", finished=True, pipeline="sink",
                     dataset="orders")
    assert "sink_reconciliation_missing_or_breached" in _checks(_diagnose(conn, wf))


# --------------------------------------------------------------------------- #
# DLQ anomalies
# --------------------------------------------------------------------------- #
def test_dlq_row_missing_trace_context(conn):
    run_id, wf = _new_run(conn, status="succeeded", finished=True)
    # DLQ row with NULL quarantine_output_link_id -> the failure has no
    # first-class quarantine output to trace through. (reason is NOT NULL at the
    # table level, so the missing piece here is the quarantine output link.)
    conn.execute(
        """
        INSERT INTO cp.dlq (run_id, stage, reason, source_ref, payload_ref,
                            record_count, failed_payload, status,
                            quarantine_output_link_id)
        VALUES (%s,'validate','bad row', %s, 's3://dlq/x.json', 1, %s, 'open', NULL)
        """,
        [run_id, json.dumps({}), json.dumps({"bad": 1})],
    )
    assert "dlq_row_missing_trace_context" in _checks(_diagnose(conn, wf))


def test_quarantine_output_missing_dlq_rows(conn):
    run_id, wf = _new_run(conn, status="succeeded", finished=True)
    # A first-class quarantine output_link with NO cp.dlq row referencing it.
    _output_link(conn, run_id, edge_type="quarantine",
                 target_ref={"path": "s3://dlq/orphan.json",
                             "content_hash": "h", "version": 1})
    assert "quarantine_output_without_dlq_rows" in _checks(_diagnose(conn, wf))


# --------------------------------------------------------------------------- #
# schema-validation anomaly
# --------------------------------------------------------------------------- #
def test_schema_validation_output_missing_schema_version(conn):
    # Seed a schema contract for (insurance, claim, canonical) then create a
    # curated_to_canonical output whose target_ref omits 'schema_version'.
    conn.execute(
        """
        INSERT INTO cp.schema_contract
            (domain, dataset, layer, schema_version, required_columns)
        VALUES ('insurance','claim','canonical','claim.v1','[]'::jsonb)
        """,
    )
    run_id, wf = _new_run(conn, status="succeeded", finished=True,
                          domain="insurance", dataset="claim",
                          pipeline="canonicalization")
    # A sibling canonical output that DOES record schema_version: this proves the
    # workflow adopted the convention, so the omitting output below is a defect.
    _output_link(conn, run_id, edge_type="curated_to_canonical",
                 target_ref={"path": "s3://canon/claim-ok.json", "content_hash": "h1",
                             "version": 1, "schema_version": "claim.v1"})
    # The offending output: same workflow/dataset, but no schema_version.
    _output_link(conn, run_id, edge_type="curated_to_canonical",
                 target_ref={"path": "s3://canon/claim-bad.json",
                             "content_hash": "h2", "version": 1})  # no schema_version
    assert ("schema_validation_output_missing_schema_version"
            in _checks(_diagnose(conn, wf)))


# --------------------------------------------------------------------------- #
# exception behaviour is preserved (regression on 022 contract)
# --------------------------------------------------------------------------- #
def test_unknown_workflow_raises(conn):
    with pytest.raises(Exception, match="workflow_run_id .* not found"):
        conn.execute("SELECT * FROM cp.developer_diagnostics(%s)",
                     [str(uuid4())]).fetchall()


def test_missing_target_table_raises(conn):
    _, wf = _new_run(conn, status="succeeded", finished=True)
    with pytest.raises(Exception, match="does not exist"):
        conn.execute("SELECT * FROM cp.developer_diagnostics(%s,%s)",
                     [wf, "ods.no_such_table"]).fetchall()


# --------------------------------------------------------------------------- #
# optional lookup functions (Required Additions)
# --------------------------------------------------------------------------- #
def test_dashboard_file_usage_round_trip(conn):
    demo = run_demo(conn, commit=False)
    wf = demo["day1"]["workflow_run_id"]
    # Pick a raw file consumed in this workflow (raw_to_curated edges name it).
    file_id = conn.execute(
        """
        SELECT ie.source_file_id
        FROM cp.run_log r
        JOIN cp.output_link ol ON ol.consumer_run_id = r.run_id
        JOIN cp.input_edge ie ON ie.output_link_id = ol.output_link_id
        WHERE r.workflow_run_id = %s AND ie.source_file_id IS NOT NULL
        LIMIT 1
        """,
        [wf],
    ).fetchone()[0]
    rows = conn.execute(
        "SELECT * FROM cp.dashboard_file_usage(%s)", [file_id]
    ).fetchall()
    assert rows, "dashboard_file_usage returned nothing for a consumed raw file"


def test_dashboard_target_row_trace_round_trip(conn):
    demo = run_demo(conn, commit=False)
    wf = demo["day1"]["workflow_run_id"]
    row_id = conn.execute(
        "SELECT min(row_id) FROM ods.policy_claim WHERE _ods_workflow_run_id = %s",
        [wf],
    ).fetchone()[0]
    trace = conn.execute(
        "SELECT * FROM cp.dashboard_target_row_trace('ods','policy_claim',%s)",
        [row_id],
    ).fetchall()
    # Trace should reach at least one raw file (a non-null raw_s3_path hop).
    raw_paths = {r[-1] for r in trace if r[-1] is not None}
    assert trace and raw_paths


def test_dashboard_airflow_lookup_round_trip(conn):
    demo = run_demo(conn, commit=False)
    wf = demo["day1"]["workflow_run_id"]
    dag_id, dag_run_id = conn.execute(
        "SELECT orchestrator_dag_id, orchestrator_run_id FROM cp.run_log "
        "WHERE workflow_run_id = %s AND orchestrator_run_id IS NOT NULL LIMIT 1",
        [wf],
    ).fetchone()
    rows = conn.execute(
        "SELECT * FROM cp.dashboard_airflow_lookup(%s,%s)", [dag_id, dag_run_id]
    ).fetchall()
    assert any(r[0] == wf for r in rows), "airflow lookup did not return the demo workflow"
