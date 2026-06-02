"""Tests for the insurance policy/claims Airflow-oriented DEMO workflow.

Spec: docs/specs/2026-06-02-airflow-orchestrator-policy-claims-workflow.md
      (§"Policy/Claims Workflow Shape", §"Three-Day Demo Data", §"Tests").

The headline behaviour under test is CHANGED-ONLY, business-key-grain target
visibility (Option A): on the Day-2 claim refeed only the CHANGED business keys
are re-activated (their prior active row -> status N, the corrected row -> Y),
while UNCHANGED Day-2 keys keep their ORIGINAL active row untouched.

Each test runs run_demo(conn, commit=False) once via a function-scoped fixture
that rolls back through the `conn` fixture, so no committed state leaks.
"""
import datetime as dt
import pathlib

import pytest

from harness.policy_claims_workflow import (
    AGG_DATASET,
    AGG_TARGET,
    BUSINESS_DATES,
    CLAIM_ROWS,
    CORRECTED_CLAIM_ROWS,
    DETAIL_DATASET,
    DETAIL_TARGET,
    DOMAIN,
    POLICY_ROWS,
    REFEED_BUSINESS_DATE,
    SINK_TYPE,
    aggregate_business_key,
    detail_business_key,
    run_demo,
)


TRACE_SQL = (pathlib.Path(__file__).resolve().parents[1]
             / "control" / "queries" / "trace_row.sql").read_text()

DAY1, DAY2, DAY3 = (str(d) for d in BUSINESS_DATES)
REFEED_DATE = str(REFEED_BUSINESS_DATE)


@pytest.fixture
def demo(conn):
    """Run the full demo once (rolled back by the `conn` fixture)."""
    return run_demo(conn, commit=False)


def _trace(conn, link_id):
    return conn.execute(TRACE_SQL, {"link_id": link_id}).fetchall()


def _raw_paths(trace_rows):
    """The non-null raw_s3_path leaves of a trace (column index 5)."""
    return {row[5] for row in trace_rows if row[5] is not None}


def _vis_rows(conn, *, dataset, replacement_key, business_date=REFEED_DATE):
    """All visibility rows for one (dataset, business_date, business_key).

    The active-uniqueness index is scoped by business_date, so the same detail
    key (policy_id:claim_id) legitimately has one active row PER business_date.
    Scope to the business_date under test (default: the refeed Day-2 slice).
    """
    return conn.execute(
        """
        SELECT status, lineage_link_id::text, workflow_run_id, deactivated_at
        FROM ods.target_visibility
        WHERE domain = %s AND dataset = %s AND business_date = %s
          AND replacement_scope = 'business_key' AND replacement_key = %s
        ORDER BY activated_at, created_at
        """,
        (DOMAIN, dataset, business_date, replacement_key),
    ).fetchall()


# --------------------------------------------------------------------------- #
# Test 1: three normal workflow executions are produced.
# --------------------------------------------------------------------------- #
def test_01_three_normal_executions_produced(demo, conn):
    normals = [e for e in demo["executions"] if e["execution_type"] == "normal"]
    assert len(normals) == 3
    assert {e["business_date"] for e in normals} == {DAY1, DAY2, DAY3}

    # Each normal execution is the full 8-run shape.
    for e in normals:
        n = conn.execute(
            "SELECT count(*) FROM cp.run_log WHERE workflow_run_id = %s",
            (e["workflow_run_id"],),
        ).fetchone()[0]
        assert n == 8, f"normal {e['business_date']} should have 8 runs, got {n}"


# --------------------------------------------------------------------------- #
# Test 2: exactly one claim refeed is produced for 2026-05-29.
# --------------------------------------------------------------------------- #
def test_02_one_claim_refeed_for_day2(demo, conn):
    refeeds = [e for e in demo["executions"] if e["execution_type"] == "refeed"]
    assert len(refeeds) == 1
    assert refeeds[0]["business_date"] == REFEED_DATE

    # The refeed is 6 runs: claim ingest, claim silver, merge, detail sink,
    # aggregate, aggregate sink (policy is REUSED, not re-run).
    n = conn.execute(
        "SELECT count(*) FROM cp.run_log WHERE workflow_run_id = %s",
        (refeeds[0]["workflow_run_id"],),
    ).fetchone()[0]
    assert n == 6
    # No policy ingest/canonicalization run under the refeed workflow.
    policy_runs = conn.execute(
        "SELECT count(*) FROM cp.run_log WHERE workflow_run_id = %s "
        "AND dataset = %s",
        (refeeds[0]["workflow_run_id"], "policy"),
    ).fetchone()[0]
    assert policy_runs == 0


# --------------------------------------------------------------------------- #
# Test 3: refeed points to the original Day-2 workflow.
# --------------------------------------------------------------------------- #
def test_03_refeed_points_to_original_day2_workflow(demo, conn):
    refeed = demo["refeed"]
    assert refeed["refeed_of_workflow_run_id"] == demo["day2"]["workflow_run_id"]

    # And the control plane recorded it (run_log.refeed_of_workflow_run_id on
    # the refeed runs, via replay_of_run_id chain is on ingest; the execution
    # metadata is the durable demo record).
    meta = next(e for e in demo["executions"]
                if e["execution_type"] == "refeed")
    assert meta["refeed_of_workflow_run_id"] == demo["day2"]["workflow_run_id"]


# --------------------------------------------------------------------------- #
# Test 4: refeed merge consumes original Day-2 policy silver + corrected claim.
# --------------------------------------------------------------------------- #
def test_04_refeed_merge_consumes_original_policy_and_corrected_claim(demo, conn):
    merge_link = demo["refeed"]["merge"]["link_id"]
    upstreams = {
        row[0]
        for row in conn.execute(
            "SELECT upstream_lineage_link_id::text FROM cp.lineage_edge "
            "WHERE lineage_link_id = %s",
            (merge_link,),
        ).fetchall()
    }
    original_policy_silver = demo["day2"]["policy_silver"]["link_id"]
    corrected_claim_silver = demo["refeed"]["claim_silver"]["link_id"]
    original_claim_silver = demo["day2"]["claim_silver"]["link_id"]

    assert upstreams == {original_policy_silver, corrected_claim_silver}
    # Must NOT consume the original Day-2 claim silver.
    assert original_claim_silver not in upstreams


# --------------------------------------------------------------------------- #
# Test 5: corrected Day-2 detail rows are stamped with the refeed output link.
# --------------------------------------------------------------------------- #
def test_05_corrected_detail_rows_stamped_with_refeed_output_link(demo, conn):
    refeed = demo["refeed"]
    detail_sink_link = refeed["detail_sink"]["link_id"]
    wfid = refeed["workflow_run_id"]

    rows = conn.execute(
        f"""
        SELECT payload->>'policy_id', payload->>'claim_id',
               _ods_lineage_link_id::text, _ods_output_link_id::text
        FROM ods.{DETAIL_DATASET}
        WHERE _ods_workflow_run_id = %s
        ORDER BY payload->>'claim_id'
        """,
        (wfid,),
    ).fetchall()

    # Only the CHANGED detail rows were written (CL100 amount, CL102 status).
    written = {(r[0], r[1]) for r in rows}
    expected = {(r["policy_id"], r["claim_id"])
                for r in refeed["changed_detail_rows"]}
    assert written == expected == {("P001", "CL100"), ("P002", "CL102")}

    # Every refeed detail row is stamped with the refeed detail sink link, on
    # BOTH the authoritative FK column and the 018 new-name mirror.
    for _pid, _cid, link, out_link in rows:
        assert link == detail_sink_link
        assert out_link == detail_sink_link


# --------------------------------------------------------------------------- #
# Test 6: a corrected aggregate row traces back through detail_to_aggregate to
#         the CORRECTED claim raw (v_provenance / trace_row.sql).
# --------------------------------------------------------------------------- #
def test_06_corrected_aggregate_traces_to_corrected_claim_raw(demo, conn):
    refeed = demo["refeed"]
    agg_link = refeed["aggregate"]["link_id"]

    # The aggregate output link uses the detail_to_aggregate edge type.
    link_edge_type = conn.execute(
        "SELECT edge_type FROM cp.lineage_link WHERE lineage_link_id = %s",
        (agg_link,),
    ).fetchone()[0]
    assert link_edge_type == "detail_to_aggregate"

    # Walk the provenance chain from the aggregate output back to raw.
    paths = _raw_paths(_trace(conn, agg_link))
    # Reaches the CORRECTED claim raw (distinct refeed path) and the policy raw.
    assert f"s3://raw/{DOMAIN}/claim/{REFEED_DATE}-refeed.json" in paths
    assert f"s3://raw/{DOMAIN}/policy/{REFEED_DATE}.json" in paths
    # Must NOT reach the original (non-refeed) Day-2 claim raw.
    assert f"s3://raw/{DOMAIN}/claim/{REFEED_DATE}.json" not in paths


# --------------------------------------------------------------------------- #
# Test 7: CHANGED-ONLY visibility (the headline requirement).
#   - changed Day-2 detail key  -> original row N + corrected row Y
#   - unchanged Day-2 detail key -> still the ORIGINAL Y (no N, not deactivated)
#   - exactly one Y per replacement_key
#   - the Day-2 slice was NOT blanket-deactivated:
#       count(N) == number of changed keys (not the whole slice)
# --------------------------------------------------------------------------- #
def test_07_changed_only_visibility(demo, conn):
    day2 = demo["day2"]
    refeed = demo["refeed"]
    day2_detail_sink = day2["detail_sink"]["link_id"]
    day2_agg_sink = day2["aggregate_sink"]["link_id"]
    refeed_detail_sink = refeed["detail_sink"]["link_id"]
    refeed_agg_sink = refeed["aggregate_sink"]["link_id"]

    # ---- A CHANGED detail key: original N (superseded), corrected Y. ----
    changed_key = detail_business_key(
        {"policy_id": "P001", "claim_id": "CL100"})  # amount 500 -> 600
    rows = _vis_rows(conn, dataset=DETAIL_DATASET, replacement_key=changed_key)
    statuses = [r[0] for r in rows]
    links = [r[1] for r in rows]
    assert statuses == ["N", "Y"], f"{changed_key}: expected N then Y, got {statuses}"
    assert links[0] == day2_detail_sink      # original Day-2 link, now N
    assert links[1] == refeed_detail_sink    # corrected refeed link, now Y
    assert rows[0][3] is not None            # original has deactivated_at

    # ---- An UNCHANGED detail key: still the ORIGINAL Y, no N. ----
    unchanged_key = detail_business_key(
        {"policy_id": "P003", "claim_id": "CL103"})  # identical in refeed
    rows = _vis_rows(conn, dataset=DETAIL_DATASET, replacement_key=unchanged_key)
    assert [r[0] for r in rows] == ["Y"], (
        f"{unchanged_key}: unchanged key must keep a single original Y, "
        f"got {[r[0] for r in rows]}")
    assert rows[0][1] == day2_detail_sink            # still the ORIGINAL link
    assert rows[0][2] == day2["workflow_run_id"]     # original workflow
    assert rows[0][3] is None                        # never deactivated

    # ---- Changed AGGREGATE key (auto): original N, corrected Y. ----
    changed_agg_key = aggregate_business_key(
        {"business_date": REFEED_DATE, "policy_type": "auto"})
    rows = _vis_rows(conn, dataset=AGG_DATASET, replacement_key=changed_agg_key)
    assert [r[0] for r in rows] == ["N", "Y"]
    assert rows[0][1] == day2_agg_sink
    assert rows[1][1] == refeed_agg_sink

    # ---- Unchanged AGGREGATE key (home): still original Y, no N. ----
    unchanged_agg_key = aggregate_business_key(
        {"business_date": REFEED_DATE, "policy_type": "home"})
    rows = _vis_rows(conn, dataset=AGG_DATASET, replacement_key=unchanged_agg_key)
    assert [r[0] for r in rows] == ["Y"]
    assert rows[0][1] == day2_agg_sink
    assert rows[0][3] is None

    # ---- Exactly one Y per replacement_key across the whole demo. ----
    dup_y = conn.execute(
        """
        SELECT dataset, business_date, replacement_key, count(*)
        FROM ods.target_visibility
        WHERE status = 'Y'
        GROUP BY dataset, business_date, replacement_key
        HAVING count(*) > 1
        """
    ).fetchall()
    assert dup_y == [], f"more than one active Y for some key: {dup_y}"

    # ---- The Day-2 slice was NOT blanket-deactivated: number of N rows for
    #      the Day-2 business_date equals the number of CHANGED keys exactly
    #      (changed detail keys + changed aggregate keys), NOT the whole slice.
    n_count = conn.execute(
        """
        SELECT count(*) FROM ods.target_visibility
        WHERE business_date = %s AND status = 'N'
        """,
        (REFEED_DATE,),
    ).fetchone()[0]
    changed_keys = (len(refeed["changed_detail_rows"])
                    + len(refeed["changed_aggregate_rows"]))
    assert n_count == changed_keys, (
        f"expected exactly {changed_keys} superseded (N) rows for the Day-2 "
        f"slice (changed keys only), got {n_count} — slice was blanket-deactivated")

    # The full Day-2 slice has more keys than were changed (proves the assertion
    # above is meaningful: we did NOT just deactivate everything).
    total_day2_detail_keys = len(CLAIM_ROWS)               # 4 detail keys
    total_day2_agg_keys = len({(REFEED_DATE, p["policy_type"]) for p in POLICY_ROWS})
    assert changed_keys < total_day2_detail_keys + total_day2_agg_keys


# --------------------------------------------------------------------------- #
# Test 7b (supporting): all runs carry orchestrator_type='airflow'.
# --------------------------------------------------------------------------- #
def test_07b_all_runs_orchestrator_type_airflow(demo, conn):
    wfids = tuple(e["workflow_run_id"] for e in demo["executions"])
    types = conn.execute(
        "SELECT DISTINCT orchestrator_type, trigger_type "
        "FROM cp.run_log WHERE workflow_run_id = ANY(%s)",
        (list(wfids),),
    ).fetchall()
    assert types == [("airflow", "airflow")], types

    # The orchestrator payload + scalar columns are populated for every run.
    bad = conn.execute(
        "SELECT count(*) FROM cp.run_log WHERE workflow_run_id = ANY(%s) "
        "AND (orchestrator_dag_id <> 'ods_policy_claims' "
        "     OR orchestrator_payload = '{}'::jsonb)",
        (list(wfids),),
    ).fetchone()[0]
    assert bad == 0
