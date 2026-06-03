"""Tests for the customer-transaction lineage DEMO workflow (spec tests 1-16).

Spec: docs/specs/2026-05-30-customer-transaction-lineage-dashboard.md (§Tests).

Each test runs run_demo(conn, commit=False) once via a function-scoped fixture
that rolls back through the `conn` fixture, so no committed state leaks.
"""
import datetime as dt
import pathlib

import pytest

from harness.customer_transaction_workflow import (
    AGG_DATASET,
    BUSINESS_DATES,
    CORRECTED_TRANSACTION_ROWS,
    CUSTOMER_DATASET,
    CUSTOMER_ROWS,
    DETAIL_DATASET,
    REFEED_BUSINESS_DATE,
    TRANSACTION_DATASET,
    TRANSACTION_ROWS,
    export_demo_snapshot,
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


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
def _trace(conn, link_id):
    return conn.execute(TRACE_SQL, {"link_id": link_id}).fetchall()


def _raw_paths(trace_rows):
    """The non-null raw_s3_path leaves of a trace (column index 5)."""
    return {row[5] for row in trace_rows if row[5] is not None}


def _detail_link_for(conn, workflow_run_id):
    """The canonical_to_sink link stamped on a detail row of this execution."""
    return conn.execute(
        f"""
        SELECT _ods_lineage_link_id::text
        FROM ods.{DETAIL_DATASET}
        WHERE _ods_workflow_run_id = %s
        ORDER BY row_id LIMIT 1
        """,
        (workflow_run_id,),
    ).fetchone()[0]


def _run_count(conn, workflow_run_id, *, pipeline_type=None, dataset=None):
    sql = "SELECT count(*) FROM cp.run_log WHERE workflow_run_id = %s"
    params = [workflow_run_id]
    if pipeline_type is not None:
        sql += " AND pipeline_type = %s"
        params.append(pipeline_type)
    if dataset is not None:
        sql += " AND dataset = %s"
        params.append(dataset)
    return conn.execute(sql, params).fetchone()[0]


# --------------------------------------------------------------------------- #
# Test 1: raw registrations for all 3 normal days (customer + transaction).
# --------------------------------------------------------------------------- #
def test_01_raw_registrations_for_all_three_normal_days(demo, conn):
    for date in (DAY1, DAY2, DAY3):
        for dataset in (CUSTOMER_DATASET, TRANSACTION_DATASET):
            n = conn.execute(
                """
                SELECT count(*) FROM cp.file_catalogue
                WHERE business_date = %s AND dataset = %s
                  AND s3_raw_path = %s
                """,
                (date, dataset, f"s3://raw/sales/{dataset}/{date}.json"),
            ).fetchone()[0]
            assert n == 1, f"missing raw registration {dataset} {date}"

        # And each normal day has an ingestion run per dataset.
        wfid = demo["normals_by_date"][date]["workflow_run_id"]
        assert _run_count(conn, wfid, pipeline_type="ingestion") == 2


# --------------------------------------------------------------------------- #
# Test 2: customer raw -> customer silver lineage per business date.
# --------------------------------------------------------------------------- #
def test_02_customer_silver_per_day(demo, conn):
    for date in (DAY1, DAY2, DAY3):
        wfid = demo["normals_by_date"][date]["workflow_run_id"]
        assert _run_count(conn, wfid, pipeline_type="canonicalization",
                          dataset=CUSTOMER_DATASET) == 1
        silver = demo["normals_by_date"][date]["customer_silver"]
        link = conn.execute(
            "SELECT edge_type, record_count FROM cp.lineage_link "
            "WHERE lineage_link_id = %s",
            (silver["link_id"],),
        ).fetchone()
        assert link == ("curated_to_canonical", len(demo["normals_by_date"][date]
                        ["files"]["customer"]["rows"]))


# --------------------------------------------------------------------------- #
# Test 3: transaction raw -> transaction silver lineage per business date.
# --------------------------------------------------------------------------- #
def test_03_transaction_silver_per_day(demo, conn):
    for date in (DAY1, DAY2, DAY3):
        wfid = demo["normals_by_date"][date]["workflow_run_id"]
        assert _run_count(conn, wfid, pipeline_type="canonicalization",
                          dataset=TRANSACTION_DATASET) == 1
        silver = demo["normals_by_date"][date]["transaction_silver"]
        edge_type = conn.execute(
            "SELECT edge_type FROM cp.lineage_link WHERE lineage_link_id = %s",
            (silver["link_id"],),
        ).fetchone()[0]
        assert edge_type == "curated_to_canonical"


# --------------------------------------------------------------------------- #
# Test 4: each normal merge link has 2 upstream edges.
# --------------------------------------------------------------------------- #
def test_04_normal_merge_has_two_upstream_edges(demo, conn):
    for date in (DAY1, DAY2, DAY3):
        merge_link = demo["normals_by_date"][date]["merge"]["link_id"]
        edges = conn.execute(
            "SELECT count(*) FROM cp.lineage_edge WHERE lineage_link_id = %s",
            (merge_link,),
        ).fetchone()[0]
        assert edges == 2, f"merge link {date} has {edges} edges, expected 2"


# --------------------------------------------------------------------------- #
# Test 5: detail sink writes expected Postgres rows for all 3 days.
# --------------------------------------------------------------------------- #
def test_05_detail_sink_rows_for_all_three_days(demo, conn):
    for date in (DAY1, DAY2, DAY3):
        wfid = demo["normals_by_date"][date]["workflow_run_id"]
        n = conn.execute(
            f"SELECT count(*) FROM ods.{DETAIL_DATASET} "
            "WHERE _ods_workflow_run_id = %s",
            (wfid,),
        ).fetchone()[0]
        assert n == len(TRANSACTION_ROWS)


# --------------------------------------------------------------------------- #
# Test 6: aggregate sink writes expected Postgres rows for all 3 days.
# --------------------------------------------------------------------------- #
def test_06_aggregate_sink_rows_for_all_three_days(demo, conn):
    for date in (DAY1, DAY2, DAY3):
        wfid = demo["normals_by_date"][date]["workflow_run_id"]
        n = conn.execute(
            f"SELECT count(*) FROM ods.{AGG_DATASET} "
            "WHERE _ods_workflow_run_id = %s",
            (wfid,),
        ).fetchone()[0]
        assert n == len(CUSTOMER_ROWS)


# --------------------------------------------------------------------------- #
# Test 6b: the aggregate output link uses edge_type 'detail_to_aggregate'
#          (migration 021), NOT the overloaded 'merge_to_canonical'. The merge
#          step still uses 'merge_to_canonical'; only the AGGREGATE link changed.
# --------------------------------------------------------------------------- #
def test_06b_aggregate_link_uses_detail_to_aggregate_edge_type(demo, conn):
    for date in (DAY1, DAY2, DAY3):
        result = demo["normals_by_date"][date]
        agg_link = result["aggregate"]["link_id"]
        merge_link = result["merge"]["link_id"]

        # Aggregate OUTPUT link and its single consuming edge are now
        # detail_to_aggregate.
        link_edge_type, edge_edge_types = conn.execute(
            """
            SELECT l.edge_type,
                   array_agg(e.edge_type ORDER BY e.lineage_edge_id)
            FROM cp.lineage_link l
            JOIN cp.lineage_edge e ON e.lineage_link_id = l.lineage_link_id
            WHERE l.lineage_link_id = %s
            GROUP BY l.edge_type
            """,
            (agg_link,),
        ).fetchone()
        assert link_edge_type == "detail_to_aggregate"
        assert edge_edge_types == ["detail_to_aggregate"]

        # The merge step is unchanged: still merge_to_canonical.
        merge_edge_type = conn.execute(
            "SELECT edge_type FROM cp.lineage_link WHERE lineage_link_id = %s",
            (merge_link,),
        ).fetchone()[0]
        assert merge_edge_type == "merge_to_canonical"

    # Refeed aggregate link is detail_to_aggregate too.
    refeed_agg_link = demo["refeed"]["aggregate"]["link_id"]
    assert conn.execute(
        "SELECT edge_type FROM cp.lineage_link WHERE lineage_link_id = %s",
        (refeed_agg_link,),
    ).fetchone()[0] == "detail_to_aggregate"


# --------------------------------------------------------------------------- #
# Test 7: a Day-1 detail row traces ONLY to Day-1 raw files.
# --------------------------------------------------------------------------- #
def test_07_day1_detail_traces_only_to_day1_raw(demo, conn):
    wfid = demo["normals_by_date"][DAY1]["workflow_run_id"]
    paths = _raw_paths(_trace(conn, _detail_link_for(conn, wfid)))
    assert any(f"/{CUSTOMER_DATASET}/" in p for p in paths)
    assert any(f"/{TRANSACTION_DATASET}/" in p for p in paths)
    # Day-1 only: no Day-2 or Day-3 raw paths.
    assert all(DAY1 in p for p in paths), paths
    assert not any(DAY2 in p for p in paths)
    assert not any(DAY3 in p for p in paths)


# --------------------------------------------------------------------------- #
# Test 8: a Day-3 detail row traces only to Day-3 raw files.
# --------------------------------------------------------------------------- #
def test_08_day3_detail_traces_only_to_day3_raw(demo, conn):
    wfid = demo["normals_by_date"][DAY3]["workflow_run_id"]
    paths = _raw_paths(_trace(conn, _detail_link_for(conn, wfid)))
    assert any(f"/{CUSTOMER_DATASET}/" in p for p in paths)
    assert any(f"/{TRANSACTION_DATASET}/" in p for p in paths)
    assert all(DAY3 in p for p in paths), paths
    assert not any(DAY1 in p for p in paths)
    assert not any(DAY2 in p for p in paths)


# --------------------------------------------------------------------------- #
# Test 9: the Day-2 refeed creates a distinct corrected transaction file
#         identity (different file_id AND md5 from original Day-2 transaction).
# --------------------------------------------------------------------------- #
def test_09_refeed_distinct_corrected_transaction_file(demo, conn):
    original_file = demo["day2"]["files"]["transaction"]
    corrected_file = demo["refeed"]["files"]["transaction"]

    assert original_file["file_md5"] != corrected_file["file_md5"]
    assert original_file["s3_raw_path"] != corrected_file["s3_raw_path"]
    assert "refeed" in corrected_file["s3_raw_path"]

    original_id = demo["day2"]["transaction_ingest"]["file_id"]
    corrected_id = demo["refeed"]["transaction_ingest"]["file_id"]
    assert original_id != corrected_id

    # Same business_date / domain / dataset (spec §Corrected Transaction Refeed).
    rows = conn.execute(
        "SELECT business_date::text, domain, dataset, file_md5 "
        "FROM cp.file_catalogue WHERE file_id = ANY(%s::uuid[]) ORDER BY file_md5",
        ([original_id, corrected_id],),
    ).fetchall()
    assert len(rows) == 2
    assert rows[0][0] == rows[1][0] == REFEED_DATE
    assert rows[0][1] == rows[1][1] and rows[0][2] == rows[1][2]
    assert rows[0][3] != rows[1][3]


# --------------------------------------------------------------------------- #
# Test 10: corrected merge consumes ORIGINAL customer silver link + CORRECTED
#          transaction silver link (assert the merge edges' upstream link ids).
# --------------------------------------------------------------------------- #
def test_10_corrected_merge_consumes_original_customer_and_corrected_tx(demo, conn):
    merge_link = demo["refeed"]["merge"]["link_id"]
    upstreams = {
        row[0]
        for row in conn.execute(
            "SELECT upstream_lineage_link_id::text FROM cp.lineage_edge "
            "WHERE lineage_link_id = %s",
            (merge_link,),
        ).fetchall()
    }
    original_customer_silver = demo["day2"]["customer_silver"]["link_id"]
    corrected_transaction_silver = demo["refeed"]["transaction_silver"]["link_id"]
    original_transaction_silver = demo["day2"]["transaction_silver"]["link_id"]

    assert upstreams == {original_customer_silver, corrected_transaction_silver}
    # Must NOT consume the original Day-2 transaction silver (spec line 301).
    assert original_transaction_silver not in upstreams


# --------------------------------------------------------------------------- #
# Test 11: a Day-2 ORIGINAL detail row traces to the original Day-2 transaction
#          raw (by source_file_id / path), not the corrected one.
# --------------------------------------------------------------------------- #
def test_11_day2_original_detail_traces_to_original_transaction_raw(demo, conn):
    wfid = demo["day2"]["workflow_run_id"]
    paths = _raw_paths(_trace(conn, _detail_link_for(conn, wfid)))
    assert f"s3://raw/sales/{TRANSACTION_DATASET}/{REFEED_DATE}.json" in paths
    assert f"s3://raw/sales/{CUSTOMER_DATASET}/{REFEED_DATE}.json" in paths
    # Original chain must NOT reach the corrected refeed raw.
    assert not any("refeed" in p for p in paths)


# --------------------------------------------------------------------------- #
# Test 12: a Day-2 CORRECTED detail row traces to the corrected transaction raw.
# --------------------------------------------------------------------------- #
def test_12_day2_corrected_detail_traces_to_corrected_transaction_raw(demo, conn):
    wfid = demo["refeed"]["workflow_run_id"]
    paths = _raw_paths(_trace(conn, _detail_link_for(conn, wfid)))
    corrected = f"s3://raw/sales/{TRANSACTION_DATASET}/{REFEED_DATE}-refeed.json"
    original = f"s3://raw/sales/{TRANSACTION_DATASET}/{REFEED_DATE}.json"
    assert corrected in paths
    # Reuses the ORIGINAL customer raw; must NOT reach the original transaction raw.
    assert f"s3://raw/sales/{CUSTOMER_DATASET}/{REFEED_DATE}.json" in paths
    assert original not in paths


# --------------------------------------------------------------------------- #
# Test 13 (F1): a Day-2 corrected AGGREGATE row traces back to ALL of its
#          contributing detail outputs' raws.
#
# The recomputed C001 aggregate = T100 (UNCHANGED, from the ORIGINAL Day-2 detail
# sink -> original transaction raw) + T101 (CHANGED, from the corrected refeed
# detail sink -> corrected refeed raw). With COMPLETE provenance (F1) the
# aggregate names BOTH contributing detail sinks, so it legitimately traces to
# BOTH the corrected AND the original transaction raw (the original raw is the
# true origin of the unchanged contributing row, NOT a leak). Reuses the original
# customer raw. (Before F1 the aggregate named only the corrected detail sink, so
# it under-reported its provenance and missed the original raw.)
# --------------------------------------------------------------------------- #
def test_13_day2_corrected_aggregate_traces_to_all_contributing_raws(demo, conn):
    wfid = demo["refeed"]["workflow_run_id"]
    agg_link = conn.execute(
        f"""
        SELECT _ods_lineage_link_id::text FROM ods.{AGG_DATASET}
        WHERE _ods_workflow_run_id = %s ORDER BY row_id LIMIT 1
        """,
        (wfid,),
    ).fetchone()[0]
    paths = _raw_paths(_trace(conn, agg_link))
    corrected = f"s3://raw/sales/{TRANSACTION_DATASET}/{REFEED_DATE}-refeed.json"
    original = f"s3://raw/sales/{TRANSACTION_DATASET}/{REFEED_DATE}.json"
    # Reaches the corrected refeed raw (changed contributing row T101)...
    assert corrected in paths
    # ...AND the original transaction raw (unchanged contributing row T100 came
    # from the original Day-2 detail output) — complete provenance, not a leak.
    assert original in paths
    assert f"s3://raw/sales/{CUSTOMER_DATASET}/{REFEED_DATE}.json" in paths


# --------------------------------------------------------------------------- #
# Test 13b: refeed sink writes only rows whose payload changed.
# --------------------------------------------------------------------------- #
def test_13b_refeed_target_upsert_only_writes_changed_rows(demo, conn):
    wfid = demo["refeed"]["workflow_run_id"]

    detail_rows = conn.execute(
        f"""
        SELECT payload->>'transaction_id'
        FROM ods.{DETAIL_DATASET}
        WHERE _ods_workflow_run_id = %s
        ORDER BY payload->>'transaction_id'
        """,
        (wfid,),
    ).fetchall()
    assert [row[0] for row in detail_rows] == ["T101", "T104"]

    aggregate_rows = conn.execute(
        f"""
        SELECT payload->>'customer_id'
        FROM ods.{AGG_DATASET}
        WHERE _ods_workflow_run_id = %s
        ORDER BY payload->>'customer_id'
        """,
        (wfid,),
    ).fetchall()
    assert [row[0] for row in aggregate_rows] == ["C001", "C003"]


# --------------------------------------------------------------------------- #
# Test 14: snapshot includes executions, runs, links, tables, files, traces.
# --------------------------------------------------------------------------- #
def test_14_snapshot_has_all_sections(demo, conn):
    snap = export_demo_snapshot(conn, demo["executions"])

    for key in ("executions", "runs", "links", "files", "tables", "traces",
                "scenario", "generated_at"):
        assert key in snap, f"snapshot missing {key}"

    assert len(snap["executions"]) == 4
    # 3 normal executions * 8 runs + 1 refeed * 6 runs = 30 runs.
    assert len(snap["runs"]) == 3 * 8 + 6
    assert {DETAIL_DATASET, AGG_DATASET} <= set(snap["tables"].keys())
    # 3 normal detail-sink executions plus changed-only refeed upserts.
    assert len(snap["tables"][DETAIL_DATASET]) == (
        3 * len(TRANSACTION_ROWS) + len(demo["refeed"]["changed_detail_rows"])
    )
    assert len(snap["tables"][AGG_DATASET]) == (
        3 * len(CUSTOMER_ROWS) + len(demo["refeed"]["changed_aggregate_rows"])
    )
    # customer files: 3 (one per normal day; refeed reuses) + transaction 3
    # original + 1 corrected = 7 distinct file rows.
    assert len(snap["files"]) == 7
    assert snap["scenario"]["refeed_business_date"] == REFEED_DATE

    # Every link with edges has a trace entry; runs carry nested stages.
    assert all("stages" in r for r in snap["runs"])
    assert all("edges" in l for l in snap["links"])
    assert all(l["lineage_link_id"] in snap["traces"] for l in snap["links"])

    # Orchestrator identity fields (migration 020) are PRESENT on every run row,
    # even though this manual demo leaves them NULL / {} (no external orchestrator).
    orchestrator_keys = {
        "orchestrator_type", "orchestrator_dag_id", "orchestrator_run_id",
        "orchestrator_task_id", "orchestrator_try_number",
        "orchestrator_map_index", "orchestrator_url", "orchestrator_payload",
    }
    for run in snap["runs"]:
        assert orchestrator_keys <= set(run.keys()), (
            f"run missing orchestrator keys: {orchestrator_keys - set(run.keys())}"
        )
        assert run["orchestrator_type"] is None
        assert run["orchestrator_payload"] == {}


# --------------------------------------------------------------------------- #
# Test 15: a selected target row has _ods_lineage_link_id present in links
#          (enough to focus Tab 1 by lineage_link_id).
# --------------------------------------------------------------------------- #
def test_15_target_row_link_id_present_in_links(demo, conn):
    snap = export_demo_snapshot(conn, demo["executions"])
    link_ids = {l["lineage_link_id"] for l in snap["links"]}

    for table in (DETAIL_DATASET, AGG_DATASET):
        for row in snap["tables"][table]:
            assert row["_ods_lineage_link_id"] is not None
            assert row["_ods_workflow_run_id"] is not None
            assert "row_id" in row and "payload" in row
            assert row["_ods_lineage_link_id"] in link_ids


# --------------------------------------------------------------------------- #
# Test 16 (F3): customer_transaction now WIRES target visibility activation.
#
# A4-01: this workflow writes business-visible Postgres sinks but previously
# NEVER called control.visibility.activate, so `sales` had 0 target_visibility
# rows and write-contract step 9 was skipped. It now activates per business_key
# (changed-only on the refeed), mirroring policy_claims. Scoped to THIS run's
# own wfids (robust to committed data).
# --------------------------------------------------------------------------- #
def _vis_rows(conn, *, dataset, replacement_key, business_date, workflow_run_ids):
    return conn.execute(
        """
        SELECT status, lineage_link_id::text, workflow_run_id, deactivated_at
        FROM ods.target_visibility
        WHERE domain = 'sales' AND dataset = %s AND business_date = %s
          AND replacement_scope = 'business_key' AND replacement_key = %s
          AND workflow_run_id = ANY(%s)
        ORDER BY activated_at, created_at
        """,
        (dataset, business_date, replacement_key, list(workflow_run_ids)),
    ).fetchall()


def test_16_customer_transaction_wires_visibility(demo, conn):
    wfids = [demo[k]["workflow_run_id"] for k in ("day1", "day2", "day3", "refeed")]

    # `sales` now has target_visibility rows produced by THIS demo's wfids.
    n = conn.execute(
        "SELECT count(*) FROM ods.target_visibility "
        "WHERE domain = 'sales' AND workflow_run_id = ANY(%s)",
        (wfids,),
    ).fetchone()[0]
    assert n > 0, "customer_transaction must activate target_visibility (F3)"

    # Day-1 normal: every detail business key and every aggregate key is active Y,
    # produced by the Day-1 wfid (single Y, never superseded by anything).
    day1 = demo["day1"]
    day1_detail_key = f"{CUSTOMER_ROWS[0]['customer_id']}:T100"
    rows = _vis_rows(conn, dataset=DETAIL_DATASET, replacement_key=day1_detail_key,
                     business_date=DAY1, workflow_run_ids=wfids)
    assert [r[0] for r in rows] == ["Y"]
    assert rows[0][2] == day1["workflow_run_id"]
    assert rows[0][3] is None

    # ---- The Day-2 transaction refeed is CHANGED-ONLY. ----
    day2 = demo["day2"]
    refeed = demo["refeed"]

    # A CHANGED detail key (T101, amount 74.50 -> 79.50): original N + corrected Y.
    changed_key = "C001:T101"
    rows = _vis_rows(conn, dataset=DETAIL_DATASET, replacement_key=changed_key,
                     business_date=REFEED_DATE, workflow_run_ids=wfids)
    assert [r[0] for r in rows] == ["N", "Y"], rows
    assert rows[0][1] == day2["detail_sink"]["link_id"]      # original now N
    assert rows[1][1] == refeed["detail_sink"]["link_id"]    # corrected now Y
    assert rows[0][3] is not None                            # deactivated_at set

    # An UNCHANGED detail key (T102, C002 identical in refeed): single original Y.
    unchanged_key = "C002:T102"
    rows = _vis_rows(conn, dataset=DETAIL_DATASET, replacement_key=unchanged_key,
                     business_date=REFEED_DATE, workflow_run_ids=wfids)
    assert [r[0] for r in rows] == ["Y"], rows
    assert rows[0][2] == day2["workflow_run_id"]
    assert rows[0][3] is None

    # No double-active: exactly one Y per key across this demo's wfids.
    dup = conn.execute(
        """
        SELECT dataset, business_date, replacement_key, count(*)
        FROM ods.target_visibility
        WHERE status = 'Y' AND domain = 'sales' AND workflow_run_id = ANY(%s)
        GROUP BY dataset, business_date, replacement_key HAVING count(*) > 1
        """,
        (wfids,),
    ).fetchall()
    assert dup == [], f"more than one active Y for some key: {dup}"


# --------------------------------------------------------------------------- #
# Test 17 (F1): COMPLETE refeed-aggregate provenance.
#
# The refeed corrected T101 (C001) and T104 (C003). The recomputed C001 daily
# aggregate draws from BOTH the ORIGINAL Day-2 detail output (the unchanged T100)
# and the CORRECTED refeed detail output (T101); its detail_to_aggregate input
# edges must name BOTH detail sink outputs, with per-edge contributing counts
# summing to the aggregate's transaction_count. Mirrors the DLQ workflow's
# test_19. The unchanged C002 aggregate is NOT recomputed.
# --------------------------------------------------------------------------- #
def test_17_refeed_aggregate_names_both_contributing_details(demo, conn):
    refeed = demo["refeed"]
    agg_link = refeed["aggregate"]["link_id"]
    normal_detail_link = str(demo["day2"]["detail_sink"]["link_id"])
    refeed_detail_link = str(refeed["detail_sink"]["link_id"])

    edges = conn.execute(
        "SELECT upstream_output_link_id::text, record_count FROM cp.input_edge "
        "WHERE output_link_id = %s AND edge_type = 'detail_to_aggregate' "
        "ORDER BY input_slot",
        (agg_link,),
    ).fetchall()
    upstreams = {e[0] for e in edges}
    assert len(edges) >= 2, (
        "recomputed refeed aggregate must name >=2 contributing detail outputs")
    assert normal_detail_link in upstreams, (
        "refeed aggregate must name the ORIGINAL Day-2 detail output (T100/T105)")
    assert refeed_detail_link in upstreams, (
        "refeed aggregate must name the CORRECTED refeed detail output (T101/T104)")

    counts = {e[0]: e[1] for e in edges}
    # C001: T100 (original, unchanged) + T101 (refeed, changed) -> 1 + 1.
    # C003: T105 (original, unchanged) + T104 (refeed, changed) -> 1 + 1.
    # Per-edge contributing counts sum to the recomputed aggregate's tx count.
    changed_tx_count = sum(
        r["transaction_count"] for r in refeed["changed_aggregate_rows"])
    assert counts[normal_detail_link] + counts[refeed_detail_link] == changed_tx_count


# --------------------------------------------------------------------------- #
# Test 18 (F6 fact-spine, migration 031): cross-hop reconcile_workflow ran and
#               reconciles ok on each normal day along the FACT SPINE.
#
# customer_transaction is a star schema (customer DIMENSION joined to the
# transaction FACT, then a row-reducing daily aggregate). The only universal
# cross-hop invariant is the fact spine: raw('transaction' FACT) == leaf
# 'customer_transaction' detail + dlq_unresolved. raw_in counts ONLY the 6
# transaction FACT rows (the 3-row customer DIMENSION is OFF-spine, excluded);
# sink_out counts ONLY the 6 customer_transaction leaf-detail rows (the daily
# aggregate is OFF-spine, verified per-hop by reconcile_sink_link). So a normal
# day reconciles ok: fact 6 == leaf-detail 6 + dlq 0.
#
# The CHANGED-ONLY refeed reconciles at changed-slice grain via per-output
# reconcile_sink_link; whole-fact reconcile_workflow is not applicable to a
# changed-only slice, so the refeed records NO workflow recon row.
# --------------------------------------------------------------------------- #
def test_18_workflow_recon_ran_and_normal_days_ok(demo, conn):
    def _wf_recon(wfid):
        return conn.execute(
            """
            SELECT rl.status, rl.metrics FROM cp.reconciliation_log rl
            JOIN cp.run_log r ON r.run_id = rl.run_id
            WHERE r.workflow_run_id = %s AND rl.check_type = 'workflow'
            """,
            (wfid,),
        ).fetchall()

    # The three NORMAL days each have exactly one workflow recon row and reconcile
    # ok on the fact spine (transaction raw 6 == customer_transaction detail 6).
    for date in (DAY1, DAY2, DAY3):
        wfid = demo["normals_by_date"][date]["workflow_run_id"]
        rows = _wf_recon(wfid)
        assert len(rows) == 1, (
            f"normal {date}: expected one workflow recon row, got {len(rows)}")
        status, metrics = rows[0]
        assert status == "ok", f"normal {date}: workflow recon {status}, {metrics}"
        assert metrics["raw_in"] == metrics["sink_out"] == 6
        assert metrics["dlq_out"] == 0
        assert metrics["source_datasets"] == ["transaction"]
        assert metrics["leaf_target"] == "customer_transaction"
        assert metrics["aggregates_excluded"] is True
        assert metrics["workflow_run_id"] == wfid

        # The customer DIMENSION (raw 3) is NOT in raw_in: the whole-workflow raw
        # sum is 9 (3 customer + 6 transaction), but fact-scoped raw_in is 6.
        all_raw = conn.execute(
            "SELECT coalesce(sum(l.record_count),0) FROM cp.lineage_link l "
            "JOIN cp.run_log r ON r.run_id=l.consumer_run_id "
            "WHERE r.workflow_run_id=%s AND l.edge_type='raw_to_curated'", (wfid,)
        ).fetchone()[0]
        assert all_raw == 9 and metrics["raw_in"] == 6, (
            "customer dimension must be excluded from raw_in")

        # The daily AGGREGATE (3 rows in ods.customer_transaction_daily) is NOT in
        # sink_out: detail 6 + aggregate 3 = 9 canonical_to_sink rows total, but
        # leaf-scoped sink_out is 6.
        all_sink = conn.execute(
            """
            SELECT count(*) FROM (
                SELECT 1 FROM ods.customer_transaction t
                JOIN cp.lineage_link l ON l.lineage_link_id=t._ods_lineage_link_id
                JOIN cp.run_log r ON r.run_id=l.consumer_run_id
                WHERE r.workflow_run_id=%s AND l.edge_type='canonical_to_sink'
                UNION ALL
                SELECT 1 FROM ods.customer_transaction_daily t
                JOIN cp.lineage_link l ON l.lineage_link_id=t._ods_lineage_link_id
                JOIN cp.run_log r ON r.run_id=l.consumer_run_id
                WHERE r.workflow_run_id=%s AND l.edge_type='canonical_to_sink'
            ) s
            """, (wfid, wfid)).fetchone()[0]
        assert all_sink == 9 and metrics["sink_out"] == 6, (
            "daily aggregate must be excluded from sink_out")

    # The changed-only refeed deliberately has NO workflow recon row.
    assert _wf_recon(demo["refeed"]["workflow_run_id"]) == []
