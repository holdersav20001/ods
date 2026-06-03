"""Fact-spine conservation for cp.reconcile_workflow (migration 031).

The 030 body computed a WHOLE-WORKFLOW conservation (raw_in = Σ ALL raw_to_curated;
sink_out = canonical_to_sink OR detail_to_aggregate rows). That is UNSOUND for any
merge/aggregate (star-schema) pipeline: it sums the DIMENSION raw input into raw_in
and the row-REDUCING AGGREGATE rollup into sink_out, neither of which is on the
row-conservation spine. It passed `sales` only by arithmetic coincidence.

Migration 031 re-declares reconcile_workflow as a SOUND FACT SPINE:
    raw(fact dataset) == leaf-detail canonical_to_sink rows + dlq_unresolved
with the DIMENSION excluded from raw_in (via p_source_datasets) and the AGGREGATE
never in sink_out (verified per-hop by reconcile_sink_link instead).

These tests build a star-schema workflow DIRECTLY via the sanctioned cp.* SQL and
assert the fact-spine behaviour, the new metrics, and that the OLD unsound model is
gone. Every query is scoped to the test's own workflow_run_id; the `conn` fixture
rolls everything back (nothing committed).
"""
import datetime as dt
import json
from uuid import uuid4

import pytest

from control import recon

BD = dt.date(2026, 5, 29)


def _run(conn, wf, pipeline, dataset, *, file_id=None):
    return conn.execute(
        "SELECT cp.start_run(%s,%s,'sales',%s,%s,'manual',%s)",
        (wf, pipeline, dataset, BD, file_id),
    ).fetchone()[0]


def _raw_to_curated(conn, run_id, file_id, n):
    return conn.execute(
        "SELECT cp.write_lineage_link(%s,'raw_to_curated',%s,%s,%s)",
        (run_id, json.dumps({"path": f"s3://silver/{uuid4().hex}",
                             "content_hash": uuid4().hex, "version": 1}), n,
         json.dumps([{"edge_type": "raw_to_curated", "source_file_id": str(file_id),
                      "source_ref": {}, "record_count": n}])),
    ).fetchone()[0]


def _sink(conn, run_id, dataset, upstream_run, upstream_link, n, edge_type):
    """Write a canonical_to_sink link + n rows into ods.<dataset> (created ad-hoc).

    edge_type is the UPSTREAM edge label carried in the input edge's source_ref;
    the link itself is always canonical_to_sink (the real sink-class link both a
    leaf detail and an aggregate carry). Returns the new link id."""
    conn.execute(
        f"CREATE TABLE IF NOT EXISTS ods.{dataset} ("
        "row_id BIGSERIAL PRIMARY KEY, payload JSONB NOT NULL, "
        "_ods_workflow_run_id TEXT, "
        "_ods_lineage_link_id UUID NOT NULL REFERENCES cp.lineage_link(lineage_link_id), "
        "_ods_output_link_id UUID)")
    return conn.execute(
        "SELECT cp.write_link_then_rows(%s,'canonical_to_sink',%s,%s,%s,%s,'postgres')",
        (run_id, json.dumps({"path": f"pg://{dataset}", "content_hash": uuid4().hex,
                             "version": 1}), n,
         json.dumps([{"edge_type": "canonical_to_sink",
                      "upstream_run_id": str(upstream_run),
                      "upstream_lineage_link_id": str(upstream_link),
                      "source_ref": {"upstream_edge_type": edge_type},
                      "record_count": n}]),
         json.dumps([{"k": i} for i in range(n)])),
    ).fetchone()[0]


def _file(conn, dataset):
    return conn.execute(
        "SELECT cp.register_file(%s,%s,%s,'sales',%s)",
        (f"s3://raw/{uuid4().hex}.csv", uuid4().hex, BD, dataset),
    ).fetchone()[0]


def _build_star_schema(conn, wf, *, dim_raw, fact_raw, detail_rows, agg_rows):
    """A DIMENSION (dim_raw rows) + a FACT (fact_raw rows) -> merge -> a leaf
    detail SINK (detail_rows in ods.cust_tx) -> an AGGREGATE SINK (agg_rows in
    ods.cust_tx_daily, also a canonical_to_sink link). Returns nothing; the
    caller reconciles by wfid."""
    # dimension raw -> curated
    dr = _run(conn, wf, "ingestion", "customer", file_id=_file(conn, "customer"))
    _raw_to_curated(conn, dr, _file(conn, "customer"), dim_raw)
    # fact raw -> curated
    fr = _run(conn, wf, "ingestion", "transaction", file_id=_file(conn, "transaction"))
    fact_link = _raw_to_curated(conn, fr, _file(conn, "transaction"), fact_raw)
    # merge -> leaf detail sink
    mr = _run(conn, wf, "merge", "cust_tx")
    merge_link = conn.execute(
        "SELECT cp.write_lineage_link(%s,'merge_to_canonical',%s,%s,%s)",
        (mr, json.dumps({"path": "s3://silver/cust_tx", "content_hash": uuid4().hex,
                         "version": 1}), detail_rows,
         json.dumps([{"edge_type": "merge_to_canonical", "upstream_run_id": str(fr),
                      "upstream_lineage_link_id": str(fact_link),
                      "source_ref": {}, "record_count": detail_rows}])),
    ).fetchone()[0]
    sr = _run(conn, wf, "sink", "cust_tx")
    detail_sink_link = _sink(conn, sr, "cust_tx", mr, merge_link, detail_rows,
                             "merge_to_canonical")
    # aggregate -> aggregate sink (detail_to_aggregate link, then canonical_to_sink)
    ar = _run(conn, wf, "aggregation", "cust_tx_daily")
    agg_link = conn.execute(
        "SELECT cp.write_lineage_link(%s,'detail_to_aggregate',%s,%s,%s)",
        (ar, json.dumps({"path": "s3://gold/cust_tx_daily",
                         "content_hash": uuid4().hex, "version": 1}), agg_rows,
         json.dumps([{"edge_type": "detail_to_aggregate", "upstream_run_id": str(sr),
                      "upstream_lineage_link_id": str(detail_sink_link),
                      "source_ref": {}, "record_count": detail_rows}])),
    ).fetchone()[0]
    asr = _run(conn, wf, "sink", "cust_tx_daily")
    _sink(conn, asr, "cust_tx_daily", ar, agg_link, agg_rows, "detail_to_aggregate")


def _wf_recon(conn, wf):
    return conn.execute(
        "SELECT status, source_count, accounted_count, discrepancy, metrics "
        "FROM cp.reconciliation_log WHERE check_type='workflow' "
        "AND metrics->>'workflow_run_id'=%s ORDER BY recon_id DESC LIMIT 1",
        (wf,)).fetchone()


def test_fact_spine_star_schema_reconciles_ok(conn):
    """A star schema where the DIMENSION (4 raw) differs from the FACT (6 raw) and
    a row-REDUCING aggregate (3) sits off-spine. Fact-scoped reconcile: raw_in =
    fact 6 == leaf-detail 6 + dlq 0 -> ok, with the new metrics populated."""
    wf = str(uuid4())
    _build_star_schema(conn, wf, dim_raw=4, fact_raw=6, detail_rows=6, agg_rows=3)
    recon.reconcile_workflow(conn, workflow_run_id=wf,
                             source_datasets=["transaction"],
                             leaf_target="cust_tx", commit=False)
    status, src, acc, disc, m = _wf_recon(conn, wf)
    assert (status, src, acc, disc) == ("ok", 6, 6, 0), (status, src, acc, disc)
    assert m["raw_in"] == 6 and m["sink_out"] == 6 and m["dlq_out"] == 0
    assert m["source_datasets"] == ["transaction"]
    assert m["leaf_target"] == "cust_tx"
    assert m["aggregates_excluded"] is True


def test_old_unsound_whole_workflow_model_is_gone(conn):
    """Same star schema. Under the OLD model (raw_in = ALL raw_to_curated = dim 4 +
    fact 6 = 10; sink_out = detail 6 + aggregate 3 = 9) it BREACHES by 1. The
    fact-scoped reconcile reconciles ok because the dimension is off-spine and the
    aggregate is never in sink_out. We prove BOTH: the old whole-workflow numbers
    do not balance, yet fact-scoping is ok."""
    wf = str(uuid4())
    _build_star_schema(conn, wf, dim_raw=4, fact_raw=6, detail_rows=6, agg_rows=3)

    # The OLD whole-workflow quantities (what 030 summed) do NOT balance.
    all_raw = conn.execute(
        "SELECT coalesce(sum(l.record_count),0) FROM cp.lineage_link l "
        "JOIN cp.run_log r ON r.run_id=l.consumer_run_id "
        "WHERE r.workflow_run_id=%s AND l.edge_type='raw_to_curated'", (wf,)
    ).fetchone()[0]
    detail_plus_agg = conn.execute(
        """
        SELECT count(*) FROM (
            SELECT t._ods_lineage_link_id FROM ods.cust_tx t
            JOIN cp.lineage_link l ON l.lineage_link_id=t._ods_lineage_link_id
            JOIN cp.run_log r ON r.run_id=l.consumer_run_id
            WHERE r.workflow_run_id=%s AND l.edge_type='canonical_to_sink'
            UNION ALL
            SELECT t._ods_lineage_link_id FROM ods.cust_tx_daily t
            JOIN cp.lineage_link l ON l.lineage_link_id=t._ods_lineage_link_id
            JOIN cp.run_log r ON r.run_id=l.consumer_run_id
            WHERE r.workflow_run_id=%s AND l.edge_type='canonical_to_sink'
        ) s
        """, (wf, wf)).fetchone()[0]
    assert all_raw == 10 and detail_plus_agg == 9
    assert all_raw != detail_plus_agg, "old model would breach this star schema"

    # The fact-scoped recon is ok.
    recon.reconcile_workflow(conn, workflow_run_id=wf,
                             source_datasets=["transaction"],
                             leaf_target="cust_tx", commit=False)
    status, src, acc, _disc, m = _wf_recon(conn, wf)
    assert status == "ok" and src == 6 and acc == 6


def test_dimension_excluded_from_raw_in(conn):
    """raw_in must NOT include the customer DIMENSION raw rows: scoping to the fact
    dataset gives raw_in = fact (6), not fact + dimension (10)."""
    wf = str(uuid4())
    _build_star_schema(conn, wf, dim_raw=4, fact_raw=6, detail_rows=6, agg_rows=3)
    recon.reconcile_workflow(conn, workflow_run_id=wf,
                             source_datasets=["transaction"],
                             leaf_target="cust_tx", commit=False)
    _status, _src, _acc, _disc, m = _wf_recon(conn, wf)
    assert m["raw_in"] == 6, f"dimension leaked into raw_in: {m}"


def test_aggregate_excluded_from_sink_out(conn):
    """sink_out must NOT include the daily AGGREGATE rows even though the aggregate
    output carries a canonical_to_sink sink link. Scoping to the leaf gives
    sink_out = leaf detail (6), not detail + aggregate (9). A workflow WITH an
    aggregate still reconciles ok."""
    wf = str(uuid4())
    _build_star_schema(conn, wf, dim_raw=6, fact_raw=6, detail_rows=6, agg_rows=3)
    recon.reconcile_workflow(conn, workflow_run_id=wf,
                             source_datasets=["transaction"],
                             leaf_target="cust_tx", commit=False)
    status, _src, _acc, _disc, m = _wf_recon(conn, wf)
    assert m["sink_out"] == 6, f"aggregate leaked into sink_out: {m}"
    assert status == "ok"


def test_fact_spine_with_dlq_balances_good_plus_quarantined(conn):
    """The DLQ headline at workflow grain: fact 4 == leaf-detail 3 + dlq 1.
    A genuine quarantine accounts for the missing fact row, so it reconciles ok."""
    wf = str(uuid4())
    fr = _run(conn, wf, "ingestion", "claim", file_id=_file(conn, "claim"))
    fid = _file(conn, "claim")
    fact_link = _raw_to_curated(conn, fr, fid, 4)
    # quarantine 1 of the 4 fact rows (status 'open' -> unresolved)
    conn.execute(
        "SELECT cp.quarantine(%s,'validate','bad',%s,'s3://dlq/x.json',1,%s,%s)",
        (fr, json.dumps({"raw_file_id": str(fid)}), json.dumps({"x": 1}), str(fid)))
    # 3 good -> merge -> leaf sink
    mr = _run(conn, wf, "merge", "pc")
    merge_link = conn.execute(
        "SELECT cp.write_lineage_link(%s,'merge_to_canonical',%s,3,%s)",
        (mr, json.dumps({"path": "s3://silver/pc", "content_hash": uuid4().hex,
                         "version": 1}),
         json.dumps([{"edge_type": "merge_to_canonical", "upstream_run_id": str(fr),
                      "upstream_lineage_link_id": str(fact_link),
                      "source_ref": {}, "record_count": 3}]))).fetchone()[0]
    sr = _run(conn, wf, "sink", "pc")
    _sink(conn, sr, "pc", mr, merge_link, 3, "merge_to_canonical")

    recon.reconcile_workflow(conn, workflow_run_id=wf,
                             source_datasets=["claim"], leaf_target="pc",
                             commit=False)
    status, src, acc, disc, m = _wf_recon(conn, wf)
    assert (status, src, acc, disc) == ("ok", 4, 4, 0), (status, src, acc, disc, m)
    assert m["raw_in"] == 4 and m["sink_out"] == 3 and m["dlq_out"] == 1


def test_single_source_fallback_no_params_still_works(conn):
    """Back-compat: a single-source workflow (no dimension/aggregate) reconciled
    with NO scoping params behaves exactly as before — raw_in = all raw_to_curated,
    sink_out = all canonical_to_sink leaf rows. 4 raw == 4 sink -> ok."""
    wf = str(uuid4())
    fid = _file(conn, "orders")
    ir = _run(conn, wf, "ingestion", "orders", file_id=fid)
    raw_link = _raw_to_curated(conn, ir, fid, 4)
    sr = _run(conn, wf, "sink", "orders")
    _sink(conn, sr, "orders", ir, raw_link, 4, "raw_to_curated")
    recon.reconcile_workflow(conn, workflow_run_id=wf, commit=False)
    status, src, acc, disc, m = _wf_recon(conn, wf)
    assert (status, src, acc, disc) == ("ok", 4, 4, 0)
    assert m["source_datasets"] is None and m["leaf_target"] is None
    assert m["aggregates_excluded"] is True


def test_reconcile_workflow_rejects_vacuous_all_zero(conn):
    """Re-audit #4 (migration 032): a reconcile that selects NOTHING on either
    side (raw_in=0, sink_out=0, dlq_out=0) must RAISE, not report a vacuous false
    'ok'. The 031 body computed 0 == 0 + 0 -> status 'ok' and inserted a green
    reconciliation_log row, so a typo'd/empty p_source_datasets (or a p_leaf_target
    matching no rows) silently passed.

    PROOF this catches the 031 bug: the same call against the 031 body inserts an
    'ok' workflow row (no exception); against 032 it RAISES and inserts nothing."""
    wf = str(uuid4())
    fid = _file(conn, "transaction")
    fr = _run(conn, wf, "ingestion", "transaction", file_id=fid)
    fact_link = _raw_to_curated(conn, fr, fid, 6)
    sr = _run(conn, wf, "sink", "cust_tx")
    _sink(conn, sr, "cust_tx", fr, fact_link, 6, "raw_to_curated")

    # A typo'd source dataset matches no raw_to_curated link AND the leaf matches
    # no canonical_to_sink rows -> raw_in=0, sink_out=0, dlq_out=0 -> RAISE.
    with pytest.raises(Exception) as exc:
        recon.reconcile_workflow(conn, workflow_run_id=wf,
                                 source_datasets=["NONEXISTENT_DS"],
                                 leaf_target="customer_transaction", commit=False)
    assert "nothing to reconcile" in str(exc.value)
    # the transaction is now aborted; roll back to a clean savepoint-free state so
    # the fixture teardown rollback is the only cleanup.
    conn.rollback()
    # And NO vacuous 'ok' workflow row was written (the RAISE aborted the INSERT).
    n = conn.execute(
        "SELECT count(*) FROM cp.reconciliation_log "
        "WHERE check_type='workflow' AND metrics->>'workflow_run_id'=%s", (wf,)
    ).fetchone()[0]
    assert n == 0


def test_missing_leaf_table_raises(conn):
    """When p_leaf_target names a dataset whose ods.<leaf> table is absent, the
    loop's dynamic %I + to_regclass guard RAISES. We write a real sink (so the
    canonical_to_sink link exists for that dataset) then DROP the table so the
    table is gone at reconcile time."""
    wf = str(uuid4())
    fid = _file(conn, "transaction")
    fr = _run(conn, wf, "ingestion", "transaction", file_id=fid)
    fact_link = _raw_to_curated(conn, fr, fid, 6)
    leaf = "missing_leaf_zzz"
    sr = _run(conn, wf, "sink", leaf)
    _sink(conn, sr, leaf, fr, fact_link, 6, "raw_to_curated")
    conn.execute(f"DROP TABLE ods.{leaf}")
    with pytest.raises(Exception):
        recon.reconcile_workflow(conn, workflow_run_id=wf,
                                 source_datasets=["transaction"],
                                 leaf_target=leaf, commit=False)
