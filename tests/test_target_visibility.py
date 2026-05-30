"""P10-D — target-visibility / active-slice layer (spec §9 test plan).

Spec: docs/specs/2026-05-30-target-visibility-active-slice.md

These tests prove the SEPARATION of immutable lineage (audit truth) from business
truth (which output is active now), and that the active-slice supersession closes
at the business layer on refeed — WITHOUT deleting lineage or rewriting target
rows. They derive directly from the spec §9 plan:

  1 initial activation (one Y row)
  2 refed slice (old->N, new->Y, superseded_by set)
  3 business view returns only corrected rows after refeed
  4 no activation on failed run (RAISE / no Y row)
  5 no activation on recon breach (row loss -> breach -> activate RAISES, no Y row)
  6 idempotent retry (activate twice same link -> one Y row)
  7 [slice-scope] whole-slice replacement deactivates prior slice row
  8 aggregate output activates by lineage_link_id with file_id NULL
  9 audit: from an inactive (N) row, follow superseded_by to the active replacement

NOTE: spec §9 test #7 'file-scope replacement' is SKIPPED — this pass is
slice-scope only (spec §10 default). See test_file_scope_replacement_skipped.
"""
import json
from uuid import uuid4

import pytest
import psycopg

from harness import composers, fakes

BD = "2026-05-30"


def _file(domain="sales", dataset="orders", business_date=BD, n=5):
    return {
        "s3_raw_path": f"s3://raw/{uuid4()}.csv",
        "file_md5": uuid4().hex,
        "business_date": business_date,
        "domain": domain,
        "dataset": dataset,
        "record_count": n,
    }


def _active_rows(conn, link_id):
    return conn.execute(
        "SELECT visibility_id, status FROM ods.target_visibility "
        "WHERE lineage_link_id=%s", (link_id,)).fetchall()


# 1 -------------------------------------------------------------------------
def test_1_initial_activation_one_Y_row(conn):
    """A successful sink run creates exactly one status='Y' visibility row."""
    res = composers.run_to_sink(conn, file=_file(n=3), commit=False)
    link_id = res["sink"]["link_id"]

    rows = conn.execute(
        "SELECT status, domain, dataset, business_date, sink_type, target_name, "
        "replacement_scope, replacement_key, producer_run_id "
        "FROM ods.target_visibility WHERE lineage_link_id=%s", (link_id,)
    ).fetchall()
    assert len(rows) == 1, "expected exactly one visibility row"
    r = rows[0]
    assert r[0] == "Y"
    assert (r[1], r[2], r[4], r[5]) == ("sales", "orders", "postgres", "ods.orders")
    assert r[6] == "slice"
    assert r[7] == f"sales/orders/{BD}"
    assert str(r[8]) == str(res["sink"]["run_id"])

    n_active = conn.execute(
        "SELECT count(*) FROM ods.target_visibility "
        "WHERE replacement_key=%s AND status='Y'", (f"sales/orders/{BD}",)
    ).fetchone()[0]
    assert n_active == 1
    print("\n[§9.1] initial activation: one Y row, key=", r[7], "status=", r[0])


# 2 -------------------------------------------------------------------------
def test_2_refeed_slice_supersedes(conn):
    """Refeed: prior slice row -> N (with superseded_by set to the new row), the
    corrected output -> Y. Both lineage outputs remain immutable."""
    f = _file(n=3)
    res1 = composers.run_to_sink(conn, file=f, commit=False)
    orig_link = res1["sink"]["link_id"]
    orig_vis = res1["sink"]["visibility_id"]

    # Corrected refeed: same slice key, NEW file bytes (new chain, new lineage).
    corrected = dict(f, s3_raw_path=f"s3://raw/{uuid4()}.csv",
                     file_md5=uuid4().hex, record_count=4)
    res2 = composers.replay_single_file(
        conn, original_run_id=res1["sink"]["run_id"], file=corrected,
        commit=False)
    new_link = res2["sink"]["link_id"]
    new_vis = res2["sink"]["visibility_id"]

    key = f"sales/orders/{BD}"
    # exactly one active row, and it is the NEW one.
    active = conn.execute(
        "SELECT visibility_id FROM ods.target_visibility "
        "WHERE replacement_key=%s AND status='Y'", (key,)).fetchall()
    assert len(active) == 1
    assert str(active[0][0]) == str(new_vis)

    # the original row is deactivated with superseded_by -> the new row.
    orig = conn.execute(
        "SELECT status, deactivated_at, superseded_by "
        "FROM ods.target_visibility WHERE visibility_id=%s", (orig_vis,)
    ).fetchone()
    assert orig[0] == "N"
    assert orig[1] is not None
    assert str(orig[2]) == str(new_vis)

    # lineage immutable: BOTH sink links still exist.
    assert conn.execute(
        "SELECT count(*) FROM cp.lineage_link WHERE lineage_link_id IN (%s,%s)",
        (orig_link, new_link)).fetchone()[0] == 2
    print(f"\n[§9.2] refeed supersession: old={orig_vis} status=N "
          f"superseded_by={orig[2]} ; new={new_vis} status=Y")


# 3 -------------------------------------------------------------------------
def test_3_business_view_returns_only_corrected_rows(conn):
    """ods.v_orders_active returns ONLY the corrected rows after refeed; the old
    rows remain physically in ods.orders but are filtered out."""
    f = _file(n=3)
    res1 = composers.run_to_sink(conn, file=f, commit=False)
    orig_link = res1["sink"]["link_id"]

    corrected = dict(f, s3_raw_path=f"s3://raw/{uuid4()}.csv",
                     file_md5=uuid4().hex, record_count=4)
    res2 = composers.replay_single_file(
        conn, original_run_id=res1["sink"]["run_id"], file=corrected,
        commit=False)
    new_link = res2["sink"]["link_id"]

    # old rows still physically present in the base table.
    assert conn.execute(
        "SELECT count(*) FROM ods.orders WHERE _ods_lineage_link_id=%s",
        (orig_link,)).fetchone()[0] == 3

    # the view returns ONLY the corrected rows (4 of them, all the new link).
    view_links = conn.execute(
        "SELECT DISTINCT _ods_lineage_link_id FROM ods.v_orders_active"
    ).fetchall()
    assert [str(r[0]) for r in view_links] == [str(new_link)]
    assert conn.execute("SELECT count(*) FROM ods.v_orders_active").fetchone()[0] == 4
    print("\n[§9.3] business view after refeed: rows=4 all from new link; "
          "old rows present in base but filtered out")


# 4 -------------------------------------------------------------------------
def test_4_no_activation_on_failed_run(conn):
    """A non-succeeded producer run cannot be activated: activation RAISES and
    leaves no visibility row."""
    # Build a real sink link+rows+ok recon, but a run that is NOT succeeded.
    run_id = conn.execute(
        "SELECT cp.start_run(%s,'sink','sales','orders',%s,'manual')",
        (str(uuid4()), BD)).fetchone()[0]
    up_run = conn.execute(
        "SELECT cp.start_run(%s,'ingestion','sales','orders',%s,'manual')",
        (str(uuid4()), BD)).fetchone()[0]
    fid = conn.execute(
        "SELECT cp.register_file(%s,%s,%s,'sales','orders')",
        (f"s3://raw/{uuid4()}.csv", uuid4().hex, BD)).fetchone()[0]
    up_link = conn.execute(
        "SELECT cp.write_lineage_link(%s,'raw_to_curated',%s,%s,%s)",
        (up_run, json.dumps({"path": "s3://cur/f4", "content_hash": "f4up",
                             "version": 1}), 2,
         json.dumps([{"edge_type": "raw_to_curated", "source_file_id": str(fid),
                      "source_ref": {}, "record_count": 2}]))).fetchone()[0]
    edges = [{"edge_type": "canonical_to_sink", "upstream_run_id": str(up_run),
              "upstream_lineage_link_id": str(up_link),
              "source_ref": {}, "record_count": 2}]
    link = conn.execute(
        "SELECT cp.write_link_then_rows(%s,'canonical_to_sink',%s,%s,%s,%s,'postgres')",
        (run_id, json.dumps({"path": "pg://f4", "content_hash": "f4s",
                             "version": 1}), 2,
         json.dumps(edges), json.dumps([{"order_id": i} for i in range(2)]))
    ).fetchone()[0]
    conn.execute("SELECT cp.reconcile_sink_link(%s,%s)", (link, 2))  # ok
    # run is STILL 'running' (never finalised) -> activation must refuse.
    with pytest.raises(psycopg.errors.RaiseException, match="must be succeeded"):
        conn.execute(
            "SELECT cp.activate_target_visibility("
            "'sales','orders',%s,'postgres','ods.orders',NULL,%s,%s,%s)",
            (BD, link, run_id, "wf-f4"))
    # rollback the failed statement's aborted state to assert no Y row.
    conn.rollback()
    assert conn.execute("SELECT count(*) FROM ods.target_visibility").fetchone()[0] == 0
    print("\n[§9.4] non-succeeded run: activate RAISED 'must be succeeded', no Y row")


# 5 -------------------------------------------------------------------------
def test_5_no_activation_on_recon_breach(conn):
    """A sink write with ROW LOSS records a breach; activation RAISES and writes
    no Y row. The falsifiable graph-derived check is the gate."""
    run_id = conn.execute(
        "SELECT cp.start_run(%s,'sink','sales','orders',%s,'manual')",
        (str(uuid4()), BD)).fetchone()[0]
    up_run = conn.execute(
        "SELECT cp.start_run(%s,'ingestion','sales','orders',%s,'manual')",
        (str(uuid4()), BD)).fetchone()[0]
    fid = conn.execute(
        "SELECT cp.register_file(%s,%s,%s,'sales','orders')",
        (f"s3://raw/{uuid4()}.csv", uuid4().hex, BD)).fetchone()[0]
    up_link = conn.execute(
        "SELECT cp.write_lineage_link(%s,'raw_to_curated',%s,%s,%s)",
        (up_run, json.dumps({"path": "s3://cur/f5", "content_hash": "f5up",
                             "version": 1}), 5,
         json.dumps([{"edge_type": "raw_to_curated", "source_file_id": str(fid),
                      "source_ref": {}, "record_count": 5}]))).fetchone()[0]
    edges = [{"edge_type": "canonical_to_sink", "upstream_run_id": str(up_run),
              "upstream_lineage_link_id": str(up_link),
              "source_ref": {}, "record_count": 5}]
    # ROW LOSS: source claims 5 but only 3 rows are actually written.
    link = conn.execute(
        "SELECT cp.write_link_then_rows(%s,'canonical_to_sink',%s,%s,%s,%s,'postgres')",
        (run_id, json.dumps({"path": "pg://f5", "content_hash": "f5s",
                             "version": 1}), 5,
         json.dumps(edges), json.dumps([{"order_id": i} for i in range(3)]))
    ).fetchone()[0]
    # graph-derived per-link recon: source 5 vs accounted 3 -> breach.
    conn.execute("SELECT cp.reconcile_sink_link(%s,%s)", (link, 5))
    status = conn.execute(
        "SELECT status FROM cp.reconciliation_log WHERE check_type='sink_link' "
        "AND metrics->>'lineage_link_id'=%s ORDER BY recon_id DESC LIMIT 1",
        (str(link),)).fetchone()[0]
    assert status == "breach"
    # finalise succeeded so ONLY the recon gate can stop activation.
    conn.execute("SELECT cp.patch_run(%s,%s)",
                 (run_id, json.dumps({"status": "succeeded"})))
    with pytest.raises(psycopg.errors.RaiseException, match="must be ok"):
        conn.execute(
            "SELECT cp.activate_target_visibility("
            "'sales','orders',%s,'postgres','ods.orders',NULL,%s,%s,%s)",
            (BD, link, run_id, "wf-f5"))
    conn.rollback()
    assert conn.execute("SELECT count(*) FROM ods.target_visibility").fetchone()[0] == 0
    print("\n[§9.5] recon breach (5 vs 3): activate RAISED 'must be ok', no Y row")


# 6 -------------------------------------------------------------------------
def test_6_idempotent_retry_one_row(conn):
    """Calling activation twice for the SAME link returns the same active row and
    leaves exactly one Y row."""
    res = composers.run_to_sink(conn, file=_file(n=3), commit=False)
    link_id = res["sink"]["link_id"]
    run_id = res["sink"]["run_id"]
    first = res["sink"]["visibility_id"]

    # call activate AGAIN for the same link.
    second = conn.execute(
        "SELECT cp.activate_target_visibility("
        "'sales','orders',%s,'postgres','ods.orders',%s,%s,%s,%s)",
        (BD, res["sink"]["source_file_id"], link_id, run_id,
         res["workflow_run_id"])).fetchone()[0]
    assert str(second) == str(first), "retry created a different row"
    rows = _active_rows(conn, link_id)
    assert len([r for r in rows if r[1] == "Y"]) == 1
    assert len(rows) == 1
    print(f"\n[§9.6] idempotent retry: both calls -> {first}; one Y row")


# 7 -------------------------------------------------------------------------
def test_7_slice_scope_whole_slice_replacement(conn):
    """Slice scope: the corrected slice deactivates the prior active slice row for
    that slice key (whole-slice replacement, §9.8)."""
    f = _file(n=3)
    res1 = composers.run_to_sink(conn, file=f, commit=False)
    key = f"sales/orders/{BD}"
    assert conn.execute(
        "SELECT count(*) FROM ods.target_visibility "
        "WHERE replacement_key=%s AND status='Y'", (key,)).fetchone()[0] == 1

    corrected = dict(f, s3_raw_path=f"s3://raw/{uuid4()}.csv",
                     file_md5=uuid4().hex, record_count=2)
    composers.replay_single_file(
        conn, original_run_id=res1["sink"]["run_id"], file=corrected,
        commit=False)

    # still exactly one Y row for the slice (prior deactivated).
    ys = conn.execute(
        "SELECT count(*) FROM ods.target_visibility "
        "WHERE replacement_key=%s AND status='Y'", (key,)).fetchone()[0]
    ns = conn.execute(
        "SELECT count(*) FROM ods.target_visibility "
        "WHERE replacement_key=%s AND status='N'", (key,)).fetchone()[0]
    assert ys == 1 and ns == 1
    print(f"\n[§9.8] whole-slice replacement: Y={ys} N={ns} for key={key}")


# 8 -------------------------------------------------------------------------
def test_8_aggregate_output_activates_with_null_file_id(conn):
    """An AGGREGATE (merge) output activates by lineage_link_id with file_id NULL
    — we do not pretend an aggregate came from one file."""
    files = [_file(n=2), _file(n=3)]
    # same slice, two files -> merge -> aggregate canonical -> sink.
    for fl in files:
        fl["domain"], fl["dataset"], fl["business_date"] = "sales", "orders", BD
    mres = composers.run_multi_file(conn, files=files, commit=False)
    wf = mres["workflow_run_id"]
    total = sum(f["record_count"] for f in files)
    sink = fakes.fake_sink(
        conn, workflow_run_id=wf, domain="sales", dataset="orders",
        business_date=BD, record_count=total, sink_type="postgres",
        upstream_pipeline_type="merge", commit=False)

    assert sink["source_file_id"] is None, "aggregate must not claim one file"
    row = conn.execute(
        "SELECT status, file_id FROM ods.target_visibility WHERE lineage_link_id=%s",
        (sink["link_id"],)).fetchone()
    assert row[0] == "Y"
    assert row[1] is None
    # rows are visible via the link-only branch of the view (file_id NULL).
    assert conn.execute("SELECT count(*) FROM ods.v_orders_active").fetchone()[0] == total
    print(f"\n[§9.9] aggregate output: file_id NULL, status=Y, view rows={total}")


# 9 -------------------------------------------------------------------------
def test_9_audit_superseded_by_chain(conn):
    """Audit: from an inactive (N) visibility row, follow superseded_by to the
    active (Y) replacement."""
    f = _file(n=3)
    res1 = composers.run_to_sink(conn, file=f, commit=False)
    corrected = dict(f, s3_raw_path=f"s3://raw/{uuid4()}.csv",
                     file_md5=uuid4().hex, record_count=4)
    res2 = composers.replay_single_file(
        conn, original_run_id=res1["sink"]["run_id"], file=corrected,
        commit=False)

    # start from the inactive row, walk superseded_by to the live one.
    chain = conn.execute(
        "SELECT n.visibility_id, n.status, y.visibility_id, y.status "
        "FROM ods.target_visibility n "
        "JOIN ods.target_visibility y ON y.visibility_id = n.superseded_by "
        "WHERE n.status='N'").fetchall()
    assert len(chain) == 1
    n_id, n_st, y_id, y_st = chain[0]
    assert n_st == "N" and y_st == "Y"
    assert str(n_id) == str(res1["sink"]["visibility_id"])
    assert str(y_id) == str(res2["sink"]["visibility_id"])
    print(f"\n[§9.10] audit chain: inactive {n_id}(N) -> superseded_by -> {y_id}(Y)")


# spec §9.7 file-scope: SKIPPED this pass (slice-scope only, §10 default) -----
@pytest.mark.skip(reason="file-scope replacement deferred — slice-scope only "
                         "this pass (spec §10 open decision defaulted to slice)")
def test_file_scope_replacement_skipped():
    """Spec §9 test #7 (file-scope: two active files in one slice, corrected file
    deactivates only its predecessor). Not implemented this pass — the
    replacement_scope='file' logic is intentionally deferred. The columns exist
    (extensible) but no file-scope code path is built."""
