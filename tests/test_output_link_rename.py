"""output_link / input_edge naming cleanup — Option B (additive) tests.

Spec: docs/specs/2026-05-30-output-link-input-edge-rename.md (§"Tests To Update /
Add" 1-8). These prove the new human-readable surface WITHOUT any physical
rename — old names keep working (verified by the rest of the suite):

  1 cp.output_link view == cp.lineage_link (column-mapped)
  2 cp.input_edge view == cp.lineage_edge (column-mapped)
  3 write_output_link creates exactly one output_link + >=1 input_edge rows
  4 write_output_then_rows stamps _ods_output_link_id on target rows
  5 merge output: one output_link, two input_edge rows, each w/ upstream_output_link_id
  6 raw output: one output_link, one input_edge w/ source_file_id
  7 row trace works from _ods_output_link_id back to a raw file
  8 visibility activation accepts output_link_id terminology

Every test uses the `conn` rollback fixture (commit=False) so nothing leaks.
Test 8 commits a namespaced sink via the composer and cleans it up explicitly.
"""
import json
from uuid import uuid4

import pytest
import psycopg

from control import lineage, visibility
from harness import composers

BD = "2026-05-30"


# ---- helpers (mirror tests/test_contract.py) --------------------------------

def _start_run(conn, dataset="orders", pipeline="ingestion"):
    wfid = str(uuid4())
    run_id = conn.execute(
        "SELECT cp.start_run(%s,%s,'sales',%s,%s,'manual')",
        (wfid, pipeline, dataset, BD),
    ).fetchone()[0]
    return run_id, wfid


def _file(conn):
    return conn.execute(
        "SELECT cp.register_file(%s,%s,%s,'sales','orders')",
        (f"s3://raw/{uuid4()}.csv", uuid4().hex, BD),
    ).fetchone()[0]


# ---- 1: cp.output_link view == cp.lineage_link ------------------------------

def test_1_output_link_view_matches_lineage_link(conn):
    run_id, _ = _start_run(conn)
    edges = [{"edge_type": "raw_to_curated", "source_file_id": str(_file(conn)),
              "source_ref": {"k": 1}, "record_count": 5}]
    link = conn.execute(
        "SELECT cp.write_lineage_link(%s,'raw_to_curated',%s,%s,%s)",
        (run_id, json.dumps({"path": "s3://c/v1", "content_hash": "v1",
                             "version": 1}), 5, json.dumps(edges)),
    ).fetchone()[0]

    base = conn.execute(
        "SELECT lineage_link_id, consumer_run_id, edge_type, sink_type, "
        "target_ref, transform_version, record_count, created_at "
        "FROM cp.lineage_link WHERE lineage_link_id=%s", (link,)).fetchone()
    view = conn.execute(
        "SELECT output_link_id, consumer_run_id, edge_type, sink_type, "
        "target_ref, transform_version, record_count, created_at "
        "FROM cp.output_link WHERE output_link_id=%s", (link,)).fetchone()
    assert view == base
    assert view[0] == link  # output_link_id is the lineage_link_id


# ---- 2: cp.input_edge view == cp.lineage_edge -------------------------------

def test_2_input_edge_view_matches_lineage_edge(conn):
    run_id, _ = _start_run(conn)
    edges = [{"edge_type": "raw_to_curated", "source_file_id": str(_file(conn)),
              "source_ref": {"k": 2}, "record_count": 7}]
    link = conn.execute(
        "SELECT cp.write_lineage_link(%s,'raw_to_curated',%s,%s,%s)",
        (run_id, json.dumps({"path": "s3://c/v2", "content_hash": "v2",
                             "version": 1}), 7, json.dumps(edges)),
    ).fetchone()[0]

    base = conn.execute(
        "SELECT lineage_edge_id, lineage_link_id, upstream_run_id, "
        "upstream_lineage_link_id, source_file_id, input_slot, edge_type, "
        "source_ref, record_count "
        "FROM cp.lineage_edge WHERE lineage_link_id=%s", (link,)).fetchall()
    view = conn.execute(
        "SELECT input_edge_id, output_link_id, upstream_run_id, "
        "upstream_output_link_id, source_file_id, input_slot, edge_type, "
        "source_ref, record_count "
        "FROM cp.input_edge WHERE output_link_id=%s", (link,)).fetchall()
    assert view == base
    assert len(view) == 1


# ---- 3: write_output_link creates one output + >=1 input rows ----------------

def test_3_write_output_link_one_output_many_inputs(conn):
    run_id, _ = _start_run(conn)
    output_link_id = lineage.write_output_link(
        conn,
        consumer_run_id=run_id,
        edge_type="raw_to_curated",
        target_ref={"path": "s3://c/wo3", "content_hash": "wo3", "version": 1},
        record_count=4,
        inputs=[{
            "source_file_id": str(_file(conn)),
            "edge_type": "raw_to_curated",
            "source_ref": {"k": 1},
            "record_count": 4,
        }],
        commit=False,
    )
    n_out = conn.execute(
        "SELECT count(*) FROM cp.output_link WHERE output_link_id=%s",
        (output_link_id,)).fetchone()[0]
    n_in = conn.execute(
        "SELECT count(*) FROM cp.input_edge WHERE output_link_id=%s",
        (output_link_id,)).fetchone()[0]
    assert n_out == 1
    assert n_in >= 1


# ---- 4: write_output_then_rows stamps _ods_output_link_id --------------------

def test_4_write_output_then_rows_stamps_output_link_id(conn):
    run_id, wfid = _start_run(conn)  # dataset 'orders' (has _ods_output_link_id)
    rows = [{"order_id": 1, "amt": 10}, {"order_id": 2, "amt": 20}]
    output_link_id = lineage.write_output_then_rows(
        conn,
        consumer_run_id=run_id,
        edge_type="raw_to_curated",
        target_ref={"path": "s3://c/wo4", "content_hash": "wo4", "version": 1},
        record_count=len(rows),
        inputs=[{
            "source_file_id": str(_file(conn)),
            "edge_type": "raw_to_curated",
            "source_ref": {"k": 1},
            "record_count": len(rows),
        }],
        rows=rows,
        commit=False,
    )
    got = conn.execute(
        "SELECT count(*), "
        "count(*) FILTER (WHERE _ods_output_link_id=%s) "
        "FROM ods.orders WHERE _ods_lineage_link_id=%s",
        (output_link_id, output_link_id)).fetchone()
    assert got[0] == 2
    assert got[1] == 2, "every target row mirrors _ods_output_link_id"


# ---- 5: merge output: one output, two inputs, each w/ upstream_output_link_id

def test_5_merge_output_two_inputs_with_upstream_output_link_id(conn):
    # two upstream curated outputs to point at
    up_run, _ = _start_run(conn)
    up_a = lineage.write_output_link(
        conn, consumer_run_id=up_run, edge_type="raw_to_curated",
        target_ref={"path": "s3://c/up_a", "content_hash": "up_a", "version": 1},
        record_count=3,
        inputs=[{"source_file_id": str(_file(conn)),
                 "edge_type": "raw_to_curated", "source_ref": {}, "record_count": 3}],
        commit=False)
    up_b = lineage.write_output_link(
        conn, consumer_run_id=up_run, edge_type="raw_to_curated",
        target_ref={"path": "s3://c/up_b", "content_hash": "up_b", "version": 1},
        record_count=3,
        inputs=[{"source_file_id": str(_file(conn)),
                 "edge_type": "raw_to_curated", "source_ref": {}, "record_count": 3}],
        commit=False)

    merge_run, _ = _start_run(conn, dataset="orders", pipeline="merge")
    merge = lineage.write_output_link(
        conn,
        consumer_run_id=merge_run,
        edge_type="merge_to_canonical",
        target_ref={"path": "s3://c/merge5", "content_hash": "merge5", "version": 1},
        record_count=6,
        inputs=[
            {"upstream_run_id": str(up_run), "upstream_output_link_id": up_a,
             "input_slot": 0, "edge_type": "merge_to_canonical",
             "source_ref": {"role": "dim"}, "record_count": 3},
            {"upstream_run_id": str(up_run), "upstream_output_link_id": up_b,
             "input_slot": 1, "edge_type": "merge_to_canonical",
             "source_ref": {"role": "fact"}, "record_count": 3},
        ],
        commit=False,
    )
    n_out = conn.execute(
        "SELECT count(*) FROM cp.output_link WHERE output_link_id=%s",
        (merge,)).fetchone()[0]
    inputs = conn.execute(
        "SELECT upstream_output_link_id FROM cp.input_edge "
        "WHERE output_link_id=%s ORDER BY input_slot", (merge,)).fetchall()
    assert n_out == 1
    assert len(inputs) == 2
    assert {str(r[0]) for r in inputs} == {str(up_a), str(up_b)}
    assert all(r[0] is not None for r in inputs)


# ---- 6: raw output: one output, one input w/ source_file_id ------------------

def test_6_raw_output_one_input_with_source_file_id(conn):
    run_id, _ = _start_run(conn)
    fid = _file(conn)
    output_link_id = lineage.write_output_link(
        conn,
        consumer_run_id=run_id,
        edge_type="raw_to_curated",
        target_ref={"path": "s3://c/raw6", "content_hash": "raw6", "version": 1},
        record_count=2,
        inputs=[{"source_file_id": str(fid), "edge_type": "raw_to_curated",
                 "source_ref": {}, "record_count": 2}],
        commit=False,
    )
    n_out = conn.execute(
        "SELECT count(*) FROM cp.output_link WHERE output_link_id=%s",
        (output_link_id,)).fetchone()[0]
    rows = conn.execute(
        "SELECT source_file_id, upstream_output_link_id FROM cp.input_edge "
        "WHERE output_link_id=%s", (output_link_id,)).fetchall()
    assert n_out == 1
    assert len(rows) == 1
    assert str(rows[0][0]) == str(fid)
    assert rows[0][1] is None  # raw leaf has no upstream output


# ---- 7: row trace works from _ods_output_link_id back to a raw file ----------

def test_7_row_trace_from_output_link_id_to_raw(conn):
    run_id, wfid = _start_run(conn)
    fid = _file(conn)
    raw_path = conn.execute(
        "SELECT s3_raw_path FROM cp.file_catalogue WHERE file_id=%s",
        (fid,)).fetchone()[0]
    rows = [{"order_id": 1}, {"order_id": 2}]
    output_link_id = lineage.write_output_then_rows(
        conn,
        consumer_run_id=run_id,
        edge_type="raw_to_curated",
        target_ref={"path": "s3://c/trace7", "content_hash": "trace7", "version": 1},
        record_count=len(rows),
        inputs=[{"source_file_id": str(fid), "edge_type": "raw_to_curated",
                 "source_ref": {"path": raw_path}, "record_count": len(rows)}],
        rows=rows,
        source_file_id=str(fid),
        commit=False,
    )
    # read the new-name column off a target row, then trace from it
    row_output_link_id = conn.execute(
        "SELECT _ods_output_link_id FROM ods.orders "
        "WHERE _ods_lineage_link_id=%s LIMIT 1", (output_link_id,)).fetchone()[0]
    assert str(row_output_link_id) == str(output_link_id)

    trace_sql = open("control/queries/trace_row.sql").read()
    hops = conn.execute(trace_sql, {"link_id": row_output_link_id}).fetchall()
    # the trace reaches the raw file: some hop carries source_file_id + raw path
    raw_hops = [h for h in hops if h[4] is not None]
    assert raw_hops, "trace did not reach a raw file"
    assert str(raw_hops[0][4]) == str(fid)
    assert raw_hops[-1][5] == raw_path  # raw_s3_path column


# ---- 8: visibility activation accepts output_link_id terminology -------------

def test_8_visibility_activation_accepts_output_link_id(conn):
    """activate() accepts the PREFERRED new-name kwarg output_link_id (alias of
    the still-accepted lineage_link_id) and resolves it to the SAME active row.

    Uses the rollback `conn` fixture: the composer drives a full recon-ok sink
    with commit=False (succeeded run + sink_link recon ok), the composer's own
    activation uses the OLD kwarg, then we re-activate idempotently via the NEW
    kwarg. Nothing commits — the fixture rolls everything back.
    """
    file = {
        "s3_raw_path": f"s3://raw/{uuid4()}.csv",
        "file_md5": uuid4().hex,
        "business_date": BD,
        "domain": "sales",
        "dataset": "orders",
        "record_count": 3,
    }
    res = composers.run_to_sink(conn, file=file, commit=False)
    output_link_id = res["sink"]["link_id"]
    sink_run_id = res["sink"]["run_id"]
    wfid = conn.execute(
        "SELECT workflow_run_id FROM cp.run_log WHERE run_id=%s",
        (sink_run_id,)).fetchone()[0]

    # the composer already activated via the OLD kwarg path.
    existing = conn.execute(
        "SELECT visibility_id FROM ods.target_visibility "
        "WHERE lineage_link_id=%s AND status='Y'", (output_link_id,)
    ).fetchone()[0]

    # PREFERRED new-name kwarg -> idempotent, returns the SAME active row.
    conn.execute("SAVEPOINT s8")
    vid = visibility.activate(
        conn,
        domain="sales", dataset="orders", business_date=BD,
        sink_type="postgres", target_name="ods.orders",
        file_id=res["sink"].get("source_file_id"),
        output_link_id=output_link_id,   # <-- new terminology
        producer_run_id=sink_run_id, workflow_run_id=wfid,
        commit=False,
    )
    assert str(vid) == str(existing)

    # conflicting old+new kwargs (different values) is rejected by the wrapper.
    with pytest.raises(ValueError, match="output_link_id"):
        visibility.activate(
            conn, domain="sales", dataset="orders", business_date=BD,
            sink_type="postgres", target_name="ods.orders", file_id=None,
            output_link_id=output_link_id, lineage_link_id=str(uuid4()),
            producer_run_id=sink_run_id, workflow_run_id=wfid, commit=False)
