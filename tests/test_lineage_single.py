"""GATE A evidence — lineage invariants for the single-file ingest hop.

These tests drive the harness composer (commit=False; the conn fixture rolls
back) and BOTH assert the GATE-A invariants AND print the gate-query output.
Run with `pytest tests/test_lineage_single.py -v -s` to capture the prints —
that printed output is the gate evidence.

GATE A checklist (ingest hop):
  1. Trace-to-raw: every produced curated link reaches a raw source_file_id /
     file_catalogue row via cp.v_provenance (zero orphan curated links).
  2. No empty links: no produced lineage_link lacks a lineage_edge.
  3. Provenance excludes triggers: every v_provenance row for this run has an
     is_provenance edge_type (no 'orchestrates').
  4. One workflow_run_id: all run_log rows produced share the single id the
     composer minted.
  5. MUST-NOT spot-check: the link has exactly one edge whose source_file_id is
     the registered file and edge_type='raw_to_curated'.
Plus: run control/queries/trace_row.sql against the produced link and assert it
returns the raw path; print the reconstructed chain.
"""
import datetime
import pathlib

import pytest

from harness import composers

BD = datetime.date(2025, 2, 3)

TRACE_SQL = (pathlib.Path(__file__).resolve().parents[1]
             / "control" / "queries" / "trace_row.sql").read_text()


def _file(record_count=42):
    import uuid
    md5 = "md5-" + uuid.uuid4().hex
    return {
        "s3_raw_path": f"s3://raw/sales/orders/{md5}.csv",
        "file_md5": md5,
        "business_date": BD,
        "domain": "sales",
        "dataset": "orders",
        "record_count": record_count,
    }


@pytest.fixture
def result(conn):
    """Run the ingest hop once (rolled back by the conn fixture)."""
    f = _file()
    res = composers.run_single_file(conn, file=f, commit=False)
    res["_file"] = f
    return res


def test_trace_to_raw_no_orphan_curated_links(conn, result):
    """GATE A.1: the produced curated link reaches a raw source_file_id."""
    link_id = result["link_id"]
    rows = conn.execute(
        "SELECT lineage_link_id, edge_type, consumer_run_id, upstream_run_id, "
        "source_file_id FROM cp.v_provenance WHERE lineage_link_id=%s",
        (link_id,)).fetchall()
    assert rows, "curated link is an ORPHAN — not in cp.v_provenance"
    # at least one edge must anchor to a raw file
    raw_anchors = [r for r in rows if r[4] is not None]
    assert raw_anchors, "curated link reaches no raw source_file_id"
    assert str(raw_anchors[0][4]) == result["file_id"]

    print("\n[GATE A.1] v_provenance for produced link", link_id)
    for r in rows:
        print("   ", dict(zip(
            ["link", "edge_type", "consumer_run", "upstream_run", "source_file"], r)))


def test_no_empty_links(conn, result):
    """GATE A.2: the produced link has at least one edge (no empty links)."""
    empty = conn.execute(
        "SELECT count(*) FROM cp.lineage_link l "
        "WHERE l.lineage_link_id = %s "
        "AND NOT EXISTS (SELECT 1 FROM cp.lineage_edge e "
        "                WHERE e.lineage_link_id = l.lineage_link_id)",
        (result["link_id"],)).fetchone()[0]
    assert empty == 0
    print("\n[GATE A.2] empty links for this run:", empty)


def test_provenance_excludes_triggers(conn, result):
    """GATE A.3: every v_provenance row for this run is an is_provenance edge."""
    run_id = result["run_id"]
    rows = conn.execute(
        "SELECT p.edge_type, t.is_provenance "
        "FROM cp.v_provenance p "
        "JOIN cp.edge_type t ON t.edge_type = p.edge_type "
        "WHERE p.consumer_run_id = %s", (run_id,)).fetchall()
    assert rows
    assert all(r[1] is True for r in rows), "non-provenance edge leaked into v_provenance"
    assert all(r[0] != "orchestrates" for r in rows)
    print("\n[GATE A.3] provenance edge_types for run", run_id, ":",
          sorted({r[0] for r in rows}))


def test_one_workflow_run_id(conn, result):
    """GATE A.4: all run_log rows produced share the minted workflow_run_id."""
    wfid = result["workflow_run_id"]
    rows = conn.execute(
        "SELECT DISTINCT workflow_run_id FROM cp.run_log WHERE run_id = %s",
        (result["run_id"],)).fetchall()
    assert [r[0] for r in rows] == [wfid]
    print("\n[GATE A.4] workflow_run_id:", wfid)


def test_must_not_spot_check_single_raw_edge(conn, result):
    """GATE A.5: exactly one edge, source_file_id == registered file, raw_to_curated."""
    edges = conn.execute(
        "SELECT source_file_id, edge_type, upstream_run_id "
        "FROM cp.lineage_edge WHERE lineage_link_id = %s",
        (result["link_id"],)).fetchall()
    assert len(edges) == 1
    source_file_id, edge_type, upstream_run_id = edges[0]
    assert str(source_file_id) == result["file_id"]
    assert edge_type == "raw_to_curated"
    assert upstream_run_id is None  # ingest has no upstream run
    print("\n[GATE A.5] single edge:", dict(zip(
        ["source_file_id", "edge_type", "upstream_run_id"], edges[0])))


def test_trace_row_sql_reconstructs_chain_to_raw(conn, result):
    """Run control/queries/trace_row.sql and assert it returns the raw path."""
    link_id = result["link_id"]
    rows = conn.execute(TRACE_SQL, {"link_id": link_id}).fetchall()
    cols = ["hop", "edge_type", "consumer_run_id", "upstream_run_id",
            "source_file_id", "raw_s3_path"]
    assert rows, "trace_row.sql returned no chain"
    chain = [dict(zip(cols, r)) for r in rows]
    # the raw leaf carries the registered raw path
    raw_paths = [c["raw_s3_path"] for c in chain if c["raw_s3_path"] is not None]
    assert result["_file"]["s3_raw_path"] in raw_paths

    print("\n[trace_row.sql] reconstructed chain for link", link_id)
    for c in chain:
        print("   ", c)
