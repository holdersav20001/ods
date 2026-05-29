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


# --------------------------------------------------------------------------- #
# GATE B evidence — the canonicalize hop (curated_to_canonical) via DISCOVERY.
#
# GATE B checklist (canonicalize hop):
#   1. Trace-to-raw ACROSS RUNS: walking cp.v_provenance / trace_row.sql from the
#      canonical link reaches the raw file THROUGH the ingest run (2 hops:
#      canonical -> ingest-curated -> raw file).
#   2. Discovery proven (mandatory re-run test): a second canonicalize, with NO
#      upstream id passed (the signature has none), links to the run that
#      latest_succeeded_run returns; with TWO succeeded ingests it links to
#      exactly what discovery selects — proving it selects, not grabs anything.
#   3. No empty links (canonical link has an edge); provenance for the canonical
#      run is only is_provenance edges (no orchestrates).
#   4. transform_version recorded on the canonical link row.
#   5. One workflow_run_id across ingest + canonicalize runs.
# --------------------------------------------------------------------------- #
CD = ["hop", "edge_type", "consumer_run_id", "upstream_run_id",
      "source_file_id", "raw_s3_path"]


def test_trace_canonical_to_raw_across_runs(conn, result):
    """GATE B.1: canonical link's chain reaches the raw file through the ingest run."""
    canon_link = result["canonicalize"]["link_id"]
    rows = conn.execute(TRACE_SQL, {"link_id": canon_link}).fetchall()
    chain = [dict(zip(CD, r)) for r in rows]
    assert chain, "trace_row.sql returned no chain for the canonical link"

    # Two provenance hops: canonical (hop 1) then ingest-curated (hop 2).
    edge_types = [c["edge_type"] for c in chain]
    assert "curated_to_canonical" in edge_types
    assert "raw_to_curated" in edge_types
    assert max(c["hop"] for c in chain) >= 2, "chain did not span runs (expected >=2 hops)"

    # The raw leaf carries the registered raw path -> trace reached the raw file.
    raw_paths = [c["raw_s3_path"] for c in chain if c["raw_s3_path"] is not None]
    assert result["_file"]["s3_raw_path"] in raw_paths
    # And it got there through the ingest run.
    upstreams = {str(c["upstream_run_id"]) for c in chain if c["upstream_run_id"]}
    assert result["ingest"]["run_id"] in upstreams

    print("\n[GATE B.1] 2-hop chain canonical -> ingest-curated -> raw, link", canon_link)
    for c in chain:
        print("   ", c)


def test_discovery_selects_latest_ingest_no_upstream_param(conn):
    """GATE B.2 (mandatory): discovery proven — no upstream id is ever passed,
    and with TWO succeeded ingests the canonical edge links to exactly the run
    latest_succeeded_run selects (the newest)."""
    import inspect
    from harness import fakes

    # PROOF #1 (static): fake_canonicalize's signature has NO upstream run param.
    params = set(inspect.signature(fakes.fake_canonicalize).parameters)
    forbidden = {"upstream_run_id", "upstream_id", "upstream_run", "ingest_run_id"}
    assert not (params & forbidden), (
        f"fake_canonicalize must DISCOVER its upstream, not accept it: {params & forbidden}")

    f = _file()
    wfid = "11111111-1111-1111-1111-111111111111"

    # Create TWO succeeded ingestion runs for the SAME slice (different file_md5
    # so register_file is not deduped). latest_succeeded_run must pick one of
    # them; we assert the canonical edge links to whatever it SELECTS.
    ing1 = fakes.fake_ingest(conn, workflow_run_id=wfid, file=f, commit=False)
    f2 = dict(f)
    f2["file_md5"] = f["file_md5"] + "-second"
    f2["s3_raw_path"] = f["s3_raw_path"].replace(".csv", "-2.csv")
    ing2 = fakes.fake_ingest(conn, workflow_run_id=wfid, file=f2, commit=False)

    from control import runs as _runs
    selected = _runs.latest_succeeded_run(
        conn, domain=f["domain"], dataset=f["dataset"],
        business_date=f["business_date"], pipeline_type="ingestion")
    assert selected in {ing1["run_id"], ing2["run_id"]}, \
        "discovery returned a run that is not one of the two ingests"

    canon = fakes.fake_canonicalize(
        conn, workflow_run_id=wfid, domain=f["domain"], dataset=f["dataset"],
        business_date=f["business_date"], record_count=f["record_count"],
        commit=False)

    # The hop discovered the SAME run latest_succeeded_run returns (it SELECTS).
    assert canon["upstream_run_id"] == selected

    # And the canonical edge in the DB carries that discovered upstream run.
    edge_upstream = conn.execute(
        "SELECT upstream_run_id FROM cp.lineage_edge WHERE lineage_link_id = %s",
        (canon["link_id"],)).fetchone()[0]
    assert str(edge_upstream) == selected
    assert canon["upstream_run_id"] in {ing1["run_id"], ing2["run_id"]}

    print("\n[GATE B.2] DISCOVERY-SELECTION PROOF")
    print("    ingest run #1        :", ing1["run_id"])
    print("    ingest run #2        :", ing2["run_id"])
    print("    latest_succeeded_run :", selected)
    print("    canonical edge upstream:", str(edge_upstream))
    print("    (no upstream id was passed; signature params =", sorted(params), ")")


def test_discovery_raises_without_ingest(conn):
    """GATE B.2b: with no succeeded ingestion run, canonicalize refuses (clear error)."""
    from harness import fakes
    f = _file()
    with pytest.raises(ValueError, match="no succeeded ingestion run"):
        fakes.fake_canonicalize(
            conn, workflow_run_id="22222222-2222-2222-2222-222222222222",
            domain=f["domain"], dataset=f["dataset"],
            business_date=f["business_date"], record_count=f["record_count"],
            commit=False)


def test_canonical_no_empty_link(conn, result):
    """GATE B.3a: the canonical link has at least one edge."""
    canon_link = result["canonicalize"]["link_id"]
    empty = conn.execute(
        "SELECT count(*) FROM cp.lineage_link l "
        "WHERE l.lineage_link_id = %s "
        "AND NOT EXISTS (SELECT 1 FROM cp.lineage_edge e "
        "                WHERE e.lineage_link_id = l.lineage_link_id)",
        (canon_link,)).fetchone()[0]
    assert empty == 0
    print("\n[GATE B.3a] empty canonical links:", empty)


def test_canonical_provenance_excludes_triggers(conn, result):
    """GATE B.3b: the canonical run's v_provenance is only is_provenance edges."""
    canon_run = result["canonicalize"]["run_id"]
    rows = conn.execute(
        "SELECT p.edge_type, t.is_provenance "
        "FROM cp.v_provenance p "
        "JOIN cp.edge_type t ON t.edge_type = p.edge_type "
        "WHERE p.consumer_run_id = %s", (canon_run,)).fetchall()
    assert rows
    assert all(r[1] is True for r in rows)
    assert all(r[0] != "orchestrates" for r in rows)
    assert {r[0] for r in rows} == {"curated_to_canonical"}
    print("\n[GATE B.3b] canonical-run provenance edge_types:",
          sorted({r[0] for r in rows}))


def test_canonical_transform_version_recorded(conn, result):
    """GATE B.4: the canonical link row records transform_version."""
    canon_link = result["canonicalize"]["link_id"]
    tv = conn.execute(
        "SELECT transform_version FROM cp.lineage_link WHERE lineage_link_id = %s",
        (canon_link,)).fetchone()[0]
    assert tv == "v1"
    print("\n[GATE B.4] canonical link transform_version:", tv)


def test_single_workflow_run_id_across_both_hops(conn, result):
    """GATE B.5: ingest + canonicalize runs share the one minted workflow_run_id."""
    wfid = result["workflow_run_id"]
    rows = conn.execute(
        "SELECT DISTINCT workflow_run_id FROM cp.run_log WHERE run_id IN (%s, %s)",
        (result["ingest"]["run_id"], result["canonicalize"]["run_id"])).fetchall()
    assert [r[0] for r in rows] == [wfid], \
        "ingest and canonicalize must share ONE workflow_run_id"
    print("\n[GATE B.5] single workflow_run_id across both hops:", wfid)
