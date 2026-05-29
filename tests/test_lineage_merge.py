"""GATE C evidence — lineage invariants for the MERGE hop (merge_to_canonical).

The merge hop is the 1:N lineage case: N upstream ingest runs fan into ONE
canonical link. The harness DISCOVERS the N upstreams (control.runs.succeeded_runs)
and is handed only synthetic per-slot row counts — never any upstream run id.

These tests drive harness.composers.run_multi_file (commit=False; the conn
fixture rolls back) and BOTH assert the GATE-C invariants AND print the gate
evidence. Run with `pytest tests/test_lineage_merge.py -v -s` to capture the
prints — that printed output is the gate evidence.

GATE C checklist (merge hop):
  1. N-edge merge: the merge link has exactly N edges, distinct input_slot,
     distinct non-null upstream_run_id (N different ingest runs), and
     SUM(edge.record_count) == link.record_count.
  2. Trace-to-raw: each merge edge traces (via cp.v_provenance / trace_row.sql)
     back through its ingest run to a raw file_catalogue row — all N raw files
     reached, zero orphan slots, no duplicate hops under fan-in.
  3. Per-slot recon: the merge reconciliation_log row's metrics.per_slot holds
     the N counts and they sum to the link record_count.
  4. No empty links; provenance excludes triggers; ONE workflow_run_id across
     all ingest + merge runs.
  5. Discovery proven: fake_merge's signature has NO upstream param (introspect);
     the merge edges' upstream_run_ids == exactly the set succeeded_runs returns.
"""
import datetime
import pathlib
import uuid

import pytest

from harness import composers

BD = datetime.date(2025, 3, 7)

TRACE_SQL = (pathlib.Path(__file__).resolve().parents[1]
             / "control" / "queries" / "trace_row.sql").read_text()

CD = ["hop", "edge_type", "consumer_run_id", "upstream_run_id",
      "source_file_id", "raw_s3_path"]


def _file(record_count):
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
def files():
    # 3 files, DISTINCT record counts so per-slot sums are unambiguous.
    return [_file(10), _file(20), _file(30)]


@pytest.fixture
def result(conn, files):
    """Run 3 ingests + the merge hop once (rolled back by the conn fixture)."""
    res = composers.run_multi_file(conn, files=files, commit=False)
    res["_files"] = files
    return res


# --- GATE C.1: N-edge merge --------------------------------------------------

def test_n_edge_merge_distinct_slots_and_upstreams_sum(conn, result):
    link_id = result["merge"]["link_id"]
    edges = conn.execute(
        "SELECT input_slot, upstream_run_id, edge_type, record_count "
        "FROM cp.lineage_edge WHERE lineage_link_id=%s ORDER BY input_slot",
        (link_id,)).fetchall()
    assert len(edges) == 3, "merge link must have exactly 3 edges (one per upstream)"

    slots = [e[0] for e in edges]
    upstreams = [e[1] for e in edges]
    assert slots == [0, 1, 2], f"input_slots must be distinct 0,1,2; got {slots}"
    assert all(u is not None for u in upstreams), "every merge edge needs an upstream run"
    assert len({str(u) for u in upstreams}) == 3, "upstream_run_ids must be 3 distinct runs"
    assert all(e[2] == "merge_to_canonical" for e in edges)

    link_rc = conn.execute(
        "SELECT record_count FROM cp.lineage_link WHERE lineage_link_id=%s",
        (link_id,)).fetchone()[0]
    edge_sum = sum(e[3] for e in edges)
    assert edge_sum == link_rc == 60, f"SUM(edge)={edge_sum} link={link_rc}"
    # the per-slot counts are exactly the files' counts (as a multiset)
    assert sorted(e[3] for e in edges) == sorted(f["record_count"] for f in result["_files"])

    print("\n[GATE C.1] N-edge merge link", link_id)
    for s, u, et, rc in edges:
        print(f"    slot={s} upstream_run={u} edge_type={et} record_count={rc}")
    print(f"    SUM(edge.record_count)={edge_sum} == link.record_count={link_rc}")


# --- GATE C.2: trace each slot to raw ---------------------------------------

def test_each_merge_edge_traces_to_raw_no_orphan_slots(conn, result):
    link_id = result["merge"]["link_id"]

    # Per-edge provenance via cp.v_provenance: each merge edge's upstream_run_id
    # is an ingest run; that ingest run's link anchors to a raw source_file_id.
    merge_edges = conn.execute(
        "SELECT input_slot, upstream_run_id FROM cp.lineage_edge "
        "WHERE lineage_link_id=%s ORDER BY input_slot", (link_id,)).fetchall()

    reached_raw = {}
    for slot, upstream in merge_edges:
        raw = conn.execute(
            "SELECT fc.s3_raw_path "
            "FROM cp.lineage_link l "
            "JOIN cp.lineage_edge e ON e.lineage_link_id=l.lineage_link_id "
            "JOIN cp.file_catalogue fc ON fc.file_id=e.source_file_id "
            "WHERE l.consumer_run_id=%s AND e.edge_type='raw_to_curated'",
            (upstream,)).fetchall()
        assert raw, f"slot {slot} (upstream {upstream}) reached NO raw file — orphan slot"
        reached_raw[slot] = [r[0] for r in raw]

    assert len(reached_raw) == 3, "not every slot reached a raw file"
    all_raw = {p for paths in reached_raw.values() for p in paths}
    expected_raw = {f["s3_raw_path"] for f in result["_files"]}
    assert all_raw == expected_raw, f"raw files reached {all_raw} != expected {expected_raw}"

    # trace_row.sql on the MERGE link: must show 3 DISTINCT raw paths, no dupes.
    rows = conn.execute(TRACE_SQL, {"link_id": link_id}).fetchall()
    chain = [dict(zip(CD, r)) for r in rows]
    assert chain, "trace_row.sql returned no chain for the merge link"
    assert chain == [dict(zip(CD, r)) for r in
                     dict.fromkeys(rows)], "trace_row.sql emitted duplicate hops"
    raw_in_chain = [c["raw_s3_path"] for c in chain if c["raw_s3_path"] is not None]
    assert len(raw_in_chain) == 3, f"expected 3 raw leaves, got {raw_in_chain}"
    assert set(raw_in_chain) == expected_raw
    edge_types = {c["edge_type"] for c in chain}
    assert "merge_to_canonical" in edge_types and "raw_to_curated" in edge_types

    print("\n[GATE C.2] each slot -> ingest run -> raw file")
    for slot in sorted(reached_raw):
        print(f"    slot {slot}: {reached_raw[slot]}")
    print("\n[GATE C.2] trace_row.sql reconstructed chain for merge link", link_id)
    for c in chain:
        print("   ", c)


# --- GATE C.3: per-slot recon ------------------------------------------------

def test_per_slot_recon_sums_to_link(conn, result):
    run_id = result["merge"]["run_id"]
    link_id = result["merge"]["link_id"]
    row = conn.execute(
        "SELECT source_count, accounted_count, status, metrics "
        "FROM cp.reconciliation_log WHERE run_id=%s AND check_type='merge'",
        (run_id,)).fetchone()
    assert row is not None, "no merge reconciliation_log row"
    source_count, accounted_count, status, metrics = row
    per_slot = metrics["per_slot"]
    assert set(per_slot.keys()) == {"0", "1", "2"}
    assert sum(per_slot.values()) == source_count == accounted_count
    assert status == "ok"

    link_rc = conn.execute(
        "SELECT record_count FROM cp.lineage_link WHERE lineage_link_id=%s",
        (link_id,)).fetchone()[0]
    assert sum(per_slot.values()) == link_rc

    print("\n[GATE C.3] recon metrics.per_slot:", per_slot,
          "sum=", sum(per_slot.values()), "link.record_count=", link_rc,
          "status=", status)


# --- GATE C.4: structural invariants ----------------------------------------

def test_no_empty_links_provenance_excludes_triggers(conn, result):
    link_id = result["merge"]["link_id"]
    run_id = result["merge"]["run_id"]

    empty = conn.execute(
        "SELECT count(*) FROM cp.lineage_link l WHERE l.lineage_link_id=%s "
        "AND NOT EXISTS (SELECT 1 FROM cp.lineage_edge e "
        "WHERE e.lineage_link_id=l.lineage_link_id)", (link_id,)).fetchone()[0]
    assert empty == 0

    prov = conn.execute(
        "SELECT p.edge_type, t.is_provenance FROM cp.v_provenance p "
        "JOIN cp.edge_type t ON t.edge_type=p.edge_type "
        "WHERE p.consumer_run_id=%s", (run_id,)).fetchall()
    assert prov
    assert all(r[1] is True for r in prov)
    assert all(r[0] != "orchestrates" for r in prov)

    print("\n[GATE C.4] empty merge links:", empty,
          "| merge-run provenance edge_types:", sorted({r[0] for r in prov}))


def test_single_workflow_run_id_across_all_runs(conn, result):
    wfid = result["workflow_run_id"]
    run_ids = [i["run_id"] for i in result["ingests"]] + [result["merge"]["run_id"]]
    distinct = conn.execute(
        "SELECT DISTINCT workflow_run_id FROM cp.run_log WHERE run_id = ANY(%s)",
        (run_ids,)).fetchall()
    assert [r[0] for r in distinct] == [wfid], \
        "all ingest + merge runs must share ONE workflow_run_id"
    print("\n[GATE C.4] single workflow_run_id across", len(run_ids),
          "runs:", wfid)


# --- GATE C.5: discovery proven ---------------------------------------------

def test_discovery_no_upstream_param_and_edges_match_succeeded_runs(conn, result):
    import inspect
    from harness import fakes
    from control import runs as _runs

    # STATIC PROOF: fake_merge's signature has NO upstream-run param.
    params = set(inspect.signature(fakes.fake_merge).parameters)
    forbidden = {"upstream_run_id", "upstream_run_ids", "upstream_ids",
                 "upstreams", "ingest_run_ids"}
    assert not (params & forbidden), (
        f"fake_merge must DISCOVER upstreams, not accept them: {params & forbidden}")

    # DYNAMIC PROOF: the merge edges' upstreams == exactly what succeeded_runs returns.
    f0 = result["_files"][0]
    discovered = set(_runs.succeeded_runs(
        conn, domain=f0["domain"], dataset=f0["dataset"],
        business_date=f0["business_date"], pipeline_type="ingestion"))
    edge_upstreams = {str(r[0]) for r in conn.execute(
        "SELECT upstream_run_id FROM cp.lineage_edge WHERE lineage_link_id=%s",
        (result["merge"]["link_id"],)).fetchall()}
    ingest_runs = {i["run_id"] for i in result["ingests"]}

    assert edge_upstreams == discovered, \
        f"merge edges {edge_upstreams} != succeeded_runs {discovered}"
    assert edge_upstreams == ingest_runs, \
        "merge edges must point at exactly the 3 ingest runs"
    assert set(result["merge"]["upstream_run_ids"]) == discovered

    print("\n[GATE C.5] DISCOVERY PROOF")
    print("    fake_merge signature params:", sorted(params))
    print("    succeeded_runs returned     :", discovered)
    print("    merge edge upstream_run_ids :", edge_upstreams)


def test_merge_raises_with_fewer_than_two_upstreams(conn):
    """A single ingest is not a merge — fake_merge refuses with a clear error."""
    from harness import fakes
    f = _file(5)
    wfid = str(uuid.uuid4())
    fakes.fake_ingest(conn, workflow_run_id=wfid, file=f, commit=False)
    with pytest.raises(ValueError, match="merge needs >=2"):
        fakes.fake_merge(
            conn, workflow_run_id=wfid, domain=f["domain"], dataset=f["dataset"],
            business_date=f["business_date"], slot_counts=[5], commit=False)
