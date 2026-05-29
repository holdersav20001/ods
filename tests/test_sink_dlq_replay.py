"""GATE D evidence — the SINK hop (canonical_to_sink), DLQ, REPLAY, cycle guard.

The sink hop is the TERMINAL hop and the only one that writes target rows
(ods.orders) — and POSTGRES WRITE IS LAST: the link+edges are committed before
the rows via control.lineage.write_link_then_rows, and the FK on
_ods_lineage_link_id guarantees no target row exists without its link.

These tests drive harness composers (commit=False; the conn fixture rolls back)
and BOTH assert the GATE-D invariants AND print the evidence. Run with
`pytest tests/test_sink_dlq_replay.py -v -s` to capture the prints — that
printed output is the gate evidence.

GATE D checklist:
  1. Sink trace-to-raw: a produced ods.orders row's _ods_lineage_link_id traces
     via cp.v_provenance / trace_row.sql to the raw file (sink -> canonical ->
     curated -> raw). Zero orphan sink rows.
  2. Ordering / FK: every ods.orders row has a non-null _ods_lineage_link_id
     that exists in cp.lineage_link (no row without a committed link).
  3. Fan-out: sink the same canonical to postgres AND kafka; two
     canonical_to_sink links, each record_count == the canonical parent's; you
     can select "kafka rows" via join on sink_type.
  4. DLQ in graph: fake_fail's quarantine link/edge is reachable in
     cp.v_provenance; recon good+dlq == source (balanced).
  5. Replay traces to raw (X5): a replay run has trigger_type='replay' +
     replay_of_run_id set + a NEW workflow_run_id; a 'replay' edge links to the
     original; AND a replayed sink row traces to raw via its OWN re-written
     chain.
  6. No empty links; provenance excludes triggers (no orchestrates); one
     workflow_run_id per execution (original one id; replay a different id).
  7. Cycle guard: a deliberate 2-run provenance cycle does not hang the walk.
"""
import datetime
import pathlib
import uuid

import pytest

from control import dlq, lineage, runs
from harness import composers, fakes

BD = datetime.date(2025, 4, 9)

TRACE_SQL = (pathlib.Path(__file__).resolve().parents[1]
             / "control" / "queries" / "trace_row.sql").read_text()


@pytest.fixture
def committing_conn():
    """A connection that COMMITS (autocommit) for the replay X5 test, which needs
    distinct per-stage finished_at timestamps so discovery picks the newest
    ingest run deterministically. Cleans up ALL rows it could have created for
    the (sales/orders, BD) slice afterwards so no committed state leaks between
    tests (mirrors the rollback isolation of the `conn` fixture)."""
    from control.db import connect
    c = connect()
    c.autocommit = True

    def _cleanup():
        # Delete in FK-safe order, scoped to the slice this module uses.
        c.execute(
            "DELETE FROM ods.orders WHERE _ods_lineage_link_id IN ("
            "  SELECT lineage_link_id FROM cp.lineage_link l "
            "  JOIN cp.run_log r ON r.run_id = l.consumer_run_id "
            "  WHERE r.dataset='orders' AND r.domain='sales' AND r.business_date=%s)",
            (BD,))
        c.execute(
            "DELETE FROM cp.lineage_edge WHERE lineage_link_id IN ("
            "  SELECT lineage_link_id FROM cp.lineage_link l "
            "  JOIN cp.run_log r ON r.run_id = l.consumer_run_id "
            "  WHERE r.dataset='orders' AND r.domain='sales' AND r.business_date=%s)",
            (BD,))
        c.execute(
            "DELETE FROM cp.lineage_link WHERE consumer_run_id IN ("
            "  SELECT run_id FROM cp.run_log "
            "  WHERE dataset='orders' AND domain='sales' AND business_date=%s)",
            (BD,))
        c.execute(
            "DELETE FROM cp.reconciliation_log WHERE run_id IN ("
            "  SELECT run_id FROM cp.run_log "
            "  WHERE dataset='orders' AND domain='sales' AND business_date=%s)",
            (BD,))
        c.execute(
            "DELETE FROM cp.dlq WHERE run_id IN ("
            "  SELECT run_id FROM cp.run_log "
            "  WHERE dataset='orders' AND domain='sales' AND business_date=%s)",
            (BD,))
        c.execute(
            "DELETE FROM cp.run_stage_log WHERE run_id IN ("
            "  SELECT run_id FROM cp.run_log "
            "  WHERE dataset='orders' AND domain='sales' AND business_date=%s)",
            (BD,))
        # run_log: clear replay_of self-references first, then delete.
        c.execute(
            "UPDATE cp.run_log SET replay_of_run_id=NULL "
            "WHERE dataset='orders' AND domain='sales' AND business_date=%s",
            (BD,))
        c.execute(
            "DELETE FROM cp.run_log "
            "WHERE dataset='orders' AND domain='sales' AND business_date=%s",
            (BD,))
        c.execute(
            "DELETE FROM cp.file_catalogue WHERE business_date=%s "
            "AND domain='sales' AND dataset='orders'", (BD,))

    _cleanup()  # pre-clean in case a prior crashed run left rows
    try:
        yield c
    finally:
        _cleanup()
        c.close()


def _file(record_count=12):
    md5 = "md5-" + uuid.uuid4().hex
    return {
        "s3_raw_path": f"s3://raw/sales/orders/{md5}.csv",
        "file_md5": md5,
        "business_date": BD,
        "domain": "sales",
        "dataset": "orders",
        "record_count": record_count,
    }


# --------------------------------------------------------------------------- #
# GATE D.1 — sink trace-to-raw (3-hop: sink -> canonical -> curated -> raw)
# --------------------------------------------------------------------------- #
def test_sink_row_traces_to_raw(conn):
    f = _file(12)
    res = composers.run_to_sink(conn, file=f, commit=False)
    sink_link_id = res["sink"]["link_id"]

    # Take a produced ods.orders row and read its _ods_lineage_link_id.
    row = conn.execute(
        "SELECT row_id, _ods_lineage_link_id FROM ods.orders "
        "WHERE _ods_lineage_link_id=%s LIMIT 1", (sink_link_id,)).fetchone()
    assert row is not None, "no ods.orders row was written for the sink link"
    row_link_id = str(row[1])
    assert row_link_id == sink_link_id

    # Walk trace_row.sql from THAT row's link back to raw.
    chain = conn.execute(TRACE_SQL, {"link_id": row_link_id}).fetchall()
    edge_types = [c[1] for c in chain]
    raw_paths = [c[5] for c in chain if c[5] is not None]

    assert "canonical_to_sink" in edge_types
    assert "curated_to_canonical" in edge_types
    assert "raw_to_curated" in edge_types
    assert raw_paths, "sink row did NOT trace to a raw file (orphan!)"
    assert f["s3_raw_path"] in raw_paths

    print("\n[GATE D.1] SINK row traces to raw — chain for link", row_link_id)
    for c in chain:
        print("    hop", c[0], c[1], "consumer=", c[2], "upstream=", c[3],
              "raw=", c[5])


# --------------------------------------------------------------------------- #
# GATE D.2 — ordering / FK: no ods.orders row without a committed link
# --------------------------------------------------------------------------- #
def test_no_orphan_sink_rows(conn):
    f = _file(8)
    res = composers.run_to_sink(conn, file=f, commit=False)
    link_id = res["sink"]["link_id"]

    total = conn.execute(
        "SELECT count(*) FROM ods.orders WHERE _ods_lineage_link_id=%s",
        (link_id,)).fetchone()[0]
    assert total == f["record_count"]

    orphans = conn.execute(
        "SELECT count(*) FROM ods.orders o "
        "WHERE o._ods_lineage_link_id=%s "
        "AND NOT EXISTS (SELECT 1 FROM cp.lineage_link l "
        "                WHERE l.lineage_link_id = o._ods_lineage_link_id)",
        (link_id,)).fetchone()[0]
    assert orphans == 0

    nulls = conn.execute(
        "SELECT count(*) FROM ods.orders WHERE _ods_lineage_link_id IS NULL"
    ).fetchone()[0]
    assert nulls == 0

    print("\n[GATE D.2] sink rows:", total, "orphans:", orphans,
          "null-link rows:", nulls)


# --------------------------------------------------------------------------- #
# GATE D.3 — fan-out: same canonical to postgres AND kafka
# --------------------------------------------------------------------------- #
def test_fanout_two_sinks(conn):
    f = _file(15)
    res = composers.run_to_fanout_sinks(
        conn, file=f, sink_types=("postgres", "kafka"), commit=False)
    canonical_rc = f["record_count"]
    wfid = res["workflow_run_id"]

    links = conn.execute(
        "SELECT l.sink_type, l.record_count, l.lineage_link_id "
        "FROM cp.lineage_link l "
        "JOIN cp.run_log r ON r.run_id = l.consumer_run_id "
        "WHERE l.edge_type='canonical_to_sink' AND r.workflow_run_id=%s "
        "ORDER BY l.sink_type", (wfid,)).fetchall()

    sink_types = {row[0] for row in links}
    assert sink_types == {"postgres", "kafka"}, sink_types
    for st, rc, _lid in links:
        assert rc == canonical_rc, f"{st} link rc {rc} != canonical {canonical_rc}"

    # "rows that went to kafka" via join on the link's sink_type.
    kafka_rows = conn.execute(
        "SELECT count(*) FROM ods.orders o "
        "JOIN cp.lineage_link l ON l.lineage_link_id = o._ods_lineage_link_id "
        "JOIN cp.run_log r ON r.run_id = l.consumer_run_id "
        "WHERE l.sink_type='kafka' AND r.workflow_run_id=%s", (wfid,)).fetchone()[0]
    pg_rows = conn.execute(
        "SELECT count(*) FROM ods.orders o "
        "JOIN cp.lineage_link l ON l.lineage_link_id = o._ods_lineage_link_id "
        "JOIN cp.run_log r ON r.run_id = l.consumer_run_id "
        "WHERE l.sink_type='postgres' AND r.workflow_run_id=%s", (wfid,)).fetchone()[0]
    assert kafka_rows == canonical_rc
    assert pg_rows == canonical_rc

    print("\n[GATE D.3] FAN-OUT — canonical rc:", canonical_rc)
    for st, rc, lid in links:
        print("    sink_type", st, "link rc", rc)
    print("    kafka rows:", kafka_rows, "postgres rows:", pg_rows)


# --------------------------------------------------------------------------- #
# GATE D.4 — DLQ in graph + recon balanced
# --------------------------------------------------------------------------- #
def test_dlq_in_graph_and_recon(conn):
    f = _file(20)
    wfid = str(uuid.uuid4())
    # Need a real ingest run so the (corrected) downstream looks normal; but for
    # DLQ proof we just run fake_fail directly under its own workflow_run_id.
    res = fakes.fake_fail(
        conn, workflow_run_id=wfid, domain=f["domain"], dataset=f["dataset"],
        business_date=f["business_date"], good_count=17, bad_count=3,
        commit=False)
    run_id = res["run_id"]
    q_link_id = res["link_id"]

    # The quarantine link+edge exists and is a 'quarantine' edge.
    q_edges = conn.execute(
        "SELECT edge_type FROM cp.lineage_edge WHERE lineage_link_id=%s",
        (q_link_id,)).fetchall()
    assert q_edges and all(e[0] == "quarantine" for e in q_edges)

    # The quarantined rows are reachable in cp.v_provenance (DLQ visible).
    in_prov = conn.execute(
        "SELECT count(*) FROM cp.v_provenance WHERE lineage_link_id=%s",
        (q_link_id,)).fetchone()[0]
    assert in_prov > 0, "quarantine link is NOT in cp.v_provenance"

    # Recon: good + dlq == source, status ok (balanced).
    recon_row = conn.execute(
        "SELECT source_count, accounted_count, discrepancy, status, metrics "
        "FROM cp.reconciliation_log WHERE run_id=%s", (run_id,)).fetchone()
    source_count, accounted, disc, status, metrics = recon_row
    assert source_count == 20
    assert metrics["good"] + metrics["dlq"] == source_count == accounted
    assert disc == 0 and status == "ok"

    print("\n[GATE D.4] DLQ in graph — quarantine link", q_link_id,
          "in v_provenance rows:", in_prov)
    print("    recon: source", source_count, "good", metrics["good"],
          "dlq", metrics["dlq"], "discrepancy", disc, "status", status)


# --------------------------------------------------------------------------- #
# GATE D.5 — replay traces to raw via its OWN re-written chain (X5)
# --------------------------------------------------------------------------- #
def test_replay_traces_to_raw(committing_conn):
    """X5: a replayed sink row traces to raw via its OWN re-written chain.

    NOTE: this test COMMITS each stage in its own transaction (and cleans up
    afterwards) rather than using the rolled-back `conn` fixture. That is
    deliberate and production-faithful: discovery (latest_succeeded_run) orders
    by finished_at, which is stamped with now() — TRANSACTION start time. In
    production the original pipeline and the later replay commit in SEPARATE
    transactions, so the replay ingest run has a strictly newer finished_at and
    discovery unambiguously selects it. If we drove BOTH the original and the
    replay inside ONE uncommitted transaction (as the other gates do), every
    finished_at would collapse to the same now() and discovery would tiebreak on
    the random run_id UUID — non-deterministically anchoring the replay's
    re-canonicalize to the WRONG (original) ingest run. Committing per stage
    reproduces the real timing and makes the X5 property hold deterministically.
    """
    conn = committing_conn
    # Original execution: full pipeline through sink (committed).
    orig_file = _file(10)
    orig = composers.run_to_sink(conn, file=orig_file, commit=True)
    original_run_id = orig["canonicalize"]["run_id"]
    orig_wfid = orig["workflow_run_id"]

    # Replay with a CORRECTED file (new md5) under a NEW execution (committed).
    corrected = _file(10)
    rep = composers.replay_single_file(
        conn, original_run_id=original_run_id, file=corrected, commit=True)
    rep_wfid = rep["workflow_run_id"]
    replay_run_id = rep["replay_run_id"]

    # New workflow_run_id (different execution).
    assert rep_wfid != orig_wfid

    # Discovery MUST have anchored the replay canonical to the REPLAY ingest run,
    # not the original — the heart of the X5 property.
    assert rep["canonicalize"]["upstream_run_id"] == replay_run_id, (
        "replay re-canonicalize discovered the WRONG ingest run "
        f"({rep['canonicalize']['upstream_run_id']} != replay {replay_run_id})")

    # The replay run carries trigger_type='replay' + replay_of_run_id=original.
    trig, replay_of = conn.execute(
        "SELECT trigger_type, replay_of_run_id FROM cp.run_log WHERE run_id=%s",
        (replay_run_id,)).fetchone()
    assert trig == "replay"
    assert str(replay_of) == original_run_id

    # A 'replay' edge links the new chain to the ORIGINAL run.
    canon_link = rep["canonicalize"]["link_id"]
    replay_edges = conn.execute(
        "SELECT upstream_run_id FROM cp.lineage_edge "
        "WHERE lineage_link_id=%s AND edge_type='replay'",
        (canon_link,)).fetchall()
    assert replay_edges, "no 'replay' annotation edge on the replay canonical link"
    assert str(replay_edges[0][0]) == original_run_id

    # X5 PROOF: a replayed sink row traces to raw via its OWN re-written chain
    # (the CORRECTED file's raw path), NOT dead-ending at the original run.
    sink_link_id = rep["sink"]["link_id"]
    row = conn.execute(
        "SELECT _ods_lineage_link_id FROM ods.orders "
        "WHERE _ods_lineage_link_id=%s LIMIT 1", (sink_link_id,)).fetchone()
    assert row is not None, "replay produced no sink rows"

    chain = conn.execute(TRACE_SQL, {"link_id": sink_link_id}).fetchall()
    edge_types = [c[1] for c in chain]
    raw_paths = [c[5] for c in chain if c[5] is not None]

    assert "canonical_to_sink" in edge_types
    assert "curated_to_canonical" in edge_types
    assert "raw_to_curated" in edge_types
    assert corrected["s3_raw_path"] in raw_paths, \
        "replay sink row did NOT trace to the CORRECTED raw file via its own chain"
    # And it must NOT have dead-ended at the original file only.
    assert orig_file["s3_raw_path"] not in raw_paths or len(raw_paths) > 1

    print("\n[GATE D.5] REPLAY — orig wfid", orig_wfid, "replay wfid", rep_wfid)
    print("    replay run trigger:", trig, "replay_of:", replay_of)
    print("    discovered replay ingest upstream:",
          rep["canonicalize"]["upstream_run_id"], "== replay run", replay_run_id)
    print("    replay edge -> original run:", replay_edges[0][0])
    print("    corrected raw:", corrected["s3_raw_path"])
    print("    replayed sink row chain to raw:")
    for c in chain:
        print("        hop", c[0], c[1], "consumer=", c[2], "upstream=", c[3],
              "raw=", c[5])


# --------------------------------------------------------------------------- #
# GATE D.6 — structural invariants: no empty links; no orchestrates; one wfid
# --------------------------------------------------------------------------- #
def test_no_empty_links_no_triggers_one_wfid(conn):
    f = _file(7)
    res = composers.run_to_sink(conn, file=f, commit=False)
    wfid = res["workflow_run_id"]
    run_ids = [res[h]["run_id"] for h in ("ingest", "canonicalize", "sink")]

    # No empty links among this execution's links.
    empty = conn.execute(
        "SELECT count(*) FROM cp.lineage_link l "
        "WHERE l.consumer_run_id = ANY(%s) "
        "AND NOT EXISTS (SELECT 1 FROM cp.lineage_edge e "
        "                WHERE e.lineage_link_id = l.lineage_link_id)",
        (run_ids,)).fetchone()[0]
    assert empty == 0

    # provenance excludes triggers: no 'orchestrates' edge appears in v_provenance.
    orch = conn.execute(
        "SELECT count(*) FROM cp.v_provenance p "
        "JOIN cp.run_log r ON r.run_id = p.consumer_run_id "
        "WHERE r.workflow_run_id=%s AND p.edge_type='orchestrates'",
        (wfid,)).fetchone()[0]
    assert orch == 0

    # one workflow_run_id across the whole execution.
    wfids = conn.execute(
        "SELECT DISTINCT workflow_run_id FROM cp.run_log WHERE run_id = ANY(%s)",
        (run_ids,)).fetchall()
    assert len(wfids) == 1 and wfids[0][0] == wfid

    print("\n[GATE D.6] empty links:", empty, "orchestrates in prov:", orch,
          "distinct wfids:", len(wfids))


# --------------------------------------------------------------------------- #
# GATE D.7 — cycle guard: a deliberate 2-run provenance cycle TERMINATES
# --------------------------------------------------------------------------- #
def test_provenance_cycle_terminates(conn):
    """Build two links A and B whose edges reference each other's link as the
    upstream output (A's edge upstream_lineage_link_id=B, B's=A) — a 2-link
    provenance cycle (the v_provenance recursion is now link->link, so the cycle
    must close on upstream_lineage_link_id, the recursion/CYCLE key). The links
    are written via the client write_link; the second edge is then redirected to
    close the cycle with a raw UPDATE (the link ids only exist after the writes —
    a chicken-and-egg the client path can't express in one call). Assert SELECT
    FROM cp.v_provenance TERMINATES (the CYCLE clause stops the walk) rather than
    hanging / erroring under max recursion.
    """
    wfid = str(uuid.uuid4())
    run_a = runs.start(
        conn, workflow_run_id=wfid, pipeline_type="canonicalization",
        domain="sales", dataset="orders", business_date=BD,
        trigger_type="manual", commit=False)
    run_b = runs.start(
        conn, workflow_run_id=wfid, pipeline_type="canonicalization",
        domain="sales", dataset="orders", business_date=BD,
        trigger_type="manual", commit=False)

    # A's link: needs an upstream link to satisfy the CHECK before B exists.
    # Mint a throwaway raw_to_curated seed link and point A's edge at it; the
    # edge is redirected to B below to close the cycle.
    seed = lineage.write_link(
        conn, consumer_run_id=run_a, edge_type="raw_to_curated",
        target_ref={"path": "s3://cyc/seed", "content_hash": "cyc-seed", "version": 1},
        record_count=1,
        edges=[{"edge_type": "raw_to_curated", "source_ref": {"cyc": "seed"},
                "record_count": 1}],
        commit=False)
    link_a = lineage.write_link(
        conn, consumer_run_id=run_a, edge_type="curated_to_canonical",
        target_ref={"path": "s3://cyc/a", "content_hash": "cyc-a", "version": 1},
        record_count=1,
        edges=[{"upstream_run_id": run_b, "upstream_lineage_link_id": seed,
                "edge_type": "curated_to_canonical",
                "source_ref": {"cyc": "a->b"}, "record_count": 1}],
        commit=False)
    # B's link: edge upstream is A. Closes the cycle at the LINK level.
    link_b = lineage.write_link(
        conn, consumer_run_id=run_b, edge_type="curated_to_canonical",
        target_ref={"path": "s3://cyc/b", "content_hash": "cyc-b", "version": 1},
        record_count=1,
        edges=[{"upstream_run_id": run_a, "upstream_lineage_link_id": link_a,
                "edge_type": "curated_to_canonical",
                "source_ref": {"cyc": "b->a"}, "record_count": 1}],
        commit=False)
    # Redirect A's edge to point at B (now that link_b exists) — closes A<->B.
    conn.execute(
        "UPDATE cp.lineage_edge SET upstream_lineage_link_id=%s "
        "WHERE lineage_link_id=%s", (link_b, link_a))

    # If the CYCLE clause were absent this would loop forever / raise. With it,
    # the query returns a finite result set. Statement timeout as a safety net.
    conn.execute("SET LOCAL statement_timeout = '10s'")
    rows = conn.execute(
        "SELECT lineage_link_id, consumer_run_id, upstream_run_id, is_cycle "
        "FROM cp.v_provenance "
        "WHERE consumer_run_id = ANY(%s)", ([run_a, run_b],)).fetchall()

    assert rows, "expected the cyclic links to appear in the walk"
    cycle_marked = [r for r in rows if r[3]]
    assert cycle_marked, "CYCLE clause did not mark any row is_cycle=true"

    print("\n[GATE D.7] CYCLE GUARD — walk TERMINATED, rows:", len(rows),
          "is_cycle rows:", len(cycle_marked))
    print("    links:", link_a, link_b)
