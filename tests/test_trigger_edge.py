"""P4 item 8 — orchestration trigger-edge (H-trigger): present in lineage_edge,
ABSENT from cp.v_provenance.

A workflow that triggers a run records that causal fact as an 'orchestrates'
lineage link via control.lineage.write_trigger. 'orchestrates' is
is_provenance=false in cp.edge_type, and cp.v_provenance joins edge_type ON
is_provenance — so the trigger edge is real audit history but it is
STRUCTURALLY excluded from trace-to-raw. This proves a trigger never pollutes
provenance.

Run `pytest tests/test_trigger_edge.py -v -s` for the printed proof.
"""
import datetime
import uuid

from control import lineage, runs

BD = datetime.date(2026, 2, 14)


def _start(conn):
    return runs.start(
        conn, workflow_run_id=str(uuid.uuid4()), pipeline_type="ingestion",
        domain="sales", dataset="orders", business_date=BD,
        trigger_type="manual", commit=False)


def test_trigger_edge_in_lineage_but_not_in_provenance(conn):
    triggered = _start(conn)
    link_id = lineage.write_trigger(
        conn, triggered_run_id=triggered, trigger_source="upstream_scheduler",
        commit=False)
    assert link_id is not None

    # (a) The orchestrates link + edge EXISTS in the lineage tables.
    link_row = conn.execute(
        "SELECT edge_type FROM cp.lineage_link WHERE lineage_link_id=%s",
        (link_id,)).fetchone()
    assert link_row is not None and link_row[0] == "orchestrates"

    edge_rows = conn.execute(
        "SELECT edge_type FROM cp.lineage_edge WHERE lineage_link_id=%s",
        (link_id,)).fetchall()
    assert edge_rows, "orchestrates link has no edge"
    assert all(e[0] == "orchestrates" for e in edge_rows)

    # (b) BUT it does NOT appear in cp.v_provenance (is_provenance=false).
    in_prov = conn.execute(
        "SELECT count(*) FROM cp.v_provenance WHERE lineage_link_id=%s",
        (link_id,)).fetchone()[0]
    assert in_prov == 0, "orchestrates edge LEAKED into v_provenance"

    # And it must be flagged non-provenance in the edge_type table.
    is_prov = conn.execute(
        "SELECT is_provenance FROM cp.edge_type WHERE edge_type='orchestrates'"
    ).fetchone()[0]
    assert is_prov is False

    print("\n[H-TRIGGER] orchestrates link", link_id,
          "edges in lineage_edge:", len(edge_rows),
          "| rows in v_provenance:", in_prov, "(excluded)")
    print("    edge_type.orchestrates.is_provenance =", is_prov)


def test_trigger_edge_idempotent(conn):
    """Triggering the SAME run twice is a no-op for links (content_hash =
    triggered run id), so we never accrue duplicate trigger edges."""
    triggered = _start(conn)
    a = lineage.write_trigger(
        conn, triggered_run_id=triggered, trigger_source="sched", commit=False)
    b = lineage.write_trigger(
        conn, triggered_run_id=triggered, trigger_source="sched", commit=False)
    assert a == b
    n_edges = conn.execute(
        "SELECT count(*) FROM cp.lineage_edge WHERE lineage_link_id=%s",
        (a,)).fetchone()[0]
    assert n_edges == 1
    print("\n[H-TRIGGER] idempotent: 2 triggers -> 1 link", a, "1 edge")
