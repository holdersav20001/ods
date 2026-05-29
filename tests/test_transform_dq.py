"""P4 item 7 — transform cast-to-NULL = quarantine (Lineage M1).

A canonicalize transform that would cast a value to NULL is a DATA-QUALITY
failure. Such rows must NOT be silently written as NULL — they must be routed to
the DLQ (a quarantine event visible in the lineage graph). fake_canonicalize
gains an optional `dq_failures` arg: with dq_failures=k it quarantines k rows
and canonicalizes only the (record_count - k) good ones; recon stays balanced as
good + dlq == source.

PROOF: the bad rows appear as a 'quarantine' link in cp.v_provenance, the
canonical link's record_count == good_count, and recon is balanced.

Run `pytest tests/test_transform_dq.py -v -s` for the printed evidence.
"""
import datetime
import uuid

from harness import fakes

BD = datetime.date(2026, 3, 3)


def _file(record_count=20):
    md5 = "md5-" + uuid.uuid4().hex
    return {
        "s3_raw_path": f"s3://raw/sales/orders/{md5}.csv",
        "file_md5": md5,
        "business_date": BD,
        "domain": "sales",
        "dataset": "orders",
        "record_count": record_count,
    }


def test_canonicalize_dq_failures_are_quarantined_not_nulled(conn):
    wfid = str(uuid.uuid4())
    f = _file(20)
    fakes.fake_ingest(conn, workflow_run_id=wfid, file=f, commit=False)

    # 4 rows would cast to NULL -> must be quarantined, not written as NULL.
    res = fakes.fake_canonicalize(
        conn, workflow_run_id=wfid, domain=f["domain"], dataset=f["dataset"],
        business_date=f["business_date"], record_count=f["record_count"],
        dq_failures=4, commit=False)

    good = res["good_count"]
    assert good == 16
    assert res["dq_failures"] == 4
    assert res["dlq_link_id"] is not None

    # Canonical link record_count == good_count (only good rows canonicalized).
    canon_rc = conn.execute(
        "SELECT record_count FROM cp.lineage_link WHERE lineage_link_id=%s",
        (res["link_id"],)).fetchone()[0]
    assert canon_rc == good, f"canonical link rc {canon_rc} != good {good}"

    # The bad rows are a 'quarantine' link visible in cp.v_provenance.
    q_edges = conn.execute(
        "SELECT edge_type FROM cp.lineage_edge WHERE lineage_link_id=%s",
        (res["dlq_link_id"],)).fetchall()
    assert q_edges and all(e[0] == "quarantine" for e in q_edges)
    in_prov = conn.execute(
        "SELECT count(*) FROM cp.v_provenance WHERE lineage_link_id=%s",
        (res["dlq_link_id"],)).fetchone()[0]
    assert in_prov > 0, "DQ-quarantined rows are NOT visible in v_provenance"

    # The dlq row carries the bad count.
    dlq_rc = conn.execute(
        "SELECT record_count FROM cp.dlq WHERE dlq_id=%s",
        (res["dlq_id"],)).fetchone()[0]
    assert dlq_rc == 4

    # Recon balanced: good + dlq == source.
    src, acc, disc, status, metrics = conn.execute(
        "SELECT source_count, accounted_count, discrepancy, status, metrics "
        "FROM cp.reconciliation_log WHERE run_id=%s", (res["run_id"],)).fetchone()
    assert src == 20
    assert acc == 20
    assert disc == 0 and status == "ok"
    assert metrics["good"] + metrics["dq_quarantined"] == 20

    print("\n[TRANSFORM-DQ] source=20 good(canonicalized)=", good,
          "dq_quarantined=", res["dq_failures"])
    print("    canonical link rc =", canon_rc, "(== good)")
    print("    quarantine link in v_provenance rows:", in_prov)
    print("    recon: source", src, "accounted", acc,
          "discrepancy", disc, "status", status)


def test_canonicalize_no_dq_is_unchanged(conn):
    """dq_failures=0 (default) keeps the original behaviour: no quarantine, the
    canonical link record_count == full record_count, recon balanced."""
    wfid = str(uuid.uuid4())
    f = _file(12)
    fakes.fake_ingest(conn, workflow_run_id=wfid, file=f, commit=False)
    res = fakes.fake_canonicalize(
        conn, workflow_run_id=wfid, domain=f["domain"], dataset=f["dataset"],
        business_date=f["business_date"], record_count=12, commit=False)
    assert res["dq_failures"] == 0
    assert res["dlq_link_id"] is None
    canon_rc = conn.execute(
        "SELECT record_count FROM cp.lineage_link WHERE lineage_link_id=%s",
        (res["link_id"],)).fetchone()[0]
    assert canon_rc == 12
    # No quarantine link for this run.
    q = conn.execute(
        "SELECT count(*) FROM cp.lineage_link "
        "WHERE consumer_run_id=%s AND edge_type='quarantine'",
        (res["run_id"],)).fetchone()[0]
    assert q == 0
