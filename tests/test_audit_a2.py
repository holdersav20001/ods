"""A2 adversarial audit — "passes for the wrong reason" probes.

INDEPENDENT auditor file. Each probe encodes a REQUIREMENT directly (the hardest
case the spec implies) rather than asserting on whatever shape the harness
happened to produce. Where a probe FAILS, it has surfaced a real hidden bug that
the existing green test suite could not catch (the test and the buggy code shared
a hidden assumption — same failure pattern as the original fan-out defect).

Findings doc: docs/reviews/2026-05-30-audit-a2.md
Requirements:  docs/specs/2026-05-29-control-plane-design-v2.md
               docs/reviews/2026-05-29-lineage-link-decision.md

Isolation: every probe uses the rolled-back `conn` fixture (commit=False). No
committed data; nothing to clean up. Schema is NEVER dropped.

Probe verdicts (expected at time of writing — 2026-05-30):
  * test_audit_merge_slot_count_binds_to_correct_upstream  -> FAILS (REAL BUG)
        fake_merge zips file-order slot_counts against newest-first discovery, so
        per-slot record_count is attributed to the WRONG upstream run. The merge
        total still balances, so test_lineage_merge.py (multiset/sum asserts only)
        stays green. This is the headline finding.
  * test_audit_merge_per_slot_matches_upstream_via_succeeded_runs_order -> FAILS
        Same root cause, expressed against the discovery primitive's documented
        newest-first order.
  * test_audit_discovery_picks_the_NEWEST_not_just_a_member -> PASSES (mechanism
        sound). Proves test_lineage_single.py::test_discovery_selects_latest_ingest
        is weak (asserts membership, not "newest") but the code is currently
        correct — a weak test, not a hidden bug.
  * test_audit_fanout_same_hash_two_links_requirement -> PASSES (mechanism sound,
        post-009). Proves test_sink_dlq_replay.py::test_fanout_two_sinks is weak
        (never asserts the SAME content_hash) but the hardened key holds.
"""
import datetime
import uuid

import pytest

from control import lineage, runs
from harness import composers, fakes


# =========================================================================== #
# PROBE 1 (HEADLINE) — MERGE per-slot count must bind to the CORRECT upstream.
#
# Requirement (spec C.1 / merge hop): the merge_to_canonical link has one edge
# per discovered upstream ingest run; each edge carries input_slot=i AND the
# record_count of THAT upstream's data. The existing test_lineage_merge.py only
# checks the multiset {edge.record_count} == {file.record_count} and the SUM, so
# it cannot detect a per-slot MISATTRIBUTION (slot i's count pinned to the wrong
# upstream run). We use DISTINCT per-file counts (10/20/30) and assert each merge
# edge's record_count equals the REAL ingested count of the upstream run that
# edge names (run_log.record_count_out) — the hardest case the requirement implies.
# =========================================================================== #
def test_audit_merge_slot_count_binds_to_correct_upstream(conn):
    BD = datetime.date(2025, 3, 7)

    def _file(rc):
        md5 = "audit_a2_md5_" + uuid.uuid4().hex
        return {
            "s3_raw_path": f"s3://raw/sales/orders/{md5}.csv",
            "file_md5": md5,
            "business_date": BD,
            "domain": "sales",
            "dataset": "orders",
            "record_count": rc,
        }

    files = [_file(10), _file(20), _file(30)]
    res = composers.run_multi_file(conn, files=files, commit=False)
    link_id = res["merge"]["link_id"]

    edges = conn.execute(
        "SELECT input_slot, upstream_run_id, record_count "
        "FROM cp.lineage_edge WHERE lineage_link_id=%s ORDER BY input_slot",
        (link_id,)).fetchall()
    assert len(edges) == 3

    # For each merge edge, the count it claims for its upstream must equal the
    # count that upstream run ACTUALLY ingested (its own record_count_out).
    mismatches = []
    for slot, upstream, edge_rc in edges:
        upstream_real = conn.execute(
            "SELECT record_count_out FROM cp.run_log WHERE run_id=%s",
            (upstream,)).fetchone()[0]
        if edge_rc != upstream_real:
            mismatches.append(
                f"slot {slot}: edge.record_count={edge_rc} but upstream run "
                f"{upstream} actually ingested {upstream_real}")

    assert not mismatches, (
        "MERGE PER-SLOT MISATTRIBUTION (real bug): fake_merge zips file-order "
        "slot_counts against newest-first succeeded_runs(), pinning each slot's "
        "count to the WRONG upstream run. The link total still balances so "
        "test_lineage_merge.py stays green.\n  " + "\n  ".join(mismatches))


# =========================================================================== #
# PROBE 2 — same defect via the discovery primitive's DOCUMENTED order.
#
# control.runs.succeeded_runs is documented "newest-first". fake_merge zips
# slot_counts[i] to upstreams[i]. So slot i's count is attributed to the i-th
# NEWEST run. Because ingests run oldest->newest with counts 10,20,30, the newest
# run is the 30-row file; slot 0 should therefore carry 30, not 10. Assert the
# per-slot binding the harness/spec contract actually implies.
# =========================================================================== #
def test_audit_merge_per_slot_matches_upstream_via_succeeded_runs_order(conn):
    BD = datetime.date(2025, 3, 8)

    def _file(rc):
        md5 = "audit_a2_md5_" + uuid.uuid4().hex
        return {
            "s3_raw_path": f"s3://raw/sales/orders/{md5}.csv",
            "file_md5": md5, "business_date": BD, "domain": "sales",
            "dataset": "orders", "record_count": rc,
        }

    files = [_file(10), _file(20), _file(30)]
    res = composers.run_multi_file(conn, files=files, commit=False)
    link_id = res["merge"]["link_id"]

    upstreams = runs.succeeded_runs(
        conn, domain="sales", dataset="orders", business_date=BD,
        pipeline_type="ingestion")  # newest-first per its docstring

    # Map upstream_run -> the count that run truly ingested.
    real_for = {
        up: conn.execute(
            "SELECT record_count_out FROM cp.run_log WHERE run_id=%s",
            (up,)).fetchone()[0]
        for up in upstreams
    }

    edges = {
        slot: (str(up), erc)
        for slot, up, erc in conn.execute(
            "SELECT input_slot, upstream_run_id, record_count "
            "FROM cp.lineage_edge WHERE lineage_link_id=%s", (link_id,)).fetchall()
    }

    bad = []
    for slot, (up, erc) in edges.items():
        if erc != real_for.get(up):
            bad.append(f"slot {slot}: edge_rc={erc} != upstream {up} real "
                       f"{real_for.get(up)}")
    assert not bad, (
        "merge edge record_count not bound to its named upstream's real count: "
        + "; ".join(bad))


# =========================================================================== #
# PROBE 3 — discovery must select the NEWEST run, not merely "a member".
#
# test_lineage_single.py::test_discovery_selects_latest_ingest_no_upstream_param
# claims (docstring) discovery returns "the newest" but only asserts
# `selected in {ing1, ing2}` — it would pass even if discovery returned the OLDER
# run. This probe pins the requirement: with two ingests, discovery MUST return
# the second (newer) one. (Expected PASS: mechanism is correct via 007
# clock_timestamp; the existing test is weak-but-currently-correct.)
# =========================================================================== #
def test_audit_discovery_picks_the_NEWEST_not_just_a_member(conn):
    BD = datetime.date(2025, 7, 2)

    def _file():
        md5 = "audit_a2_md5_" + uuid.uuid4().hex
        return {
            "s3_raw_path": f"s3://raw/sales/orders/{md5}.csv",
            "file_md5": md5, "business_date": BD, "domain": "sales",
            "dataset": "orders", "record_count": 7,
        }

    wfid = str(uuid.uuid4())
    ing1 = fakes.fake_ingest(conn, workflow_run_id=wfid, file=_file(), commit=False)
    ing2 = fakes.fake_ingest(conn, workflow_run_id=wfid, file=_file(), commit=False)
    assert ing1["run_id"] != ing2["run_id"]

    selected = runs.latest_succeeded_run(
        conn, domain="sales", dataset="orders", business_date=BD,
        pipeline_type="ingestion")
    assert selected == ing2["run_id"], (
        "discovery did NOT return the NEWEST ingest run. The existing test only "
        "asserts membership in {ing1,ing2}, so it could not catch an "
        "older-run-returned regression.")


# =========================================================================== #
# PROBE 4 — fan-out requirement: TWO sinks of the SAME canonical bytes => TWO
# links that SHARE one content_hash, disambiguated only by sink_type/path.
#
# test_sink_dlq_replay.py::test_fanout_two_sinks asserts two distinct sink_types
# and equal counts but NEVER that the two links share a content_hash — exactly the
# original wrong-reason fan-out gap. This probe pins the SAME-hash requirement.
# (Expected PASS post-009: the hardened key keys on sink_type+path+content_hash.)
# =========================================================================== #
def test_audit_fanout_same_hash_two_links_requirement(conn):
    BD = datetime.date(2026, 5, 30)
    md5 = "audit_a2_md5_" + uuid.uuid4().hex
    f = {
        "s3_raw_path": f"s3://raw/sales/orders/{md5}.csv",
        "file_md5": md5, "business_date": BD, "domain": "sales",
        "dataset": "orders", "record_count": 9,
    }
    res = composers.run_to_fanout_sinks(
        conn, file=f, sink_types=("postgres", "kafka"), commit=False)
    wfid = res["workflow_run_id"]

    rows = conn.execute(
        "SELECT l.sink_type, l.target_ref->>'content_hash' "
        "FROM cp.lineage_link l JOIN cp.run_log r ON r.run_id=l.consumer_run_id "
        "WHERE l.edge_type='canonical_to_sink' AND r.workflow_run_id=%s",
        (wfid,)).fetchall()

    assert {r[0] for r in rows} == {"postgres", "kafka"}
    assert len({r[1] for r in rows}) == 1, (
        "fan-out requirement breached: the two sink links of the SAME canonical "
        "bytes do NOT share one content_hash. test_fanout_two_sinks never checks "
        "this, so it would pass even if sink_type were baked back into the hash.")


# =========================================================================== #
# PROBE 5 (control) — prove the merge total still BALANCES despite probe 1/2.
#
# This documents WHY the existing tests stay green: the link total and recon are
# order-invariant (a sum), so the per-slot misattribution is invisible to any
# sum/multiset assertion. This probe is EXPECTED TO PASS even though the data is
# wrong — it is the smoking gun for "passes for the wrong reason".
# =========================================================================== #
def test_audit_merge_total_balances_even_when_slots_misattributed(conn):
    BD = datetime.date(2025, 3, 9)

    def _file(rc):
        md5 = "audit_a2_md5_" + uuid.uuid4().hex
        return {
            "s3_raw_path": f"s3://raw/sales/orders/{md5}.csv",
            "file_md5": md5, "business_date": BD, "domain": "sales",
            "dataset": "orders", "record_count": rc,
        }

    files = [_file(10), _file(20), _file(30)]
    res = composers.run_multi_file(conn, files=files, commit=False)
    run_id = res["merge"]["run_id"]
    link_id = res["merge"]["link_id"]

    link_rc = conn.execute(
        "SELECT record_count FROM cp.lineage_link WHERE lineage_link_id=%s",
        (link_id,)).fetchone()[0]
    src, acc, disc, status = conn.execute(
        "SELECT source_count, accounted_count, discrepancy, status "
        "FROM cp.reconciliation_log WHERE run_id=%s AND check_type='merge'",
        (run_id,)).fetchone()

    # Multiset of edge counts also matches — this is exactly what the existing
    # test checks, and it passes regardless of slot<->upstream binding.
    edge_counts = sorted(r[0] for r in conn.execute(
        "SELECT record_count FROM cp.lineage_edge WHERE lineage_link_id=%s",
        (link_id,)).fetchall())

    assert link_rc == 60
    assert (src, acc, disc, status) == (60, 60, 0, "ok")
    assert edge_counts == [10, 20, 30]  # multiset matches -> existing test green
