"""A4 — adversarial audit: provenance correctness under stress.

INDEPENDENT auditor probes. Goal: produce an OVER-CLAIM (a trace pulls
ancestors it shouldn't) or an UNDER-REACH (a sink row can't trace to raw, or
recon misses a real discrepancy) that the shipped tests do not cover.

Requirements derived from the spec, NOT from harness output:
  * spec  docs/specs/2026-05-29-control-plane-design-v2.md
  * dec.  docs/reviews/2026-05-29-lineage-link-decision.md
    - C1: a lineage_link is the id of ONE output / ONE write-event, never a
      per-run id. "Once a single run can produce multiple outputs, anything
      that identifies an output by run_id (or by a key that omits output
      identity) loses or corrupts lineage."
    - Recon non-vacuous (H-recon): good+dlq==source -> ok; <source -> breach;
      >source -> double_count.
    - Trace-to-raw: every sink row walks v_provenance back to its raw file.
    - Merge H5: fake_merge accounts the caller-supplied slots; the merge link's
      SUM(edge.record_count)==link.record_count and recon per_slot sums to it.
    - DLQ decision 3: a quarantine edge is present so the provenance walk is
      rows-complete; good rows are counted separately from dlq.
    - Cycle guard (006/009): the recursive walk must terminate on a malformed
      cyclic edge, and must NOT truncate a legitimate deep chain.

METHOD
  * conn (rollback) fixture for non-committing probes.
  * committing_conn for scenarios that need cross-transaction visibility
    (refeed / replay / discovery ordering). EVERYTHING is namespaced
    audit_a4_* and torn down in a finally.
  * A finding is CONFIRMED with a failing/parametrised assertion or an inline
    query exhibiting the over-claim / under-reach. Where the system HOLDS, the
    probe asserts the holding property so it stays a regression guard.

NOTE ON SEVERITY MARKERS: probes that demonstrate a CONFIRMED DEFECT assert the
DEFECTIVE behaviour (so the suite stays green and documents the defect as a
characterization test). Each such probe is named ``*_DEFECT_*`` and prints the
over-claim / under-reach. See docs/reviews/2026-05-30-audit-a4.md.
"""
import os
import uuid

import pytest

from control import dlq, lineage, recon, runs, stages
from control.db import connect
from harness import composers, fakes

# ISOLATION: the committing probes discover their upstream run via
# latest_succeeded_run / succeeded_runs, which key on (domain, dataset,
# business_date, pipeline_type). A prior INTERRUPTED run (or any committed demo
# data) sharing that slice would be discovered as a stale "newest succeeded run"
# and silently anchor a replay/sink to the WRONG run — an intermittent flake on a
# re-run against a data-present DB. We close that window two ways:
#   (1) the namespace prefix is UNIQUE PER PYTEST SESSION (a short uuid tag), so no
#       committed demo/other-session/prior-interrupted data can EVER live in this
#       session's slice — discovery cannot collide; and
#   (2) the committing fixture PRE-CLEANS this session's namespace at setup (not
#       just in finally), so even a re-entrant slice starts empty.
# The unique tag is a prefix of the original "audit_a4" name, so the LIKE-scoped
# cleanup and every ``A4 + "_sX"`` per-test domain inherit the isolation for free.
A4 = "audit_a4_" + uuid.uuid4().hex[:8]
TRACE_SQL = open(
    os.path.join(os.path.dirname(__file__), "..", "control", "queries",
                 "trace_row.sql")).read()


# --------------------------------------------------------------------------- #
# committing fixture — like the X5 test's, but cleans up ALL audit_a4_* rows.
# --------------------------------------------------------------------------- #
@pytest.fixture
def cc():
    c = connect()
    c.autocommit = True
    _cleanup(c)  # PRE-clean: a prior interrupted run can't pollute discovery.
    try:
        yield c
    finally:
        _cleanup(c)
        c.close()


def _cleanup(c):
    """Delete every audit_a4_* artefact, children first. NEVER drops schema."""
    # sink rows (ods.orders) stamped by our workflow runs
    c.execute(
        "DELETE FROM ods.orders WHERE _ods_workflow_run_id IN "
        "(SELECT workflow_run_id FROM cp.run_log WHERE domain LIKE %s)",
        (A4 + "%",))
    runs_sub = "SELECT run_id FROM cp.run_log WHERE domain LIKE %s"
    links_sub = ("SELECT lineage_link_id FROM cp.lineage_link "
                 "WHERE consumer_run_id IN (" + runs_sub + ")")
    c.execute("DELETE FROM cp.lineage_edge WHERE lineage_link_id IN ("
              + links_sub + ")", (A4 + "%",))
    c.execute("DELETE FROM cp.lineage_link WHERE consumer_run_id IN ("
              + runs_sub + ")", (A4 + "%",))
    c.execute("DELETE FROM cp.reconciliation_log WHERE run_id IN ("
              + runs_sub + ")", (A4 + "%",))
    c.execute("DELETE FROM cp.dlq WHERE run_id IN (" + runs_sub + ")",
              (A4 + "%",))
    c.execute("DELETE FROM cp.run_stage_log WHERE run_id IN (" + runs_sub + ")",
              (A4 + "%",))
    c.execute("UPDATE cp.run_log SET replay_of_run_id=NULL WHERE domain LIKE %s",
              (A4 + "%",))
    c.execute("DELETE FROM cp.run_log WHERE domain LIKE %s", (A4 + "%",))
    c.execute("DELETE FROM cp.file_catalogue WHERE domain LIKE %s", (A4 + "%",))


def _settle_order(c, *, domain, dataset="orders", bd="2026-05-01"):
    """Backdate every EXISTING succeeded run in this slice by an hour so a run
    committed AFTERWARDS is unambiguously the newest.

    DISCOVERY DETERMINISM: latest_succeeded_run / succeeded_runs order
    ``finished_at DESC, run_id DESC``. finished_at is clock_timestamp() (007), so
    two runs committed in quick succession can tie at microsecond resolution under
    load — the ``run_id DESC`` (random uuid) tiebreak then picks NON-
    deterministically (the very fragility test_s2_refeed_discovery_ordering_
    fragility_NOTE proves is real). When a probe builds an ORIGINAL chain then a
    REPLAY chain in one slice, that tie can make the replay re-canonicalize/sink
    discover the ORIGINAL ingest/canon instead of the replay — an intermittent
    flake. Calling this BETWEEN the original and the replay makes the replay's
    runs strictly newest, so discovery is deterministic. No assertion's meaning
    changes; test-only data nudge (same technique _s2b uses); never touches
    production code."""
    c.execute(
        "UPDATE cp.run_log SET finished_at = finished_at - interval '1 hour' "
        "WHERE domain=%s AND dataset=%s AND business_date=%s "
        "AND status='succeeded' AND finished_at IS NOT NULL",
        (domain, dataset, bd))


def _file(n, *, domain, dataset="orders", bd="2026-05-01", tag="x"):
    return {"s3_raw_path": f"s3://raw/{domain}/{tag}", "file_md5": f"md5_{domain}_{tag}",
            "business_date": bd, "domain": domain, "dataset": dataset,
            "record_count": n}


def _trace_raw_paths(conn, link_id):
    """Run trace_row.sql for link_id; return the set of distinct raw_s3_path hit."""
    rows = conn.execute(TRACE_SQL, {"link_id": link_id}).fetchall()
    return {r[5] for r in rows if r[5] is not None}, rows


# ========================================================================= #
# SCENARIO 1 — multi-output run, SAME edge_type (the UNDOCUMENTED C1 variant)
#   The documented fan-out is two canonical_to_sink links distinguished by
#   sink_type. Here we stress a run that emits TWO raw_to_curated outputs
#   (same edge_type, NO sink_type to disambiguate). Per C1 each is a distinct
#   output; a downstream stage must be able to wire to the SPECIFIC one.
#   DEFECT: run_output_link keys only on (run_id, edge_type) and breaks ties on
#   created_at (= now(), identical within a txn) then a RANDOM lineage_link_id.
#   => downstream discovery is NON-DETERMINISTIC and cannot name the sibling.
# ========================================================================= #
def test_s1_multioutput_same_edgetype_FIXED_discovery_raises_then_addresses(conn):
    """FIXED (F1 / migration 010): a run emits TWO raw_to_curated outputs (same
    edge_type, distinct paths/hashes). run_output_link no longer random-picks one
    via a created_at tie + UUID sort. With no target_path it RAISES (ambiguous);
    each output is addressable by its target path.

    RED-was: returned the UUID-DESC winner, sibling un-nameable. GREEN-now:
    ambiguity RAISES; both outputs individually addressable by path."""
    import psycopg
    dom = A4 + "_s1"
    fa = runs.register_file(conn, s3_raw_path="s3://raw/A", file_md5=dom + "_A",
                            business_date="2026-05-01", domain=dom,
                            dataset="orders", commit=False)
    fb = runs.register_file(conn, s3_raw_path="s3://raw/B", file_md5=dom + "_B",
                            business_date="2026-05-01", domain=dom,
                            dataset="orders", commit=False)
    run = runs.start(conn, workflow_run_id=str(uuid.uuid4()),
                     pipeline_type="ingestion", domain=dom, dataset="orders",
                     business_date="2026-05-01", trigger_type="manual",
                     file_id=fa, commit=False)
    # TWO raw_to_curated outputs from ONE run, distinct files/paths/hashes.
    la = lineage.write_link(
        conn, consumer_run_id=run, edge_type="raw_to_curated",
        target_ref={"path": "s3://cur/A.parquet", "content_hash": "hA",
                    "version": 1},
        record_count=10,
        edges=[{"source_file_id": fa, "edge_type": "raw_to_curated",
                "source_ref": {"p": "A"}, "record_count": 10}], commit=False)
    lb = lineage.write_link(
        conn, consumer_run_id=run, edge_type="raw_to_curated",
        target_ref={"path": "s3://cur/B.parquet", "content_hash": "hB",
                    "version": 1},
        record_count=20,
        edges=[{"source_file_id": fb, "edge_type": "raw_to_curated",
                "source_ref": {"p": "B"}, "record_count": 20}], commit=False)
    runs.finalise(conn, run, status="succeeded", record_count_out=30,
                  commit=False)
    assert la != lb, "C1: two outputs of one run must be two distinct links"

    # created_at is identical for both (default now() = txn start, not clock) —
    # exactly the tie the old random-pick relied on; now it cannot decide an
    # output, so discovery RAISES instead of guessing.
    cas = conn.execute(
        "SELECT created_at FROM cp.lineage_link WHERE consumer_run_id=%s",
        (run,)).fetchall()
    assert cas[0][0] == cas[1][0], (
        "PRECONDITION: both links share created_at (now()) — the old tiebreak")

    # FIXED: run_output_link with no target_path RAISES (ambiguous) rather than
    # random-picking the UUID-DESC winner.
    conn.execute("SAVEPOINT s1_ambig")
    with pytest.raises(psycopg.errors.RaiseException, match="ambiguous"):
        runs.run_output_link(conn, run_id=run, edge_type="raw_to_curated")
    conn.execute("ROLLBACK TO SAVEPOINT s1_ambig")

    # Each output is addressable by its EXACT target path — no silent mis-wire.
    got_a = runs.run_output_link(conn, run_id=run, edge_type="raw_to_curated",
                                 target_path="s3://cur/A.parquet")
    got_b = runs.run_output_link(conn, run_id=run, edge_type="raw_to_curated",
                                 target_path="s3://cur/B.parquet")
    assert {got_a, got_b} == {la, lb}, (
        "both multi-output links must be addressable by path (C1/C3 fixed)")
    print("\n[S1 FIXED] run emitted 2 raw_to_curated outputs", la[:8], lb[:8],
          "| ambiguous discovery RAISES; each addressable by path")


def test_s1_downstream_traces_to_chosen_sibling_FIXED(conn):
    """FIXED (F1): wire a curated_to_canonical edge via run_output_link
    (as the real harness does) for a multi-output upstream. With output-identity
    discovery the consumer names the EXACT intended output by path, and the
    canonical link traces to that output's raw sibling — never the other.

    RED-was: discovery random-picked a sibling; the canonical could trace to the
    WRONG raw. GREEN-now: the consumer addresses the intended output by path and
    traces deterministically to its raw."""
    dom = A4 + "_s1b"
    fa = runs.register_file(conn, s3_raw_path="s3://raw/wantA", file_md5=dom + "_A",
                            business_date="2026-05-01", domain=dom,
                            dataset="orders", commit=False)
    fb = runs.register_file(conn, s3_raw_path="s3://raw/wantB", file_md5=dom + "_B",
                            business_date="2026-05-01", domain=dom,
                            dataset="orders", commit=False)
    ing = runs.start(conn, workflow_run_id=str(uuid.uuid4()),
                     pipeline_type="ingestion", domain=dom, dataset="orders",
                     business_date="2026-05-01", trigger_type="manual",
                     file_id=fa, commit=False)
    la = lineage.write_link(
        conn, consumer_run_id=ing, edge_type="raw_to_curated",
        target_ref={"path": "s3://cur/A", "content_hash": "hA", "version": 1},
        record_count=10,
        edges=[{"source_file_id": fa, "edge_type": "raw_to_curated",
                "source_ref": {}, "record_count": 10}], commit=False)
    lb = lineage.write_link(
        conn, consumer_run_id=ing, edge_type="raw_to_curated",
        target_ref={"path": "s3://cur/B", "content_hash": "hB", "version": 1},
        record_count=20,
        edges=[{"source_file_id": fb, "edge_type": "raw_to_curated",
                "source_ref": {}, "record_count": 20}], commit=False)
    runs.finalise(conn, ing, status="succeeded", record_count_out=30, commit=False)

    # The consumer INTENDS output A. It names A's EXACT output by target path,
    # not by a random pick — output-identity discovery (F1).
    picked = runs.run_output_link(conn, run_id=ing, edge_type="raw_to_curated",
                                  target_path="s3://cur/A")
    assert picked == la, "discovery must return the EXACT requested output"
    picked_raw = "s3://raw/wantA"

    can = runs.start(conn, workflow_run_id=str(uuid.uuid4()),
                     pipeline_type="canonicalization", domain=dom,
                     dataset="orders", business_date="2026-05-01",
                     trigger_type="manual", commit=False)
    can_link = lineage.write_link(
        conn, consumer_run_id=can, edge_type="curated_to_canonical",
        target_ref={"path": "s3://canon/AB", "content_hash": "cAB", "version": 1},
        record_count=10,
        edges=[{"upstream_run_id": ing, "upstream_lineage_link_id": picked,
                "edge_type": "curated_to_canonical", "source_ref": {},
                "record_count": 10}], commit=False)
    runs.finalise(conn, can, status="succeeded", commit=False)

    raws, _ = _trace_raw_paths(conn, can_link)
    # The canonical traces to EXACTLY the raw of the CHOSEN output; the sibling
    # is correctly excluded because the consumer named the precise upstream.
    assert raws == {picked_raw}, raws
    print("\n[S1b FIXED] canonical traced to", picked_raw,
          "(the chosen output, addressed by path) — deterministic, no mis-trace.")


# ========================================================================= #
# SCENARIO 2 — late-arriving / refeed under a NEW workflow_run_id.
#   Old sink rows must still trace to OLD raw; new to NEW (no cross-contam).
#   Probe with COMMITS so the two executions are time-ordered (the real case).
# ========================================================================= #
def test_s2_refeed_no_cross_contamination(cc):
    dom = A4 + "_s2"
    f1 = _file(10, domain=dom, tag="orig")
    r1 = composers.run_to_sink(cc, file=f1, sink_type="postgres", commit=True)
    orig_sink_link = r1["sink"]["link_id"]
    orig_raws, _ = _trace_raw_paths(cc, orig_sink_link)

    # Corrected file arrives later (different md5/path) -> replay under NEW wfid.
    f2 = _file(12, domain=dom, tag="fixed")
    f2["file_md5"] = dom + "_fixed_md5"
    f2["s3_raw_path"] = "s3://raw/" + dom + "/CORRECTED"
    _settle_order(cc, domain=dom)  # replay runs strictly newest -> deterministic
    rep = composers.replay_single_file(
        cc, original_run_id=r1["ingest"]["run_id"], file=f2, commit=True)

    # OLD sink link is immutable — still traces to the OLD raw only.
    orig_raws_after, _ = _trace_raw_paths(cc, orig_sink_link)
    assert orig_raws_after == orig_raws == {f1["s3_raw_path"]}, (
        "UNDER-REACH/contamination: old sink row no longer traces to old raw")

    # NEW replay chain traces to the CORRECTED raw, and a replay edge is present.
    rep_canon = rep["canonicalize"]["link_id"]
    rep_raws, _ = _trace_raw_paths(cc, rep_canon)
    assert f2["s3_raw_path"] in rep_raws, (
        "UNDER-REACH: replay chain does not reach the corrected raw")
    # Discovery picked the REPLAY ingest run (committed -> newer finished_at).
    assert rep["canonicalize"]["upstream_run_id"] == rep["replay_run_id"], (
        "discovery anchored replay canonical to the WRONG ingest run")
    print("\n[S2 PASS] old->{} new->{} (committed, time-ordered)".format(
        orig_raws_after, rep_raws))


def test_s2_refeed_discovery_ordering_tiebreak_FIXED(cc):
    """The no-contamination guarantee above relies on the corrected ingest being
    discovered as the latest. latest_succeeded_run / run_output_link order by
    finished_at DESC; if two ingests in the slice share finished_at
    (clock_timestamp ties, or an out-of-order/backfilled finished_at), the
    secondary key decides.

    HISTORY: this probe USED to document a fragility — the secondary key was a
    RANDOM run_id (uuid) DESC, so on a finished_at tie discovery was a coin-flip
    and a refeed could re-canonicalize a STALE ingest.

    FIXED (migration 028): the secondary key is now `seq DESC` (a monotonic
    BIGSERIAL insert-order key). On a finished_at tie the run CREATED LATER
    (higher seq) wins DETERMINISTICALLY — i.e. the corrected/newer ingest, not a
    uuid coin-flip. This probe now ASSERTS that fixed, deterministic behaviour."""
    dom = A4 + "_s2b"
    f1 = _file(10, domain=dom, tag="one")
    f2 = _file(20, domain=dom, tag="two")
    f2["file_md5"] = dom + "_two"
    i1 = fakes.fake_ingest(cc, workflow_run_id=str(uuid.uuid4()), file=f1,
                           commit=True)
    i2 = fakes.fake_ingest(cc, workflow_run_id=str(uuid.uuid4()), file=f2,
                           commit=True)
    # Force an EXACT finished_at tie (the hazard the clock normally hides).
    cc.execute("UPDATE cp.run_log SET finished_at = (SELECT finished_at FROM "
               "cp.run_log WHERE run_id=%s) WHERE run_id=%s", (i1["run_id"], i2["run_id"]))
    # The deterministic winner is the LATER-created run (higher seq) — i2, the
    # corrected/newer ingest — regardless of how the random uuids compare.
    seq1, seq2 = cc.execute(
        "SELECT (SELECT seq FROM cp.run_log WHERE run_id=%s),"
        "       (SELECT seq FROM cp.run_log WHERE run_id=%s)",
        (i1["run_id"], i2["run_id"]),
    ).fetchone()
    assert seq2 > seq1, "i2 (created later) must have the higher seq"
    disc = runs.latest_succeeded_run(cc, domain=dom, dataset="orders",
                                     business_date="2026-05-01",
                                     pipeline_type="ingestion")
    assert str(disc) == i2["run_id"], (
        "ORDERING FRAGILITY FIXED (028): on a finished_at tie, discovery now picks "
        "the higher-seq (later-created) run deterministically — the corrected/newer "
        "ingest — NOT a random run_id DESC coin-flip. Stale-ingest re-canonicalize "
        "is closed at the source.")
    print("\n[S2b FIXED] finished_at tie -> discovery picked", str(disc)[:8],
          "by seq DESC (deterministic, = later-created ingest)")


# ========================================================================= #
# SCENARIO 3 — double replay: idempotent counts, no over-claim / dup lineage.
# ========================================================================= #
def test_s3_double_replay_idempotent(cc):
    dom = A4 + "_s3"
    f0 = _file(10, domain=dom, tag="orig")
    base = composers.run_to_sink(cc, file=f0, sink_type="postgres", commit=True)
    orig_recon = cc.execute(
        "SELECT source_count, accounted_count, status FROM cp.reconciliation_log "
        "WHERE run_id=%s", (base["ingest"]["run_id"],)).fetchall()

    fc = _file(10, domain=dom, tag="fix")
    fc["file_md5"] = dom + "_fix"
    fc["s3_raw_path"] = "s3://raw/" + dom + "/FIX"

    _settle_order(cc, domain=dom)  # replay1 runs strictly newest -> deterministic
    rep1 = composers.replay_single_file(
        cc, original_run_id=base["ingest"]["run_id"], file=fc, commit=True)
    # SECOND replay of the SAME corrected file (idempotent re-drive). It mints a
    # fresh wfid+run but the curated/canonical content_hash is identical.
    _settle_order(cc, domain=dom)  # replay2 runs strictly newest -> deterministic
    rep2 = composers.replay_single_file(
        cc, original_run_id=base["ingest"]["run_id"], file=fc, commit=True)

    # Original recon UNCHANGED (QA H1).
    orig_recon_after = cc.execute(
        "SELECT source_count, accounted_count, status FROM cp.reconciliation_log "
        "WHERE run_id=%s", (base["ingest"]["run_id"],)).fetchall()
    assert orig_recon == orig_recon_after, "original recon mutated by replay"

    # Each replay canonical link traces to the corrected raw exactly once (no
    # duplicated hops / over-claim). DISTINCT in trace_row.sql must collapse.
    raws1, rows1 = _trace_raw_paths(cc, rep1["canonicalize"]["link_id"])
    raws2, rows2 = _trace_raw_paths(cc, rep2["canonicalize"]["link_id"])
    assert raws1 == raws2 == {fc["s3_raw_path"]}
    raw_hops1 = [r for r in rows1 if r[4] is not None]
    assert len(raw_hops1) == 1, ("double replay over-claims: >1 raw leaf on a "
                                 "single-file chain: %r" % (raw_hops1,))
    print("\n[S3 PASS] double replay idempotent; original recon stable; each "
          "chain reaches the corrected raw exactly once")


# ========================================================================= #
# SCENARIO 4 — concurrent writers, ONE workflow_run_id: no lost/dup link, and a
#   trace that sees BOTH branches. Two stages write links concurrently on the
#   same workflow_run_id from two connections.
# ========================================================================= #
def test_s4_concurrent_writers_one_workflow(cc):
    dom = A4 + "_s4"
    wfid = str(uuid.uuid4())
    f = _file(10, domain=dom, tag="seed")
    ing = fakes.fake_ingest(cc, workflow_run_id=wfid, file=f, commit=True)

    # Two concurrent canonicalize-shaped writers, distinct datasets/paths, both
    # under the SAME wfid, discovering the SAME ingest upstream.
    up_link = runs.run_output_link(cc, run_id=ing["run_id"],
                                   edge_type="raw_to_curated")
    c2 = connect(); c2.autocommit = False
    try:
        rA = runs.start(cc, workflow_run_id=wfid, pipeline_type="canonicalization",
                        domain=dom, dataset="orders", business_date="2026-05-01",
                        trigger_type="manual", commit=False)
        rB = runs.start(c2, workflow_run_id=wfid, pipeline_type="canonicalization",
                        domain=dom, dataset="orders", business_date="2026-05-01",
                        trigger_type="manual", commit=False)
        lA = lineage.write_link(
            cc, consumer_run_id=rA, edge_type="curated_to_canonical",
            target_ref={"path": "s3://canon/A", "content_hash": "cA",
                        "version": 1},
            record_count=10,
            edges=[{"upstream_run_id": ing["run_id"],
                    "upstream_lineage_link_id": up_link,
                    "edge_type": "curated_to_canonical", "source_ref": {},
                    "record_count": 10}], commit=False)
        lB = lineage.write_link(
            c2, consumer_run_id=rB, edge_type="curated_to_canonical",
            target_ref={"path": "s3://canon/B", "content_hash": "cB",
                        "version": 1},
            record_count=10,
            edges=[{"upstream_run_id": ing["run_id"],
                    "upstream_lineage_link_id": up_link,
                    "edge_type": "curated_to_canonical", "source_ref": {},
                    "record_count": 10}], commit=False)
        cc.commit(); c2.commit()
    finally:
        c2.rollback(); c2.close()

    # No link lost, none duplicated: exactly two distinct canonical links.
    n = cc.execute("SELECT count(DISTINCT lineage_link_id) FROM cp.lineage_link "
                   "WHERE consumer_run_id IN (%s,%s)" % (
                       "'" + rA + "'", "'" + rB + "'")).fetchone()[0]
    assert n == 2, "lost or duplicated a concurrent link"
    # Each branch independently traces to the seed raw.
    rawsA, _ = _trace_raw_paths(cc, lA)
    rawsB, _ = _trace_raw_paths(cc, lB)
    assert rawsA == rawsB == {f["s3_raw_path"]}
    print("\n[S4 PASS] two concurrent links under one wfid: both persisted, "
          "both trace to raw, none lost/dup")


# ========================================================================= #
# SCENARIO 5 — merge with a FAILED/partial upstream slot.
#   Only SUCCEEDED ingests become slots (H5 / succeeded_runs). The merge's own
#   per-slot recon stays correct. BUT the failed ingest's rows are accounted by
#   NOTHING — no recon anywhere flags them. We assert both halves.
# ========================================================================= #
def test_s5_merge_failed_slot_recon(cc):
    dom = A4 + "_s5"
    bd = "2026-05-02"
    fa = _file(10, domain=dom, dataset="ds_s5", bd=bd, tag="A")
    fb = _file(20, domain=dom, dataset="ds_s5", bd=bd, tag="B")
    fa["file_md5"] = dom + "_A"; fb["file_md5"] = dom + "_B"
    wf = str(uuid.uuid4())
    ia = fakes.fake_ingest(cc, workflow_run_id=wf, file=fa, commit=True)
    ib = fakes.fake_ingest(cc, workflow_run_id=wf, file=fb, commit=True)
    # Third file (30 rows) ARRIVES but its ingest FAILS — no link, status failed.
    fcat = runs.register_file(cc, s3_raw_path="s3://raw/" + dom + "/C",
                              file_md5=dom + "_C", business_date=bd, domain=dom,
                              dataset="ds_s5", commit=True)
    rc = runs.start(cc, workflow_run_id=wf, pipeline_type="ingestion", domain=dom,
                    dataset="ds_s5", business_date=bd, trigger_type="manual",
                    file_id=fcat, commit=True)
    runs.finalise(cc, rc, status="failed", commit=True)

    # PART A (HOLDS): merge discovers ONLY the 2 succeeded slots; per_slot recon
    # is internally correct (sum == link.record_count == source == accounted).
    m = fakes.fake_merge(cc, workflow_run_id=wf, domain=dom, dataset="ds_s5",
                         business_date=bd, slot_counts=[10, 20], commit=True)
    src, acc, status, metrics = cc.execute(
        "SELECT source_count, accounted_count, status, metrics FROM "
        "cp.reconciliation_log WHERE run_id=%s AND check_type='merge'",
        (m["run_id"],)).fetchone()
    per_slot = metrics["per_slot"]
    link_rc = cc.execute("SELECT record_count FROM cp.lineage_link WHERE "
                         "lineage_link_id=%s", (m["link_id"],)).fetchone()[0]
    assert sum(per_slot.values()) == src == acc == link_rc == 30
    assert status == "ok"
    edge_sum = cc.execute("SELECT COALESCE(SUM(record_count),0) FROM "
                          "cp.lineage_edge WHERE lineage_link_id=%s",
                          (m["link_id"],)).fetchone()[0]
    assert edge_sum == link_rc, "merge edges do not sum to link.record_count"
    n_slots = cc.execute("SELECT count(*) FROM cp.lineage_edge WHERE "
                         "lineage_link_id=%s", (m["link_id"],)).fetchone()[0]
    assert n_slots == 2, "merge created a slot for the FAILED ingest"

    # PART B (UNDER-REACH): the failed ingest's 30 rows are accounted by NO recon
    # row anywhere. There is no end-to-end recon that compares files-arrived
    # (60) to rows-reaching-canonical (30). recon misses a real discrepancy.
    failed_recon = cc.execute(
        "SELECT count(*) FROM cp.reconciliation_log WHERE run_id=%s", (rc,)
    ).fetchone()[0]
    assert failed_recon == 0, "unexpected recon for the failed run"
    # Sum of ALL recon source for this slice's merge == 30, but 60 rows arrived.
    print("\n[S5] merge per-slot recon HOLDS (2 succeeded slots, 30==30). "
          "UNDER-REACH: failed ingest's 30 rows are flagged by NO recon — "
          "files-arrived(60) vs canonical(30) is never reconciled.")


# ========================================================================= #
# SCENARIO 6 — DLQ + trace, with a FORCED imbalance (not the self-consistent
#   balanced fake). Quarantine link must be reachable in v_provenance AND
#   excluded from the good sink count; recon must BREACH when good+dlq<source.
# ========================================================================= #
def test_s6_dlq_reachable_and_recon_catches_imbalance(conn):
    dom = A4 + "_s6"
    f = _file(20, domain=dom, dataset="orders", tag="dq")
    wf = str(uuid.uuid4())
    # 20 source, 15 good, 3 quarantined -> good+dlq=18 < source=20: 2 rows LOST
    # without being quarantined. recon MUST breach (non-vacuous).
    run = runs.start(conn, workflow_run_id=wf, pipeline_type="canonicalization",
                     domain=dom, dataset="orders", business_date="2026-05-01",
                     trigger_type="manual", commit=False)
    dlq_id = dlq.quarantine(conn, run_id=run, stage="canonicalize",
                            reason="dq", source_ref={"n": "bad"},
                            payload_ref="s3://dlq/x", record_count=3,
                            commit=False)
    q_link = str(conn.execute("SELECT lineage_link_id FROM cp.lineage_link WHERE "
                              "consumer_run_id=%s AND edge_type='quarantine'",
                              (run,)).fetchone()[0])
    recon.write_check(conn, run_id=run, check_type="canonicalize",
                      source_count=20, accounted_count=18, commit=False)
    runs.finalise(conn, run, status="succeeded", commit=False)

    # (a) quarantine link is reachable in v_provenance (rows-complete).
    in_prov = conn.execute("SELECT count(*) FROM cp.v_provenance WHERE "
                           "lineage_link_id=%s", (q_link,)).fetchone()[0]
    assert in_prov > 0, "UNDER-REACH: quarantine link NOT in v_provenance"

    # (b) recon CATCHES the forced imbalance as a breach (good+dlq<source).
    src, acc, disc, status = conn.execute(
        "SELECT source_count, accounted_count, discrepancy, status FROM "
        "cp.reconciliation_log WHERE run_id=%s", (run,)).fetchone()
    assert (src, acc, disc, status) == (20, 18, 2, "breach"), (
        "recon FAILED to catch a real good+dlq<source imbalance")

    # (c) the quarantine edge is NOT a canonical_to_sink: it never inflates the
    # GOOD sink count. (No canonical_to_sink link exists for this run.)
    sink_links = conn.execute("SELECT count(*) FROM cp.lineage_link WHERE "
                              "consumer_run_id=%s AND edge_type='canonical_to_sink'",
                              (run,)).fetchone()[0]
    assert sink_links == 0
    print("\n[S6 PASS] dlq link in v_provenance; forced good+dlq<source -> "
          "BREACH (discrepancy=2); quarantine excluded from sink count")


def test_s6_double_count_detected(conn):
    """The >source side of H-recon: good+dlq>source -> double_count."""
    dom = A4 + "_s6b"
    run = runs.start(conn, workflow_run_id=str(uuid.uuid4()),
                     pipeline_type="canonicalization", domain=dom,
                     dataset="orders", business_date="2026-05-01",
                     trigger_type="manual", commit=False)
    recon.write_check(conn, run_id=run, check_type="canonicalize",
                      source_count=20, accounted_count=23, commit=False)
    status = conn.execute("SELECT status FROM cp.reconciliation_log WHERE "
                          "run_id=%s", (run,)).fetchone()[0]
    assert status == "double_count", status
    print("\n[S6b PASS] good+dlq>source -> double_count detected")


# ========================================================================= #
# SCENARIO 7 — cycle guard terminates a malformed replay edge AND a legit deep
#   chain (5+ hops) still fully traces (CYCLE clause must not truncate it).
# ========================================================================= #
def test_s7a_cycle_view_and_trace_row_both_terminate_FIXED(conn):
    """Forge a malformed cyclic upstream_lineage_link_id (link points to itself
    via a provenance edge).

    FIXED (F3 / A4-S7a): BOTH guarded walks now terminate on the cycle.
      * cp.v_provenance HOLDS: the 009 CYCLE clause (key=lineage_link_id) marks
        the revisited row is_cycle=true and terminates.
      * control/queries/trace_row.sql now has its OWN CYCLE clause
        (key=lineage_link_id) on its `WITH RECURSIVE chain`. The same malformed
        edge that previously HUNG the trace query now TERMINATES — the spec's
        "trace one ods row -> raw" query is cycle-safe.

    RED-was: trace_row.sql HUNG (QueryCanceled under statement_timeout).
    GREEN-now: trace_row.sql returns within a tight statement_timeout (a
    regression that re-hangs would trip the timeout -> the test fails)."""
    dom = A4 + "_s7a"
    fcat = runs.register_file(conn, s3_raw_path="s3://raw/s7a", file_md5=dom + "_f",
                              business_date="2026-05-01", domain=dom,
                              dataset="orders", commit=False)
    # A real raw->curated anchor link to satisfy the run-edge upstream CHECK.
    ing = runs.start(conn, workflow_run_id=str(uuid.uuid4()),
                     pipeline_type="ingestion", domain=dom, dataset="orders",
                     business_date="2026-05-01", trigger_type="manual",
                     file_id=fcat, commit=False)
    anchor = lineage.write_link(
        conn, consumer_run_id=ing, edge_type="raw_to_curated",
        target_ref={"path": "s3://cur/s7a", "content_hash": "s7a", "version": 1},
        record_count=1, edges=[{"source_file_id": fcat,
                                "edge_type": "raw_to_curated",
                                "source_ref": {}, "record_count": 1}],
        commit=False)
    runs.finalise(conn, ing, status="succeeded", commit=False)
    run = runs.start(conn, workflow_run_id=str(uuid.uuid4()),
                     pipeline_type="canonicalization", domain=dom,
                     dataset="orders", business_date="2026-05-01",
                     trigger_type="manual", commit=False)
    # Write a valid run-edge naming the anchor (passes the non-null CHECK)...
    link = lineage.write_link(
        conn, consumer_run_id=run, edge_type="curated_to_canonical",
        target_ref={"path": "s3://canon/self", "content_hash": "self",
                    "version": 1},
        record_count=1,
        edges=[{"upstream_run_id": ing, "upstream_lineage_link_id": anchor,
                "edge_type": "curated_to_canonical", "source_ref": {},
                "record_count": 1}], commit=False)
    # ...then FORGE the cycle: repoint the edge's upstream at its OWN link. The
    # CHECK only requires non-null, so a self-cycle is NOT prevented — only the
    # view's CYCLE clause stops the infinite walk.
    conn.execute("UPDATE cp.lineage_edge SET upstream_lineage_link_id=%s WHERE "
                 "lineage_link_id=%s", (link, link))
    runs.finalise(conn, run, status="succeeded", commit=False)

    # (a) HOLDS: cp.v_provenance terminates and flags the cycle row.
    conn.execute("SET LOCAL statement_timeout = '4000'")
    rows = conn.execute("SELECT lineage_link_id, edge_type, is_cycle FROM "
                        "cp.v_provenance WHERE lineage_link_id=%s",
                        (link,)).fetchall()
    assert rows, "v_provenance returned nothing for the cyclic link"
    assert any(r[2] for r in rows), "v_provenance did not flag the cycle"

    # (b) FIXED: trace_row.sql (the spec's trace query) now has its OWN CYCLE
    # guard and TERMINATES on the same forged edge. We run it under a TIGHT
    # statement_timeout in a savepoint: if a regression dropped the guard the
    # query would re-hang and trip the timeout (QueryCanceled), failing the test.
    # Termination = the query returns rows (no exception) inside the timeout.
    conn.execute("SAVEPOINT trace_probe")
    conn.execute("SET LOCAL statement_timeout = '4000'")
    rows = conn.execute(TRACE_SQL, {"link_id": link}).fetchall()
    conn.execute("ROLLBACK TO SAVEPOINT trace_probe")
    # A3 audit (2026-06-03): the prior assert was ``rows is not None`` — a no-op
    # (a .fetchall() cursor result is NEVER None, so it would pass even if the
    # CYCLE guard were dropped and the query timed out before this line — except
    # the timeout would raise first). Strengthened to pin what termination MEANS:
    #   * the guarded walk actually RAN (it emitted >=1 hop for the cyclic link),
    #   * and it TERMINATED at a BOUNDED hop count instead of looping. The forged
    #     self-cycle (link -> itself) admits exactly the start hop + the one
    #     revisit the CYCLE clause stops at, so the walk must be tiny (<=2 hops on
    #     the cyclic link). A regression that dropped the guard would either trip
    #     the 4s statement_timeout (QueryCanceled) above OR — if it somehow
    #     returned — emit an unbounded hop count, failing this assertion.
    assert rows, "trace_row.sql returned no hop rows for the cyclic link"
    cyclic_hops = [r for r in rows if str(r[2]) == str(run)]
    assert cyclic_hops, "trace did not even visit the cyclic link's consumer run"
    max_hop = max(r[0] for r in rows)
    assert max_hop <= 2, (
        "CYCLE guard failed to bound the self-cycle walk: max hop %s (a dropped "
        "guard loops until the statement_timeout)" % max_hop)
    print("\n[S7a FIXED] v_provenance cycle-safe (is_cycle flagged) AND "
          "trace_row.sql now has its own CYCLE guard — it TERMINATES on the "
          "same forged edge (returned %d hop rows, max hop %d, within the "
          "timeout)." % (len(rows), max_hop))


def test_s7b_deep_chain_5hops_fully_traces(conn):
    """A legitimate 5-hop chain (raw->curated->canonical->c2->c3->sink) must
    fully reach raw; the CYCLE clause must NOT truncate real lineage."""
    dom = A4 + "_s7b"
    fcat = runs.register_file(conn, s3_raw_path="s3://raw/deep",
                              file_md5=dom + "_deep", business_date="2026-05-01",
                              domain=dom, dataset="orders", commit=False)
    wf = str(uuid.uuid4())
    # hop1 raw_to_curated
    r1 = runs.start(conn, workflow_run_id=wf, pipeline_type="ingestion",
                    domain=dom, dataset="orders", business_date="2026-05-01",
                    trigger_type="manual", file_id=fcat, commit=False)
    l1 = lineage.write_link(conn, consumer_run_id=r1, edge_type="raw_to_curated",
                            target_ref={"path": "s3://c/1", "content_hash": "1",
                                        "version": 1},
                            record_count=5,
                            edges=[{"source_file_id": fcat,
                                    "edge_type": "raw_to_curated",
                                    "source_ref": {}, "record_count": 5}],
                            commit=False)
    runs.finalise(conn, r1, status="succeeded", commit=False)
    prev_run, prev_link = r1, l1
    # hops 2..5: chain curated_to_canonical edges (run-to-run, link-disambiguated)
    for hop in range(2, 6):
        rN = runs.start(conn, workflow_run_id=wf,
                        pipeline_type="canonicalization", domain=dom,
                        dataset="orders", business_date="2026-05-01",
                        trigger_type="manual", commit=False)
        lN = lineage.write_link(
            conn, consumer_run_id=rN, edge_type="curated_to_canonical",
            target_ref={"path": f"s3://c/{hop}", "content_hash": str(hop),
                        "version": 1},
            record_count=5,
            edges=[{"upstream_run_id": prev_run,
                    "upstream_lineage_link_id": prev_link,
                    "edge_type": "curated_to_canonical", "source_ref": {},
                    "record_count": 5}], commit=False)
        runs.finalise(conn, rN, status="succeeded", commit=False)
        prev_run, prev_link = rN, lN

    raws, rows = _trace_raw_paths(conn, prev_link)
    assert raws == {"s3://raw/deep"}, (
        "UNDER-REACH: deep 5-hop chain did NOT fully trace to raw: %r" % (raws,))
    max_hop = max(r[0] for r in rows)
    assert max_hop >= 5, ("CYCLE clause truncated a legit deep chain "
                          "(max hop %s < 5)" % max_hop)
    print("\n[S7b PASS] 5-hop chain fully traces to raw; max hop", max_hop)
