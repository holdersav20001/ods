"""R2 REFEED-ATTACKER — break lineage under REPLAY / REFEED (corrected/late data).

Mission (decision #5): a corrected/late/reprocessed file is a NEW execution —
the composer mints a NEW workflow_run_id, trigger_type='replay', and
replay_of_run_id pointing at the original run. The FULL provenance chain is
re-written PLUS a 'replay' annotation edge, so the refed row still traces to raw
(X5) AND you can navigate original -> correction -> what-it-superseded.

Posture: assume you CANNOT answer "where did this row come from, and what did it
correct?" until proven. Each probe is named:

  CONFIRMED_*  — a demonstrated DEFECT (the attack landed).
  SOUND_*      — the attack was attempted and the system held (negative result,
                 i.e. the promise is kept). Kept in-suite as a regression guard.

These run against the REAL client + ods_cp (schema 001-012). They use a COMMITTING
connection because replay discovery (cp.latest_succeeded_run) orders by
finished_at DESC, so the chain needs distinct per-stage commit timestamps for the
newest run to win deterministically. Every probe cleans up its own slice in a
finally block (FK-safe order) so NO committed state leaks. Namespace: p9r2_*.

NEVER drops schema. Audit only.
"""
import datetime
import pathlib
import uuid

import pytest

from control import lineage, runs
from harness import composers, fakes

TRACE_SQL = (pathlib.Path(__file__).resolve().parents[1]
             / "control" / "queries" / "trace_row.sql").read_text()

# Each probe uses its OWN (domain, dataset) pair so concurrent/sequential probes
# never share a slice, and cleanup is exact. business_date is fixed per probe.
BD = datetime.date(2025, 7, 14)


# --------------------------------------------------------------------------- #
# committing connection + per-slice cleanup
# --------------------------------------------------------------------------- #
def _cleanup_slice(c, *, domain, dataset):
    """Delete every row this module could have created for (domain, dataset),
    across all business_dates, in FK-safe order. Idempotent."""
    # ods.orders rows (the only target table the sink writes) — scope by the
    # consuming run's slice.
    c.execute(
        "DELETE FROM ods.orders WHERE _ods_lineage_link_id IN ("
        "  SELECT lineage_link_id FROM cp.lineage_link l "
        "  JOIN cp.run_log r ON r.run_id = l.consumer_run_id "
        "  WHERE r.domain=%s AND r.dataset=%s)", (domain, dataset))
    c.execute(
        "DELETE FROM cp.lineage_edge WHERE lineage_link_id IN ("
        "  SELECT lineage_link_id FROM cp.lineage_link l "
        "  JOIN cp.run_log r ON r.run_id = l.consumer_run_id "
        "  WHERE r.domain=%s AND r.dataset=%s)", (domain, dataset))
    c.execute(
        "DELETE FROM cp.lineage_link WHERE consumer_run_id IN ("
        "  SELECT run_id FROM cp.run_log WHERE domain=%s AND dataset=%s)",
        (domain, dataset))
    c.execute(
        "DELETE FROM cp.reconciliation_log WHERE run_id IN ("
        "  SELECT run_id FROM cp.run_log WHERE domain=%s AND dataset=%s)",
        (domain, dataset))
    c.execute(
        "DELETE FROM cp.dlq WHERE run_id IN ("
        "  SELECT run_id FROM cp.run_log WHERE domain=%s AND dataset=%s)",
        (domain, dataset))
    c.execute(
        "DELETE FROM cp.run_stage_log WHERE run_id IN ("
        "  SELECT run_id FROM cp.run_log WHERE domain=%s AND dataset=%s)",
        (domain, dataset))
    c.execute(
        "UPDATE cp.run_log SET replay_of_run_id=NULL "
        "WHERE domain=%s AND dataset=%s", (domain, dataset))
    c.execute("DELETE FROM cp.run_log WHERE domain=%s AND dataset=%s",
              (domain, dataset))
    c.execute("DELETE FROM cp.file_catalogue WHERE domain=%s AND dataset=%s",
              (domain, dataset))


@pytest.fixture
def cc():
    """Committing connection. Tests pick their own slice and clean it up via the
    returned helper in a finally block."""
    from control.db import connect
    c = connect()
    c.autocommit = True
    created = []

    def _slice(domain, dataset):
        created.append((domain, dataset))
        _cleanup_slice(c, domain=domain, dataset=dataset)  # pre-clean
        return domain, dataset

    try:
        yield c, _slice
    finally:
        for domain, dataset in created:
            _cleanup_slice(c, domain=domain, dataset=dataset)
        c.close()


def _file(domain, dataset, *, md5=None, record_count=10, bd=BD):
    md5 = md5 or ("md5-" + uuid.uuid4().hex)
    return {
        "s3_raw_path": f"s3://raw/{domain}/{dataset}/{md5}.csv",
        "file_md5": md5,
        "business_date": bd,
        "domain": domain,
        "dataset": dataset,
        "record_count": record_count,
    }


def _trace(conn, link_id):
    return conn.execute(TRACE_SQL, {"link_id": link_id}).fetchall()


def _raw_paths(chain):
    return [c[5] for c in chain if c[5] is not None]


def _edge_types(chain):
    return [c[1] for c in chain]


# --------------------------------------------------------------------------- #
# ATTACK 1 — the THREE things provenance must answer for a corrected row:
#   (a) where it came from (raw), (b) that it was a CORRECTION, (c) WHAT it
#   superseded (the original run's output). Walk all three from a replayed row.
# --------------------------------------------------------------------------- #
def test_SOUND_replayed_row_answers_origin_correction_and_superseded(cc):
    conn, mkslice = cc
    domain, dataset = mkslice("p9r2_a1", "orders")

    # Original chain to sink.
    orig_f = _file(domain, dataset, record_count=10)
    orig = composers.run_to_sink(conn, file=orig_f, commit=True)
    orig_canon_run = orig["canonicalize"]["run_id"]
    orig_ingest_run = orig["ingest"]["run_id"]

    # Corrected file (NEW md5) — replay the ingest run.
    corrected = _file(domain, dataset, record_count=10)
    rep = composers.replay_single_file(
        conn, original_run_id=orig_ingest_run, file=corrected, commit=True)
    rep_canon_link = rep["canonicalize"]["link_id"]
    rep_canon_run = rep["canonicalize"]["run_id"]
    rep_sink_link = rep["sink"]["link_id"]

    # (a) WHERE FROM: a replayed sink row traces to the CORRECTED raw file.
    chain = _trace(conn, rep_sink_link)
    raws = _raw_paths(chain)
    assert corrected["s3_raw_path"] in raws, (
        "replayed sink row does not trace to the corrected raw file")
    assert orig_f["s3_raw_path"] not in raws, (
        "replayed row leaks the OLD raw into its chain")

    # (b) IT WAS A CORRECTION: the replay canon run carries trigger_type='replay'
    # and replay_of_run_id -> the original ingest run; AND there is a 'replay'
    # provenance edge on the replay canonical link.
    tt, replay_of = conn.execute(
        "SELECT trigger_type, replay_of_run_id FROM cp.run_log WHERE run_id=%s",
        (rep_canon_run,)).fetchone()
    assert tt == "replay"
    assert str(replay_of) == orig_ingest_run

    replay_edges = conn.execute(
        "SELECT upstream_run_id FROM cp.lineage_edge "
        "WHERE lineage_link_id=%s AND edge_type='replay'",
        (rep_canon_link,)).fetchall()
    assert len(replay_edges) == 1, "no single 'replay' annotation edge found"
    superseded_run = str(replay_edges[0][0])

    # (c) WHAT IT SUPERSEDED: the 'replay' edge points at the ORIGINAL run, and
    # from there we can reach the original run's OWN output link + chain.
    assert superseded_run == orig_ingest_run, (
        "replay edge does not name the original run it superseded")

    # Can we navigate to what the ORIGINAL produced? The original sink link still
    # traces to the OLD raw (its superseded output is intact and reconstructable).
    orig_sink_link = orig["sink"]["link_id"]
    orig_chain = _trace(conn, orig_sink_link)
    assert orig_f["s3_raw_path"] in _raw_paths(orig_chain)

    print("\n[R2-A1] correction history navigable: replayed row -> corrected raw;"
          " replay edge -> original run", orig_ingest_run,
          "; original output still traces to old raw")


# --------------------------------------------------------------------------- #
# ATTACK 1b — CAN YOU RECONSTRUCT THE FULL CORRECTION HISTORY in ONE query?
#   The headline question: from a replayed sink ROW, can a consumer walk to the
#   list of runs in its correction lineage (orig <- replay)? trace_row.sql
#   does NOT recurse 'replay' edges (they carry no upstream_lineage_link_id), so
#   the row's trace-to-raw does NOT surface the superseded run. You must JOIN
#   run_log.replay_of_run_id separately. This probe documents whether the
#   correction history is reachable FROM THE ROW, or only via a side channel.
# --------------------------------------------------------------------------- #
def test_correction_history_reachability_from_a_replayed_row(cc):
    conn, mkslice = cc
    domain, dataset = mkslice("p9r2_a1b", "orders")

    orig_f = _file(domain, dataset, record_count=7)
    orig = composers.run_to_sink(conn, file=orig_f, commit=True)
    orig_ingest_run = orig["ingest"]["run_id"]

    corrected = _file(domain, dataset, record_count=7)
    rep = composers.replay_single_file(
        conn, original_run_id=orig_ingest_run, file=corrected, commit=True)
    rep_sink_link = rep["sink"]["link_id"]

    # From the produced row, get its link, then its consuming run's chain.
    row = conn.execute(
        "SELECT _ods_lineage_link_id FROM ods.orders WHERE _ods_lineage_link_id=%s"
        " LIMIT 1", (rep_sink_link,)).fetchone()
    assert row is not None
    chain = _trace(conn, rep_sink_link)

    # Collect every consumer/upstream run on the trace-to-raw chain.
    chain_runs = set()
    for hop in chain:
        if hop[2] is not None:
            chain_runs.add(str(hop[2]))
        if hop[3] is not None:
            chain_runs.add(str(hop[3]))

    # Does the trace-to-raw chain itself surface the SUPERSEDED original run?
    superseded_in_chain = orig_ingest_run in chain_runs

    # Side-channel reconstruction: walk replay_of_run_id transitively from any
    # 'replay' trigger run in the chain.
    replay_edge_runs = conn.execute(
        "SELECT DISTINCT e.upstream_run_id FROM cp.lineage_edge e "
        "JOIN cp.lineage_link l ON l.lineage_link_id = e.lineage_link_id "
        "JOIN cp.run_log r ON r.run_id = l.consumer_run_id "
        "WHERE e.edge_type='replay' AND r.domain=%s AND r.dataset=%s",
        (domain, dataset)).fetchall()
    history_via_replay_edge = {str(r[0]) for r in replay_edge_runs}

    print("\n[R2-A1b] superseded original on trace-to-raw chain:",
          superseded_in_chain,
          "| reachable via 'replay' edge side-channel:",
          orig_ingest_run in history_via_replay_edge)

    # SOUND requirement: the correction history MUST be reachable by SOME walk.
    # We assert the side-channel works (the 'replay' edge names the original).
    assert orig_ingest_run in history_via_replay_edge, (
        "CONFIRMED GAP: a replayed row's correction history is NOT reachable — "
        "neither the trace-to-raw chain nor the 'replay' edge surfaces the "
        "superseded original run")


# --------------------------------------------------------------------------- #
# ATTACK 2 — NO CROSS-CONTAMINATION. After a refeed with a DIFFERENT corrected
#   file (new md5), the OLD sink row still traces to the OLD raw, and the NEW to
#   the NEW. The lineage-isolation core promise.
# --------------------------------------------------------------------------- #
def test_SOUND_no_cross_contamination_old_traces_old_new_traces_new(cc):
    conn, mkslice = cc
    domain, dataset = mkslice("p9r2_a2", "orders")

    orig_f = _file(domain, dataset, record_count=9)
    orig = composers.run_to_sink(conn, file=orig_f, commit=True)
    orig_ingest_run = orig["ingest"]["run_id"]
    orig_sink_link = orig["sink"]["link_id"]

    corrected = _file(domain, dataset, record_count=9)  # DIFFERENT md5
    rep = composers.replay_single_file(
        conn, original_run_id=orig_ingest_run, file=corrected, commit=True)
    rep_sink_link = rep["sink"]["link_id"]

    old_raws = _raw_paths(_trace(conn, orig_sink_link))
    new_raws = _raw_paths(_trace(conn, rep_sink_link))

    # The OLD sink row traces to the OLD raw ONLY.
    assert orig_f["s3_raw_path"] in old_raws
    assert corrected["s3_raw_path"] not in old_raws, (
        "CONFIRMED: old sink row's chain pulled in the NEW corrected raw "
        "(cross-contamination)")
    # The NEW sink row traces to the NEW raw ONLY.
    assert corrected["s3_raw_path"] in new_raws
    assert orig_f["s3_raw_path"] not in new_raws, (
        "CONFIRMED: replayed sink row's chain pulled in the OLD raw "
        "(cross-contamination)")

    print("\n[R2-A2] isolation held: old->", old_raws, " new->", new_raws)


# --------------------------------------------------------------------------- #
# ATTACK 3 — refeed with the SAME md5 (no-op correction). register_file dedups
#   on (file_md5, business_date, domain, dataset). Does a replay of an UNCHANGED
#   file build a coherent NEW chain that still traces to that same raw, or a
#   confusing duplicate that traces ambiguously?
# --------------------------------------------------------------------------- #
def test_same_md5_replay_builds_coherent_chain_to_the_same_raw(cc):
    conn, mkslice = cc
    domain, dataset = mkslice("p9r2_a3", "orders")

    f = _file(domain, dataset, record_count=6)
    orig = composers.run_to_sink(conn, file=f, commit=True)
    orig_ingest_run = orig["ingest"]["run_id"]
    orig_file_id = orig["ingest"]["file_id"]

    # Replay the SAME file (same md5) — register_file must dedup to the same
    # file_id, and the new chain must still trace to that one raw.
    same = dict(f)  # same md5, same path, same slice
    rep = composers.replay_single_file(
        conn, original_run_id=orig_ingest_run, file=same, commit=True)
    rep_file_id = rep["ingest"]["file_id"]
    rep_sink_link = rep["sink"]["link_id"]

    assert rep_file_id == orig_file_id, (
        "register_file did NOT dedup the unchanged refeed — two file rows for "
        "the same md5/slice")

    chain = _trace(conn, rep_sink_link)
    raws = _raw_paths(chain)
    # Exactly one raw path, and it is the shared file.
    assert raws.count(f["s3_raw_path"]) >= 1
    assert set(raws) == {f["s3_raw_path"]}, (
        f"no-op replay produced an ambiguous/multi-raw chain: {raws}")

    # file_catalogue holds exactly one row for this md5/slice.
    n_files = conn.execute(
        "SELECT count(*) FROM cp.file_catalogue WHERE file_md5=%s AND "
        "business_date=%s AND domain=%s AND dataset=%s",
        (f["file_md5"], f["business_date"], domain, dataset)).fetchone()[0]
    assert n_files == 1

    print("\n[R2-A3] no-op refeed: shared file_id", orig_file_id,
          "single raw", set(raws))


# --------------------------------------------------------------------------- #
# ATTACK 4 — DOUBLE REPLAY / REPLAY-OF-REPLAY. Replay original O -> R1, then
#   replay R1 -> R2. Does replay_of_run_id form a navigable chain O <- R1 <- R2,
#   can R2's row trace to raw AND can we reconstruct the full correction history?
# --------------------------------------------------------------------------- #
def test_replay_of_replay_forms_navigable_correction_chain(cc):
    conn, mkslice = cc
    domain, dataset = mkslice("p9r2_a4", "orders")

    f0 = _file(domain, dataset, record_count=5)
    o = composers.run_to_sink(conn, file=f0, commit=True)
    o_ingest = o["ingest"]["run_id"]

    f1 = _file(domain, dataset, record_count=5)
    r1 = composers.replay_single_file(
        conn, original_run_id=o_ingest, file=f1, commit=True)
    r1_ingest = r1["ingest"]["run_id"]   # the replay ingest run (carries markers)

    f2 = _file(domain, dataset, record_count=5)
    r2 = composers.replay_single_file(
        conn, original_run_id=r1_ingest, file=f2, commit=True)
    r2_ingest = r2["ingest"]["run_id"]
    r2_sink_link = r2["sink"]["link_id"]

    # R2's row traces to raw via ITS OWN corrected file.
    raws = _raw_paths(_trace(conn, r2_sink_link))
    assert f2["s3_raw_path"] in raws
    assert f1["s3_raw_path"] not in raws
    assert f0["s3_raw_path"] not in raws

    # replay_of_run_id forms a navigable chain R2 -> R1 -> O. Walk it.
    history = []
    cur = r2_ingest
    seen = set()
    while cur is not None and cur not in seen:
        seen.add(cur)
        history.append(cur)
        row = conn.execute(
            "SELECT replay_of_run_id FROM cp.run_log WHERE run_id=%s",
            (cur,)).fetchone()
        cur = str(row[0]) if row and row[0] is not None else None

    assert r1_ingest in history, "R2 does not chain back to R1 via replay_of_run_id"
    assert o_ingest in history, (
        "CONFIRMED GAP: replay-of-replay does NOT chain back to the original O "
        f"(history reconstructed: {history})")
    assert history.index(r1_ingest) < history.index(o_ingest), (
        "correction history out of order")

    print("\n[R2-A4] correction chain O<-R1<-R2 navigable:", list(reversed(history)))


def test_double_replay_idempotent_counts_stable(cc):
    """Replay the SAME corrected file twice (two replays of O with the same
    corrected md5). Counts must stay stable — each replay is its own execution,
    so two replays = two new sink links each producing record_count rows; the
    ORIGINAL output is untouched. Documents whether 'replay twice identical'
    drifts the produced-row count for the slice."""
    conn, mkslice = cc
    domain, dataset = mkslice("p9r2_a4b", "orders")

    f0 = _file(domain, dataset, record_count=4)
    o = composers.run_to_sink(conn, file=f0, commit=True)
    o_ingest = o["ingest"]["run_id"]

    corrected_md5 = "md5-" + uuid.uuid4().hex
    fa = _file(domain, dataset, md5=corrected_md5, record_count=4)
    ra = composers.replay_single_file(
        conn, original_run_id=o_ingest, file=fa, commit=True)
    fb = _file(domain, dataset, md5=corrected_md5, record_count=4)
    rb = composers.replay_single_file(
        conn, original_run_id=o_ingest, file=fb, commit=True)

    # Each replay's sink link produced exactly record_count rows (no drift,
    # no double-count within a link).
    for r in (ra, rb):
        n = conn.execute(
            "SELECT count(*) FROM ods.orders WHERE _ods_lineage_link_id=%s",
            (r["sink"]["link_id"],)).fetchone()[0]
        assert n == 4, f"replay sink link produced {n} rows, expected 4"

    # Both replays deduped to ONE file row for the corrected md5 (idempotent
    # registration), and each still traces to that same corrected raw.
    n_files = conn.execute(
        "SELECT count(*) FROM cp.file_catalogue WHERE file_md5=%s AND domain=%s "
        "AND dataset=%s", (corrected_md5, domain, dataset)).fetchone()[0]
    assert n_files == 1
    assert fa["s3_raw_path"] in _raw_paths(_trace(conn, ra["sink"]["link_id"]))
    assert fb["s3_raw_path"] in _raw_paths(_trace(conn, rb["sink"]["link_id"]))

    print("\n[R2-A4b] double identical replay: counts stable (4 each), 1 file row")


# --------------------------------------------------------------------------- #
# ATTACK 5 — CODEX P1 CROSS-CHECK (refeed dual of the restart bug).
#   A refeed writes corrected canonical at the SAME path with a NEW content_hash.
#   Does cp.run_output_link(run, edge_type, path) (PATH-ONLY selector) return the
#   OLD link -> a downstream consumer wires to STALE content after a correction?
#
#   Two parts:
#   (5a) The DEFAULT harness: replay canonical writes to a DIFFERENT path
#        ('{bd}-replay.parquet'), so the path-only selector cannot collide.
#        Confirm the sink discovers the REPLAY canonical (newest), not the
#        original. (negative / SOUND for the default wiring.)
#   (5b) The ADVERSARIAL construction: force the corrected canonical onto the
#        SAME path as the original (with a NEW content_hash) under ONE run, then
#        probe run_output_link(run, edge_type, target_path). Because the selector
#        filters ONLY on (consumer_run_id, edge_type, path) and NOT content_hash,
#        does it conflate two outputs at one path? The hardened-target unique
#        index is on (consumer_run_id, edge_type, content_hash) — NOT path — so a
#        single run CAN hold two links at the same path with different hashes.
# --------------------------------------------------------------------------- #
def test_SOUND_5a_default_refeed_sink_discovers_replay_canonical_not_original(cc):
    conn, mkslice = cc
    domain, dataset = mkslice("p9r2_a5a", "orders")

    f = _file(domain, dataset, record_count=8)
    orig = composers.run_to_sink(conn, file=f, commit=True)
    orig_canon_run = orig["canonicalize"]["run_id"]
    orig_ingest = orig["ingest"]["run_id"]

    corrected = _file(domain, dataset, record_count=8)
    rep = composers.replay_single_file(
        conn, original_run_id=orig_ingest, file=corrected, commit=True)
    rep_canon_run = rep["canonicalize"]["run_id"]
    rep_sink_link = rep["sink"]["link_id"]

    # The replay sink's canonical_to_sink edge must name the REPLAY canon run's
    # output, not the original's.
    up_run = conn.execute(
        "SELECT upstream_run_id FROM cp.lineage_edge "
        "WHERE lineage_link_id=%s AND edge_type='canonical_to_sink'",
        (rep_sink_link,)).fetchone()[0]
    assert str(up_run) == rep_canon_run, (
        "replay sink wired to the ORIGINAL canonical run — stale content")
    assert str(up_run) != orig_canon_run

    # And the original and replay canonical links live at DIFFERENT paths, so the
    # path-only selector can never conflate them (the default wiring is safe).
    paths = conn.execute(
        "SELECT consumer_run_id, target_ref->>'path' FROM cp.lineage_link "
        "WHERE edge_type='curated_to_canonical' AND consumer_run_id IN (%s,%s)",
        (orig_canon_run, rep_canon_run)).fetchall()
    path_set = {p[1] for p in paths}
    assert len(path_set) == 2, f"replay reused the original canonical path: {path_set}"

    print("\n[R2-A5a] default refeed: distinct canonical paths", path_set,
          "; sink wired to replay canon", rep_canon_run)


def test_CONFIRMED_5b_run_output_link_path_selector_ignores_content_hash(cc):
    """ADVERSARIAL: cp.run_output_link(run, edge_type, path) filters on
    (consumer_run_id, edge_type, target_ref->>'path') and NOT content_hash. The
    hardened uniqueness index is on (consumer_run_id, edge_type, content_hash),
    so ONE run may legitimately hold TWO curated_to_canonical links at the SAME
    path with DIFFERENT content_hashes (an original write + an in-place
    correction). When a downstream stage names its upstream by PATH (as the
    harness does for fan-out via target_path), the selector cannot tell the
    corrected output from the stale one — it RAISES 'no rows'/ambiguity or
    returns a non-deterministic pick. This is the refeed dual of the Codex P1
    restart bug: path is NOT a content-identity, yet it is used as the
    input-side disambiguator.

    We construct the two-links-one-path-different-hash state directly through
    the SANCTIONED primitive (lineage.write_link) under one canon run, then probe
    the selector."""
    conn, mkslice = cc
    domain, dataset = mkslice("p9r2_a5b", "orders")

    f = _file(domain, dataset, record_count=3)
    res = composers.run_single_file(conn, file=f, commit=True)
    canon_run = res["canonicalize"]["run_id"]

    same_path = f"s3://canonical/{dataset}/{BD}-INPLACE.parquet"

    # Original canonical output at PATH p, content_hash H1 (a SECOND output of
    # this run at a NEW path — legal: differs from the run's first output by
    # path AND hash).
    link_h1 = lineage.write_link(
        conn,
        consumer_run_id=canon_run,
        edge_type="curated_to_canonical",
        target_ref={"path": same_path, "content_hash": "HASH-ORIGINAL", "version": 1},
        record_count=3,
        edges=[{
            "upstream_run_id": res["ingest"]["run_id"],
            "upstream_lineage_link_id": res["ingest"]["link_id"],
            "edge_type": "curated_to_canonical",
            "source_ref": {"note": "original at path"},
            "record_count": 3,
        }],
        commit=True)

    # In-place CORRECTION at the SAME path, NEW content_hash H2. The hardened
    # index (consumer_run_id, edge_type, content_hash) does NOT block this —
    # different hash => different index key => INSERT succeeds. Now ONE run holds
    # TWO curated_to_canonical links at one path.
    correction_ok = True
    err = None
    try:
        link_h2 = lineage.write_link(
            conn,
            consumer_run_id=canon_run,
            edge_type="curated_to_canonical",
            target_ref={"path": same_path, "content_hash": "HASH-CORRECTED",
                        "version": 2},
            record_count=3,
            edges=[{
                "upstream_run_id": res["ingest"]["run_id"],
                "upstream_lineage_link_id": res["ingest"]["link_id"],
                "edge_type": "curated_to_canonical",
                "source_ref": {"note": "correction at same path"},
                "record_count": 3,
            }],
            commit=True)
    except Exception as e:  # noqa: BLE001 — we are probing whether the DB blocks it
        correction_ok = False
        err = str(e)

    print("\n[R2-A5b] in-place same-path correction accepted:", correction_ok,
          "" if correction_ok else f"(blocked: {err[:120]})")

    if not correction_ok:
        # The DB blocked two-links-one-path — selector can never be ambiguous on
        # path. That would be the SOUND outcome; record it and stop.
        pytest.skip("DB blocked same-path/different-hash second link — selector "
                    "cannot be path-ambiguous (SOUND); see printed evidence")

    # Two links at one path now exist. Probe the PATH-ONLY selector.
    selector_raised = False
    selected = None
    try:
        selected = runs.run_output_link(
            conn, run_id=canon_run, edge_type="curated_to_canonical",
            target_path=same_path)
    except Exception as e:  # noqa: BLE001
        selector_raised = True
        sel_err = str(e)

    # Count how many links actually sit at that (run, edge_type, path).
    n_at_path = conn.execute(
        "SELECT count(*) FROM cp.lineage_link WHERE consumer_run_id=%s AND "
        "edge_type='curated_to_canonical' AND target_ref->>'path'=%s",
        (canon_run, same_path)).fetchone()[0]

    print(f"[R2-A5b] links at (run,edge,path)={n_at_path}; "
          f"selector raised={selector_raised}; selected={selected}; "
          f"H1={link_h1} H2={link_h2}")

    # CONFIRMED defect condition: TWO links share the path AND the path-only
    # selector silently returns ONE of them (no RAISE on ambiguity). A downstream
    # consumer naming its upstream by path gets a NON-DETERMINISTIC / possibly
    # STALE output after the correction.
    assert n_at_path == 2, "could not construct the two-links-one-path state"

    # This probe is a REGRESSION SENTINEL for a CONFIRMED defect: with the bug
    # present, the path-only selector does NOT raise and silently returns one of
    # the two links. We assert the BUGGY behaviour so the suite stays green while
    # documenting the hole — the assert FLIPS (forcing a fix-side update) the day
    # cp.run_output_link's path branch gains the same ambiguity guard the no-path
    # branch has (010 F1).
    assert not selector_raised, (
        "EXPECTED-FAIL FLIPPED: cp.run_output_link's PATH branch now raises on "
        "two-at-path — the refeed dual of Codex P1 appears FIXED. Update this "
        "sentinel to the SOUND assertion.")
    wired_hash = conn.execute(
        "SELECT target_ref->>'content_hash' FROM cp.lineage_link "
        "WHERE lineage_link_id=%s", (selected,)).fetchone()[0]
    print(
        "[R2-A5b] CONFIRMED (refeed dual of Codex P1): path-only selector "
        f"returned a single link ({selected}, hash={wired_hash}) while TWO links "
        "share that (run, edge_type, path) with different content_hashes — a "
        "downstream consumer naming its upstream by PATH can wire to STALE "
        "pre-correction content. The 010 F1 ambiguity guard is on the NO-PATH "
        "branch only; the path branch does NO count check.")


# --------------------------------------------------------------------------- #
# ATTACK 6 — DLQ DRAIN REFEED. A quarantined batch is later replayed. Does the
#   drained row now trace to raw via a proper chain, AND is its DLQ origin still
#   discoverable (the 'quarantine' edge)? Or does draining orphan it?
# --------------------------------------------------------------------------- #
def test_dlq_drain_refeed_traces_to_raw_and_keeps_quarantine_origin(cc):
    conn, mkslice = cc
    domain, dataset = mkslice("p9r2_a6", "orders")

    # 1. A failed ingest that quarantines a batch. fake_fail writes a run + a
    #    'quarantine' lineage link/edge (good rows pass, bad rows quarantined).
    fail = fakes.fake_fail(
        conn,
        workflow_run_id=str(uuid.uuid4()),
        domain=domain,
        dataset=dataset,
        business_date=BD,
        good_count=3,
        bad_count=2,
        commit=True,
    )
    failed_run = fail["run_id"]

    # The quarantine edge is in the graph and reachable in v_provenance.
    q = conn.execute(
        "SELECT count(*) FROM cp.lineage_link l "
        "JOIN cp.lineage_edge e ON e.lineage_link_id=l.lineage_link_id "
        "WHERE l.consumer_run_id=%s AND e.edge_type='quarantine'",
        (failed_run,)).fetchone()[0]
    assert q >= 1, "no quarantine edge recorded for the failed run"

    # 2. DRAIN: replay the quarantined batch as a corrected file. Same composer
    #    (dlq_drain is a replay flavour); the drained chain must reach raw.
    corrected = _file(domain, dataset, record_count=5)
    rep = composers.replay_single_file(
        conn, original_run_id=failed_run, file=corrected, commit=True)
    rep_sink_link = rep["sink"]["link_id"]

    # The drained/replayed row traces to raw via its OWN chain (not orphaned).
    raws = _raw_paths(_trace(conn, rep_sink_link))
    assert corrected["s3_raw_path"] in raws, (
        "CONFIRMED: drained (replayed) row does NOT trace to raw — orphaned")

    # The DLQ origin is still discoverable: the replay canon link's 'replay' edge
    # names the failed run, and that failed run still carries its quarantine edge.
    rep_canon_link = rep["canonicalize"]["link_id"]
    replay_to = conn.execute(
        "SELECT upstream_run_id FROM cp.lineage_edge "
        "WHERE lineage_link_id=%s AND edge_type='replay'",
        (rep_canon_link,)).fetchone()[0]
    assert str(replay_to) == failed_run, (
        "replay edge does not point back at the quarantined run — DLQ origin lost")

    still_quarantined = conn.execute(
        "SELECT count(*) FROM cp.lineage_link l "
        "JOIN cp.lineage_edge e ON e.lineage_link_id=l.lineage_link_id "
        "WHERE l.consumer_run_id=%s AND e.edge_type='quarantine'",
        (failed_run,)).fetchone()[0]
    assert still_quarantined >= 1, "draining erased the quarantine origin"

    print("\n[R2-A6] DLQ drain: drained row traces to raw", corrected["s3_raw_path"],
          "; quarantine origin preserved on run", failed_run)
