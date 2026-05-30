"""A1 adversarial audit — identity / dedup / uniqueness / idempotency probes.

Author: INDEPENDENT auditor (A1 lens). These probes are derived from the SPEC
(docs/specs/2026-05-29-control-plane-design-v2.md) and the decision doc
(docs/reviews/2026-05-29-lineage-link-decision.md), NOT from the harness fakes.

Posture: assume MORE bugs of the P5 fan-out-dedup class exist. Each probe tries
to make an identity key COLLAPSE two distinct things, or OVER-CLAIM / mis-wire.

A probe named ``probe_CONFIRMED_*`` asserts the buggy behaviour we actually
observed (so it PASSES and pins the defect in place as evidence). A probe named
``probe_SOUND_*`` tries hard to break a mechanism and fails to — evidence the
mechanism is sound. Read the docstring of each for the spec clause + verdict.

Isolation: uses the ``conn`` rollback fixture. Nothing commits. We call the SQL
primitives directly (cp.write_lineage_link etc.) rather than the harness, because
the harness bakes in assumptions (e.g. it never sets target_ref->>'path' on
non-sink links) that hide the very defects we are hunting.
"""
import uuid

import pytest


# --------------------------------------------------------------------------- #
# helpers — talk to cp.* directly, derive data from the spec not the fakes
# --------------------------------------------------------------------------- #
def _mk_run(conn, *, pipeline_type="canonicalization", dataset="audit_a1_ds",
            domain="audit_a1_dom", business_date="2026-05-29",
            workflow_run_id=None, trigger_type="manual"):
    wf = workflow_run_id or f"audit_a1_wf_{uuid.uuid4()}"
    return conn.execute(
        "SELECT cp.start_run(%s,%s,%s,%s,%s,%s)",
        [wf, pipeline_type, domain, dataset, business_date, trigger_type],
    ).fetchone()[0]


def _finish(conn, run_id, status="succeeded"):
    conn.execute("SELECT cp.patch_run(%s, %s::jsonb)",
                 [run_id, f'{{"status":"{status}"}}'])


def _write_link(conn, run_id, edge_type, target_ref, *, edges, sink_type=None,
                transform_version=None, record_count=1):
    import json
    return conn.execute(
        "SELECT cp.write_lineage_link(%s,%s,%s::jsonb,%s,%s::jsonb,%s,%s)",
        [run_id, edge_type, json.dumps(target_ref), record_count,
         json.dumps(edges), sink_type, transform_version],
    ).fetchone()[0]


def _file_edge(file_id):
    return [{"source_file_id": str(file_id), "edge_type": "raw_to_curated",
             "record_count": 1}]


def _register_file(conn, md5, *, business_date="2026-05-29",
                   dataset="audit_a1_ds", domain="audit_a1_dom",
                   path=None):
    return conn.execute(
        "SELECT cp.register_file(%s,%s,%s,%s,%s)",
        [path or f"s3://raw/{md5}", md5, business_date, domain, dataset],
    ).fetchone()[0]


# =========================================================================== #
# PROBE 1 — run_output_link silently mis-wires a MULTI-OUTPUT upstream.
#
# Spec C1 invariant: "a run that produces K outputs mints K links; run_id is
# NEVER the implied output id." Spec C3: a run-to-run edge must name the EXACT
# upstream output via upstream_lineage_link_id.
#
# cp.run_output_link(run_id, edge_type) returns exactly ONE link (ORDER BY
# created_at DESC, lineage_link_id DESC LIMIT 1). The harness uses its return
# value verbatim as the edge's upstream_lineage_link_id (fakes.py:140,276,367).
# So when an upstream run legitimately has TWO links of the same edge_type, the
# consumer's edge is wired to ONE of them, arbitrarily — re-introducing the very
# input-side over-claim/mis-claim C3 was meant to kill.
# =========================================================================== #
def test_probe_FIXED_run_output_link_multioutput_raises_then_disambiguates(conn):
    """FIXED (F1 / migration 010): a run produces TWO distinct raw_to_curated
    outputs (two paths/hashes -> two links, exactly as C1 permits). The old
    cp.run_output_link(run, edge_type) random-picked ONE via a created_at tie
    broken by a UUID sort — the other output was un-nameable. Now discovery keys
    on OUTPUT IDENTITY:
      * with no target_path it RAISES (ambiguous) instead of random-picking;
      * with a target_path it returns the EXACT matching output.

    RED-was: returned one arbitrary link, the sibling unreachable. GREEN-now:
    ambiguity RAISES, and BOTH outputs are individually addressable by path."""
    import psycopg
    up_run = _mk_run(conn, pipeline_type="ingestion")
    f = _register_file(conn, "audit_a1_md5_p1")
    link_a = _write_link(
        conn, up_run, "raw_to_curated",
        {"path": "s3://curated/a.parquet", "content_hash": "audit_a1-A", "version": 1},
        edges=_file_edge(f))
    link_b = _write_link(
        conn, up_run, "raw_to_curated",
        {"path": "s3://curated/b.parquet", "content_hash": "audit_a1-B", "version": 1},
        edges=_file_edge(f))
    assert link_a != link_b, "precondition: C1 minted two distinct links"
    _finish(conn, up_run)

    # (a) ambiguous discovery (no path) RAISES instead of silently picking one.
    conn.execute("SAVEPOINT ambig")
    with pytest.raises(psycopg.errors.RaiseException, match="ambiguous"):
        conn.execute("SELECT cp.run_output_link(%s,%s)",
                     [up_run, "raw_to_curated"]).fetchone()
    conn.execute("ROLLBACK TO SAVEPOINT ambig")

    # (b) BOTH outputs are individually addressable by their target path — the
    # sibling is no longer un-nameable.
    got_a = str(conn.execute(
        "SELECT cp.run_output_link(%s,%s,%s)",
        [up_run, "raw_to_curated", "s3://curated/a.parquet"]).fetchone()[0])
    got_b = str(conn.execute(
        "SELECT cp.run_output_link(%s,%s,%s)",
        [up_run, "raw_to_curated", "s3://curated/b.parquet"]).fetchone()[0])
    assert got_a == str(link_a)
    assert got_b == str(link_b)
    assert {got_a, got_b} == {str(link_a), str(link_b)}, (
        "both multi-output links must be addressable by path (C1/C3 satisfied)")


def test_probe_CONFIRMED_run_output_link_tiebreak_is_nondeterministic(conn):
    """All links written in one transaction share created_at (now() is
    txn-stable, verified: now()!=clock_timestamp()). So run_output_link's
    'ORDER BY created_at DESC' is a TIE for same-run multi-output, broken only by
    'lineage_link_id DESC' — a random gen_random_uuid(). Which output a sink
    wires to is therefore an accident of UUID sort order, not of intent. We pin
    the txn-stable-clock precondition that makes this non-deterministic."""
    stable = conn.execute("SELECT now() <> clock_timestamp()").fetchone()[0]
    assert stable, (
        "now() is txn-stable; multi-output links in one txn tie on created_at, "
        "so run_output_link's winner is decided by random UUID order")


# =========================================================================== #
# PROBE 2 — COALESCE('') folds two DISTINCT non-sink outputs into ONE link.
#
# Decision C2 / migration 009: the key is
#   (consumer_run_id, edge_type, COALESCE(sink_type,''),
#    COALESCE(target_ref->>'path',''), COALESCE(target_ref->>'content_hash',''))
# Spec C1: K distinct outputs of one run => K links.
#
# The harness NEVER relies on 'path' to disambiguate non-sink links (it always
# sets a distinct content_hash). But the SPEC permits identity by path OR hash.
# If a real producer writes two outputs that differ ONLY by path (e.g. two
# canonical partitions, same logical content_hash placeholder, or content_hash
# legitimately absent), do they collapse? And worse: two outputs BOTH missing
# path AND content_hash collapse unconditionally.
# =========================================================================== #
def test_probe_FIXED_empty_identity_link_rejected(conn):
    """FIXED (F4 / migration 010): a link with NEITHER a path NOR a content_hash
    has no output identity. Under the COALESCE('') key two such outputs would
    fold to the same (run, edge, '', '', '') key and the second would be silently
    dropped (its edges lost). The new CHECK target_ref_has_identity forbids the
    identity-less link outright: the FIRST hashless+pathless write already
    RAISES, so the collapse can never happen. RED-was: l1==l2 and the 2nd edge
    silently dropped. GREEN-now: the write is rejected at the source."""
    import psycopg
    run = _mk_run(conn, pipeline_type="ingestion")
    f = _register_file(conn, "audit_a1_md5_p2")
    edges = _file_edge(f)
    # An output with NO content_hash AND NO path -> identity-less -> rejected.
    # (012 target_ref_contract strengthens the old F4 OR-check: BOTH path and
    # content_hash must be non-empty AND a version key present.)
    conn.execute("SAVEPOINT empty_id")
    with pytest.raises(psycopg.errors.CheckViolation):
        _write_link(conn, run, "raw_to_curated", {"version": 1}, edges=edges)
    conn.execute("ROLLBACK TO SAVEPOINT empty_id")
    # Sanity: a link WITH full identity (path + content_hash + version) is
    # accepted. Under the 012 contract content_hash alone no longer suffices —
    # path is mandatory too.
    ok = _write_link(conn, run, "raw_to_curated",
                     {"path": "s3://canon/has-id",
                      "content_hash": "audit_a1-has-id", "version": 1},
                     edges=edges)
    assert ok is not None


def test_probe_SOUND_distinct_path_disambiguates(conn):
    """Control: two outputs that share a content_hash but have DISTINCT paths must
    NOT collapse (path is part of the dedup key). Proves path disambiguation
    works. (012 target_ref_contract now mandates BOTH path AND content_hash, so
    the prior 'hashless' variant of this control is no longer expressible — the
    disambiguation property is unchanged: distinct paths mint distinct links.)"""
    run = _mk_run(conn, pipeline_type="ingestion")
    f = _register_file(conn, "audit_a1_md5_p2b")
    edges = _file_edge(f)
    l1 = _write_link(conn, run, "raw_to_curated",
                     {"path": "s3://canon/p1", "content_hash": "shared",
                      "version": 1}, edges=edges)
    l2 = _write_link(conn, run, "raw_to_curated",
                     {"path": "s3://canon/p2", "content_hash": "shared",
                      "version": 1}, edges=edges)
    assert l1 != l2, "distinct paths must mint distinct links (sound)"


# =========================================================================== #
# PROBE 3 — register_file ON CONFLICT(file_md5, business_date): two DISTINCT
# physical files with the SAME md5 on the same business_date collapse to one
# file_id, and the SECOND file's s3_raw_path is silently discarded.
#
# Spec: file_catalogue "dedups across runs" on (file_md5, business_date). The
# DO UPDATE SET state=state is a NO-OP, so a genuinely different path arriving
# under the same md5 (e.g. same bytes re-delivered to a new location, or an md5
# collision) is dropped — the catalogue keeps the FIRST path. For lineage that
# pins to source_file_id this silently re-points provenance to the wrong path.
# =========================================================================== #
def test_probe_CONFIRMED_register_file_drops_second_path(conn):
    md5 = "audit_a1_dupmd5"
    f1 = _register_file(conn, md5, path="s3://raw/locationA")
    f2 = _register_file(conn, md5, path="s3://raw/locationB")
    assert f1 == f2, "same (md5,bd) dedups to one file_id (expected)"
    stored = conn.execute(
        "SELECT s3_raw_path FROM cp.file_catalogue WHERE file_id=%s",
        [f1]).fetchone()[0]
    # CONFIRMED: locationB is lost; provenance for any run that thinks it read
    # locationB actually pins to locationA.
    assert stored == "s3://raw/locationA", (
        f"expected first path retained, second silently dropped; got {stored}")


# =========================================================================== #
# PROBE 4 — quarantine link identity (post-I1: content_hash = dlq_id).
# Spec decision #3: DLQ is a lineage edge. Two quarantine events in one run with
# the SAME payload_ref must NOT collapse. We verify the dlq_id discriminator
# actually holds (this is the *sound* expectation — if it fails it's a NEW bug).
# =========================================================================== #
def test_probe_SOUND_two_quarantines_same_payload_distinct(conn):
    run = _mk_run(conn)
    d1 = conn.execute(
        "SELECT cp.quarantine(%s,%s,%s,%s::jsonb,%s,%s)",
        [run, "canonicalize", "r", "{}", "s3://dlq/same.json", 1]).fetchone()[0]
    d2 = conn.execute(
        "SELECT cp.quarantine(%s,%s,%s,%s::jsonb,%s,%s)",
        [run, "canonicalize", "r", "{}", "s3://dlq/same.json", 1]).fetchone()[0]
    assert d1 != d2
    links = conn.execute(
        "SELECT count(*) FROM cp.lineage_link "
        "WHERE consumer_run_id=%s AND edge_type='quarantine'", [run]).fetchone()[0]
    assert links == 2, (
        f"two quarantine events same payload_ref must mint two links; got {links}")


# =========================================================================== #
# PROBE 5 — quarantine link path collision: the quarantine link sets
# content_hash=dlq_id (unique) BUT also path=payload_ref. The 5-part key folds on
# (run,edge,'',payload_ref,dlq_id). dlq_id is always unique so quarantines never
# collapse. BUT: a quarantine link and a *non-sink provenance* link could share
# the same (run, '') sink-slot — only edge_type differs. Verify a quarantine and
# a curated_to_canonical in the same run do not collide (sound check).
# =========================================================================== #
def test_probe_SOUND_quarantine_does_not_collide_with_canonical(conn):
    run = _mk_run(conn, pipeline_type="ingestion")
    f = _register_file(conn, "audit_a1_md5_p5")
    _write_link(conn, run, "raw_to_curated",
                {"path": "s3://dlq/x.json", "content_hash": "audit_a1-c",
                 "version": 1},
                edges=_file_edge(f))
    d = conn.execute(
        "SELECT cp.quarantine(%s,%s,%s,%s::jsonb,%s,%s)",
        [run, "s", "r", "{}", "s3://dlq/x.json", 1]).fetchone()[0]
    assert d is not None
    n = conn.execute(
        "SELECT count(*) FROM cp.lineage_link WHERE consumer_run_id=%s",
        [run]).fetchone()[0]
    assert n == 2, f"quarantine and canonical must coexist; got {n} links"


# =========================================================================== #
# PROBE 6 — lineage_edge has NO uniqueness constraint at all (only PK on its own
# random id). write_lineage_link's idempotent-replay branch protects against
# DUPLICATE LINKS, but if a caller invokes write_lineage_link twice with the
# SAME content_hash but a DIFFERENT (longer) edge list... the second call hits
# the dedup branch and silently DROPS the new edges. Worse: there is no DB-level
# guard preventing duplicate edges under a link if a primitive ever inserted
# them. We probe the SILENT-EDGE-DROP on idempotent replay with changed edges.
# =========================================================================== #
def test_probe_FIXED_replay_with_changed_edges_raises(conn):
    """FIXED (F5 / migration 010): decision #5 says re-running an UNCHANGED task
    is idempotent. The reuse branch keyed ONLY on (run,edge,sink,path,hash) and
    did NOT verify the edges match — a second call with the SAME target but a
    DIFFERENT edge set returned the old link and DISCARDED the new edges with no
    error (silent stale lineage). Now the reuse branch compares the incoming edge
    set to the stored one and RAISES on any difference.

    RED-was: l1==l2 and edge count stayed 1 (the changed provenance silently
    dropped). GREEN-now: (a) a DIFFERENT edge set RAISES; (b) an IDENTICAL
    re-call still returns the same link silently (idempotent)."""
    import psycopg
    run = _mk_run(conn, pipeline_type="ingestion")
    f = _register_file(conn, "audit_a1_md5_p6")
    f2 = _register_file(conn, "audit_a1_md5_p6b")
    tgt = {"path": "s3://canon/x", "content_hash": "audit_a1-fixed", "version": 1}
    l1 = _write_link(conn, run, "raw_to_curated", tgt, edges=_file_edge(f))

    # (a) second call, SAME target identity, but TWO edges now (changed
    # provenance) -> non-idempotent reuse -> RAISE (not a silent drop).
    conn.execute("SAVEPOINT changed_edges")
    with pytest.raises(psycopg.errors.RaiseException,
                       match="different edge set"):
        _write_link(conn, run, "raw_to_curated", tgt,
                    edges=[{"source_file_id": str(f), "edge_type": "raw_to_curated",
                            "record_count": 1},
                           {"source_file_id": str(f2), "edge_type": "raw_to_curated",
                            "record_count": 99}])
    conn.execute("ROLLBACK TO SAVEPOINT changed_edges")

    # (b) IDENTICAL re-call is still idempotent: same link, no error, one edge.
    l2 = _write_link(conn, run, "raw_to_curated", tgt, edges=_file_edge(f))
    assert l1 == l2, "identical re-call must dedup to the same link (idempotent)"
    n = conn.execute("SELECT count(*) FROM cp.lineage_edge WHERE lineage_link_id=%s",
                     [l1]).fetchone()[0]
    assert n == 1, f"identical re-call must not duplicate edges (got {n})"


# =========================================================================== #
# PROBE 7 — restart-task vs refeed boundary (decision #5). Same input bytes +
# same workflow_run_id => one link (dedup). Different bytes => new link. We probe
# the OVERWRITE-MUTABILITY case the spec warns about: a mid-run clear-task with
# CHANGED upstream content (new content_hash) must mint a NEW link and NOT
# supersede the old one. Verify BOTH links persist (sound) — and flag that the
# stale link is NOT auto-removed (the spec's own acknowledged hazard).
# =========================================================================== #
def test_probe_SOUND_changed_content_mints_new_link_but_leaves_stale(conn):
    run = _mk_run(conn, pipeline_type="ingestion")
    f = _register_file(conn, "audit_a1_md5_p7")
    edges = _file_edge(f)
    old = _write_link(conn, run, "raw_to_curated",
                      {"path": "s3://canon/v", "content_hash": "audit_a1-old",
                       "version": 1},
                      edges=edges)
    new = _write_link(conn, run, "raw_to_curated",
                      {"path": "s3://canon/v", "content_hash": "audit_a1-new",
                       "version": 1},
                      edges=edges)
    assert old != new, "changed content_hash mints a new link"
    # BOTH exist; run_output_link will now return 'new' (newer) but 'old' lingers
    # as an orphan unless recon flags it — the spec's acknowledged operational gap.
    cnt = conn.execute(
        "SELECT count(*) FROM cp.lineage_link "
        "WHERE consumer_run_id=%s AND edge_type='raw_to_curated'",
        [run]).fetchone()[0]
    assert cnt == 2, f"stale link not superseded; both persist (got {cnt})"
