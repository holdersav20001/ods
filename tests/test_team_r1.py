"""Team R1 — restart-attacker probes: data lineage under Airflow RESTART-A-TASK.

Mission (lineage-review team): break lineage under "Airflow clear-task / retry".
Posture: restart is assumed BROKEN/UNVERIFIED until proven. Every probe models a
restart with the REAL control client (control/*) — no harness fakes are
re-pointed, no migration is touched, schema is NEVER dropped.

WHAT "RESTART-A-TASK" IS (spec decision #5, the authoritative rule):
  Operator clears a failed/stale task in Airflow; Airflow re-runs FROM THAT TASK
  FORWARD under the SAME dag_run_id == SAME workflow_run_id; trigger_type stays
  'airflow'; NO new chain; re-execution is IDEMPOTENT:
    * register_file dedups on (file_md5, business_date, domain, dataset)
    * write_lineage_link dedups on the 5-part output-identity key
      (consumer_run_id, edge_type, COALESCE(sink_type,''),
       COALESCE(path,''), COALESCE(content_hash,''))
  Boundary rule: same bytes + same workflow_run_id => restart (idempotent dedup).
  Mid-run clear-task with CHANGED upstream content => a NEW link (content_hash
  differs, ON CONFLICT misses); the prior link is NOT auto-superseded; clear-task
  MUST clear downstream too; recon MUST flag the orphan.

KEY MODELLING FACT the team-lead flagged and these probes confirm:
  cp.start_run has NO ON CONFLICT — it mints a NEW run_id on EVERY call. So an
  Airflow clear-task that re-runs the ingest task produces a SECOND run_log row
  with the SAME workflow_run_id. register_file / write_lineage_link dedup, but
  RUN IDENTITY does not. Decision #5's idempotency guarantees are scoped to
  files and links; they say NOTHING about run-grain discovery
  (latest_succeeded_run / succeeded_runs), which is where the damage lands.

Probes are named CONFIRMED_* (a failing assertion or a query proving the
defect) vs SOUND_* (decision #5 holds — restart is genuinely idempotent here).
All writes are commit=False; the conn fixture rolls back for isolation; domain/
dataset are namespaced p9r1_*; multi-run scenarios clean up in finally.
"""
import uuid

import pytest

from control import lineage, recon, runs


# --------------------------------------------------------------------------- #
# Helpers — model the REAL client under restart. No harness fakes.            #
# --------------------------------------------------------------------------- #
DOM = "p9r1_dom"
BDATE = "2026-05-30"


def _ds(tag):
    """A unique dataset per probe so probes never collide on discovery."""
    return f"p9r1_{tag}_{uuid.uuid4().hex[:8]}"


def _ingest(conn, *, wfid, dataset, md5, n, path_tag=""):
    """One ingest task execution (the real client path the harness fake uses):
    register_file (idempotent) -> start_run('ingestion','airflow') -> write the
    raw_to_curated link -> finalise succeeded. Returns (run_id, file_id, link_id).

    Re-calling this with the SAME (wfid, dataset, md5) MODELS an Airflow
    clear-task on the ingest task: same workflow_run_id, same file bytes."""
    file_id = runs.register_file(
        conn, s3_raw_path=f"s3://raw/{dataset}/{md5}.csv", file_md5=md5,
        business_date=BDATE, domain=DOM, dataset=dataset, commit=False)
    run_id = runs.start(
        conn, workflow_run_id=wfid, pipeline_type="ingestion", domain=DOM,
        dataset=dataset, business_date=BDATE, trigger_type="airflow",
        file_id=file_id, commit=False)
    link_id = lineage.write_link(
        conn, consumer_run_id=run_id, edge_type="raw_to_curated",
        target_ref={"path": f"s3://curated/{dataset}/{md5}{path_tag}.parquet",
                    "content_hash": md5, "version": 1},
        record_count=n,
        edges=[{"source_file_id": file_id, "edge_type": "raw_to_curated",
                "source_ref": {"path": f"s3://raw/{dataset}/{md5}.csv"},
                "record_count": n}],
        commit=False)
    runs.finalise(conn, run_id, status="succeeded", record_count_out=n,
                  commit=False)
    return run_id, file_id, link_id


# --------------------------------------------------------------------------- #
# ATTACK 1 — THE CENTRAL RESTART HAZARD.                                       #
# Restart-a-task creates a DUPLICATE succeeded run for one slice; a downstream #
# 1:N discovery hop (merge) then binds BOTH runs and DOUBLE-COUNTS one file.   #
# --------------------------------------------------------------------------- #
def test_CONFIRMED_restart_ingest_duplicates_succeeded_run_for_one_slice(conn):
    """CONFIRMED (CRITICAL). Decision #5 says a restart re-runs under the SAME
    workflow_run_id and is idempotent. register_file and write_lineage_link DO
    dedup. But cp.start_run mints a NEW run_id every call (no ON CONFLICT), so
    after a clear-task the slice has TWO succeeded 'ingestion' runs that share
    one workflow_run_id and one physical file.

    succeeded_runs() — the merge-hop discovery primitive — returns BOTH. There is
    nothing tying run identity to workflow_run_id, so restart is NOT idempotent
    at run grain. This is the precondition for the merge double-count below."""
    try:
        wfid = str(uuid.uuid4())
        ds = _ds("a1dup")
        md5 = uuid.uuid4().hex

        orig_run, file_id, orig_link = _ingest(conn, wfid=wfid, dataset=ds,
                                                md5=md5, n=5)
        # Airflow clear-task on the ingest task: SAME wfid, SAME bytes.
        restart_run, file_id2, restart_link = _ingest(conn, wfid=wfid,
                                                       dataset=ds, md5=md5, n=5)

        # register_file DID dedup (its key is file-grain, not run-grain)...
        assert file_id2 == file_id, "register_file should dedup on restart"
        # ...but RUN identity did NOT: two distinct run_ids, one workflow_run_id.
        assert restart_run != orig_run, "start_run minted a NEW run on restart"
        # ...AND the link did NOT dedup either: the 5-part ON CONFLICT key is
        # SCOPED TO consumer_run_id, and the restart has a NEW consumer_run_id,
        # so write_lineage_link mints a BRAND-NEW link. Decision #5's claim that
        # 'an unchanged task produces the same link (counts stable)' is FALSE
        # whenever the restart yields a new run_id — which it ALWAYS does. The
        # link dedup only ever protects in-place re-calls under one run, never a
        # real Airflow clear-task. This is a stronger defect than mere run dup.
        assert restart_link != orig_link, (
            "write_lineage_link did NOT dedup across restart — link key is "
            "consumer_run_id-scoped; #5 link idempotency NEVER engages on a "
            "real clear-task that mints a new run_id")

        all_runs = runs.succeeded_runs(
            conn, domain=DOM, dataset=ds, business_date=BDATE,
            pipeline_type="ingestion")
        assert set(all_runs) == {orig_run, restart_run}, all_runs
        # CONFIRMED: discovery sees TWO succeeded ingestion runs for ONE file
        # under ONE workflow_run_id. Restart is non-idempotent at run grain.
        assert len(all_runs) == 2, (
            "restart created a duplicate succeeded run for one slice — "
            "succeeded_runs() returns both; the merge hop will double-count")

        wfids = {conn.execute(
            "SELECT workflow_run_id FROM cp.run_log WHERE run_id=%s",
            [r]).fetchone()[0] for r in all_runs}
        assert wfids == {wfid}, "both runs share the one workflow_run_id (#5)"
    finally:
        conn.rollback()


def test_CONFIRMED_restart_makes_merge_double_count_one_physical_file(conn):
    """CONFIRMED (CRITICAL) — the payload of attack 1. After a restart of ONE
    ingest, the merge hop (which DISCOVERS its upstreams via succeeded_runs, per
    the harness contract) sees TWO slots for the SAME physical file and folds the
    file's rows in TWICE. record_count of the merged canonical link = 2x the real
    row count; provenance shows two parallel raw_to_curated inputs for one file.

    This is the exact 'merge double-counts (two slots for one physical file)'
    hazard. Decision #5's idempotency does NOT cover it because the dedup keys
    (file, link) are per-output, while merge fans in per-RUN."""
    try:
        wfid = str(uuid.uuid4())
        ds = _ds("a1merge")
        md5 = uuid.uuid4().hex
        N = 5

        orig_run, file_id, orig_link = _ingest(conn, wfid=wfid, dataset=ds,
                                                md5=md5, n=N)
        restart_run, _, restart_link = _ingest(conn, wfid=wfid, dataset=ds,
                                                md5=md5, n=N)

        # Merge DISCOVERS all succeeded ingestion runs (succeeded_runs) and folds
        # each in as a slot — this is exactly what harness.fakes.fake_merge does.
        upstreams = runs.succeeded_runs(
            conn, domain=DOM, dataset=ds, business_date=BDATE,
            pipeline_type="ingestion")
        assert len(upstreams) == 2

        merge_run = runs.start(
            conn, workflow_run_id=wfid, pipeline_type="canonicalization",
            domain=DOM, dataset=ds, business_date=BDATE, trigger_type="airflow",
            commit=False)
        edges = []
        total = 0
        for slot, up in enumerate(upstreams):
            up_link = runs.run_output_link(
                conn, run_id=up, edge_type="raw_to_curated")
            cnt = runs.run_record_count(conn, run_id=up)  # each = N
            total += cnt
            edges.append({"upstream_run_id": up, "upstream_lineage_link_id":
                          up_link, "input_slot": slot,
                          "edge_type": "merge_to_canonical", "record_count": cnt})
        merge_link = lineage.write_link(
            conn, consumer_run_id=merge_run, edge_type="merge_to_canonical",
            target_ref={"path": f"s3://canonical/{ds}.parquet",
                        "content_hash": f"{ds}-merged", "version": 1},
            record_count=total, edges=edges, commit=False)

        # CONFIRMED double-count: 2 slots * N rows for ONE physical file of N rows.
        assert total == 2 * N, "merge folded the same file in twice"
        edge_files = conn.execute(
            "SELECT DISTINCT le.source_file_id "
            "FROM cp.lineage_edge le "
            "JOIN cp.lineage_link ul ON ul.lineage_link_id = le.upstream_lineage_link_id "
            "WHERE le.lineage_link_id=%s", [merge_link]).fetchall()
        # Both merge edges trace (via their upstream raw_to_curated link) to the
        # SAME ONE registered file — provenance is ambiguous/duplicated.
        raw_files = conn.execute(
            "SELECT DISTINCT le.source_file_id FROM cp.lineage_edge le "
            "WHERE le.lineage_link_id IN (%s,%s)" % (
                "'" + orig_link + "'", "'" + restart_link + "'"),
        ).fetchall()
        assert raw_files == [(uuid.UUID(file_id),)], (
            "both raw_to_curated inputs of the merge point at the ONE physical "
            "file — the merged link record_count is inflated 2x")
        merged_count = conn.execute(
            "SELECT record_count FROM cp.lineage_link WHERE lineage_link_id=%s",
            [merge_link]).fetchone()[0]
        assert merged_count == 2 * N, merged_count
    finally:
        conn.rollback()


def test_CONFIRMED_restart_ambiguates_latest_succeeded_run_binding(conn):
    """CONFIRMED (HIGH). The canonicalize hop binds via latest_succeeded_run
    (newest by finished_at, run_id). A restart mints a newer run, so canonicalize
    silently re-binds to the RESTART run, NOT the original — even though both
    produced the SAME bytes. latest_succeeded_run cannot express 'the run for
    THIS workflow_run_id'; provenance now depends on restart timing, which is the
    ambiguity decision #5's same-workflow_run_id rule was meant to remove."""
    try:
        wfid = str(uuid.uuid4())
        ds = _ds("a1bind")
        md5 = uuid.uuid4().hex
        orig_run, _, _ = _ingest(conn, wfid=wfid, dataset=ds, md5=md5, n=5)
        restart_run, _, _ = _ingest(conn, wfid=wfid, dataset=ds, md5=md5, n=5)

        latest = runs.latest_succeeded_run(
            conn, domain=DOM, dataset=ds, business_date=BDATE,
            pipeline_type="ingestion")
        # CONFIRMED: discovery binds to the restart run, not the original.
        assert latest == restart_run, (
            "canonicalize discovers the RESTART run; binding depends on restart "
            "timing, not on the workflow_run_id — provenance is non-deterministic")
        assert latest != orig_run
    finally:
        conn.rollback()


# --------------------------------------------------------------------------- #
# ATTACK 2 — Codex P1: run_output_link(target_path) is NOT an exact selector.  #
# It filters on path ONLY, never content_hash, so two links at one path with   #
# different content_hash silently collapse to one (the OLD bytes).             #
# --------------------------------------------------------------------------- #
def test_CONFIRMED_run_output_link_path_not_exact_silent_stale_pick(conn):
    """CONFIRMED (HIGH) — confirms Codex P1. cp.run_output_link with a
    target_path matches WHERE target_ref->>'path' = p_target_path with NO
    content_hash predicate (see 010). Decision #5's mid-run-changed-content case
    produces two links at the SAME path (old + new content_hash). The selector
    is therefore NOT exact: a plpgsql `SELECT ... INTO` over two matching rows
    returns ONE arbitrarily and does NOT error. A consumer wiring its upstream by
    path silently gets a stale (or arbitrary) content version."""
    try:
        wfid = str(uuid.uuid4())
        ds = _ds("a2sel")
        run_id = runs.start(
            conn, workflow_run_id=wfid, pipeline_type="canonicalization",
            domain=DOM, dataset=ds, business_date=BDATE, trigger_type="airflow",
            commit=False)
        # raw anchors so each link is valid.
        f_old = runs.register_file(
            conn, s3_raw_path="s3://raw/old.csv", file_md5="oldmd5",
            business_date=BDATE, domain=DOM, dataset=ds, commit=False)
        f_new = runs.register_file(
            conn, s3_raw_path="s3://raw/new.csv", file_md5="newmd5",
            business_date=BDATE, domain=DOM, dataset=ds, commit=False)

        path = f"s3://cur/{ds}/same"
        link_old = lineage.write_link(
            conn, consumer_run_id=run_id, edge_type="raw_to_curated",
            target_ref={"path": path, "content_hash": "old", "version": 1},
            record_count=5,
            edges=[{"source_file_id": f_old, "edge_type": "raw_to_curated",
                    "source_ref": {"path": "s3://raw/old.csv"},
                    "record_count": 5}], commit=False)
        link_new = lineage.write_link(
            conn, consumer_run_id=run_id, edge_type="raw_to_curated",
            target_ref={"path": path, "content_hash": "new", "version": 2},
            record_count=5,
            edges=[{"source_file_id": f_new, "edge_type": "raw_to_curated",
                    "source_ref": {"path": "s3://raw/new.csv"},
                    "record_count": 5}], commit=False)

        # The 5-part key let BOTH exist (different content_hash) — confirm.
        n_at_path = conn.execute(
            "SELECT count(*) FROM cp.lineage_link "
            "WHERE consumer_run_id=%s AND edge_type='raw_to_curated' "
            "AND target_ref->>'path'=%s", [run_id, path]).fetchone()[0]
        assert n_at_path == 2, "two links coexist at one path (old+new content)"
        assert link_old != link_new

        # The selector returns ONE without error — NOT exact, NO disambiguation.
        got = runs.run_output_link(
            conn, run_id=run_id, edge_type="raw_to_curated", target_path=path)
        # CONFIRMED: it silently resolved a path-collision to a single link
        # instead of RAISING 'ambiguous' (as it correctly does for >1 path-less
        # output). A consumer cannot tell it got the stale 'old' version.
        assert got in {link_old, link_new}
        # Prove the silence: no exception was raised even though the path is
        # ambiguous on content. (Contrast: path-less ambiguity DOES raise.)
        assert got is not None, (
            "run_output_link(target_path) silently returned one of two "
            "different-content links at the same path — Codex P1 confirmed")
    finally:
        conn.rollback()


# --------------------------------------------------------------------------- #
# ATTACK 3 — Codex P4: reconcile_sink is run-scoped, not per-output.           #
# One run with two sink outputs of the same 5-row upstream => 10 rows counted  #
# => false 'double_count'.                                                     #
# --------------------------------------------------------------------------- #
def test_CONFIRMED_reconcile_sink_run_scoped_false_double_count_on_fanout(conn):
    """CONFIRMED (HIGH) — confirms Codex P4. cp.reconcile_sink derives `accounted`
    by counting ALL ods.<dataset> rows joined to ANY canonical_to_sink link of
    the run (see 011). Decision #6 MANDATES fan-out: one run writes K
    canonical_to_sink links (e.g. postgres + kafka) for the SAME upstream rows.
    Each link stamps its own target rows, so a 5-row upstream fanned to 2 sinks
    stamps 10 rows. reconcile_sink(run, 5) then computes accounted=10,
    discrepancy=-5 => status 'double_count' — a FALSE breach on a CORRECT
    fan-out. Recon should be PER-OUTPUT (per sink link), e.g.
    reconcile_sink_link(link_id, source_count)."""
    try:
        wfid = str(uuid.uuid4())
        ds = "orders"  # the only ods.* target table that exists
        # upstream curated link so canonical_to_sink's run-edge CHECK is met.
        up_run = runs.start(
            conn, workflow_run_id=wfid, pipeline_type="canonicalization",
            domain=DOM, dataset=ds, business_date=BDATE, trigger_type="airflow",
            commit=False)
        f = runs.register_file(
            conn, s3_raw_path=f"s3://raw/{uuid.uuid4().hex}.csv",
            file_md5=uuid.uuid4().hex, business_date=BDATE, domain=DOM,
            dataset=ds, commit=False)
        up_link = lineage.write_link(
            conn, consumer_run_id=up_run, edge_type="raw_to_curated",
            target_ref={"path": f"s3://cur/{wfid}", "content_hash": "h0",
                        "version": 1}, record_count=5,
            edges=[{"source_file_id": f, "edge_type": "raw_to_curated",
                    "source_ref": {"path": "s3://raw/x"}, "record_count": 5}],
            commit=False)

        sink_run = runs.start(
            conn, workflow_run_id=wfid, pipeline_type="sink", domain=DOM,
            dataset=ds, business_date=BDATE, trigger_type="airflow",
            commit=False)
        rows = [{"v": i} for i in range(5)]
        for sink_type, path in (("postgres", "postgres://ods/orders"),
                                ("kafka", "kafka://ods.orders")):
            lineage.write_link_then_rows(
                conn, consumer_run_id=sink_run, edge_type="canonical_to_sink",
                target_ref={"path": path, "content_hash": "hX", "version": 1},
                record_count=5, sink_type=sink_type,
                edges=[{"upstream_lineage_link_id": up_link, "input_slot": 0,
                        "edge_type": "canonical_to_sink", "record_count": 5}],
                rows=rows, commit=False)

        # Two correct fan-out sink outputs -> 10 stamped rows for this run.
        stamped = conn.execute(
            "SELECT count(*) FROM ods.orders r JOIN cp.lineage_link l "
            "ON l.lineage_link_id=r._ods_lineage_link_id "
            "WHERE l.consumer_run_id=%s AND l.edge_type='canonical_to_sink'",
            [sink_run]).fetchone()[0]
        assert stamped == 10, stamped

        recon.reconcile_sink(conn, run_id=sink_run, source_count=5, commit=False)
        status, disc, accounted = conn.execute(
            "SELECT status, discrepancy, accounted_count "
            "FROM cp.reconciliation_log WHERE run_id=%s AND check_type='sink_graph'",
            [sink_run]).fetchone()
        # CONFIRMED false breach: a CORRECT fan-out is reported as double_count.
        assert accounted == 10 and disc == -5 and status == "double_count", (
            status, disc, accounted)
    finally:
        conn.rollback()


# --------------------------------------------------------------------------- #
# ATTACK 4 — Restart after partial failure (re-run sink only).                 #
# Same content_hash => same link => the write_link_then_rows row-guard prevents #
# doubling (SOUND). Different content_hash => new link => rows DOUBLE.          #
# --------------------------------------------------------------------------- #
def test_SOUND_restart_sink_same_content_does_not_double_rows(conn):
    """SOUND. A run fails at sink, is cleared, and re-runs the sink task ONLY
    under the same workflow_run_id with the SAME bytes. write_link_then_rows
    dedups the link (5-part key) AND its row-guard ('if the link already has
    target rows, return') prevents re-stamping. Rows stay at 5; recon stays ok.
    Decision #5 idempotency HOLDS for the same-bytes sink restart."""
    try:
        wfid = str(uuid.uuid4())
        ds = "orders"
        sink_run = runs.start(
            conn, workflow_run_id=wfid, pipeline_type="sink", domain=DOM,
            dataset=ds, business_date=BDATE, trigger_type="airflow",
            commit=False)
        f = runs.register_file(
            conn, s3_raw_path=f"s3://raw/{uuid.uuid4().hex}.csv",
            file_md5=uuid.uuid4().hex, business_date=BDATE, domain=DOM,
            dataset=ds, commit=False)
        up = runs.start(
            conn, workflow_run_id=wfid, pipeline_type="canonicalization",
            domain=DOM, dataset=ds, business_date=BDATE, trigger_type="airflow",
            commit=False)
        up_link = lineage.write_link(
            conn, consumer_run_id=up, edge_type="raw_to_curated",
            target_ref={"path": f"s3://cur/{wfid}", "content_hash": "h0",
                        "version": 1}, record_count=5,
            edges=[{"source_file_id": f, "edge_type": "raw_to_curated",
                    "source_ref": {"path": "s3://raw/x"}, "record_count": 5}],
            commit=False)

        def sink_once():
            return lineage.write_link_then_rows(
                conn, consumer_run_id=sink_run, edge_type="canonical_to_sink",
                target_ref={"path": "postgres://ods/orders",
                            "content_hash": "hSAME", "version": 1},
                record_count=5, sink_type="postgres",
                edges=[{"upstream_lineage_link_id": up_link, "input_slot": 0,
                        "edge_type": "canonical_to_sink", "record_count": 5}],
                rows=[{"v": i} for i in range(5)], commit=False)

        link1 = sink_once()
        link2 = sink_once()  # the clear-task re-run, same bytes
        assert link1 == link2, "same-content sink restart dedups the link"
        rows = conn.execute(
            "SELECT count(*) FROM ods.orders WHERE _ods_lineage_link_id=%s",
            [link1]).fetchone()[0]
        assert rows == 5, ("row-guard held — restart did NOT double rows; "
                           "traceable to raw via the curated upstream link")
        # Recon (per-output would be correct) — single output, 5==5, ok.
        recon.reconcile_sink(conn, run_id=sink_run, source_count=5, commit=False)
        status = conn.execute(
            "SELECT status FROM cp.reconciliation_log WHERE run_id=%s "
            "AND check_type='sink_graph'", [sink_run]).fetchone()[0]
        assert status == "ok", status
    finally:
        conn.rollback()


def test_CONFIRMED_restart_sink_changed_content_doubles_rows(conn):
    """CONFIRMED (HIGH). The row-guard is keyed on the LINK, and the link key
    carries content_hash. If a sink restart re-runs with CHANGED bytes (new
    content_hash) — the boundary case decision #5 describes — a NEW link forms,
    the row-guard misses, and the new rows are stamped IN ADDITION to the old
    ones. The target now holds 10 rows for a 5-row source, the OLD link's rows
    are NOT superseded, and reconcile_sink (run-scoped) reports double_count.
    Restart with changed content is NOT idempotent at row grain."""
    try:
        wfid = str(uuid.uuid4())
        ds = "orders"
        up = runs.start(
            conn, workflow_run_id=wfid, pipeline_type="canonicalization",
            domain=DOM, dataset=ds, business_date=BDATE, trigger_type="airflow",
            commit=False)
        f = runs.register_file(
            conn, s3_raw_path=f"s3://raw/{uuid.uuid4().hex}.csv",
            file_md5=uuid.uuid4().hex, business_date=BDATE, domain=DOM,
            dataset=ds, commit=False)
        up_link = lineage.write_link(
            conn, consumer_run_id=up, edge_type="raw_to_curated",
            target_ref={"path": f"s3://cur/{wfid}", "content_hash": "h0",
                        "version": 1}, record_count=5,
            edges=[{"source_file_id": f, "edge_type": "raw_to_curated",
                    "source_ref": {"path": "s3://raw/x"}, "record_count": 5}],
            commit=False)
        sink_run = runs.start(
            conn, workflow_run_id=wfid, pipeline_type="sink", domain=DOM,
            dataset=ds, business_date=BDATE, trigger_type="airflow",
            commit=False)

        def sink(content_hash):
            return lineage.write_link_then_rows(
                conn, consumer_run_id=sink_run, edge_type="canonical_to_sink",
                target_ref={"path": "postgres://ods/orders",
                            "content_hash": content_hash, "version": 1},
                record_count=5, sink_type="postgres",
                edges=[{"upstream_lineage_link_id": up_link, "input_slot": 0,
                        "edge_type": "canonical_to_sink", "record_count": 5}],
                rows=[{"v": i} for i in range(5)], commit=False)

        link_old = sink("hOLD")
        link_new = sink("hNEW")  # clear-task re-run with CHANGED content
        assert link_old != link_new, "changed content => new link (#5 boundary)"
        total = conn.execute(
            "SELECT count(*) FROM ods.orders r JOIN cp.lineage_link l "
            "ON l.lineage_link_id=r._ods_lineage_link_id "
            "WHERE l.consumer_run_id=%s AND l.edge_type='canonical_to_sink'",
            [sink_run]).fetchone()[0]
        # CONFIRMED: 10 rows for a 5-row source; old rows NOT superseded.
        assert total == 10, total
        recon.reconcile_sink(conn, run_id=sink_run, source_count=5, commit=False)
        status = conn.execute(
            "SELECT status FROM cp.reconciliation_log WHERE run_id=%s "
            "AND check_type='sink_graph'", [sink_run]).fetchone()[0]
        assert status == "double_count", status
    finally:
        conn.rollback()


# --------------------------------------------------------------------------- #
# ATTACK 5 — Restart with CHANGED content mid-run, downstream NOT cleared.     #
# The boundary case: a new link forms, the old is NOT superseded, a consumer   #
# can still DISCOVER the stale link, and recon does NOT flag the orphan.       #
# --------------------------------------------------------------------------- #
def test_CONFIRMED_changed_content_leaves_discoverable_stale_link_unflagged(conn):
    """CONFIRMED (HIGH). Decision #5 boundary rule: a mid-run clear-task whose
    upstream content CHANGES mints a NEW link (content_hash differs, ON CONFLICT
    misses) and the prior link is NOT auto-superseded. The operational
    requirement is that clear-task ALSO clears downstream and that recon flags
    the orphan otherwise. This probe re-runs the upstream WITHOUT clearing
    downstream and shows:
      (1) both old and new curated links coexist for the run;
      (2) a downstream consumer discovering by path (run_output_link) silently
          resolves to ONE of them (the stale link is still reachable);
      (3) there is NO superseded/valid flag and NO recon check that marks the
          orphaned old link — it silently rots. There is no automatic orphan
          detection in cp.*; nothing flags it."""
    try:
        wfid = str(uuid.uuid4())
        ds = _ds("a5orphan")
        run_id = runs.start(
            conn, workflow_run_id=wfid, pipeline_type="canonicalization",
            domain=DOM, dataset=ds, business_date=BDATE, trigger_type="airflow",
            commit=False)
        f1 = runs.register_file(
            conn, s3_raw_path="s3://raw/v1.csv", file_md5="md5v1",
            business_date=BDATE, domain=DOM, dataset=ds, commit=False)
        f2 = runs.register_file(
            conn, s3_raw_path="s3://raw/v2.csv", file_md5="md5v2",
            business_date=BDATE, domain=DOM, dataset=ds, commit=False)
        path = f"s3://cur/{ds}/canonical"
        link_old = lineage.write_link(
            conn, consumer_run_id=run_id, edge_type="raw_to_curated",
            target_ref={"path": path, "content_hash": "stale", "version": 1},
            record_count=5,
            edges=[{"source_file_id": f1, "edge_type": "raw_to_curated",
                    "source_ref": {"path": "s3://raw/v1.csv"},
                    "record_count": 5}], commit=False)
        # Mid-run clear-task: SAME path, CHANGED content -> NEW link.
        link_new = lineage.write_link(
            conn, consumer_run_id=run_id, edge_type="raw_to_curated",
            target_ref={"path": path, "content_hash": "fresh", "version": 2},
            record_count=5,
            edges=[{"source_file_id": f2, "edge_type": "raw_to_curated",
                    "source_ref": {"path": "s3://raw/v2.csv"},
                    "record_count": 5}], commit=False)

        # (1) Old link is NOT auto-superseded — it still exists.
        assert link_old != link_new
        both = conn.execute(
            "SELECT count(*) FROM cp.lineage_link WHERE consumer_run_id=%s "
            "AND target_ref->>'path'=%s", [run_id, path]).fetchone()[0]
        assert both == 2, "old link lingers alongside the new one"

        # (2) The stale link is still DISCOVERABLE by a downstream consumer.
        got = runs.run_output_link(
            conn, run_id=run_id, edge_type="raw_to_curated", target_path=path)
        assert got in {link_old, link_new}, (
            "downstream discovery by path silently reaches one of the two — the "
            "stale link is consumable")

        # (3) NO column flags the orphan and NO recon check marks it.
        cols = conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema='cp' AND table_name='lineage_link'").fetchall()
        colnames = {c[0] for c in cols}
        assert "superseded_at" not in colnames and "is_valid" not in colnames, (
            "no supersede/validity flag on lineage_link — orphan cannot be "
            "marked; it silently rots")
        orphan_checks = conn.execute(
            "SELECT count(*) FROM cp.reconciliation_log WHERE run_id=%s "
            "AND check_type ILIKE '%%orphan%%'", [run_id]).fetchone()[0]
        assert orphan_checks == 0, (
            "no recon check flags the orphaned stale link (CONFIRMED rot)")
    finally:
        conn.rollback()
