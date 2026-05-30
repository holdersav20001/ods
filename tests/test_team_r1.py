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

import psycopg
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
def test_FIXED_restart_ingest_reuses_run_for_one_slice(conn):
    """FIXED (P10-A, migration 013) — FLIPPED from
    test_CONFIRMED_restart_ingest_duplicates_succeeded_run_for_one_slice.

    Decision #5: a restart re-runs under the SAME workflow_run_id and is
    idempotent. cp.start_run is now idempotent on the partial uq_run_identity
    index (workflow_run_id, pipeline_type, slice, COALESCE(file_id,sentinel)),
    so an Airflow clear-task on the ingest task REUSES the same run_id instead of
    minting a new one. The slice therefore has exactly ONE succeeded 'ingestion'
    run for the one physical file, and — because consumer_run_id is now stable —
    write_lineage_link's 5-part dedup key ENGAGES, so the restart re-uses the
    SAME link too (decision #5's "same task => same link, counts stable")."""
    try:
        wfid = str(uuid.uuid4())
        ds = _ds("a1dup")
        md5 = uuid.uuid4().hex

        orig_run, file_id, orig_link = _ingest(conn, wfid=wfid, dataset=ds,
                                                md5=md5, n=5)
        # Airflow clear-task on the ingest task: SAME wfid, SAME bytes.
        restart_run, file_id2, restart_link = _ingest(conn, wfid=wfid,
                                                       dataset=ds, md5=md5, n=5)

        # register_file dedups (file-grain key)...
        assert file_id2 == file_id, "register_file should dedup on restart"
        # ...AND run identity now holds: the restart REUSES the same run_id.
        assert restart_run == orig_run, (
            "start_run is idempotent on restart — same (wfid, ingestion, slice, "
            "file) REUSES the run_id instead of minting a new one (013)")
        # ...AND the link dedups too: same consumer_run_id => the 5-part
        # ON CONFLICT key now engages => the SAME link, counts stable (#5).
        assert restart_link == orig_link, (
            "write_lineage_link dedup ENGAGES on restart now that consumer_run_id "
            "is stable — #5 link idempotency holds on a real clear-task")

        all_runs = runs.succeeded_runs(
            conn, domain=DOM, dataset=ds, business_date=BDATE,
            pipeline_type="ingestion")
        # FIXED: discovery sees exactly ONE succeeded ingestion run for ONE file
        # under ONE workflow_run_id. Restart is idempotent at run grain.
        assert all_runs == [orig_run], all_runs
        assert len(all_runs) == 1, (
            "restart reused the run — succeeded_runs() returns exactly one; the "
            "merge hop can no longer double-count")

        wfids = {conn.execute(
            "SELECT workflow_run_id FROM cp.run_log WHERE run_id=%s",
            [r]).fetchone()[0] for r in all_runs}
        assert wfids == {wfid}, "the one run carries the one workflow_run_id (#5)"
    finally:
        conn.rollback()


def test_FIXED_restart_merge_does_not_double_count_physical_file(conn):
    """FIXED (P10-A, migration 013) — FLIPPED from
    test_CONFIRMED_restart_makes_merge_double_count_one_physical_file.

    Construct so the OLD code double-counted and the NEW code does not: ingest
    file A, RESTART that ingest (same wfid/bytes), AND ingest a genuinely SECOND
    physical file B into the same slice. A real 2-file merge must have exactly
    TWO slots (one per PHYSICAL file) summing to the real total — NOT three slots
    with A folded in twice.

    Pre-013 succeeded_runs() returned THREE runs (A, A-restart, B) -> merge
    double-counted A (3 slots, total = 3*N). Post-013 the A-restart reuses A's
    run_id, so succeeded_runs() returns exactly TWO runs (A, B) -> two slots,
    SUM(edges) == the real 2*N, and the two merge edges trace to the two DISTINCT
    physical files."""
    try:
        wfid = str(uuid.uuid4())
        ds = _ds("a1merge")
        md5_a = uuid.uuid4().hex
        md5_b = uuid.uuid4().hex
        N = 5

        run_a, file_a, link_a = _ingest(conn, wfid=wfid, dataset=ds,
                                        md5=md5_a, n=N)
        # Airflow clear-task on file A's ingest: SAME wfid, SAME bytes -> reuse.
        run_a2, file_a2, link_a2 = _ingest(conn, wfid=wfid, dataset=ds,
                                           md5=md5_a, n=N)
        assert run_a2 == run_a, "restart of file A reused its run (013)"
        assert file_a2 == file_a and link_a2 == link_a
        # A genuinely SECOND physical file in the same slice (distinct file_id).
        run_b, file_b, link_b = _ingest(conn, wfid=wfid, dataset=ds,
                                        md5=md5_b, n=N)
        assert run_b != run_a and file_b != file_a

        # Merge DISCOVERS all succeeded ingestion runs (succeeded_runs) and folds
        # each in as a slot — exactly what harness.fakes.fake_merge does.
        upstreams = runs.succeeded_runs(
            conn, domain=DOM, dataset=ds, business_date=BDATE,
            pipeline_type="ingestion")
        # FIXED: exactly TWO upstream runs (one per PHYSICAL file), not three.
        assert set(upstreams) == {run_a, run_b}, upstreams
        assert len(upstreams) == 2, (
            "merge sees one run per physical file — the restart did NOT add a "
            "third slot")

        merge_run = runs.start(
            conn, workflow_run_id=wfid, pipeline_type="merge",
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

        # FIXED: SUM(edges) == the real total (2*N), NOT inflated to 3*N.
        assert total == 2 * N, ("merge total is the real per-physical-file sum, "
                                "not inflated by the restart")
        # The two merge edges trace to the two DISTINCT physical files.
        raw_files = conn.execute(
            "SELECT DISTINCT le.source_file_id FROM cp.lineage_edge le "
            "WHERE le.lineage_link_id IN (%s,%s)" % (
                "'" + link_a + "'", "'" + link_b + "'"),
        ).fetchall()
        assert {r[0] for r in raw_files} == {uuid.UUID(file_a), uuid.UUID(file_b)}, (
            "the merge's raw inputs are the two distinct physical files — no "
            "duplicated provenance")
        merged_count = conn.execute(
            "SELECT record_count FROM cp.lineage_link WHERE lineage_link_id=%s",
            [merge_link]).fetchone()[0]
        assert merged_count == 2 * N, merged_count
    finally:
        conn.rollback()


def test_FIXED_restart_latest_succeeded_run_is_deterministic(conn):
    """FIXED (P10-A, migration 013) — FLIPPED from
    test_CONFIRMED_restart_ambiguates_latest_succeeded_run_binding.

    The canonicalize/sink hop binds via latest_succeeded_run. Pre-013 a restart
    minted a NEWER run, so discovery silently re-bound to the restart run and the
    binding depended on restart timing / the finished_at,run_id tie-break. Post-
    013 the restart REUSES the one run_id, so there is no second run to tie-break
    against: latest_succeeded_run returns the one reused run deterministically,
    and it IS the original run_id (the same logical run for this
    workflow_run_id)."""
    try:
        wfid = str(uuid.uuid4())
        ds = _ds("a1bind")
        md5 = uuid.uuid4().hex
        orig_run, _, _ = _ingest(conn, wfid=wfid, dataset=ds, md5=md5, n=5)
        restart_run, _, _ = _ingest(conn, wfid=wfid, dataset=ds, md5=md5, n=5)
        assert restart_run == orig_run, "restart reused the run (013)"

        latest = runs.latest_succeeded_run(
            conn, domain=DOM, dataset=ds, business_date=BDATE,
            pipeline_type="ingestion")
        # FIXED: discovery returns the one reused run, deterministically — no
        # dependence on a 2nd run or a finished_at/run_id tie-break.
        assert latest == orig_run, (
            "latest_succeeded_run returns the one reused run deterministically — "
            "binding no longer depends on restart timing")
    finally:
        conn.rollback()


# --------------------------------------------------------------------------- #
# ATTACK 2 — Codex P1: run_output_link(target_path) is NOT an exact selector.  #
# It filters on path ONLY, never content_hash, so two links at one path with   #
# different content_hash silently collapse to one (the OLD bytes).             #
# --------------------------------------------------------------------------- #
def test_FIXED_run_output_link_path_ambiguous_raises_content_hash_exact(conn):
    """FIXED (THEME B / migration 015) — confirms Codex P1 closed. Two links at
    the SAME path (old + new content_hash) coexist under the hardened 5-part key.
    The path branch of cp.run_output_link is now EXACT-or-RAISE: with >1 link at
    (run, edge_type, path) it RAISES 'ambiguous — pass p_content_hash' (no silent
    stale pick), and passing the disambiguating content_hash returns the EXACT
    one. (Was: silently returned one arbitrarily.)"""
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

        # FIXED: the path branch is now EXACT-or-RAISE. Two links at one path =>
        # the path-only selector RAISES 'ambiguous' (no silent stale pick), the
        # same loud-failure contract the path-less branch already had.
        conn.execute("SAVEPOINT c_ambig_path")
        with pytest.raises(psycopg.errors.RaiseException, match="ambiguous"):
            runs.run_output_link(
                conn, run_id=run_id, edge_type="raw_to_curated", target_path=path)
        conn.execute("ROLLBACK TO SAVEPOINT c_ambig_path")

        # Disambiguating by content_hash returns the EXACT link — no guessing.
        got_old = runs.run_output_link(
            conn, run_id=run_id, edge_type="raw_to_curated",
            target_path=path, content_hash="old")
        got_new = runs.run_output_link(
            conn, run_id=run_id, edge_type="raw_to_curated",
            target_path=path, content_hash="new")
        assert str(got_old) == str(link_old), (got_old, link_old)
        assert str(got_new) == str(link_new), (got_new, link_new)

        # content_hash alone (no path) is also exact.
        got_old2 = runs.run_output_link(
            conn, run_id=run_id, edge_type="raw_to_curated", content_hash="old")
        assert str(got_old2) == str(link_old)
    finally:
        conn.rollback()


# --------------------------------------------------------------------------- #
# ATTACK 3 — Codex P4 CLOSED (THEME E): per-OUTPUT sink recon.                  #
# One run with two sink outputs of the same 5-row upstream stamps 10 rows; the  #
# run-scoped reconcile_sink would call that a false 'double_count'. The new      #
# per-link reconcile_sink_link scopes `accounted` to ONE link's rows, so each   #
# correct fan-out output reconciles ok.                                         #
# --------------------------------------------------------------------------- #
def test_FIXED_reconcile_sink_link_per_output_no_false_double_count_on_fanout(conn):
    """FIXED (THEME E / migration 015) — closes Codex P4. cp.reconcile_sink is
    run-scoped: it counts ALL ods.<dataset> rows joined to ANY canonical_to_sink
    link of the run (see 011), so a Decision-#6 fan-out (one run, K
    canonical_to_sink links for the SAME upstream rows) double-counts: a 5-row
    upstream fanned to 2 sinks stamps 10 rows => run-scoped recon reports
    discrepancy=-5 / 'double_count' — a FALSE breach. The fix is a per-OUTPUT
    recon keyed on the link: cp.reconcile_sink_link(link_id, source_count) counts
    ONLY rows stamped with THAT link, so each correct fan-out output reconciles
    'ok'. This probe asserts BOTH: the run-scoped check still (correctly, by its
    own contract) double-counts, AND the per-link check is clean per output."""
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
        link_ids = {}
        for sink_type, path in (("postgres", "postgres://ods/orders"),
                                ("kafka", "kafka://ods.orders")):
            link_ids[sink_type] = lineage.write_link_then_rows(
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

        # Run-scoped recon STILL double-counts (that is its contract; it cannot
        # tell a legitimate fan-out from a duplicate) — documents WHY per-output.
        recon.reconcile_sink(conn, run_id=sink_run, source_count=5, commit=False)
        status, disc, accounted = conn.execute(
            "SELECT status, discrepancy, accounted_count "
            "FROM cp.reconciliation_log WHERE run_id=%s AND check_type='sink_graph'",
            [sink_run]).fetchone()
        assert accounted == 10 and disc == -5 and status == "double_count", (
            status, disc, accounted)

        # FIXED: the per-OUTPUT recon scopes accounted to ONE link's rows, so
        # each correct fan-out output reconciles cleanly (5 == 5, ok).
        for sink_type, link_id in link_ids.items():
            recon.reconcile_sink_link(
                conn, lineage_link_id=link_id, source_count=5, commit=False)
            st, ds_disc, acc = conn.execute(
                "SELECT status, discrepancy, accounted_count "
                "FROM cp.reconciliation_log "
                "WHERE check_type='sink_link' AND metrics->>'lineage_link_id'=%s",
                [str(link_id)]).fetchone()
            assert (st, ds_disc, acc) == ("ok", 0, 5), (sink_type, st, ds_disc, acc)
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
      (2) a downstream consumer discovering by PATH alone now RAISES 'ambiguous'
          (THEME B / 015 — no more silent stale pick) and must disambiguate by
          content_hash to reach the EXACT version it intends;
      (3) there is NO superseded/valid flag and NO recon check that marks the
          orphaned old link — it silently rots. There is no automatic orphan
          detection in cp.*; nothing flags it. (THE HEADLINE: 015 closed the
          silent-path-pick, but the orphaned stale link STILL has no
          supersede/validity flag and no orphan recon — the rot is UNCHANGED.)"""
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

        # (2) Path-alone discovery now RAISES 'ambiguous' (THEME B/015) instead
        # of silently reaching one of the two — the consumer must disambiguate.
        conn.execute("SAVEPOINT c_orphan_path")
        with pytest.raises(psycopg.errors.RaiseException, match="ambiguous"):
            runs.run_output_link(
                conn, run_id=run_id, edge_type="raw_to_curated", target_path=path)
        conn.execute("ROLLBACK TO SAVEPOINT c_orphan_path")
        # The stale link is STILL discoverable IF the consumer asks for it by
        # content_hash — it was not removed; nothing superseded it (-> point 3).
        got_stale = runs.run_output_link(
            conn, run_id=run_id, edge_type="raw_to_curated",
            target_path=path, content_hash="stale")
        assert str(got_stale) == str(link_old), (got_stale, link_old)

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
