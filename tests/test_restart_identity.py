"""P10-A — restart-identity: cp.start_run is idempotent on an Airflow clear-task.

These tests pin the FIX delivered by migration 013 (the flip of the team-R1
restart probes lives in tests/test_team_r1.py; this file adds the harness-modelled
restart and the same-bytes idempotency / discovery-determinism guarantees, plus
the sink-exclusion correctness that keeps fan-out untouched).

THE RULE (spec decision #5): a restart re-runs FROM THAT TASK FORWARD under the
SAME workflow_run_id; re-execution is idempotent. Migration 013 makes start_run
upsert on a PARTIAL run-identity index
  (workflow_run_id, pipeline_type, domain, dataset, business_date,
   COALESCE(file_id, sentinel))  WHERE pipeline_type <> 'sink'
so a re-run REUSES the existing run_id. Sink is excluded (terminal — nothing
DISCOVERS a sink run, so a duplicate sink run cannot cause a downstream
double-count; and the legitimate fan-out model mints one sink run per
destination). Sink-restart ROW idempotency + per-output recon are P10-C/P10-D.

Isolation: the rollback `conn` fixture; every write commit=False; namespace
p10a_*; schema NEVER dropped.
"""
import uuid

import pytest

from control import lineage, runs
from harness import composers, fakes

DOM = "p10a_dom"
BDATE = "2026-05-30"


def _ds(tag):
    return f"p10a_{tag}_{uuid.uuid4().hex[:8]}"


def _file(ds, md5, n=5):
    return {
        "s3_raw_path": f"s3://raw/{ds}/{md5}.csv",
        "file_md5": md5,
        "business_date": BDATE,
        "domain": DOM,
        "dataset": ds,
        "record_count": n,
    }


# --------------------------------------------------------------------------- #
# 1. Harness MODELS restart: fake_ingest twice for one (wfid, file) => one run. #
# --------------------------------------------------------------------------- #
def test_harness_restart_ingest_observes_one_run_id(conn):
    """The restart was never MODELLED before — that is why the bug shipped. The
    harness now exposes restart_ingest: re-run the ingest stage under the SAME
    workflow_run_id. With 013 the second run REUSES the first run_id, so a test
    can call fake_ingest twice for one (workflow_run_id, file) and observe ONE
    run_id and exactly ONE succeeded ingestion run for the slice."""
    try:
        ds = _ds("harness")
        md5 = uuid.uuid4().hex
        wfid = str(uuid.uuid4())
        f = _file(ds, md5, n=5)

        first = fakes.fake_ingest(conn, workflow_run_id=wfid, file=f,
                                  commit=False)
        # Airflow clear-task: re-run the ingest stage, SAME wfid, SAME bytes.
        again = composers.restart_ingest(conn, workflow_run_id=wfid, file=f,
                                          commit=False)

        assert again["run_id"] == first["run_id"], (
            "restart_ingest under the same workflow_run_id REUSES the run_id")
        assert again["file_id"] == first["file_id"]
        assert again["link_id"] == first["link_id"]

        succeeded = runs.succeeded_runs(
            conn, domain=DOM, dataset=ds, business_date=BDATE,
            pipeline_type="ingestion")
        assert succeeded == [first["run_id"]], succeeded
        rows = conn.execute(
            "SELECT count(*) FROM cp.run_log WHERE workflow_run_id=%s "
            "AND pipeline_type='ingestion'", [wfid]).fetchone()[0]
        assert rows == 1, "exactly one ingestion run_log row for the slice"
    finally:
        conn.rollback()


# --------------------------------------------------------------------------- #
# 2. Same-bytes restart is idempotent: same link, stable counts (dedup engages).#
# --------------------------------------------------------------------------- #
def test_same_bytes_restart_is_idempotent_counts_stable(conn):
    """Re-running an UNCHANGED task produces the SAME link and stable counts. Now
    that start_run reuses the run_id, write_lineage_link's 5-part dedup key
    (consumer_run_id-led) ENGAGES across the restart: no new link, the run's
    record_count_out is unchanged, and there is exactly one raw_to_curated link
    for the run."""
    try:
        ds = _ds("idem")
        md5 = uuid.uuid4().hex
        wfid = str(uuid.uuid4())
        f = _file(ds, md5, n=7)

        first = fakes.fake_ingest(conn, workflow_run_id=wfid, file=f,
                                  commit=False)
        rc_before = runs.run_record_count(conn, run_id=first["run_id"])

        again = composers.restart_ingest(conn, workflow_run_id=wfid, file=f,
                                          commit=False)
        assert again["link_id"] == first["link_id"], "same link on same-bytes restart"

        rc_after = runs.run_record_count(conn, run_id=first["run_id"])
        assert rc_after == rc_before == 7, (rc_before, rc_after)

        n_links = conn.execute(
            "SELECT count(*) FROM cp.lineage_link WHERE consumer_run_id=%s "
            "AND edge_type='raw_to_curated'", [first["run_id"]]).fetchone()[0]
        assert n_links == 1, "no duplicate link minted on restart (dedup engaged)"

        # The run is back to a terminal 'succeeded' state (the re-run finalised it).
        status = conn.execute(
            "SELECT status FROM cp.run_log WHERE run_id=%s",
            [first["run_id"]]).fetchone()[0]
        assert status == "succeeded", status
    finally:
        conn.rollback()


# --------------------------------------------------------------------------- #
# 3. Discovery is deterministic: one reused run, no tie-break dependence.       #
# --------------------------------------------------------------------------- #
def test_discovery_deterministic_after_restart(conn):
    """latest_succeeded_run returns the one reused run deterministically — there
    is no second run to tie-break against on finished_at/run_id."""
    try:
        ds = _ds("disc")
        md5 = uuid.uuid4().hex
        wfid = str(uuid.uuid4())
        f = _file(ds, md5, n=5)

        first = fakes.fake_ingest(conn, workflow_run_id=wfid, file=f,
                                  commit=False)
        composers.restart_ingest(conn, workflow_run_id=wfid, file=f, commit=False)
        composers.restart_ingest(conn, workflow_run_id=wfid, file=f, commit=False)

        latest = runs.latest_succeeded_run(
            conn, domain=DOM, dataset=ds, business_date=BDATE,
            pipeline_type="ingestion")
        assert latest == first["run_id"], (
            "discovery returns the one reused run, deterministically")
    finally:
        conn.rollback()


# --------------------------------------------------------------------------- #
# 4. Restart of a SLICE-grained stage (canonicalize) also collapses.           #
# --------------------------------------------------------------------------- #
def test_restart_canonicalize_slice_grained_reuses_run(conn):
    """Canonicalize is one-run-per-slice (file_id NULL -> sentinel). A restart of
    the canonicalize task under the same workflow_run_id reuses its run too — the
    COALESCE(file_id, sentinel) makes the slice-grained stages collapse."""
    try:
        ds = _ds("canon")
        md5 = uuid.uuid4().hex
        wfid = str(uuid.uuid4())
        f = _file(ds, md5, n=5)

        fakes.fake_ingest(conn, workflow_run_id=wfid, file=f, commit=False)
        c1 = fakes.fake_canonicalize(
            conn, workflow_run_id=wfid, domain=DOM, dataset=ds,
            business_date=BDATE, record_count=5, commit=False)
        # Clear-task on the canonicalize stage: SAME wfid, SAME slice.
        c2 = fakes.fake_canonicalize(
            conn, workflow_run_id=wfid, domain=DOM, dataset=ds,
            business_date=BDATE, record_count=5, commit=False)
        assert c2["run_id"] == c1["run_id"], (
            "canonicalize restart reuses the one slice run (file_id sentinel)")
        canon_runs = conn.execute(
            "SELECT count(*) FROM cp.run_log WHERE workflow_run_id=%s "
            "AND pipeline_type='canonicalization'", [wfid]).fetchone()[0]
        assert canon_runs == 1, canon_runs
    finally:
        conn.rollback()


# --------------------------------------------------------------------------- #
# 5. SINK is EXCLUDED: fan-out keeps minting one sink run PER destination.      #
#    (Terminal stage — nothing discovers it; partial index WHERE pt<>'sink'.)   #
# --------------------------------------------------------------------------- #
def test_sink_excluded_fanout_still_two_distinct_runs(conn):
    """The partial index excludes sink, so the legitimate fan-out (one canonical
    sinked to TWO destinations via two fake_sink calls sharing
    (wfid,'sink',slice,file_id NULL)) keeps producing TWO DISTINCT sink runs — it
    does NOT collapse. This is the orthogonal flow P10-A intentionally leaves
    untouched (per-output recon + one-run fan-out are P10-C/P10-D)."""
    try:
        ds = "orders"  # the only ods.* target table
        md5 = uuid.uuid4().hex
        f = _file(ds, md5, n=5)
        res = composers.run_to_fanout_sinks(
            conn, file=f, sink_types=("postgres", "kafka"), commit=False)
        wfid = res["workflow_run_id"]
        sink_runs = {res["sinks"]["postgres"]["run_id"],
                     res["sinks"]["kafka"]["run_id"]}
        assert len(sink_runs) == 2, ("sink fan-out still mints one run per "
                                     "destination (partial index excludes sink)")
        n_sink_runs = conn.execute(
            "SELECT count(*) FROM cp.run_log WHERE workflow_run_id=%s "
            "AND pipeline_type='sink'", [wfid]).fetchone()[0]
        assert n_sink_runs == 2, n_sink_runs
    finally:
        conn.rollback()
