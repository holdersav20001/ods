"""Migration 028 — deterministic monotonic tie-break in run-grain discovery.

PRODUCTION FRAGILITY (root cause):
    cp.latest_succeeded_run / cp.succeeded_runs ordered by
    ``finished_at DESC NULLS LAST, run_id DESC``. finished_at is clock_timestamp()
    (007), which advances within a txn — but two runs can still land on the SAME
    microsecond (a real tie under load, or a refeed whose finished_at coincides
    with an existing ingest's). The secondary key ``run_id DESC`` is a RANDOM
    gen_random_uuid(), so on a tie discovery picks NON-DETERMINISTICALLY — a
    refeed could re-canonicalize a STALE ingest, or the canonical run could bind
    to an arbitrary upstream.

FIX (028):
    Add a monotonic insert-order key ``cp.run_log.seq BIGSERIAL`` and change ONLY
    the discovery ORDER BY to ``finished_at DESC NULLS LAST, seq DESC``. On a true
    finished_at tie, the run CREATED LATER (higher seq) wins deterministically —
    the correct "latest succeeded" semantics — instead of a random uuid coin-flip.

These tests force the tie EXPLICITLY (direct UPDATE setting both runs'
finished_at equal) so the ORDER BY tie-break is the ONLY thing that can decide
the winner. They run inside the rolled-back `conn` fixture (commit=False), so
their passing proves the fix in-transaction with zero committed-state leakage.
"""
import datetime
import uuid

from control import runs

BD = datetime.date(2025, 8, 1)
DOMAIN = "sales"
DATASET = "orders"
PIPE = "ingestion"


def _start_and_finalise(conn):
    """Start + finalise one succeeded ingestion run for the BD slice (no commit)."""
    run_id = runs.start(
        conn,
        workflow_run_id=str(uuid.uuid4()),
        pipeline_type=PIPE,
        domain=DOMAIN,
        dataset=DATASET,
        business_date=BD,
        trigger_type="manual",
        commit=False,
    )
    runs.finalise(conn, run_id, status="succeeded", record_count_out=1,
                  commit=False)
    return run_id


def _force_finished_at_tie(conn, *run_ids):
    """Stamp every given run's finished_at to ONE identical instant, defeating the
    clock_timestamp() advance so only the (seq) tie-break can pick a winner.

    Returns (seq_by_run_id) so the test can assert the higher-seq run wins."""
    conn.execute(
        "UPDATE cp.run_log SET finished_at = now() WHERE run_id = ANY(%s)",
        [list(run_ids)],
    )
    rows = conn.execute(
        "SELECT run_id::text, seq, finished_at FROM cp.run_log "
        "WHERE run_id = ANY(%s)",
        [list(run_ids)],
    ).fetchall()
    # All finished_at must now be byte-identical (a genuine tie).
    fins = {r[2] for r in rows}
    assert len(fins) == 1, f"failed to force a finished_at tie: {fins}"
    return {r[0]: r[1] for r in rows}


def test_tied_finished_at_resolves_to_higher_seq(conn):
    """TWO succeeded runs, SAME finished_at: discovery returns the one with the
    HIGHER seq (created later), NOT a random uuid pick."""
    first = _start_and_finalise(conn)
    second = _start_and_finalise(conn)
    assert first != second

    seqs = _force_finished_at_tie(conn, first, second)
    assert seqs[second] > seqs[first], "second run must have the higher seq"

    discovered = runs.latest_succeeded_run(
        conn, domain=DOMAIN, dataset=DATASET, business_date=BD, pipeline_type=PIPE)
    assert discovered == second, (
        f"on a finished_at tie discovery must pick the higher-seq run {second}, "
        f"got {discovered} (first={first})")


def test_tie_break_is_deterministic_repeated(conn):
    """Re-query the tied state several times: discovery ALWAYS returns the same
    higher-seq run. (A random-uuid tie-break would flap across calls.)"""
    first = _start_and_finalise(conn)
    second = _start_and_finalise(conn)
    _force_finished_at_tie(conn, first, second)

    picks = {
        runs.latest_succeeded_run(
            conn, domain=DOMAIN, dataset=DATASET, business_date=BD,
            pipeline_type=PIPE)
        for _ in range(10)
    }
    assert picks == {second}, (
        f"tie-break non-deterministic across repeats: {picks} (want only {second})")


def test_higher_seq_wins_regardless_of_insert_winner(conn):
    """Construct the tie with THREE runs; the latest-created (max seq) wins, even
    though all three share finished_at. Proves seq — not run_id — is the key."""
    runs_ids = [_start_and_finalise(conn) for _ in range(3)]
    seqs = _force_finished_at_tie(conn, *runs_ids)
    expected = max(runs_ids, key=lambda r: seqs[r])

    discovered = runs.latest_succeeded_run(
        conn, domain=DOMAIN, dataset=DATASET, business_date=BD, pipeline_type=PIPE)
    assert discovered == expected, (
        f"max-seq run {expected} must win the tie, got {discovered}")


def test_succeeded_runs_newest_first_by_finished_at_then_seq(conn):
    """cp.succeeded_runs returns ALL succeeded runs for the slice ordered
    newest-first by (finished_at DESC, seq DESC). With a forced finished_at tie
    the order is fully determined by seq DESC."""
    a = _start_and_finalise(conn)
    b = _start_and_finalise(conn)
    c = _start_and_finalise(conn)
    seqs = _force_finished_at_tie(conn, a, b, c)

    got = runs.succeeded_runs(
        conn, domain=DOMAIN, dataset=DATASET, business_date=BD, pipeline_type=PIPE)
    # restrict to the three we created (slice may be otherwise empty in-txn)
    trio = [r for r in got if r in {a, b, c}]
    expected = sorted([a, b, c], key=lambda r: seqs[r], reverse=True)
    assert trio == expected, (
        f"succeeded_runs not seq-DESC ordered on a tie: got {trio}, want {expected}")


def test_distinct_finished_at_still_orders_by_finished_at(conn):
    """seq is ONLY a tie-break: when finished_at differs (the common case, via
    clock_timestamp), the newer finished_at wins even if it has a LOWER seq.
    We give the EARLIER-created run (lower seq) the LATER finished_at and assert
    it is discovered — proving seq does not override finished_at."""
    early = _start_and_finalise(conn)   # lower seq
    late = _start_and_finalise(conn)    # higher seq
    # Invert: make the lower-seq `early` run finish AFTER the higher-seq `late`.
    conn.execute(
        "UPDATE cp.run_log SET finished_at = now() WHERE run_id = %s", [late])
    conn.execute(
        "UPDATE cp.run_log SET finished_at = now() + interval '1 second' "
        "WHERE run_id = %s", [early])

    discovered = runs.latest_succeeded_run(
        conn, domain=DOMAIN, dataset=DATASET, business_date=BD, pipeline_type=PIPE)
    assert discovered == early, (
        "finished_at must dominate seq: the later-finished (lower-seq) run wins")


def test_restart_refinalised_run_is_discovered_latest(conn):
    """RESTART-IDENTITY (013) intact: a restart reuses the run row (seq UNCHANGED)
    and refreshes finished_at on re-finalise, so it sorts latest by finished_at —
    seq is never even consulted. Confirms 028 did not disturb restart semantics."""
    wfid = str(uuid.uuid4())

    def _attempt():
        rid = runs.start(
            conn, workflow_run_id=wfid, pipeline_type=PIPE, domain=DOMAIN,
            dataset=DATASET, business_date=BD, trigger_type="manual", commit=False)
        runs.finalise(conn, rid, status="succeeded", record_count_out=1,
                      commit=False)
        return rid

    # An unrelated competing run for the same slice.
    other = _start_and_finalise(conn)

    first_attempt = _attempt()
    seq_first = conn.execute(
        "SELECT seq FROM cp.run_log WHERE run_id=%s", [first_attempt]).fetchone()[0]

    # Airflow clear-task restart under the SAME workflow_run_id -> 013 reuses row.
    restart = _attempt()
    assert restart == first_attempt, "013 restart-identity must reuse the run row"
    seq_restart = conn.execute(
        "SELECT seq FROM cp.run_log WHERE run_id=%s", [restart]).fetchone()[0]
    assert seq_restart == seq_first, "restart must NOT mint a new seq (row reused)"

    # The restart re-finalised newest -> it is the discovered latest over `other`.
    discovered = runs.latest_succeeded_run(
        conn, domain=DOMAIN, dataset=DATASET, business_date=BD, pipeline_type=PIPE)
    assert discovered == restart, (
        f"restart-refinalised run {restart} must be latest, got {discovered} "
        f"(other={other})")
