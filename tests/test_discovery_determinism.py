"""P4 item 1 — discovery determinism in-transaction (the 007 clock_timestamp fix).

BUG (found in P3d): finished_at was stamped with now() (transaction_timestamp),
fixed for the whole transaction. Two ingestion runs started AND finalised inside
ONE transaction therefore got IDENTICAL finished_at, so
cp.latest_succeeded_run's tiebreak (ORDER BY finished_at DESC, run_id DESC) fell
to the RANDOM run_id UUID — non-deterministic discovery in-transaction (this is
why the X5 replay test needed a committing connection).

FIX (007): stamp finished_at with clock_timestamp(), which advances WITHIN a
transaction. The second run finalised in sequence now has a strictly newer
finished_at, so discovery deterministically returns it — even uncommitted.

These tests run entirely inside the rolled-back `conn` fixture (no commit), so
their passing IS the proof the fix works in-transaction.
"""
import datetime
import uuid

from control import runs

BD = datetime.date(2025, 7, 1)


def _start_and_finalise_ingest(conn):
    """Start + finalise one ingestion run for the BD slice, all commit=False."""
    wfid = str(uuid.uuid4())
    run_id = runs.start(
        conn,
        workflow_run_id=wfid,
        pipeline_type="ingestion",
        domain="sales",
        dataset="orders",
        business_date=BD,
        trigger_type="manual",
        commit=False,
    )
    runs.finalise(conn, run_id, status="succeeded", record_count_out=1,
                  commit=False)
    return run_id


def test_latest_succeeded_is_second_run_in_one_transaction(conn):
    """Two ingestion runs finalised in sequence in ONE transaction: discovery
    returns the SECOND (later clock_timestamp), deterministically."""
    first = _start_and_finalise_ingest(conn)
    second = _start_and_finalise_ingest(conn)
    assert first != second

    # clock_timestamp advances within the txn -> second.finished_at > first's.
    f1, f2 = conn.execute(
        "SELECT (SELECT finished_at FROM cp.run_log WHERE run_id=%s),"
        "       (SELECT finished_at FROM cp.run_log WHERE run_id=%s)",
        (first, second),
    ).fetchone()
    assert f2 > f1, "clock_timestamp did not advance within the transaction"

    discovered = runs.latest_succeeded_run(
        conn, domain="sales", dataset="orders", business_date=BD,
        pipeline_type="ingestion")
    assert discovered == second, (
        f"discovery non-deterministic: returned {discovered}, expected second "
        f"{second} (first was {first})")


def test_discovery_deterministic_repeated(conn):
    """Re-run the in-transaction race a few times; discovery ALWAYS selects the
    last-finalised run. (If discovery were tiebreaking on random run_id this
    would flap.)"""
    last = None
    for _ in range(5):
        last = _start_and_finalise_ingest(conn)
        discovered = runs.latest_succeeded_run(
            conn, domain="sales", dataset="orders", business_date=BD,
            pipeline_type="ingestion")
        assert discovered == last, (
            f"discovery picked {discovered}, expected most-recent {last}")


def test_succeeded_runs_ordered_by_clock(conn):
    """cp.succeeded_runs returns ALL succeeded runs for the slice; with the
    clock fix their finished_at values are strictly increasing in creation
    order, so the set is complete and the ordering is stable."""
    ids = [_start_and_finalise_ingest(conn) for _ in range(3)]
    got = set(runs.succeeded_runs(
        conn, domain="sales", dataset="orders", business_date=BD,
        pipeline_type="ingestion"))
    assert set(ids) <= got, "succeeded_runs dropped a run"
