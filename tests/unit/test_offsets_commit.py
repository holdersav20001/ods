"""Exactly-once offset commit tests (B8 / T6).

Asserts that ``ods_pipeline.offsets.persist_ranges`` writes per-partition
ranges to ``pipeline.run_kafka_offsets`` and that ``has_recorded_offsets``
detects them — the resume-time idempotency primitive used by ``runs.start``
to skip republish on retry.

Crash simulation: drives the (Kafka-tx-committed, PG-not-yet-committed)
sequence and verifies the second attempt sees no offsets and would republish,
while a successful flow leaves rows that mark the work as already-done.
"""
from __future__ import annotations

import uuid

import psycopg2
import pytest

from ods_pipeline import offsets, runs


@pytest.fixture
def pg_conn():
    conn = psycopg2.connect(host="localhost", port=5440, dbname="ods_dev",
                            user="ods", password="ods")
    yield conn
    conn.rollback()
    conn.close()


@pytest.fixture
def run_id(pg_conn):
    rid = str(uuid.uuid4())
    runs.start(
        pg_conn,
        run_id=rid,
        pipeline_type="file",
        domain="insurance",
        dataset="policies",
        business_date="2026-05-02",
    )
    yield rid
    with pg_conn.cursor() as cur:
        cur.execute("DELETE FROM pipeline.run_kafka_offsets WHERE run_id=%s", (rid,))
        cur.execute("DELETE FROM pipeline.run_log WHERE run_id=%s", (rid,))
    pg_conn.commit()


def test_persist_ranges_writes_one_row_per_partition(pg_conn, run_id):
    n = offsets.persist_ranges(
        pg_conn,
        run_id=run_id,
        stage="kafka_publish",
        topic="ods.insurance.policies",
        ranges={0: (100, 105), 1: (200, 215)},
    )
    assert n == 2
    pg_conn.rollback()  # confirm commit happened

    out = offsets.read_ranges(pg_conn, run_id=run_id)
    assert out == {"ods.insurance.policies": {0: (100, 105), 1: (200, 215)}}


def test_persist_ranges_with_commit_false_holds_tx(pg_conn, run_id):
    """When commit=False the caller owns the tx — rollback discards work."""
    offsets.persist_ranges(
        pg_conn,
        run_id=run_id,
        stage="kafka_publish",
        topic="ods.insurance.policies",
        ranges={0: (10, 20)},
        commit=False,
    )
    pg_conn.rollback()  # caller-owned rollback discards
    assert offsets.has_recorded_offsets(pg_conn, run_id=run_id, stage="kafka_publish") is False


def test_persist_ranges_upsert_replaces_existing(pg_conn, run_id):
    offsets.persist_ranges(
        pg_conn,
        run_id=run_id, stage="kafka_publish",
        topic="ods.insurance.policies",
        ranges={0: (100, 105)},
    )
    offsets.persist_ranges(
        pg_conn,
        run_id=run_id, stage="kafka_publish",
        topic="ods.insurance.policies",
        ranges={0: (100, 200)},  # extended range — same key
    )
    out = offsets.read_ranges(pg_conn, run_id=run_id)
    assert out == {"ods.insurance.policies": {0: (100, 200)}}


def test_persist_ranges_empty_is_noop(pg_conn, run_id):
    n = offsets.persist_ranges(
        pg_conn,
        run_id=run_id, stage="kafka_publish",
        topic="ods.insurance.policies", ranges={},
    )
    assert n == 0
    assert offsets.has_recorded_offsets(pg_conn, run_id=run_id, stage="kafka_publish") is False


def test_has_recorded_offsets_returns_false_for_unknown_run(pg_conn):
    assert offsets.has_recorded_offsets(
        pg_conn, run_id=str(uuid.uuid4()), stage="kafka_publish"
    ) is False


def test_has_recorded_offsets_returns_true_after_persist(pg_conn, run_id):
    offsets.persist_ranges(
        pg_conn,
        run_id=run_id, stage="kafka_publish",
        topic="ods.insurance.policies",
        ranges={0: (1, 2)},
    )
    assert offsets.has_recorded_offsets(
        pg_conn, run_id=run_id, stage="kafka_publish"
    ) is True


def test_crash_simulation_kafka_committed_pg_not_committed(pg_conn, run_id):
    """B8 exactly-once: simulate crash between Kafka commit and PG commit.

    Sequence: caller does producer.commit_transaction() (Kafka side), then
    persist_ranges(commit=False), then crashes BEFORE conn.commit() of the
    run-status update. On restart the resume-time check must see no offset
    rows and would correctly republish.
    """
    # Kafka tx commits (we don't model that here; assume done).
    offsets.persist_ranges(
        pg_conn,
        run_id=run_id, stage="kafka_publish",
        topic="ods.insurance.policies",
        ranges={0: (10, 20)},
        commit=False,
    )
    # CRASH — connection lost without commit.
    pg_conn.rollback()

    # Resume: a fresh check sees no rows. Caller will republish (Kafka idempotence
    # makes that safe; tracker.delivered_count guards against partial retry).
    assert offsets.has_recorded_offsets(
        pg_conn, run_id=run_id, stage="kafka_publish"
    ) is False


def test_persisted_run_marks_resume_skip(pg_conn, run_id):
    """Successful flow: PG commit happens — resume sees rows, skip republish."""
    offsets.persist_ranges(
        pg_conn,
        run_id=run_id, stage="kafka_publish",
        topic="ods.insurance.policies",
        ranges={0: (10, 20)},
        commit=True,  # atomic with run-status in real flow
    )
    assert offsets.has_recorded_offsets(
        pg_conn, run_id=run_id, stage="kafka_publish"
    ) is True
