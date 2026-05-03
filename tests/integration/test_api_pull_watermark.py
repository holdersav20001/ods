"""WatermarkStore integration tests against the real ``api_pull_watermark`` table.

These exercise the pending/committed cursor split, lock acquire/release,
and promote/clear semantics. Mocking these would only verify our SQL
spelling — the table contract is what matters, so we hit Postgres.
"""
from __future__ import annotations

import uuid

import pytest

from ods_pipeline.ingest.api_pull import WatermarkStore


DOMAIN = "insurance"
DATASET = "api_pull_watermark_test"
SOURCE_APPLICATION = "demo_api_test"
CURSOR_TYPE = "since_timestamp"


@pytest.fixture
def store(pg_conn):
    s = WatermarkStore(pg_conn)
    yield s
    with pg_conn.cursor() as cur:
        cur.execute(
            "DELETE FROM pipeline.api_pull_watermark "
            "WHERE domain=%s AND dataset=%s AND source_application=%s",
            (DOMAIN, DATASET, SOURCE_APPLICATION),
        )
    pg_conn.commit()


def _run() -> str:
    return str(uuid.uuid4())


def test_initial_read_creates_row_with_null_cursors(store):
    row = store.read(
        domain=DOMAIN,
        dataset=DATASET,
        source_application=SOURCE_APPLICATION,
        cursor_type=CURSOR_TYPE,
    )
    assert row.committed_cursor_value is None
    assert row.pending_cursor_value is None
    assert row.pending_run_id is None
    assert row.last_successful_run_id is None
    assert row.locked is False
    assert row.cursor_type == CURSOR_TYPE


def test_lock_then_unlock(store):
    store.read(domain=DOMAIN, dataset=DATASET,
               source_application=SOURCE_APPLICATION, cursor_type=CURSOR_TYPE)
    run_id = _run()
    assert store.try_lock(domain=DOMAIN, dataset=DATASET,
                          source_application=SOURCE_APPLICATION, run_id=run_id)
    # Second lock attempt while held returns False.
    assert not store.try_lock(domain=DOMAIN, dataset=DATASET,
                              source_application=SOURCE_APPLICATION,
                              run_id=_run())
    store.unlock(domain=DOMAIN, dataset=DATASET,
                 source_application=SOURCE_APPLICATION)
    # After unlock another caller can acquire.
    assert store.try_lock(domain=DOMAIN, dataset=DATASET,
                          source_application=SOURCE_APPLICATION, run_id=_run())
    store.unlock(domain=DOMAIN, dataset=DATASET,
                 source_application=SOURCE_APPLICATION)


def test_promote_moves_pending_to_committed(store):
    store.read(domain=DOMAIN, dataset=DATASET,
               source_application=SOURCE_APPLICATION, cursor_type=CURSOR_TYPE)
    run_id = _run()
    store.try_lock(domain=DOMAIN, dataset=DATASET,
                   source_application=SOURCE_APPLICATION, run_id=run_id)
    store.record_pending(domain=DOMAIN, dataset=DATASET,
                         source_application=SOURCE_APPLICATION,
                         run_id=run_id,
                         new_cursor_value="2026-04-03T00:00:00Z")
    store.unlock(domain=DOMAIN, dataset=DATASET,
                 source_application=SOURCE_APPLICATION)

    row = store.read(domain=DOMAIN, dataset=DATASET,
                     source_application=SOURCE_APPLICATION, cursor_type=CURSOR_TYPE)
    assert row.pending_cursor_value == "2026-04-03T00:00:00Z"
    assert row.pending_run_id == run_id
    assert row.committed_cursor_value is None

    assert store.promote(domain=DOMAIN, dataset=DATASET,
                         source_application=SOURCE_APPLICATION, run_id=run_id)

    row = store.read(domain=DOMAIN, dataset=DATASET,
                     source_application=SOURCE_APPLICATION, cursor_type=CURSOR_TYPE)
    assert row.committed_cursor_value == "2026-04-03T00:00:00Z"
    assert row.pending_cursor_value is None
    assert row.pending_run_id is None
    assert row.last_successful_run_id == run_id


def test_clear_pending_leaves_committed_intact(store):
    store.read(domain=DOMAIN, dataset=DATASET,
               source_application=SOURCE_APPLICATION, cursor_type=CURSOR_TYPE)
    # First successful poll: promote to committed.
    run_a = _run()
    store.try_lock(domain=DOMAIN, dataset=DATASET,
                   source_application=SOURCE_APPLICATION, run_id=run_a)
    store.record_pending(domain=DOMAIN, dataset=DATASET,
                         source_application=SOURCE_APPLICATION, run_id=run_a,
                         new_cursor_value="2026-04-01T00:00:00Z")
    store.unlock(domain=DOMAIN, dataset=DATASET,
                 source_application=SOURCE_APPLICATION)
    store.promote(domain=DOMAIN, dataset=DATASET,
                  source_application=SOURCE_APPLICATION, run_id=run_a)

    # Second poll archives but downstream fails.
    run_b = _run()
    store.try_lock(domain=DOMAIN, dataset=DATASET,
                   source_application=SOURCE_APPLICATION, run_id=run_b)
    store.record_pending(domain=DOMAIN, dataset=DATASET,
                         source_application=SOURCE_APPLICATION, run_id=run_b,
                         new_cursor_value="2026-04-02T00:00:00Z")
    store.unlock(domain=DOMAIN, dataset=DATASET,
                 source_application=SOURCE_APPLICATION)
    store.clear_pending(domain=DOMAIN, dataset=DATASET,
                        source_application=SOURCE_APPLICATION, run_id=run_b)

    row = store.read(domain=DOMAIN, dataset=DATASET,
                     source_application=SOURCE_APPLICATION, cursor_type=CURSOR_TYPE)
    # Committed cursor preserved from successful run_a.
    assert row.committed_cursor_value == "2026-04-01T00:00:00Z"
    # Pending discarded.
    assert row.pending_cursor_value is None
    assert row.pending_run_id is None
    # last_successful_run_id still points at run_a.
    assert row.last_successful_run_id == run_a


def test_promote_only_for_owner_run(store):
    store.read(domain=DOMAIN, dataset=DATASET,
               source_application=SOURCE_APPLICATION, cursor_type=CURSOR_TYPE)
    run_a = _run()
    other = _run()
    store.try_lock(domain=DOMAIN, dataset=DATASET,
                   source_application=SOURCE_APPLICATION, run_id=run_a)
    store.record_pending(domain=DOMAIN, dataset=DATASET,
                         source_application=SOURCE_APPLICATION, run_id=run_a,
                         new_cursor_value="2026-04-09T00:00:00Z")
    store.unlock(domain=DOMAIN, dataset=DATASET,
                 source_application=SOURCE_APPLICATION)
    # A different run_id must not promote run_a's pending cursor.
    assert not store.promote(domain=DOMAIN, dataset=DATASET,
                             source_application=SOURCE_APPLICATION,
                             run_id=other)
    row = store.read(domain=DOMAIN, dataset=DATASET,
                     source_application=SOURCE_APPLICATION,
                     cursor_type=CURSOR_TYPE)
    assert row.committed_cursor_value is None
    assert row.pending_run_id == run_a
