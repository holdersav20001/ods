"""Watermark promotion linkage safety.

Proves the P0 invariant from docs/api-pull-backlog.md item 6: a replay
of the same file_id, or a concurrent dag_ingest run that was NOT
launched by this api_pull poll, cannot promote or clear the pending
cursor of this poll.

The mechanism under test is:

  1. ``dag_ingest.init_run`` records a ``triggered_by_api_pull`` edge
     in ``run_log.orchestrators`` when the trigger conf carries
     ``triggered_by_run_id``.
  2. ``dag_api_pull._ingest_status_for_api_pull_run`` looks up the
     downstream parent run by JSONB containment on that edge — not by
     "latest run for this file_id".

The test inserts run_log rows directly so we exercise the SQL and
JSONB containment guarantee independent of the Airflow execution path.
"""
from __future__ import annotations

import json
import uuid

import pytest

from ods_pipeline.ingest.api_pull import (
    WatermarkStore,
    derive_dag_ingest_parent_run_id,
    ingest_status_for_api_pull_run,
)


DOMAIN = "insurance"
DATASET = "api_pull_linkage_test"
SOURCE_APPLICATION = "demo_api_test"


def _insert_run_log(conn, *, run_id, file_id, orchestrators, status="succeeded"):
    """Insert a synthetic s3_batch run_log row used as a stand-in for a
    dag_ingest parent run. Real dag_ingest goes through runs.start, but
    the linkage we're testing is purely about JSONB containment."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline.run_log
                (run_id, pipeline_type, domain, dataset, business_date,
                 file_id, status, orchestrators)
            VALUES (%s, 'orchestration', %s, %s, %s, %s, %s, %s::jsonb)
            ON CONFLICT (run_id) DO NOTHING
            """,
            (
                run_id, DOMAIN, DATASET, "2026-05-02",
                file_id, status, json.dumps(orchestrators),
            ),
        )
    conn.commit()


def _insert_file_catalogue(conn, *, file_id, run_id):
    """File catalogue row so the run_log file_id FK / lookup tooling has
    something to point at. Pipeline schema requires file_md5 on this
    table; we use a synthetic value."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline.file_catalogue
                (file_id, domain, dataset, business_date, file_md5,
                 s3_raw_path, state, last_run_id)
            VALUES (%s,%s,%s,%s,%s,%s,'received',%s)
            ON CONFLICT DO NOTHING
            """,
            (
                file_id, DOMAIN, DATASET, "2026-05-02",
                "0" * 32, f"s3://test/{file_id}.jsonl.gz", run_id,
            ),
        )
    conn.commit()


@pytest.fixture
def cleanup(pg_conn):
    yield
    with pg_conn.cursor() as cur:
        cur.execute(
            "DELETE FROM pipeline.api_pull_watermark "
            "WHERE domain=%s AND dataset=%s AND source_application=%s",
            (DOMAIN, DATASET, SOURCE_APPLICATION),
        )
        cur.execute(
            "DELETE FROM pipeline.run_log WHERE domain=%s AND dataset=%s",
            (DOMAIN, DATASET),
        )
        cur.execute(
            "DELETE FROM pipeline.file_catalogue WHERE domain=%s AND dataset=%s",
            (DOMAIN, DATASET),
        )
    pg_conn.commit()


def test_lookup_finds_only_run_with_matching_triggered_by_edge(pg_conn, cleanup):
    """Two dag_ingest rows for the same file_id but only one carries our
    triggered_by_api_pull edge. The lookup must return THAT one's status.
    """
    api_pull_run_id = str(uuid.uuid4())
    file_id = str(uuid.uuid4())
    _insert_file_catalogue(pg_conn, file_id=file_id, run_id=api_pull_run_id)

    # Older replay of the same file_id (no triggered_by_api_pull edge).
    older_dag_ingest_run = str(uuid.uuid4())
    _insert_run_log(
        pg_conn,
        run_id=older_dag_ingest_run,
        file_id=file_id,
        orchestrators=[{"run_id": str(uuid.uuid4()), "edge_type": "replay"}],
        status="failed",
    )

    # The dag_ingest run actually triggered by THIS api_pull poll.
    triggered_dag_ingest_run = str(uuid.uuid4())
    _insert_run_log(
        pg_conn,
        run_id=triggered_dag_ingest_run,
        file_id=file_id,
        orchestrators=[{
            "run_id": api_pull_run_id,
            "edge_type": "triggered_by_api_pull",
        }],
        status="succeeded",
    )

    status = ingest_status_for_api_pull_run(
        pg_conn, api_pull_run_id,
    )
    assert status == "succeeded"


def test_unrelated_replay_does_not_leak_status(pg_conn, cleanup):
    """A different api_pull poll's dag_ingest run on the same file_id must
    not be observed by THIS poll's lookup."""
    file_id = str(uuid.uuid4())
    _insert_file_catalogue(pg_conn, file_id=file_id, run_id=str(uuid.uuid4()))

    other_api_pull_run_id = str(uuid.uuid4())
    _insert_run_log(
        pg_conn,
        run_id=str(uuid.uuid4()),
        file_id=file_id,
        orchestrators=[{
            "run_id": other_api_pull_run_id,
            "edge_type": "triggered_by_api_pull",
        }],
        status="succeeded",
    )

    our_api_pull_run_id = str(uuid.uuid4())
    status = ingest_status_for_api_pull_run(
        pg_conn, our_api_pull_run_id,
    )
    assert status is None, (
        "lookup must not match a dag_ingest run launched by a different "
        "api_pull poll, even on the same file_id"
    )


def test_two_rows_with_same_edge_returns_none_when_no_expected_parent(
    pg_conn, cleanup,
):
    """Defensive: if SOMEHOW two dag_ingest rows both carry the
    triggered_by_api_pull edge for the same api_pull_run_id (retry,
    manual replay, bug), the edge-only fallback lookup MUST return
    None — 'ambiguous, do not promote'."""
    api_pull_run_id = str(uuid.uuid4())
    file_id_a = str(uuid.uuid4())
    file_id_b = str(uuid.uuid4())
    _insert_file_catalogue(pg_conn, file_id=file_id_a, run_id=api_pull_run_id)
    _insert_file_catalogue(pg_conn, file_id=file_id_b, run_id=api_pull_run_id)

    _insert_run_log(
        pg_conn,
        run_id=str(uuid.uuid4()),
        file_id=file_id_a,
        orchestrators=[{"run_id": api_pull_run_id, "edge_type": "triggered_by_api_pull"}],
        status="succeeded",
    )
    _insert_run_log(
        pg_conn,
        run_id=str(uuid.uuid4()),
        file_id=file_id_b,
        orchestrators=[{"run_id": api_pull_run_id, "edge_type": "triggered_by_api_pull"}],
        status="failed",
    )

    # No expected_parent_run_id supplied — fallback path. Two matching
    # rows means we cannot tell which is OUR downstream run.
    assert ingest_status_for_api_pull_run(
        pg_conn, api_pull_run_id,
    ) is None


def test_exact_parent_run_id_match_disambiguates(pg_conn, cleanup):
    """When dag_api_pull pre-mints the deterministic upstream_run_id and
    passes it as expected_parent_run_id, the lookup is by PK so the
    presence of OTHER rows carrying the same edge cannot mislead it.
    """
    api_pull_run_id = str(uuid.uuid4())
    expected_parent = derive_dag_ingest_parent_run_id(api_pull_run_id)
    file_id_real = str(uuid.uuid4())
    file_id_other = str(uuid.uuid4())
    _insert_file_catalogue(pg_conn, file_id=file_id_real, run_id=api_pull_run_id)
    _insert_file_catalogue(pg_conn, file_id=file_id_other, run_id=api_pull_run_id)

    # The dag_ingest run actually triggered by us — uses the
    # deterministic upstream_run_id.
    _insert_run_log(
        pg_conn,
        run_id=expected_parent,
        file_id=file_id_real,
        orchestrators=[{"run_id": api_pull_run_id, "edge_type": "triggered_by_api_pull"}],
        status="succeeded",
    )
    # A spurious second row carrying the same edge but a DIFFERENT PK.
    _insert_run_log(
        pg_conn,
        run_id=str(uuid.uuid4()),
        file_id=file_id_other,
        orchestrators=[{"run_id": api_pull_run_id, "edge_type": "triggered_by_api_pull"}],
        status="failed",
    )

    # Edge-only lookup is ambiguous.
    assert ingest_status_for_api_pull_run(
        pg_conn, api_pull_run_id,
    ) is None
    # Exact-PK lookup pinpoints OUR run.
    assert ingest_status_for_api_pull_run(
        pg_conn, api_pull_run_id, expected_parent_run_id=expected_parent,
    ) == "succeeded"


def test_exact_parent_run_id_rejects_pk_collision_without_edge(pg_conn, cleanup):
    """Defence-in-depth: a row with the expected PK but missing the
    triggered_by_api_pull edge must NOT be observed."""
    api_pull_run_id = str(uuid.uuid4())
    expected_parent = derive_dag_ingest_parent_run_id(api_pull_run_id)
    file_id = str(uuid.uuid4())
    _insert_file_catalogue(pg_conn, file_id=file_id, run_id=api_pull_run_id)

    # Same PK, different edge type.
    _insert_run_log(
        pg_conn,
        run_id=expected_parent,
        file_id=file_id,
        orchestrators=[{"run_id": api_pull_run_id, "edge_type": "replay"}],
        status="succeeded",
    )

    assert ingest_status_for_api_pull_run(
        pg_conn, api_pull_run_id, expected_parent_run_id=expected_parent,
    ) is None


def test_derive_parent_run_id_is_deterministic_and_namespaced():
    a = derive_dag_ingest_parent_run_id("abc")
    b = derive_dag_ingest_parent_run_id("abc")
    c = derive_dag_ingest_parent_run_id("xyz")
    assert a == b
    assert a != c
    # uuid5 over NAMESPACE_OID yields a v5 UUID.
    assert uuid.UUID(a).version == 5


def test_replay_cannot_promote_pending_cursor(pg_conn, cleanup):
    """End-to-end replay-safety check.

    1. api_pull poll A archives a batch, records pending cursor.
    2. A replay of the SAME file_id (different api_pull run, no link
       to A) is recorded as a dag_ingest succeeded run_log row.
    3. The lookup for poll A returns None — the replay is not visible
       — so promote() will not be called, and the pending cursor of
       poll A remains pending.
    """
    store = WatermarkStore(pg_conn)
    store.read(domain=DOMAIN, dataset=DATASET,
               source_application=SOURCE_APPLICATION,
               cursor_type="since_timestamp")

    poll_a_run_id = str(uuid.uuid4())
    file_id = str(uuid.uuid4())
    _insert_file_catalogue(pg_conn, file_id=file_id, run_id=poll_a_run_id)

    # Poll A stages its pending cursor.
    store.try_lock(domain=DOMAIN, dataset=DATASET,
                   source_application=SOURCE_APPLICATION,
                   run_id=poll_a_run_id)
    store.record_pending(domain=DOMAIN, dataset=DATASET,
                         source_application=SOURCE_APPLICATION,
                         run_id=poll_a_run_id,
                         new_cursor_value="2026-04-09T00:00:00Z")
    store.unlock(domain=DOMAIN, dataset=DATASET,
                 source_application=SOURCE_APPLICATION)

    # An unrelated replay (manual rerun of file_id) records a successful
    # dag_ingest row. It carries a 'replay' parent edge — not a
    # triggered_by_api_pull edge for poll A.
    _insert_run_log(
        pg_conn,
        run_id=str(uuid.uuid4()),
        file_id=file_id,
        orchestrators=[{"run_id": str(uuid.uuid4()), "edge_type": "replay"}],
        status="succeeded",
    )

    # Lookup for poll A finds nothing — the sensor would keep waiting.
    assert ingest_status_for_api_pull_run(
        pg_conn, poll_a_run_id,
    ) is None

    # Pending cursor is still pending — committed has not advanced.
    row = store.read(domain=DOMAIN, dataset=DATASET,
                     source_application=SOURCE_APPLICATION,
                     cursor_type="since_timestamp")
    assert row.pending_cursor_value == "2026-04-09T00:00:00Z"
    assert row.committed_cursor_value is None
    assert row.last_successful_run_id is None
