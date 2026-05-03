"""Lineage closure invariant (T13 / A3)."""
from __future__ import annotations

import uuid

import psycopg2
import pytest

from ods_pipeline import runs
from ods_pipeline.runs import LineageInvariantError


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
    runs.start(pg_conn, run_id=rid, pipeline_type="file",
               domain="insurance", dataset="policies",
               business_date="2026-05-02")
    yield rid
    with pg_conn.cursor() as cur:
        cur.execute("DELETE FROM pipeline.lineage_edge WHERE child_run_id=%s OR parent_run_id=%s",
                    (rid, rid))
        cur.execute("DELETE FROM pipeline.run_stage_log WHERE run_id=%s", (rid,))
        cur.execute("DELETE FROM pipeline.run_log WHERE run_id=%s", (rid,))
    pg_conn.commit()


def test_finalise_passes_on_run_with_no_publishes_and_closed_stages(pg_conn, run_id):
    runs.finalise(pg_conn, run_id)


def test_finalise_raises_when_published_but_no_lineage(pg_conn, run_id):
    runs.update(pg_conn, run_id, record_count_published=42)
    with pytest.raises(LineageInvariantError, match="orphaned run"):
        runs.finalise(pg_conn, run_id)


def test_finalise_marks_run_failed_on_violation(pg_conn, run_id):
    runs.update(pg_conn, run_id, record_count_published=42)
    with pytest.raises(LineageInvariantError):
        runs.finalise(pg_conn, run_id)
    with pg_conn.cursor() as cur:
        cur.execute(
            "SELECT status, error_summary FROM pipeline.run_log WHERE run_id=%s",
            (run_id,),
        )
        status, err = cur.fetchone()
    assert status == "failed"
    assert "lineage invariant violated" in err


def test_finalise_passes_when_published_and_lineage_exists(pg_conn, run_id):
    runs.update(pg_conn, run_id, record_count_published=42)
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline.lineage_edge
                (child_run_id, parent_run_id, edge_type, record_count)
            VALUES (%s::uuid, NULL, 'curated_to_kafka', 42)
            """,
            (run_id,),
        )
    pg_conn.commit()
    runs.finalise(pg_conn, run_id)


def test_finalise_raises_when_open_stages_remain(pg_conn, run_id):
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline.run_stage_log
                (run_id, stage, attempt_number, event_type, status, started_at)
            VALUES (%s, 'kafka_publish', 1, 'stage_started', 'running', NOW())
            """,
            (run_id,),
        )
    pg_conn.commit()
    with pytest.raises(LineageInvariantError, match="non-terminal stages remain"):
        runs.finalise(pg_conn, run_id)


def test_finalise_raises_for_unknown_run_id(pg_conn):
    bogus = str(uuid.uuid4())
    with pytest.raises(LineageInvariantError, match="not found"):
        runs.finalise(pg_conn, bogus)
