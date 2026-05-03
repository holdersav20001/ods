"""Replay/rerun CLI integration tests against local Postgres.

These tests intentionally avoid mocked Postgres cursors. They seed real
pipeline rows, run the replay planning code, and verify it does not pre-create
disconnected run_log rows.
"""
from __future__ import annotations

import os
import uuid

import psycopg2
import pytest

from ods_pipeline.ops.runs import _RunsOps, _dag_for


@pytest.fixture
def pg_conn():
    conn = psycopg2.connect(
        host=os.environ.get("TEST_PG_HOST", "127.0.0.1"),
        port=int(os.environ.get("TEST_PG_PORT", "5440")),
        dbname=os.environ.get("TEST_PG_DB", "ods_dev"),
        user=os.environ.get("TEST_PG_USER", "ods"),
        password=os.environ.get("TEST_PG_PASSWORD", "ods"),
    )
    yield conn
    conn.close()


def _seed_run(pg_conn, *, run_id: str, file_id: str, pipeline_type: str, started_rank: int):
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline.file_catalogue
                (file_id, domain, dataset, business_date, s3_raw_path, file_md5, state)
            VALUES (%s::uuid, 'insurance', 'policies', '2026-05-03',
                    's3://ods-raw-local/replay-cli-test.csv', %s, 'sunk')
            ON CONFLICT (file_id) DO NOTHING
            """,
            (file_id, uuid.uuid4().hex),
        )
        cur.execute(
            """
            INSERT INTO pipeline.run_log
                (run_id, pipeline_type, domain, dataset, business_date,
                 file_id, status, started_at, kafka_topic)
            VALUES (%s::uuid, %s, 'insurance', 'policies', '2026-05-03',
                    %s::uuid, 'succeeded', now() + (%s || ' seconds')::interval,
                    'ods.insurance.policies')
            """,
            (run_id, pipeline_type, file_id, started_rank),
        )
    pg_conn.commit()


def _cleanup(pg_conn, file_id: str):
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM pipeline.run_stage_log
             WHERE run_id IN (SELECT run_id FROM pipeline.run_log WHERE file_id = %s::uuid)
            """,
            (file_id,),
        )
        cur.execute("DELETE FROM pipeline.run_log WHERE file_id = %s::uuid", (file_id,))
        cur.execute("DELETE FROM pipeline.file_catalogue WHERE file_id = %s::uuid", (file_id,))
    pg_conn.commit()


def test_replay_file_dry_run_uses_one_parent_candidate_and_does_not_insert_runs(pg_conn):
    file_id = str(uuid.uuid4())
    parent_run_id = str(uuid.uuid4())
    child_run_id = str(uuid.uuid4())
    _cleanup(pg_conn, file_id)
    try:
        _seed_run(pg_conn, run_id=child_run_id, file_id=file_id, pipeline_type="publish", started_rank=20)
        _seed_run(pg_conn, run_id=parent_run_id, file_id=file_id, pipeline_type="s3_batch", started_rank=10)

        ops = _RunsOps(pg_conn=pg_conn, airflow=None)
        result = ops.replay_file(file_id, dry_run=True)

        assert result["file_id"] == file_id
        assert result["replay"]["original_run_id"] == parent_run_id
        assert result["replay"]["status"] == "dry-run"
        assert result["replay"]["dry_run"] is True
        assert "replay_request_id" in result["replay"]

        with pg_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM pipeline.run_log WHERE file_id = %s::uuid", (file_id,))
            assert cur.fetchone()[0] == 2
    finally:
        _cleanup(pg_conn, file_id)


def test_rerun_dry_run_reads_real_run_without_side_effects(pg_conn):
    file_id = str(uuid.uuid4())
    run_id = str(uuid.uuid4())
    _cleanup(pg_conn, file_id)
    try:
        _seed_run(pg_conn, run_id=run_id, file_id=file_id, pipeline_type="file", started_rank=0)
        ops = _RunsOps(pg_conn=pg_conn, airflow=None)

        result = ops.rerun(run_id, dry_run=True)

        assert result["original_run_id"] == run_id
        assert result["pipeline_type"] == "file"
        assert result["status"] == "dry-run"
        assert _dag_for(result["pipeline_type"]) == "dag_ingest"
    finally:
        _cleanup(pg_conn, file_id)
