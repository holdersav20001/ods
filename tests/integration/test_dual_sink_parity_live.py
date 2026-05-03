"""Dual-sink parity tests using real Postgres tables."""
from __future__ import annotations

import os
import uuid

import psycopg2
import pytest
from psycopg2 import sql

from ods_pipeline import reconciliation


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


@pytest.fixture(autouse=True)
def parity_tables(pg_conn):
    with pg_conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS pipeline_test")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS pipeline_test.dual_current (
                id bigserial PRIMARY KEY,
                _ods_run_id uuid NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS pipeline_test.dual_history (
                id bigserial PRIMARY KEY,
                _ods_run_id uuid NOT NULL
            )
            """
        )
        cur.execute("TRUNCATE pipeline_test.dual_current, pipeline_test.dual_history")
    pg_conn.commit()
    yield
    with pg_conn.cursor() as cur:
        cur.execute("TRUNCATE pipeline_test.dual_current, pipeline_test.dual_history")
    pg_conn.commit()


def _pairs():
    return {
        "dual_test": (
            "pipeline_test.dual_current",
            "pipeline_test.dual_history",
            "_ods_run_id",
        )
    }


def _insert_rows(pg_conn, table: str, run_id: str, count: int):
    schema, name = table.split(".", 1)
    with pg_conn.cursor() as cur:
        for _ in range(count):
            cur.execute(
                sql.SQL("INSERT INTO {}.{} (_ods_run_id) VALUES (%s::uuid)").format(
                    sql.Identifier(schema),
                    sql.Identifier(name),
                ),
                (run_id,),
            )
    pg_conn.commit()


def test_parity_ok_when_live_counts_match(pg_conn):
    run_id = str(uuid.uuid4())
    _insert_rows(pg_conn, "pipeline_test.dual_current", run_id, 3)
    _insert_rows(pg_conn, "pipeline_test.dual_history", run_id, 3)

    result = reconciliation.check_dual_sink_parity(
        pg_conn,
        run_id=run_id,
        domain="pipeline_test",
        dataset="dual_test",
        business_date="2026-05-03",
        pairs=_pairs(),
    )

    assert result["status"] == "ok"
    assert result["delta"] == 0
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            SELECT status, source_count, postgres_count
              FROM pipeline.reconciliation_log
             WHERE run_id = %s::uuid
               AND check_type = 'dual_sink_parity'
            """,
            (run_id,),
        )
        assert cur.fetchone() == ("ok", 3, 3)


def test_parity_failed_when_live_counts_diverge(pg_conn):
    run_id = str(uuid.uuid4())
    _insert_rows(pg_conn, "pipeline_test.dual_current", run_id, 3)
    _insert_rows(pg_conn, "pipeline_test.dual_history", run_id, 1)

    result = reconciliation.check_dual_sink_parity(
        pg_conn,
        run_id=run_id,
        domain="pipeline_test",
        dataset="dual_test",
        business_date="2026-05-03",
        pairs=_pairs(),
    )

    assert result["status"] == "failed"
    assert result["delta"] == -2


def test_parity_rejects_unsafe_override_before_querying(pg_conn):
    run_id = str(uuid.uuid4())
    with pytest.raises(ValueError, match="table reference"):
        reconciliation.check_dual_sink_parity(
            pg_conn,
            run_id=run_id,
            domain="pipeline_test",
            dataset="dual_test",
            business_date="2026-05-03",
            pairs={
                "dual_test": (
                    "pipeline_test.dual_current; DROP TABLE pipeline.run_log",
                    "pipeline_test.dual_history",
                    "_ods_run_id",
                )
            },
        )
    pg_conn.rollback()
