"""Current-vs-history row-value reconciliation against live Postgres."""
from __future__ import annotations

import os
import uuid

import psycopg2
import pytest

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
def row_value_tables(pg_conn):
    with pg_conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS pipeline_test")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS pipeline_test.current_policy (
                policy_id varchar PRIMARY KEY,
                status varchar,
                premium numeric(10, 2),
                _ods_business_date varchar,
                _ods_ingested_at timestamp DEFAULT now()
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS pipeline_test.history_policy (
                id bigserial PRIMARY KEY,
                policy_id varchar NOT NULL,
                status varchar,
                premium numeric(10, 2),
                _ods_business_date varchar,
                _ods_ingested_at timestamp DEFAULT now()
            )
            """
        )
        cur.execute("TRUNCATE pipeline_test.current_policy, pipeline_test.history_policy")
    pg_conn.commit()
    yield
    with pg_conn.cursor() as cur:
        cur.execute("TRUNCATE pipeline_test.current_policy, pipeline_test.history_policy")
    pg_conn.commit()


def _tables():
    return {
        "policy_row_value": (
            "pipeline_test.current_policy",
            "pipeline_test.history_policy",
            ("policy_id",),
            ("status", "premium"),
            "_ods_ingested_at",
        )
    }


def test_current_history_row_value_ok(pg_conn):
    run_id = str(uuid.uuid4())
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline_test.current_policy
                (policy_id, status, premium, _ods_business_date)
            VALUES ('P1', 'ACTIVE', 10.00, '2026-05-03')
            """
        )
        cur.execute(
            """
            INSERT INTO pipeline_test.history_policy
                (policy_id, status, premium, _ods_business_date)
            VALUES ('P1', 'ACTIVE', 10.00, '2026-05-03')
            """
        )
    pg_conn.commit()

    result = reconciliation.compare_history_vs_current(
        pg_conn,
        run_id=run_id,
        domain="pipeline_test",
        dataset="policy_row_value",
        business_date="2026-05-03",
        tables=_tables(),
    )

    assert result["status"] == "ok"
    assert result["mismatch_count"] == 0
    assert result["missing_history_count"] == 0


def test_current_history_row_value_fails_with_sample(pg_conn):
    run_id = str(uuid.uuid4())
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline_test.current_policy
                (policy_id, status, premium, _ods_business_date)
            VALUES ('P1', 'ACTIVE', 12.00, '2026-05-03')
            """
        )
        cur.execute(
            """
            INSERT INTO pipeline_test.history_policy
                (policy_id, status, premium, _ods_business_date)
            VALUES ('P1', 'ACTIVE', 10.00, '2026-05-03')
            """
        )
    pg_conn.commit()

    result = reconciliation.compare_history_vs_current(
        pg_conn,
        run_id=run_id,
        domain="pipeline_test",
        dataset="policy_row_value",
        business_date="2026-05-03",
        tables=_tables(),
    )

    assert result["status"] == "failed"
    assert result["mismatch_count"] == 1
    assert result["samples"][0]["type"] == "mismatch"
    assert result["samples"][0]["key"] == {"policy_id": "P1"}
