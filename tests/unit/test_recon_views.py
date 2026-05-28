"""Recon dashboard views (T14 / 8.5) — shape contract."""
from __future__ import annotations

import psycopg2
import pytest


@pytest.fixture
def pg_conn():
    conn = psycopg2.connect(host="localhost", port=5440, dbname="ods_dev",
                            user="ods", password="ods")
    yield conn
    conn.close()


VIEWS_AND_COLUMNS = {
    "ods.v_recon_latest_failed": {
        "run_id", "check_type", "domain", "dataset", "business_date",
        "source_count", "accounted_count", "postgres_count",
        "discrepancy_count", "discrepancy_pct", "detail", "created_at",
    },
    "ods.v_recon_t0_t1_t2_trend": {
        "day", "check_type", "ok_count", "failed_count",
        "pending_count", "total_count",
    },
    "ods.v_recon_dlq_adjusted": {
        "run_id", "domain", "dataset", "business_date",
        "record_count_source", "dlq_count", "record_count_target",
        "adjusted_delta", "status", "started_at", "ended_at",
    },
    "ods.v_current_history_consistency": {
        "domain", "dataset", "business_date",
        "current_total", "history_total", "total_delta",
        "matched_runs", "diverged_runs", "lagging_runs",
    },
    "ods.v_rerun_candidates": {
        "run_id", "pipeline_type", "domain", "dataset", "business_date",
        "file_id", "error_summary", "ended_at", "has_succeeded_parent",
    },
}


@pytest.mark.parametrize("view,expected_columns", VIEWS_AND_COLUMNS.items())
def test_view_exists_and_exposes_expected_columns(pg_conn, view, expected_columns):
    schema, name = view.split(".")
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name FROM information_schema.columns
             WHERE table_schema = %s AND table_name = %s
            """,
            (schema, name),
        )
        got = {r[0] for r in cur.fetchall()}
    missing = expected_columns - got
    assert not missing, f"{view} missing columns: {missing} (got {got})"


@pytest.mark.parametrize("view", list(VIEWS_AND_COLUMNS))
def test_view_is_queryable(pg_conn, view):
    """Each view must execute without error (may return zero rows)."""
    with pg_conn.cursor() as cur:
        cur.execute(f"SELECT * FROM {view} LIMIT 1")
        cur.fetchall()
