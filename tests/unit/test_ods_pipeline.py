# tests/unit/test_ods_pipeline.py
"""
Unit tests for the ods_pipeline package.
No live database required — connections are mocked with MagicMock.
"""
from __future__ import annotations

import os
import sys
import uuid
from unittest.mock import MagicMock, call, patch

import pytest

# Ensure repo root is on sys.path so ods_pipeline is importable
HERE = os.path.dirname(__file__)
sys.path.insert(0, os.path.abspath(os.path.join(HERE, "..", "..")))

import ods_pipeline
from ods_pipeline.models import Stage, StageEvent, TERMINAL_STATUSES, ALLOWED_RUN_FIELDS
from ods_pipeline._db import build_dsn
from ods_pipeline import runs, lineage, reconciliation


# ---------------------------------------------------------------------------
# models — Stage and StageEvent constants
# ---------------------------------------------------------------------------

class TestStageModel:
    def test_all_values_contains_expected(self):
        vals = Stage.all_values()
        for expected in (
            "raw_read", "schema_validate", "dq_check",
            "curated_write", "curated_read", "kafka_publish",
            "recon_t0", "sink_pg_wait", "sink_s3_wait", "finalise",
        ):
            assert expected in vals, f"Stage.all_values() missing '{expected}'"

    def test_all_values_returns_frozenset(self):
        assert isinstance(Stage.all_values(), frozenset)

    def test_constants_match_strings(self):
        assert Stage.RAW_READ == "raw_read"
        assert Stage.KAFKA_PUBLISH == "kafka_publish"
        assert Stage.FINALISE == "finalise"


class TestStageEventModel:
    def test_terminal_is_frozenset(self):
        assert isinstance(StageEvent.TERMINAL, frozenset)

    def test_terminal_contains_expected(self):
        assert StageEvent.TERMINAL == frozenset({
            "stage_completed", "stage_failed", "stage_skipped", "stage_warned"
        })

    def test_started_not_in_terminal(self):
        assert StageEvent.STARTED not in StageEvent.TERMINAL

    def test_constants(self):
        assert StageEvent.STARTED == "stage_started"
        assert StageEvent.COMPLETED == "stage_completed"
        assert StageEvent.FAILED == "stage_failed"
        assert StageEvent.SKIPPED == "stage_skipped"
        assert StageEvent.WARNED == "stage_warned"


# ---------------------------------------------------------------------------
# _db.build_dsn
# ---------------------------------------------------------------------------

class TestBuildDsn:
    def test_explicit_dsn_returned_as_is(self):
        result = build_dsn("host=myhost dbname=mydb")
        assert result == "host=myhost dbname=mydb"

    def test_reads_pipeline_pg_dsn_env_var(self, monkeypatch):
        monkeypatch.setenv("PIPELINE_PG_DSN", "host=envhost dbname=envdb")
        monkeypatch.delenv("PG_DSN", raising=False)
        monkeypatch.delenv("POSTGRES_HOST", raising=False)
        result = build_dsn()
        assert result == "host=envhost dbname=envdb"

    def test_reads_pg_dsn_env_var_fallback(self, monkeypatch):
        monkeypatch.delenv("PIPELINE_PG_DSN", raising=False)
        monkeypatch.setenv("PG_DSN", "host=fallback dbname=fb")
        monkeypatch.delenv("POSTGRES_HOST", raising=False)
        result = build_dsn()
        assert result == "host=fallback dbname=fb"

    def test_builds_from_postgres_host_env(self, monkeypatch):
        monkeypatch.delenv("PIPELINE_PG_DSN", raising=False)
        monkeypatch.delenv("PG_DSN", raising=False)
        monkeypatch.setenv("POSTGRES_HOST", "myserver")
        monkeypatch.setenv("POSTGRES_PORT", "5433")
        monkeypatch.setenv("POSTGRES_DB", "mydb")
        monkeypatch.setenv("POSTGRES_USER", "myuser")
        monkeypatch.setenv("POSTGRES_PASSWORD", "mypw")
        result = build_dsn()
        assert "host=myserver" in result
        assert "port=5433" in result
        assert "dbname=mydb" in result

    def test_raises_value_error_when_nothing_set(self, monkeypatch):
        monkeypatch.delenv("PIPELINE_PG_DSN", raising=False)
        monkeypatch.delenv("PG_DSN", raising=False)
        monkeypatch.delenv("POSTGRES_HOST", raising=False)
        with pytest.raises(ValueError, match="No Postgres DSN configured"):
            build_dsn()

    def test_explicit_dsn_wins_over_env(self, monkeypatch):
        monkeypatch.setenv("PIPELINE_PG_DSN", "host=envhost")
        result = build_dsn("host=explicit")
        assert result == "host=explicit"


# ---------------------------------------------------------------------------
# runs.update — validation, mocked conn
# ---------------------------------------------------------------------------

def _mock_conn():
    """Return a MagicMock that mimics a psycopg2 connection."""
    conn = MagicMock()
    cursor = MagicMock()
    cursor.__enter__ = MagicMock(return_value=cursor)
    cursor.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value = cursor
    return conn, cursor


class TestRunsUpdate:
    def test_raises_value_error_for_unknown_field(self):
        conn, _ = _mock_conn()
        with pytest.raises(ValueError, match="Unknown run_log fields"):
            runs.update(conn, "run-123", totally_bogus_field="x")

    def test_raises_for_multiple_unknown_fields(self):
        conn, _ = _mock_conn()
        with pytest.raises(ValueError):
            runs.update(conn, "run-123", bad_col="x", another_bad="y")

    def test_does_not_raise_for_valid_fields(self):
        conn, cursor = _mock_conn()
        # Should not raise
        runs.update(conn, "run-123", status="succeeded", record_count_source=100)
        conn.commit.assert_called_once()

    def test_no_op_when_no_fields(self):
        conn, cursor = _mock_conn()
        runs.update(conn, "run-123")
        conn.cursor.assert_not_called()
        conn.commit.assert_not_called()

    def test_terminal_status_appends_ended_at_in_sql(self):
        conn, cursor = _mock_conn()
        runs.update(conn, "run-123", status="succeeded")
        execute_call = cursor.execute.call_args
        sql = execute_call[0][0]
        assert "ended_at" in sql

    def test_non_terminal_status_no_ended_at(self):
        conn, cursor = _mock_conn()
        runs.update(conn, "run-123", record_count_source=5)
        execute_call = cursor.execute.call_args
        sql = execute_call[0][0]
        assert "ended_at" not in sql

    def test_all_allowed_fields_accepted(self):
        conn, cursor = _mock_conn()
        # Pass one valid field from the allowed set
        for field in ALLOWED_RUN_FIELDS:
            conn2, _ = _mock_conn()
            # Use a harmless value; we just want no ValueError
            runs.update(conn2, "run-123", **{field: "test_value"})


# ---------------------------------------------------------------------------
# lineage.write_edge — ValueError when no parent supplied
# ---------------------------------------------------------------------------

class TestLineageWriteEdge:
    def test_raises_when_both_parents_none(self):
        conn, _ = _mock_conn()
        with pytest.raises(ValueError, match="parent_run_id or parent_file_id"):
            lineage.write_edge(
                conn,
                child_run_id="run-abc",
                edge_type="raw_to_curated",
                parent_run_id=None,
                parent_file_id=None,
            )

    def test_succeeds_with_parent_run_id(self):
        conn, cursor = _mock_conn()
        lineage.write_edge(
            conn,
            child_run_id="run-child",
            edge_type="raw_to_curated",
            parent_run_id="run-parent",
        )
        conn.commit.assert_called_once()

    def test_succeeds_with_parent_file_id(self):
        conn, cursor = _mock_conn()
        lineage.write_edge(
            conn,
            child_run_id="run-child",
            edge_type="curated_to_kafka",
            parent_file_id="file-uuid",
        )
        conn.commit.assert_called_once()

    def test_succeeds_with_both_parents(self):
        conn, cursor = _mock_conn()
        lineage.write_edge(
            conn,
            child_run_id="run-child",
            edge_type="raw_to_curated",
            parent_run_id="run-parent",
            parent_file_id="file-uuid",
        )
        conn.commit.assert_called_once()


# ---------------------------------------------------------------------------
# reconciliation.write_check — discrepancy math (pure logic, mocked conn)
# ---------------------------------------------------------------------------

class TestReconciliationWriteCheck:
    def _capture_discrepancy(self, **kwargs):
        """Call write_check and return the discrepancy_count arg passed to execute."""
        conn, cursor = _mock_conn()
        reconciliation.write_check(conn, **kwargs)
        # The execute call passes a tuple of values; discrepancy is at index 10
        args_tuple = cursor.execute.call_args[0][1]
        discrepancy = args_tuple[10]
        pct = args_tuple[11]
        return discrepancy, pct

    def test_source_kafka_discrepancy_zero(self):
        disc, pct = self._capture_discrepancy(
            check_type="t0", run_id="r1", domain="ins", dataset="pol",
            business_date="2026-04-28",
            source_count=100, kafka_count=100,
            status="ok",
        )
        assert disc == 0
        assert pct == 0.0

    def test_source_kafka_discrepancy_negative(self):
        disc, pct = self._capture_discrepancy(
            check_type="t0", run_id="r1", domain="ins", dataset="pol",
            business_date="2026-04-28",
            source_count=10, kafka_count=9,
            status="failed",
        )
        assert disc == -1
        assert pct == round(100.0 * -1 / 10, 4)

    def test_source_kafka_discrepancy_positive(self):
        disc, pct = self._capture_discrepancy(
            check_type="t0", run_id="r1", domain="ins", dataset="pol",
            business_date="2026-04-28",
            source_count=10, kafka_count=12,
            status="failed",
        )
        assert disc == 2
        assert pct == round(100.0 * 2 / 10, 4)

    def test_kafka_postgres_discrepancy(self):
        disc, pct = self._capture_discrepancy(
            check_type="t1", run_id="r1", domain="ins", dataset="pol",
            business_date="2026-04-28",
            kafka_count=50, postgres_count=48,
            status="failed",
        )
        assert disc == -2   # postgres_count - kafka_count

    def test_no_counts_gives_none_discrepancy(self):
        disc, pct = self._capture_discrepancy(
            check_type="t0", run_id="r1", domain="ins", dataset="pol",
            business_date="2026-04-28",
            status="ok",
        )
        assert disc is None
        assert pct is None

    def test_commits_on_success(self):
        conn, cursor = _mock_conn()
        reconciliation.write_check(
            conn,
            check_type="t0", run_id="r1", domain="ins", dataset="pol",
            business_date="2026-04-28",
            source_count=5, kafka_count=5,
            status="ok",
        )
        conn.commit.assert_called_once()
