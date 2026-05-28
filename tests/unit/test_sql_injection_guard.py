"""Negative tests for dynamic-identifier handling in run_log/write_job_log.

Both ``ods_pipeline.runs.update`` and ``glue.jobs.utils.write_job_log`` build
SQL containing column names supplied by the caller.  Without a whitelist these
are an injection vector.  These tests assert that:

* unknown / malformed identifiers are rejected with ``ValueError`` BEFORE any
  cursor is touched (so a stub connection that explodes on use is sufficient);
* whitelisted identifiers continue to flow through to the cursor with safe SQL.
"""
from __future__ import annotations

import os
import sys

import pytest

HERE = os.path.dirname(__file__)
sys.path.insert(0, os.path.abspath(os.path.join(HERE, "..", "..")))

from glue.jobs import utils as glue_utils  # noqa: E402
from ods_pipeline import runs  # noqa: E402


class _ExplodingCursor:
    def __enter__(self):
        raise AssertionError("cursor should not be opened when validation fails")

    def __exit__(self, *exc):
        return False


class _ExplodingConn:
    def cursor(self):
        return _ExplodingCursor()

    def commit(self):
        raise AssertionError("commit should not be reached")

    def rollback(self):
        return None


class _RecordingCursor:
    def __init__(self, sink):
        self.sink = sink

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, query, params=None):
        # ``query`` may be psycopg2.sql.Composed — render via str() for inspection.
        self.sink.append((str(query), list(params) if params else []))


class _RecordingConn:
    def __init__(self):
        self.executed: list[tuple[str, list]] = []
        self.committed = 0

    def cursor(self):
        return _RecordingCursor(self.executed)

    def commit(self):
        self.committed += 1

    def rollback(self):
        return None


@pytest.mark.parametrize(
    "bad_field",
    [
        "value); DROP TABLE pipeline.run_log; --",
        "status; DELETE FROM pipeline.run_log; --",
        "status, ended_at",
        "status status",
        "STATUS",  # uppercase not in whitelist
        "",
        "1status",
    ],
)
def test_runs_update_rejects_non_whitelisted_field(bad_field):
    conn = _ExplodingConn()
    with pytest.raises(ValueError):
        runs.update(conn, "rid-1", **{bad_field: "x"})


def test_runs_update_accepts_whitelisted_field():
    conn = _RecordingConn()
    runs.update(conn, "rid-1", status="succeeded")
    assert conn.committed == 1
    assert len(conn.executed) == 1
    sql_text, params = conn.executed[0]
    assert "pipeline.control_patch_run" in sql_text
    assert params[0] == "rid-1"
    assert params[1].adapted == {"status": "succeeded"}


@pytest.mark.parametrize(
    "bad_field",
    [
        "foo bar",
        "value); DROP TABLE pipeline.glue_job_log; --",
        "job_name; --",
        "",
        "1col",
        "JOB_NAME",
    ],
)
def test_write_job_log_rejects_non_whitelisted_field(bad_field):
    conn = _ExplodingConn()
    with pytest.raises(ValueError):
        glue_utils.write_job_log(conn, **{bad_field: 1})


def test_write_job_log_accepts_whitelisted_fields():
    conn = _RecordingConn()
    glue_utils.write_job_log(
        conn,
        run_id="rid-1",
        job_name="ods_ingestion",
        pipeline_type="ingestion",
        domain="insurance",
        dataset="policies",
        status="succeeded",
        record_count=10,
    )
    assert conn.committed == 1
    sql_text, params = conn.executed[0]
    assert "INSERT INTO pipeline.glue_job_log" in sql_text
    for col in ("run_id", "job_name", "pipeline_type", "domain", "dataset", "status", "record_count"):
        assert f"Identifier('{col}')" in sql_text
    assert params == [
        "rid-1", "ods_ingestion", "ingestion", "insurance",
        "policies", "succeeded", 10,
    ]


def test_write_job_log_empty_fields_raises():
    conn = _ExplodingConn()
    with pytest.raises(ValueError):
        glue_utils.write_job_log(conn)
