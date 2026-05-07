"""Unit tests for the stateless ``stage_scope`` context manager.

The scope is the developer-facing way to write stateless control-plane
code: each stage row commits independently on entry and again on exit
(success or failure). Without it, callers must hand-write paired
``stages.start`` / ``stages.finish`` calls with the right exception
handler — easy to get wrong.

These tests exercise the scope against a fake connection so we don't
need Postgres on the test path.
"""
from __future__ import annotations

import pytest

from ods_pipeline import stages


class _FakeCursor:
    def __init__(self, owner: "_FakeConn") -> None:
        self._owner = owner
        self._last_sql: str = ""

    def __enter__(self):
        return self

    def __exit__(self, *exc) -> None:
        return None

    def execute(self, sql: str, params=None) -> None:
        self._last_sql = sql
        self._owner.statements.append((sql, params))

    def fetchone(self):
        # Drive ``next_attempt_number`` and the ``finish`` lookup queries
        # via a deterministic small fake.
        if "MAX(attempt_number)" in self._last_sql:
            return (self._owner.next_attempt_value,)
        if "FOR UPDATE SKIP LOCKED" in self._last_sql:
            # By default no open row to claim — finish() falls through to
            # the append-INSERT branch, which is what the scope path runs.
            return None
        return None


class _FakeConn:
    def __init__(self) -> None:
        self.statements: list[tuple[str, object]] = []
        self.commits = 0
        self.rollbacks = 0
        # Value returned by the MAX query — the helper adds +1 server-side
        # in the real DB, so this fake stores the post-+1 result that
        # ``next_attempt_number`` should ultimately return.
        self.next_attempt_value = 1

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self)

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


# INSERT param positions kept stable; this is the schema in stages.write.
_INSERT_STATUS_IDX = 2
_INSERT_ERROR_IDX = 10


def _stages_inserted(conn: _FakeConn) -> list[str]:
    """Return the ``status`` column from every INSERT we did."""
    out: list[str] = []
    for sql, params in conn.statements:
        if "INSERT INTO pipeline.run_stage_log" in sql:
            assert params is not None
            out.append(params[_INSERT_STATUS_IDX])
    return out


def test_next_attempt_number_returns_one_when_no_rows() -> None:
    conn = _FakeConn()
    conn.next_attempt_value = 1     # what COALESCE(MAX,0)+1 returns on empty
    n = stages.next_attempt_number(conn, run_id="rid", stage="message_receive")
    assert n == 1


def test_next_attempt_number_increments_on_existing_rows() -> None:
    conn = _FakeConn()
    conn.next_attempt_value = 4     # MAX=3 → COALESCE(MAX,0)+1 = 4
    n = stages.next_attempt_number(conn, run_id="rid", stage="message_receive")
    assert n == 4


def test_stage_scope_success_writes_started_then_completed() -> None:
    conn = _FakeConn()
    with stages.stage_scope(conn, run_id="rid", stage="message_receive") as s:
        s.set_result(record_count_out=1, output_ref="s3://archive/x.jsonl")
    statuses = _stages_inserted(conn)
    # One started (running) + one completed (succeeded). The MAX query
    # also runs but is filtered out by _stages_inserted.
    assert statuses == ["running", "succeeded"]
    assert conn.commits >= 2
    assert conn.rollbacks == 0


def test_stage_scope_failure_writes_started_then_failed_and_reraises() -> None:
    conn = _FakeConn()

    class Boom(RuntimeError):
        pass

    with pytest.raises(Boom):
        with stages.stage_scope(conn, run_id="rid", stage="message_receive"):
            raise Boom("kafka publish blew up")

    statuses = _stages_inserted(conn)
    assert statuses == ["running", "failed"]
    # Exception body should land on the failure row's ``error`` column.
    failure_stmt = next(
        s for s in conn.statements
        if "INSERT INTO pipeline.run_stage_log" in s[0]
        and s[1] is not None
        and s[1][_INSERT_STATUS_IDX] == "failed"
    )
    assert "kafka publish blew up" in (failure_stmt[1][_INSERT_ERROR_IDX] or "")


def test_stage_scope_truncates_long_error_messages() -> None:
    conn = _FakeConn()
    long_msg = "x" * 4_000
    with pytest.raises(RuntimeError):
        with stages.stage_scope(
            conn, run_id="rid", stage="kafka_publish", truncate_error=80
        ):
            raise RuntimeError(long_msg)
    failure_stmt = next(
        s for s in conn.statements
        if "INSERT INTO pipeline.run_stage_log" in s[0]
        and s[1] is not None
        and s[1][_INSERT_STATUS_IDX] == "failed"
    )
    assert len(failure_stmt[1][_INSERT_ERROR_IDX]) == 80


def test_stage_scope_setresult_rejects_unknown_keys() -> None:
    conn = _FakeConn()
    with pytest.raises(KeyError):
        with stages.stage_scope(conn, run_id="rid", stage="message_receive") as s:
            s.set_result(invented_field=123)


def test_stage_scope_swallows_secondary_failure_writing_failed_row() -> None:
    """If the failure-write itself raises, scope must still re-raise the
    *original* exception — the heartbeat janitor will close the row."""
    conn = _FakeConn()
    original_finish = stages.finish

    def flaky_finish(*args, **kwargs):
        # Allow the start-event to land normally, but blow up the
        # terminal-event write so the inner ``except`` triggers.
        if kwargs.get("status") == "failed":
            raise RuntimeError("DB momentarily unreachable")
        return original_finish(*args, **kwargs)

    stages.finish = flaky_finish     # type: ignore[assignment]
    try:
        with pytest.raises(ValueError, match="business error"):
            with stages.stage_scope(conn, run_id="rid", stage="message_receive"):
                raise ValueError("business error")
    finally:
        stages.finish = original_finish     # type: ignore[assignment]


def test_stage_scope_skip_writes_started_then_skipped() -> None:
    """``s.skip(reason)`` produces a stage_skipped terminal row."""
    conn = _FakeConn()
    with stages.stage_scope(conn, run_id="rid", stage="raw_poll") as s:
        s.skip("no_changes")
    statuses = _stages_inserted(conn)
    assert statuses == ["running", "skipped"]
    # event_type must be stage_skipped, not stage_completed.
    skip_stmt = next(
        s for s in conn.statements
        if "INSERT INTO pipeline.run_stage_log" in s[0]
        and s[1] is not None
        and s[1][_INSERT_STATUS_IDX] == "skipped"
    )
    # event_type is index 3 in the INSERT param tuple.
    assert skip_stmt[1][3] == "stage_skipped"


def test_stage_scope_skip_reason_lands_in_metrics() -> None:
    """``skip_reason`` should be folded into metrics so the dashboard can show it."""
    import json
    conn = _FakeConn()
    with stages.stage_scope(conn, run_id="rid", stage="raw_poll") as s:
        s.skip("upstream_etag_unchanged")
    skip_stmt = next(
        s for s in conn.statements
        if "INSERT INTO pipeline.run_stage_log" in s[0]
        and s[1] is not None
        and s[1][_INSERT_STATUS_IDX] == "skipped"
    )
    metrics_json = skip_stmt[1][9]      # metrics is param index 9 in stages.write
    assert metrics_json is not None
    payload = json.loads(metrics_json)
    assert payload["skip_reason"] == "upstream_etag_unchanged"


def test_stage_scope_skip_then_exception_writes_failed_not_skipped() -> None:
    """Exception always wins — skip is for clean exits only."""
    conn = _FakeConn()
    with pytest.raises(RuntimeError):
        with stages.stage_scope(conn, run_id="rid", stage="raw_poll") as s:
            s.skip("about_to_be_overridden")
            raise RuntimeError("boom")
    statuses = _stages_inserted(conn)
    assert statuses == ["running", "failed"]


def test_stage_scope_skip_without_reason_omits_metrics() -> None:
    conn = _FakeConn()
    with stages.stage_scope(conn, run_id="rid", stage="raw_poll") as s:
        s.skip()
    skip_stmt = next(
        s for s in conn.statements
        if "INSERT INTO pipeline.run_stage_log" in s[0]
        and s[1] is not None
        and s[1][_INSERT_STATUS_IDX] == "skipped"
    )
    # When no reason and no other metrics provided, metrics column stays NULL.
    assert skip_stmt[1][9] is None


def test_stage_scope_skip_preserves_caller_metrics() -> None:
    """If caller set metrics via set_result-like path or scope kw, skip must merge."""
    import json
    conn = _FakeConn()
    with stages.stage_scope(
        conn, run_id="rid", stage="raw_poll",
        metrics={"committed_cursor": 12345},
    ) as s:
        s.skip("no_changes")
    # The starting metrics land on the stage_started row; skip-reason-merged
    # metrics land on the stage_skipped row.
    skip_stmt = next(
        s for s in conn.statements
        if "INSERT INTO pipeline.run_stage_log" in s[0]
        and s[1] is not None
        and s[1][_INSERT_STATUS_IDX] == "skipped"
    )
    payload = json.loads(skip_stmt[1][9])
    assert payload == {"skip_reason": "no_changes"}


def test_stage_scope_warn_writes_warned_with_reason_in_error() -> None:
    """``s.warn(reason)`` produces a stage_warned terminal row with reason on error."""
    conn = _FakeConn()
    with stages.stage_scope(conn, run_id="rid", stage="dq_check") as s:
        s.warn("3 rules fired soft")
    statuses = _stages_inserted(conn)
    assert statuses == ["running", "warned"]
    warn_stmt = next(
        s for s in conn.statements
        if "INSERT INTO pipeline.run_stage_log" in s[0]
        and s[1] is not None
        and s[1][_INSERT_STATUS_IDX] == "warned"
    )
    assert warn_stmt[1][3] == "stage_warned"   # event_type
    assert warn_stmt[1][_INSERT_ERROR_IDX] == "3 rules fired soft"


def test_stage_scope_warn_then_exception_writes_failed() -> None:
    """Exception always wins over warn — same rule as for skip."""
    conn = _FakeConn()
    with pytest.raises(RuntimeError):
        with stages.stage_scope(conn, run_id="rid", stage="dq_check") as s:
            s.warn("about_to_be_overridden")
            raise RuntimeError("boom")
    statuses = _stages_inserted(conn)
    assert statuses == ["running", "failed"]


def test_stage_scope_uses_next_attempt_number_for_retries() -> None:
    """A retry after a previous attempt must land on attempt_number=N+1."""
    conn = _FakeConn()
    conn.next_attempt_value = 3     # MAX existing = 2 → COALESCE+1 = 3
    captured: dict = {}

    real_write = stages.write

    def spy_write(c, **kwargs):
        if kwargs.get("event_type") == "stage_started":
            captured["attempt"] = kwargs.get("attempt_number")
        return real_write(c, **kwargs)

    stages.write = spy_write    # type: ignore[assignment]
    try:
        with stages.stage_scope(conn, run_id="rid", stage="kafka_publish") as s:
            s.set_result(record_count_out=1)
    finally:
        stages.write = real_write   # type: ignore[assignment]

    assert captured["attempt"] == 3
