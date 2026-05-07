"""Unit tests for ``dag_run_janitor.reap_orphan_runs``.

The DAG itself is wired by Airflow at import time; we test the
business logic via direct call against a fake connection.
"""
from __future__ import annotations

import importlib.util
import os
import sys
import types

import pytest


HERE = os.path.dirname(__file__)
REPO = os.path.abspath(os.path.join(HERE, "..", ".."))


def _stub_airflow() -> None:
    """Sufficient airflow surface for ``dag_run_janitor`` to import."""
    if "airflow" in sys.modules:
        # Other test modules may have installed thinner stubs; evict.
        for mod in list(sys.modules):
            if mod == "airflow" or mod.startswith("airflow."):
                sys.modules.pop(mod, None)

    airflow = types.ModuleType("airflow")

    class _DAG:
        def __init__(self, *a, **kw): ...
        def __enter__(self): return self
        def __exit__(self, *a): return False

    airflow.DAG = _DAG
    sys.modules["airflow"] = airflow

    decorators = types.ModuleType("airflow.decorators")

    class _TaskShim:
        def __init__(self, fn): self._fn = fn
        def __call__(self, *a, **kw): return self
        def expand(self, *a, **kw): return self
        def __rshift__(self, other): return other
        def __lshift__(self, other): return other

    def _task(*dargs, **dkwargs):
        if dargs and callable(dargs[0]):
            return _TaskShim(dargs[0])

        def _wrap(fn): return _TaskShim(fn)
        return _wrap

    decorators.task = _task
    sys.modules["airflow.decorators"] = decorators

    utils_dates = types.ModuleType("airflow.utils.dates")
    utils_dates.days_ago = lambda n: None
    sys.modules["airflow.utils"] = types.ModuleType("airflow.utils")
    sys.modules["airflow.utils.dates"] = utils_dates


def _import_dag():
    _stub_airflow()
    spec = importlib.util.spec_from_file_location(
        "dag_run_janitor_under_test",
        os.path.join(REPO, "airflow", "dags", "dag_run_janitor.py"),
    )
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
def janitor():
    return _import_dag()


class _FakeCursor:
    def __init__(self, owner: "_FakeConn") -> None:
        self._owner = owner
        self._last_sql: str = ""

    def __enter__(self): return self

    def __exit__(self, *exc): return None

    def execute(self, sql: str, params=None) -> None:
        self._last_sql = sql
        self._owner.statements.append((sql, params))

    def fetchall(self):
        if "SELECT r.run_id" in self._last_sql:
            return [(rid,) for rid in self._owner.stuck_run_ids]
        return []


class _FakeConn:
    def __init__(self) -> None:
        self.statements: list[tuple[str, object]] = []
        self.commits = 0
        self.rollbacks = 0
        self.stuck_run_ids: list[str] = []

    def __enter__(self): return self

    def __exit__(self, exc_type, *_):
        if exc_type is None:
            self.commits += 1
        else:
            self.rollbacks += 1
        return False

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self)


def test_reap_returns_empty_when_no_orphans(janitor) -> None:
    conn = _FakeConn()
    conn.stuck_run_ids = []
    out = janitor.reap_orphan_runs(conn)
    assert out == []
    # Only the SELECT executed; no INSERT or UPDATE.
    assert all(
        "SELECT" in sql or sql.startswith("\n")
        for sql, _ in conn.statements
    )


def test_reap_writes_kill_stage_and_updates_run_log(janitor) -> None:
    conn = _FakeConn()
    conn.stuck_run_ids = ["run_a", "run_b"]
    out = janitor.reap_orphan_runs(conn, grace_minutes=5)
    assert out == ["run_a", "run_b"]

    inserts = [s for s, _ in conn.statements if "INSERT INTO pipeline.run_stage_log" in s]
    assert len(inserts) == 2     # one kill row per orphan

    update = next(s for s, _ in conn.statements if "UPDATE pipeline.run_log" in s)
    assert "status = 'failed'" in update.replace("  ", " ")
    assert "janitor_no_heartbeat" in update or any(
        "janitor_no_heartbeat" in str(p) for _, p in conn.statements
    )
    assert conn.commits == 1    # one transaction wraps both writes
    assert conn.rollbacks == 0


def test_reap_uses_grace_window_in_seconds(janitor) -> None:
    conn = _FakeConn()
    conn.stuck_run_ids = []
    janitor.reap_orphan_runs(conn, grace_minutes=42)
    select_stmt = next(
        (sql, params)
        for sql, params in conn.statements
        if "SELECT r.run_id" in sql
    )
    assert select_stmt[1] == ("42 minutes", "42 minutes")
