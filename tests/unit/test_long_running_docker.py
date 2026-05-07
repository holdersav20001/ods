"""Unit tests for ``airflow/dags/common/long_running_docker.py`` (R8).

Pure unit tests — no Airflow runtime, no real docker, no real psycopg2.
The helper accepts ``psycopg2_module`` and ``docker_client_factory`` as
injection points so we can drive it with mocks.
"""
from __future__ import annotations

import os
import sys
import time
from types import SimpleNamespace
from unittest import mock

import pytest

# Make sure the airflow dags directory + repo root are importable.
_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
_DAGS = os.path.join(_REPO_ROOT, "airflow", "dags")
for _p in (_REPO_ROOT, _DAGS):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from common.long_running_docker import (  # noqa: E402
    _write_heartbeat_row,
    make_long_running_docker_operator,
)
from ods_pipeline.models import StageEvent  # noqa: E402


# --------------------------------------------------------------------- #
# fakes
# --------------------------------------------------------------------- #


class FakeCursor:
    def __init__(self, store):
        self._store = store

    def execute(self, sql, params):
        self._store.append((sql, params))

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class FakeConn:
    def __init__(self, store):
        self._store = store
        self.closed = False

    def cursor(self):
        return FakeCursor(self._store)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def close(self):
        self.closed = True


class FakePsycopg2:
    def __init__(self):
        self.calls = []
        self.connections = []

    def connect(self, dsn):
        conn = FakeConn(self.calls)
        self.connections.append(conn)
        return conn


class FakeContainer:
    """Walk through a sequence of statuses then expose an exit code."""

    def __init__(self, statuses, exit_code=0, id_="cid-abc123"):
        self._statuses = list(statuses)
        self.status = self._statuses[0] if self._statuses else "running"
        self._exit_code = exit_code
        self.id = id_
        self.short_id = id_[:12]
        self.killed = False
        self.removed = False
        self._reload_calls = 0

    def reload(self):
        self._reload_calls += 1
        if self._reload_calls < len(self._statuses):
            self.status = self._statuses[self._reload_calls]
        else:
            self.status = self._statuses[-1] if self._statuses else "running"

    def wait(self, **_):
        return {"StatusCode": self._exit_code}

    def kill(self):
        self.killed = True
        self.status = "exited"

    def remove(self, force=False):  # noqa: ARG002
        self.removed = True


class FakeContainersAPI:
    def __init__(self, container):
        self._container = container
        self.run_calls = []

    def run(self, image, **kwargs):
        self.run_calls.append((image, kwargs))
        return self._container


class FakeDockerClient:
    def __init__(self, container):
        self.containers = FakeContainersAPI(container)


# --------------------------------------------------------------------- #
# unit tests
# --------------------------------------------------------------------- #


def test_stage_heartbeat_event_value():
    """Spec requires the StageEvent enum to expose stage_heartbeat."""
    assert StageEvent.HEARTBEAT == "stage_heartbeat"


def test_write_heartbeat_row_inserts_expected_columns():
    fake = FakePsycopg2()
    _write_heartbeat_row(
        dsn="dsn://test",
        run_id="run-1",
        stage="stage_heartbeat",
        task_id="stage_canonicalize",
        container_id="cid-xyz",
        psycopg2_module=fake,
    )
    assert len(fake.calls) == 1
    sql, params = fake.calls[0]
    assert "INSERT INTO pipeline.run_stage_log" in sql
    # event_type position 4 (0-indexed=3)
    assert params[3] == "stage_heartbeat"
    assert params[0] == "run-1"
    assert params[1] == "stage_heartbeat"
    assert fake.connections[0].closed is True


def test_operator_polls_until_container_exits():
    """Operator polls container.reload() then returns exit_code=0 cleanly."""
    container = FakeContainer(["running", "running", "exited"], exit_code=0)
    docker_client = FakeDockerClient(container)
    fake_pg = FakePsycopg2()

    op = make_long_running_docker_operator(
        task_id="stage_canonicalize",
        image="glue:test",
        command="spark-submit job.py",
        environment={"FOO": "bar"},
        mounts=[],
        heartbeat_seconds=1,
        soft_timeout_minutes=10,
        poll_interval_seconds=0,  # tight loop for tests
        psycopg2_module=fake_pg,
        docker_client_factory=lambda: docker_client,
    )

    # No Airflow context — minimal dict.
    result = op.execute({"ti": None})

    assert result["exit_code"] == 0
    assert result["container_id"] == "cid-abc123"
    assert container._reload_calls >= 2
    # Initial heartbeat row was written.
    assert any(p[3] == "stage_heartbeat" for _, p in fake_pg.calls)
    assert container.removed is True


def test_operator_raises_on_nonzero_exit():
    container = FakeContainer(["running", "exited"], exit_code=42)
    docker_client = FakeDockerClient(container)
    op = make_long_running_docker_operator(
        task_id="stage_canonicalize",
        image="glue:test",
        command="spark-submit fail.py",
        heartbeat_seconds=1,
        soft_timeout_minutes=10,
        poll_interval_seconds=0,
        psycopg2_module=FakePsycopg2(),
        docker_client_factory=lambda: docker_client,
    )
    with pytest.raises(RuntimeError, match="non-zero"):
        op.execute({"ti": None})


def test_force_stop_when_no_heartbeat_for_2x_interval():
    """If state.last_beat_at goes stale the operator must kill the container."""
    # Container says it stays running forever — only force-stop ends the loop.
    container = FakeContainer(["running"] * 50, exit_code=0)
    docker_client = FakeDockerClient(container)
    fake_pg = FakePsycopg2()

    op = make_long_running_docker_operator(
        task_id="stage_postgres_write",
        image="glue:test",
        command="spark-submit job.py",
        heartbeat_seconds=1,           # stall = 2s
        soft_timeout_minutes=999,      # rule out the soft timeout path
        poll_interval_seconds=0,
        psycopg2_module=fake_pg,
        docker_client_factory=lambda: docker_client,
    )

    # Patch out the heartbeat-loop thread so last_beat_at never advances.
    with mock.patch.object(
        op.__class__, "_heartbeat_loop", lambda *a, **k: None
    ):
        # Backdate last_beat_at so the stall trips on the first poll.
        original = op._poll_until_done

        def _poll_with_stale_state(*, container, run_id, state):
            state.last_beat_at = time.time() - 999
            return original(container=container, run_id=run_id, state=state)

        op._poll_until_done = _poll_with_stale_state  # type: ignore[assignment]

        with pytest.raises(RuntimeError, match="heartbeat stalled"):
            op.execute({"ti": None})

    assert container.killed is True


def test_soft_timeout_force_stop():
    container = FakeContainer(["running"] * 50, exit_code=0)
    docker_client = FakeDockerClient(container)
    op = make_long_running_docker_operator(
        task_id="stage_canonicalize",
        image="glue:test",
        command="spark-submit job.py",
        heartbeat_seconds=999,        # rule out the stall path
        soft_timeout_minutes=0,       # 0 minutes = trip immediately
        poll_interval_seconds=0,
        psycopg2_module=FakePsycopg2(),
        docker_client_factory=lambda: docker_client,
    )
    with mock.patch.object(op.__class__, "_heartbeat_loop", lambda *a, **k: None):
        with pytest.raises(RuntimeError, match="soft timeout"):
            op.execute({"ti": None})
    assert container.killed is True


def test_run_id_pulled_from_xcom():
    container = FakeContainer(["running", "exited"], exit_code=0)
    docker_client = FakeDockerClient(container)
    fake_pg = FakePsycopg2()

    op = make_long_running_docker_operator(
        task_id="stage_canonicalize",
        image="glue:test",
        command="spark-submit job.py",
        heartbeat_seconds=99,
        soft_timeout_minutes=10,
        poll_interval_seconds=0,
        run_id_xcom_task="prepare_canonicalize",
        run_id_xcom_key="canonicalize_run_id",
        psycopg2_module=fake_pg,
        docker_client_factory=lambda: docker_client,
    )

    fake_ti = SimpleNamespace(
        xcom_pull=mock.Mock(return_value="run-from-xcom-1234"),
    )
    op.execute({"ti": fake_ti})

    fake_ti.xcom_pull.assert_called_once_with(
        task_ids="prepare_canonicalize", key="canonicalize_run_id",
    )
    # Initial heartbeat captures the resolved run_id.
    inserted_run_ids = [p[0] for _, p in fake_pg.calls]
    assert "run-from-xcom-1234" in inserted_run_ids
