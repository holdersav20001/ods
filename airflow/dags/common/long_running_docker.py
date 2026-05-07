"""Long-running DockerOperator with heartbeat + force-stop (R8).

The standard ``DockerOperator`` blocks an Airflow worker slot for the entire
duration of the container. Multi-hour Glue jobs (canonicalize over a 30-day
soak; large historic backfills; direct-Postgres bulk loads) burn a slot for
hours and lose all liveness signal — if the container hangs nothing notices
until the Airflow task timeout fires.

This helper wraps ``DockerOperator`` so that:

* The container is started and then polled every ``poll_interval_seconds``.
* A background thread writes a ``stage_heartbeat`` row to
  ``pipeline.run_stage_log`` every ``heartbeat_seconds`` while the container
  is alive — ops dashboards and recon tools can detect stalled stages.
* If a heartbeat write fails (or no heartbeat happens for >2× the configured
  interval) the operator force-stops the container and raises so the run is
  marked failed rather than wedged.
* Each Airflow log line is prefixed with ``[long_running pid=<pid> cid=<id>]``
  so ops can grep both PID (host-side worker) and container ID.

Public API: :func:`make_long_running_docker_operator`.
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
import uuid
from typing import Any, Optional

# Airflow / docker SDK imports are deferred to runtime so unit tests can
# instantiate the class without an Airflow runtime present.
try:  # pragma: no cover - import-time path
    from airflow.providers.docker.operators.docker import DockerOperator
except Exception:  # pragma: no cover
    DockerOperator = object  # type: ignore[assignment,misc]


_HEARTBEAT_STAGE_DEFAULT = "stage_heartbeat"


def _default_pg_dsn() -> str:
    return os.environ.get(
        "PIPELINE_PG_DSN",
        "host=postgres port=5432 dbname=ods_dev user=ods password=ods",
    )


def _write_heartbeat_row(
    *,
    dsn: str,
    run_id: str,
    stage: str,
    task_id: str,
    container_id: Optional[str],
    psycopg2_module: Any = None,
    conn: Any = None,
) -> Any:
    """Insert a single ``stage_heartbeat`` row.

    If ``conn`` is supplied the caller owns it (Reality Checker F5: the
    loop persists one connection across all beats so a long-running
    Glue task does not connect/close once per heartbeat — this saturates
    ``pg_stat_activity`` under PgBouncer). Otherwise a fresh connection
    is opened and closed for the one-shot initial heartbeat path.

    Returns the connection used so callers can persist it.
    """
    if psycopg2_module is None:  # pragma: no cover - real-runtime path
        import psycopg2 as psycopg2_module  # type: ignore[import-not-found]

    owns_conn = conn is None
    if owns_conn:
        # Bound the connect attempt: a hung Postgres must not stall the
        # heartbeat thread past `thread.join(timeout=5)` (CR R8 🟡).
        conn = psycopg2_module.connect(dsn, connect_timeout=5)
    # Code Reviewer R8 🔴: jsonb metrics built via ``%`` interpolation
    # produced invalid JSON when ``task_id`` / ``container_id`` contained
    # quotes or backslashes — the INSERT then failed under jsonb parsing.
    # Build via ``json.dumps`` and bind separately.
    metrics_json = json.dumps({
        "task_id": task_id,
        "container_id": container_id or "unknown",
    })
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO pipeline.run_stage_log
                        (run_id, stage, status, event_type, attempt_number,
                         started_at, ended_at,
                         metrics)
                    VALUES (%s, %s, %s, %s, %s, NOW(), NOW(), %s)
                    """,
                    (
                        run_id,
                        stage,
                        "running",
                        "stage_heartbeat",
                        1,
                        metrics_json,
                    ),
                )
    except Exception:
        if owns_conn:
            try:
                conn.close()
            except Exception:
                pass
        raise
    if owns_conn:
        conn.close()
        return None
    return conn


class _HeartbeatState:
    """Mutable state shared between the heartbeat thread and the main loop."""

    __slots__ = ("last_beat_at", "stop", "errors")

    def __init__(self) -> None:
        self.last_beat_at: float = time.time()
        self.stop: threading.Event = threading.Event()
        self.errors: list[str] = []


def make_long_running_docker_operator(
    *,
    task_id: str,
    image: str,
    command: str,
    environment: Optional[dict] = None,
    mounts: Optional[list] = None,
    heartbeat_seconds: int = 30,
    soft_timeout_minutes: int = 240,
    poll_interval_seconds: int = 10,
    run_id_xcom_key: str = "run_id",
    run_id_xcom_task: Optional[str] = None,
    pg_dsn: Optional[str] = None,
    heartbeat_stage: str = _HEARTBEAT_STAGE_DEFAULT,
    psycopg2_module: Any = None,
    docker_client_factory: Any = None,
    **docker_kwargs: Any,
):
    """Construct a long-running DockerOperator subclass *instance*.

    The returned object is a ``DockerOperator`` instance with overridden
    ``execute`` semantics. Subclassing happens dynamically so the public
    callsite stays a single function call (matches the spec signature).

    Parameters
    ----------
    task_id, image, command, environment, mounts:
        Standard ``DockerOperator`` params, forwarded as-is.
    heartbeat_seconds:
        How often to write a ``stage_heartbeat`` row (default 30s).
    soft_timeout_minutes:
        Force-stop the container if it has been running this long (default 240).
    poll_interval_seconds:
        How often to check ``container.status`` (default 10s).
    run_id_xcom_task / run_id_xcom_key:
        Where to read the canonical run_id used for the heartbeat rows. If
        ``run_id_xcom_task`` is None the run_id falls back to ``task_id``
        plus a uuid suffix (heartbeat still works, just unattached to a run).
    pg_dsn, psycopg2_module, docker_client_factory:
        Injection points for tests.
    **docker_kwargs:
        Anything else passed to ``DockerOperator`` (e.g. network_mode).
    """
    if mounts is None:
        mounts = []

    # Resolve at construction time so tests can patch easily.
    resolved_dsn = pg_dsn or _default_pg_dsn()
    resolved_psycopg2 = psycopg2_module

    if docker_client_factory is None:  # pragma: no cover - runtime-only
        def docker_client_factory():  # type: ignore[no-redef]
            import docker  # noqa: WPS433
            return docker.from_env()

    base_cls = DockerOperator if DockerOperator is not object else object

    class LongRunningDockerOperator(base_cls):  # type: ignore[misc,valid-type]
        """DockerOperator that polls + heartbeats long containers."""

        # Class attributes captured at definition time (closure).
        _heartbeat_seconds = heartbeat_seconds
        _soft_timeout_seconds = soft_timeout_minutes * 60
        _poll_interval_seconds = poll_interval_seconds
        _heartbeat_stage = heartbeat_stage
        _pg_dsn = resolved_dsn
        _psycopg2 = resolved_psycopg2
        _docker_client_factory = staticmethod(docker_client_factory)
        _run_id_xcom_task = run_id_xcom_task
        _run_id_xcom_key = run_id_xcom_key
        _logger = logging.getLogger("airflow.task.long_running_docker")

        # ----- helpers -------------------------------------------------- #

        def _resolve_run_id(self, context: dict) -> str:
            """Pull a stable run_id for the heartbeat rows."""
            if self._run_id_xcom_task is None:
                return f"{self.task_id}-{uuid.uuid4()}"
            ti = context.get("ti") or context.get("task_instance")
            if ti is None:
                return f"{self.task_id}-{uuid.uuid4()}"
            try:
                value = ti.xcom_pull(
                    task_ids=self._run_id_xcom_task,
                    key=self._run_id_xcom_key,
                )
            except Exception:  # pragma: no cover - belt+braces
                value = None
            return str(value) if value else f"{self.task_id}-{uuid.uuid4()}"

        def _heartbeat_loop(
            self,
            *,
            run_id: str,
            container_id_box: dict,
            state: _HeartbeatState,
        ) -> None:
            """Background thread: write a row every ``heartbeat_seconds``.

            Holds one persistent psycopg2 connection across all beats
            (Reality Checker F5). If a write fails the connection is
            dropped and reopened on the next iteration so a flapping DB
            cannot wedge the loop on a half-dead socket.
            """
            persistent_conn: Any = None
            try:
                while not state.stop.wait(self._heartbeat_seconds):
                    try:
                        persistent_conn = _write_heartbeat_row(
                            dsn=self._pg_dsn,
                            run_id=run_id,
                            stage=self._heartbeat_stage,
                            task_id=self.task_id,
                            container_id=container_id_box.get("id"),
                            psycopg2_module=self._psycopg2,
                            conn=persistent_conn,
                        )
                        state.last_beat_at = time.time()
                    except Exception as exc:  # noqa: BLE001
                        state.errors.append(str(exc))
                        self._logger.warning(
                            "[long_running pid=%s cid=%s] heartbeat write failed: %s",
                            os.getpid(),
                            container_id_box.get("id"),
                            exc,
                        )
                        if persistent_conn is not None:
                            try:
                                persistent_conn.close()
                            except Exception:
                                pass
                        persistent_conn = None
            finally:
                if persistent_conn is not None:
                    try:
                        persistent_conn.close()
                    except Exception:
                        pass

        def _poll_until_done(
            self,
            *,
            container,
            run_id: str,
            state: _HeartbeatState,
        ) -> int:
            """Loop polling container.status; force-stop on stall/timeout.

            Returns the container exit code.
            """
            started_at = time.time()
            stall_window = 2 * self._heartbeat_seconds
            cid = getattr(container, "id", None) or getattr(container, "short_id", None)

            while True:
                container.reload()
                status = getattr(container, "status", "running")
                self._logger.info(
                    "[long_running pid=%s cid=%s] poll status=%s",
                    os.getpid(),
                    cid,
                    status,
                )
                if status in {"exited", "dead", "removed"}:
                    result = container.wait()
                    return int(result.get("StatusCode", 0))

                # Hard stop conditions.
                runtime = time.time() - started_at
                stalled = (time.time() - state.last_beat_at) > stall_window
                if runtime > self._soft_timeout_seconds:
                    self._logger.error(
                        "[long_running pid=%s cid=%s] soft timeout %.1fs reached, killing",
                        os.getpid(), cid, runtime,
                    )
                    container.kill()
                    raise RuntimeError(
                        f"long-running container exceeded soft timeout "
                        f"({self._soft_timeout_seconds}s) for task {self.task_id}"
                    )
                if stalled:
                    self._logger.error(
                        "[long_running pid=%s cid=%s] no heartbeat for >%.0fs, killing",
                        os.getpid(), cid, stall_window,
                    )
                    container.kill()
                    raise RuntimeError(
                        f"long-running container heartbeat stalled "
                        f"(>{stall_window}s) for task {self.task_id}"
                    )

                time.sleep(self._poll_interval_seconds)

        # ----- main entry ----------------------------------------------- #

        def execute(self, context):  # type: ignore[override]
            """Run the container with heartbeat + poll instead of blocking wait."""
            run_id = self._resolve_run_id(context)
            client = self._docker_client_factory()
            container_id_box: dict = {"id": None}
            state = _HeartbeatState()

            self._logger.info(
                "[long_running pid=%s cid=pending] starting image=%s task=%s",
                os.getpid(), image, self.task_id,
            )

            container = client.containers.run(
                image,
                command=command,
                environment=environment or {},
                mounts=mounts,
                detach=True,
                **{k: v for k, v in docker_kwargs.items() if k != "task_id"},
            )
            container_id_box["id"] = getattr(container, "id", None)

            # Initial heartbeat so the dashboard shows liveness instantly.
            try:
                _write_heartbeat_row(
                    dsn=self._pg_dsn,
                    run_id=run_id,
                    stage=self._heartbeat_stage,
                    task_id=self.task_id,
                    container_id=container_id_box["id"],
                    psycopg2_module=self._psycopg2,
                )
            except Exception as exc:  # noqa: BLE001
                self._logger.warning(
                    "[long_running pid=%s cid=%s] initial heartbeat failed: %s",
                    os.getpid(), container_id_box["id"], exc,
                )

            thread = threading.Thread(
                target=self._heartbeat_loop,
                kwargs={
                    "run_id": run_id,
                    "container_id_box": container_id_box,
                    "state": state,
                },
                daemon=True,
                name=f"hb-{self.task_id}",
            )
            thread.start()

            try:
                exit_code = self._poll_until_done(
                    container=container, run_id=run_id, state=state,
                )
            finally:
                state.stop.set()
                thread.join(timeout=5)
                try:
                    container.remove(force=True)
                except Exception:  # pragma: no cover - best-effort cleanup
                    pass

            if exit_code != 0:
                raise RuntimeError(
                    f"long-running container exited non-zero ({exit_code}) "
                    f"for task {self.task_id}"
                )
            return {"exit_code": exit_code, "container_id": container_id_box["id"]}

    # Build the operator instance. We pass DockerOperator-compat kwargs
    # so introspection (Airflow templating, log links) keeps working — but
    # ``execute`` is overridden so the standard wait path never runs.
    base_kwargs = dict(
        task_id=task_id,
        image=image,
        command=command,
        environment=environment or {},
        mounts=mounts,
    )
    base_kwargs.update(docker_kwargs)

    if base_cls is object:  # tests / no Airflow installed
        instance = LongRunningDockerOperator()
        instance.task_id = task_id  # type: ignore[attr-defined]
        instance.image = image  # type: ignore[attr-defined]
        instance.command = command  # type: ignore[attr-defined]
        return instance

    return LongRunningDockerOperator(**base_kwargs)


__all__ = ["make_long_running_docker_operator"]
