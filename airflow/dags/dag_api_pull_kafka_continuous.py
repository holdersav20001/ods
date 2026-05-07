"""Long-running direct-Kafka api_pull runner — sibling to ``dag_api_pull``.

The scheduled ``dag_api_pull`` DAG calls ``run_once`` per Airflow tick
(15-minute schedule by default). Datasets that need sub-tick latency
opt in by setting ``source_config.continuous = true`` in
``pipeline.dataset_config``. This DAG runs at ``schedule=None`` —
operators trigger it manually (or via an external supervisor) and the
PythonOperator stays alive looping ``run_once`` until the Airflow
heartbeat is lost or the operator is killed.

Trade-offs vs. the scheduled DAG:
  * No new infrastructure — same scheduler, same workers.
  * Long-running tasks consume one worker slot for the duration. Use
    a dedicated pool ``api_pull_continuous`` to isolate the slot
    accounting from short-lived tasks.
  * The scheduled ``dag_api_pull`` still owns finalisation and
    promotion. The loop only writes ``pending_cursor_value``; the
    finalise step in the scheduled DAG continues to promote based on
    JDBC sink consumer offsets.

See ``docs/api-pull-direct-kafka-design.md`` § Components →
"Long-running poller" option (a).
"""
from __future__ import annotations

import json
import os
import signal
import sys
import threading
from datetime import datetime, timezone

import psycopg2

from airflow import DAG
from airflow.operators.python import PythonOperator

# Match dag_api_pull's path bootstrapping so ods_pipeline imports work
# whether the DAG file is loaded from /opt/airflow/dags or a bind-mounted
# repo root.
_DAG_DIR = os.path.dirname(__file__)
for _root in (
    os.path.abspath(os.path.join(_DAG_DIR, "..")),
    os.path.abspath(os.path.join(_DAG_DIR, "..", "..")),
):
    if _root not in sys.path:
        sys.path.insert(0, _root)

from ods_pipeline.ingest.api_pull import WatermarkStore  # noqa: E402
from ods_pipeline.ingest.api_pull_kafka import run_loop  # noqa: E402

PG_DSN = os.environ.get(
    "PIPELINE_PG_DSN",
    "host=postgres port=5432 dbname=ods_dev user=ods password=ods",
)
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "broker:29092")
SCHEMA_REGISTRY_URL = os.environ.get(
    "SCHEMA_REGISTRY_URL", "http://schema-registry:8081"
)
HEARTBEAT_TIMEOUT_SECONDS = float(
    os.environ.get("API_PULL_LOOP_HEARTBEAT_TIMEOUT_SECONDS", "3600")
)


def _connect_pg():
    return psycopg2.connect(PG_DSN)


class _ConnectionBackedStore:
    """Watermark store that owns its own connection so ``close`` is safe.

    ``run_loop`` calls ``factory()`` per iteration and then ``close()``
    on the returned object — wrapping :class:`WatermarkStore` lets us
    reuse a fresh psycopg2 connection per tick without leaking sockets.
    """

    def __init__(self):
        self._conn = _connect_pg()
        self._store = WatermarkStore(self._conn)

    def read(self, **kwargs):
        return self._store.read(**kwargs)

    def record_pending(self, **kwargs):
        return self._store.record_pending(**kwargs)

    def close(self):
        try:
            self._conn.close()
        except Exception:
            pass


def _list_continuous_datasets() -> list[dict]:
    """Return active datasets where ``delivery=direct_kafka`` AND
    ``source_config.continuous`` is true.

    Run inside the operator (not at DAG parse time) so we always see
    the current ``dataset_config`` snapshot when a manual trigger fires.
    """
    conn = _connect_pg()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT domain, dataset, schema_id, schema_version,
                       target_topic, source_config, config_version_id
                  FROM pipeline.dataset_config
                 WHERE active = TRUE
                   AND source_type = 'api_pull'
                   AND COALESCE(delivery, 'file_pipeline') = 'direct_kafka'
                """
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    out: list[dict] = []
    for (
        domain, dataset, schema_id, schema_version,
        target_topic, source_config, config_version_id,
    ) in rows:
        if isinstance(source_config, str):
            source_config = json.loads(source_config)
        source = source_config or {}
        if not bool(source.get("continuous")):
            continue
        out.append({
            "domain": domain,
            "dataset": dataset,
            "schema_id": schema_id,
            "schema_version": schema_version,
            "target_topic": target_topic,
            "source": source,
            "config_version_id": config_version_id,
        })
    return out


def _fetch_schema_str(subject: str) -> str:
    """Mirror of dag_api_pull._fetch_schema_str — kept inline so a
    Schema Registry blip doesn't break the scheduled DAG's parse."""
    import requests
    resp = requests.get(
        f"{SCHEMA_REGISTRY_URL}/subjects/{subject}-value/versions/latest",
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()["schema"]


def _run_dataset_loop(cfg: dict) -> None:
    """PythonOperator entrypoint for one dataset.

    Sets up a stop_event wired to SIGTERM so Airflow's task-kill path
    drains the producer cleanly. Returns when the loop exits — either
    because the operator was killed or because ``run_once`` raised an
    unrecoverable error.
    """
    stop_event = threading.Event()

    def _handle_signal(signum, _frame):  # pragma: no cover — signals
        stop_event.set()

    try:
        signal.signal(signal.SIGTERM, _handle_signal)
        signal.signal(signal.SIGINT, _handle_signal)
    except (ValueError, OSError):
        # Not on the main thread — Airflow's executor variant may run
        # operators in a thread pool. The operator's own kill path
        # still raises so the loop will exit.
        pass

    schema_str = _fetch_schema_str(cfg["schema_id"])
    dataset_config = {
        "domain": cfg["domain"],
        "dataset": cfg["dataset"],
        "schema_id": cfg["schema_id"],
        "schema_version": cfg.get("schema_version", 1),
        "target_topic": cfg["target_topic"],
        "source": cfg["source"],
    }
    run_loop(
        dataset_config,
        kafka_bootstrap=KAFKA_BOOTSTRAP,
        schema_registry_url=SCHEMA_REGISTRY_URL,
        schema_str=schema_str,
        watermark_store_factory=_ConnectionBackedStore,
        stop_event=stop_event,
    )


# ---------------------------------------------------------------------------
# DAG construction
# ---------------------------------------------------------------------------

with DAG(
    dag_id="dag_api_pull_kafka_continuous",
    description=(
        "Long-running direct-Kafka api_pull poller. One PythonOperator "
        "per dataset where source_config.continuous=true."
    ),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    tags=["api_pull", "direct_kafka", "continuous"],
    default_args={
        "owner": "ods-platform",
        "retries": 0,
        # Long-running operator: rely on heartbeat / SIGTERM, not retries.
        "execution_timeout": None,
    },
) as dag:
    # Dataset selection runs at parse time so each dataset gets its own
    # Airflow task id. A manual trigger picks up the snapshot from the
    # last scheduler parse — operators redeploy this DAG (or trigger
    # ``dag_config_sync``) when adding a new continuous dataset.
    try:
        _DATASETS = _list_continuous_datasets()
    except Exception:  # noqa: BLE001 — DAG must parse even if PG is down
        _DATASETS = []

    for _cfg in _DATASETS:
        _task_id = f"loop__{_cfg['domain']}__{_cfg['dataset']}"
        PythonOperator(
            task_id=_task_id,
            python_callable=_run_dataset_loop,
            op_args=[_cfg],
            pool="api_pull_continuous",
        )
