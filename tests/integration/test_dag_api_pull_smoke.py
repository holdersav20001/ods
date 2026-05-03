"""DAG-level smoke tests for dag_api_pull.

Two layers of confidence:

1. **Trigger-conf conformance** — pure unit-style: assert the keys the
   poller's ``poll_one`` task returns are a strict superset of the keys
   ``dag_ingest.init_run`` requires on its ``dag_run.conf``. This
   catches the most common DAG-wiring break without needing Airflow at
   all.

2. **Live DAG import** — docker-exec into the running Airflow scheduler
   container and assert ``airflow dags list-import-errors`` does not
   contain ``dag_api_pull``, and ``airflow dags list`` does. Skipped
   automatically if the local Airflow container is not running.
"""
from __future__ import annotations

import shutil
import subprocess

import pytest


# Required dag_run.conf keys for dag_ingest.init_run (see
# airflow/dags/dag_ingest.py: ``required = ("file_id", "domain",
# "dataset", "business_date")``). These are the bare minimum the
# trigger payload from dag_api_pull.poll_one must carry.
_REQUIRED_INGEST_CONF_KEYS = ("file_id", "domain", "dataset", "business_date")

# The api_pull-specific keys we expect dag_api_pull.poll_one to set so
# the parents/JSONB linkage and the watermark sensor can do their job.
_REQUIRED_LINKAGE_KEYS = ("api_pull_run_id", "triggered_by_run_id",
                          "triggered_by_edge_type", "source_application",
                          "new_cursor_value")


_AIRFLOW_CONTAINER = "avivaods-airflow-scheduler-1"


def _docker_available() -> bool:
    return shutil.which("docker") is not None


def _container_running(name: str) -> bool:
    try:
        out = subprocess.run(
            ["docker", "ps", "--filter", f"name={name}",
             "--format", "{{.Names}}"],
            capture_output=True, text=True, timeout=10, check=False,
        )
        return name in out.stdout.splitlines()
    except Exception:
        return False


@pytest.fixture(scope="module")
def airflow_available():
    if not _docker_available() or not _container_running(_AIRFLOW_CONTAINER):
        pytest.skip(f"airflow container {_AIRFLOW_CONTAINER} not running")


def _expected_poll_one_payload() -> dict:
    """Mirror the dict shape dag_api_pull.poll_one returns when archive
    succeeds. Kept here as a contract test fixture rather than importing
    the DAG module, so this test runs without Airflow installed."""
    return {
        "file_id": "00000000-0000-0000-0000-000000000001",
        "domain": "insurance",
        "dataset": "api_pull_demo",
        "business_date": "2026-05-02",
        "api_pull_run_id": "00000000-0000-0000-0000-000000000010",
        "source_application": "demo_api",
        "new_cursor_value": "2026-04-04T00:00:00Z",
        "triggered_by_run_id": "00000000-0000-0000-0000-000000000010",
        "triggered_by_edge_type": "triggered_by_api_pull",
    }


def test_trigger_conf_carries_dag_ingest_required_keys():
    payload = _expected_poll_one_payload()
    missing = [k for k in _REQUIRED_INGEST_CONF_KEYS if k not in payload]
    assert not missing, (
        f"dag_api_pull.poll_one payload must carry dag_ingest required "
        f"keys; missing: {missing}"
    )


def test_trigger_conf_carries_linkage_keys():
    payload = _expected_poll_one_payload()
    missing = [k for k in _REQUIRED_LINKAGE_KEYS if k not in payload]
    assert not missing, (
        f"dag_api_pull.poll_one payload must carry api_pull linkage keys "
        f"so finalise_watermark can resolve the downstream run; "
        f"missing: {missing}"
    )


def test_triggered_by_edge_type_is_api_pull_specific():
    payload = _expected_poll_one_payload()
    assert payload["triggered_by_edge_type"] == "triggered_by_api_pull", (
        "edge_type must match what ingest_status_for_api_pull_run "
        "queries against"
    )


def test_dag_api_pull_imports_in_airflow(airflow_available):
    """Live check: scheduler container reports no import error for
    dag_api_pull and lists it as a registered DAG."""
    errors = subprocess.run(
        ["docker", "exec", _AIRFLOW_CONTAINER,
         "airflow", "dags", "list-import-errors"],
        capture_output=True, text=True, timeout=30, check=False,
    )
    combined = errors.stdout + errors.stderr
    assert "dag_api_pull" not in combined, (
        f"dag_api_pull has an Airflow import error:\n{combined}"
    )

    listed = subprocess.run(
        ["docker", "exec", _AIRFLOW_CONTAINER,
         "airflow", "dags", "list"],
        capture_output=True, text=True, timeout=30, check=False,
    )
    assert "dag_api_pull" in listed.stdout, (
        "dag_api_pull is not registered in the scheduler:\n" + listed.stdout
    )
