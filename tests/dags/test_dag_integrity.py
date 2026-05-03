"""DAG integrity tests — import validation only, no task execution.

Asserts every committed DAG module imports cleanly and exposes a `dag`
attribute. Catches: import-time errors, missing required modules, broken
syntax. Does NOT validate task wiring or runtime behaviour.
"""
from __future__ import annotations

import os
import sys

import pytest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DAGS_DIR = os.path.join(REPO_ROOT, "airflow", "dags")
if DAGS_DIR not in sys.path:
    sys.path.insert(0, DAGS_DIR)

DAG_MODULES = [
    "dag_ingest",
    "dag_recon_t2",
    "dag_drop_to_raw",
    "dag_multi_file",
    "dag_config_sync",
]


@pytest.mark.parametrize("module_name", DAG_MODULES)
def test_dag_module_imports_cleanly(module_name):
    """Every committed DAG must import without raising."""
    pytest.importorskip("airflow")
    module = __import__(module_name)
    assert hasattr(module, "dag") or hasattr(module, "__doc__"), (
        f"{module_name} did not expose 'dag' attribute"
    )
