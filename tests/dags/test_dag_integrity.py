# tests/dags/test_dag_integrity.py
"""DAG integrity tests — import validation only, no task execution."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
import pytest
from airflow.models import DAG

def test_dag2_imports_cleanly():
    import dags.dag2_etl_trigger as m
    assert hasattr(m, "dag")
    assert isinstance(m.dag, DAG)

def test_dag2_has_required_tasks():
    import dags.dag2_etl_trigger as m
    task_ids = {t.task_id for t in m.dag.tasks}
    for required in ["check_file_catalogue", "check_idempotency",
                     "verify_checksum", "trigger_glue_ingestion", "update_file_state"]:
        assert required in task_ids, f"Missing task: {required}"

def test_publish_dag_imports_cleanly():
    import dags.dag_publish as m
    assert hasattr(m, "dag")
    assert isinstance(m.dag, DAG)

def test_publish_dag_has_required_tasks():
    import dags.dag_publish as m
    task_ids = {t.task_id for t in m.dag.tasks}
    for required in ["check_idempotency", "load_config", "set_processing",
                     "trigger_glue_publish", "set_completed", "emit_audit_event"]:
        assert required in task_ids, f"Missing task: {required}"
