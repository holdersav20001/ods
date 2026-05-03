import importlib.util
import sys
import types


def _load_recon_module():
    pendulum = types.ModuleType("pendulum")
    pendulum.datetime = lambda *args, **kwargs: None
    sys.modules["pendulum"] = pendulum

    airflow = types.ModuleType("airflow")

    class DummyDAG:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    airflow.DAG = DummyDAG
    sys.modules["airflow"] = airflow

    class TaskStub:
        def __init__(self, func):
            self.func = func

        def __call__(self, *args, **kwargs):
            return None

    decorators = types.ModuleType("airflow.decorators")
    decorators.task = lambda f: TaskStub(f)
    sys.modules["airflow.decorators"] = decorators

    spec = importlib.util.spec_from_file_location(
        "dag_recon_t2_test", "airflow/dags/dag_recon_t2.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _Cursor:
    def __init__(self, rows):
        self._rows = rows

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, *_args, **_kwargs):
        return None

    def fetchall(self):
        return self._rows


class _Conn:
    def __init__(self, rows):
        self._rows = rows
        self.commits = 0
        self.closed = False

    def cursor(self, *_args, **_kwargs):
        return _Cursor(self._rows)

    def commit(self):
        self.commits += 1

    def close(self):
        self.closed = True


def _row(pipeline_type, write_mode):
    return {
        "run_id": f"{pipeline_type}-{write_mode}",
        "pipeline_type": pipeline_type,
        "domain": "insurance",
        "dataset": f"policies_{write_mode}",
        "business_date": "2026-08-01",
        "file_id": "file-1",
        "record_count_source": 2,
        "record_count_dq_pass": None,
        "postgres_target_table": "ods.insurance_policy",
        "write_mode": write_mode,
        "key_fields": ["policy_id"],
        "schema_def": {"fields": [{"name": "policy_id"}, {"name": "status"}]},
        "tol_rec": 0,
        "tol_pct": 0,
    }


def test_t2_reconciliation_skips_publish_runs(monkeypatch):
    module = _load_recon_module()
    rows = [
        _row("publish", "append"),
        _row("ingestion", "append"),
        _row("publish", "upsert"),
        _row("ingestion", "upsert"),
    ]
    conn = _Conn(rows)
    calls = []

    monkeypatch.setattr(module.psycopg2, "connect", lambda *_args, **_kwargs: conn)
    monkeypatch.setattr(module, "_table_exists", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(
        module,
        "_history_table_for",
        lambda *_args, **_kwargs: ("ods", "insurance_policy_history"),
    )
    monkeypatch.setattr(
        module,
        "_reconcile_append_file_count",
        lambda *_args: calls.append("append"),
    )
    monkeypatch.setattr(
        module,
        "_reconcile_history_file_count",
        lambda *_args: calls.append("history"),
    )
    monkeypatch.setattr(
        module,
        "_reconcile_current_consistency",
        lambda *_args: calls.append("current"),
    )

    module.reconcile.func()

    assert calls == ["append", "history", "current"]
    assert conn.closed


def test_t2_reconciliation_records_upsert_without_history(monkeypatch):
    module = _load_recon_module()
    rows = [_row("canonicalize", "upsert")]
    conn = _Conn(rows)
    calls = []

    monkeypatch.setattr(module.psycopg2, "connect", lambda *_args, **_kwargs: conn)
    monkeypatch.setattr(module, "_table_exists", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(module, "_history_table_for", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        module,
        "_record_current_history_missing",
        lambda *_args: calls.append("missing_history"),
    )
    monkeypatch.setattr(
        module,
        "_reconcile_history_file_count",
        lambda *_args: calls.append("history"),
    )
    monkeypatch.setattr(
        module,
        "_reconcile_current_consistency",
        lambda *_args: calls.append("current"),
    )

    module.reconcile.func()

    assert calls == ["missing_history"]


def test_message_api_runs_are_source_runs():
    module = _load_recon_module()

    assert module._is_source_run(_row("message_api", "append"))
    assert module._is_source_run(_row("canonicalize", "append"))
    assert not module._is_source_run(_row("publish", "append"))
