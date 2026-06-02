"""Import-safety tests for the ods_policy_claims Airflow DAG.

Spec: docs/specs/2026-06-02-airflow-orchestrator-policy-claims-workflow.md
      (§"Airflow DAG Requirements", §"Airflow Context and workflow_run_id").

Airflow is NOT installed in this environment. These tests assert that:

  * ``import dags.policy_claims_dag`` succeeds with NO Airflow installed.
  * When ``AIRFLOW`` is False the module still exposes every task-callable and
    the ``build_dag`` builder without raising (build_dag returns None).
  * The deterministic workflow_run_id derivation is stable and DAG-run-scoped.
  * The task callables / dependency wiring match the spec task graph.

None of these tests require Airflow or a database connection — they only import
and inspect the module, so they run in the default suite.
"""
import importlib

import dags.policy_claims_dag as dag_module


def test_module_imports_without_airflow():
    """The module must import cleanly even when Airflow is absent."""
    mod = importlib.import_module("dags.policy_claims_dag")
    assert mod is dag_module
    # In this environment Airflow is not installed.
    assert dag_module.AIRFLOW is False


def test_dag_id_matches_spec():
    assert dag_module.DAG_ID == "ods_policy_claims"


def test_all_task_callables_exposed_without_airflow():
    """Every spec task id maps to a defined module-level callable."""
    expected = [
        "ingest_policy",
        "ingest_claim",
        "canonicalize_policy",
        "canonicalize_claim",
        "merge_policy_claim",
        "sink_policy_claim",
        "aggregate_policy_claim_daily",
        "sink_policy_claim_daily",
    ]
    assert dag_module.TASK_IDS == expected
    assert list(dag_module.TASK_CALLABLES.keys()) == expected
    for task_id in expected:
        callable_ = dag_module.TASK_CALLABLES[task_id]
        assert callable(callable_)
        # exposed at module scope under the same name
        assert getattr(dag_module, task_id) is callable_


def test_build_dag_returns_none_without_airflow():
    """build_dag must not raise and returns None when Airflow is unavailable."""
    assert dag_module.build_dag() is None
    # The module-level `dag` object is only defined when AIRFLOW is True.
    assert getattr(dag_module, "dag", None) is None


def test_workflow_run_id_is_deterministic_and_run_scoped():
    """Same (dag_id, dag_run_id) -> same ODS workflow id; different run -> different."""
    a = dag_module.derive_workflow_run_id("ods_policy_claims", "run-1")
    a_again = dag_module.derive_workflow_run_id("ods_policy_claims", "run-1")
    b = dag_module.derive_workflow_run_id("ods_policy_claims", "run-2")
    assert a == a_again
    assert a != b
    # valid uuid text
    assert len(a) == 36 and a.count("-") == 4


def test_airflow_orchestrator_context_reexported():
    """The spec helper is importable from the DAG module."""
    from harness.policy_claims_workflow import airflow_orchestrator_context as src
    assert dag_module.airflow_orchestrator_context is src


def test_ingest_policy_pushes_and_derives_workflow_run_id():
    """First task derives the shared id and pushes it via XCom (no DB needed).

    We stub the connection layer and the harness ingest so the test never
    touches Postgres; it only verifies the orchestration glue: derive once,
    push to XCom under the agreed key, and call the harness business function.
    """
    captured = {}

    class _TI:
        def xcom_push(self, *, key, value):
            captured["push"] = (key, value)

    class _Dag:
        dag_id = "ods_policy_claims"

    context = {"dag": _Dag(), "run_id": "scheduled__2026-05-29", "ti": _TI()}

    expected_id = dag_module.derive_workflow_run_id(
        "ods_policy_claims", "scheduled__2026-05-29")

    # Stub connect() and the harness _ingest so no DB / no real work happens.
    import contextlib

    class _FakeConn:
        def commit(self):
            captured["committed"] = True

    @contextlib.contextmanager
    def _fake_connect():
        yield _FakeConn()

    def _fake_ingest(conn, *, workflow_run_id, file, dag_run_id, task_id,
                     execution_type, **kw):
        captured["ingest"] = {
            "workflow_run_id": workflow_run_id,
            "task_id": task_id,
            "execution_type": execution_type,
        }
        return {"run_id": "run-xyz", "file_id": "f", "link_id": "l"}

    orig_connect = dag_module.connect
    orig_ingest = dag_module.pcw._ingest
    orig_binding_enter = dag_module._OrchestratorBinding.__enter__
    orig_binding_exit = dag_module._OrchestratorBinding.__exit__
    try:
        dag_module.connect = _fake_connect
        dag_module.pcw._ingest = _fake_ingest
        # Neutralise the orchestrator monkeypatch binding (it touches pcw only).
        dag_module._OrchestratorBinding.__enter__ = lambda self: self
        dag_module._OrchestratorBinding.__exit__ = lambda self, *exc: False
        result = dag_module.ingest_policy(**context)
    finally:
        dag_module.connect = orig_connect
        dag_module.pcw._ingest = orig_ingest
        dag_module._OrchestratorBinding.__enter__ = orig_binding_enter
        dag_module._OrchestratorBinding.__exit__ = orig_binding_exit

    assert captured["push"] == (dag_module.XCOM_WORKFLOW_RUN_ID, expected_id)
    assert captured["ingest"]["workflow_run_id"] == expected_id
    assert captured["ingest"]["task_id"] == "ingest_policy"
    assert result == "run-xyz"
    assert captured.get("committed") is True
