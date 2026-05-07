"""Unit tests for R9 — pin Schema Registry subject lookups to the
configured ``dataset_config.schema_version`` instead of ``latest``.

Two runs of the same dataset must hit the same Schema Registry URL,
even if a newer version was registered between runs. This guards
against silent wire-shape changes for direct-Kafka API pull datasets.
"""
from __future__ import annotations

import os
import sys
from unittest import mock

import pytest


# Resolve repo root so ``airflow.dags.dag_api_pull`` imports cleanly when
# the test runs outside the Airflow scheduler container.
_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)


def _stub_airflow() -> None:
    """Stub the airflow surface dag_api_pull touches at import time so
    this unit test runs outside the scheduler container. We only need
    enough scaffolding to let the module load — no DAG execution.

    Other test modules (e.g. test_recon_t2) install thinner stubs that
    don't accept keyword args. Evict them so our richer shim wins.
    """
    import types

    for _mod in (
        "airflow",
        "airflow.decorators",
        "airflow.operators",
        "airflow.operators.trigger_dagrun",
        "airflow.utils",
        "airflow.utils.trigger_rule",
    ):
        sys.modules.pop(_mod, None)

    if "airflow" not in sys.modules:
        airflow_mod = types.ModuleType("airflow")

        class _DAG:  # noqa: D401 — placeholder
            def __init__(self, *args, **kwargs) -> None: ...
            def __enter__(self): return self
            def __exit__(self, *exc): return False

        airflow_mod.DAG = _DAG
        sys.modules["airflow"] = airflow_mod

    if "airflow.decorators" not in sys.modules:
        decorators_mod = types.ModuleType("airflow.decorators")

        class _TaskShim:
            """Stand-in for @task-decorated callables. Calling the shim
            (or .expand) returns another shim instead of running the body
            so module-level DAG construction can't reach the DB."""
            def __init__(self, fn):
                self._fn = fn
            def __call__(self, *a, **kw):
                return _TaskShim(self._fn)
            def expand(self, *a, **kw):
                return _TaskShim(self._fn)
            def __rshift__(self, other): return other
            def __lshift__(self, other): return other

        def _task(*dargs, **dkwargs):
            if dargs and callable(dargs[0]):
                return _TaskShim(dargs[0])
            def _wrap(fn):
                return _TaskShim(fn)
            return _wrap
        decorators_mod.task = _task
        sys.modules["airflow.decorators"] = decorators_mod

    if "airflow.operators" not in sys.modules:
        sys.modules["airflow.operators"] = types.ModuleType("airflow.operators")
    if "airflow.operators.trigger_dagrun" not in sys.modules:
        trigger_mod = types.ModuleType("airflow.operators.trigger_dagrun")

        class _OpShim:
            def __init__(self, *args, **kwargs) -> None: ...
            def expand(self, *a, **kw): return self
            def __rshift__(self, other): return other
            def __lshift__(self, other): return other

        class _TriggerDagRunOperator:
            def __init__(self, *args, **kwargs) -> None: ...
            @classmethod
            def partial(cls, *a, **kw): return _OpShim()
        trigger_mod.TriggerDagRunOperator = _TriggerDagRunOperator
        sys.modules["airflow.operators.trigger_dagrun"] = trigger_mod

    if "airflow.utils" not in sys.modules:
        sys.modules["airflow.utils"] = types.ModuleType("airflow.utils")
    if "airflow.utils.trigger_rule" not in sys.modules:
        rule_mod = types.ModuleType("airflow.utils.trigger_rule")
        class _TR:
            ALL_DONE = "all_done"
            ALL_SUCCESS = "all_success"
            ONE_FAILED = "one_failed"
            NONE_FAILED = "none_failed"
        rule_mod.TriggerRule = _TR
        sys.modules["airflow.utils.trigger_rule"] = rule_mod


def _import_module():
    # Importing the DAG module pulls in airflow; do it lazily so a
    # collection error in one test doesn't poison the whole file.
    _stub_airflow()
    import importlib
    spec_path = os.path.join(_REPO_ROOT, "airflow", "dags", "dag_api_pull.py")
    import importlib.util
    spec = importlib.util.spec_from_file_location("dag_api_pull_under_test", spec_path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    return mod


class _FakeResponse:
    def __init__(self, payload: dict) -> None:
        self._payload = payload
        self.status_code = 200

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return self._payload


@pytest.fixture
def dag_module():
    return _import_module()


def test_fetch_schema_str_uses_explicit_int_version(dag_module):
    """Numeric version is sent verbatim — never `latest`."""
    captured: dict[str, str] = {}

    def fake_get(url: str, timeout: int = 10) -> _FakeResponse:
        captured["url"] = url
        return _FakeResponse({"schema": "{\"type\":\"record\"}"})

    with mock.patch("requests.get", side_effect=fake_get):
        out = dag_module._fetch_schema_str("ods.policies-value", 3)

    assert out == "{\"type\":\"record\"}"
    assert "/subjects/ods.policies-value/versions/3" in captured["url"]
    assert "latest" not in captured["url"]


def test_fetch_schema_str_accepts_numeric_string(dag_module):
    """Numeric strings (Postgres NUMERIC -> str) coerce to the int path."""
    captured: dict[str, str] = {}

    def fake_get(url: str, timeout: int = 10) -> _FakeResponse:
        captured["url"] = url
        return _FakeResponse({"schema": "{}"})

    with mock.patch("requests.get", side_effect=fake_get):
        dag_module._fetch_schema_str("ods.claims-value", "7")

    assert "/versions/7" in captured["url"]
    assert "latest" not in captured["url"]


def test_fetch_schema_str_falls_back_to_latest_when_unset(dag_module):
    """Backward-compat: callers passing the literal default still work."""
    captured: dict[str, str] = {}

    def fake_get(url: str, timeout: int = 10) -> _FakeResponse:
        captured["url"] = url
        return _FakeResponse({"schema": "{}"})

    with mock.patch("requests.get", side_effect=fake_get):
        dag_module._fetch_schema_str("ods.policies-value")

    assert "/versions/latest" in captured["url"]


def test_fetch_schema_str_non_numeric_string_is_treated_as_latest(dag_module):
    """Defensive: only numeric versions are pinned; junk falls through."""
    captured: dict[str, str] = {}

    def fake_get(url: str, timeout: int = 10) -> _FakeResponse:
        captured["url"] = url
        return _FakeResponse({"schema": "{}"})

    with mock.patch("requests.get", side_effect=fake_get):
        dag_module._fetch_schema_str("ods.policies-value", "v2-rebased")

    assert "/versions/latest" in captured["url"]


@pytest.mark.parametrize("bad_version,expected_path", [
    ("3.0", "latest"),         # semver-style — not pinned
    ("-1", "latest"),           # SR sentinel for "latest" — fall back explicitly
    ("0", "latest"),            # SR rejects 0 anyway
    (-5, "latest"),             # negative int
    (0, "latest"),              # zero int
    ("  ", "latest"),           # whitespace
    ("", "latest"),             # empty
    (" 3 ", "3"),               # whitespace stripped, valid int kept
    ("03", "3"),                # leading zeros stripped (SR strict deploys reject /versions/03)
    (None, "latest"),           # explicit None
])
def test_fetch_schema_str_edge_cases(dag_module, bad_version, expected_path):
    """Reality Checker F8 — pin parser must not silently degrade or break SR."""
    captured: dict[str, str] = {}

    def fake_get(url: str, timeout: int = 10) -> _FakeResponse:
        captured["url"] = url
        return _FakeResponse({"schema": "{}"})

    with mock.patch("requests.get", side_effect=fake_get):
        dag_module._fetch_schema_str("ods.policies-value", bad_version)

    assert f"/versions/{expected_path}" in captured["url"]
