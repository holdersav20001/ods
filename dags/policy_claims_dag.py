"""Airflow DAG ``ods_policy_claims`` — import-safe scaffolding.

This module wires the EXISTING policy/claims business functions in
``harness/policy_claims_workflow.py`` into an Airflow DAG. It is written so that
``import dags.policy_claims_dag`` succeeds WITHOUT Airflow installed:

  * The Airflow imports are guarded; ``AIRFLOW`` is True only when they succeed.
  * The DAG object is only constructed when ``AIRFLOW`` is True.
  * The task-callable functions and the ``build_dag`` builder are always defined
    at module scope, so they are importable/inspectable with or without Airflow.

It does NOT run locally here (Airflow is not installed). It is the real wiring a
deployment would use: one ``workflow_run_id`` is minted by the first task and
threaded to every downstream task via XCom (tasks do NOT each mint their own),
each task opens its own ``control.db.connect()`` connection and commits per task,
and every ``runs.start`` is passed ``airflow_orchestrator_context(context)`` so
the run_log carries Airflow orchestrator identity (migration 020 columns).

Task graph (policy and claim ingest/canonicalize run in PARALLEL branches):

    ingest_policy >> canonicalize_policy
    ingest_claim  >> canonicalize_claim
    [canonicalize_policy, canonicalize_claim] >> merge_policy_claim
    merge_policy_claim >> sink_policy_claim
    sink_policy_claim >> aggregate_policy_claim_daily
    aggregate_policy_claim_daily >> sink_policy_claim_daily

The per-task callables below are THIN wrappers over the harness business
functions (``_ingest``, ``_canonicalize_to_silver``, ``_merge_to_detail``,
``_sink_rows``, ``_aggregate_from_detail``, plus the row/visibility helpers).
The harness is NOT refactored: each callable rebuilds the same per-hop inputs
the harness uses and pushes the small ids the next task needs through XCom.
"""
from __future__ import annotations

import datetime as dt
import uuid
from typing import Any

from control.db import connect
from harness import policy_claims_workflow as pcw
from harness.policy_claims_workflow import airflow_orchestrator_context  # re-export

# --------------------------------------------------------------------------- #
# Import-safe Airflow guard. The module must import with NO Airflow installed.
# --------------------------------------------------------------------------- #
try:  # pragma: no cover - exercised only where Airflow is installed
    from airflow import DAG
    from airflow.operators.python import PythonOperator

    AIRFLOW = True
except ImportError:  # the local dev / test environment has no Airflow
    DAG = None  # type: ignore[assignment]
    PythonOperator = None  # type: ignore[assignment]
    AIRFLOW = False


DAG_ID = pcw.DAG_ID  # "ods_policy_claims"

# Ordered list of (task_id, callable_name). Used by build_dag and by the test to
# assert every task callable is defined at module scope without Airflow.
TASK_IDS = [
    "ingest_policy",
    "ingest_claim",
    "canonicalize_policy",
    "canonicalize_claim",
    "merge_policy_claim",
    "sink_policy_claim",
    "aggregate_policy_claim_daily",
    "sink_policy_claim_daily",
]

# XCom keys threaded between tasks. The whole point: ONE workflow_run_id for the
# DAG run, plus the run_id/link_id of each upstream hop the next task consumes.
XCOM_WORKFLOW_RUN_ID = "ods_workflow_run_id"


# --------------------------------------------------------------------------- #
# workflow_run_id derivation. The FIRST task mints/derives it ONCE; downstream
# tasks read the SAME id from XCom. Deterministic from dag_id + dag_run_id so a
# retry of the whole run lands on the same logical workflow id.
# --------------------------------------------------------------------------- #
def derive_workflow_run_id(dag_id: str, dag_run_id: str) -> str:
    """Deterministic ODS workflow_run_id for one Airflow DAG run.

    UUID5 over (dag_id, dag_run_id) so the same DAG run always maps to the same
    ODS workflow id (idempotent re-runs), while different DAG runs differ.
    """
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"{dag_id}/{dag_run_id}"))


def _workflow_run_id_for(context: dict[str, Any]) -> str:
    dag = context.get("dag")
    dag_id = getattr(dag, "dag_id", None) or context.get("dag_id") or DAG_ID
    dag_run = context.get("dag_run")
    dag_run_id = (
        context.get("run_id")
        or getattr(dag_run, "run_id", None)
        or "manual"
    )
    return derive_workflow_run_id(dag_id, dag_run_id)


def _pull_workflow_run_id(context: dict[str, Any]) -> str:
    """Read the shared workflow_run_id pushed by ``ingest_policy`` via XCom.

    Falls back to deriving it (same deterministic function) if XCom is missing,
    so a partially-replayed run still binds to the same logical id.
    """
    ti = context.get("ti") or context.get("task_instance")
    pulled = None
    if ti is not None and hasattr(ti, "xcom_pull"):
        pulled = ti.xcom_pull(task_ids="ingest_policy", key=XCOM_WORKFLOW_RUN_ID)
    return pulled or _workflow_run_id_for(context)


def _business_date(context: dict[str, Any]) -> dt.date:
    """Use the DAG run's logical date as the ODS business_date.

    Defaults to the demo Day-2 date so a bare invocation is still well-formed.
    """
    logical = context.get("logical_date") or context.get("execution_date")
    if logical is not None:
        if isinstance(logical, dt.datetime):
            return logical.date()
        if isinstance(logical, dt.date):
            return logical
    return pcw.REFEED_BUSINESS_DATE


# --------------------------------------------------------------------------- #
# Per-task callables. Each opens its own connection and commits per task. Each
# passes airflow_orchestrator_context(context) as orchestrator= into runs.start
# (the harness hop helpers accept the orchestrator context indirectly via their
# own _orchestrator builder; here we drive them through the same business
# functions but stamp identity from the live Airflow context).
#
# To keep the harness UNCHANGED while still stamping the *live* Airflow context,
# each callable temporarily binds the harness ``airflow_orchestrator_context``
# output by monkeypatching the harness ``_orchestrator`` builder for the scope
# of the task. This keeps a SINGLE source of truth for the orchestrator dict
# shape (the spec helper) and avoids forking the harness.
# --------------------------------------------------------------------------- #
class _OrchestratorBinding:
    """Bind the harness ``_orchestrator`` to the live Airflow context for one task.

    The harness hop helpers call ``pcw._orchestrator(dag_run_id=..., task_id=...,
    ...)`` internally. For the DAG we want the dict that
    ``airflow_orchestrator_context(context)`` produces (live try_number, log_url,
    map_index). This context-manager swaps in a builder that returns that live
    dict, then restores the original so harness tests are untouched.
    """

    def __init__(self, context: dict[str, Any]):
        self._context = context
        self._original = None

    def __enter__(self):
        live = airflow_orchestrator_context(self._context)
        self._original = pcw._orchestrator

        def _live(*, dag_run_id, task_id, business_date, execution_type,
                  try_number: int = 1):
            merged = dict(live)
            # keep the harness-provided per-hop task_id / execution_type context
            merged["task_id"] = task_id or merged.get("task_id")
            payload = dict(merged.get("payload") or {})
            payload.setdefault("execution_date", f"{business_date}T00:00:00+00:00")
            payload["execution_type"] = execution_type
            merged["payload"] = payload
            return merged

        pcw._orchestrator = _live  # type: ignore[assignment]
        return self

    def __exit__(self, *exc):
        pcw._orchestrator = self._original  # type: ignore[assignment]
        return False


def ingest_policy(**context: Any) -> str:
    """First task: mint/derive the shared workflow_run_id, push it, ingest policy."""
    workflow_run_id = _workflow_run_id_for(context)
    business_date = _business_date(context)
    ti = context.get("ti") or context.get("task_instance")
    if ti is not None and hasattr(ti, "xcom_push"):
        ti.xcom_push(key=XCOM_WORKFLOW_RUN_ID, value=workflow_run_id)
    policy_file = pcw._file(pcw.POLICY_DATASET, business_date, pcw.POLICY_ROWS)
    with connect() as conn, _OrchestratorBinding(context):
        result = pcw._ingest(
            conn, workflow_run_id=workflow_run_id, file=policy_file,
            dag_run_id=_dag_run_id(context), task_id="ingest_policy",
            execution_type="normal", commit=True)
        conn.commit()
    return result["run_id"]


def ingest_claim(**context: Any) -> str:
    """Parallel branch: ingest the claim raw file under the SAME workflow_run_id."""
    workflow_run_id = _pull_workflow_run_id(context)
    business_date = _business_date(context)
    claim_file = pcw._file(pcw.CLAIM_DATASET, business_date, pcw.CLAIM_ROWS)
    with connect() as conn, _OrchestratorBinding(context):
        result = pcw._ingest(
            conn, workflow_run_id=workflow_run_id, file=claim_file,
            dag_run_id=_dag_run_id(context), task_id="ingest_claim",
            execution_type="normal", commit=True)
        conn.commit()
    return result["run_id"]


def canonicalize_policy(**context: Any) -> str:
    """Canonicalize policy -> silver, binding to this run's policy ingest."""
    workflow_run_id = _pull_workflow_run_id(context)
    business_date = _business_date(context)
    policy_file = pcw._file(pcw.POLICY_DATASET, business_date, pcw.POLICY_ROWS)
    ingest = _latest_hop(workflow_run_id, "ingestion", pcw.POLICY_DATASET)
    with connect() as conn, _OrchestratorBinding(context):
        result = pcw._canonicalize_to_silver(
            conn, workflow_run_id=workflow_run_id, file=policy_file,
            ingest_run_id=ingest["run_id"], ingest_link_id=ingest["link_id"],
            dag_run_id=_dag_run_id(context), task_id="canonicalize_policy",
            execution_type="normal", commit=True)
        conn.commit()
    return result["run_id"]


def canonicalize_claim(**context: Any) -> str:
    """Canonicalize claim -> silver, binding to this run's claim ingest."""
    workflow_run_id = _pull_workflow_run_id(context)
    business_date = _business_date(context)
    claim_file = pcw._file(pcw.CLAIM_DATASET, business_date, pcw.CLAIM_ROWS)
    ingest = _latest_hop(workflow_run_id, "ingestion", pcw.CLAIM_DATASET)
    with connect() as conn, _OrchestratorBinding(context):
        result = pcw._canonicalize_to_silver(
            conn, workflow_run_id=workflow_run_id, file=claim_file,
            ingest_run_id=ingest["run_id"], ingest_link_id=ingest["link_id"],
            dag_run_id=_dag_run_id(context), task_id="canonicalize_claim",
            execution_type="normal", commit=True)
        conn.commit()
    return result["run_id"]


def merge_policy_claim(**context: Any) -> str:
    """Join both silver outputs into the policy_claim detail output."""
    workflow_run_id = _pull_workflow_run_id(context)
    business_date = _business_date(context)
    policy_silver = _latest_hop(workflow_run_id, "canonicalization", pcw.POLICY_DATASET)
    claim_silver = _latest_hop(workflow_run_id, "canonicalization", pcw.CLAIM_DATASET)
    detail_rows = pcw._merge_rows(business_date, pcw.CLAIM_ROWS)
    with connect() as conn, _OrchestratorBinding(context):
        result = pcw._merge_to_detail(
            conn, workflow_run_id=workflow_run_id, business_date=business_date,
            policy_silver=policy_silver, claim_silver=claim_silver,
            detail_rows=detail_rows, content_tag=f"{workflow_run_id}-orig",
            dag_run_id=_dag_run_id(context), execution_type="normal", commit=True)
        conn.commit()
    return result["run_id"]


def sink_policy_claim(**context: Any) -> str:
    """Write the detail rows to ods.policy_claim and activate visibility."""
    workflow_run_id = _pull_workflow_run_id(context)
    business_date = _business_date(context)
    merge = _latest_hop(workflow_run_id, "merge", pcw.DETAIL_DATASET)
    detail_rows = pcw._merge_rows(business_date, pcw.CLAIM_ROWS)
    with connect() as conn, _OrchestratorBinding(context):
        detail_sink = pcw._sink_rows(
            conn, workflow_run_id=workflow_run_id, business_date=business_date,
            dataset=pcw.DETAIL_DATASET, upstream_run_id=merge["run_id"],
            upstream_link_id=merge["link_id"],
            upstream_edge_type="merge_to_canonical", rows=detail_rows,
            content_tag=f"{workflow_run_id}-orig",
            stage_name="upsert_policy_claim", task_id="sink_policy_claim",
            dag_run_id=_dag_run_id(context), execution_type="normal", commit=True)
        pcw._activate_business_keys(
            conn, dataset=pcw.DETAIL_DATASET, target_name=pcw.DETAIL_TARGET,
            business_date=business_date, sink=detail_sink, file_id=None,
            workflow_run_id=workflow_run_id, rows=detail_rows,
            key_fn=pcw.detail_business_key, reason="normal load", commit=True)
        conn.commit()
    return detail_sink["run_id"]


def aggregate_policy_claim_daily(**context: Any) -> str:
    """Aggregate the detail sink output into policy_claim_daily."""
    workflow_run_id = _pull_workflow_run_id(context)
    business_date = _business_date(context)
    detail_sink = _latest_hop(workflow_run_id, "sink", pcw.DETAIL_DATASET)
    detail_rows = pcw._merge_rows(business_date, pcw.CLAIM_ROWS)
    aggregate_rows = pcw._aggregate_rows(detail_rows)
    with connect() as conn, _OrchestratorBinding(context):
        result = pcw._aggregate_from_detail(
            conn, workflow_run_id=workflow_run_id, business_date=business_date,
            detail_sink=detail_sink, detail_rows=detail_rows,
            aggregate_rows=aggregate_rows, content_tag=f"{workflow_run_id}-orig",
            dag_run_id=_dag_run_id(context), execution_type="normal", commit=True)
        conn.commit()
    return result["run_id"]


def sink_policy_claim_daily(**context: Any) -> str:
    """Write the aggregate rows to ods.policy_claim_daily and activate visibility."""
    workflow_run_id = _pull_workflow_run_id(context)
    business_date = _business_date(context)
    aggregate = _latest_hop(workflow_run_id, "aggregation", pcw.AGG_DATASET)
    detail_rows = pcw._merge_rows(business_date, pcw.CLAIM_ROWS)
    aggregate_rows = pcw._aggregate_rows(detail_rows)
    with connect() as conn, _OrchestratorBinding(context):
        aggregate_sink = pcw._sink_rows(
            conn, workflow_run_id=workflow_run_id, business_date=business_date,
            dataset=pcw.AGG_DATASET, upstream_run_id=aggregate["run_id"],
            upstream_link_id=aggregate["link_id"],
            upstream_edge_type="detail_to_aggregate", rows=aggregate_rows,
            content_tag=f"{workflow_run_id}-orig",
            stage_name="upsert_policy_claim_daily",
            task_id="sink_policy_claim_daily",
            dag_run_id=_dag_run_id(context), execution_type="normal", commit=True)
        pcw._activate_business_keys(
            conn, dataset=pcw.AGG_DATASET, target_name=pcw.AGG_TARGET,
            business_date=business_date, sink=aggregate_sink, file_id=None,
            workflow_run_id=workflow_run_id, rows=aggregate_rows,
            key_fn=pcw.aggregate_business_key, reason="normal load", commit=True)
        conn.commit()
    return aggregate_sink["run_id"]


# Ordered mapping consumed by build_dag and the import-safety test.
TASK_CALLABLES = {
    "ingest_policy": ingest_policy,
    "ingest_claim": ingest_claim,
    "canonicalize_policy": canonicalize_policy,
    "canonicalize_claim": canonicalize_claim,
    "merge_policy_claim": merge_policy_claim,
    "sink_policy_claim": sink_policy_claim,
    "aggregate_policy_claim_daily": aggregate_policy_claim_daily,
    "sink_policy_claim_daily": sink_policy_claim_daily,
}


def _dag_run_id(context: dict[str, Any]) -> str:
    dag_run = context.get("dag_run")
    return (
        context.get("run_id")
        or getattr(dag_run, "run_id", None)
        or "manual"
    )


def _latest_hop(workflow_run_id: str, pipeline_type: str, dataset: str) -> dict[str, str]:
    """Look up the run_id + its output_link_id for a completed hop in this workflow.

    Tasks run in separate processes so they cannot pass Python objects directly;
    XCom carries small scalars and the rest is recovered from the control plane.
    This reads the latest succeeded run for (workflow_run_id, pipeline_type,
    dataset) and its single produced output link.
    """
    with connect() as conn:
        row = conn.execute(
            """
            SELECT r.run_id, l.lineage_link_id
              FROM cp.run_log r
              JOIN cp.lineage_link l ON l.consumer_run_id = r.run_id
             WHERE r.workflow_run_id = %s
               AND r.pipeline_type = %s
               AND r.dataset = %s
               AND r.status = 'succeeded'
             ORDER BY r.finished_at DESC NULLS LAST, r.started_at DESC
             LIMIT 1
            """,
            (workflow_run_id, pipeline_type, dataset),
        ).fetchone()
    if row is None:
        raise RuntimeError(
            f"no succeeded {pipeline_type}/{dataset} hop for workflow {workflow_run_id}"
        )
    return {"run_id": str(row[0]), "link_id": str(row[1])}


# --------------------------------------------------------------------------- #
# DAG builder. Only constructs the DAG object when Airflow is importable.
# --------------------------------------------------------------------------- #
def build_dag():
    """Build and return the ``ods_policy_claims`` Airflow DAG.

    Returns ``None`` when Airflow is not installed so import stays safe.
    """
    if not AIRFLOW:
        return None

    with DAG(
        dag_id=DAG_ID,
        description="ODS insurance policy + claims workflow (parallel branches).",
        schedule="@daily",
        start_date=dt.datetime(2026, 5, 28),
        catchup=False,
        tags=["ods", "insurance", "policy", "claims"],
    ) as dag:
        ops = {
            task_id: PythonOperator(task_id=task_id, python_callable=callable_)
            for task_id, callable_ in TASK_CALLABLES.items()
        }
        # policy + claim ingest/canonicalize run as PARALLEL branches.
        ops["ingest_policy"] >> ops["canonicalize_policy"]
        ops["ingest_claim"] >> ops["canonicalize_claim"]
        (
            [ops["canonicalize_policy"], ops["canonicalize_claim"]]
            >> ops["merge_policy_claim"]
            >> ops["sink_policy_claim"]
            >> ops["aggregate_policy_claim_daily"]
            >> ops["sink_policy_claim_daily"]
        )
    return dag


# Module-level DAG object only when Airflow is present (Airflow scheduler picks
# it up by scanning module globals). Stays None / absent otherwise.
if AIRFLOW:  # pragma: no cover - only runs where Airflow is installed
    dag = build_dag()
