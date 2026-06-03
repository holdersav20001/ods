"""Insurance policy + claims Airflow-oriented DEMO workflow.

Mirrors ``harness/customer_transaction_workflow.py`` (multi-day + Day-2 refeed)
but is driven as an Airflow DAG would drive it (every run carries
``trigger_type='airflow'`` and a simulated Airflow orchestrator context), and it
EXERCISES the target-visibility / active-slice layer with CHANGED-ONLY,
business-key-grain supersession (Option A): on refeed only the CHANGED business
keys are re-activated; unchanged keys keep their original active row untouched.

Shape per NORMAL execution (8 runs, one shared workflow_run_id):

  policy raw file -> ingest policy -> policy silver
  claim raw file  -> ingest claim  -> claim silver
  policy silver + claim silver -> merge policy_claim -> ods.policy_claim (detail)
  detail output   -> aggregate     -> ods.policy_claim_daily (by business_date+policy_type)

The Day-2 CLAIM REFEED execution (6 runs, its own workflow_run_id) REUSES the
original Day-2 policy silver output and re-ingests a CORRECTED claim file:

  corrected raw claim -> ingest -> corrected claim silver
  ORIGINAL policy silver + corrected claim silver -> merge -> detail sink (CHANGED rows only)
  -> aggregate (CHANGED policy_type rows only) -> aggregate sink

Every control-plane write goes through the sanctioned client wrappers in
``control/``; read-only SELECTs for the snapshot/tests are fine. No raw ``cp.*``
inserts in the harness.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import pathlib
import uuid
from typing import Any

from control import lineage, recon, runs, stages, visibility
from control.db import connect
from harness.snapshot import export_workflow_snapshot


DOMAIN = "insurance"
POLICY_DATASET = "policy"
CLAIM_DATASET = "claim"
DETAIL_DATASET = "policy_claim"
AGG_DATASET = "policy_claim_daily"

SINK_TYPE = "postgres"
DETAIL_TARGET = f"ods.{DETAIL_DATASET}"
AGG_TARGET = f"ods.{AGG_DATASET}"

DAG_ID = "ods_policy_claims"

BUSINESS_DATES = [
    dt.date(2026, 5, 28),
    dt.date(2026, 5, 29),
    dt.date(2026, 5, 30),
]
REFEED_BUSINESS_DATE = dt.date(2026, 5, 29)

# Stable policy roster per the spec (§Input Data Model). Same policies each day.
# Two policy_types (auto, home) so the refeed can change ONE policy_type's claims
# and leave the other policy_type's aggregate business key UNCHANGED.
POLICY_ROWS = [
    {"policy_id": "P001", "customer_id": "C001", "policy_type": "auto",
     "effective_date": "2026-01-01", "expiry_date": "2026-12-31",
     "premium_amount": 1200.00},
    {"policy_id": "P002", "customer_id": "C002", "policy_type": "auto",
     "effective_date": "2026-01-01", "expiry_date": "2026-12-31",
     "premium_amount": 900.00},
    {"policy_id": "P003", "customer_id": "C003", "policy_type": "home",
     "effective_date": "2026-01-01", "expiry_date": "2026-12-31",
     "premium_amount": 1500.00},
]

# Original claim file per the spec (§Input Data Model). Claims span both
# policy_types: P001/P002 -> auto, P003 -> home.
CLAIM_ROWS = [
    {"claim_id": "CL100", "policy_id": "P001", "claim_date": "2026-05-20",
     "claim_status": "open", "claim_amount": 500.00},
    {"claim_id": "CL101", "policy_id": "P001", "claim_date": "2026-05-22",
     "claim_status": "closed", "claim_amount": 250.00},
    {"claim_id": "CL102", "policy_id": "P002", "claim_date": "2026-05-21",
     "claim_status": "open", "claim_amount": 300.00},
    {"claim_id": "CL103", "policy_id": "P003", "claim_date": "2026-05-19",
     "claim_status": "closed", "claim_amount": 750.00},
]

# Corrected Day-2 claim refeed (§"Three-Day Demo Data"). Same ids/structure.
# ONLY the AUTO claims change: CL100 amount corrected 500 -> 600, CL102 status
# corrected open -> closed. The HOME claim CL103 is IDENTICAL -> its business
# keys (detail "P003:CL103" and aggregate "2026-05-29:home") are UNCHANGED and
# must keep their original active visibility row. Distinct content => distinct
# file_md5 => distinct raw file identity.
CORRECTED_CLAIM_ROWS = [
    {"claim_id": "CL100", "policy_id": "P001", "claim_date": "2026-05-20",
     "claim_status": "open", "claim_amount": 600.00},
    {"claim_id": "CL101", "policy_id": "P001", "claim_date": "2026-05-22",
     "claim_status": "closed", "claim_amount": 250.00},
    {"claim_id": "CL102", "policy_id": "P002", "claim_date": "2026-05-21",
     "claim_status": "closed", "claim_amount": 300.00},
    {"claim_id": "CL103", "policy_id": "P003", "claim_date": "2026-05-19",
     "claim_status": "closed", "claim_amount": 750.00},
]


# --------------------------------------------------------------------------- #
# Business-key helpers (Option A: business_key replacement_scope).
# --------------------------------------------------------------------------- #
def detail_business_key(row: dict[str, Any]) -> str:
    """Per-row business key for the ods.policy_claim detail target."""
    return f"{row['policy_id']}:{row['claim_id']}"


def aggregate_business_key(row: dict[str, Any]) -> str:
    """Per-row business key for the ods.policy_claim_daily aggregate target."""
    return f"{row['business_date']}:{row['policy_type']}"


# --------------------------------------------------------------------------- #
# Airflow orchestrator context.
# --------------------------------------------------------------------------- #
def airflow_orchestrator_context(context: dict[str, Any]) -> dict[str, Any]:
    """Build the ODS orchestrator-identity dict from an Airflow task context.

    The real DAG passes Airflow's task ``context`` (``dag``, ``run_id``, ``ti``,
    ``task``); this maps it to the ``orchestrator=`` dict ``runs.start`` records
    (migration 020 columns). Defensive ``.get`` so a partial context still
    yields a usable dict. The harness builds an equivalent dict directly via
    ``_orchestrator``.
    """
    dag = context.get("dag")
    ti = context.get("task_instance") or context.get("ti")
    task = context.get("task")
    dag_id = getattr(dag, "dag_id", None) or context.get("dag_id") or DAG_ID
    run_id = context.get("run_id") or getattr(context.get("dag_run"), "run_id", None)
    task_id = getattr(task, "task_id", None) or getattr(ti, "task_id", None)
    try_number = getattr(ti, "try_number", 1)
    map_index = getattr(ti, "map_index", -1)
    return {
        "type": "airflow",
        "dag_id": dag_id,
        "run_id": run_id,
        "task_id": task_id,
        "try_number": try_number,
        "map_index": map_index,
        "url": context.get("log_url")
        or f"http://airflow/dags/{dag_id}/runs/{run_id}/tasks/{task_id}",
        "payload": {
            "execution_date": str(context.get("logical_date")
                                  or context.get("execution_date") or ""),
            "operator": getattr(task, "task_type", "PythonOperator"),
        },
    }


def _orchestrator(*, dag_run_id: str, task_id: str, business_date: dt.date,
                  execution_type: str, try_number: int = 1) -> dict[str, Any]:
    """The simulated Airflow context dict the harness records per run.

    Equivalent to what ``airflow_orchestrator_context`` would build under a real
    DAG run (same shape/keys), so the harness path is the tested path.
    """
    return {
        "type": "airflow",
        "dag_id": DAG_ID,
        "run_id": dag_run_id,
        "task_id": task_id,
        "try_number": try_number,
        "map_index": -1,
        "url": f"http://airflow/dags/{DAG_ID}/runs/{dag_run_id}/tasks/{task_id}",
        "payload": {
            "execution_date": f"{business_date}T00:00:00+00:00",
            "execution_type": execution_type,
        },
    }


def ensure_targets(conn) -> None:
    """Create the two demo target tables (same column shape as the customer demo).

    Ad-hoc demo tables — NOT a core migration. Each row carries the ODS stamps:
    payload + _ods_workflow_run_id + _ods_lineage_link_id (FK) + _ods_source_file_id
    + _ods_output_link_id (018 new-name mirror).
    """
    for table in (DETAIL_DATASET, AGG_DATASET):
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS ods.{table} (
                row_id BIGSERIAL PRIMARY KEY,
                payload JSONB NOT NULL,
                _ods_workflow_run_id TEXT,
                _ods_lineage_link_id UUID NOT NULL
                    REFERENCES cp.lineage_link(lineage_link_id),
                _ods_source_file_id UUID,
                _ods_output_link_id UUID
            )
            """
        )
        conn.execute(
            f"ALTER TABLE ods.{table} "
            f"ADD COLUMN IF NOT EXISTS _ods_source_file_id UUID"
        )
        conn.execute(
            f"ALTER TABLE ods.{table} "
            f"ADD COLUMN IF NOT EXISTS _ods_output_link_id UUID"
        )
        # FIX-A (F8) demo-table guard: mirror the ods.orders CHECK so the dual
        # _ods_* mirror columns stay consistent (rename shelved). A null on
        # either side is tolerated. DROP+ADD keeps it idempotent.
        conn.execute(
            f"ALTER TABLE ods.{table} "
            f"DROP CONSTRAINT IF EXISTS ods_{table}_link_mirror_consistent"
        )
        conn.execute(
            f"ALTER TABLE ods.{table} "
            f"ADD CONSTRAINT ods_{table}_link_mirror_consistent CHECK ("
            f"_ods_output_link_id IS NULL OR _ods_lineage_link_id IS NULL "
            f"OR _ods_output_link_id = _ods_lineage_link_id)"
        )


def reset_demo_state(conn) -> None:
    """Clear THIS domain's generated demo/control state before a fresh snapshot.

    DOMAIN-SCOPED (domain = ``insurance``): clears ONLY this demo's data so the
    customer/transaction (``sales``) demo can coexist live in the same database.
    Keeps static reference tables (cp.edge_type, cp.dataset_config) and uses no
    global ``TRUNCATE ... CASCADE``.

    Removes (scoped to this domain's run set / domain column): the two demo
    target tables' rows, this domain's target-visibility rows, and the control-
    plane reconciliation/DLQ/lineage-edge/lineage-link/run-stage/run/file rows
    for this domain. Deleted in FK-safe child->parent order. Re-running the demo
    yields the same single set (no duplicates).
    """
    ensure_targets(conn)

    # The two target tables belong entirely to this domain -> clear all rows.
    conn.execute(f"DELETE FROM ods.{DETAIL_DATASET}")
    conn.execute(f"DELETE FROM ods.{AGG_DATASET}")

    # This domain's target-visibility rows (changed-only supersession lives here).
    conn.execute("DELETE FROM ods.target_visibility WHERE domain = %s", (DOMAIN,))

    # Control-plane rows, FK-safe child->parent order, all scoped to this
    # domain's run set (run_log.domain = DOMAIN). The lineage_link/edge graph is
    # self-contained within one domain's runs (the refeed merge references the
    # original policy silver link -- same insurance domain).
    domain_runs = "SELECT run_id FROM cp.run_log WHERE domain = %s"
    conn.execute(
        f"DELETE FROM cp.reconciliation_log WHERE run_id IN ({domain_runs})",
        (DOMAIN,),
    )
    conn.execute(
        f"DELETE FROM cp.dlq WHERE run_id IN ({domain_runs})",
        (DOMAIN,),
    )
    conn.execute(
        f"""
        DELETE FROM cp.lineage_edge
        WHERE lineage_link_id IN (
            SELECT lineage_link_id FROM cp.lineage_link
            WHERE consumer_run_id IN ({domain_runs})
        )
        """,
        (DOMAIN,),
    )
    conn.execute(
        f"DELETE FROM cp.lineage_link WHERE consumer_run_id IN ({domain_runs})",
        (DOMAIN,),
    )
    conn.execute(
        f"DELETE FROM cp.run_stage_log WHERE run_id IN ({domain_runs})",
        (DOMAIN,),
    )
    conn.execute("DELETE FROM cp.run_log WHERE domain = %s", (DOMAIN,))
    conn.execute("DELETE FROM cp.file_catalogue WHERE domain = %s", (DOMAIN,))


def _content_md5(rows: list[dict[str, Any]]) -> str:
    blob = json.dumps(rows, sort_keys=True, separators=(",", ":"))
    return hashlib.md5(blob.encode("utf-8")).hexdigest()


def _file(dataset: str, business_date: dt.date, rows: list[dict[str, Any]], *,
          raw_suffix: str | None = None) -> dict[str, Any]:
    leaf = raw_suffix if raw_suffix is not None else str(business_date)
    return {
        "s3_raw_path": f"s3://raw/{DOMAIN}/{dataset}/{leaf}.json",
        "file_md5": _content_md5(rows),
        "business_date": business_date,
        "domain": DOMAIN,
        "dataset": dataset,
        "record_count": len(rows),
        "rows": rows,
    }


# --------------------------------------------------------------------------- #
# Per-hop helpers. Each returns the run/link/file ids the next hop needs.
# --------------------------------------------------------------------------- #
def _ingest(conn, *, workflow_run_id: str, file: dict[str, Any],
            dag_run_id: str, task_id: str, execution_type: str,
            replay_of_run_id: str | None = None, commit: bool) -> dict[str, str]:
    """Register the raw file and run the ingestion hop -> raw_to_curated link."""
    rows = file["rows"]
    n = file["record_count"]
    file_id = runs.register_file(
        conn,
        s3_raw_path=file["s3_raw_path"],
        file_md5=file["file_md5"],
        business_date=file["business_date"],
        domain=file["domain"],
        dataset=file["dataset"],
        commit=commit,
    )
    run_id = runs.start(
        conn,
        workflow_run_id=workflow_run_id,
        pipeline_type="ingestion",
        domain=file["domain"],
        dataset=file["dataset"],
        business_date=file["business_date"],
        trigger_type="airflow",
        file_id=file_id,
        replay_of_run_id=replay_of_run_id,
        orchestrator=_orchestrator(
            dag_run_id=dag_run_id, task_id=task_id,
            business_date=file["business_date"], execution_type=execution_type),
        commit=commit,
    )
    with stages.stage_scope(conn, run_id, "ingest_raw", commit=commit) as st:
        st.record_in = n
        st.record_out = n
        st.metrics = {"sample_ids": [r.get("claim_id") or r.get("policy_id")
                                     for r in rows]}

    link_id = lineage.write_output_link(
        conn,
        consumer_run_id=run_id,
        edge_type="raw_to_curated",
        target_ref={
            "path": f"s3://bronze/{file['dataset']}/{file['business_date']}.json",
            "content_hash": f"bronze-{file['file_md5']}",
            "version": 1,
        },
        record_count=n,
        inputs=[{
            "source_file_id": file_id,
            "edge_type": "raw_to_curated",
            "source_ref": {"path": file["s3_raw_path"]},
            "record_count": n,
        }],
        transform_version="raw-v1",
        commit=commit,
    )
    recon.write_check(
        conn, run_id=run_id, check_type="ingest",
        source_count=n, accounted_count=n, commit=commit)
    runs.finalise(conn, run_id, status="succeeded", record_count_out=n,
                  commit=commit)
    return {"run_id": run_id, "file_id": file_id, "link_id": link_id}


def _canonicalize_to_silver(conn, *, workflow_run_id: str, file: dict[str, Any],
                            ingest_run_id: str, ingest_link_id: str,
                            dag_run_id: str, task_id: str, execution_type: str,
                            commit: bool) -> dict[str, str]:
    """Transform a raw file's curated output into a silver canonical output.

    Upstream is passed EXPLICITLY (the ingest run/link of THIS execution) so the
    refeed binds to its own corrected ingest and a normal day to its own.
    """
    n = file["record_count"]
    run_id = runs.start(
        conn,
        workflow_run_id=workflow_run_id,
        pipeline_type="canonicalization",
        domain=file["domain"],
        dataset=file["dataset"],
        business_date=file["business_date"],
        trigger_type="airflow",
        orchestrator=_orchestrator(
            dag_run_id=dag_run_id, task_id=task_id,
            business_date=file["business_date"], execution_type=execution_type),
        commit=commit,
    )
    with stages.stage_scope(conn, run_id, "canonicalize_to_silver",
                            commit=commit) as st:
        st.record_in = n
        st.record_out = n

    link_id = lineage.write_output_link(
        conn,
        consumer_run_id=run_id,
        edge_type="curated_to_canonical",
        target_ref={
            "path": f"s3://silver/{file['dataset']}/{file['business_date']}.parquet",
            # content_hash includes the md5 so the corrected claim silver has a
            # DIFFERENT target_ref.content_hash than the original.
            "content_hash": f"silver-{file['file_md5']}",
            "version": 1,
        },
        record_count=n,
        inputs=[{
            "upstream_run_id": ingest_run_id,
            "upstream_output_link_id": ingest_link_id,
            "edge_type": "curated_to_canonical",
            "source_ref": {"layer": "bronze", "dataset": file["dataset"]},
            "record_count": n,
        }],
        transform_version="silver-v1",
        commit=commit,
    )
    recon.write_check(
        conn, run_id=run_id, check_type="canonicalize_to_silver",
        source_count=n, accounted_count=n, commit=commit)
    runs.finalise(conn, run_id, status="succeeded", record_count_out=n,
                  commit=commit)
    return {"run_id": run_id, "link_id": link_id}


def _merge_rows(business_date: dt.date,
                claim_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Join claims to their policy on policy_id -> the merged detail rows."""
    policies = {row["policy_id"]: row for row in POLICY_ROWS}
    merged = []
    for claim in claim_rows:
        policy = policies[claim["policy_id"]]
        merged.append({
            "business_date": str(business_date),
            "policy_id": policy["policy_id"],
            "customer_id": policy["customer_id"],
            "policy_type": policy["policy_type"],
            "premium_amount": policy["premium_amount"],
            "claim_id": claim["claim_id"],
            "claim_date": claim["claim_date"],
            "claim_status": claim["claim_status"],
            "claim_amount": claim["claim_amount"],
        })
    return merged


def _aggregate_rows(detail_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Aggregate the detail by business_date + policy_type."""
    by_type: dict[tuple[str, str], dict[str, Any]] = {}
    for row in detail_rows:
        key = (row["business_date"], row["policy_type"])
        bucket = by_type.setdefault(key, {
            "business_date": row["business_date"],
            "policy_type": row["policy_type"],
            "claim_count": 0,
            "total_claim_amount": 0.0,
            "open_claim_count": 0,
            "closed_claim_count": 0,
        })
        bucket["claim_count"] += 1
        bucket["total_claim_amount"] += float(row["claim_amount"])
        if row["claim_status"] == "open":
            bucket["open_claim_count"] += 1
        elif row["claim_status"] == "closed":
            bucket["closed_claim_count"] += 1
    return [
        {**row, "total_claim_amount": round(row["total_claim_amount"], 2)}
        for row in by_type.values()
    ]


def _changed_rows(original_rows: list[dict[str, Any]],
                  new_rows: list[dict[str, Any]],
                  key_fields: tuple[str, ...]) -> list[dict[str, Any]]:
    """Rows whose business key is new or whose payload actually changed."""
    original_by_key = {
        tuple(str(row[field]) for field in key_fields): row
        for row in original_rows
    }
    changed = []
    for row in new_rows:
        key = tuple(str(row[field]) for field in key_fields)
        if original_by_key.get(key) != row:
            changed.append(row)
    return changed


def _merge_to_detail(conn, *, workflow_run_id: str, business_date: dt.date,
                     policy_silver: dict[str, str],
                     claim_silver: dict[str, str],
                     detail_rows: list[dict[str, Any]], content_tag: str,
                     dag_run_id: str, execution_type: str,
                     commit: bool) -> dict[str, str]:
    """Join policy + claim silver. TWO upstream merge_to_canonical edges:
    policy silver in slot 0 (input_role policy), claim silver in slot 1
    (input_role claim). Each edge names the EXACT upstream output via
    upstream_output_link_id (provenance to each silver).
    """
    run_id = runs.start(
        conn,
        workflow_run_id=workflow_run_id,
        pipeline_type="merge",
        domain=DOMAIN,
        dataset=DETAIL_DATASET,
        business_date=business_date,
        trigger_type="airflow",
        orchestrator=_orchestrator(
            dag_run_id=dag_run_id, task_id="merge_policy_claim",
            business_date=business_date, execution_type=execution_type),
        commit=commit,
    )
    with stages.stage_scope(conn, run_id, "merge_policy_claim",
                            commit=commit) as st:
        st.record_in = len(POLICY_ROWS) + len(detail_rows)
        st.record_out = len(detail_rows)
        st.metrics = {
            "inputs": [POLICY_DATASET, CLAIM_DATASET],
            "join_key": "policy_id",
        }

    link_id = lineage.write_output_link(
        conn,
        consumer_run_id=run_id,
        edge_type="merge_to_canonical",
        target_ref={
            "path": f"s3://silver/{DETAIL_DATASET}/{business_date}.parquet",
            "content_hash": f"silver-{DETAIL_DATASET}-{content_tag}",
            "version": 1,
        },
        record_count=len(detail_rows),
        inputs=[
            {
                "upstream_run_id": policy_silver["run_id"],
                "upstream_output_link_id": policy_silver["link_id"],
                "input_slot": 0,
                "edge_type": "merge_to_canonical",
                "source_ref": {"input_role": "policy", "dataset": POLICY_DATASET},
                "record_count": len(POLICY_ROWS),
            },
            {
                "upstream_run_id": claim_silver["run_id"],
                "upstream_output_link_id": claim_silver["link_id"],
                "input_slot": 1,
                "edge_type": "merge_to_canonical",
                "source_ref": {"input_role": "claim", "dataset": CLAIM_DATASET},
                "record_count": len(detail_rows),
            },
        ],
        transform_version="join-v1",
        commit=commit,
    )
    recon.write_check(
        conn, run_id=run_id, check_type="merge_policy_claim",
        source_count=len(detail_rows), accounted_count=len(detail_rows),
        metrics={"source_records_read": len(POLICY_ROWS) + len(detail_rows)},
        commit=commit)
    runs.finalise(conn, run_id, status="succeeded",
                  record_count_out=len(detail_rows), commit=commit)
    return {"run_id": run_id, "link_id": link_id}


def _sink_rows(conn, *, workflow_run_id: str, business_date: dt.date, dataset: str,
               upstream_run_id: str, upstream_link_id: str,
               upstream_edge_type: str, rows: list[dict[str, Any]],
               content_tag: str, stage_name: str, task_id: str,
               dag_run_id: str, execution_type: str,
               commit: bool) -> dict[str, str]:
    """Upsert rows to ods.<dataset> via the sanctioned output-then-rows path.

    write_output_then_rows stamps each target row with _ods_workflow_run_id,
    _ods_lineage_link_id and (where present) _ods_output_link_id. The
    canonical_to_sink link's content_hash carries content_tag so a corrected
    sink link is a DISTINCT output from the original.
    """
    run_id = runs.start(
        conn,
        workflow_run_id=workflow_run_id,
        pipeline_type="sink",
        domain=DOMAIN,
        dataset=dataset,
        business_date=business_date,
        trigger_type="airflow",
        orchestrator=_orchestrator(
            dag_run_id=dag_run_id, task_id=task_id,
            business_date=business_date, execution_type=execution_type),
        commit=commit,
    )
    with stages.stage_scope(conn, run_id, stage_name, commit=commit) as st:
        st.record_in = len(rows)
        st.record_out = len(rows)

    link_id = lineage.write_output_then_rows(
        conn,
        consumer_run_id=run_id,
        edge_type="canonical_to_sink",
        target_ref={
            "path": f"postgres://ods/{dataset}",
            "content_hash": f"postgres-{dataset}-{content_tag}",
            "version": 1,
        },
        record_count=len(rows),
        inputs=[{
            "upstream_run_id": upstream_run_id,
            "upstream_output_link_id": upstream_link_id,
            "edge_type": "canonical_to_sink",
            "source_ref": {"upstream_edge_type": upstream_edge_type},
            "record_count": len(rows),
        }],
        rows=rows,
        sink_type=SINK_TYPE,
        transform_version="postgres-upsert-v1",
        commit=commit,
    )
    # Per-OUTPUT graph-derived recon (P10-C): accounted = rows stamped with THIS
    # link only. This is the sink_link recon row that gates visibility.activate.
    recon.reconcile_sink_link(conn, lineage_link_id=link_id,
                              source_count=len(rows), commit=commit)
    runs.finalise(conn, run_id, status="succeeded",
                  record_count_out=len(rows), commit=commit)
    return {"run_id": run_id, "link_id": link_id}


def _aggregate_from_detail(conn, *, workflow_run_id: str, business_date: dt.date,
                           detail_inputs: list[dict[str, Any]],
                           aggregate_rows: list[dict[str, Any]],
                           content_tag: str, dag_run_id: str,
                           execution_type: str, commit: bool) -> dict[str, str]:
    """Aggregate detail SINK output(s) as a first-class detail_to_aggregate run.

    ``detail_inputs`` is one entry per CONTRIBUTING detail output, each
    ``{"run_id", "link_id", "record_count", optional "role"}`` -> one
    detail_to_aggregate input edge. A NORMAL aggregate passes its single detail
    sink. A recomputed REFEED aggregate consumes EVERY contributing active detail
    output (the ORIGINAL normal detail sink for the unchanged contributing rows
    of the affected key PLUS the corrected refeed detail sink), so the
    aggregate's provenance is COMPLETE rather than only the corrected slice
    (mirrors policy_claims_dlq_workflow._aggregate_from_detail / F1).
    """
    rows_in = sum(int(di["record_count"]) for di in detail_inputs)
    run_id = runs.start(
        conn,
        workflow_run_id=workflow_run_id,
        pipeline_type="aggregation",
        domain=DOMAIN,
        dataset=AGG_DATASET,
        business_date=business_date,
        trigger_type="airflow",
        orchestrator=_orchestrator(
            dag_run_id=dag_run_id, task_id="aggregate_policy_claim_daily",
            business_date=business_date, execution_type=execution_type),
        commit=commit,
    )
    with stages.stage_scope(conn, run_id, "aggregate_policy_claim_daily",
                            commit=commit) as st:
        st.record_in = rows_in
        st.record_out = len(aggregate_rows)
        st.metrics = {"group_by": ["business_date", "policy_type"],
                      "detail_inputs": len(detail_inputs)}

    link_id = lineage.write_output_link(
        conn,
        consumer_run_id=run_id,
        edge_type="detail_to_aggregate",
        target_ref={
            "path": f"s3://gold/{AGG_DATASET}/{business_date}.parquet",
            "content_hash": f"gold-{AGG_DATASET}-{content_tag}",
            "version": 1,
        },
        record_count=len(aggregate_rows),
        inputs=[{
            "upstream_run_id": di["run_id"],
            "upstream_output_link_id": di["link_id"],
            "input_slot": i,
            "edge_type": "detail_to_aggregate",
            "source_ref": {"input_role": di.get("role", "detail"),
                           "table": DETAIL_TARGET},
            "record_count": int(di["record_count"]),
        } for i, di in enumerate(detail_inputs)],
        transform_version="agg-v1",
        commit=commit,
    )
    recon.write_check(
        conn, run_id=run_id, check_type="aggregate_policy_claim_daily",
        source_count=rows_in, accounted_count=rows_in,
        metrics={"aggregate_rows": len(aggregate_rows)}, commit=commit)
    runs.finalise(conn, run_id, status="succeeded",
                  record_count_out=len(aggregate_rows), commit=commit)
    return {"run_id": run_id, "link_id": link_id}


def _activate_business_keys(conn, *, dataset: str, target_name: str,
                            business_date: dt.date, sink: dict[str, str],
                            file_id: str | None, workflow_run_id: str,
                            rows: list[dict[str, Any]], key_fn, reason: str | None,
                            commit: bool) -> list[str]:
    """Activate ONE target-visibility row per business key produced by this sink.

    Each call to cp.activate_target_visibility supersedes (status N) only the
    prior active row for that SAME (domain, dataset, business_date, sink_type,
    target_name, replacement_scope='business_key', replacement_key) and inserts
    the new Y. Because the function keys idempotency/deactivation on the full
    replacement scope/key (NOT output_link_id alone), the SAME refeed sink link
    can carry MANY per-key Y rows — exactly the changed-only behaviour. Unchanged
    keys are simply never passed here, so their original Y is untouched.
    """
    visibility_ids = []
    for row in rows:
        vis_id = visibility.activate(
            conn,
            domain=DOMAIN,
            dataset=dataset,
            business_date=business_date,
            sink_type=SINK_TYPE,
            target_name=target_name,
            file_id=file_id,
            output_link_id=sink["link_id"],
            producer_run_id=sink["run_id"],
            workflow_run_id=workflow_run_id,
            replacement_scope="business_key",
            replacement_key=key_fn(row),
            reason=reason,
            commit=commit,
        )
        visibility_ids.append(vis_id)
    return visibility_ids


# --------------------------------------------------------------------------- #
# Executions.
# --------------------------------------------------------------------------- #
def normal_execution(conn, business_date: dt.date,
                     policy_rows: list[dict[str, Any]],
                     claim_rows: list[dict[str, Any]], *,
                     workflow_run_id: str, dag_run_id: str,
                     commit: bool) -> dict[str, Any]:
    """Full 8-run normal shape for one (business_date, policy, claim).

    After each successful sink + recon-ok, activate ONE visibility row per
    business key produced (status Y).
    """
    policy_file = _file(POLICY_DATASET, business_date, policy_rows)
    claim_file = _file(CLAIM_DATASET, business_date, claim_rows)

    policy_ingest = _ingest(
        conn, workflow_run_id=workflow_run_id, file=policy_file,
        dag_run_id=dag_run_id, task_id="ingest_policy",
        execution_type="normal", commit=commit)
    claim_ingest = _ingest(
        conn, workflow_run_id=workflow_run_id, file=claim_file,
        dag_run_id=dag_run_id, task_id="ingest_claim",
        execution_type="normal", commit=commit)
    policy_silver = _canonicalize_to_silver(
        conn, workflow_run_id=workflow_run_id, file=policy_file,
        ingest_run_id=policy_ingest["run_id"],
        ingest_link_id=policy_ingest["link_id"],
        dag_run_id=dag_run_id, task_id="canonicalize_policy",
        execution_type="normal", commit=commit)
    claim_silver = _canonicalize_to_silver(
        conn, workflow_run_id=workflow_run_id, file=claim_file,
        ingest_run_id=claim_ingest["run_id"],
        ingest_link_id=claim_ingest["link_id"],
        dag_run_id=dag_run_id, task_id="canonicalize_claim",
        execution_type="normal", commit=commit)

    detail_rows = _merge_rows(business_date, claim_rows)
    content_tag = f"{workflow_run_id}-orig"
    merge = _merge_to_detail(
        conn, workflow_run_id=workflow_run_id, business_date=business_date,
        policy_silver=policy_silver, claim_silver=claim_silver,
        detail_rows=detail_rows, content_tag=content_tag,
        dag_run_id=dag_run_id, execution_type="normal", commit=commit)
    detail_sink = _sink_rows(
        conn, workflow_run_id=workflow_run_id, business_date=business_date,
        dataset=DETAIL_DATASET, upstream_run_id=merge["run_id"],
        upstream_link_id=merge["link_id"], upstream_edge_type="merge_to_canonical",
        rows=detail_rows, content_tag=content_tag,
        stage_name="upsert_policy_claim", task_id="sink_policy_claim",
        dag_run_id=dag_run_id, execution_type="normal", commit=commit)
    _activate_business_keys(
        conn, dataset=DETAIL_DATASET, target_name=DETAIL_TARGET,
        business_date=business_date, sink=detail_sink, file_id=None,
        workflow_run_id=workflow_run_id, rows=detail_rows,
        key_fn=detail_business_key, reason="normal load", commit=commit)

    aggregate_rows = _aggregate_rows(detail_rows)
    aggregate = _aggregate_from_detail(
        conn, workflow_run_id=workflow_run_id, business_date=business_date,
        detail_inputs=[{"run_id": detail_sink["run_id"],
                        "link_id": detail_sink["link_id"],
                        "record_count": len(detail_rows)}],
        aggregate_rows=aggregate_rows, content_tag=content_tag,
        dag_run_id=dag_run_id, execution_type="normal", commit=commit)
    aggregate_sink = _sink_rows(
        conn, workflow_run_id=workflow_run_id, business_date=business_date,
        dataset=AGG_DATASET, upstream_run_id=aggregate["run_id"],
        upstream_link_id=aggregate["link_id"],
        upstream_edge_type="detail_to_aggregate", rows=aggregate_rows,
        content_tag=content_tag, stage_name="upsert_policy_claim_daily",
        task_id="sink_policy_claim_daily", dag_run_id=dag_run_id,
        execution_type="normal", commit=commit)
    _activate_business_keys(
        conn, dataset=AGG_DATASET, target_name=AGG_TARGET,
        business_date=business_date, sink=aggregate_sink, file_id=None,
        workflow_run_id=workflow_run_id, rows=aggregate_rows,
        key_fn=aggregate_business_key, reason="normal load", commit=commit)

    # F6 (fact-spine, migration 031): cross-hop reconciliation on the FACT SPINE.
    # This is a star schema (policy DIMENSION joined to the claim FACT, then a
    # row-REDUCING daily aggregate), so the only universal invariant is the fact
    # spine: raw_in counts ONLY the 'claim' FACT raw_to_curated link (the 'policy'
    # DIMENSION is OFF-spine, excluded); sink_out counts ONLY the 'policy_claim'
    # leaf-detail canonical_to_sink rows (the daily aggregate is OFF-spine,
    # verified per-hop by reconcile_sink_link). So a normal day reconciles ok:
    # fact 4 == leaf-detail 4 + dlq 0. Recorded as check_type='workflow'.
    recon.reconcile_workflow(
        conn, workflow_run_id=workflow_run_id,
        source_datasets=[CLAIM_DATASET], leaf_target=DETAIL_DATASET,
        commit=commit)

    return {
        "workflow_run_id": workflow_run_id,
        "dag_run_id": dag_run_id,
        "business_date": business_date,
        "execution_type": "normal",
        "files": {"policy": policy_file, "claim": claim_file},
        "policy_ingest": policy_ingest,
        "claim_ingest": claim_ingest,
        "policy_silver": policy_silver,
        "claim_silver": claim_silver,
        "merge": merge,
        "detail_sink": detail_sink,
        "aggregate": aggregate,
        "aggregate_sink": aggregate_sink,
        "detail_rows": detail_rows,
        "aggregate_rows": aggregate_rows,
    }


def refeed_execution(conn, *, original_day2_result: dict[str, Any],
                     corrected_claim_rows: list[dict[str, Any]],
                     workflow_run_id: str, dag_run_id: str,
                     commit: bool) -> dict[str, Any]:
    """Day-2 corrected-CLAIM refeed (6 runs, NEW workflow_run_id).

    REUSE the ORIGINAL Day-2 policy silver output (do NOT re-run policy).
    Re-ingest the corrected claim (new file_id on the new md5;
    replay_of_run_id = original Day-2 claim ingest), re-canonicalize claim
    silver (different content_hash), merge with the ORIGINAL policy silver +
    CORRECTED claim silver, sink ONLY the CHANGED detail rows, aggregate, sink
    ONLY the CHANGED aggregate rows. Then activate visibility ONLY for the
    CHANGED business keys (changed-only supersession).
    """
    business_date = original_day2_result["business_date"]
    corrected_file = _file(
        CLAIM_DATASET, business_date, corrected_claim_rows,
        raw_suffix=f"{business_date}-refeed")

    claim_ingest = _ingest(
        conn, workflow_run_id=workflow_run_id, file=corrected_file,
        dag_run_id=dag_run_id, task_id="ingest_claim",
        execution_type="refeed",
        replay_of_run_id=original_day2_result["claim_ingest"]["run_id"],
        commit=commit)
    claim_silver = _canonicalize_to_silver(
        conn, workflow_run_id=workflow_run_id, file=corrected_file,
        ingest_run_id=claim_ingest["run_id"],
        ingest_link_id=claim_ingest["link_id"],
        dag_run_id=dag_run_id, task_id="canonicalize_claim",
        execution_type="refeed", commit=commit)

    # REUSE original Day-2 policy silver run + link (no re-run of policy).
    policy_silver = original_day2_result["policy_silver"]

    detail_rows = _merge_rows(business_date, corrected_claim_rows)
    changed_detail_rows = _changed_rows(
        original_day2_result["detail_rows"], detail_rows,
        ("policy_id", "claim_id"))
    content_tag = f"{workflow_run_id}-corrected"
    merge = _merge_to_detail(
        conn, workflow_run_id=workflow_run_id, business_date=business_date,
        policy_silver=policy_silver, claim_silver=claim_silver,
        detail_rows=detail_rows, content_tag=content_tag,
        dag_run_id=dag_run_id, execution_type="refeed", commit=commit)
    detail_sink = _sink_rows(
        conn, workflow_run_id=workflow_run_id, business_date=business_date,
        dataset=DETAIL_DATASET, upstream_run_id=merge["run_id"],
        upstream_link_id=merge["link_id"], upstream_edge_type="merge_to_canonical",
        rows=changed_detail_rows, content_tag=content_tag,
        stage_name="upsert_policy_claim", task_id="sink_policy_claim",
        dag_run_id=dag_run_id, execution_type="refeed", commit=commit)
    # CHANGED-ONLY visibility: supersede only the changed detail business keys.
    _activate_business_keys(
        conn, dataset=DETAIL_DATASET, target_name=DETAIL_TARGET,
        business_date=business_date, sink=detail_sink, file_id=None,
        workflow_run_id=workflow_run_id, rows=changed_detail_rows,
        key_fn=detail_business_key, reason="claim refeed (changed only)",
        commit=commit)

    aggregate_rows = _aggregate_rows(detail_rows)
    changed_aggregate_rows = _changed_rows(
        original_day2_result["aggregate_rows"], aggregate_rows,
        ("business_date", "policy_type"))

    # F1 — COMPLETE provenance for the recomputed (changed-only) aggregate. The
    # recomputed aggregate for an affected policy_type is computed from the FULL
    # current detail set of that policy_type: the CHANGED rows (now in the refeed
    # detail sink) PLUS the UNCHANGED rows of the same policy_type (still in the
    # ORIGINAL Day-2 detail sink). Each contributing active detail output gets one
    # detail_to_aggregate input edge with its per-upstream contributing
    # record_count, so tracing the refed aggregate reaches ALL its contributing
    # detail outputs -> raw (mirrors policy_claims_dlq_workflow / its test_19).
    affected_types = {row["policy_type"] for row in changed_aggregate_rows}
    changed_keys = {(r["policy_id"], r["claim_id"]) for r in changed_detail_rows}
    # Unchanged contributing rows of the affected policy_type(s): they were
    # written by the ORIGINAL Day-2 detail sink (the refeed sink only carried the
    # changed rows). Count them from the original detail set.
    original_affected_unchanged = [
        r for r in original_day2_result["detail_rows"]
        if r["policy_type"] in affected_types
        and (r["policy_id"], r["claim_id"]) not in changed_keys
    ]
    # Changed contributing rows of the affected policy_type(s): the refeed sink.
    refeed_affected_changed = [
        r for r in changed_detail_rows if r["policy_type"] in affected_types
    ]
    detail_inputs: list[dict[str, Any]] = []
    if original_affected_unchanged:
        detail_inputs.append({
            "run_id": original_day2_result["detail_sink"]["run_id"],
            "link_id": original_day2_result["detail_sink"]["link_id"],
            "record_count": len(original_affected_unchanged),
            "role": "detail_original"})
    detail_inputs.append({
        "run_id": detail_sink["run_id"], "link_id": detail_sink["link_id"],
        "record_count": len(refeed_affected_changed), "role": "detail_corrected"})

    aggregate = _aggregate_from_detail(
        conn, workflow_run_id=workflow_run_id, business_date=business_date,
        detail_inputs=detail_inputs, aggregate_rows=changed_aggregate_rows,
        content_tag=content_tag, dag_run_id=dag_run_id, execution_type="refeed",
        commit=commit)
    aggregate_sink = _sink_rows(
        conn, workflow_run_id=workflow_run_id, business_date=business_date,
        dataset=AGG_DATASET, upstream_run_id=aggregate["run_id"],
        upstream_link_id=aggregate["link_id"],
        upstream_edge_type="detail_to_aggregate", rows=changed_aggregate_rows,
        content_tag=content_tag, stage_name="upsert_policy_claim_daily",
        task_id="sink_policy_claim_daily", dag_run_id=dag_run_id,
        execution_type="refeed", commit=commit)
    # CHANGED-ONLY visibility: supersede only the changed aggregate business keys.
    _activate_business_keys(
        conn, dataset=AGG_DATASET, target_name=AGG_TARGET,
        business_date=business_date, sink=aggregate_sink, file_id=None,
        workflow_run_id=workflow_run_id, rows=changed_aggregate_rows,
        key_fn=aggregate_business_key, reason="claim refeed (changed only)",
        commit=commit)

    # NOTE on F6 (fact-spine): refeed reconciles at changed-slice grain via
    # per-output reconcile_sink_link (already gating visibility); whole-fact
    # reconcile_workflow is not applicable to a changed-only slice.

    return {
        "workflow_run_id": workflow_run_id,
        "dag_run_id": dag_run_id,
        "business_date": business_date,
        "execution_type": "refeed",
        "refeed_of_workflow_run_id": original_day2_result["workflow_run_id"],
        "files": {"claim": corrected_file},
        "policy_silver": policy_silver,
        "claim_ingest": claim_ingest,
        "claim_silver": claim_silver,
        "merge": merge,
        "detail_sink": detail_sink,
        "aggregate": aggregate,
        "aggregate_sink": aggregate_sink,
        "detail_rows": detail_rows,
        "changed_detail_rows": changed_detail_rows,
        "aggregate_rows": aggregate_rows,
        "changed_aggregate_rows": changed_aggregate_rows,
    }


# --------------------------------------------------------------------------- #
# Orchestrator.
# --------------------------------------------------------------------------- #
def run_demo(conn, *, commit: bool = True) -> dict[str, Any]:
    """Run Day1/2/3 normal + Day2 claim refeed, each its own workflow_run_id."""
    ensure_targets(conn)

    normals = {}
    for business_date in BUSINESS_DATES:
        wfid = str(uuid.uuid4())
        dag_run_id = f"scheduled__{business_date}T00:00:00+00:00"
        normals[business_date] = normal_execution(
            conn, business_date, POLICY_ROWS, CLAIM_ROWS,
            workflow_run_id=wfid, dag_run_id=dag_run_id, commit=commit)

    refeed_wfid = str(uuid.uuid4())
    refeed_dag_run_id = f"manual__{REFEED_BUSINESS_DATE}T12:00:00+00:00-refeed"
    refeed = refeed_execution(
        conn, original_day2_result=normals[REFEED_BUSINESS_DATE],
        corrected_claim_rows=CORRECTED_CLAIM_ROWS,
        workflow_run_id=refeed_wfid, dag_run_id=refeed_dag_run_id, commit=commit)

    day1 = normals[BUSINESS_DATES[0]]
    day2 = normals[BUSINESS_DATES[1]]
    day3 = normals[BUSINESS_DATES[2]]

    executions_meta = [
        {
            "workflow_run_id": day1["workflow_run_id"],
            "business_date": str(day1["business_date"]),
            "execution_type": "normal",
            "refeed_of_workflow_run_id": None,
            "description": "Day 1 normal load (2026-05-28)",
        },
        {
            "workflow_run_id": day2["workflow_run_id"],
            "business_date": str(day2["business_date"]),
            "execution_type": "normal",
            "refeed_of_workflow_run_id": None,
            "description": "Day 2 normal load (2026-05-29)",
        },
        {
            "workflow_run_id": day3["workflow_run_id"],
            "business_date": str(day3["business_date"]),
            "execution_type": "normal",
            "refeed_of_workflow_run_id": None,
            "description": "Day 3 normal load (2026-05-30)",
        },
        {
            "workflow_run_id": refeed["workflow_run_id"],
            "business_date": str(refeed["business_date"]),
            "execution_type": "refeed",
            "refeed_of_workflow_run_id": refeed["refeed_of_workflow_run_id"],
            "description": "Day 2 claim refeed (corrected CL100 amount + CL102 status)",
        },
    ]

    return {
        "workflow_run_id": day1["workflow_run_id"],
        "day1": day1,
        "day2": day2,
        "day3": day3,
        "refeed": refeed,
        "normals_by_date": {str(d): r for d, r in normals.items()},
        "executions": executions_meta,
    }


# --------------------------------------------------------------------------- #
# Snapshot export.
# --------------------------------------------------------------------------- #
def export_demo_snapshot(conn, executions_meta: list[dict[str, Any]]) -> dict[str, Any]:
    """Export control-table + target-row state across ALL demo executions.

    Thin wrapper over the shared ``export_workflow_snapshot``: binds the
    insurance scenario + the policy_claim / policy_claim_daily target tables.
    """
    workflow_run_ids = [e["workflow_run_id"] for e in executions_meta]
    return export_workflow_snapshot(
        conn,
        workflow_run_ids=workflow_run_ids,
        detail_tables=[DETAIL_DATASET, AGG_DATASET],
        scenario={
            "domain": DOMAIN,
            "dag_id": DAG_ID,
            "business_dates": [str(d) for d in BUSINESS_DATES],
            "refeed_business_date": str(REFEED_BUSINESS_DATE),
            "visibility": {
                "replacement_scope": "business_key",
                "detail_key": "policy_id:claim_id",
                "aggregate_key": "business_date:policy_type",
            },
        },
        executions=executions_meta,
    )


def write_dashboard_snapshot(path: str | pathlib.Path = "dashboard/data/policy-claims-workflow.json",
                             *, reset: bool = True) -> dict[str, Any]:
    """Run the demo against Postgres (LEAVING data committed) and write JSON."""
    out = pathlib.Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    with connect() as conn:
        conn.autocommit = False
        if reset:
            reset_demo_state(conn)
        result = run_demo(conn, commit=True)
        snapshot = export_demo_snapshot(conn, result["executions"])
        conn.commit()
    out.write_text(json.dumps(snapshot, indent=2), encoding="utf-8")
    return snapshot


def main() -> None:
    parser = argparse.ArgumentParser(description="Insurance policy/claims lineage demo.")
    parser.add_argument(
        "--out",
        default="dashboard/data/policy-claims-workflow.json",
        help="Path to write dashboard JSON.",
    )
    parser.add_argument(
        "--no-reset",
        action="store_true",
        help="Append demo rows instead of clearing generated control/target tables first.",
    )
    args = parser.parse_args()
    snapshot = write_dashboard_snapshot(args.out, reset=not args.no_reset)
    print(f"wrote {args.out}")
    print(f"executions={len(snapshot['executions'])} "
          f"runs={len(snapshot['runs'])} links={len(snapshot['links'])} "
          f"files={len(snapshot['files'])}")
    for ex in snapshot["executions"]:
        print(f"  {ex['execution_type']:7s} {ex['business_date']} "
              f"{ex['workflow_run_id']}  {ex['description']}")


if __name__ == "__main__":
    main()
