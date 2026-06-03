"""Insurance policy/claims DLQ DEMO workflow — proves the schema-validation +
dead-letter-queue (quarantine) + replay/resolve story end to end.

Spec: docs/specs/2026-06-03-working-platform-completion-plan.md
      area 2 (DLQ, incl. "DLQ Implementation For This Repository") + area 3
      (schema validation).

This is a SEPARATE, focused workflow from harness/policy_claims_workflow.py. It
mirrors that demo's merge/sink/aggregate lineage shape (so it is a real lineage
story) but is NAMESPACED to its own datasets (``*_dlq``) so its domain-scoped
reset clears ONLY this workflow's data and NEVER touches the existing
customer/policy-claims demo:

  raw claim file (4 rows, 1 BAD) -> ingest -> canonicalize (SCHEMA-VALIDATE)
      -> 3 GOOD canonical rows + 1 QUARANTINED row (cp.dlq + 'quarantine' output)
  policy raw file -> ingest -> policy silver
  policy silver + GOOD claim canonical -> merge -> ods.policy_claim_dlq (detail)
  detail -> aggregate -> ods.policy_claim_daily_dlq (by business_date+policy_type)

The bad row is NOT business-visible (no active target_visibility for its key).

Then a REPLAY/FIX execution (its own workflow_run_id, trigger_type='replay'):
  corrected bad row -> replay run CONSUMES the quarantine output_link identity
      (a 'replay'-annotated input edge naming the quarantine output) AND the raw
      claim file (so the corrected output traces to the ORIGINAL raw + DLQ
      context) -> writes the corrected canonical output -> sinks the corrected
      detail row -> flips the dlq row to 'resolved' (resolved_by_run_id +
      resolved_by_output_link_id) -> activates the corrected row's business_key.

Every control-plane write goes through the sanctioned wrappers in ``control/``.
Read-only SELECTs (snapshot/tests) are fine; no raw cp.* inserts in the harness.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import pathlib
import uuid
from typing import Any

from control import dlq, lineage, recon, runs, schema, stages, visibility
from control.db import connect
from harness.snapshot import export_workflow_snapshot


# DISTINCT domain (NOT plain 'insurance') so this workflow's DOMAIN-SCOPED reset
# clears ONLY its own runs, and the existing insurance policy/claims demo's own
# domain-scoped reset never reaches (and FK-violates) this workflow's committed
# target rows. Datasets are ALSO namespaced (*_dlq) for a self-evident snapshot.
DOMAIN = "insurance_dlq"
POLICY_DATASET = "policy_dlq"
CLAIM_DATASET = "claim_dlq"
DETAIL_DATASET = "policy_claim_dlq"
AGG_DATASET = "policy_claim_daily_dlq"

# The schema contract is keyed on the canonical CLAIM dataset name ('claim') and
# layer 'canonicalization' (seeded in migration 025). The workflow tags its runs
# with the namespaced dataset but reads the contract under the canonical name.
CONTRACT_DATASET = "claim"
CONTRACT_LAYER = "canonicalization"
SCHEMA_VERSION = "claim.v1"

SINK_TYPE = "postgres"
DETAIL_TARGET = f"ods.{DETAIL_DATASET}"
AGG_TARGET = f"ods.{AGG_DATASET}"

DAG_ID = "ods_policy_claims_dlq"

BUSINESS_DATE = dt.date(2026, 5, 29)
DLQ_STAGE = "validate_schema"

# Stable policy roster (two policy_types so the aggregate has structure).
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

# The claim file: 4 rows, ONE bad. CL900 has a NULL policy_id -> it violates the
# claim.v1 contract (policy_id is required + non-nullable) and is QUARANTINED.
GOOD_CLAIM_ROWS = [
    {"claim_id": "CL500", "policy_id": "P001", "claim_date": "2026-05-20",
     "claim_status": "open", "claim_amount": 500.00},
    {"claim_id": "CL501", "policy_id": "P002", "claim_date": "2026-05-22",
     "claim_status": "closed", "claim_amount": 250.00},
    {"claim_id": "CL502", "policy_id": "P003", "claim_date": "2026-05-19",
     "claim_status": "closed", "claim_amount": 750.00},
]
BAD_CLAIM_ROW = {
    "claim_id": "CL900", "policy_id": None, "claim_date": "2026-05-21",
    "claim_status": "open", "claim_amount": 300.00,
}
CLAIM_ROWS = [GOOD_CLAIM_ROWS[0], GOOD_CLAIM_ROWS[1], BAD_CLAIM_ROW, GOOD_CLAIM_ROWS[2]]

# The CORRECTED bad row used by the replay/fix execution: same claim_id, the
# missing policy_id supplied (CL900 belongs to P002 -> auto).
CORRECTED_CLAIM_ROW = {
    "claim_id": "CL900", "policy_id": "P002", "claim_date": "2026-05-21",
    "claim_status": "open", "claim_amount": 300.00,
}


# --------------------------------------------------------------------------- #
# Business-key helpers (replacement_scope='business_key').
# --------------------------------------------------------------------------- #
def detail_business_key(row: dict[str, Any]) -> str:
    """Per-row business key for the ods.policy_claim_dlq detail target."""
    return f"{row['policy_id']}:{row['claim_id']}"


def aggregate_business_key(row: dict[str, Any]) -> str:
    """Per-row business key for the ods.policy_claim_daily_dlq aggregate target."""
    return f"{row['business_date']}:{row['policy_type']}"


def _orchestrator(*, dag_run_id: str, task_id: str, business_date: dt.date,
                  execution_type: str, try_number: int = 1) -> dict[str, Any]:
    """The simulated Airflow context dict recorded per run (migration 020)."""
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
    """Create the two demo target tables (same column shape as the other demo)."""
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


def reset_demo_state(conn) -> None:
    """Clear THIS workflow's generated demo/control state before a fresh snapshot.

    DOMAIN-SCOPED (domain = ``insurance_dlq``): clears ONLY this workflow's data.
    The existing insurance policy/claims demo runs under domain ``insurance``, so
    its rows are NEVER touched here AND its own domain-scoped reset never reaches
    these rows (distinct domains => no cross-demo FK violation). No global
    TRUNCATE; deleted in FK-safe child->parent order, scoped to this domain's run
    set. Re-running yields the same single set (no duplicates).
    """
    ensure_targets(conn)

    conn.execute(f"DELETE FROM ods.{DETAIL_DATASET}")
    conn.execute(f"DELETE FROM ods.{AGG_DATASET}")
    conn.execute("DELETE FROM ods.target_visibility WHERE domain = %s", (DOMAIN,))

    own_runs = "SELECT run_id FROM cp.run_log WHERE domain = %s"
    params = (DOMAIN,)
    conn.execute(
        f"DELETE FROM cp.reconciliation_log WHERE run_id IN ({own_runs})", params)
    conn.execute(f"DELETE FROM cp.dlq WHERE run_id IN ({own_runs})", params)
    conn.execute(
        f"""
        DELETE FROM cp.lineage_edge
        WHERE lineage_link_id IN (
            SELECT lineage_link_id FROM cp.lineage_link
            WHERE consumer_run_id IN ({own_runs})
        )
        """,
        params)
    conn.execute(
        f"DELETE FROM cp.lineage_link WHERE consumer_run_id IN ({own_runs})", params)
    conn.execute(
        f"DELETE FROM cp.run_stage_log WHERE run_id IN ({own_runs})", params)
    conn.execute("DELETE FROM cp.run_log WHERE domain = %s", (DOMAIN,))
    conn.execute("DELETE FROM cp.file_catalogue WHERE domain = %s", (DOMAIN,))


def _content_md5(rows: list[dict[str, Any]]) -> str:
    blob = json.dumps(rows, sort_keys=True, separators=(",", ":"))
    return hashlib.md5(blob.encode("utf-8")).hexdigest()


def _file(dataset: str, rows: list[dict[str, Any]], *,
          raw_suffix: str | None = None) -> dict[str, Any]:
    leaf = raw_suffix if raw_suffix is not None else str(BUSINESS_DATE)
    return {
        "s3_raw_path": f"s3://raw/{DOMAIN}/{dataset}/{leaf}.json",
        "file_md5": _content_md5(rows),
        "business_date": BUSINESS_DATE,
        "domain": DOMAIN,
        "dataset": dataset,
        "record_count": len(rows),
        "rows": rows,
    }


# --------------------------------------------------------------------------- #
# Per-hop helpers.
# --------------------------------------------------------------------------- #
def _ingest(conn, *, workflow_run_id: str, file: dict[str, Any],
            dag_run_id: str, task_id: str, commit: bool) -> dict[str, str]:
    """Register the raw file and run the ingestion hop -> raw_to_curated link."""
    rows = file["rows"]
    n = file["record_count"]
    file_id = runs.register_file(
        conn, s3_raw_path=file["s3_raw_path"], file_md5=file["file_md5"],
        business_date=file["business_date"], domain=file["domain"],
        dataset=file["dataset"], commit=commit)
    run_id = runs.start(
        conn, workflow_run_id=workflow_run_id, pipeline_type="ingestion",
        domain=file["domain"], dataset=file["dataset"],
        business_date=file["business_date"], trigger_type="airflow",
        file_id=file_id,
        orchestrator=_orchestrator(
            dag_run_id=dag_run_id, task_id=task_id,
            business_date=file["business_date"], execution_type="normal"),
        commit=commit)
    with stages.stage_scope(conn, run_id, "ingest_raw", commit=commit) as st:
        st.record_in = n
        st.record_out = n
    link_id = lineage.write_output_link(
        conn, consumer_run_id=run_id, edge_type="raw_to_curated",
        target_ref={
            "path": f"s3://bronze/{file['dataset']}/{file['business_date']}.json",
            "content_hash": f"bronze-{file['file_md5']}", "version": 1},
        record_count=n,
        inputs=[{
            "source_file_id": file_id, "edge_type": "raw_to_curated",
            "source_ref": {"path": file["s3_raw_path"]}, "record_count": n}],
        transform_version="raw-v1", commit=commit)
    recon.write_check(conn, run_id=run_id, check_type="ingest",
                      source_count=n, accounted_count=n, commit=commit)
    runs.finalise(conn, run_id, status="succeeded", record_count_out=n,
                  commit=commit)
    return {"run_id": run_id, "file_id": file_id, "link_id": link_id}


def _canonicalize_policy(conn, *, workflow_run_id: str, file: dict[str, Any],
                         ingest: dict[str, str], dag_run_id: str,
                         commit: bool) -> dict[str, str]:
    """Canonicalize the policy raw file to silver (no validation; policies clean)."""
    n = file["record_count"]
    run_id = runs.start(
        conn, workflow_run_id=workflow_run_id, pipeline_type="canonicalization",
        domain=DOMAIN, dataset=file["dataset"], business_date=BUSINESS_DATE,
        trigger_type="airflow",
        orchestrator=_orchestrator(
            dag_run_id=dag_run_id, task_id="canonicalize_policy",
            business_date=BUSINESS_DATE, execution_type="normal"),
        commit=commit)
    with stages.stage_scope(conn, run_id, "canonicalize_to_silver",
                            commit=commit) as st:
        st.record_in = n
        st.record_out = n
    link_id = lineage.write_output_link(
        conn, consumer_run_id=run_id, edge_type="curated_to_canonical",
        target_ref={
            "path": f"s3://silver/{file['dataset']}/{BUSINESS_DATE}.parquet",
            "content_hash": f"silver-{file['file_md5']}", "version": 1},
        record_count=n,
        inputs=[{
            "upstream_run_id": ingest["run_id"],
            "upstream_output_link_id": ingest["link_id"],
            "edge_type": "curated_to_canonical",
            "source_ref": {"layer": "bronze", "dataset": file["dataset"]},
            "record_count": n}],
        transform_version="silver-v1", commit=commit)
    recon.write_check(conn, run_id=run_id, check_type="canonicalize_to_silver",
                      source_count=n, accounted_count=n, commit=commit)
    runs.finalise(conn, run_id, status="succeeded", record_count_out=n,
                  commit=commit)
    return {"run_id": run_id, "link_id": link_id}


def _canonicalize_claim_with_validation(
        conn, *, workflow_run_id: str, file: dict[str, Any],
        ingest: dict[str, str], dag_run_id: str, commit: bool) -> dict[str, Any]:
    """Canonicalize the CLAIM raw file with SCHEMA VALIDATION (areas 2 + 3).

    Reads the claim.v1 contract (cp.schema_contract) via control.schema, splits
    the raw rows into good + bad, writes the GOOD curated output (record_count =
    #good, target_ref carrying schema_version), QUARANTINES the bad row (cp.dlq
    row + first-class 'quarantine' output_link, failed_payload preserved), records
    validation metrics on the stage, and writes a schema_validation recon row
    (input == good + dlq -> status 'ok').
    """
    raw_rows = file["rows"]
    n_in = file["record_count"]
    contract = schema.get_contract(
        conn, domain=DOMAIN, dataset=CONTRACT_DATASET, layer=CONTRACT_LAYER,
        schema_version=SCHEMA_VERSION)
    if contract is None:
        raise RuntimeError(
            f"no schema contract for {DOMAIN}/{CONTRACT_DATASET}/{CONTRACT_LAYER}"
            f"/{SCHEMA_VERSION} — apply migration 025")
    good_rows, bad = schema.validate_rows(raw_rows, contract)
    reasons = [reason for _row, reason in bad]

    run_id = runs.start(
        conn, workflow_run_id=workflow_run_id, pipeline_type="canonicalization",
        domain=DOMAIN, dataset=file["dataset"], business_date=BUSINESS_DATE,
        trigger_type="airflow",
        orchestrator=_orchestrator(
            dag_run_id=dag_run_id, task_id="canonicalize_claim",
            business_date=BUSINESS_DATE, execution_type="normal"),
        commit=commit)
    with stages.stage_scope(conn, run_id, DLQ_STAGE, commit=commit) as st:
        st.record_in = n_in
        st.record_out = len(good_rows)
        st.metrics = {
            "schema_version": SCHEMA_VERSION,
            "rows_in": n_in,
            "good": len(good_rows),
            "quarantined": len(bad),
            "reasons": reasons,
        }

    good_link = lineage.write_output_link(
        conn, consumer_run_id=run_id, edge_type="curated_to_canonical",
        target_ref={
            "path": f"s3://silver/{file['dataset']}/{BUSINESS_DATE}.parquet",
            "content_hash": f"silver-{file['file_md5']}",
            "schema_version": SCHEMA_VERSION,
            "version": 1},
        record_count=len(good_rows),
        inputs=[{
            "upstream_run_id": ingest["run_id"],
            "upstream_output_link_id": ingest["link_id"],
            "edge_type": "curated_to_canonical",
            "source_ref": {"layer": "bronze", "dataset": file["dataset"]},
            "record_count": len(good_rows)}],
        transform_version="silver-v1", commit=commit)

    # Quarantine each bad row: a cp.dlq row (failed_payload preserved, status
    # 'open') + a first-class 'quarantine' output_link. source_ref names the raw
    # file so the quarantine event is anchored to its origin.
    dlq_ids = []
    for bad_row, reason in bad:
        dlq_id = dlq.quarantine(
            conn, run_id=run_id, stage=DLQ_STAGE, reason=reason,
            source_ref={"raw_file_id": ingest["file_id"],
                        "raw_path": file["s3_raw_path"],
                        "schema_version": SCHEMA_VERSION},
            payload_ref=f"s3://dlq/{DOMAIN}/{CONTRACT_DATASET}/{BUSINESS_DATE}/"
                        f"claim-v1-errors.json",
            record_count=1, failed_payload=bad_row, commit=commit)
        dlq_ids.append(dlq_id)

    # Recon: input rows == good + dlq (spec lines 304-308) -> status 'ok'.
    recon.write_check(
        conn, run_id=run_id, check_type="schema_validation",
        source_count=n_in, accounted_count=len(good_rows) + len(bad),
        metrics={"schema_version": SCHEMA_VERSION, "good": len(good_rows),
                 "quarantined": len(bad)},
        commit=commit)
    runs.finalise(conn, run_id, status="succeeded",
                  record_count_out=len(good_rows), commit=commit)
    return {
        "run_id": run_id, "link_id": good_link, "good_rows": good_rows,
        "bad": bad, "dlq_ids": dlq_ids, "reasons": reasons,
    }


def _merge_rows(claim_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Join claims to their policy on policy_id -> the merged detail rows."""
    policies = {row["policy_id"]: row for row in POLICY_ROWS}
    merged = []
    for claim in claim_rows:
        policy = policies[claim["policy_id"]]
        merged.append({
            "business_date": str(BUSINESS_DATE),
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


def _merge_to_detail(conn, *, workflow_run_id: str, policy_silver: dict[str, str],
                     claim_silver: dict[str, str],
                     detail_rows: list[dict[str, Any]], content_tag: str,
                     dag_run_id: str, execution_type: str,
                     commit: bool) -> dict[str, str]:
    """Join policy + claim silver. Two merge_to_canonical input edges (policy in
    slot 0, claim in slot 1), each naming the EXACT upstream output."""
    run_id = runs.start(
        conn, workflow_run_id=workflow_run_id, pipeline_type="merge",
        domain=DOMAIN, dataset=DETAIL_DATASET, business_date=BUSINESS_DATE,
        trigger_type="airflow",
        orchestrator=_orchestrator(
            dag_run_id=dag_run_id, task_id="merge_policy_claim",
            business_date=BUSINESS_DATE, execution_type=execution_type),
        commit=commit)
    with stages.stage_scope(conn, run_id, "merge_policy_claim",
                            commit=commit) as st:
        st.record_in = len(POLICY_ROWS) + len(detail_rows)
        st.record_out = len(detail_rows)
        st.metrics = {"inputs": [POLICY_DATASET, CLAIM_DATASET],
                      "join_key": "policy_id"}
    link_id = lineage.write_output_link(
        conn, consumer_run_id=run_id, edge_type="merge_to_canonical",
        target_ref={
            "path": f"s3://silver/{DETAIL_DATASET}/{BUSINESS_DATE}.parquet",
            "content_hash": f"silver-{DETAIL_DATASET}-{content_tag}", "version": 1},
        record_count=len(detail_rows),
        inputs=[
            {"upstream_run_id": policy_silver["run_id"],
             "upstream_output_link_id": policy_silver["link_id"], "input_slot": 0,
             "edge_type": "merge_to_canonical",
             "source_ref": {"input_role": "policy", "dataset": POLICY_DATASET},
             "record_count": len(POLICY_ROWS)},
            {"upstream_run_id": claim_silver["run_id"],
             "upstream_output_link_id": claim_silver["link_id"], "input_slot": 1,
             "edge_type": "merge_to_canonical",
             "source_ref": {"input_role": "claim", "dataset": CLAIM_DATASET},
             "record_count": len(detail_rows)},
        ],
        transform_version="join-v1", commit=commit)
    recon.write_check(
        conn, run_id=run_id, check_type="merge_policy_claim",
        source_count=len(detail_rows), accounted_count=len(detail_rows),
        metrics={"source_records_read": len(POLICY_ROWS) + len(detail_rows)},
        commit=commit)
    runs.finalise(conn, run_id, status="succeeded",
                  record_count_out=len(detail_rows), commit=commit)
    return {"run_id": run_id, "link_id": link_id}


def _sink_rows(conn, *, workflow_run_id: str, dataset: str,
               upstream_run_id: str, upstream_link_id: str,
               upstream_edge_type: str, rows: list[dict[str, Any]],
               content_tag: str, stage_name: str, task_id: str,
               dag_run_id: str, execution_type: str,
               pipeline_type: str = "sink", commit: bool = True) -> dict[str, str]:
    """Upsert rows to ods.<dataset> via the sanctioned output-then-rows path."""
    run_id = runs.start(
        conn, workflow_run_id=workflow_run_id, pipeline_type=pipeline_type,
        domain=DOMAIN, dataset=dataset, business_date=BUSINESS_DATE,
        trigger_type="airflow",
        orchestrator=_orchestrator(
            dag_run_id=dag_run_id, task_id=task_id,
            business_date=BUSINESS_DATE, execution_type=execution_type),
        commit=commit)
    with stages.stage_scope(conn, run_id, stage_name, commit=commit) as st:
        st.record_in = len(rows)
        st.record_out = len(rows)
    link_id = lineage.write_output_then_rows(
        conn, consumer_run_id=run_id, edge_type="canonical_to_sink",
        target_ref={
            "path": f"postgres://ods/{dataset}",
            "content_hash": f"postgres-{dataset}-{content_tag}", "version": 1},
        record_count=len(rows),
        inputs=[{
            "upstream_run_id": upstream_run_id,
            "upstream_output_link_id": upstream_link_id,
            "edge_type": "canonical_to_sink",
            "source_ref": {"upstream_edge_type": upstream_edge_type},
            "record_count": len(rows)}],
        rows=rows, sink_type=SINK_TYPE, transform_version="postgres-upsert-v1",
        commit=commit)
    recon.reconcile_sink_link(conn, lineage_link_id=link_id,
                              source_count=len(rows), commit=commit)
    runs.finalise(conn, run_id, status="succeeded", record_count_out=len(rows),
                  commit=commit)
    return {"run_id": run_id, "link_id": link_id}


def _aggregate_from_detail(conn, *, workflow_run_id: str,
                           detail_sink: dict[str, str],
                           detail_rows: list[dict[str, Any]],
                           aggregate_rows: list[dict[str, Any]],
                           content_tag: str, dag_run_id: str,
                           execution_type: str, commit: bool) -> dict[str, str]:
    """Aggregate the detail SINK output as a first-class detail_to_aggregate run."""
    run_id = runs.start(
        conn, workflow_run_id=workflow_run_id, pipeline_type="aggregation",
        domain=DOMAIN, dataset=AGG_DATASET, business_date=BUSINESS_DATE,
        trigger_type="airflow",
        orchestrator=_orchestrator(
            dag_run_id=dag_run_id, task_id="aggregate_policy_claim_daily",
            business_date=BUSINESS_DATE, execution_type=execution_type),
        commit=commit)
    with stages.stage_scope(conn, run_id, "aggregate_policy_claim_daily",
                            commit=commit) as st:
        st.record_in = len(detail_rows)
        st.record_out = len(aggregate_rows)
        st.metrics = {"group_by": ["business_date", "policy_type"]}
    link_id = lineage.write_output_link(
        conn, consumer_run_id=run_id, edge_type="detail_to_aggregate",
        target_ref={
            "path": f"s3://gold/{AGG_DATASET}/{BUSINESS_DATE}.parquet",
            "content_hash": f"gold-{AGG_DATASET}-{content_tag}", "version": 1},
        record_count=len(aggregate_rows),
        inputs=[{
            "upstream_run_id": detail_sink["run_id"],
            "upstream_output_link_id": detail_sink["link_id"],
            "edge_type": "detail_to_aggregate",
            "source_ref": {"input_role": "detail", "table": DETAIL_TARGET},
            "record_count": len(detail_rows)}],
        transform_version="agg-v1", commit=commit)
    recon.write_check(
        conn, run_id=run_id, check_type="aggregate_policy_claim_daily",
        source_count=len(detail_rows), accounted_count=len(detail_rows),
        metrics={"aggregate_rows": len(aggregate_rows)}, commit=commit)
    runs.finalise(conn, run_id, status="succeeded",
                  record_count_out=len(aggregate_rows), commit=commit)
    return {"run_id": run_id, "link_id": link_id}


def _activate_business_keys(conn, *, dataset: str, target_name: str,
                            sink: dict[str, str], workflow_run_id: str,
                            rows: list[dict[str, Any]], key_fn, reason: str | None,
                            commit: bool) -> list[str]:
    """Activate ONE target-visibility row per business key produced by this sink."""
    visibility_ids = []
    for row in rows:
        vis_id = visibility.activate(
            conn, domain=DOMAIN, dataset=dataset, business_date=BUSINESS_DATE,
            sink_type=SINK_TYPE, target_name=target_name, file_id=None,
            output_link_id=sink["link_id"], producer_run_id=sink["run_id"],
            workflow_run_id=workflow_run_id, replacement_scope="business_key",
            replacement_key=key_fn(row), reason=reason, commit=commit)
        visibility_ids.append(vis_id)
    return visibility_ids


# --------------------------------------------------------------------------- #
# Executions.
# --------------------------------------------------------------------------- #
def normal_execution(conn, *, workflow_run_id: str, dag_run_id: str,
                     commit: bool) -> dict[str, Any]:
    """The validating load: ingest policy + claim, canonicalize claim WITH schema
    validation (3 good + 1 quarantined), merge GOOD claims with policy, sink the
    detail, aggregate, sink the aggregate, and activate visibility for the GOOD
    rows only (the bad row is NEVER business-visible)."""
    policy_file = _file(POLICY_DATASET, POLICY_ROWS)
    claim_file = _file(CLAIM_DATASET, CLAIM_ROWS)

    policy_ingest = _ingest(
        conn, workflow_run_id=workflow_run_id, file=policy_file,
        dag_run_id=dag_run_id, task_id="ingest_policy", commit=commit)
    claim_ingest = _ingest(
        conn, workflow_run_id=workflow_run_id, file=claim_file,
        dag_run_id=dag_run_id, task_id="ingest_claim", commit=commit)
    policy_silver = _canonicalize_policy(
        conn, workflow_run_id=workflow_run_id, file=policy_file,
        ingest=policy_ingest, dag_run_id=dag_run_id, commit=commit)
    claim_canonical = _canonicalize_claim_with_validation(
        conn, workflow_run_id=workflow_run_id, file=claim_file,
        ingest=claim_ingest, dag_run_id=dag_run_id, commit=commit)

    good_claim_rows = claim_canonical["good_rows"]
    detail_rows = _merge_rows(good_claim_rows)
    content_tag = f"{workflow_run_id}-orig"
    merge = _merge_to_detail(
        conn, workflow_run_id=workflow_run_id, policy_silver=policy_silver,
        claim_silver=claim_canonical, detail_rows=detail_rows,
        content_tag=content_tag, dag_run_id=dag_run_id, execution_type="normal",
        commit=commit)
    detail_sink = _sink_rows(
        conn, workflow_run_id=workflow_run_id, dataset=DETAIL_DATASET,
        upstream_run_id=merge["run_id"], upstream_link_id=merge["link_id"],
        upstream_edge_type="merge_to_canonical", rows=detail_rows,
        content_tag=content_tag, stage_name="upsert_policy_claim",
        task_id="sink_policy_claim", dag_run_id=dag_run_id,
        execution_type="normal", commit=commit)
    _activate_business_keys(
        conn, dataset=DETAIL_DATASET, target_name=DETAIL_TARGET, sink=detail_sink,
        workflow_run_id=workflow_run_id, rows=detail_rows,
        key_fn=detail_business_key, reason="normal load", commit=commit)

    aggregate_rows = _aggregate_rows(detail_rows)
    aggregate = _aggregate_from_detail(
        conn, workflow_run_id=workflow_run_id, detail_sink=detail_sink,
        detail_rows=detail_rows, aggregate_rows=aggregate_rows,
        content_tag=content_tag, dag_run_id=dag_run_id, execution_type="normal",
        commit=commit)
    aggregate_sink = _sink_rows(
        conn, workflow_run_id=workflow_run_id, dataset=AGG_DATASET,
        upstream_run_id=aggregate["run_id"], upstream_link_id=aggregate["link_id"],
        upstream_edge_type="detail_to_aggregate", rows=aggregate_rows,
        content_tag=content_tag, stage_name="upsert_policy_claim_daily",
        task_id="sink_policy_claim_daily", dag_run_id=dag_run_id,
        execution_type="normal", commit=commit)
    _activate_business_keys(
        conn, dataset=AGG_DATASET, target_name=AGG_TARGET, sink=aggregate_sink,
        workflow_run_id=workflow_run_id, rows=aggregate_rows,
        key_fn=aggregate_business_key, reason="normal load", commit=commit)

    return {
        "workflow_run_id": workflow_run_id,
        "dag_run_id": dag_run_id,
        "execution_type": "normal",
        "files": {"policy": policy_file, "claim": claim_file},
        "policy_ingest": policy_ingest,
        "claim_ingest": claim_ingest,
        "policy_silver": policy_silver,
        "claim_canonical": claim_canonical,
        "merge": merge,
        "detail_sink": detail_sink,
        "aggregate": aggregate,
        "aggregate_sink": aggregate_sink,
        "detail_rows": detail_rows,
        "aggregate_rows": aggregate_rows,
        "good_claim_rows": good_claim_rows,
    }


def replay_dlq(conn, *, normal_result: dict[str, Any],
               corrected_claim_row: dict[str, Any], workflow_run_id: str,
               dag_run_id: str, commit: bool) -> dict[str, Any]:
    """Replay/fix the quarantined row (areas 2 "replay" + DLQ acceptance).

    A DLQ replay run (trigger_type='replay', replay_of_run_id = the original
    canonicalization run) that:
      * CONSUMES the prior DLQ/quarantine identity: its corrected canonical
        output carries a 'replay'-annotated input edge whose
        upstream_output_link_id IS the quarantine output_link the quarantine
        event created (so the replay literally references the quarantine link),
      * traces back to the ORIGINAL raw claim file: a second input edge names the
        raw claim file's source_file_id (the corrected row came from that raw
        file), so the corrected output's provenance reaches raw,
      * writes the corrected detail row into NORMAL lineage (merge -> sink),
      * resolves the dlq row: dlq.resolve(status='resolved',
        resolved_by_run_id=replay sink run, resolved_by_output_link_id=corrected
        canonical output),
      * makes the corrected row business-visible (activate its business_key).
    """
    claim_canonical = normal_result["claim_canonical"]
    original_canon_run = claim_canonical["run_id"]
    dlq_id = claim_canonical["dlq_ids"][0]
    raw_claim_file_id = normal_result["claim_ingest"]["file_id"]
    raw_claim_link_id = normal_result["claim_ingest"]["link_id"]
    policy_silver = normal_result["policy_silver"]

    quarantine_link_id = conn.execute(
        "SELECT quarantine_output_link_id FROM cp.dlq WHERE dlq_id=%s",
        (dlq_id,)).fetchone()[0]
    quarantine_link_id = str(quarantine_link_id)

    # Mark the dlq row corrected (under_review -> corrected) before replay.
    dlq.resolve(conn, dlq_id=dlq_id, status="corrected", commit=commit)

    # Replay canonicalization run: corrected row re-validated, then re-canonicalized.
    contract = schema.get_contract(
        conn, domain=DOMAIN, dataset=CONTRACT_DATASET, layer=CONTRACT_LAYER,
        schema_version=SCHEMA_VERSION)
    good, bad = schema.validate_rows([corrected_claim_row], contract)
    if bad:
        raise RuntimeError(f"corrected row still invalid: {bad[0][1]}")

    replay_run_id = runs.start(
        conn, workflow_run_id=workflow_run_id, pipeline_type="canonicalization",
        domain=DOMAIN, dataset=CLAIM_DATASET, business_date=BUSINESS_DATE,
        trigger_type="replay", replay_of_run_id=original_canon_run,
        orchestrator=_orchestrator(
            dag_run_id=dag_run_id, task_id="replay_canonicalize_claim",
            business_date=BUSINESS_DATE, execution_type="replay"),
        commit=commit)
    with stages.stage_scope(conn, replay_run_id, "replay_validate_schema",
                            commit=commit) as st:
        st.record_in = 1
        st.record_out = 1
        st.metrics = {"schema_version": SCHEMA_VERSION, "replayed_dlq_id": dlq_id,
                      "good": 1, "quarantined": 0}

    # The corrected canonical output: TWO provenance input edges.
    #   slot 0 — raw_to_curated-style file edge to the ORIGINAL raw claim file:
    #            the corrected row's content came from that raw file, so the
    #            output traces back to raw. (edge_type matches the link.)
    #   replay annotation — names the quarantine output_link as
    #            upstream_output_link_id (the prior DLQ identity the replay
    #            consumes). edge_type='replay' is the one allowed annotation that
    #            may differ from the link's curated_to_canonical type; it is a
    #            leaf in the walk (the quarantine link it points at terminates),
    #            so it adds DLQ context without changing the trace-to-raw result.
    corrected_link_id = lineage.write_output_link(
        conn, consumer_run_id=replay_run_id, edge_type="curated_to_canonical",
        target_ref={
            "path": f"s3://silver/{CLAIM_DATASET}/{BUSINESS_DATE}-replay.parquet",
            "content_hash": f"silver-{CLAIM_DATASET}-replay-{dlq_id}",
            "schema_version": SCHEMA_VERSION, "version": 1},
        record_count=1,
        inputs=[
            {"upstream_run_id": normal_result["claim_ingest"]["run_id"],
             "upstream_output_link_id": raw_claim_link_id,
             "input_slot": 0,
             "edge_type": "curated_to_canonical",
             "source_ref": {"layer": "bronze", "dataset": CLAIM_DATASET,
                            "raw_file_id": raw_claim_file_id, "replay": True},
             "record_count": 1},
            {"upstream_output_link_id": quarantine_link_id,
             "edge_type": "replay",
             "source_ref": {"dlq_id": dlq_id, "replays_quarantine": True},
             "record_count": 1},
        ],
        transform_version="silver-replay-v1", commit=commit)
    recon.write_check(
        conn, run_id=replay_run_id, check_type="schema_validation",
        source_count=1, accounted_count=1,
        metrics={"schema_version": SCHEMA_VERSION, "replayed_dlq_id": dlq_id},
        commit=commit)
    runs.finalise(conn, replay_run_id, status="succeeded", record_count_out=1,
                  commit=commit)

    # Re-merge the corrected claim with policy, then sink the corrected detail row
    # into NORMAL lineage. The merge consumes the corrected canonical output.
    detail_rows = _merge_rows([corrected_claim_row])
    content_tag = f"{workflow_run_id}-corrected"
    merge = _merge_to_detail(
        conn, workflow_run_id=workflow_run_id, policy_silver=policy_silver,
        claim_silver={"run_id": replay_run_id, "link_id": corrected_link_id},
        detail_rows=detail_rows, content_tag=content_tag, dag_run_id=dag_run_id,
        execution_type="replay", commit=commit)
    detail_sink = _sink_rows(
        conn, workflow_run_id=workflow_run_id, dataset=DETAIL_DATASET,
        upstream_run_id=merge["run_id"], upstream_link_id=merge["link_id"],
        upstream_edge_type="merge_to_canonical", rows=detail_rows,
        content_tag=content_tag, stage_name="upsert_policy_claim",
        task_id="replay_sink_policy_claim", dag_run_id=dag_run_id,
        execution_type="replay", commit=commit)

    # Resolve the dlq row: corrected -> resolved, pointing at the replay run + the
    # corrected canonical output. failed_payload / reason are NEVER touched.
    dlq.resolve(conn, dlq_id=dlq_id, status="resolved",
                resolved_by_run_id=replay_run_id,
                resolved_by_output_link_id=corrected_link_id, commit=commit)

    # Make the corrected detail row business-visible (activate its business_key).
    _activate_business_keys(
        conn, dataset=DETAIL_DATASET, target_name=DETAIL_TARGET, sink=detail_sink,
        workflow_run_id=workflow_run_id, rows=detail_rows,
        key_fn=detail_business_key, reason="dlq replay (corrected row)",
        commit=commit)

    # ----------------------------------------------------------------------- #
    # P1b FIX — recompute the AFFECTED aggregate key(s) so
    # policy_claim_daily_dlq is not left STALE after replay.
    #
    # The corrected detail row changed exactly the policy_type(s) it belongs to
    # (CL900 -> P002 -> 'auto'). The NORMAL aggregate for 'auto' counted only the
    # 2 originally-good auto claims (CL500, CL501 -> count=2, total=750). Now that
    # the corrected CL900 (auto, 300) is in the current detail set, the 'auto'
    # aggregate must be recomputed from the FULL current auto detail set
    # (count=3, total=1050). We recompute CHANGED-ONLY: only the aggregate keys
    # whose claims changed (the corrected row's policy_type(s)); unchanged keys
    # (e.g. 'home') are left untouched and stay active.
    aggregate_replay = _recompute_affected_aggregates(
        conn, workflow_run_id=workflow_run_id,
        normal_result=normal_result, corrected_detail_rows=detail_rows,
        detail_sink=detail_sink, content_tag=content_tag, dag_run_id=dag_run_id,
        commit=commit)

    return {
        "workflow_run_id": workflow_run_id,
        "dag_run_id": dag_run_id,
        "execution_type": "replay",
        "dlq_id": dlq_id,
        "quarantine_link_id": quarantine_link_id,
        "replay_run_id": replay_run_id,
        "corrected_link_id": corrected_link_id,
        "merge": merge,
        "detail_sink": detail_sink,
        "detail_rows": detail_rows,
        "corrected_business_key": detail_business_key(detail_rows[0]),
        "aggregate_replay": aggregate_replay,
    }


def _recompute_affected_aggregates(
        conn, *, workflow_run_id: str, normal_result: dict[str, Any],
        corrected_detail_rows: list[dict[str, Any]], detail_sink: dict[str, str],
        content_tag: str, dag_run_id: str, commit: bool) -> dict[str, Any]:
    """Recompute + re-sink + re-activate ONLY the aggregate key(s) the replay
    changed, superseding the stale aggregate row(s) for those keys.

    The current full detail set after replay = the NORMAL good detail rows PLUS
    the corrected detail row(s). The affected aggregate keys are the policy_type
    buckets that the corrected row(s) touch; we recompute those buckets from the
    full current detail set so claim_count / total reflect the corrected reality
    (e.g. 'auto' -> 3 claims / 1050). The aggregation run consumes the CORRECTED
    DETAIL SINK output (edge_type detail_to_aggregate), mirroring the NORMAL
    aggregate path, so the recomputed aggregate traces to the corrected detail.
    """
    # The full current detail set: normal good detail rows + the corrected rows.
    current_detail = list(normal_result["detail_rows"]) + list(corrected_detail_rows)

    # The aggregate keys CHANGED by the replay = the policy_types of the corrected
    # rows (changed-only; unchanged keys are not recomputed).
    affected_types = {row["policy_type"] for row in corrected_detail_rows}

    # Recompute EVERY aggregate bucket from the full current detail set, then keep
    # ONLY the affected ones (so the recomputed counts include both pre-existing
    # and corrected claims for that policy_type).
    all_recomputed = _aggregate_rows(current_detail)
    affected_aggregate_rows = [
        row for row in all_recomputed if row["policy_type"] in affected_types
    ]
    if not affected_aggregate_rows:
        return {"affected_policy_types": [], "aggregate": None,
                "aggregate_sink": None, "aggregate_rows": []}

    agg_content_tag = f"{content_tag}-agg-replay"
    aggregate = _aggregate_from_detail(
        conn, workflow_run_id=workflow_run_id, detail_sink=detail_sink,
        detail_rows=corrected_detail_rows, aggregate_rows=affected_aggregate_rows,
        content_tag=agg_content_tag, dag_run_id=dag_run_id,
        execution_type="replay", commit=commit)
    aggregate_sink = _sink_rows(
        conn, workflow_run_id=workflow_run_id, dataset=AGG_DATASET,
        upstream_run_id=aggregate["run_id"], upstream_link_id=aggregate["link_id"],
        upstream_edge_type="detail_to_aggregate", rows=affected_aggregate_rows,
        content_tag=agg_content_tag, stage_name="upsert_policy_claim_daily",
        task_id="replay_sink_policy_claim_daily", dag_run_id=dag_run_id,
        execution_type="replay", commit=commit)
    # Activate the recomputed aggregate business_key(s), superseding the stale
    # NORMAL aggregate row(s) for the SAME key(s). business_key scope means only
    # these keys are superseded; unchanged aggregate keys stay active.
    _activate_business_keys(
        conn, dataset=AGG_DATASET, target_name=AGG_TARGET, sink=aggregate_sink,
        workflow_run_id=workflow_run_id, rows=affected_aggregate_rows,
        key_fn=aggregate_business_key,
        reason="dlq replay (recomputed aggregate)", commit=commit)

    return {
        "affected_policy_types": sorted(affected_types),
        "aggregate": aggregate,
        "aggregate_sink": aggregate_sink,
        "aggregate_rows": affected_aggregate_rows,
        "affected_aggregate_keys": [
            aggregate_business_key(row) for row in affected_aggregate_rows],
    }


# --------------------------------------------------------------------------- #
# Orchestrator.
# --------------------------------------------------------------------------- #
def run_demo(conn, *, commit: bool = True) -> dict[str, Any]:
    """Run the validating normal load + the DLQ replay/fix, each its own wfid."""
    ensure_targets(conn)

    normal_wfid = str(uuid.uuid4())
    normal_dag_run_id = f"scheduled__{BUSINESS_DATE}T00:00:00+00:00"
    normal = normal_execution(
        conn, workflow_run_id=normal_wfid, dag_run_id=normal_dag_run_id,
        commit=commit)

    replay_wfid = str(uuid.uuid4())
    replay_dag_run_id = f"manual__{BUSINESS_DATE}T12:00:00+00:00-dlq-replay"
    replay = replay_dlq(
        conn, normal_result=normal, corrected_claim_row=CORRECTED_CLAIM_ROW,
        workflow_run_id=replay_wfid, dag_run_id=replay_dag_run_id, commit=commit)

    executions_meta = [
        {
            "workflow_run_id": normal["workflow_run_id"],
            "business_date": str(BUSINESS_DATE),
            "execution_type": "normal",
            "replay_of_workflow_run_id": None,
            "description": "Validating load (4 claim rows, 1 quarantined CL900)",
        },
        {
            "workflow_run_id": replay["workflow_run_id"],
            "business_date": str(BUSINESS_DATE),
            "execution_type": "replay",
            "replay_of_workflow_run_id": normal["workflow_run_id"],
            "description": "DLQ replay/fix (corrected CL900 policy_id; resolved)",
        },
    ]

    return {
        "workflow_run_id": normal["workflow_run_id"],
        "normal": normal,
        "replay": replay,
        "executions": executions_meta,
    }


# --------------------------------------------------------------------------- #
# Snapshot export.
# --------------------------------------------------------------------------- #
def export_demo_snapshot(conn, executions_meta: list[dict[str, Any]]) -> dict[str, Any]:
    """Export control-table + target-row state across the DLQ demo executions."""
    workflow_run_ids = [e["workflow_run_id"] for e in executions_meta]
    return export_workflow_snapshot(
        conn,
        workflow_run_ids=workflow_run_ids,
        detail_tables=[DETAIL_DATASET, AGG_DATASET],
        scenario={
            "domain": DOMAIN,
            "dag_id": DAG_ID,
            "business_date": str(BUSINESS_DATE),
            "story": "schema-validation + DLQ quarantine + replay/resolve",
            "schema_version": SCHEMA_VERSION,
            "claim_rows_in": len(CLAIM_ROWS),
            "quarantined_claim_id": BAD_CLAIM_ROW["claim_id"],
            "dlq_stage": DLQ_STAGE,
            "visibility": {
                "replacement_scope": "business_key",
                "detail_key": "policy_id:claim_id",
                "aggregate_key": "business_date:policy_type",
            },
        },
        executions=executions_meta,
    )


def write_dashboard_snapshot(
        path: str | pathlib.Path = "dashboard/data/policy-claims-dlq-workflow.json",
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
    parser = argparse.ArgumentParser(
        description="Insurance policy/claims DLQ (schema-validation + replay) demo.")
    parser.add_argument(
        "--out", default="dashboard/data/policy-claims-dlq-workflow.json",
        help="Path to write dashboard JSON.")
    parser.add_argument(
        "--no-reset", action="store_true",
        help="Append demo rows instead of clearing this workflow's data first.")
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
