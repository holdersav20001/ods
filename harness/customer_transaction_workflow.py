"""Customer + transaction lineage DEMO workflow (multi-day + refeed).

A self-contained demo fixture on top of the existing ODS control plane. It proves
multi-input lineage, target-row traceability, and original-vs-corrected (refeed)
provenance across three business dates plus one Day-2 transaction refeed.

Shape per NORMAL execution (8 runs, one shared workflow_run_id):

  raw customer file    -> ingest customer      -> customer silver
  raw transaction file -> ingest transaction   -> transaction silver
  customer + transaction silver -> merge customer_transaction -> ods.customer_transaction
  detail output        -> aggregate            -> ods.customer_transaction_daily

The Day-2 REFEED execution (6 runs, its own workflow_run_id) REUSES the original
Day-2 customer silver output and re-ingests a CORRECTED transaction file:

  corrected raw transaction -> ingest -> corrected transaction silver
  ORIGINAL customer silver + corrected transaction silver -> merge -> detail sink
  -> aggregate -> aggregate sink

Every control-plane write goes through the sanctioned client wrappers in
``control/`` so the dashboard can be built purely from control tables plus the
stamped target rows. No raw ``cp.*`` inserts in the harness; read-only SELECTs
for the snapshot/tests are fine.
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


DOMAIN = "sales"
CUSTOMER_DATASET = "customer"
TRANSACTION_DATASET = "transaction"
DETAIL_DATASET = "customer_transaction"
AGG_DATASET = "customer_transaction_daily"

SINK_TYPE = "postgres"
DETAIL_TARGET = f"ods.{DETAIL_DATASET}"
AGG_TARGET = f"ods.{AGG_DATASET}"


# --------------------------------------------------------------------------- #
# Business-key helpers (replacement_scope='business_key'; changed-only refeed).
# --------------------------------------------------------------------------- #
def detail_business_key(row: dict[str, Any]) -> str:
    """Per-row business key for the ods.customer_transaction detail target."""
    return f"{row['customer_id']}:{row['transaction_id']}"


def aggregate_business_key(row: dict[str, Any]) -> str:
    """Per-row business key for the ods.customer_transaction_daily aggregate."""
    return f"{row['business_date']}:{row['customer_id']}"

BUSINESS_DATES = [
    dt.date(2026, 5, 28),
    dt.date(2026, 5, 29),
    dt.date(2026, 5, 30),
]
REFEED_BUSINESS_DATE = dt.date(2026, 5, 29)

# Stable customer roster per the spec (§Input Files). Same customers each day.
CUSTOMER_ROWS = [
    {"customer_id": "C001", "customer_name": "Ada Lovelace", "segment": "premium"},
    {"customer_id": "C002", "customer_name": "Grace Hopper", "segment": "standard"},
    {"customer_id": "C003", "customer_name": "Katherine Johnson", "segment": "premium"},
]

# Original transaction file per the spec (§Input Files).
TRANSACTION_ROWS = [
    {"transaction_id": "T100", "customer_id": "C001", "amount": 125.50},
    {"transaction_id": "T101", "customer_id": "C001", "amount": 74.50},
    {"transaction_id": "T102", "customer_id": "C002", "amount": 33.00},
    {"transaction_id": "T103", "customer_id": "C002", "amount": 48.25},
    {"transaction_id": "T104", "customer_id": "C003", "amount": 210.00},
    {"transaction_id": "T105", "customer_id": "C003", "amount": 19.95},
]

# Corrected Day-2 transaction refeed (§"Corrected Transaction Refeed File").
# Same ids/structure, two amounts corrected. Distinct content =>
# distinct file_md5 => distinct raw file identity.
CORRECTED_TRANSACTION_ROWS = [
    {"transaction_id": "T100", "customer_id": "C001", "amount": 125.50},
    {"transaction_id": "T101", "customer_id": "C001", "amount": 79.50},
    {"transaction_id": "T102", "customer_id": "C002", "amount": 33.00},
    {"transaction_id": "T103", "customer_id": "C002", "amount": 48.25},
    {"transaction_id": "T104", "customer_id": "C003", "amount": 225.00},
    {"transaction_id": "T105", "customer_id": "C003", "amount": 19.95},
]


def ensure_demo_targets(conn) -> None:
    """Create the two demo target tables expected by write_link_then_rows.

    Ad-hoc demo tables (spec lines 313-318 / 373-378). NOT a core migration —
    this stays a self-contained demo fixture.
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
                -- output_link naming cleanup (Option B / spec §"Target Row
                -- Columns" option 1): additive new-name mirror of the link id
                -- (= cp.output_link.output_link_id). NO FK — the authoritative
                -- FK stays on _ods_lineage_link_id. write_link_then_rows
                -- best-effort stamps this column when the target table has it.
                _ods_output_link_id UUID
            )
            """
        )
        # Idempotent backfill of the additive column on tables that already exist
        # (CREATE TABLE IF NOT EXISTS would otherwise skip the new column).
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

    DOMAIN-SCOPED (domain = ``sales``): clears ONLY this demo's data so the
    policy/claims (``insurance``) demo can coexist live in the same database.
    The static reference tables (`cp.edge_type`, `cp.dataset_config`) are kept,
    and no global ``TRUNCATE ... CASCADE`` is used.

    Removes (scoped to this domain's run set / domain column): the two demo
    target tables' rows, this domain's target-visibility rows, and the control-
    plane reconciliation/DLQ/lineage-edge/lineage-link/run-stage/run/file rows
    for this domain. Deleted in FK-safe child->parent order. Re-running the demo
    yields the same single set (no duplicates).
    """
    ensure_demo_targets(conn)

    # The two target tables belong entirely to this domain -> clear all rows.
    conn.execute(f"DELETE FROM ods.{DETAIL_DATASET}")
    conn.execute(f"DELETE FROM ods.{AGG_DATASET}")

    # This domain's target-visibility rows.
    conn.execute("DELETE FROM ods.target_visibility WHERE domain = %s", (DOMAIN,))

    # Control-plane rows, FK-safe child->parent order, all scoped to this
    # domain's run set (run_log.domain = DOMAIN). The lineage_link/edge graph is
    # self-contained within one domain's runs.
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
    """Deterministic content hash of a raw file's rows.

    Distinct content (e.g. corrected transaction amounts) yields a distinct md5, so
    the corrected refeed gets a genuinely different raw file identity.
    """
    blob = json.dumps(rows, sort_keys=True, separators=(",", ":"))
    return hashlib.md5(blob.encode("utf-8")).hexdigest()


def _file(dataset: str, business_date: dt.date, rows: list[dict[str, Any]], *,
          raw_suffix: str | None = None) -> dict[str, Any]:
    """Build a raw-file descriptor with a business-date-specific path.

    ``raw_suffix`` distinguishes the corrected refeed path from the original
    Day-2 path so traces are visibly distinguishable.
    """
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
            trigger_type: str, replay_of_run_id: str | None = None,
            commit: bool) -> dict[str, str]:
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
        trigger_type=trigger_type,
        file_id=file_id,
        replay_of_run_id=replay_of_run_id,
        commit=commit,
    )
    with stages.stage_scope(conn, run_id, "ingest_raw", commit=commit) as st:
        st.record_in = n
        st.record_out = n
        st.metrics = {"sample_ids": [r.get("transaction_id") or r.get("customer_id")
                                     for r in rows]}

    link_id = lineage.write_link(
        conn,
        consumer_run_id=run_id,
        edge_type="raw_to_curated",
        target_ref={
            "path": f"s3://bronze/{file['dataset']}/{file['business_date']}.json",
            "content_hash": f"bronze-{file['file_md5']}",
            "version": 1,
        },
        record_count=n,
        edges=[{
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
                            trigger_type: str, commit: bool) -> dict[str, str]:
    """Transform a raw file's curated output into a silver canonical output.

    The upstream is passed EXPLICITLY (ingest run/link of THIS execution) rather
    than discovered, so the refeed binds to its own corrected ingest and a normal
    day binds to its own — no cross-execution ``latest_succeeded_run`` ambiguity.
    """
    n = file["record_count"]
    # content_hash includes the file md5 so the corrected transaction silver has
    # a DIFFERENT target_ref.content_hash than the original (spec line 129).
    run_id = runs.start(
        conn,
        workflow_run_id=workflow_run_id,
        pipeline_type="canonicalization",
        domain=file["domain"],
        dataset=file["dataset"],
        business_date=file["business_date"],
        trigger_type=trigger_type,
        commit=commit,
    )
    with stages.stage_scope(conn, run_id, "canonicalize_to_silver",
                            commit=commit) as st:
        st.record_in = n
        st.record_out = n

    link_id = lineage.write_link(
        conn,
        consumer_run_id=run_id,
        edge_type="curated_to_canonical",
        target_ref={
            "path": f"s3://silver/{file['dataset']}/{file['business_date']}.parquet",
            "content_hash": f"silver-{file['file_md5']}",
            "version": 1,
        },
        record_count=n,
        edges=[{
            "upstream_run_id": ingest_run_id,
            "upstream_lineage_link_id": ingest_link_id,
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
                transaction_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    customers = {row["customer_id"]: row for row in CUSTOMER_ROWS}
    merged = []
    for tx in transaction_rows:
        customer = customers[tx["customer_id"]]
        merged.append({
            "transaction_id": tx["transaction_id"],
            "customer_id": tx["customer_id"],
            "customer_name": customer["customer_name"],
            "segment": customer["segment"],
            "amount": tx["amount"],
            "business_date": str(business_date),
        })
    return merged


def _aggregate_rows(detail_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_customer: dict[str, dict[str, Any]] = {}
    for row in detail_rows:
        bucket = by_customer.setdefault(row["customer_id"], {
            "business_date": row["business_date"],
            "customer_id": row["customer_id"],
            "customer_name": row["customer_name"],
            "transaction_count": 0,
            "total_amount": 0.0,
        })
        bucket["transaction_count"] += 1
        bucket["total_amount"] += float(row["amount"])
    return [
        {**row, "total_amount": round(row["total_amount"], 2)}
        for row in by_customer.values()
    ]


def _changed_rows(original_rows: list[dict[str, Any]],
                  new_rows: list[dict[str, Any]],
                  key_fields: tuple[str, ...]) -> list[dict[str, Any]]:
    """Return rows whose business key is new or whose payload actually changed."""
    original_by_key = {
        tuple(row[field] for field in key_fields): row
        for row in original_rows
    }
    changed = []
    for row in new_rows:
        key = tuple(row[field] for field in key_fields)
        if original_by_key.get(key) != row:
            changed.append(row)
    return changed


def _merge_to_detail(conn, *, workflow_run_id: str, business_date: dt.date,
                     customer_silver: dict[str, str],
                     transaction_silver: dict[str, str],
                     detail_rows: list[dict[str, Any]], content_tag: str,
                     trigger_type: str, commit: bool) -> dict[str, str]:
    """Join customer + transaction silver. TWO upstream merge_to_canonical edges:
    customer silver in slot 0, transaction silver in slot 1. Each edge names the
    EXACT upstream output via upstream_lineage_link_id (provenance to each silver).
    """
    run_id = runs.start(
        conn,
        workflow_run_id=workflow_run_id,
        pipeline_type="merge",
        domain=DOMAIN,
        dataset=DETAIL_DATASET,
        business_date=business_date,
        trigger_type=trigger_type,
        commit=commit,
    )
    with stages.stage_scope(conn, run_id, "merge_customer_transaction",
                            commit=commit) as st:
        st.record_in = len(CUSTOMER_ROWS) + len(detail_rows)
        st.record_out = len(detail_rows)
        st.metrics = {
            "inputs": [CUSTOMER_DATASET, TRANSACTION_DATASET],
            "join_key": "customer_id",
        }

    link_id = lineage.write_link(
        conn,
        consumer_run_id=run_id,
        edge_type="merge_to_canonical",
        target_ref={
            "path": f"s3://silver/{DETAIL_DATASET}/{business_date}.parquet",
            # content_tag differs original vs corrected => distinct content_hash
            # for the corrected merge output (spec line 130).
            "content_hash": f"silver-{DETAIL_DATASET}-{content_tag}",
            "version": 1,
        },
        record_count=len(detail_rows),
        edges=[
            {
                "upstream_run_id": customer_silver["run_id"],
                "upstream_lineage_link_id": customer_silver["link_id"],
                "input_slot": 0,
                "edge_type": "merge_to_canonical",
                "source_ref": {"dataset": CUSTOMER_DATASET, "role": "dimension"},
                "record_count": len(CUSTOMER_ROWS),
            },
            {
                "upstream_run_id": transaction_silver["run_id"],
                "upstream_lineage_link_id": transaction_silver["link_id"],
                "input_slot": 1,
                "edge_type": "merge_to_canonical",
                "source_ref": {"dataset": TRANSACTION_DATASET, "role": "fact"},
                "record_count": len(detail_rows),
            },
        ],
        transform_version="join-v1",
        commit=commit,
    )
    recon.write_check(
        conn, run_id=run_id, check_type="merge_customer_transaction",
        source_count=len(detail_rows), accounted_count=len(detail_rows),
        metrics={"source_records_read": len(CUSTOMER_ROWS) + len(detail_rows)},
        commit=commit)
    runs.finalise(conn, run_id, status="succeeded",
                  record_count_out=len(detail_rows), commit=commit)
    return {"run_id": run_id, "link_id": link_id}


def _sink_rows(conn, *, workflow_run_id: str, business_date: dt.date, dataset: str,
               upstream_run_id: str, upstream_link_id: str,
               upstream_edge_type: str, rows: list[dict[str, Any]],
               content_tag: str, stage_name: str, trigger_type: str,
               commit: bool) -> dict[str, str]:
    """Upsert rows to ods.<dataset> via the sanctioned link-then-rows path.

    write_link_then_rows stamps each target row with _ods_workflow_run_id and
    _ods_lineage_link_id. The canonical_to_sink link's content_hash carries the
    content_tag so a corrected sink link is a DISTINCT output (distinct id +
    content_hash) from the original (spec lines 131-132).
    """
    run_id = runs.start(
        conn,
        workflow_run_id=workflow_run_id,
        pipeline_type="sink",
        domain=DOMAIN,
        dataset=dataset,
        business_date=business_date,
        trigger_type=trigger_type,
        commit=commit,
    )
    with stages.stage_scope(conn, run_id, stage_name, commit=commit) as st:
        st.record_in = len(rows)
        st.record_out = len(rows)

    link_id = lineage.write_link_then_rows(
        conn,
        consumer_run_id=run_id,
        edge_type="canonical_to_sink",
        target_ref={
            "path": f"postgres://ods/{dataset}",
            "content_hash": f"postgres-{dataset}-{content_tag}",
            "version": 1,
        },
        record_count=len(rows),
        edges=[{
            "upstream_run_id": upstream_run_id,
            "upstream_lineage_link_id": upstream_link_id,
            "edge_type": "canonical_to_sink",
            "source_ref": {"upstream_edge_type": upstream_edge_type},
            "record_count": len(rows),
        }],
        rows=rows,
        sink_type="postgres",
        transform_version="postgres-upsert-v1",
        commit=commit,
    )
    # Per-OUTPUT graph-derived recon (P10-C): accounted = rows stamped with THIS
    # link only. Avoids the run-scoped false breach reconcile_sink would raise.
    recon.reconcile_sink_link(conn, lineage_link_id=link_id,
                              source_count=len(rows), commit=commit)
    runs.finalise(conn, run_id, status="succeeded",
                  record_count_out=len(rows), commit=commit)
    return {"run_id": run_id, "link_id": link_id}


def _aggregate_from_detail(conn, *, workflow_run_id: str, business_date: dt.date,
                           detail_inputs: list[dict[str, Any]],
                           aggregate_rows: list[dict[str, Any]],
                           content_tag: str, trigger_type: str,
                           commit: bool) -> dict[str, str]:
    """Aggregate detail SINK output(s) as a first-class detail_to_aggregate run.

    ``detail_inputs`` is one entry per CONTRIBUTING detail output, each
    ``{"run_id", "link_id", "record_count", optional "role"}`` -> one
    detail_to_aggregate input edge (the 021 CHECK requires each edge name its
    exact upstream output via upstream_lineage_link_id). A NORMAL aggregate
    passes its single detail sink. A recomputed REFEED aggregate consumes EVERY
    contributing active detail output (the ORIGINAL Day-2 detail sink for the
    unchanged contributing rows of the affected key PLUS the corrected refeed
    detail sink), so the aggregate's provenance is COMPLETE rather than only the
    corrected slice (F1; mirrors policy_claims_dlq_workflow).
    """
    rows_in = sum(int(di["record_count"]) for di in detail_inputs)
    run_id = runs.start(
        conn,
        workflow_run_id=workflow_run_id,
        pipeline_type="aggregation",
        domain=DOMAIN,
        dataset=AGG_DATASET,
        business_date=business_date,
        trigger_type=trigger_type,
        commit=commit,
    )
    with stages.stage_scope(conn, run_id, "aggregate_customer_daily",
                            commit=commit) as st:
        st.record_in = rows_in
        st.record_out = len(aggregate_rows)
        st.metrics = {"group_by": ["business_date", "customer_id"],
                      "detail_inputs": len(detail_inputs)}

    link_id = lineage.write_link(
        conn,
        consumer_run_id=run_id,
        edge_type="detail_to_aggregate",
        target_ref={
            "path": f"s3://gold/{AGG_DATASET}/{business_date}.parquet",
            "content_hash": f"gold-{AGG_DATASET}-{content_tag}",
            "version": 1,
        },
        record_count=len(aggregate_rows),
        edges=[{
            "upstream_run_id": di["run_id"],
            "upstream_lineage_link_id": di["link_id"],
            "input_slot": i,
            "edge_type": "detail_to_aggregate",
            "source_ref": {"input_role": di.get("role", "detail"),
                           "table": f"ods.{DETAIL_DATASET}"},
            "record_count": int(di["record_count"]),
        } for i, di in enumerate(detail_inputs)],
        transform_version="agg-v1",
        commit=commit,
    )
    recon.write_check(
        conn, run_id=run_id, check_type="aggregate_customer_daily",
        source_count=rows_in, accounted_count=rows_in,
        metrics={"aggregate_rows": len(aggregate_rows)}, commit=commit)
    runs.finalise(conn, run_id, status="succeeded",
                  record_count_out=len(aggregate_rows), commit=commit)
    return {"run_id": run_id, "link_id": link_id}


def _activate_business_keys(conn, *, dataset: str, target_name: str,
                            business_date: dt.date, sink: dict[str, str],
                            workflow_run_id: str, rows: list[dict[str, Any]],
                            key_fn, reason: str | None, commit: bool) -> list[str]:
    """Activate ONE target-visibility row per business key produced by this sink.

    Mirrors policy_claims_workflow._activate_business_keys: each call supersedes
    (status N) only the prior active row for the SAME (domain, dataset,
    business_date, sink_type, target_name, replacement_scope='business_key',
    replacement_key) and inserts the new Y. Unchanged keys are simply never
    passed here, so their original Y is untouched (changed-only refeed).
    """
    visibility_ids = []
    for row in rows:
        vis_id = visibility.activate(
            conn, domain=DOMAIN, dataset=dataset, business_date=business_date,
            sink_type=SINK_TYPE, target_name=target_name, file_id=None,
            output_link_id=sink["link_id"], producer_run_id=sink["run_id"],
            workflow_run_id=workflow_run_id, replacement_scope="business_key",
            replacement_key=key_fn(row), reason=reason, commit=commit)
        visibility_ids.append(vis_id)
    return visibility_ids


# --------------------------------------------------------------------------- #
# Executions.
# --------------------------------------------------------------------------- #
def normal_execution(conn, business_date: dt.date,
                     customer_rows: list[dict[str, Any]],
                     transaction_rows: list[dict[str, Any]], *,
                     workflow_run_id: str, trigger_type: str = "manual",
                     commit: bool) -> dict[str, Any]:
    """Full 8-run normal shape for one (business_date, customer, transaction)."""
    customer_file = _file(CUSTOMER_DATASET, business_date, customer_rows)
    transaction_file = _file(TRANSACTION_DATASET, business_date, transaction_rows)

    customer_ingest = _ingest(
        conn, workflow_run_id=workflow_run_id, file=customer_file,
        trigger_type=trigger_type, commit=commit)
    transaction_ingest = _ingest(
        conn, workflow_run_id=workflow_run_id, file=transaction_file,
        trigger_type=trigger_type, commit=commit)
    customer_silver = _canonicalize_to_silver(
        conn, workflow_run_id=workflow_run_id, file=customer_file,
        ingest_run_id=customer_ingest["run_id"],
        ingest_link_id=customer_ingest["link_id"],
        trigger_type=trigger_type, commit=commit)
    transaction_silver = _canonicalize_to_silver(
        conn, workflow_run_id=workflow_run_id, file=transaction_file,
        ingest_run_id=transaction_ingest["run_id"],
        ingest_link_id=transaction_ingest["link_id"],
        trigger_type=trigger_type, commit=commit)

    detail_rows = _merge_rows(business_date, transaction_rows)
    content_tag = f"{workflow_run_id}-orig"
    merge = _merge_to_detail(
        conn, workflow_run_id=workflow_run_id, business_date=business_date,
        customer_silver=customer_silver, transaction_silver=transaction_silver,
        detail_rows=detail_rows, content_tag=content_tag,
        trigger_type=trigger_type, commit=commit)
    detail_sink = _sink_rows(
        conn, workflow_run_id=workflow_run_id, business_date=business_date,
        dataset=DETAIL_DATASET, upstream_run_id=merge["run_id"],
        upstream_link_id=merge["link_id"], upstream_edge_type="merge_to_canonical",
        rows=detail_rows, content_tag=content_tag,
        stage_name="upsert_customer_transaction", trigger_type=trigger_type,
        commit=commit)
    # F3: after the successful sink + recon-ok, activate ONE visibility row per
    # detail business key (status Y) — write-contract step 9.
    _activate_business_keys(
        conn, dataset=DETAIL_DATASET, target_name=DETAIL_TARGET,
        business_date=business_date, sink=detail_sink,
        workflow_run_id=workflow_run_id, rows=detail_rows,
        key_fn=detail_business_key, reason="normal load", commit=commit)

    aggregate_rows = _aggregate_rows(detail_rows)
    aggregate = _aggregate_from_detail(
        conn, workflow_run_id=workflow_run_id, business_date=business_date,
        detail_inputs=[{"run_id": detail_sink["run_id"],
                        "link_id": detail_sink["link_id"],
                        "record_count": len(detail_rows)}],
        aggregate_rows=aggregate_rows, content_tag=content_tag,
        trigger_type=trigger_type, commit=commit)
    aggregate_sink = _sink_rows(
        conn, workflow_run_id=workflow_run_id, business_date=business_date,
        dataset=AGG_DATASET, upstream_run_id=aggregate["run_id"],
        upstream_link_id=aggregate["link_id"],
        upstream_edge_type="detail_to_aggregate", rows=aggregate_rows,
        content_tag=content_tag, stage_name="upsert_customer_transaction_daily",
        trigger_type=trigger_type, commit=commit)
    _activate_business_keys(
        conn, dataset=AGG_DATASET, target_name=AGG_TARGET,
        business_date=business_date, sink=aggregate_sink,
        workflow_run_id=workflow_run_id, rows=aggregate_rows,
        key_fn=aggregate_business_key, reason="normal load", commit=commit)

    # F6 (fact-spine, migration 031): cross-hop reconciliation on the FACT SPINE.
    # raw_in counts ONLY the 'transaction' FACT raw_to_curated link (the
    # 'customer' DIMENSION is OFF-spine, excluded); sink_out counts ONLY the
    # 'customer_transaction' leaf-detail canonical_to_sink rows (the daily
    # aggregate is OFF-spine, verified per-hop by reconcile_sink_link). So a
    # normal day reconciles ok: fact 6 == leaf-detail 6 + dlq 0. Recorded in
    # reconciliation_log (check_type='workflow').
    recon.reconcile_workflow(
        conn, workflow_run_id=workflow_run_id,
        source_datasets=[TRANSACTION_DATASET], leaf_target=DETAIL_DATASET,
        commit=commit)

    return {
        "workflow_run_id": workflow_run_id,
        "business_date": business_date,
        "execution_type": "normal",
        "files": {"customer": customer_file, "transaction": transaction_file},
        "customer_ingest": customer_ingest,
        "transaction_ingest": transaction_ingest,
        "customer_silver": customer_silver,
        "transaction_silver": transaction_silver,
        "merge": merge,
        "detail_sink": detail_sink,
        "aggregate": aggregate,
        "aggregate_sink": aggregate_sink,
        "detail_rows": detail_rows,
        "aggregate_rows": aggregate_rows,
    }


def refeed_execution(conn, *, original_day2_result: dict[str, Any],
                     corrected_transaction_rows: list[dict[str, Any]],
                     workflow_run_id: str, commit: bool) -> dict[str, Any]:
    """Day-2 corrected-transaction refeed (6 runs, NEW workflow_run_id).

    Per spec lines 296-301 + test 10: REUSE the ORIGINAL Day-2 customer silver
    output (do NOT re-run customer). Re-ingest the corrected transaction (new
    file_id on the new md5; replay_of_run_id = original Day-2 transaction ingest),
    re-canonicalize transaction silver (different content_hash), merge with the
    ORIGINAL customer silver + CORRECTED transaction silver, sink detail,
    aggregate, sink aggregate. trigger_type='replay'.
    """
    business_date = original_day2_result["business_date"]
    corrected_file = _file(
        TRANSACTION_DATASET, business_date, corrected_transaction_rows,
        raw_suffix=f"{business_date}-refeed")

    transaction_ingest = _ingest(
        conn, workflow_run_id=workflow_run_id, file=corrected_file,
        trigger_type="replay",
        replay_of_run_id=original_day2_result["transaction_ingest"]["run_id"],
        commit=commit)
    transaction_silver = _canonicalize_to_silver(
        conn, workflow_run_id=workflow_run_id, file=corrected_file,
        ingest_run_id=transaction_ingest["run_id"],
        ingest_link_id=transaction_ingest["link_id"],
        trigger_type="replay", commit=commit)

    # REUSE original Day-2 customer silver run + link (no re-run of customer).
    customer_silver = original_day2_result["customer_silver"]

    detail_rows = _merge_rows(business_date, corrected_transaction_rows)
    changed_detail_rows = _changed_rows(
        original_day2_result["detail_rows"],
        detail_rows,
        ("business_date", "transaction_id"),
    )
    content_tag = f"{workflow_run_id}-corrected"
    merge = _merge_to_detail(
        conn, workflow_run_id=workflow_run_id, business_date=business_date,
        customer_silver=customer_silver, transaction_silver=transaction_silver,
        detail_rows=detail_rows, content_tag=content_tag,
        trigger_type="replay", commit=commit)
    detail_sink = _sink_rows(
        conn, workflow_run_id=workflow_run_id, business_date=business_date,
        dataset=DETAIL_DATASET, upstream_run_id=merge["run_id"],
        upstream_link_id=merge["link_id"], upstream_edge_type="merge_to_canonical",
        rows=changed_detail_rows, content_tag=content_tag,
        stage_name="upsert_customer_transaction", trigger_type="replay",
        commit=commit)
    # F3 CHANGED-ONLY visibility: supersede only the changed detail business keys
    # (changed -> old N + corrected Y; unchanged keys keep their original Y).
    _activate_business_keys(
        conn, dataset=DETAIL_DATASET, target_name=DETAIL_TARGET,
        business_date=business_date, sink=detail_sink,
        workflow_run_id=workflow_run_id, rows=changed_detail_rows,
        key_fn=detail_business_key, reason="transaction refeed (changed only)",
        commit=commit)

    aggregate_rows = _aggregate_rows(detail_rows)
    changed_aggregate_rows = _changed_rows(
        original_day2_result["aggregate_rows"],
        aggregate_rows,
        ("business_date", "customer_id"),
    )

    # F1 — COMPLETE provenance for the recomputed (changed-only) aggregate. The
    # recomputed aggregate for an affected customer is computed from that
    # customer's FULL current detail set: the CHANGED rows (now in the refeed
    # detail sink) PLUS the UNCHANGED rows of the same customer (still in the
    # ORIGINAL Day-2 detail sink). Each contributing active detail output gets one
    # detail_to_aggregate input edge with its per-upstream contributing
    # record_count, so tracing the refed aggregate reaches ALL its contributing
    # detail outputs -> raw (mirrors policy_claims_dlq_workflow / its test_19).
    affected_customers = {row["customer_id"] for row in changed_aggregate_rows}
    changed_keys = {(r["business_date"], r["transaction_id"])
                    for r in changed_detail_rows}
    original_affected_unchanged = [
        r for r in original_day2_result["detail_rows"]
        if r["customer_id"] in affected_customers
        and (r["business_date"], r["transaction_id"]) not in changed_keys
    ]
    refeed_affected_changed = [
        r for r in changed_detail_rows if r["customer_id"] in affected_customers
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
        content_tag=content_tag, trigger_type="replay", commit=commit)
    aggregate_sink = _sink_rows(
        conn, workflow_run_id=workflow_run_id, business_date=business_date,
        dataset=AGG_DATASET, upstream_run_id=aggregate["run_id"],
        upstream_link_id=aggregate["link_id"],
        upstream_edge_type="detail_to_aggregate", rows=changed_aggregate_rows,
        content_tag=content_tag, stage_name="upsert_customer_transaction_daily",
        trigger_type="replay", commit=commit)
    # F3 CHANGED-ONLY visibility: supersede only the changed aggregate keys.
    _activate_business_keys(
        conn, dataset=AGG_DATASET, target_name=AGG_TARGET,
        business_date=business_date, sink=aggregate_sink,
        workflow_run_id=workflow_run_id, rows=changed_aggregate_rows,
        key_fn=aggregate_business_key, reason="transaction refeed (changed only)",
        commit=commit)

    # NOTE on F6 (fact-spine): refeed reconciles at changed-slice grain via
    # per-output reconcile_sink_link (already gating visibility); whole-fact
    # reconcile_workflow is not applicable to a changed-only slice.

    return {
        "workflow_run_id": workflow_run_id,
        "business_date": business_date,
        "execution_type": "refeed",
        "refeed_of_workflow_run_id": original_day2_result["workflow_run_id"],
        "files": {"transaction": corrected_file},
        "customer_silver": customer_silver,
        "transaction_ingest": transaction_ingest,
        "transaction_silver": transaction_silver,
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
    """Run Day1/2/3 normal + Day2 refeed, each its own workflow_run_id."""
    ensure_demo_targets(conn)

    normals = {}
    for business_date in BUSINESS_DATES:
        wfid = str(uuid.uuid4())
        normals[business_date] = normal_execution(
            conn, business_date, CUSTOMER_ROWS, TRANSACTION_ROWS,
            workflow_run_id=wfid, trigger_type="manual", commit=commit)

    refeed_wfid = str(uuid.uuid4())
    refeed = refeed_execution(
        conn, original_day2_result=normals[REFEED_BUSINESS_DATE],
        corrected_transaction_rows=CORRECTED_TRANSACTION_ROWS,
        workflow_run_id=refeed_wfid, commit=commit)

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
            "description": "Day 2 transaction refeed (corrected T101 and T104)",
        },
    ]

    return {
        # Back-compat: a single workflow_run_id pointer (Day-1 normal).
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

    Thin wrapper over the shared, dataset-agnostic ``export_workflow_snapshot``:
    binds the customer-demo scenario + detail/aggregate tables. The export shape
    (runs/stages, links/edges, files, tables, traces, orchestrator fields) lives
    in ``harness/snapshot.py`` so the policy/claims demo can reuse it verbatim.
    """
    workflow_run_ids = [e["workflow_run_id"] for e in executions_meta]
    return export_workflow_snapshot(
        conn,
        workflow_run_ids=workflow_run_ids,
        detail_tables=[DETAIL_DATASET, AGG_DATASET],
        scenario={
            "business_dates": [str(d) for d in BUSINESS_DATES],
            "refeed_business_date": str(REFEED_BUSINESS_DATE),
        },
        executions=executions_meta,
    )


def write_dashboard_snapshot(path: str | pathlib.Path, *, reset: bool = True) -> dict[str, Any]:
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
    parser = argparse.ArgumentParser(description="Customer-transaction lineage demo.")
    parser.add_argument(
        "--out",
        default="dashboard/data/demo-workflow.json",
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
