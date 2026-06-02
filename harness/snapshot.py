"""Shared workflow snapshot exporter.

Builds the dashboard JSON snapshot from the ODS control tables plus the stamped
target rows for an arbitrary set of workflow executions. Extracted out of
``harness/customer_transaction_workflow.py`` so a SECOND demo (policy/claims) can
reuse the exact same export shape without copy-pasting the query logic.

GENERICITY: the only workflow-specific inputs are the ``workflow_run_ids`` to
export, the list of ``detail_tables`` (the ``ods.<table>`` sinks to dump rows
from), the ``scenario`` dict (verbatim into the snapshot), and the per-execution
metadata (``executions``). Nothing here hardcodes the customer/transaction
dataset names — pass the policy/claims tables and scenario and it just works.

Snapshot shape (unchanged from the original ``export_demo_snapshot``):

  {
    "workflow_run_id": <first execution's id, back-compat single pointer>,
    "generated_at": <iso8601 utc>,
    "scenario": <verbatim scenario dict>,
    "executions": <verbatim executions list>,
    "runs": [ {... run columns ..., orchestrator_*, "stages": [...] }, ... ],
    "links": [ {... link columns ..., "edges": [...] }, ... ],
    "files": [ {... file columns ...}, ... ],
    "tables": { "<table>": [ {row_id, payload, _ods_*}, ... ], ... },
    "traces": { "<lineage_link_id>": [ {hop, edge_type, ...}, ... ], ... },
  }

Every control-plane read here is a read-only SELECT; this module never writes.
"""
from __future__ import annotations

import datetime as dt
import pathlib
import uuid
from typing import Any


# The trace query is shared with the harness/tests; read it once at import time.
_TRACE_SQL = (pathlib.Path(__file__).resolve().parents[1]
              / "control" / "queries" / "trace_row.sql").read_text()


def _rows_as_dicts(cursor) -> list[dict[str, Any]]:
    cols = [desc.name for desc in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]


def _jsonify(value: Any) -> Any:
    if isinstance(value, (uuid.UUID, dt.date, dt.datetime)):
        return str(value)
    if isinstance(value, list):
        return [_jsonify(v) for v in value]
    if isinstance(value, dict):
        return {k: _jsonify(v) for k, v in value.items()}
    return value


def export_workflow_snapshot(
    conn,
    *,
    workflow_run_ids: list[str],
    detail_tables: list[str],
    scenario: dict[str, Any],
    executions: list[dict[str, Any]],
) -> dict[str, Any]:
    """Export control-table + target-row state for the given executions.

    Parameters
    ----------
    conn
        An open DB connection (read-only SELECTs only).
    workflow_run_ids
        The ``cp.run_log.workflow_run_id`` values to include. The FIRST id is
        also surfaced as the back-compat top-level ``workflow_run_id`` pointer.
    detail_tables
        The ``ods.<table>`` sink tables to dump rows from (e.g. the detail and
        aggregate tables). Each becomes a key under ``snapshot["tables"]``.
    scenario
        Workflow-specific scenario metadata, copied verbatim into the snapshot.
    executions
        Per-execution metadata, copied verbatim into the snapshot. Each entry is
        expected to carry a ``workflow_run_id`` key (consistent with
        ``workflow_run_ids``).
    """
    runs_cur = conn.execute(
        """
        SELECT run_id::text, workflow_run_id, pipeline_type, domain, dataset,
               business_date::text, trigger_type, status, record_count_in,
               record_count_out, replay_of_run_id::text,
               started_at::text, finished_at::text,
               orchestrator_type, orchestrator_dag_id, orchestrator_run_id,
               orchestrator_task_id, orchestrator_try_number,
               orchestrator_map_index, orchestrator_url, orchestrator_payload
        FROM cp.run_log
        WHERE workflow_run_id = ANY(%s)
        ORDER BY workflow_run_id, started_at, pipeline_type, dataset
        """,
        (workflow_run_ids,),
    )
    run_rows = _rows_as_dicts(runs_cur)
    run_ids = [row["run_id"] for row in run_rows]

    stages_by_run = {run_id: [] for run_id in run_ids}
    if run_ids:
        for row in _rows_as_dicts(conn.execute(
            """
            SELECT run_id::text, stage, attempt, status, record_count_in,
                   record_count_out, metrics, started_at::text, finished_at::text
            FROM cp.run_stage_log
            WHERE run_id = ANY(%s::uuid[])
            ORDER BY started_at, stage_log_id
            """,
            (run_ids,),
        )):
            stages_by_run[row.pop("run_id")].append(row)

    links = _rows_as_dicts(conn.execute(
        """
        SELECT l.lineage_link_id::text, l.consumer_run_id::text,
               r.workflow_run_id, l.edge_type, l.sink_type, l.target_ref,
               l.transform_version, l.record_count, l.created_at::text
        FROM cp.lineage_link l
        JOIN cp.run_log r ON r.run_id = l.consumer_run_id
        WHERE r.workflow_run_id = ANY(%s)
        ORDER BY r.workflow_run_id, l.created_at, l.edge_type
        """,
        (workflow_run_ids,),
    ))
    link_ids = [row["lineage_link_id"] for row in links]

    edges_by_link = {link_id: [] for link_id in link_ids}
    if link_ids:
        for row in _rows_as_dicts(conn.execute(
            """
            SELECT lineage_link_id::text, lineage_edge_id::text,
                   upstream_run_id::text, upstream_lineage_link_id::text,
                   source_file_id::text, input_slot, edge_type, source_ref,
                   record_count
            FROM cp.lineage_edge
            WHERE lineage_link_id = ANY(%s::uuid[])
            ORDER BY lineage_link_id, input_slot, lineage_edge_id
            """,
            (link_ids,),
        )):
            edges_by_link[row.pop("lineage_link_id")].append(row)

    traces = {}
    for link_id in link_ids:
        traces[link_id] = _rows_as_dicts(
            conn.execute(_TRACE_SQL, {"link_id": link_id}))

    for row in run_rows:
        row["stages"] = stages_by_run.get(row["run_id"], [])
    for row in links:
        row["edges"] = edges_by_link.get(row["lineage_link_id"], [])

    tables = {}
    for table in detail_tables:
        tables[table] = _rows_as_dicts(conn.execute(
            f"""
            SELECT row_id, payload, _ods_workflow_run_id,
                   _ods_lineage_link_id::text,
                   _ods_output_link_id::text
            FROM ods.{table}
            WHERE _ods_workflow_run_id = ANY(%s)
            ORDER BY row_id
            """,
            (workflow_run_ids,),
        ))

    files = _rows_as_dicts(conn.execute(
        """
        SELECT f.file_id::text, f.s3_raw_path, f.file_md5,
               f.business_date::text, f.state, f.domain, f.dataset
        FROM cp.file_catalogue f
        WHERE f.file_id IN (
            SELECT DISTINCT r.file_id FROM cp.run_log r
            WHERE r.workflow_run_id = ANY(%s) AND r.file_id IS NOT NULL
        )
        ORDER BY f.dataset, f.business_date, f.s3_raw_path
        """,
        (workflow_run_ids,),
    ))

    return _jsonify({
        # Back-compat single pointer = the first (e.g. Day-1 normal) execution.
        "workflow_run_id": workflow_run_ids[0],
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "scenario": scenario,
        "executions": executions,
        "runs": run_rows,
        "links": links,
        "files": files,
        "tables": tables,
        "traces": traces,
    })
