"""Run-linkage helpers for api_pull <-> dag_ingest.

The api_pull control-plane needs to look up the EXACT dag_ingest parent
run launched by a given api_pull poll, not "the latest s3_batch run for
this file_id" or "any run carrying our edge". Two scenarios drove the
two-layer match below:

  1. A replay of the same ``file_id`` with a different api_pull poll
     would match a "latest by file_id" lookup. Fixed by requiring the
     ``triggered_by_api_pull`` edge in ``run_log.orchestrators``.
  2. TriggerDagRunOperator retries, manual re-triggers, or bugs could
     produce TWO rows that both carry the ``triggered_by_api_pull``
     edge for the same ``api_pull_run_id``. A "latest by edge" lookup
     could then promote/clear the wrong cursor. Fixed by requiring the
     caller (dag_api_pull) to pre-mint a deterministic
     ``expected_parent_run_id`` and matching it as the run_log primary
     key. PK uniqueness eliminates ambiguity by definition. The edge
     check stays as defence-in-depth.

If the caller cannot supply ``expected_parent_run_id`` (legacy / test
paths), the lookup falls back to JSONB containment but returns ``None``
when more than one row matches — "ambiguous, do not promote".

Lives outside the Airflow DAG module so plain pytest (no Airflow
runtime) can exercise the SQL contract.
"""
from __future__ import annotations

import json
import uuid

TRIGGERED_BY_API_PULL_EDGE = "triggered_by_api_pull"


def derive_dag_ingest_parent_run_id(api_pull_run_id: str) -> str:
    """Compute the deterministic ``run_id`` that dag_ingest.init_run will
    use as its parent run when triggered by this api_pull poll.

    Both ends derive the same value from the same api_pull_run_id so
    no extra column or coordination state is needed. UUID5 namespacing
    keeps it stable across processes / restarts and collision-free with
    randomly-minted UUIDs.
    """
    return str(
        uuid.uuid5(uuid.NAMESPACE_OID, f"api_pull:{api_pull_run_id}")
    )


def ingest_status_for_api_pull_run(
    conn,
    api_pull_run_id: str,
    *,
    expected_parent_run_id: str | None = None,
) -> str | None:
    """Return the status of the exact dag_ingest parent run launched by
    THIS api_pull poll, or ``None`` if no unambiguous match exists.

    Behaviour:

      * If ``expected_parent_run_id`` is supplied, look up by PK and
        verify the ``triggered_by_api_pull`` edge is present in
        ``run_log.orchestrators``. Returns the status iff both match.
      * Otherwise, fall back to JSONB containment on the edge alone.
        If MORE THAN ONE row matches, return ``None`` — the lookup is
        ambiguous and the caller (finalise_watermark) MUST NOT promote
        or clear the watermark on an ambiguous result.
    """
    needle = json.dumps([{
        "run_id": api_pull_run_id,
        "edge_type": TRIGGERED_BY_API_PULL_EDGE,
    }])
    with conn.cursor() as cur:
        if expected_parent_run_id:
            # Exact-PK match. PK uniqueness in run_log makes the result
            # at most one row; the edge check rejects same-PK rows that
            # somehow lack the api_pull linkage.
            cur.execute(
                """
                SELECT status
                  FROM pipeline.run_log
                 WHERE run_id = %s::uuid
                   AND pipeline_type='orchestration'
                   AND orchestrators @> %s::jsonb
                """,
                (expected_parent_run_id, needle),
            )
            row = cur.fetchone()
            return row[0] if row else None

        # Fallback: edge-only lookup. Pull at most 2 rows so we can
        # detect ambiguity without scanning the full table.
        cur.execute(
            """
            SELECT status
              FROM pipeline.run_log
             WHERE pipeline_type='orchestration'
               AND orchestrators @> %s::jsonb
             ORDER BY started_at DESC NULLS LAST, run_id::text DESC
             LIMIT 2
            """,
            (needle,),
        )
        rows = cur.fetchall()
    if not rows:
        return None
    if len(rows) > 1:
        return None  # ambiguous — caller must NOT promote/clear
    return rows[0][0]
