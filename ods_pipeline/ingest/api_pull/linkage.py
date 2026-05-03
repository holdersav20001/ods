"""Run-linkage helpers for api_pull <-> dag_ingest.

The api_pull control-plane needs to look up the EXACT dag_ingest parent
run launched by a given api_pull poll, not "the latest s3_batch run for
this file_id". A replay of the same file_id, or a concurrent dag_ingest
run, must not be observable here — otherwise finalise_watermark could
promote or clear the wrong cursor.

The linkage is a JSONB containment check on ``run_log.parents`` against
the explicit ``triggered_by_api_pull`` edge written by
``dag_ingest.init_run`` when the trigger conf carries
``triggered_by_run_id``.

Lives outside the Airflow DAG module so plain pytest (no Airflow
runtime) can exercise the SQL contract.
"""
from __future__ import annotations

import json


TRIGGERED_BY_API_PULL_EDGE = "triggered_by_api_pull"


def ingest_status_for_api_pull_run(conn, api_pull_run_id: str) -> str | None:
    """Return the status of the dag_ingest parent run launched by the
    given api_pull poll, or None if no matching run has been recorded.

    Match is by JSONB containment on ``pipeline.run_log.parents`` against
    ``[{"run_id": api_pull_run_id, "edge_type": "triggered_by_api_pull"}]``.
    """
    needle = json.dumps([{
        "run_id": api_pull_run_id,
        "edge_type": TRIGGERED_BY_API_PULL_EDGE,
    }])
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT status
              FROM pipeline.run_log
             WHERE pipeline_type = 's3_batch'
               AND parents @> %s::jsonb
             ORDER BY started_at DESC NULLS LAST, run_id::text DESC
             LIMIT 1
            """,
            (needle,),
        )
        row = cur.fetchone()
    return row[0] if row else None
