"""Dead-letter-queue wrappers over cp.quarantine, plus replay.

cp.quarantine writes a dlq row AND a 'quarantine' lineage link+edge atomically.
"""
import uuid

from psycopg.types.json import Jsonb

from . import runs


def quarantine(conn, *, run_id, stage, reason, source_ref, payload_ref,
               record_count, commit=True) -> str:
    dlq_id = conn.execute(
        "SELECT cp.quarantine(%s,%s,%s,%s,%s,%s)",
        [run_id, stage, reason, Jsonb(source_ref), payload_ref, record_count],
    ).fetchone()[0]
    if commit:
        conn.commit()
    return str(dlq_id)


def replay(conn, *, original_run_id, pipeline_type, domain, dataset,
           business_date, commit=True):
    """Mint a NEW workflow_run_id and start a fresh run that re-drives the
    pipeline, with trigger_type='replay' and replay_of_run_id=original_run_id.

    This is the ONLY place in the client that mints a workflow_run_id (a new
    execution). Returns (new_workflow_run_id, new_run_id).

    Phase 2 scope: mint + start_run only. The full re-write chain (re-reading
    DLQ payloads and re-running stages) is Phase 3d and is intentionally not
    built here.
    """
    new_workflow_run_id = str(uuid.uuid4())
    new_run_id = runs.start(
        conn,
        workflow_run_id=new_workflow_run_id,
        pipeline_type=pipeline_type,
        domain=domain,
        dataset=dataset,
        business_date=business_date,
        trigger_type="replay",
        replay_of_run_id=original_run_id,
        commit=commit,
    )
    return new_workflow_run_id, new_run_id
