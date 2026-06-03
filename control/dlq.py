"""Dead-letter-queue wrappers over cp.quarantine, plus replay.

cp.quarantine writes a dlq row AND a 'quarantine' lineage link+edge atomically.
"""
import uuid

from psycopg.types.json import Jsonb

from . import runs


def quarantine(conn, *, run_id, stage, reason, source_ref, payload_ref,
               record_count, failed_payload=None, source_file_id=None,
               commit=True) -> str:
    """Quarantine a failed batch: writes a cp.dlq row (status='open'), a
    first-class 'quarantine' output_link + edge, and stamps the dlq row with the
    quarantine_output_link_id. ``failed_payload`` is the actual rejected row(s),
    preserved verbatim and never overwritten.

    ``source_file_id`` is the raw file the quarantined rows came from. The SQL
    function remains backward-compatible for legacy direct SQL callers, but the
    Python SDK requires this anchor so normal application code cannot create a
    DLQ output that dead-ends before the raw file.
    """
    if not source_file_id:
        raise ValueError(
            "source_file_id is required for dlq.quarantine; pass the raw "
            "cp.file_catalogue.file_id so quarantine lineage traces to raw")

    dlq_id = conn.execute(
        "SELECT cp.quarantine(%s,%s,%s,%s,%s,%s,%s,%s)",
        [run_id, stage, reason, Jsonb(source_ref), payload_ref, record_count,
         Jsonb(failed_payload) if failed_payload is not None else None,
         source_file_id],
    ).fetchone()[0]
    if commit:
        conn.commit()
    return str(dlq_id)


def resolve(conn, *, dlq_id, status, resolved_by_run_id=None,
            resolved_by_output_link_id=None, commit=True) -> None:
    """Flip a DLQ row's lifecycle status and record resolution refs. Never
    touches failed_payload/reason — failure history is preserved. ``status`` must
    be one of open/under_review/corrected/replayed/resolved/rejected.
    """
    conn.execute(
        "SELECT cp.resolve_dlq(%s,%s,%s,%s)",
        [dlq_id, status, resolved_by_run_id, resolved_by_output_link_id],
    )
    if commit:
        conn.commit()


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
