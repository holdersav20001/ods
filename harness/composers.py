"""Workflow composers: orchestrate fake stages under ONE workflow_run_id.

The composer is the single owner of the connection and of the workflow_run_id.
It mints exactly one workflow_run_id (the only sanctioned mint outside
dlq.replay) and threads it to every stage it drives. As later phases (P3b-d)
add canonicalize / merge / sink hops, they are appended here under the SAME
workflow_run_id.
"""
import uuid

from . import fakes


def run_single_file(conn, *, file, commit=True) -> dict:
    """Drive a single raw file through the (currently ingest-only) pipeline.

    Mints ONE workflow_run_id and returns {workflow_run_id, **stage_results}.
    """
    workflow_run_id = str(uuid.uuid4())
    ingest = fakes.fake_ingest(
        conn, workflow_run_id=workflow_run_id, file=file, commit=commit)
    return {"workflow_run_id": workflow_run_id, **ingest}
