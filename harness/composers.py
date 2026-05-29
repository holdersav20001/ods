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
    """Drive a single raw file through the ingest -> canonicalize pipeline.

    Mints ONE workflow_run_id and threads it to BOTH hops. The canonicalize hop
    discovers its upstream ingest run via latest_succeeded_run (it is NOT handed
    an upstream id here — the composer passes only the slice key + counts).

    Returns the nested per-hop results PLUS the ingest hop's top-level keys
    (run_id/file_id/link_id) for backward compatibility with the GATE-A tests:
        {workflow_run_id, ingest: {...}, canonicalize: {...},
         run_id, file_id, link_id}
    where the top-level run_id/file_id/link_id are the INGEST hop's (the A-tests
    assert against the curated link reaching the raw file).
    """
    workflow_run_id = str(uuid.uuid4())
    ingest = fakes.fake_ingest(
        conn, workflow_run_id=workflow_run_id, file=file, commit=commit)
    canonicalize = fakes.fake_canonicalize(
        conn,
        workflow_run_id=workflow_run_id,
        domain=file["domain"],
        dataset=file["dataset"],
        business_date=file["business_date"],
        record_count=file["record_count"],
        commit=commit,
    )
    return {
        "workflow_run_id": workflow_run_id,
        "ingest": ingest,
        "canonicalize": canonicalize,
        **ingest,  # backward-compat: ingest's run_id/file_id/link_id at top level
    }
