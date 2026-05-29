"""Run-lifecycle wrappers over cp.start_run, cp.patch_run, cp.latest_succeeded_run.

INVARIANT: workflow_run_id is always supplied BY THE CALLER (the Phase-3
composer mints it once and threads it to every stage). This module never mints
one — see dlq.replay for the single sanctioned mint of a fresh execution id.
"""
from psycopg.types.json import Jsonb

# Sentinel so callers can set error explicitly to None without it being treated
# as "leave unchanged" (the SQL whitelist only patches `error` when the key is
# present in the jsonb).
_UNSET = object()


def register_file(conn, *, s3_raw_path, file_md5, business_date, domain, dataset,
                  commit=True) -> str:
    """Thin wrapper over cp.register_file (idempotent on (file_md5, business_date)).

    Lives here because file registration is the first step of the run/file
    lifecycle that the ingest hop drives.
    """
    file_id = conn.execute(
        "SELECT cp.register_file(%s,%s,%s,%s,%s)",
        [s3_raw_path, file_md5, business_date, domain, dataset],
    ).fetchone()[0]
    if commit:
        conn.commit()
    return str(file_id)


def start(conn, *, workflow_run_id, pipeline_type, domain, dataset, business_date,
          trigger_type, file_id=None, replay_of_run_id=None, commit=True) -> str:
    run_id = conn.execute(
        "SELECT cp.start_run(%s,%s,%s,%s,%s,%s,%s,%s)",
        [workflow_run_id, pipeline_type, domain, dataset, business_date,
         trigger_type, file_id, replay_of_run_id],
    ).fetchone()[0]
    if commit:
        conn.commit()
    return str(run_id)


def patch(conn, run_id, *, status=None, record_count_in=None,
          record_count_out=None, error=_UNSET, commit=True) -> None:
    """Patch a run. Only non-None kwargs are sent; `error` uses a sentinel so it
    can be set explicitly (including to None) only when the caller passes it."""
    p = {}
    if status is not None:
        p["status"] = status
    if record_count_in is not None:
        p["record_count_in"] = record_count_in
    if record_count_out is not None:
        p["record_count_out"] = record_count_out
    if error is not _UNSET:
        p["error"] = error
    conn.execute("SELECT cp.patch_run(%s, %s)", [run_id, Jsonb(p)])
    if commit:
        conn.commit()


def finalise(conn, run_id, *, status, record_count_out=None, commit=True) -> None:
    """Convenience over patch for a terminal status (stamps finished_at in SQL)."""
    patch(conn, run_id, status=status, record_count_out=record_count_out,
          commit=commit)


def latest_succeeded_run(conn, *, domain, dataset, business_date, pipeline_type):
    run_id = conn.execute(
        "SELECT cp.latest_succeeded_run(%s,%s,%s,%s)",
        [domain, dataset, business_date, pipeline_type],
    ).fetchone()[0]
    return str(run_id) if run_id is not None else None


def succeeded_runs(conn, *, domain, dataset, business_date, pipeline_type) -> list[str]:
    """ALL succeeded runs for the slice, newest-first (the merge-hop discovery
    primitive). Where latest_succeeded_run returns the single newest run, this
    returns every succeeded run for (domain, dataset, business_date,
    pipeline_type) so the 1:N merge hop can fan in across N upstreams.

    Pure read (matches latest_succeeded_run: no commit needed for a SELECT)."""
    rows = conn.execute(
        "SELECT cp.succeeded_runs(%s,%s,%s,%s)",
        [domain, dataset, business_date, pipeline_type],
    ).fetchall()
    return [str(r[0]) for r in rows]
