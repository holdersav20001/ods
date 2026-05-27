"""pipeline.run_log operations."""
from __future__ import annotations

import ods_ingestion_control as control
from ods_pipeline.models import ALLOWED_RUN_FIELDS


def _normalise(value):
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def start(
    conn,
    *,
    run_id: str,
    pipeline_type: str,
    domain: str,
    dataset: str,
    business_date: str | None,
    file_id: str | None = None,
    kafka_topic: str | None = None,
    config_version_id=None,
    schema_version_id=None,
    orchestrators=None,
    runtime_context=None,
) -> None:
    """Insert a new ``run_log`` row with ``status='running'``.

    Duplicate ``run_id`` is allowed only when the supplied identifying
    metadata matches the existing row.  This preserves idempotency for retries
    while surfacing accidental reuse across different pipeline types/files.

    ``orchestrators``: JSONB array describing which run(s) scheduled this one
    (e.g. ``[{"run_id": route_run_id, "edge_type": "orchestrates"}]``). This
    is orchestration lineage, NOT data lineage — data parents belong in
    ``pipeline.lineage_edge``.
    """
    try:
        control.start_run(
            conn,
            run_id=run_id,
            pipeline_type=pipeline_type,
            domain=domain,
            dataset=dataset,
            business_date=str(business_date) if business_date is not None else None,
            file_id=file_id,
            kafka_topic=kafka_topic,
            config_version_id=config_version_id,
            schema_version_id=schema_version_id,
            orchestrators=orchestrators,
            runtime_context=runtime_context,
        )
    except Exception as exc:
        if "already exists with different metadata" in str(exc):
            raise ValueError(str(exc)) from exc
        raise


def update(conn, run_id: str, *, commit: bool = True, **fields) -> None:
    """Update arbitrary ``run_log`` fields for *run_id*.

    Terminal status (succeeded / failed / partial) automatically sets
    ``ended_at = COALESCE(ended_at, NOW())``.

    Raises ``ValueError`` for unknown field names.

    ``commit``: when True (default), the helper commits its own transaction —
    behaviour preserved for all existing callers.  When False, the caller is
    responsible for the surrounding transaction (used by
    ``ods_pipeline.messages.record_result`` for atomic multi-write flows).
    """
    if not fields:
        return
    invalid = set(fields) - ALLOWED_RUN_FIELDS
    if invalid:
        raise ValueError(f"Unknown run_log fields: {sorted(invalid)}")
    cols = list(fields.keys())
    # Defence in depth: every key MUST be a bare identifier *and* whitelisted.
    # ALLOWED_RUN_FIELDS is the authoritative gate; isidentifier() is a belt-
    # and-braces guard against future whitelist additions that contain unsafe
    # characters by mistake.
    for c in cols:
        if not (isinstance(c, str) and c.isidentifier() and c in ALLOWED_RUN_FIELDS):
            raise ValueError(f"Illegal run_log field name: {c!r}")
    control.patch_run(conn, run_id=run_id, fields=fields, commit=commit)


def finish(
    conn,
    run_id: str,
    status: str,
    error_summary: str | None = None,
) -> None:
    """Convenience wrapper: mark a run terminal and optionally record error."""
    kw: dict = {"status": status}
    if error_summary is not None:
        kw["error_summary"] = error_summary
    update(conn, run_id, **kw)


class LineageInvariantError(RuntimeError):
    """Raised by ``finalise`` when run state violates the lineage contract."""


def finalise(conn, run_id: str, *, commit: bool = True) -> None:
    """Validate lineage closure invariants before marking a run succeeded.

    Asserts:
      1. If ``record_count_published > 0``, at least one ``lineage_edge`` row
         exists with ``consumer_run_id = run_id`` (no orphan published runs).
      2. No non-terminal ``run_stage_log`` rows exist for ``run_id`` — every
         opened stage must have been closed.

    On violation, marks the run ``failed`` with an explanatory
    ``error_summary`` and raises :class:`LineageInvariantError`.

    Closes architectural risk A3.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COALESCE(record_count_published, 0)
              FROM pipeline.run_log
             WHERE run_id = %s
            """,
            (run_id,),
        )
        row = cur.fetchone()
        if row is None:
            raise LineageInvariantError(f"run_id {run_id} not found")
        published = int(row[0] or 0)

        cur.execute(
            """
            SELECT COUNT(*) FROM pipeline.lineage_edge
             WHERE consumer_run_id = %s
            """,
            (run_id,),
        )
        edges = int(cur.fetchone()[0])

        cur.execute(
            """
            SELECT stage, attempt_number FROM pipeline.run_stage_log
             WHERE run_id = %s
               AND event_type NOT IN ('stage_completed','stage_failed',
                                      'stage_skipped','stage_warned')
            """,
            (run_id,),
        )
        open_stages = cur.fetchall()

    failures: list[str] = []
    if published > 0 and edges == 0:
        failures.append(
            f"published={published} but no lineage_edge rows; orphaned run"
        )
    if open_stages:
        names = ", ".join(f"{s}#{a}" for s, a in open_stages)
        failures.append(f"non-terminal stages remain: {names}")

    if failures:
        summary = "lineage invariant violated: " + " | ".join(failures)
        update(conn, run_id, status="failed",
               error_summary=summary, commit=commit)
        raise LineageInvariantError(summary)
