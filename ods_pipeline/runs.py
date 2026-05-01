"""pipeline.run_log operations."""
from __future__ import annotations

import json

from ods_pipeline.models import ALLOWED_RUN_FIELDS, TERMINAL_STATUSES


def start(
    conn,
    *,
    run_id: str,
    pipeline_type: str,
    domain: str,
    dataset: str,
    business_date: str,
    file_id: str | None = None,
    kafka_topic: str | None = None,
    config_version_id=None,
    schema_version_id=None,
    parents=None,
) -> None:
    """Insert a new ``run_log`` row with ``status='running'``.

    Silently no-ops on duplicate ``run_id`` (ON CONFLICT DO NOTHING).
    """
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline.run_log
                    (run_id, pipeline_type, domain, dataset, business_date,
                     file_id, status, kafka_topic, config_version_id,
                     schema_version_id, parents)
                VALUES (%s,%s,%s,%s,%s, %s,'running',%s,%s,%s,%s)
                ON CONFLICT (run_id) DO NOTHING
                """,
                (
                    run_id, pipeline_type, domain, dataset, str(business_date),
                    file_id, kafka_topic, config_version_id, schema_version_id,
                    json.dumps(parents) if parents else None,
                ),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def update(conn, run_id: str, **fields) -> None:
    """Update arbitrary ``run_log`` fields for *run_id*.

    Terminal status (succeeded / failed / partial) automatically sets
    ``ended_at = COALESCE(ended_at, NOW())``.

    Raises ``ValueError`` for unknown field names.
    """
    if not fields:
        return
    invalid = set(fields) - ALLOWED_RUN_FIELDS
    if invalid:
        raise ValueError(f"Unknown run_log fields: {sorted(invalid)}")
    cols = list(fields.keys())
    vals = [
        json.dumps(v) if k == "parents" and v is not None else v
        for k, v in fields.items()
    ]
    sets = ", ".join(f"{c}=%s" for c in cols)
    if fields.get("status") in TERMINAL_STATUSES:
        sets += ", ended_at=COALESCE(ended_at, NOW())"
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE pipeline.run_log SET {sets} WHERE run_id=%s",
                vals + [run_id],
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise


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
