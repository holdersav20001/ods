"""pipeline.run_log operations."""
from __future__ import annotations

import json

from psycopg2 import sql

from ods_pipeline.models import ALLOWED_RUN_FIELDS, TERMINAL_STATUSES


def _json_or_none(value):
    return json.dumps(value) if value is not None else None


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
    parents=None,
) -> None:
    """Insert a new ``run_log`` row with ``status='running'``.

    Duplicate ``run_id`` is allowed only when the supplied identifying
    metadata matches the existing row.  This preserves idempotency for retries
    while surfacing accidental reuse across different pipeline types/files.
    """
    supplied = {
        "pipeline_type": pipeline_type,
        "domain": domain,
        "dataset": dataset,
        "business_date": str(business_date) if business_date is not None else None,
        "file_id": file_id,
        "kafka_topic": kafka_topic,
        "config_version_id": config_version_id,
        "schema_version_id": schema_version_id,
        "parents": _json_or_none(parents),
    }
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
                RETURNING run_id
                """,
                (
                    run_id, pipeline_type, domain, dataset,
                    supplied["business_date"],
                    file_id, kafka_topic, config_version_id, schema_version_id,
                    supplied["parents"],
                ),
            )
            inserted = cur.fetchone()
            if not inserted:
                cur.execute(
                    """
                    SELECT pipeline_type, domain, dataset, business_date::text,
                           file_id::text, kafka_topic, config_version_id,
                           schema_version_id, parents::text
                      FROM pipeline.run_log
                     WHERE run_id=%s
                    """,
                    (run_id,),
                )
                row = cur.fetchone()
                if not row:
                    raise RuntimeError(f"run_log conflict for {run_id} but row not found")
                existing = dict(zip(supplied.keys(), row))
                mismatches = {}
                for key, expected in supplied.items():
                    if expected is None:
                        continue
                    if _normalise(existing.get(key)) != _normalise(expected):
                        mismatches[key] = {
                            "existing": existing.get(key),
                            "requested": expected,
                        }
                if mismatches:
                    raise ValueError(
                        f"run_id {run_id} already exists with different metadata: "
                        f"{mismatches}"
                    )
        conn.commit()
    except Exception:
        conn.rollback()
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
    vals = [
        json.dumps(v) if k == "parents" and v is not None else v
        for k, v in fields.items()
    ]
    set_clause = sql.SQL(", ").join(
        sql.SQL("{}=%s").format(sql.Identifier(c)) for c in cols
    )
    if fields.get("status") in TERMINAL_STATUSES:
        set_clause = sql.SQL("{}, ended_at=COALESCE(ended_at, NOW())").format(set_clause)
    statement = sql.SQL("UPDATE pipeline.run_log SET {sets} WHERE run_id=%s").format(
        sets=set_clause
    )
    try:
        with conn.cursor() as cur:
            cur.execute(statement, vals + [run_id])
        if commit:
            conn.commit()
    except Exception:
        if commit:
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
