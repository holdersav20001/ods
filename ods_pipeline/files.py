"""pipeline.file_catalogue and pipeline.file_state operations."""
from __future__ import annotations

import uuid as _uuid


def upsert(
    conn,
    *,
    domain: str,
    dataset: str,
    business_date: str,
    file_md5: str,
    s3_raw_path: str | None = None,
    sftp_path: str | None = None,
    s3_curated_path: str | None = None,
    file_size_bytes: int | None = None,
    source_row_count: int | None = None,
    state: str = "ingested",
    last_run_id: str | None = None,
) -> str:
    """Upsert ``file_catalogue`` keyed on ``(domain, dataset, s3_raw_path)``.

    Returns the canonical ``file_id`` UUID string for this landed raw file.
    ``file_md5`` remains a content fingerprint; it is not the identity because
    different files can legitimately have identical content.
    """
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline.file_catalogue
                    (file_id, domain, dataset, business_date, file_md5,
                     s3_raw_path, sftp_path, s3_curated_path,
                     file_size_bytes, source_row_count, state, last_run_id)
                VALUES (%s,%s,%s,%s,%s, %s,%s,%s, %s,%s,%s,%s)
                ON CONFLICT (domain, dataset, s3_raw_path)
                WHERE s3_raw_path IS NOT NULL
                DO UPDATE SET
                    state           = EXCLUDED.state,
                    business_date   = EXCLUDED.business_date,
                    file_md5        = EXCLUDED.file_md5,
                    s3_raw_path     = COALESCE(EXCLUDED.s3_raw_path,
                                               pipeline.file_catalogue.s3_raw_path),
                    s3_curated_path = COALESCE(EXCLUDED.s3_curated_path,
                                               pipeline.file_catalogue.s3_curated_path),
                    sftp_path       = COALESCE(EXCLUDED.sftp_path,
                                               pipeline.file_catalogue.sftp_path),
                    file_size_bytes = COALESCE(EXCLUDED.file_size_bytes,
                                               pipeline.file_catalogue.file_size_bytes),
                    source_row_count= COALESCE(EXCLUDED.source_row_count,
                                               pipeline.file_catalogue.source_row_count),
                    last_run_id     = EXCLUDED.last_run_id,
                    state_updated_at= NOW()
                RETURNING file_id
                """,
                (
                    str(_uuid.uuid4()), domain, dataset, str(business_date), file_md5,
                    s3_raw_path, sftp_path, s3_curated_path,
                    file_size_bytes, source_row_count, state, last_run_id,
                ),
            )
            file_id = str(cur.fetchone()[0])
        conn.commit()
        return file_id
    except Exception:
        conn.rollback()
        raise


def update_catalogue(conn, file_id: str, **fields) -> None:
    """Update arbitrary columns on a ``file_catalogue`` row by ``file_id``."""
    if not fields:
        return
    sets = ", ".join(f"{c}=%s" for c in fields.keys())
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE pipeline.file_catalogue SET {sets}, state_updated_at=NOW()"
                f" WHERE file_id=%s",
                list(fields.values()) + [file_id],
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def set_state(
    conn,
    s3_path: str,
    run_id: str,
    status: str,
    *,
    record_count: int | None = None,
    error_reason: str | None = None,
) -> None:
    """Upsert ``pipeline.file_state`` for *s3_path*."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline.file_state
                    (s3_path, run_id, status, record_count, error_reason)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (s3_path) DO UPDATE SET
                    run_id       = EXCLUDED.run_id,
                    status       = EXCLUDED.status,
                    record_count = EXCLUDED.record_count,
                    error_reason = EXCLUDED.error_reason,
                    updated_at   = NOW()
                """,
                (s3_path, run_id, status, record_count, error_reason),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def get_state(conn, s3_path: str) -> str | None:
    """Return the current status string for *s3_path*, or ``None`` if not found."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT status FROM pipeline.file_state WHERE s3_path = %s",
            (s3_path,),
        )
        row = cur.fetchone()
    return row[0] if row else None
