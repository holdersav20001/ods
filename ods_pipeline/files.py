"""pipeline.file_catalogue and pipeline.file_processing_attempt operations."""
from __future__ import annotations

import ods_ingestion_control as control


def upsert(
    conn,
    *,
    file_id: str | None = None,
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
    return control.register_file(
        conn,
        domain=domain,
        dataset=dataset,
        business_date=str(business_date),
        file_md5=file_md5,
        s3_raw_path=s3_raw_path,
        sftp_path=sftp_path,
        s3_curated_path=s3_curated_path,
        file_size_bytes=file_size_bytes,
        source_row_count=source_row_count,
        state=state,
        last_run_id=last_run_id,
        file_id=file_id,
    )


def update_catalogue(conn, file_id: str, **fields) -> None:
    """Update arbitrary columns on a ``file_catalogue`` row by ``file_id``."""
    if not fields:
        return
    allowed = {
        "state",
        "s3_curated_path",
        "source_row_count",
        "last_run_id",
    }
    invalid = set(fields) - allowed
    if invalid:
        raise ValueError(f"Unknown file_catalogue fields: {sorted(invalid)}")
    control.update_file_catalogue(conn, file_id=file_id, **fields)


def set_state(
    conn,
    s3_path: str,
    run_id: str,
    status: str,
    *,
    record_count: int | None = None,
    error_reason: str | None = None,
) -> None:
    """Upsert ``pipeline.file_processing_attempt`` for *s3_path*."""
    control.set_file_state(
        conn,
        s3_path=s3_path,
        run_id=run_id,
        status=status,
        record_count=record_count,
        error_reason=error_reason,
    )


def get_state(conn, s3_path: str) -> str | None:
    """Return the current status string for *s3_path*, or ``None`` if not found."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT status FROM pipeline.file_processing_attempt WHERE s3_path = %s",
            (s3_path,),
        )
        row = cur.fetchone()
    return row[0] if row else None
