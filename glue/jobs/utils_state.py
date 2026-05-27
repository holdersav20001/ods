# glue/jobs/utils_state.py
"""File-state control-plane writes (pipeline.file_processing_attempt)."""

from utils_bootstrap import *  # noqa: F401,F403  ensure ods_pipeline on sys.path

import ods_pipeline


def set_file_state(conn, s3_path: str, run_id: str, status: str, **extra) -> None:
    ods_pipeline.files.set_state(
        conn,
        s3_path,
        run_id,
        status,
        record_count=extra.get("record_count"),
        error_reason=extra.get("error_reason"),
    )


def get_file_state(conn, s3_path: str) -> str | None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT status FROM pipeline.file_processing_attempt WHERE s3_path = %s",
            (s3_path,),
        )
        row = cur.fetchone()
    return row[0] if row else None
