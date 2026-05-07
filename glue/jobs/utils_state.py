# glue/jobs/utils_state.py
"""File-state control-plane writes (pipeline.file_state)."""


def set_file_state(conn, s3_path: str, run_id: str, status: str, **extra) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline.file_state (s3_path, run_id, status, record_count, error_reason)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (s3_path) DO UPDATE
              SET run_id=EXCLUDED.run_id,
                  status=EXCLUDED.status,
                  record_count=EXCLUDED.record_count,
                  error_reason=EXCLUDED.error_reason,
                  updated_at=NOW()
            """,
            (
                s3_path,
                run_id,
                status,
                extra.get("record_count"),
                extra.get("error_reason"),
            ),
        )
    conn.commit()


def get_file_state(conn, s3_path: str) -> str | None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT status FROM pipeline.file_state WHERE s3_path = %s",
            (s3_path,),
        )
        row = cur.fetchone()
    return row[0] if row else None
