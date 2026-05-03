"""Two-phase watermark store for api_pull.

The api_pull control-plane separates ``pending`` and ``committed`` cursors:

  1. read_committed()        return the cursor to issue the next poll from
  2. lock(run_id)            advisory-style lock so concurrent DAG runs do
                             not double-poll the same dataset
  3. record_pending()        after S3 archive succeeds, stage the new cursor
                             alongside the run_id that owns it
  4. promote()               called by dag_api_pull's post-trigger sensor
                             once the downstream dag_ingest run succeeds
  5. clear_pending()         called when the downstream run failed or was
                             aborted; committed cursor is left untouched

The split guarantees no records are dropped if dag_ingest fails after the
poll succeeds: the same window is re-issued on the next schedule.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass
class WatermarkRow:
    domain: str
    dataset: str
    source_application: str
    cursor_type: str
    committed_cursor_value: str | None
    pending_cursor_value: str | None
    pending_run_id: str | None
    last_successful_run_id: str | None
    locked: bool


class WatermarkStore:
    """Thin DAO over ``pipeline.api_pull_watermark``.

    All methods take an open psycopg2 connection. Each call opens its own
    short-lived cursor and commits — these are control-plane writes that
    must not piggy-back on the caller's data transaction.
    """

    def __init__(self, conn):
        self._conn = conn

    # ------------------------------------------------------------------ read

    def read(
        self,
        *,
        domain: str,
        dataset: str,
        source_application: str,
        cursor_type: str,
    ) -> WatermarkRow:
        """Return the current row, inserting a fresh one with NULL cursors
        if none exists for this (domain, dataset, source_application)."""
        with self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline.api_pull_watermark
                    (domain, dataset, source_application, cursor_type)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (domain, dataset, source_application) DO NOTHING
                """,
                (domain, dataset, source_application, cursor_type),
            )
            cur.execute(
                """
                SELECT cursor_type, committed_cursor_value, pending_cursor_value,
                       pending_run_id::text, last_successful_run_id::text,
                       locked_at IS NOT NULL
                  FROM pipeline.api_pull_watermark
                 WHERE domain=%s AND dataset=%s AND source_application=%s
                """,
                (domain, dataset, source_application),
            )
            row = cur.fetchone()
        self._conn.commit()
        existing_type, committed, pending, pending_run, last_run, locked = row
        return WatermarkRow(
            domain=domain,
            dataset=dataset,
            source_application=source_application,
            cursor_type=existing_type,
            committed_cursor_value=committed,
            pending_cursor_value=pending,
            pending_run_id=pending_run,
            last_successful_run_id=last_run,
            locked=bool(locked),
        )

    # ------------------------------------------------------------------ lock

    def try_lock(
        self,
        *,
        domain: str,
        dataset: str,
        source_application: str,
        run_id: str,
    ) -> bool:
        """Set ``locked_at=NOW()`` only if currently NULL.

        Returns True if the caller now holds the lock for this dataset.
        Caller must release via ``unlock`` even on failure.
        """
        with self._conn.cursor() as cur:
            cur.execute(
                """
                UPDATE pipeline.api_pull_watermark
                   SET locked_at=NOW(),
                       pending_run_id=%s,
                       updated_at=NOW()
                 WHERE domain=%s AND dataset=%s AND source_application=%s
                   AND locked_at IS NULL
                """,
                (run_id, domain, dataset, source_application),
            )
            acquired = cur.rowcount == 1
        self._conn.commit()
        return acquired

    def unlock(
        self,
        *,
        domain: str,
        dataset: str,
        source_application: str,
    ) -> None:
        """Release the dataset lock. Pending cursor is left untouched —
        promote/clear_pending owns that lifecycle."""
        with self._conn.cursor() as cur:
            cur.execute(
                """
                UPDATE pipeline.api_pull_watermark
                   SET locked_at=NULL,
                       updated_at=NOW()
                 WHERE domain=%s AND dataset=%s AND source_application=%s
                """,
                (domain, dataset, source_application),
            )
        self._conn.commit()

    # --------------------------------------------------------------- pending

    def record_pending(
        self,
        *,
        domain: str,
        dataset: str,
        source_application: str,
        run_id: str,
        new_cursor_value: str,
    ) -> None:
        """Stage a new cursor against ``run_id``.

        Called only after the S3 archive write succeeded. promote() will
        move this value into ``committed_cursor_value`` once dag_ingest
        for ``run_id`` finishes successfully.
        """
        with self._conn.cursor() as cur:
            cur.execute(
                """
                UPDATE pipeline.api_pull_watermark
                   SET pending_cursor_value=%s,
                       pending_run_id=%s,
                       updated_at=NOW()
                 WHERE domain=%s AND dataset=%s AND source_application=%s
                """,
                (new_cursor_value, run_id, domain, dataset, source_application),
            )
        self._conn.commit()

    def promote(
        self,
        *,
        domain: str,
        dataset: str,
        source_application: str,
        run_id: str,
    ) -> bool:
        """Move pending -> committed iff ``run_id`` still owns the pending
        cursor. Returns True if a row was promoted."""
        with self._conn.cursor() as cur:
            cur.execute(
                """
                UPDATE pipeline.api_pull_watermark
                   SET committed_cursor_value=pending_cursor_value,
                       last_successful_run_id=%s,
                       pending_cursor_value=NULL,
                       pending_run_id=NULL,
                       updated_at=NOW()
                 WHERE domain=%s AND dataset=%s AND source_application=%s
                   AND pending_run_id::text=%s
                """,
                (run_id, domain, dataset, source_application, run_id),
            )
            promoted = cur.rowcount == 1
        self._conn.commit()
        return promoted

    def clear_pending(
        self,
        *,
        domain: str,
        dataset: str,
        source_application: str,
        run_id: str,
    ) -> None:
        """Discard the pending cursor without touching committed.

        Called when dag_ingest failed for ``run_id``. The next poll re-issues
        the same window, so records are preserved.
        """
        with self._conn.cursor() as cur:
            cur.execute(
                """
                UPDATE pipeline.api_pull_watermark
                   SET pending_cursor_value=NULL,
                       pending_run_id=NULL,
                       updated_at=NOW()
                 WHERE domain=%s AND dataset=%s AND source_application=%s
                   AND pending_run_id::text=%s
                """,
                (domain, dataset, source_application, run_id),
            )
        self._conn.commit()
