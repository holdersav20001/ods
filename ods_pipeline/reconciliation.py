"""pipeline.reconciliation_log operations."""
from __future__ import annotations


def write_check(
    conn,
    *,
    check_type: str,
    run_id: str,
    domain: str,
    dataset: str,
    business_date: str,
    source_count: int | None = None,
    kafka_count: int | None = None,
    postgres_count: int | None = None,
    status: str,
    detail: str | None = None,
    window_start=None,
    window_end=None,
) -> None:
    """Insert a row into ``pipeline.reconciliation_log``.

    Computes ``discrepancy_count`` and ``discrepancy_pct`` automatically:
      * If *source_count* and *kafka_count* both supplied:
        ``discrepancy = kafka_count - source_count``
      * If *kafka_count* and *postgres_count* both supplied:
        ``discrepancy = postgres_count - kafka_count``
    """
    discrepancy: int | None = None
    if source_count is not None and kafka_count is not None:
        discrepancy = (kafka_count or 0) - (source_count or 0)
    elif kafka_count is not None and postgres_count is not None:
        discrepancy = (postgres_count or 0) - (kafka_count or 0)

    pct: float | None = None
    if discrepancy is not None and source_count:
        pct = round(100.0 * discrepancy / source_count, 4)

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline.reconciliation_log
                    (check_type, run_id, domain, dataset, business_date,
                     window_start, window_end,
                     source_count, kafka_count, postgres_count,
                     discrepancy_count, discrepancy_pct, status, detail)
                VALUES (%s,%s,%s,%s,%s, %s,%s, %s,%s,%s, %s,%s,%s,%s)
                """,
                (
                    check_type, run_id, domain, dataset, str(business_date),
                    window_start, window_end,
                    source_count, kafka_count, postgres_count,
                    discrepancy, pct, status, detail,
                ),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
