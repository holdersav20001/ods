"""pipeline.reconciliation_log operations."""
from __future__ import annotations

from collections.abc import Mapping
from typing import Any


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
    commit: bool = True,
) -> None:
    """Insert a row into ``pipeline.reconciliation_log``.

    Computes ``discrepancy_count`` and ``discrepancy_pct`` automatically:
      * If *source_count* and *kafka_count* both supplied:
        ``discrepancy = kafka_count - source_count``
      * If *kafka_count* and *postgres_count* both supplied:
        ``discrepancy = postgres_count - kafka_count``

    ``commit``: when True (default), the helper commits its own transaction.
    When False, the caller owns the surrounding tx (used by atomic
    ``record_result`` flow — B4).
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
                    check_type, run_id, domain, dataset,
                    None if business_date is None else str(business_date),
                    window_start, window_end,
                    source_count, kafka_count, postgres_count,
                    discrepancy, pct, status, detail,
                ),
            )
        if commit:
            conn.commit()
    except Exception:
        if commit:
            conn.rollback()
        raise


# Default dual-sink table pairs per dataset.
# Each entry: dataset -> (current_table, history_table, run_id_column).
DUAL_SINK_TABLES: dict[str, tuple[str, str, str]] = {
    "policies": ("ods.insurance_policy",
                 "ods.insurance_policy_history",
                 "_ods_run_id"),
}


def check_dual_sink_parity(
    conn,
    *,
    run_id: str,
    domain: str,
    dataset: str,
    business_date: str | None,
    pairs: Mapping[str, tuple[str, str, str]] | None = None,
    commit: bool = True,
) -> dict[str, Any]:
    """Compare row counts between current and history sinks for ``run_id``.

    Returns the reconciliation summary and writes a corresponding row to
    ``pipeline.reconciliation_log`` with ``check_type='dual_sink_parity'``.

    History sink lag is the most common source of divergence between the
    upsert (current) and append (history) JDBC sinks reading from the same
    canonical topic. If the history connector is paused, restart-lagging,
    or schema-evolution-blocked, the current state can advance without a
    matching history row, leaving the audit trail incomplete.

    Status:
      - ``ok``        — counts match (delta == 0)
      - ``pending``   — at least one sink has no rows yet (likely lag)
      - ``failed``    — both sinks have rows but counts diverge
    """
    pairs_to_check = pairs or DUAL_SINK_TABLES
    table_pair = pairs_to_check.get(dataset)
    if not table_pair:
        return {"status": "skipped", "reason": f"no dual-sink pair registered for {dataset}"}
    current_table, history_table, run_col = table_pair

    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {current_table} WHERE {run_col} = %s", (run_id,))
        current_count = int(cur.fetchone()[0])
        cur.execute(f"SELECT COUNT(*) FROM {history_table} WHERE {run_col} = %s", (run_id,))
        history_count = int(cur.fetchone()[0])

    delta = history_count - current_count
    if current_count == 0 or history_count == 0:
        status = "pending"
        detail = (f"current={current_count}, history={history_count}; "
                  "one or both sinks empty — likely consumer lag")
    elif delta == 0:
        status = "ok"
        detail = f"current={current_count}, history={history_count}"
    else:
        status = "failed"
        detail = (f"current={current_count}, history={history_count}, "
                  f"delta={delta}")

    write_check(
        conn,
        check_type="dual_sink_parity",
        run_id=run_id,
        domain=domain,
        dataset=dataset,
        business_date=business_date,
        source_count=current_count,
        postgres_count=history_count,
        status=status,
        detail=detail,
        commit=commit,
    )
    return {
        "status": status,
        "current_count": current_count,
        "history_count": history_count,
        "delta": delta,
        "detail": detail,
    }
