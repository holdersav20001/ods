"""pipeline.reconciliation_log operations."""
from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from psycopg2 import sql


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


# dataset -> (current_table, history_table, key_columns, compare_columns, order_column)
CURRENT_HISTORY_TABLES: dict[str, tuple[str, str, tuple[str, ...], tuple[str, ...], str]] = {
    "policies": (
        "ods.insurance_policy",
        "ods.insurance_policy_history",
        ("policy_id",),
        ("status", "premium", "effective_date", "_ods_file_id", "_ods_run_id"),
        "_ods_ingested_at",
    ),
}


def _split_table_ref(table_ref: str) -> tuple[str, str]:
    schema, sep, table = table_ref.partition(".")
    if not sep or not schema.isidentifier() or not table.isidentifier():
        raise ValueError(f"table reference must be schema.table: {table_ref!r}")
    return schema, table


def _safe_column(column: str) -> str:
    if not column.isidentifier():
        raise ValueError(f"illegal column name: {column!r}")
    return column


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
    current_schema, current_name = _split_table_ref(current_table)
    history_schema, history_name = _split_table_ref(history_table)
    run_col = _safe_column(run_col)

    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("SELECT COUNT(*) FROM {}.{} WHERE {} = %s").format(
                sql.Identifier(current_schema),
                sql.Identifier(current_name),
                sql.Identifier(run_col),
            ),
            (run_id,),
        )
        current_count = int(cur.fetchone()[0])
        cur.execute(
            sql.SQL("SELECT COUNT(*) FROM {}.{} WHERE {} = %s").format(
                sql.Identifier(history_schema),
                sql.Identifier(history_name),
                sql.Identifier(run_col),
            ),
            (run_id,),
        )
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


def compare_history_vs_current(
    conn,
    *,
    run_id: str,
    domain: str,
    dataset: str,
    business_date: str | None,
    tables: Mapping[str, tuple[str, str, tuple[str, ...], tuple[str, ...], str]] | None = None,
    sample_limit: int = 20,
    commit: bool = True,
) -> dict[str, Any]:
    """Compare current-state rows to latest matching history rows.

    Count parity can prove both sinks received the same number of rows, but it
    cannot prove the current table holds the same values as the append history
    table's latest row per business key. This check builds a latest-history
    view with ``row_number()`` and compares configured columns using
    ``IS DISTINCT FROM`` so NULLs are handled correctly.
    """
    registry = tables or CURRENT_HISTORY_TABLES
    table_config = registry.get(dataset)
    if not table_config:
        return {"status": "skipped", "reason": f"no current/history pair registered for {dataset}"}

    current_table, history_table, key_columns, compare_columns, order_column = table_config
    current_schema, current_name = _split_table_ref(current_table)
    history_schema, history_name = _split_table_ref(history_table)
    key_columns = tuple(_safe_column(column) for column in key_columns)
    compare_columns = tuple(_safe_column(column) for column in compare_columns)
    order_column = _safe_column(order_column)
    if not key_columns:
        raise ValueError("at least one key column is required")
    if not compare_columns:
        raise ValueError("at least one compare column is required")

    key_join = sql.SQL(" AND ").join(
        sql.SQL("c.{col} = h.{col}").format(col=sql.Identifier(column))
        for column in key_columns
    )
    diff_predicate = sql.SQL(" OR ").join(
        sql.SQL("c.{col} IS DISTINCT FROM h.{col}").format(col=sql.Identifier(column))
        for column in compare_columns
    )
    key_json = sql.SQL(", ").join(
        sql.SQL("{literal}, c.{col}").format(
            literal=sql.Literal(column),
            col=sql.Identifier(column),
        )
        for column in key_columns
    )
    value_json = sql.SQL(", ").join(
        sql.SQL("{literal}, jsonb_build_object('current', c.{col}, 'history', h.{col})").format(
            literal=sql.Literal(column),
            col=sql.Identifier(column),
        )
        for column in compare_columns
    )
    partition_by = sql.SQL(", ").join(sql.Identifier(column) for column in key_columns)
    history_filters = [sql.SQL("{} = %s").format(sql.Identifier("_ods_business_date"))]
    current_filters = [sql.SQL("{} = %s").format(sql.Identifier("_ods_business_date"))]
    params: list[Any] = [business_date, business_date, sample_limit]
    if business_date is None:
        history_filters = [sql.SQL("%s IS NULL")]
        current_filters = [sql.SQL("%s IS NULL")]

    query = sql.SQL(
        """
        WITH latest_history AS (
            SELECT *
              FROM (
                    SELECT h.*,
                           row_number() OVER (
                               PARTITION BY {partition_by}
                               ORDER BY {order_col} DESC NULLS LAST
                           ) AS rn
                      FROM {history_schema}.{history_table} h
                     WHERE {history_where}
                   ) ranked
             WHERE rn = 1
        ),
        current_rows AS (
            SELECT *
              FROM {current_schema}.{current_table} c
             WHERE {current_where}
        ),
        mismatches AS (
            SELECT jsonb_build_object({key_json}) AS key,
                   jsonb_build_object({value_json}) AS differences
              FROM current_rows c
              JOIN latest_history h ON {key_join}
             WHERE {diff_predicate}
        ),
        missing_history AS (
            SELECT jsonb_build_object({key_json}) AS key
              FROM current_rows c
              LEFT JOIN latest_history h ON {key_join}
             WHERE h.{first_key} IS NULL
        ),
        counts AS (
            SELECT
                (SELECT COUNT(*) FROM current_rows) AS current_count,
                (SELECT COUNT(*) FROM latest_history) AS history_count,
                (SELECT COUNT(*) FROM mismatches) AS mismatch_count,
                (SELECT COUNT(*) FROM missing_history) AS missing_history_count
        )
        SELECT counts.current_count,
               counts.history_count,
               counts.mismatch_count,
               counts.missing_history_count,
               COALESCE((
                   SELECT jsonb_agg(sample)
                     FROM (
                           SELECT jsonb_build_object(
                                      'type', 'mismatch',
                                      'key', key,
                                      'differences', differences
                                  ) AS sample
                             FROM mismatches
                            LIMIT %s
                          ) s
               ), '[]'::jsonb) ||
               COALESCE((
                   SELECT jsonb_agg(sample)
                     FROM (
                           SELECT jsonb_build_object(
                                      'type', 'missing_history',
                                      'key', key
                                  ) AS sample
                             FROM missing_history
                            LIMIT %s
                          ) s
               ), '[]'::jsonb) AS samples
          FROM counts
        """
    ).format(
        partition_by=partition_by,
        order_col=sql.Identifier(order_column),
        history_schema=sql.Identifier(history_schema),
        history_table=sql.Identifier(history_name),
        current_schema=sql.Identifier(current_schema),
        current_table=sql.Identifier(current_name),
        history_where=sql.SQL(" AND ").join(history_filters),
        current_where=sql.SQL(" AND ").join(current_filters),
        key_join=key_join,
        diff_predicate=diff_predicate,
        key_json=key_json,
        value_json=value_json,
        first_key=sql.Identifier(key_columns[0]),
    )
    params.append(sample_limit)

    with conn.cursor() as cur:
        cur.execute(query, params)
        current_count, history_count, mismatch_count, missing_history_count, samples = cur.fetchone()

    issue_count = int(mismatch_count) + int(missing_history_count)
    status = "ok" if issue_count == 0 else "failed"
    detail_payload = {
        "current_count": int(current_count),
        "history_count": int(history_count),
        "mismatch_count": int(mismatch_count),
        "missing_history_count": int(missing_history_count),
        "samples": samples,
    }
    write_check(
        conn,
        check_type="current_history_row_value",
        run_id=run_id,
        domain=domain,
        dataset=dataset,
        business_date=business_date,
        source_count=int(history_count),
        postgres_count=int(current_count),
        status=status,
        detail=json.dumps(detail_payload, default=str, sort_keys=True),
        commit=commit,
    )
    return {"status": status, **detail_payload}
