"""dag_recon_t2 - hourly cross-plane reconciliation.

Append/history tables are reconciled by immutable file/run counts.
Upsert/current-state tables are reconciled by comparing the current table with
the latest history row per business key. Current-state tables are not stable
per-file count targets because later incrementals legitimately supersede
earlier rows.
"""
from __future__ import annotations

import os

import pendulum
import psycopg2
import psycopg2.extras
from airflow import DAG
from airflow.decorators import task
from psycopg2 import sql

PG_DSN = os.environ.get(
    "PIPELINE_PG_DSN",
    "host=postgres port=5432 dbname=ods_dev user=ods password=ods",
)
LOOKBACK_HOURS = int(os.environ.get("RECON_LOOKBACK_HOURS", "24"))


def _split_table(table_ref: str | None) -> tuple[str, str] | None:
    if not table_ref:
        return None
    schema, sep, table = table_ref.partition(".")
    if not sep or not schema or not table:
        return None
    return schema, table


def _table_exists(conn, schema: str, table: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema=%s AND table_name=%s
            )
            """,
            (schema, table),
        )
        return bool(cur.fetchone()[0])


def _columns(conn, schema: str, table: str) -> set[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema=%s AND table_name=%s
            """,
            (schema, table),
        )
        return {row[0] for row in cur.fetchall()}


def _insert_recon(
    conn,
    *,
    check_type,
    run_id,
    domain,
    dataset,
    business_date,
    source_count,
    kafka_count,
    postgres_count,
    discrepancy,
    status,
    detail,
):
    pct = (abs(discrepancy) / max(int(source_count or 0), 1)) * 100
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline.reconciliation_log (
                check_type, run_id, domain, dataset, business_date,
                source_count, kafka_count, postgres_count,
                discrepancy_count, discrepancy_pct, status, detail
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                check_type,
                run_id,
                domain,
                dataset,
                business_date,
                source_count,
                kafka_count,
                postgres_count,
                discrepancy,
                round(pct, 4),
                status,
                detail,
            ),
        )


def _status(discrepancy: int, source_count: int | None, tol_rec, tol_pct) -> str:
    pct = (abs(discrepancy) / max(int(source_count or 0), 1)) * 100
    if abs(discrepancy) > int(tol_rec or 0) or pct > float(tol_pct or 0):
        return "failed"
    return "passed"


def _history_table_for(conn, schema: str, table: str) -> tuple[str, str] | None:
    candidate = f"{table}_history"
    if _table_exists(conn, schema, candidate):
        return schema, candidate
    return None


def _accepted_count(row) -> int:
    # DQ pass is the count that should land once DLQ/hard failures exist.
    return int(
        row["record_count_dq_pass"]
        if row["record_count_dq_pass"] is not None
        else row["record_count_source"] or 0
    )


def _is_source_run(row) -> bool:
    return row["pipeline_type"] in ("ingestion", "s3_batch", "canonicalize", "message_api")


def _key_fields(row) -> list[str]:
    fields = row["key_fields"] or []
    if isinstance(fields, dict):
        fields = fields.get("fields", [])
    return [str(field) for field in fields if field]


def _file_count_where(cols: set[str], run_id, file_id, business_date):
    if "_ods_file_id" in cols and file_id:
        where = sql.SQL("_ods_file_id = %s")
        params = [str(file_id)]
        detail = f"file_id={file_id}"
    else:
        where = sql.SQL("_ods_run_id = %s")
        params = [str(run_id)]
        detail = f"run_id={run_id}"
    if "_ods_business_date" in cols and business_date:
        where = sql.SQL("{} AND _ods_business_date = %s").format(where)
        params.append(str(business_date))
    return where, params, detail


def _count_landed(conn, schema: str, table: str, where, params) -> int:
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("SELECT count(*) FROM {}.{} WHERE ").format(
                sql.Identifier(schema),
                sql.Identifier(table),
            )
            + where,
            params,
        )
        return int(cur.fetchone()[0])


def _reconcile_append_file_count(conn, row, target_schema: str, target_table: str) -> None:
    accepted = _accepted_count(row)
    cols = _columns(conn, target_schema, target_table)
    where, params, identity_detail = _file_count_where(
        cols, row["run_id"], row["file_id"], row["business_date"]
    )
    landed = _count_landed(conn, target_schema, target_table, where, params)
    discrepancy = accepted - landed
    status = _status(discrepancy, accepted, row["tol_rec"], row["tol_pct"])
    _insert_recon(
        conn,
        check_type="t2_append_file_count",
        run_id=row["run_id"],
        domain=row["domain"],
        dataset=row["dataset"],
        business_date=row["business_date"],
        source_count=accepted,
        kafka_count=None,
        postgres_count=landed,
        discrepancy=discrepancy,
        status=status,
        detail=f"accepted={accepted} landed={landed} {identity_detail}",
    )


def _reconcile_history_file_count(conn, row, history_schema: str, history_table: str) -> None:
    accepted = _accepted_count(row)
    cols = _columns(conn, history_schema, history_table)
    where, params, identity_detail = _file_count_where(
        cols, row["run_id"], row["file_id"], row["business_date"]
    )
    history_count = _count_landed(conn, history_schema, history_table, where, params)
    discrepancy = accepted - history_count
    status = _status(discrepancy, accepted, row["tol_rec"], row["tol_pct"])
    _insert_recon(
        conn,
        check_type="t2_history_file_count",
        run_id=row["run_id"],
        domain=row["domain"],
        dataset=row["dataset"],
        business_date=row["business_date"],
        source_count=accepted,
        kafka_count=None,
        postgres_count=history_count,
        discrepancy=discrepancy,
        status=status,
        detail=f"accepted={accepted} history={history_count} {identity_detail}",
    )


def _compare_columns(row, current_cols: set[str], history_cols: set[str]) -> list[str]:
    key_fields = _key_fields(row)
    schema_fields = (row["schema_def"] or {}).get("fields", [])
    compare_cols = [
        field.get("name")
        for field in schema_fields
        if field.get("name")
        and field.get("name") not in key_fields
        and field.get("name") in current_cols
        and field.get("name") in history_cols
    ]
    if compare_cols:
        return compare_cols
    return [
        c
        for c in sorted(current_cols & history_cols)
        if not c.startswith("_ods_") and c not in key_fields
    ]


def _reconcile_current_consistency(
    conn,
    row,
    target_schema: str,
    target_table: str,
    history_schema: str,
    history_table: str,
) -> None:
    key_fields = _key_fields(row)
    if not key_fields:
        return

    current_cols = _columns(conn, target_schema, target_table)
    history_cols = _columns(conn, history_schema, history_table)
    compare_cols = _compare_columns(row, current_cols, history_cols)

    order_cols = [
        c for c in ("_ods_ingested_at", "_ods_business_date", "_ods_run_id")
        if c in history_cols
    ]
    if order_cols:
        order_expr = sql.SQL(", ").join(
            sql.SQL("h.{} DESC NULLS LAST").format(sql.Identifier(c))
            for c in order_cols
        )
    else:
        order_expr = sql.SQL("1")

    key_select = sql.SQL(", ").join(sql.Identifier(k) for k in key_fields)
    key_join = sql.SQL(" AND ").join(
        sql.SQL("c.{} IS NOT DISTINCT FROM h.{}").format(
            sql.Identifier(k), sql.Identifier(k)
        )
        for k in key_fields
    )
    mismatch_expr = (
        sql.SQL(" OR ").join(
            sql.SQL("c.{} IS DISTINCT FROM h.{}").format(
                sql.Identifier(c), sql.Identifier(c)
            )
            for c in compare_cols
        )
        if compare_cols
        else sql.SQL("FALSE")
    )

    query = sql.SQL(
        """
        WITH latest_history AS (
            SELECT DISTINCT ON ({key_select}) h.*
            FROM {history_schema}.{history_table} h
            ORDER BY {key_select}, {order_expr}
        ),
        compared AS (
            SELECT
                COUNT(*) FILTER (WHERE c.{first_key} IS NULL) AS missing_current,
                COUNT(*) FILTER (
                    WHERE c.{first_key} IS NOT NULL AND ({mismatch_expr})
                ) AS mismatched_current,
                COUNT(*) AS latest_history_count
            FROM latest_history h
            LEFT JOIN {target_schema}.{target_table} c
              ON {key_join}
        ),
        extras AS (
            SELECT COUNT(*) AS extra_current
            FROM {target_schema}.{target_table} c
            LEFT JOIN latest_history h
              ON {key_join}
            WHERE h.{first_key} IS NULL
        )
        SELECT compared.latest_history_count,
               compared.missing_current,
               compared.mismatched_current,
               extras.extra_current
        FROM compared CROSS JOIN extras
        """
    ).format(
        key_select=key_select,
        order_expr=order_expr,
        history_schema=sql.Identifier(history_schema),
        history_table=sql.Identifier(history_table),
        target_schema=sql.Identifier(target_schema),
        target_table=sql.Identifier(target_table),
        first_key=sql.Identifier(key_fields[0]),
        mismatch_expr=mismatch_expr,
        key_join=key_join,
    )

    with conn.cursor() as cur:
        cur.execute(query)
        latest_count, missing, mismatched, extra = [
            int(v or 0) for v in cur.fetchone()
        ]

    discrepancy = missing + mismatched + extra
    status = _status(discrepancy, latest_count, row["tol_rec"], row["tol_pct"])
    _insert_recon(
        conn,
        check_type="t2_current_latest_consistency",
        run_id=row["run_id"],
        domain=row["domain"],
        dataset=row["dataset"],
        business_date=row["business_date"],
        source_count=latest_count,
        kafka_count=None,
        postgres_count=latest_count - missing - mismatched,
        discrepancy=discrepancy,
        status=status,
        detail=(
            f"latest_history={latest_count} missing_current={missing} "
            f"mismatched_current={mismatched} extra_current={extra} "
            f"history_table={history_schema}.{history_table}"
        ),
    )


def _record_current_history_missing(conn, row, target_schema: str, target_table: str) -> None:
    _insert_recon(
        conn,
        check_type="t2_current_history_missing",
        run_id=row["run_id"],
        domain=row["domain"],
        dataset=row["dataset"],
        business_date=row["business_date"],
        source_count=_accepted_count(row),
        kafka_count=None,
        postgres_count=None,
        discrepancy=0,
        status="skipped",
        detail=(
            f"{target_schema}.{target_table} is write_mode=upsert but no "
            f"{target_table}_history table exists; current-state counts are not "
            "a valid per-run/file reconciliation target"
        ),
    )


@task
def reconcile() -> None:
    conn = psycopg2.connect(PG_DSN)
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT r.run_id, r.pipeline_type, r.domain, r.dataset, r.business_date,
                       r.file_id, r.record_count_source, r.record_count_dq_pass,
                       d.postgres_target_table, d.write_mode, d.key_fields, d.schema_def,
                       COALESCE(d.recon_tolerance_records, 0) AS tol_rec,
                       COALESCE(d.recon_tolerance_pct, 0) AS tol_pct
                  FROM pipeline.run_log r
                  JOIN pipeline.dataset_config d
                    ON d.domain = r.domain AND d.dataset = r.dataset
                 WHERE r.status = 'succeeded'
                   AND r.started_at > NOW() - (%s || ' hours')::interval
                   AND d.postgres_target_table IS NOT NULL
                 ORDER BY r.started_at DESC
                """,
                (str(LOOKBACK_HOURS),),
            )
            runs = cur.fetchall()

        current_checked: set[tuple[str, str]] = set()
        for row in runs:
            target = _split_table(row["postgres_target_table"])
            if not target:
                continue
            target_schema, target_table = target
            if not _table_exists(conn, target_schema, target_table):
                continue

            write_mode = row["write_mode"]
            if write_mode == "append":
                if _is_source_run(row):
                    _reconcile_append_file_count(conn, row, target_schema, target_table)
            elif write_mode == "upsert":
                history = _history_table_for(conn, target_schema, target_table)
                if _is_source_run(row) and history:
                    _reconcile_history_file_count(conn, row, history[0], history[1])
                elif _is_source_run(row) and not history:
                    _record_current_history_missing(conn, row, target_schema, target_table)

                dataset_key = (row["domain"], row["dataset"])
                if _is_source_run(row) and history and dataset_key not in current_checked:
                    _reconcile_current_consistency(
                        conn, row, target_schema, target_table, history[0], history[1],
                    )
                    current_checked.add(dataset_key)
            elif write_mode == "replace":
                # Replace/merge targets need dataset-specific lineage semantics.
                # Avoid noisy per-run counts against current-state outputs.
                continue
            conn.commit()
    finally:
        conn.close()


with DAG(
    dag_id="dag_recon_t2",
    start_date=pendulum.datetime(2026, 4, 28, tz="UTC"),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["ods", "reconciliation"],
):
    reconcile()
