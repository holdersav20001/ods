"""Reconciliation wrapper over cp.write_reconciliation_check.

The SQL computes discrepancy and status (ok / breach / double_count) from the
source vs accounted counts.
"""
from psycopg.types.json import Jsonb


def write_check(conn, *, run_id, check_type, source_count, accounted_count,
                metrics=None, commit=True) -> None:
    conn.execute(
        "SELECT cp.write_reconciliation_check(%s,%s,%s,%s,%s)",
        [run_id, check_type, source_count, accounted_count,
         Jsonb(metrics) if metrics is not None else None],
    )
    if commit:
        conn.commit()
