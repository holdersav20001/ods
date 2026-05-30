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


def reconcile_sink(conn, *, run_id, source_count, commit=True) -> None:
    """Graph-derived sink reconciliation (audit F7).

    Unlike write_check — which compares two CALLER-supplied numbers and so cannot
    catch real row loss — this derives `accounted` from the ACTUAL ods.<dataset>
    rows stamped with a canonical_to_sink link of this run. The caller supplies
    only `source_count` (what the upstream is believed to have produced); if the
    sink actually wrote fewer rows, the DB-derived count is smaller and recon
    BREACHES. Writes a reconciliation_log row with check_type='sink_graph' and
    metrics.graph_derived=true. Must be called AFTER the rows are written
    (write_link_then_rows), in the SAME transaction."""
    conn.execute("SELECT cp.reconcile_sink(%s,%s)", [run_id, source_count])
    if commit:
        conn.commit()
