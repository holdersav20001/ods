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


def reconcile_sink_link(conn, *, lineage_link_id, source_count, commit=True) -> None:
    """Per-OUTPUT graph-derived sink reconciliation (P10-C / THEME E, Codex P4).

    Where reconcile_sink is RUN-scoped (and so false-double-counts a legitimate
    Decision-#6 fan-out where one run writes K canonical_to_sink links for the
    SAME upstream rows), this scopes `accounted` to ONE link: the count of
    ods.<dataset> rows stamped with THIS lineage_link_id only. Each fan-out output
    therefore reconciles independently. Writes a reconciliation_log row with
    check_type='sink_link', run_id = the link's consumer_run_id, and
    metrics.graph_derived=true. Must be called AFTER the rows are written, in the
    SAME transaction."""
    conn.execute("SELECT cp.reconcile_sink_link(%s,%s)",
                 [lineage_link_id, source_count])
    if commit:
        conn.commit()


def reconcile_workflow(conn, *, workflow_run_id, commit=True) -> None:
    """End-to-end CROSS-HOP reconciliation (P10-C / THEME F, A4-S5).

    Per-run sink recon is blind to a wholly-failed upstream that silently drops
    rows: each run's recon is self-consistent. This compares what ENTERED the
    workflow (raw_in = SUM of raw_to_curated link record_counts over the
    workflow's runs) to what LEFT it (accounted = actual sink rows across the
    workflow's datasets + dlq record_counts). A genuine cross-hop loss BREACHES.
    Writes a reconciliation_log row with check_type='workflow', run_id = the
    workflow's terminal run, and metrics carrying raw_in/sink_out/dlq_out and the
    workflow_run_id."""
    conn.execute("SELECT cp.reconcile_workflow(%s)", [workflow_run_id])
    if commit:
        conn.commit()
