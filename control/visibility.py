"""Target-visibility / active-slice wrapper over cp.activate_target_visibility.

Business truth (which output is active now) vs lineage (audit truth). This thin
typed wrapper activates one output for a business slice: it deactivates the prior
active slice row and activates the corrected one (supersession), refusing to
activate non-succeeded runs or reconciliation breaches (spec §7/§8).

See docs/specs/2026-05-30-target-visibility-active-slice.md.
"""


def activate(conn, *, domain, dataset, business_date, sink_type, target_name,
             file_id, lineage_link_id, producer_run_id, workflow_run_id,
             replacement_scope="slice", replacement_key=None, reason=None,
             commit=True) -> str:
    """Activate the visibility row for a sink output and return its visibility_id.

    Idempotent for the same lineage_link_id (returns the existing active row).
    RAISES (psycopg.errors.RaiseException) when the producer run is not
    'succeeded' or graph-derived reconciliation for the link/run is not 'ok'.
    """
    visibility_id = conn.execute(
        "SELECT cp.activate_target_visibility("
        "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        [domain, dataset, business_date, sink_type, target_name,
         file_id, lineage_link_id, producer_run_id, workflow_run_id,
         replacement_scope, replacement_key, reason],
    ).fetchone()[0]
    if commit:
        conn.commit()
    return str(visibility_id)
