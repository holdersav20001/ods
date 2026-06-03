"""Target-visibility / active-slice wrapper over cp.activate_target_visibility.

Business truth (which output is active now) vs lineage (audit truth). This thin
typed wrapper activates one output for a business slice: it deactivates the prior
active slice row and activates the corrected one (supersession), refusing to
activate non-succeeded runs or reconciliation breaches (spec §7/§8).

See docs/specs/2026-05-30-target-visibility-active-slice.md.
"""


def activate(conn, *, domain, dataset, business_date, sink_type, target_name,
             file_id, output_link_id=None, lineage_link_id=None,
             producer_run_id, workflow_run_id,
             replacement_scope="slice", replacement_key=None, reason=None,
             supersede=True, commit=True) -> str:
    """Activate the visibility row for a sink output and return its visibility_id.

    Identify the output to activate with the PREFERRED new-name kwarg
    ``output_link_id`` (= cp.output_link.output_link_id; spec §"API / Wrapper
    Changes"). The OLD kwarg ``lineage_link_id`` is still accepted as an alias for
    backward compatibility — supply exactly one. The underlying SQL
    cp.activate_target_visibility keeps its physical parameter
    ``p_lineage_link_id`` (Option B — Python-forward rename); this wrapper maps
    output_link_id -> that parameter.

    ``replacement_scope`` selects the refeed REPLACEMENT POLICY (spec area 4;
    docs/reference/refeed-replacement-policy.md):

      * ``business_key`` — supersede only the prior Y for THIS scope/key (the
        per-key changed-only refeed; replacement_key = the business key).
      * ``slice`` — supersede the WHOLE (domain,dataset,business_date) slice:
        ALL currently-active rows for that slice/target go N regardless of their
        own replacement_key, then the slice's Y is inserted.
      * ``file`` — supersede only the prior row for the same source file
        (replacement_key = the source file identity).
      * ``append_only`` — pass ``supersede=False`` to ADD a new Y WITHOUT
        deactivating any prior (use a per-append-unique replacement_key so the
        partial-unique active index is not violated).

    ``supersede`` (default True) maps to the SQL ``p_supersede`` parameter; set
    it False for append_only.

    Idempotent for the same output (returns the existing active row). RAISES
    (psycopg.errors.RaiseException) when the producer run is not 'succeeded' or
    graph-derived reconciliation for the link/run is not 'ok'.
    """
    if output_link_id is not None and lineage_link_id is not None:
        if str(output_link_id) != str(lineage_link_id):
            raise ValueError(
                "activate(): output_link_id and lineage_link_id both given with "
                "different values; pass only output_link_id")
    link_id = output_link_id if output_link_id is not None else lineage_link_id
    if link_id is None:
        raise ValueError("activate(): output_link_id is required")
    visibility_id = conn.execute(
        "SELECT cp.activate_target_visibility("
        "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        [domain, dataset, business_date, sink_type, target_name,
         file_id, link_id, producer_run_id, workflow_run_id,
         replacement_scope, replacement_key, reason, supersede],
    ).fetchone()[0]
    if commit:
        conn.commit()
    return str(visibility_id)
