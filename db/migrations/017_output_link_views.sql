-- 017_output_link_views.sql — naming cleanup, OPTION B (compatibility views).
--
-- Spec: docs/specs/2026-05-30-output-link-input-edge-rename.md (§"Option B",
--       §"Suggested Implementation Order" step 1).
--
-- PURPOSE
--   The control-plane data model is correct, but the physical names confuse
--   developers:
--     * cp.lineage_link is not a generic "link" — it is ONE PRODUCED OUTPUT.
--     * cp.lineage_edge is the relationship naming WHICH INPUT(S) produced it.
--     * upstream_lineage_link_id points to a PREVIOUS OUTPUT, not a previous edge.
--
--   Human-readable model:
--     output_link            = what a run produced
--     input_edge             = what that output was made from
--     upstream_output_link_id = a previous output used as input
--
--   This migration introduces the new HUMAN NAMES as read-only compatibility
--   VIEWS over the unchanged physical tables. OPTION B is purely additive:
--     * NO physical table is renamed.
--     * NO physical column is renamed.
--     * NO existing function, view, index, FK, constraint, test, or the demo
--       changes behaviour. Every old name keeps working verbatim.
--
--   New code, the new Python wrappers (control.lineage.write_output_link /
--   write_output_then_rows), the dashboard labels, and the new tests read these
--   views by their new column names. The physical tables remain
--   cp.lineage_link / cp.lineage_edge with their current columns; a later,
--   separate change MAY decide to physically rename (Option A) or keep these
--   views permanently.
--
--   These are plain CREATE OR REPLACE VIEWs (no INSTEAD OF triggers): they are
--   read paths only. All WRITES still go through the sanctioned cp.* functions
--   against the physical tables — the views are never written to.

-- =====================================================================
-- cp.output_link — one row per produced output (over cp.lineage_link).
--   Column rename map (view alias <- physical column):
--     output_link_id        <- lineage_link_id
--     consumer_run_id        (unchanged)
--     edge_type / sink_type / target_ref / transform_version /
--     record_count / created_at (unchanged)
-- =====================================================================
CREATE OR REPLACE VIEW cp.output_link AS
SELECT lineage_link_id AS output_link_id,
       consumer_run_id,
       edge_type,
       sink_type,
       target_ref,
       transform_version,
       record_count,
       created_at
FROM cp.lineage_link;

-- =====================================================================
-- cp.input_edge — one row per input used to produce an output
--   (over cp.lineage_edge).
--   Column rename map (view alias <- physical column):
--     input_edge_id          <- lineage_edge_id
--     output_link_id         <- lineage_link_id        (the output this input fed)
--     upstream_output_link_id <- upstream_lineage_link_id (a PREVIOUS output)
--     upstream_run_id / source_file_id / input_slot /
--     edge_type / source_ref / record_count (unchanged)
-- =====================================================================
CREATE OR REPLACE VIEW cp.input_edge AS
SELECT lineage_edge_id AS input_edge_id,
       lineage_link_id AS output_link_id,
       upstream_run_id,
       upstream_lineage_link_id AS upstream_output_link_id,
       source_file_id,
       input_slot,
       edge_type,
       source_ref,
       record_count
FROM cp.lineage_edge;
