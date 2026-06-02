-- 021_detail_to_aggregate.sql — F1: register the detail_to_aggregate provenance
-- edge_type for the policy/claims aggregate hop (detail rows -> aggregate output).
--
-- Spec:   docs/specs/2026-06-02-airflow-orchestrator-policy-claims-workflow.md
--         ("Migration 021").
-- Probes: tests/test_orchestrator_identity.py
--           ::test_detail_to_aggregate_is_registered_provenance_edge
--           ::test_detail_to_aggregate_requires_upstream_link
--         tests/test_contract.py::test_detail_to_aggregate_edge_type_registered
--
-- WHY:
--   An aggregate output (e.g. a per-policy roll-up) is derived from DETAIL rows
--   produced by an upstream run. That is a run-to-run provenance edge exactly
--   like curated_to_canonical / merge_to_canonical / canonical_to_sink: it MUST
--   name the EXACT upstream output (upstream_lineage_link_id), not just the
--   upstream run, so the link->link provenance walk (009 cp.v_provenance) follows
--   the precise detail output and does not over-claim other outputs of that run.
--
--   So detail_to_aggregate must (a) be a registered is_provenance edge_type, and
--   (b) be added to the upstream_link_required_for_run_edges CHECK (009).
--
-- No function/trigger change is needed:
--   * edge_type_matches_link (014) only requires edge.edge_type == link.edge_type
--     (true here: a detail_to_aggregate edge under a detail_to_aggregate link),
--     so the unchanged trigger already accepts it.
--   * edge_must_anchor (012) is satisfied because the edge carries
--     upstream_lineage_link_id (the new CHECK below forces it).
--
-- Purely additive (one seed row + one CHECK re-add). Does not edit 001-020.

-- =====================================================================
-- 1. Register the edge_type (idempotent).
-- =====================================================================
INSERT INTO cp.edge_type(edge_type, is_provenance)
VALUES ('detail_to_aggregate', true)
ON CONFLICT DO NOTHING;

-- =====================================================================
-- 2. Extend upstream_link_required_for_run_edges (009) to also govern
--    detail_to_aggregate. The 009 predicate was:
--        edge_type NOT IN ('curated_to_canonical','merge_to_canonical',
--                          'canonical_to_sink')
--        OR upstream_lineage_link_id IS NOT NULL
--    We drop and re-add it with detail_to_aggregate appended to the NOT IN list,
--    so a detail_to_aggregate edge missing upstream_lineage_link_id is REJECTED.
--    A banner was added to 009 marking its CHECK definition SUPERSEDED by this
--    one — 021 applies last, so this wins.
-- =====================================================================
ALTER TABLE cp.lineage_edge DROP CONSTRAINT upstream_link_required_for_run_edges;
ALTER TABLE cp.lineage_edge ADD CONSTRAINT upstream_link_required_for_run_edges CHECK (
    edge_type NOT IN ('curated_to_canonical','merge_to_canonical',
                      'canonical_to_sink','detail_to_aggregate')
    OR upstream_lineage_link_id IS NOT NULL
);
