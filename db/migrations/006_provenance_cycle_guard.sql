-- 006_provenance_cycle_guard.sql — make the provenance walk cycle-safe.
--
-- cp.v_provenance (defined in 001_schema.sql) is a WITH RECURSIVE ... UNION ALL
-- walk that follows e.upstream_run_id from a consumer run to the link that
-- upstream run produced, recursing back toward raw. It had NO cycle guard: a
-- malformed provenance edge that points (directly or transitively) back to a
-- run already on the walk would make the recursion never terminate and the
-- query hang / error out under max-recursion.
--
-- The replay hop (Phase 3d) is the first writer to introduce edges between
-- separate executions (the 'replay' annotation edge links a new replay run to
-- the ORIGINAL run). Replay edges are is_provenance=true, so they participate
-- in this walk. A buggy or adversarial replay edge could therefore close a
-- cycle (run A -> run B -> run A). Per the design hard-gate, the cycle guard
-- MUST land BEFORE any replay-edge writer ships.
--
-- FIX: reproduce the 001 view definition verbatim and add a Postgres CYCLE
-- clause. The CYCLE clause tracks the set of consumer_run_id values already
-- visited on each path (column `path`); when the walk would revisit a run it
-- has already seen, it marks that row is_cycle=true and STOPS recursing down
-- that branch — guaranteeing termination instead of an infinite loop.
--
-- We keep the existing output columns (lineage_link_id, lineage_edge_id,
-- upstream_run_id, source_file_id, edge_type, consumer_run_id) and additionally
-- expose is_cycle / path (harmless: existing consumers select named columns; the
-- trace_row.sql query selects specific columns and is unaffected). The CYCLE
-- key is consumer_run_id — the value the recursive join chases via
-- pl.consumer_run_id = w.upstream_run_id — so a run reappearing on the path is
-- exactly the loop condition we want to break.

CREATE OR REPLACE VIEW cp.v_provenance AS
WITH RECURSIVE walk AS (
    SELECT l.lineage_link_id, e.lineage_edge_id, e.upstream_run_id,
           e.source_file_id, e.edge_type, l.consumer_run_id
    FROM cp.lineage_link l
    JOIN cp.lineage_edge e ON e.lineage_link_id = l.lineage_link_id
    JOIN cp.edge_type t    ON t.edge_type = e.edge_type AND t.is_provenance
  UNION ALL
    SELECT pl.lineage_link_id, pe.lineage_edge_id, pe.upstream_run_id,
           pe.source_file_id, pe.edge_type, pl.consumer_run_id
    FROM walk w
    JOIN cp.lineage_link pl ON pl.consumer_run_id = w.upstream_run_id
    JOIN cp.lineage_edge pe ON pe.lineage_link_id = pl.lineage_link_id
    JOIN cp.edge_type t     ON t.edge_type = pe.edge_type AND t.is_provenance
)
CYCLE consumer_run_id SET is_cycle USING path
-- Expose only the original output columns plus is_cycle. We deliberately do NOT
-- expose `path`: the CYCLE search-path column has pseudo-type record[], which
-- Postgres refuses as a view output column (SELECT * would fail). is_cycle is a
-- harmless boolean flag (true on the row where the walk detected a revisit and
-- pruned that branch) that existing named-column consumers and trace_row.sql
-- simply ignore.
SELECT lineage_link_id, lineage_edge_id, upstream_run_id,
       source_file_id, edge_type, consumer_run_id, is_cycle
FROM walk;
