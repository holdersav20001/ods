-- 004_indexes.sql — performance indexes (P1 exit gate).
-- Additive only: no behaviour change. Postgres does NOT auto-index FK columns,
-- so unindexed FKs cause slow joins and lock escalation on parent UPDATE/DELETE.

-- 1. Discovery index for cp.latest_succeeded_run.
-- Query: WHERE domain=? AND dataset=? AND business_date=? AND pipeline_type=?
--        AND status='succeeded' ORDER BY finished_at DESC NULLS LAST, run_id DESC LIMIT 1
-- Partial on status='succeeded' (a constant in every call) keeps the index small and
-- drops status from the key. Four equality cols lead; the two ORDER BY cols trail in
-- matching direction so the scan returns rows pre-sorted -> no Sort node, instant LIMIT 1.
CREATE INDEX idx_run_log_discovery
    ON cp.run_log (domain, dataset, business_date, pipeline_type,
                   finished_at DESC NULLS LAST, run_id DESC)
    WHERE status = 'succeeded';

-- 2. FK-column indexes (parent is a non-trivial table).
CREATE INDEX idx_run_log_file        ON cp.run_log (file_id);
CREATE INDEX idx_run_log_replay_of   ON cp.run_log (replay_of_run_id);

CREATE INDEX idx_run_stage_log_run   ON cp.run_stage_log (run_id);

CREATE INDEX idx_lineage_link_run    ON cp.lineage_link (consumer_run_id);

CREATE INDEX idx_lineage_edge_link     ON cp.lineage_edge (lineage_link_id);
CREATE INDEX idx_lineage_edge_upstream ON cp.lineage_edge (upstream_run_id);
CREATE INDEX idx_lineage_edge_srcfile  ON cp.lineage_edge (source_file_id);

CREATE INDEX idx_reconciliation_log_run ON cp.reconciliation_log (run_id);

CREATE INDEX idx_dlq_run        ON cp.dlq (run_id);
CREATE INDEX idx_dlq_replay_run ON cp.dlq (replay_run_id);

CREATE INDEX idx_orders_link    ON ods.orders (_ods_lineage_link_id);

-- SKIPPED: FK columns lineage_link.edge_type and lineage_edge.edge_type ->
-- cp.edge_type. The parent is a tiny static lookup (7 rows) that is never updated
-- or deleted, so there is no lock-escalation risk and joins to it are trivial
-- (the planner uses a seq scan / hash either way). An index here would only add
-- write overhead with no read benefit.
