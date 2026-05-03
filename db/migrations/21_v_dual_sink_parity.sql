-- T11 (A2): exposed view for the latest dual-sink parity result per
-- (domain, dataset, business_date). Backed by reconciliation_log.

BEGIN;

CREATE OR REPLACE VIEW ods.v_dual_sink_parity AS
SELECT DISTINCT ON (domain, dataset, business_date)
    run_id,
    domain,
    dataset,
    business_date,
    source_count    AS current_count,
    postgres_count  AS history_count,
    discrepancy_count AS delta,
    status,
    detail,
    created_at
  FROM pipeline.reconciliation_log
 WHERE check_type = 'dual_sink_parity'
 ORDER BY domain, dataset, business_date, created_at DESC;

COMMENT ON VIEW ods.v_dual_sink_parity IS
    'Latest dual-sink (current vs history) row-count parity per dataset/business_date. '
    'status=pending when either sink empty (consumer lag); status=failed when both '
    'have rows but counts diverge.';

COMMIT;
