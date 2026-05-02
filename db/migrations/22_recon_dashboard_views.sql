-- T14 (8.5): operator dashboard views over reconciliation_log + run_log.
--
-- Each view is read-only. The dashboard at scripts/ops_control_dashboard.py
-- exposes them as panels with date filters.

BEGIN;

-- 1. Latest failed reconciliation per (domain, dataset).
CREATE OR REPLACE VIEW ods.v_recon_latest_failed AS
SELECT DISTINCT ON (domain, dataset)
    run_id,
    check_type,
    domain,
    dataset,
    business_date,
    source_count,
    kafka_count,
    postgres_count,
    discrepancy_count,
    discrepancy_pct,
    detail,
    created_at
  FROM pipeline.reconciliation_log
 WHERE status = 'failed'
 ORDER BY domain, dataset, created_at DESC;

COMMENT ON VIEW ods.v_recon_latest_failed IS
    'Most recent failed reconciliation per (domain, dataset). Operators '
    'use this as the "what is broken right now" panel.';


-- 2. T0/T1/T2 trend by day per check_type.
CREATE OR REPLACE VIEW ods.v_recon_t0_t1_t2_trend AS
SELECT
    DATE_TRUNC('day', created_at)::date AS day,
    check_type,
    COUNT(*) FILTER (WHERE status = 'ok')      AS ok_count,
    COUNT(*) FILTER (WHERE status = 'failed')  AS failed_count,
    COUNT(*) FILTER (WHERE status = 'pending') AS pending_count,
    COUNT(*)                                   AS total_count
  FROM pipeline.reconciliation_log
 WHERE check_type IN ('t0_publish_count', 't1_canonical_count',
                      't2_sink_count', 'dual_sink_parity')
 GROUP BY 1, 2;

COMMENT ON VIEW ods.v_recon_t0_t1_t2_trend IS
    'Daily counts of ok/failed/pending per recon check_type. Use for '
    'health sparklines and SLO burn-rate calculations.';


-- 3. DLQ-adjusted publish counts.
--    Did record_count_source - dlq_count == record_count_published?
CREATE OR REPLACE VIEW ods.v_recon_dlq_adjusted AS
SELECT
    rl.run_id,
    rl.domain,
    rl.dataset,
    rl.business_date,
    rl.record_count_source,
    rl.record_count_dq_fail   AS dlq_count,
    rl.record_count_published,
    (COALESCE(rl.record_count_source, 0)
     - COALESCE(rl.record_count_dq_fail, 0)
     - COALESCE(rl.record_count_published, 0)) AS adjusted_delta,
    rl.status,
    rl.started_at,
    rl.ended_at
  FROM pipeline.run_log rl
 WHERE rl.record_count_source IS NOT NULL;

COMMENT ON VIEW ods.v_recon_dlq_adjusted IS
    'Per-run check that source - dq_fail - published == 0. Non-zero '
    'adjusted_delta means lost or duplicated records that simple T0 '
    'reconciliation may have missed.';


-- 4. Current vs history sink consistency (extends T11 view with run-level detail).
CREATE OR REPLACE VIEW ods.v_current_history_consistency AS
SELECT
    domain,
    dataset,
    business_date,
    SUM(source_count)             AS current_total,
    SUM(postgres_count)           AS history_total,
    SUM(discrepancy_count)        AS total_delta,
    COUNT(*) FILTER (WHERE status = 'ok')      AS matched_runs,
    COUNT(*) FILTER (WHERE status = 'failed')  AS diverged_runs,
    COUNT(*) FILTER (WHERE status = 'pending') AS lagging_runs
  FROM pipeline.reconciliation_log
 WHERE check_type = 'dual_sink_parity'
 GROUP BY 1, 2, 3;

COMMENT ON VIEW ods.v_current_history_consistency IS
    'Aggregated dual-sink parity per (domain, dataset, business_date). '
    'Gives an at-a-glance "is the audit trail intact" view.';


-- 5. Rerun candidates: failed runs whose upstream parents (if any) succeeded.
CREATE OR REPLACE VIEW ods.v_rerun_candidates AS
SELECT
    rl.run_id,
    rl.pipeline_type,
    rl.domain,
    rl.dataset,
    rl.business_date,
    rl.file_id,
    rl.error_summary,
    rl.ended_at,
    EXISTS (
        SELECT 1 FROM pipeline.lineage_edge le
         JOIN pipeline.run_log parent ON parent.run_id = le.parent_run_id
        WHERE le.child_run_id = rl.run_id
          AND parent.status = 'succeeded'
    ) AS has_succeeded_parent
  FROM pipeline.run_log rl
 WHERE rl.status = 'failed';

COMMENT ON VIEW ods.v_rerun_candidates IS
    'Runs in failed status. has_succeeded_parent indicates whether an '
    'upstream run is in a stable terminal state and the failed run is '
    'safe to replay via python -m ods_pipeline.ops replay.';

COMMIT;
