-- Migration 11: rename insurance_policies → insurance_policy, drop deprecated tables, clear pipeline data

-- Rename ods table
ALTER TABLE ods.insurance_policies RENAME TO insurance_policy;

UPDATE pipeline.dataset_config
   SET postgres_target_table = 'ods.insurance_policy'
 WHERE domain = 'insurance'
   AND dataset = 'policies'
   AND postgres_target_table = 'ods.insurance_policies';

-- Drop deprecated pipeline tables
DROP TABLE IF EXISTS pipeline.file_catalogue_deprecated_2026_04_28;
DROP TABLE IF EXISTS pipeline.file_state_deprecated_2026_04_28;
DROP TABLE IF EXISTS pipeline.glue_job_log_deprecated_2026_04_28;
DROP TABLE IF EXISTS pipeline.ingestion_file_state_deprecated_2026_04_28;
DROP TABLE IF EXISTS pipeline.lineage_deprecated_2026_04_28;

-- Clear pipeline operational data (preserve schema/config)
TRUNCATE TABLE
    pipeline.run_stage_log,
    pipeline.run_events,
    pipeline.reconciliation_log,
    pipeline.merge_contribution_log,
    pipeline.merge_run_log,
    pipeline.slot_staging_core,
    pipeline.slot_staging_enrichment,
    pipeline.run_log,
    pipeline.file_state,
    pipeline.file_catalogue
CASCADE;
