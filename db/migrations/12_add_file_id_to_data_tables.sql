-- Migration 12: add _ods_file_id lineage column to data tables

ALTER TABLE ods.insurance_policy
    ADD COLUMN IF NOT EXISTS _ods_file_id VARCHAR;

ALTER TABLE ods.insurance_policy_history
    ADD COLUMN IF NOT EXISTS _ods_file_id VARCHAR;
