-- Migration 16: complete ODS metadata contract on policy current/history tables

ALTER TABLE ods.insurance_policy
    ADD COLUMN IF NOT EXISTS _ods_domain VARCHAR,
    ADD COLUMN IF NOT EXISTS _ods_dataset VARCHAR,
    ADD COLUMN IF NOT EXISTS _ods_source_application VARCHAR;

ALTER TABLE ods.insurance_policy_history
    ADD COLUMN IF NOT EXISTS _ods_domain VARCHAR,
    ADD COLUMN IF NOT EXISTS _ods_dataset VARCHAR,
    ADD COLUMN IF NOT EXISTS _ods_source_application VARCHAR;

CREATE INDEX IF NOT EXISTS idx_insurance_policy_domain_dataset
    ON ods.insurance_policy(_ods_domain, _ods_dataset);

CREATE INDEX IF NOT EXISTS idx_insurance_policy_source_app
    ON ods.insurance_policy(_ods_source_application);

CREATE INDEX IF NOT EXISTS idx_insurance_policy_history_domain_dataset
    ON ods.insurance_policy_history(_ods_domain, _ods_dataset);

CREATE INDEX IF NOT EXISTS idx_insurance_policy_history_source_app
    ON ods.insurance_policy_history(_ods_source_application);
