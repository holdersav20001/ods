-- Migration 10: rename write-mode test tables to domain-named canonical tables
-- policies_upsert → insurance_policy_history (append, full history)
-- drop events_append (not yet required)

-- Drop events_append
DROP TABLE IF EXISTS ods.events_append;

-- Rename policies_upsert → insurance_policy_history
-- History is append-only: drop PK so multiple rows per policy_id are allowed
ALTER TABLE ods.policies_upsert DROP CONSTRAINT IF EXISTS policies_upsert_pkey;
ALTER TABLE ods.policies_upsert RENAME TO insurance_policy_history;

GRANT ALL ON ods.insurance_policy_history TO ods;
