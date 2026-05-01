-- Seed: insurance/policies dataset config + target ODS table.
INSERT INTO pipeline.dataset_config (
    domain, dataset, source_type, filename_pattern, target_topic,
    schema_id, schema_version, key_fields, dq_rules,
    data_classification, active, version
) VALUES (
    'insurance',
    'policies',
    's3_batch',
    'policies_(?P<bd>\d{8})\.csv',
    'ods.insurance.policies',
    'ods.insurance.policies-value',
    1,
    '["policy_id"]',
    '{
        "hard_blocks": [
            {"field": "policy_id", "rule": "not_null"},
            {"field": "policy_id", "rule": "unique"}
        ],
        "soft_warns": [
            {"fields": ["premium"], "rule": "completeness_pct", "threshold": 0.95}
        ]
    }',
    'Confidential',
    TRUE,
    1
)
ON CONFLICT (domain, dataset) DO NOTHING;

UPDATE pipeline.dataset_config
   SET s3_curated_path     = 's3://ods-curated-local/insurance/policies/',
       postgres_target_table = 'ods.insurance_policies'
 WHERE domain='insurance' AND dataset='policies';

CREATE SCHEMA IF NOT EXISTS ods;
CREATE TABLE IF NOT EXISTS ods.insurance_policies (
    policy_id           VARCHAR PRIMARY KEY,
    status              VARCHAR,
    premium             NUMERIC(10,2),
    premium_amount      NUMERIC(10,2),
    start_date          DATE,
    end_date            DATE,
    effective_date      DATE,
    agent_code          VARCHAR,
    postcode            VARCHAR,
    _ods_run_id         VARCHAR,
    _ods_business_date  VARCHAR,
    _ods_ingested_at    TIMESTAMP NOT NULL DEFAULT NOW()
);

ALTER TABLE ods.insurance_policies
    ADD COLUMN IF NOT EXISTS _ods_run_id        VARCHAR,
    ADD COLUMN IF NOT EXISTS _ods_business_date VARCHAR,
    ADD COLUMN IF NOT EXISTS _ods_ingested_at   TIMESTAMP NOT NULL DEFAULT NOW();
CREATE INDEX IF NOT EXISTS idx_insurance_policies_run_id
    ON ods.insurance_policies(_ods_run_id);
CREATE INDEX IF NOT EXISTS idx_insurance_policies_bd
    ON ods.insurance_policies(_ods_business_date);
