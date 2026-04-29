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
    'ods-insurance-policies-value',
    1,
    '["policy_id"]',
    '{
        "hard_blocks": [
            {"field": "policy_id",      "rule": "not_null"},
            {"field": "policy_id",      "rule": "unique"},
            {"field": "premium_amount", "rule": "not_null"},
            {"field": "premium_amount", "rule": "greater_than", "value": 0},
            {"field": "start_date",     "rule": "valid_date",   "format": "yyyy-MM-dd"},
            {"field": "end_date", "rule": "date_gte", "value_type": "column", "value": "start_date"}
        ],
        "soft_warns": [
            {"field": "premium_amount", "rule": "less_than",    "value": 50000},
            {"field": "end_date",       "rule": "not_past"},
            {"fields": ["agent_code", "postcode"], "rule": "completeness_pct", "threshold": 0.8}
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
    policy_id      VARCHAR PRIMARY KEY,
    status         VARCHAR,
    premium        NUMERIC(10,2),
    premium_amount NUMERIC(10,2),
    start_date     DATE,
    end_date       DATE,
    effective_date DATE,
    agent_code     VARCHAR,
    postcode       VARCHAR
);
