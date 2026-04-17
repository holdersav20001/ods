INSERT INTO pipeline.dataset_config (
    domain, dataset, filename_pattern, target_topic,
    schema_id, schema_version, key_fields, dq_rules,
    data_classification, active, version
) VALUES (
    'insurance',
    'policies',
    'policies_(\\d{8})\\.csv',
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
);

INSERT INTO pipeline.file_catalogue (name_pattern, domain, dataset, dataset_config_id)
SELECT 'policies_*.csv', 'insurance', 'policies', id
FROM pipeline.dataset_config WHERE dataset = 'policies';
