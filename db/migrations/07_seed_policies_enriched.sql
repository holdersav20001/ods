-- Seed: insurance/policies_core + policies_enrichment slot configs + merge target config.

INSERT INTO pipeline.dataset_config (
    domain, dataset, source_type, slot_name, merge_dataset, staging_table,
    filename_pattern, target_topic, schema_id, schema_version,
    key_fields, dq_rules, data_classification, active, version
) VALUES (
    'insurance', 'policies_core', 's3_batch', 'core', 'policies_enriched',
    'pipeline.slot_staging_core',
    'policies_core_(?P<bd>\d{8})\.csv',
    '',
    'ods.insurance.policies_core-value', 1,
    '["policy_id"]',
    '{"hard_blocks":[{"field":"policy_id","rule":"not_null"},{"field":"policy_id","rule":"unique"}],"soft_warns":[]}',
    'Confidential', TRUE, 1
) ON CONFLICT (domain, dataset) DO NOTHING;

INSERT INTO pipeline.dataset_config (
    domain, dataset, source_type, slot_name, merge_dataset, staging_table,
    filename_pattern, target_topic, schema_id, schema_version,
    key_fields, dq_rules, data_classification, active, version
) VALUES (
    'insurance', 'policies_enrichment', 's3_batch', 'enrichment', 'policies_enriched',
    'pipeline.slot_staging_enrichment',
    'policies_enrichment_(?P<bd>\d{8})\.csv',
    '',
    'ods.insurance.policies_enrichment-value', 1,
    '["policy_id"]',
    '{"hard_blocks":[{"field":"policy_id","rule":"not_null"},{"field":"policy_id","rule":"unique"}],"soft_warns":[]}',
    'Confidential', TRUE, 1
) ON CONFLICT (domain, dataset) DO NOTHING;

INSERT INTO pipeline.dataset_config (
    domain, dataset, source_type,
    filename_pattern, target_topic, schema_id, schema_version,
    key_fields, dq_rules, data_classification, active, version,
    s3_curated_path, postgres_target_table
) VALUES (
    'insurance', 'policies_enriched', 'multi_slot_merge',
    '',
    'ods.insurance.policies_enriched',
    'ods.insurance.policies_enriched-value', 1,
    '["policy_id"]', '{}',
    'Confidential', TRUE, 1,
    's3://ods-curated-local/insurance/policies_enriched/',
    'ods.policies_enriched'
) ON CONFLICT (domain, dataset) DO NOTHING;
