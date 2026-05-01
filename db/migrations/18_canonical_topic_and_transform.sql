-- Migration 18: support non-canonical -> canonical Kafka pipelines.

ALTER TABLE pipeline.dataset_config
    ADD COLUMN IF NOT EXISTS is_canonical        BOOLEAN NOT NULL DEFAULT TRUE,
    ADD COLUMN IF NOT EXISTS canonical_topic     VARCHAR,
    ADD COLUMN IF NOT EXISTS canonical_schema_id VARCHAR,
    ADD COLUMN IF NOT EXISTS transform_yaml_path VARCHAR;

CREATE INDEX IF NOT EXISTS idx_dataset_config_canonical_topic
    ON pipeline.dataset_config(canonical_topic)
    WHERE canonical_topic IS NOT NULL;

CREATE TABLE IF NOT EXISTS ods.insurance_risk (
    risk_id varchar NOT NULL,
    policy_id varchar NOT NULL,
    exposure_amount numeric(18, 2) NULL,
    as_of_date date NOT NULL,
    risk_key varchar NULL,
    _ods_file_id varchar NULL,
    _ods_run_id varchar NOT NULL,
    _ods_raw_run_id varchar NULL,
    _ods_canonicalize_run_id varchar NULL,
    _ods_domain varchar NULL,
    _ods_dataset varchar NULL,
    _ods_business_date date NULL,
    _ods_source_application varchar NULL,
    _ods_ingested_at timestamp DEFAULT now() NOT NULL,
    CONSTRAINT insurance_risk_pkey PRIMARY KEY (risk_id, as_of_date)
);

CREATE INDEX IF NOT EXISTS idx_insurance_risk_file_id
    ON ods.insurance_risk(_ods_file_id);

CREATE INDEX IF NOT EXISTS idx_insurance_risk_run_id
    ON ods.insurance_risk(_ods_run_id);
