-- Migration 26: non-canonical API pull risk demo sink target.

CREATE TABLE IF NOT EXISTS ods.insurance_api_pull_risk (
    risk_id varchar NOT NULL,
    policy_id varchar NOT NULL,
    exposure_amount double precision NULL,
    as_of_date date NOT NULL,
    risk_key varchar NULL,
    _ods_business_date varchar NOT NULL,
    _ods_run_id varchar NOT NULL,
    _ods_raw_run_id varchar NULL,
    _ods_canonicalize_run_id varchar NULL,
    _ods_file_id varchar NULL,
    _ods_domain varchar NULL,
    _ods_dataset varchar NULL,
    _ods_source_application varchar NULL,
    _ods_inserted_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_insurance_api_pull_risk_run_id
    ON ods.insurance_api_pull_risk(_ods_run_id);

CREATE INDEX IF NOT EXISTS idx_insurance_api_pull_risk_file_id
    ON ods.insurance_api_pull_risk(_ods_file_id);

CREATE INDEX IF NOT EXISTS idx_insurance_api_pull_risk_key
    ON ods.insurance_api_pull_risk(risk_id, as_of_date);
