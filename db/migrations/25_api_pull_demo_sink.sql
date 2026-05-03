-- Migration 25: local API pull demo JDBC sink target table.

CREATE TABLE IF NOT EXISTS ods.insurance_api_pull_demo (
    request_id varchar NOT NULL,
    payload text NULL,
    _ods_source_request_id varchar NULL,
    _ods_source_message_id varchar NULL,
    _ods_source_event_id varchar NULL,
    _ods_source_batch_id varchar NULL,
    _ods_source_application varchar NULL,
    _ods_domain varchar NULL,
    _ods_dataset varchar NULL,
    _ods_business_date varchar NULL,
    _ods_source_cursor varchar NULL,
    _ods_archive_s3_uri text NULL,
    _ods_schema_id varchar NULL,
    _ods_schema_version bigint NULL,
    _ods_run_id varchar NOT NULL,
    _ods_file_id varchar NULL,
    _ods_ingested_at varchar NULL,
    CONSTRAINT insurance_api_pull_demo_pkey PRIMARY KEY (request_id)
);

CREATE INDEX IF NOT EXISTS idx_insurance_api_pull_demo_run_id
    ON ods.insurance_api_pull_demo(_ods_run_id);

CREATE INDEX IF NOT EXISTS idx_insurance_api_pull_demo_file_id
    ON ods.insurance_api_pull_demo(_ods_file_id);

CREATE INDEX IF NOT EXISTS idx_insurance_api_pull_demo_source_request_id
    ON ods.insurance_api_pull_demo(_ods_source_request_id);
