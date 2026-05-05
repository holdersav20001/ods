-- Migration 27: direct-Kafka api_pull demo sink target.
--
-- Mirrors the file-pipeline api_pull_demo target shape (PK=request_id,
-- upsert mode), but rows are written by the JDBC sink consuming the
-- raw Kafka topic ods.insurance.api_pull_lowlat_demo, NOT by Glue.
--
-- _ods_kafka_partition + _ods_kafka_offset are populated by the
-- producer envelope so dashboards / replay tooling can locate any row
-- back to its origin offset window.

CREATE TABLE IF NOT EXISTS ods.insurance_api_pull_lowlat_demo (
    request_id              varchar     NOT NULL,
    amount                  double precision NULL,
    updated_at              varchar     NULL,

    _ods_run_id             varchar     NOT NULL,
    _ods_business_date      varchar     NOT NULL,
    _ods_source_request_id  varchar     NULL,
    _ods_source_message_id  varchar     NULL,
    _ods_source_event_id    varchar     NULL,
    _ods_source_batch_id    varchar     NULL,
    _ods_source_application varchar     NULL,
    _ods_source_cursor      varchar     NULL,
    _ods_archive_s3_uri     text        NULL,
    _ods_domain             varchar     NULL,
    _ods_dataset            varchar     NULL,
    _ods_file_id            varchar     NULL,
    _ods_ingested_at        varchar     NULL,
    _ods_schema_id          varchar     NULL,
    _ods_schema_version     bigint      NULL,
    _ods_kafka_partition    int         NULL,
    _ods_kafka_offset       bigint      NULL,

    CONSTRAINT insurance_api_pull_lowlat_demo_pkey PRIMARY KEY (request_id)
);

CREATE INDEX IF NOT EXISTS idx_insurance_api_pull_lowlat_demo_run_id
    ON ods.insurance_api_pull_lowlat_demo(_ods_run_id);

CREATE INDEX IF NOT EXISTS idx_insurance_api_pull_lowlat_demo_offset
    ON ods.insurance_api_pull_lowlat_demo(_ods_kafka_partition, _ods_kafka_offset);

CREATE INDEX IF NOT EXISTS idx_insurance_api_pull_lowlat_demo_source_request
    ON ods.insurance_api_pull_lowlat_demo(_ods_source_request_id);
