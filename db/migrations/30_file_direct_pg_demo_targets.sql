-- Migration 30: target tables for the file → direct-Postgres demos.
--
-- Two datasets, two write modes:
--   * file_direct_pg_upsert_demo  — PK on country_code (upsert / merge)
--   * file_direct_pg_append_demo  — no PK (append-only audit)
--
-- Both carry the standard ODS metadata columns plus an
-- _ods_inserted_at watermark so dashboards can plot ingestion latency
-- without a separate audit table.

BEGIN;

CREATE TABLE IF NOT EXISTS ods.insurance_file_direct_pg_upsert_demo (
    country_code            varchar     NOT NULL,
    country_name            varchar     NULL,

    _ods_run_id             varchar     NOT NULL,
    _ods_business_date      varchar     NOT NULL,
    _ods_file_id            varchar     NULL,
    _ods_domain             varchar     NULL,
    _ods_dataset            varchar     NULL,
    _ods_source_application varchar     NULL,
    _ods_ingested_at        varchar     NULL,
    _ods_inserted_at        timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT insurance_file_direct_pg_upsert_demo_pkey
        PRIMARY KEY (country_code)
);

CREATE INDEX IF NOT EXISTS idx_insurance_file_direct_pg_upsert_demo_run
    ON ods.insurance_file_direct_pg_upsert_demo(_ods_run_id);

CREATE INDEX IF NOT EXISTS idx_insurance_file_direct_pg_upsert_demo_file
    ON ods.insurance_file_direct_pg_upsert_demo(_ods_file_id);


CREATE TABLE IF NOT EXISTS ods.insurance_file_direct_pg_append_demo (
    event_id                varchar     NOT NULL,
    payload                 text        NULL,

    _ods_run_id             varchar     NOT NULL,
    _ods_business_date      varchar     NOT NULL,
    _ods_file_id            varchar     NULL,
    _ods_domain             varchar     NULL,
    _ods_dataset            varchar     NULL,
    _ods_source_application varchar     NULL,
    _ods_ingested_at        varchar     NULL,
    _ods_inserted_at        timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_insurance_file_direct_pg_append_demo_run
    ON ods.insurance_file_direct_pg_append_demo(_ods_run_id);

CREATE INDEX IF NOT EXISTS idx_insurance_file_direct_pg_append_demo_event
    ON ods.insurance_file_direct_pg_append_demo(event_id);

COMMIT;
