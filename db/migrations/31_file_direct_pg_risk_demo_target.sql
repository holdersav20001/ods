-- Migration 31: target table for the file -> direct-Postgres NON-CANONICAL
-- demo dataset (file_direct_pg_risk_demo).
--
-- Exercises the inline canonicalize path in ods_postgres_write.py:
-- curated parquet carries source-shape columns (RskID, PolNo,
-- ExposureAmt, AsOfDt) plus ODS metadata; ods_postgres_write applies
-- the YAML transform to produce canonical-shape columns
-- (risk_id, policy_id, exposure_amount, as_of_date) and writes them
-- here, preserving the ODS metadata block from the curated parquet.
--
-- Append-only history table. No PK so a single source row appearing
-- across multiple runs is recorded as separate audit rows tagged by
-- _ods_run_id (the postgres-write run_id, not the ingestion run_id).

BEGIN;

CREATE TABLE IF NOT EXISTS ods.insurance_file_direct_pg_risk_demo (
    risk_id                 varchar         NOT NULL,
    policy_id               varchar         NULL,
    exposure_amount         double precision NULL,
    as_of_date              date            NULL,

    _ods_run_id             varchar         NOT NULL,
    _ods_business_date      varchar         NOT NULL,
    _ods_file_id            varchar         NULL,
    _ods_domain             varchar         NULL,
    _ods_dataset            varchar         NULL,
    _ods_source_application varchar         NULL,
    _ods_ingested_at        varchar         NULL,
    _ods_inserted_at        timestamptz     NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_insurance_file_direct_pg_risk_demo_run
    ON ods.insurance_file_direct_pg_risk_demo(_ods_run_id);

CREATE INDEX IF NOT EXISTS idx_insurance_file_direct_pg_risk_demo_file
    ON ods.insurance_file_direct_pg_risk_demo(_ods_file_id);

CREATE INDEX IF NOT EXISTS idx_insurance_file_direct_pg_risk_demo_risk
    ON ods.insurance_file_direct_pg_risk_demo(risk_id);

COMMIT;
