-- Migration 08: write_mode column + Postgres target tables for append/upsert patterns

ALTER TABLE pipeline.dataset_config
    ADD COLUMN IF NOT EXISTS write_mode VARCHAR NOT NULL DEFAULT 'upsert'
    CHECK (write_mode IN ('append', 'upsert', 'replace'));

-- Append pattern: no PK — rows only ever added, never updated
CREATE TABLE IF NOT EXISTS ods.events_append (
    event_id           VARCHAR NOT NULL,
    policy_id          VARCHAR NOT NULL,
    event_type         VARCHAR NOT NULL,
    event_date         DATE,
    amount             NUMERIC(10,2),
    _ods_business_date VARCHAR NOT NULL,
    _ods_run_id        VARCHAR NOT NULL,
    _ods_ingested_at   TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Upsert pattern: policy_id PK — later files for the same key overwrite earlier data
CREATE TABLE IF NOT EXISTS ods.policies_upsert (
    policy_id          VARCHAR PRIMARY KEY,
    status             VARCHAR,
    premium            NUMERIC(10,2),
    effective_date     DATE,
    _ods_business_date VARCHAR,
    _ods_run_id        VARCHAR,
    _ods_ingested_at   TIMESTAMP NOT NULL DEFAULT NOW()
);

GRANT ALL ON ods.events_append   TO ods;
GRANT ALL ON ods.policies_upsert TO ods;
