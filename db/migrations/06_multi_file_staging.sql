-- Migration 06: multi-file staging + merge tables
-- Adds slot_name / merge_dataset / staging_table to dataset_config,
-- creates staging tables per slot, merge audit tables, and wide target.

-- ── dataset_config extensions ──────────────────────────────────────────────
ALTER TABLE pipeline.dataset_config
    ADD COLUMN IF NOT EXISTS slot_name     VARCHAR,
    ADD COLUMN IF NOT EXISTS merge_dataset VARCHAR,
    ADD COLUMN IF NOT EXISTS staging_table VARCHAR;

-- ── slot staging tables ────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS pipeline.slot_staging_core (
    policy_id          VARCHAR      NOT NULL,
    status             VARCHAR,
    premium            NUMERIC(10,2),
    effective_date     DATE,
    _ods_run_id        VARCHAR      NOT NULL,
    _ods_business_date DATE         NOT NULL,
    _ods_staged_at     TIMESTAMP    NOT NULL DEFAULT NOW(),
    PRIMARY KEY (policy_id, _ods_business_date)
);
CREATE INDEX IF NOT EXISTS idx_staging_core_run_id ON pipeline.slot_staging_core(_ods_run_id);
CREATE INDEX IF NOT EXISTS idx_staging_core_bd     ON pipeline.slot_staging_core(_ods_business_date);

CREATE TABLE IF NOT EXISTS pipeline.slot_staging_enrichment (
    policy_id          VARCHAR      NOT NULL,
    agent_code         VARCHAR,
    postcode           VARCHAR,
    risk_score         NUMERIC(5,2),
    channel            VARCHAR,
    _ods_run_id        VARCHAR      NOT NULL,
    _ods_business_date DATE         NOT NULL,
    _ods_staged_at     TIMESTAMP    NOT NULL DEFAULT NOW(),
    PRIMARY KEY (policy_id, _ods_business_date)
);
CREATE INDEX IF NOT EXISTS idx_staging_enrich_run_id ON pipeline.slot_staging_enrichment(_ods_run_id);
CREATE INDEX IF NOT EXISTS idx_staging_enrich_bd     ON pipeline.slot_staging_enrichment(_ods_business_date);

-- ── merge audit tables ─────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS pipeline.merge_run_log (
    merge_run_id    UUID         PRIMARY KEY,
    domain          VARCHAR      NOT NULL,
    dataset         VARCHAR      NOT NULL,
    business_date   DATE         NOT NULL,
    status          VARCHAR      NOT NULL DEFAULT 'running'
                        CHECK (status IN ('running', 'succeeded', 'failed')),
    record_count_out BIGINT,
    started_at      TIMESTAMP    NOT NULL DEFAULT NOW(),
    ended_at        TIMESTAMP,
    error_summary   TEXT
);
CREATE INDEX IF NOT EXISTS idx_merge_run_log_domain_bd ON pipeline.merge_run_log(domain, dataset, business_date);
CREATE INDEX IF NOT EXISTS idx_merge_run_log_status    ON pipeline.merge_run_log(status, started_at);

CREATE TABLE IF NOT EXISTS pipeline.merge_contribution_log (
    id              BIGSERIAL    PRIMARY KEY,
    merge_run_id    UUID         NOT NULL REFERENCES pipeline.merge_run_log(merge_run_id),
    slot_name       VARCHAR      NOT NULL,
    slot_run_id     VARCHAR      NOT NULL,
    file_id         UUID,
    s3_raw_path     VARCHAR      NOT NULL,
    columns_written TEXT[]       NOT NULL,
    record_count    BIGINT,
    created_at      TIMESTAMP    NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_mcl_merge_run_id ON pipeline.merge_contribution_log(merge_run_id);
CREATE INDEX IF NOT EXISTS idx_mcl_slot_run_id  ON pipeline.merge_contribution_log(slot_run_id);

-- ── wide target table ──────────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS ods;

CREATE TABLE IF NOT EXISTS ods.policies_enriched (
    policy_id           VARCHAR      PRIMARY KEY,
    -- core slot columns
    status              VARCHAR,
    premium             NUMERIC(10,2),
    effective_date      DATE,
    -- enrichment slot columns
    agent_code          VARCHAR,
    postcode            VARCHAR,
    risk_score          NUMERIC(5,2),
    channel             VARCHAR,
    -- lineage
    _ods_merge_run_id   UUID         NOT NULL,
    _ods_run_id_core    VARCHAR,
    _ods_run_id_enrich  VARCHAR,
    _ods_business_date  DATE         NOT NULL,
    _ods_merged_at      TIMESTAMP    NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_pe_merge_run_id ON ods.policies_enriched(_ods_merge_run_id);
CREATE INDEX IF NOT EXISTS idx_pe_bd           ON ods.policies_enriched(_ods_business_date);

GRANT USAGE ON SCHEMA ods TO ods;
GRANT ALL ON ods.policies_enriched TO ods;
GRANT ALL ON pipeline.slot_staging_core TO ods;
GRANT ALL ON pipeline.slot_staging_enrichment TO ods;
GRANT ALL ON pipeline.merge_run_log TO ods;
GRANT ALL ON pipeline.merge_contribution_log TO ods;
GRANT ALL ON SEQUENCE pipeline.merge_contribution_log_id_seq TO ods;
