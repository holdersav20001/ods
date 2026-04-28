-- 03_consolidate_pipeline_tables.sql
BEGIN;

-- Existing pipeline.file_catalogue has a different shape; rename then recreate.
ALTER TABLE pipeline.file_catalogue RENAME TO file_catalogue_deprecated_2026_04_28;

CREATE TABLE pipeline.file_catalogue (
    file_id                 UUID PRIMARY KEY,
    domain                  VARCHAR NOT NULL,
    dataset                 VARCHAR NOT NULL,
    business_date           DATE NOT NULL,
    sftp_path               VARCHAR,
    s3_raw_path             VARCHAR,
    s3_staging_parquet_path VARCHAR,
    s3_curated_path         VARCHAR,
    file_size_bytes         BIGINT,
    source_row_count        BIGINT,
    file_md5                CHAR(32) NOT NULL,
    state                   VARCHAR NOT NULL,
    state_updated_at        TIMESTAMP NOT NULL DEFAULT NOW(),
    first_seen_at           TIMESTAMP NOT NULL DEFAULT NOW(),
    last_run_id             UUID,
    UNIQUE (domain, dataset, file_md5)
);
CREATE INDEX IF NOT EXISTS idx_file_catalogue_state ON pipeline.file_catalogue(state, state_updated_at);

CREATE TABLE pipeline.run_log (
    run_id                  UUID PRIMARY KEY,
    pipeline_type           VARCHAR NOT NULL,
    domain                  VARCHAR NOT NULL,
    dataset                 VARCHAR NOT NULL,
    business_date           DATE,
    file_id                 UUID REFERENCES pipeline.file_catalogue(file_id),
    status                  VARCHAR NOT NULL,
    started_at              TIMESTAMP NOT NULL DEFAULT NOW(),
    ended_at                TIMESTAMP,
    record_count_source     BIGINT,
    record_count_dq_pass    BIGINT,
    record_count_dq_fail    BIGINT,
    record_count_published  BIGINT,
    kafka_topic             VARCHAR,
    kafka_offset_start      BIGINT,
    kafka_offset_end        BIGINT,
    config_version_id       BIGINT,
    schema_version_id       INT,
    parents                 JSONB,
    error_summary           TEXT,
    created_at              TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_run_log_dom_ds_bd ON pipeline.run_log(domain, dataset, business_date);
CREATE INDEX IF NOT EXISTS idx_run_log_status_started ON pipeline.run_log(status, started_at);

CREATE TABLE pipeline.run_stage_log (
    id               BIGSERIAL PRIMARY KEY,
    run_id           UUID NOT NULL REFERENCES pipeline.run_log(run_id),
    stage            VARCHAR NOT NULL,
    status           VARCHAR NOT NULL,
    started_at       TIMESTAMP NOT NULL DEFAULT NOW(),
    ended_at         TIMESTAMP,
    input_ref        TEXT,
    output_ref       TEXT,
    record_count_in  BIGINT,
    record_count_out BIGINT,
    metrics          JSONB,
    error            TEXT
);
CREATE INDEX IF NOT EXISTS idx_run_stage_log_run_stage ON pipeline.run_stage_log(run_id, stage);

CREATE TABLE pipeline.reconciliation_log (
    id                BIGSERIAL PRIMARY KEY,
    check_type        VARCHAR NOT NULL,
    run_id            UUID,
    domain            VARCHAR NOT NULL,
    dataset           VARCHAR NOT NULL,
    business_date     DATE,
    window_start      TIMESTAMP,
    window_end        TIMESTAMP,
    source_count      BIGINT,
    kafka_count       BIGINT,
    postgres_count    BIGINT,
    discrepancy_count BIGINT,
    discrepancy_pct   NUMERIC(8,4),
    status            VARCHAR NOT NULL,
    detail            TEXT,
    created_at        TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_recon_dom_ds_check ON pipeline.reconciliation_log(domain, dataset, check_type, created_at);

-- Backfill from existing tables.
INSERT INTO pipeline.run_log (
    run_id, pipeline_type, domain, dataset, business_date,
    status, started_at, ended_at,
    record_count_source, record_count_published,
    kafka_topic, kafka_offset_start, kafka_offset_end,
    config_version_id, schema_version_id, error_summary, created_at
)
SELECT
    g.run_id,
    g.pipeline_type,
    g.domain, g.dataset, g.business_date,
    g.status, g.created_at, g.created_at,
    CASE WHEN g.pipeline_type='ingestion' THEN g.record_count END,
    CASE WHEN g.pipeline_type='publish'   THEN g.record_count END,
    l.target_topic, l.kafka_offset_start, l.kafka_offset_end,
    g.config_version, l.schema_version, g.error_reason, g.created_at
FROM pipeline.glue_job_log g
LEFT JOIN LATERAL (
    SELECT target_topic, kafka_offset_start, kafka_offset_end, schema_version
    FROM pipeline.lineage
    WHERE run_id = g.run_id
    ORDER BY id DESC
    LIMIT 1
) l ON TRUE
ON CONFLICT (run_id) DO NOTHING;

ALTER TABLE pipeline.glue_job_log         RENAME TO glue_job_log_deprecated_2026_04_28;
ALTER TABLE pipeline.lineage              RENAME TO lineage_deprecated_2026_04_28;
ALTER TABLE pipeline.file_state           RENAME TO file_state_deprecated_2026_04_28;
ALTER TABLE pipeline.ingestion_file_state RENAME TO ingestion_file_state_deprecated_2026_04_28;

CREATE VIEW pipeline.v_lineage AS
SELECT
    r.run_id, r.pipeline_type, r.domain, r.dataset, r.business_date,
    fc.s3_raw_path AS source_ref,
    r.kafka_topic, r.kafka_offset_start, r.kafka_offset_end,
    r.config_version_id, r.schema_version_id, r.parents, r.created_at
FROM pipeline.run_log r
LEFT JOIN pipeline.file_catalogue fc ON fc.file_id = r.file_id;

COMMIT;
