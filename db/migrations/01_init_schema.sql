CREATE SCHEMA IF NOT EXISTS pipeline;

CREATE TABLE pipeline.dataset_config (
    id                  SERIAL PRIMARY KEY,
    domain              VARCHAR NOT NULL,
    dataset             VARCHAR NOT NULL,
    filename_pattern    VARCHAR NOT NULL,
    target_topic        VARCHAR NOT NULL,
    schema_id           VARCHAR NOT NULL,
    schema_version      INTEGER NOT NULL DEFAULT 1,
    key_fields          JSONB NOT NULL,
    dq_rules            JSONB NOT NULL DEFAULT '{}',
    data_classification VARCHAR NOT NULL DEFAULT 'Internal',
    active              BOOLEAN DEFAULT TRUE,
    version             INTEGER NOT NULL DEFAULT 1,
    created_at          TIMESTAMP DEFAULT NOW(),
    updated_at          TIMESTAMP DEFAULT NOW(),
    UNIQUE (domain, dataset)
);

CREATE TABLE pipeline.file_catalogue (
    id                SERIAL PRIMARY KEY,
    name_pattern      VARCHAR NOT NULL,
    sftp_path         VARCHAR NOT NULL DEFAULT '',
    domain            VARCHAR NOT NULL,
    dataset           VARCHAR NOT NULL,
    dataset_config_id INTEGER NOT NULL REFERENCES pipeline.dataset_config(id),
    active            BOOLEAN DEFAULT TRUE,
    created_at        TIMESTAMP DEFAULT NOW()
);

CREATE TABLE pipeline.file_state (
    id            SERIAL PRIMARY KEY,
    s3_path       VARCHAR NOT NULL UNIQUE,
    run_id        UUID NOT NULL,
    status        VARCHAR NOT NULL CHECK (status IN ('new','processing','completed','failed')),
    record_count  INTEGER,
    error_reason  VARCHAR,
    created_at    TIMESTAMP DEFAULT NOW(),
    updated_at    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE pipeline.ingestion_file_state (
    id            SERIAL PRIMARY KEY,
    s3_path       VARCHAR NOT NULL UNIQUE,
    status        VARCHAR NOT NULL CHECK (status IN ('detected','transferred','failed')),
    checksum_md5  VARCHAR,
    created_at    TIMESTAMP DEFAULT NOW(),
    updated_at    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE pipeline.glue_job_log (
    id              BIGSERIAL PRIMARY KEY,
    run_id          UUID NOT NULL,
    job_name        VARCHAR NOT NULL,
    pipeline_type   VARCHAR NOT NULL CHECK (pipeline_type IN ('ingestion','publish')),
    domain          VARCHAR NOT NULL,
    dataset         VARCHAR NOT NULL,
    source_path     VARCHAR,
    target_path     VARCHAR,
    business_date   DATE,
    status          VARCHAR NOT NULL,
    record_count    INTEGER,
    error_reason    VARCHAR,
    error_detail    TEXT,
    config_version  INTEGER,
    config_snapshot JSONB,
    created_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE pipeline.lineage (
    id                  BIGSERIAL PRIMARY KEY,
    run_id              UUID NOT NULL,
    domain              VARCHAR NOT NULL,
    dataset             VARCHAR NOT NULL,
    source_type         VARCHAR NOT NULL CHECK (source_type IN ('file','cdc','api','event')),
    source_ref          VARCHAR NOT NULL,
    target_topic        VARCHAR NOT NULL,
    business_date       DATE,
    kafka_offset_start  BIGINT,
    kafka_offset_end    BIGINT,
    record_count        INTEGER,
    schema_version      INTEGER,
    created_at          TIMESTAMP DEFAULT NOW()
);
