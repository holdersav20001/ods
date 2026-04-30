-- Migration 14: explicit lineage_edge table + enrich pipeline.run_events

-- ── lineage_edge ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS pipeline.lineage_edge (
    lineage_edge_id  BIGSERIAL PRIMARY KEY,
    child_run_id     UUID NOT NULL REFERENCES pipeline.run_log(run_id),
    parent_run_id    UUID REFERENCES pipeline.run_log(run_id),
    parent_file_id   UUID REFERENCES pipeline.file_catalogue(file_id),
    edge_type        VARCHAR NOT NULL,   -- raw_to_curated | curated_to_kafka | curated_to_postgres
    source_ref       TEXT,               -- S3 raw path or curated path
    target_ref       TEXT,               -- S3 curated path, Kafka topic, or Postgres table
    record_count     BIGINT,
    created_at       TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS lineage_edge_child_run_idx   ON pipeline.lineage_edge (child_run_id);
CREATE INDEX IF NOT EXISTS lineage_edge_file_id_idx     ON pipeline.lineage_edge (parent_file_id);

-- ── enrich pipeline.run_events ───────────────────────────────────────────────
ALTER TABLE pipeline.run_events
    ADD COLUMN IF NOT EXISTS file_id             VARCHAR,
    ADD COLUMN IF NOT EXISTS s3_raw_path         TEXT,
    ADD COLUMN IF NOT EXISTS s3_curated_path     TEXT,
    ADD COLUMN IF NOT EXISTS file_md5            VARCHAR,
    ADD COLUMN IF NOT EXISTS kafka_offset_start  BIGINT,
    ADD COLUMN IF NOT EXISTS stages              JSONB;

CREATE INDEX IF NOT EXISTS run_events_file_id_idx ON pipeline.run_events (file_id);
