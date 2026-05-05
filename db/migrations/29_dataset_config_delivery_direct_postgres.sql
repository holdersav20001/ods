-- Migration 29: extend dataset_config.delivery to allow direct_postgres.
--
-- See docs/file-direct-postgres-design.md. Adds a third delivery shape
-- alongside the existing 'file_pipeline' and 'direct_kafka':
--
--   direct_postgres  — Glue ingestion writes curated parquet, then a
--                      new ods_postgres_write Glue job writes Postgres
--                      rows directly. Skips Kafka publish, canonicalize-
--                      via-Kafka, and the JDBC Connect sink.

BEGIN;

ALTER TABLE pipeline.dataset_config
    DROP CONSTRAINT IF EXISTS dataset_config_delivery_chk;

ALTER TABLE pipeline.dataset_config
    ADD CONSTRAINT dataset_config_delivery_chk
    CHECK (delivery IN ('file_pipeline', 'direct_kafka', 'direct_postgres'));

-- direct_postgres datasets have no Kafka leg, so target_topic must be
-- NULLable. yaml_loader still enforces NOT NULL when delivery is
-- file_pipeline or direct_kafka.
ALTER TABLE pipeline.dataset_config
    ALTER COLUMN target_topic DROP NOT NULL;

COMMIT;
