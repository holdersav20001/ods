-- Migration 28: dataset_config.delivery for direct-Kafka api_pull routing.
--
-- Today every api_pull dataset goes through the file pipeline:
--   poll -> S3 raw -> Glue -> Parquet -> raw Kafka -> ... -> Postgres
--
-- The direct-Kafka shape (docs/api-pull-direct-kafka-design.md) needs
-- the dispatcher in dag_api_pull (build phase) to route a dataset to a
-- long-running publisher instead of poll_one + TriggerDagRunOperator.
-- Storing the choice on the row keeps dashboards and tooling table-
-- backed rather than YAML-only.
--
-- Existing rows default to 'file_pipeline'; no behaviour change until
-- a dataset is explicitly switched.

BEGIN;

ALTER TABLE pipeline.dataset_config
    ADD COLUMN IF NOT EXISTS delivery VARCHAR NOT NULL DEFAULT 'file_pipeline';

ALTER TABLE pipeline.dataset_config
    ADD CONSTRAINT dataset_config_delivery_chk
    CHECK (delivery IN ('file_pipeline', 'direct_kafka'));

COMMIT;
