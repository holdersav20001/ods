-- Migration 24: dataset_config support for api_pull source type.
--
-- - raw_format      : how the raw archive on S3 should be read by the
--                     ingestion job (csv | jsonl | parquet). Existing
--                     file-pattern datasets default to csv.
-- - source_config   : JSONB blob carrying source-system specifics for
--                     api_pull and future patterns (URL, cursor style,
--                     auth secret reference, paging style, retries).
--                     MUST NOT contain secret material — only secret_ref.
-- - filename_pattern is relaxed to nullable so api_pull datasets, which
--   have no upstream filename, can be onboarded without synthetic values.

BEGIN;

ALTER TABLE pipeline.dataset_config
    ADD COLUMN IF NOT EXISTS raw_format    VARCHAR NOT NULL DEFAULT 'csv',
    ADD COLUMN IF NOT EXISTS source_config JSONB   NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE pipeline.dataset_config
    ALTER COLUMN filename_pattern DROP NOT NULL;

COMMIT;
