-- Migration 23: API pull watermark with two-phase pending/committed cursor.
--
-- Each (domain, dataset, source_application) tracks:
--   committed_cursor_value : last cursor value confirmed downstream (dag_ingest succeeded).
--   pending_cursor_value   : cursor staged after S3 archive but before downstream success.
--   pending_run_id         : api_pull run that owns the pending cursor.
--   last_successful_run_id : last api_pull run promoted to committed.
--   locked_at              : non-null while a poll is in flight; used as advisory lock guard.
--
-- The dag_api_pull post-trigger sensor promotes pending -> committed only
-- after the triggered dag_ingest run finishes successfully. On failure the
-- pending cursor is cleared and the committed cursor is left untouched.

BEGIN;

CREATE TABLE IF NOT EXISTS pipeline.api_pull_watermark (
    domain                  VARCHAR NOT NULL,
    dataset                 VARCHAR NOT NULL,
    source_application      VARCHAR NOT NULL,
    cursor_type             VARCHAR NOT NULL,
    committed_cursor_value  TEXT NULL,
    pending_cursor_value    TEXT NULL,
    pending_run_id          UUID NULL,
    last_successful_run_id  UUID NULL,
    locked_at               TIMESTAMP NULL,
    updated_at              TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (domain, dataset, source_application)
);

CREATE INDEX IF NOT EXISTS idx_api_pull_watermark_pending
    ON pipeline.api_pull_watermark (pending_run_id)
    WHERE pending_run_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_api_pull_watermark_locked
    ON pipeline.api_pull_watermark (locked_at)
    WHERE locked_at IS NOT NULL;

COMMIT;
