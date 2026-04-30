-- Migration 13: extend pipeline.run_events with richer pipeline fields

ALTER TABLE pipeline.run_events
    ADD COLUMN IF NOT EXISTS pipeline_type          VARCHAR,
    ADD COLUMN IF NOT EXISTS record_count_source    INTEGER,
    ADD COLUMN IF NOT EXISTS record_count_dq_pass   INTEGER,
    ADD COLUMN IF NOT EXISTS record_count_dq_fail   INTEGER,
    ADD COLUMN IF NOT EXISTS error_summary          TEXT;

CREATE INDEX IF NOT EXISTS run_events_status_idx ON pipeline.run_events (status);
