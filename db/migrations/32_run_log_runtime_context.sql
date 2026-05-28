-- 32_run_log_runtime_context.sql
BEGIN;

ALTER TABLE pipeline.run_log
    ADD COLUMN IF NOT EXISTS runtime_context JSONB;

CREATE INDEX IF NOT EXISTS idx_run_log_runtime_context_gin
    ON pipeline.run_log USING GIN (runtime_context)
    WHERE runtime_context IS NOT NULL;

COMMIT;
