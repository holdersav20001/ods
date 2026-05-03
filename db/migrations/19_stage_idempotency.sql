-- Migration 19: idempotency for stage_started events
--
-- Plan §Task 3 (B4) calls for `UNIQUE (run_id, stage, attempt_number,
-- event_type) DO NOTHING` on every stage INSERT.  Implementing that literally
-- would regress the T2 acceptance test
-- (`tests/unit/test_stages_concurrency.py`), because the post-T2 finish()
-- contract intentionally allows two concurrent finishers to produce two rows
-- with `event_type='stage_completed'`.
--
-- The stated INTENT of §3.5 is "idempotent retries".  The only retry path that
-- a unique constraint should de-duplicate is `stages.start()` re-invocation —
-- terminal events (`stage_completed`, `stage_failed`, etc.) are append-only by
-- design.  This partial unique index targets that intent without breaking T2.
--
-- Idempotency contract (post-this-migration):
--   * One open `stage_started` row per `(run_id, stage, attempt_number)`.
--   * `stages.start()` becomes safe to retry; the second call hits ON CONFLICT
--     DO NOTHING and is a no-op.
--   * Terminal events (`stage_completed`, `stage_failed`, `stage_skipped`,
--     `stage_warned`) remain unconstrained — append-only.
--
-- Data safety: no historic dedupe needed.  `stage_started` rows currently only
-- arise from explicit `stages.start()` calls, which are not retried under any
-- known caller path (Glue jobs / DAG tasks invoke start exactly once per
-- attempt).  If the index creation fails with a duplicate-key error, that
-- indicates a previously-undetected double-start bug worth investigating
-- rather than silently dropping rows.
--
-- The migration runner in this repo wraps each file in a transaction, so
-- CONCURRENTLY is omitted (incompatible with explicit BEGIN).

BEGIN;

CREATE UNIQUE INDEX IF NOT EXISTS run_stage_log_started_unique
    ON pipeline.run_stage_log (run_id, stage, attempt_number)
    WHERE event_type = 'stage_started';

COMMENT ON INDEX pipeline.run_stage_log_started_unique IS
    'B4 partial unique index — one open stage_started row per attempt. '
    'Allows ON CONFLICT DO NOTHING in stages.start() for idempotent retries '
    'without constraining append-only terminal events.';

COMMIT;
