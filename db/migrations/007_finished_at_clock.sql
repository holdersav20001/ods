-- 007_finished_at_clock.sql — discovery-determinism fix (bug found in P3d).
--
-- BUG: cp.patch_run and cp.finish_stage stamped finished_at with now(), which
-- is transaction_timestamp() — FIXED for the whole transaction. Two runs that
-- start AND finalise inside ONE transaction therefore get IDENTICAL
-- finished_at values, so cp.latest_succeeded_run / cp.succeeded_runs
-- (ORDER BY finished_at DESC NULLS LAST, run_id DESC) tiebreak on the random
-- run_id UUID — i.e. discovery is NON-DETERMINISTIC in-transaction (the X5
-- replay test had to use a committing connection to work around this).
--
-- FIX: stamp finished_at with clock_timestamp(), which ADVANCES within a
-- transaction (real wall-clock at statement execution). Two runs finalised in
-- sequence — even in the same transaction — then get strictly increasing
-- finished_at, making discovery deterministic everywhere.
--
-- ONLY the finished_at assignment changes; every other line is identical to the
-- 002 definitions (CREATE OR REPLACE, no signature change).

-- cp.patch_run: update only whitelisted keys present in p_patch.
-- Whitelist: status, record_count_in, record_count_out, error.
-- Terminal status ('succeeded'/'failed') also stamps finished_at = clock_timestamp().
CREATE OR REPLACE FUNCTION cp.patch_run(
    p_run_id uuid, p_patch jsonb
) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    UPDATE cp.run_log SET
        status           = coalesce(p_patch->>'status', status),
        record_count_in  = coalesce((p_patch->>'record_count_in')::bigint, record_count_in),
        record_count_out = coalesce((p_patch->>'record_count_out')::bigint, record_count_out),
        error            = CASE WHEN p_patch ? 'error' THEN p_patch->>'error' ELSE error END,
        finished_at      = CASE WHEN p_patch->>'status' IN ('succeeded','failed')
                                THEN clock_timestamp() ELSE finished_at END
    WHERE run_id = p_run_id;
END $$;

-- cp.finish_stage: stamp status, counts, metrics, finished_at on the stage row.
CREATE OR REPLACE FUNCTION cp.finish_stage(
    p_stage_log_id bigint, p_status text, p_in bigint, p_out bigint, p_metrics jsonb
) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    UPDATE cp.run_stage_log SET
        status = p_status, record_count_in = p_in, record_count_out = p_out,
        metrics = p_metrics, finished_at = clock_timestamp()
    WHERE stage_log_id = p_stage_log_id;
END $$;
