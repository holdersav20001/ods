-- 028_discovery_tiebreak.sql — deterministic monotonic tie-break for run-grain
-- discovery (cp.latest_succeeded_run / cp.succeeded_runs).
--
-- Finding/probe: tests/test_discovery_tiebreak.py
-- Builds on:     007_finished_at_clock.sql (clock_timestamp advance),
--                013_restart_identity.sql (restart reuses the run row),
--                020_orchestrator_identity.sql (live cp.start_run signature).
--
-- THE FRAGILITY (root cause, production):
--   cp.latest_succeeded_run and cp.succeeded_runs filter a slice
--   (domain, dataset, business_date, pipeline_type, status='succeeded') and order
--   `finished_at DESC NULLS LAST, run_id DESC`. finished_at is clock_timestamp()
--   (007), which ADVANCES within a transaction — so the common case is already
--   deterministic. BUT clock_timestamp() has finite resolution: two runs can land
--   on the SAME microsecond (under load, or a REFEED whose finished_at coincides
--   with an existing ingest's). On that TIE the secondary key `run_id DESC` is a
--   RANDOM gen_random_uuid() (001) — so discovery picks NON-DETERMINISTICALLY.
--   Consequence: a refeed with a microsecond-tied finished_at can discover / re-
--   canonicalize a STALE ingest, and the canonical/sink hop can bind to an
--   arbitrary upstream. The test layer worked around this with `_settle_order`
--   nudges (tests/test_team_r2.py); 028 fixes it at the SOURCE so the nudges
--   become belt-and-suspenders rather than load-bearing.
--
-- THE FIX:
--   Add a monotonic insert-order key `cp.run_log.seq BIGSERIAL` and change ONLY
--   the discovery ORDER BY secondary key from `run_id DESC` to `seq DESC`. On a
--   true finished_at tie the run CREATED LATER (higher seq) wins deterministically
--   — the correct "latest succeeded" semantics. seq is consulted ONLY to arbitrate
--   genuine ties; whenever finished_at differs (the normal path) finished_at still
--   dominates, so a RESTART (013) — which reuses the row, leaving seq unchanged,
--   but REFRESHES finished_at on re-finalise — still sorts latest by finished_at.
--   The filter, the restart semantics, and the function signatures are UNCHANGED;
--   only the tie-break column changes.

-- =====================================================================
-- 1. Monotonic insert-order key. BIGSERIAL = an owned sequence + a NOT-NULL
--    DEFAULT nextval(); on a fresh `--drop` apply, existing rows (there are none
--    at apply time, but in general) get ascending seq in physical insert order.
--    This is NOT the primary key — run_id stays the PK; seq is purely the
--    deterministic discovery tie-break. cp.start_run does not reference seq, so
--    the INSERT path auto-populates it and the 013 ON CONFLICT DO UPDATE path
--    (restart) leaves the original row's seq untouched — both correct.
-- =====================================================================
ALTER TABLE cp.run_log ADD COLUMN seq BIGSERIAL;

-- Discovery index matching the NEW order (equality cols lead; the two ORDER BY
-- cols trail in matching direction so the scan returns rows pre-sorted -> no Sort
-- node, instant LIMIT 1). Partial on status='succeeded' (a constant in every
-- discovery call) keeps it small. Complements idx_run_log_discovery (004), which
-- trails on run_id; this one trails on seq to serve the new tie-break directly.
CREATE INDEX IF NOT EXISTS ix_run_log_discovery_seq
    ON cp.run_log (domain, dataset, business_date, pipeline_type,
                   finished_at DESC, seq DESC)
    WHERE status = 'succeeded';

-- =====================================================================
-- 2. Re-declare the two discovery functions changing ONLY the ORDER BY
--    secondary key: `run_id DESC` -> `seq DESC`. Same 4-arg signatures, same
--    filter, same return types -> CREATE OR REPLACE (no DROP needed).
--    SUPERSEDED banners go on the PRIOR live copies (002 / 005) per convention.
-- =====================================================================

-- cp.latest_succeeded_run — newest succeeded run for the slice, null if none.
--   ╔══════════════════════════════════════════════════════════════════════╗
--   ║ The live copy of cp.latest_succeeded_run is HERE (028), NOT 002.       ║
--   ║ 002's body ordered `finished_at DESC NULLS LAST, run_id DESC` (random  ║
--   ║ uuid tie-break). 028 re-declares it (CREATE OR REPLACE, same signature)║
--   ║ changing ONLY the tie-break to `seq DESC`. Edit the body HERE.         ║
--   ╚══════════════════════════════════════════════════════════════════════╝
CREATE OR REPLACE FUNCTION cp.latest_succeeded_run(
    p_domain text, p_dataset text, p_business_date date, p_pipeline_type text
) RETURNS uuid LANGUAGE plpgsql AS $$
DECLARE v_run uuid;
BEGIN
    SELECT run_id INTO v_run FROM cp.run_log
    WHERE domain = p_domain AND dataset = p_dataset
      AND business_date = p_business_date AND pipeline_type = p_pipeline_type
      AND status = 'succeeded'
    ORDER BY finished_at DESC NULLS LAST, seq DESC
    LIMIT 1;
    RETURN v_run;
END $$;

-- cp.succeeded_runs — ALL succeeded runs for the slice, newest-first.
--   ╔══════════════════════════════════════════════════════════════════════╗
--   ║ The live copy of cp.succeeded_runs is HERE (028), NOT 005.             ║
--   ║ 005's body ordered `finished_at DESC NULLS LAST, run_id DESC`. 028     ║
--   ║ re-declares it (CREATE OR REPLACE, same signature) changing ONLY the   ║
--   ║ tie-break to `seq DESC`. Edit the body HERE.                           ║
--   ╚══════════════════════════════════════════════════════════════════════╝
CREATE OR REPLACE FUNCTION cp.succeeded_runs(
    p_domain text, p_dataset text, p_business_date date, p_pipeline_type text
) RETURNS SETOF uuid LANGUAGE sql STABLE AS $$
    SELECT run_id FROM cp.run_log
    WHERE domain=p_domain AND dataset=p_dataset AND business_date=p_business_date
      AND pipeline_type=p_pipeline_type AND status='succeeded'
    ORDER BY finished_at DESC NULLS LAST, seq DESC;
$$;
