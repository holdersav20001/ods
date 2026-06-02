-- 013_restart_identity.sql — make a restarted task reuse its run (P10-A).
--
-- Finding:  docs/reviews/2026-05-30-team-r1-restart.md  (C-1/C-2/C-3, CRITICAL)
-- Probes:   tests/test_team_r1.py  (restart-a-task), tests/test_restart_identity.py
-- Spec:     docs/specs/2026-05-29-control-plane-design-v2.md  (decision #5)
-- Decision: P10-A (this migration) — start_run idempotent on re-run.
--
-- THE BUG (team R1, CONFIRMED):
--   An Airflow "clear-task" re-runs a stage FROM THAT TASK FORWARD under the
--   SAME workflow_run_id. cp.start_run (002) had NO ON CONFLICT, so every call
--   minted a FRESH run_id. A cleared ingest task therefore left TWO succeeded
--   'ingestion' run_log rows for one physical file under one workflow_run_id.
--   The damage lands at run-grain DISCOVERY:
--     * cp.succeeded_runs (merge's 1:N discovery) returns BOTH runs -> the merge
--       hop folds the SAME file in twice -> record_count inflated 2x
--       (double-count of one physical file).
--     * cp.latest_succeeded_run (canonicalize/sink discovery) binds to whichever
--       restart won the finished_at/run_id tie-break -> non-deterministic
--       provenance.
--     * write_lineage_link's 5-part dedup key leads with consumer_run_id; the
--       restart's NEW run_id means the key never conflicts -> a brand-new link
--       is minted, so decision #5's "same task => same link, counts stable"
--       idempotency is structurally unreachable on a real clear-task.
--
-- THE FIX: give a run a restart-aware IDENTITY and make start_run UPSERT on it.
--   A re-run under the same workflow_run_id for the same (pipeline, slice, file)
--   REUSES the existing run_id (reset to a fresh 'running' attempt) instead of
--   minting a new one. Then discovery returns ONE run per (file/slice), merge
--   stops double-counting, and the link's consumer_run_id is stable so
--   write_lineage_link's dedup engages -> same-bytes restart is idempotent.

-- =====================================================================
-- 1. Run-identity unique key (PARTIAL — discovered stages only).
--
-- A run's identity is (workflow_run_id, pipeline_type, slice, file). file_id is
-- NULL for the one-run-per-slice stages (canonicalize/merge/sink) and SET for
-- the one-run-per-file ingest stage; NULLs are DISTINCT in a unique index, so we
-- COALESCE file_id to an all-zero sentinel to make the slice-grained stages
-- collapse correctly on restart.
--
-- WHY PARTIAL (WHERE pipeline_type <> 'sink'):
--   The restart double-count bug is ONLY about runs that a DOWNSTREAM stage
--   DISCOVERS via succeeded_runs / latest_succeeded_run:
--       ingest        -> discovered by canonicalize / merge
--       canonicalize  -> discovered by sink
--       merge         -> discovered by sink
--   SINK is TERMINAL — nothing discovers a sink run — so a duplicate sink run
--   can never cause a downstream double-count. Collapsing sink runs would also
--   break the legitimate fan-out model, where ONE workflow_run_id sinks the same
--   canonical to TWO destinations via TWO fake_sink calls that share
--   (wfid, 'sink', slice, file_id=NULL): today those are two DISTINCT sink runs
--   (one per destination), which is correct. We therefore EXCLUDE sink from the
--   identity key: restart idempotency applies to the discovered stages where the
--   bug lives; sink keeps per-call runs exactly as before, fan-out untouched.
--
-- DEFERRED (NOT P10-A): sink-restart ROW-level idempotency on same content (a
--   restarted sink under a new run mints a new canonical_to_sink link, and
--   write_link_then_rows then writes a second row-set) and the
--   fan-out-as-one-run model with PER-OUTPUT recon are handled later:
--     * P10-C  cp.reconcile_sink_link(link_id, source_count)  — per-output recon
--     * P10-D  target_visibility — deactivate the superseded sink output
--   P10-A intentionally does NOT touch sink restart; only the discovered-run
--   double-count.
-- =====================================================================
CREATE UNIQUE INDEX uq_run_identity ON cp.run_log (
    workflow_run_id, pipeline_type, domain, dataset, business_date,
    COALESCE(file_id, '00000000-0000-0000-0000-000000000000'::uuid)
) WHERE pipeline_type <> 'sink';

-- =====================================================================
-- 2. cp.start_run — idempotent on restart (re-declared here; 002 copy bannered).
--
-- Re-declared (CREATE OR REPLACE) so the live body is THIS one. Same signature
-- and same orchestration-trigger note as 002 (DO NOT change start_run's args).
--
-- ON CONFLICT targets the PARTIAL uq_run_identity index. Postgres requires the
-- inference clause to reproduce the index's predicate, hence the trailing
-- `WHERE pipeline_type <> 'sink'`. For a SINK insert the index does not apply,
-- the arbiter never matches, and the row is inserted fresh exactly as before
-- (no conflict path) — fan-out keeps minting one sink run per destination.
--
-- For a discovered-stage restart the conflict fires and we REUSE the existing
-- run_id, RESETTING it for a fresh attempt: status back to 'running', and
-- finished_at / error cleared so a prior succeeded/failed attempt does not leak
-- into the re-run. trigger_type/replay_of_run_id are intentionally left as the
-- original run's (a clear-task is the SAME logical run, not a new chain).
--
--     ╔══════════════════════════════════════════════════════════════════╗
--     ║ SUPERSEDED by 020_orchestrator_identity.sql.                      ║
--     ║ 020 adds a 9th arg (p_orchestrator jsonb) to cp.start_run, DROPs  ║
--     ║ this 8-arg signature, and re-declares the function to also fill / ║
--     ║ refresh the orchestrator_* columns on INSERT and on the restart   ║
--     ║ ON CONFLICT branch. The 013 body below is otherwise reproduced    ║
--     ║ verbatim in 020. 020 applies last, so the 020 definition is live; ║
--     ║ this copy is kept only for migration history.                     ║
--     ╚══════════════════════════════════════════════════════════════════╝
-- =====================================================================
CREATE OR REPLACE FUNCTION cp.start_run(
    p_workflow_run_id text, p_pipeline_type text, p_domain text, p_dataset text,
    p_business_date date, p_trigger_type text, p_file_id uuid DEFAULT NULL,
    p_replay_of_run_id uuid DEFAULT NULL
) RETURNS uuid LANGUAGE plpgsql AS $$
DECLARE v_run uuid;
BEGIN
    INSERT INTO cp.run_log (workflow_run_id, trigger_type, replay_of_run_id,
                            pipeline_type, domain, dataset, business_date, file_id)
    VALUES (p_workflow_run_id, p_trigger_type, p_replay_of_run_id,
            p_pipeline_type, p_domain, p_dataset, p_business_date, p_file_id)
    ON CONFLICT (workflow_run_id, pipeline_type, domain, dataset, business_date,
                 COALESCE(file_id, '00000000-0000-0000-0000-000000000000'::uuid))
        WHERE pipeline_type <> 'sink'
    DO UPDATE SET status = 'running', finished_at = NULL, error = NULL
    RETURNING run_id INTO v_run;
    RETURN v_run;
END $$;
