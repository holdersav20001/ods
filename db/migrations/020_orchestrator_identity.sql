-- 020_orchestrator_identity.sql — F1: capture the EXTERNAL orchestrator identity
-- (Airflow today) on each control-plane run, and let cp.start_run record it.
--
-- Spec:   docs/specs/2026-06-02-airflow-orchestrator-policy-claims-workflow.md
--         ("Migration 020", "cp.start_run Change").
-- Probes: tests/test_orchestrator_identity.py,
--         tests/test_contract.py::test_start_run_populates_orchestrator_columns.
--
-- WHY:
--   The control-plane owns the LOGICAL run identity (workflow_run_id, pipeline,
--   slice, file — see 013's uq_run_identity). The orchestrator (Airflow) owns a
--   SEPARATE execution identity (dag_id / dag-run / task / try). We want to
--   correlate the two without letting the orchestrator's identity LEAK into the
--   control-plane's restart semantics: an Airflow clear-task retry is the SAME
--   logical run (013 reuses the run_id), it just bumps try_number/url. So we:
--     (1) add nullable orchestrator_* columns (purely additive, existing rows and
--         tests keep working — orchestrator_payload defaults to {}),
--     (2) index the orchestrator identity for lookup but DO NOT make it UNIQUE
--         (mapping is many-attempts -> one logical run; uniqueness on Airflow
--         identity would forbid the very restart 013 makes idempotent),
--     (3) re-declare cp.start_run with a trailing p_orchestrator jsonb arg that
--         fills the columns on INSERT and REFRESHES them on the 013 restart path.
--
-- This migration is purely additive at the table level and DOES NOT edit applied
-- migrations 001-019. It changes cp.start_run's SIGNATURE (adds a 9th arg), so
-- the old 8-arg function is DROPPED first; the live copy in 013 is bannered
-- SUPERSEDED. 020 applies last, so this definition wins.

-- =====================================================================
-- 1. Orchestrator identity columns (nullable; payload defaults to {}).
--    The 8 spec columns, no more no less. scalar cols stay NULL when no
--    orchestrator is supplied; orchestrator_payload holds the WHOLE supplied
--    object so future Airflow fields are not lost.
-- =====================================================================
ALTER TABLE cp.run_log
  ADD COLUMN IF NOT EXISTS orchestrator_type text,
  ADD COLUMN IF NOT EXISTS orchestrator_dag_id text,
  ADD COLUMN IF NOT EXISTS orchestrator_run_id text,
  ADD COLUMN IF NOT EXISTS orchestrator_task_id text,
  ADD COLUMN IF NOT EXISTS orchestrator_try_number integer,
  ADD COLUMN IF NOT EXISTS orchestrator_map_index integer,
  ADD COLUMN IF NOT EXISTS orchestrator_url text,
  ADD COLUMN IF NOT EXISTS orchestrator_payload jsonb NOT NULL DEFAULT '{}'::jsonb;

-- =====================================================================
-- 2. Lookup indexes on orchestrator identity. NON-UNIQUE on purpose: many
--    Airflow attempts (retries / mapped tasks) map onto ONE logical run; a
--    unique constraint here would forbid the restart reuse 013 introduced.
-- =====================================================================
CREATE INDEX IF NOT EXISTS ix_run_log_orchestrator_run
  ON cp.run_log (orchestrator_type, orchestrator_dag_id, orchestrator_run_id);

CREATE INDEX IF NOT EXISTS ix_run_log_orchestrator_task
  ON cp.run_log (
    orchestrator_type,
    orchestrator_dag_id,
    orchestrator_run_id,
    orchestrator_task_id
  );

-- =====================================================================
-- 3. cp.start_run — re-declared with p_orchestrator (9th arg).
--
--    Adding a parameter CHANGES the function signature, so CREATE OR REPLACE
--    would create a SECOND overload rather than replace the 8-arg one. Drop the
--    old 8-arg signature explicitly first so there is exactly ONE cp.start_run.
--
--    Body = 013 behaviour (restart-identity upsert on the partial uq_run_identity
--    index, WHERE pipeline_type <> 'sink') PLUS the orchestrator columns:
--      * on INSERT: fill the 7 scalar cols from p_orchestrator's keys and store
--        the WHOLE object in orchestrator_payload (an empty {} default means the
--        scalars come out NULL and payload {} — the no-orchestrator case).
--      * on the ON CONFLICT restart branch: alongside the existing
--        status='running', finished_at=NULL, error=NULL reset, ALSO refresh all 8
--        orchestrator cols from the NEW call, so an Airflow clear-task retry
--        updates try_number / url on the SAME logical run.
--    nullif(...,'')::int guards the two integer cols against an empty-string JSON
--    extraction (->>'' on a missing/null key yields NULL, not '', but nullif is
--    belt-and-braces and matches the spec).
-- =====================================================================
DROP FUNCTION IF EXISTS cp.start_run(text, text, text, text, date, text, uuid, uuid);

CREATE OR REPLACE FUNCTION cp.start_run(
    p_workflow_run_id text, p_pipeline_type text, p_domain text, p_dataset text,
    p_business_date date, p_trigger_type text, p_file_id uuid DEFAULT NULL,
    p_replay_of_run_id uuid DEFAULT NULL,
    p_orchestrator jsonb DEFAULT '{}'::jsonb
) RETURNS uuid LANGUAGE plpgsql AS $$
DECLARE v_run uuid;
BEGIN
    INSERT INTO cp.run_log (workflow_run_id, trigger_type, replay_of_run_id,
                            pipeline_type, domain, dataset, business_date, file_id,
                            orchestrator_type, orchestrator_dag_id, orchestrator_run_id,
                            orchestrator_task_id, orchestrator_try_number,
                            orchestrator_map_index, orchestrator_url, orchestrator_payload)
    VALUES (p_workflow_run_id, p_trigger_type, p_replay_of_run_id,
            p_pipeline_type, p_domain, p_dataset, p_business_date, p_file_id,
            p_orchestrator->>'type', p_orchestrator->>'dag_id', p_orchestrator->>'run_id',
            p_orchestrator->>'task_id', nullif(p_orchestrator->>'try_number','')::int,
            nullif(p_orchestrator->>'map_index','')::int, p_orchestrator->>'url',
            coalesce(p_orchestrator, '{}'::jsonb))
    ON CONFLICT (workflow_run_id, pipeline_type, domain, dataset, business_date,
                 COALESCE(file_id, '00000000-0000-0000-0000-000000000000'::uuid))
        WHERE pipeline_type <> 'sink'
    DO UPDATE SET
        status = 'running', finished_at = NULL, error = NULL,
        orchestrator_type       = p_orchestrator->>'type',
        orchestrator_dag_id     = p_orchestrator->>'dag_id',
        orchestrator_run_id     = p_orchestrator->>'run_id',
        orchestrator_task_id    = p_orchestrator->>'task_id',
        orchestrator_try_number = nullif(p_orchestrator->>'try_number','')::int,
        orchestrator_map_index  = nullif(p_orchestrator->>'map_index','')::int,
        orchestrator_url        = p_orchestrator->>'url',
        orchestrator_payload    = coalesce(p_orchestrator, '{}'::jsonb)
    RETURNING run_id INTO v_run;
    RETURN v_run;
END $$;
