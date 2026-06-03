-- 005_merge_discovery.sql — multi-upstream discovery for the merge hop.
--
-- The merge hop (merge_to_canonical) is the 1:N lineage case: ONE canonical
-- link produced from N upstream ingest runs. cp.latest_succeeded_run returns
-- only ONE run, which is correct for the canonicalize hop but insufficient for
-- merge: merge needs ALL succeeded upstream runs for the slice. This function
-- supplies that set, ordered newest-first (same ordering contract as
-- latest_succeeded_run: finished_at DESC NULLS LAST, run_id DESC).
-- SUPERSEDED by 028_discovery_tiebreak.sql: the ORDER BY tie-break changes from
-- `run_id DESC` (random uuid) to `seq DESC` (monotonic insert order), matching
-- latest_succeeded_run's 028 re-declare. 028 applies last (CREATE OR REPLACE,
-- same signature), so the 028 body is live; this copy is kept only for migration
-- history. Edit the body in 028, NOT here.
CREATE OR REPLACE FUNCTION cp.succeeded_runs(
    p_domain text, p_dataset text, p_business_date date, p_pipeline_type text
) RETURNS SETOF uuid LANGUAGE sql STABLE AS $$
    SELECT run_id FROM cp.run_log
    WHERE domain=p_domain AND dataset=p_dataset AND business_date=p_business_date
      AND pipeline_type=p_pipeline_type AND status='succeeded'
    ORDER BY finished_at DESC NULLS LAST, run_id DESC;
$$;
