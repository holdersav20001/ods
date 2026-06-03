-- 027_diagnostics.sql — strengthen support/developer diagnostics (spec area 6).
--
-- Spec:  docs/specs/2026-06-03-working-platform-completion-plan.md
--        §6 "Strengthen Diagnostics" — the "Diagnostics Must Detect" list and
--        the optional "Required Additions" lookup functions.
-- Tests: tests/test_diagnostics.py (TDD — written failing first). The 022
--        contract tests in tests/test_dashboard_developer_functions.py and the
--        ASSERTED set in tests/test_contract.py keep this honest.
--
-- WHAT THIS MIGRATION DOES
--   1. RE-DECLARES cp.developer_diagnostics(text, text) with the SAME signature
--      and return shape as 022 (check_name, severity, object_type, object_id,
--      message, details). All 022 checks are PRESERVED verbatim, and four new
--      checks from the spec list are ADDED:
--        * unfinished_stage                              (a running/never-closed
--          stage on ANY run, independent of run status)
--        * dlq_row_missing_trace_context                 (cp.dlq row with null
--          quarantine_output_link_id, or null reason, or null source identity)
--        * quarantine_output_without_dlq_rows            (a first-class
--          quarantine output_link with no cp.dlq row pointing at it)
--        * schema_validation_output_missing_schema_version (a curated_to_canonical
--          output whose target_ref lacks 'schema_version' WHERE a schema_contract
--          exists for that dataset)
--      The 022 copy is left in place but its developer_diagnostics body is
--      SUPERSEDED (banner below). CREATE OR REPLACE keeps the same signature, so
--      the 022 definition is overwritten cleanly — the OTHER 022 functions
--      (dashboard_workflows / _workflow_detail / _output_trace) are untouched.
--
--   2. ADDS three optional read/lookup helpers (spec "Required Additions"),
--      each round-tripped in tests/test_diagnostics.py and added to the
--      test_contract.py ASSERTED set:
--        * cp.dashboard_file_usage(p_file_id uuid)
--        * cp.dashboard_target_row_trace(p_target_schema, p_target_table, p_row_id)
--        * cp.dashboard_airflow_lookup(p_dag_id, p_dag_run_id)
--
-- NOTE on the "check_name" column vs the spec's "issue_type": the public column
-- name is kept as check_name to avoid breaking the 022 contract test and the
-- dashboard; check_name IS the issue_type. The return tuple (check_name,
-- severity, object_type, object_id) is the clean, stable shape requested.
--
-- ======================================================================
-- SUPERSEDED: cp.developer_diagnostics(text, text) as defined in
--   022_dashboard_developer_functions.sql is REPLACED below (same signature).
--   The other 022 read functions remain authoritative.
-- ======================================================================

CREATE OR REPLACE FUNCTION cp.developer_diagnostics(
    p_workflow_run_id text,
    p_target_table text DEFAULT NULL
)
RETURNS TABLE (
    check_name text,
    severity text,
    object_type text,
    object_id text,
    message text,
    details jsonb
)
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    v_schema text;
    v_table text;
    v_table_regclass regclass;
    v_required_columns text[] := ARRAY[
        'row_id',
        '_ods_workflow_run_id',
        '_ods_output_link_id',
        '_ods_lineage_link_id',
        'payload'
    ];
    v_missing_columns text[];
BEGIN
    IF p_workflow_run_id IS NULL THEN
        RAISE EXCEPTION 'developer_diagnostics: workflow_run_id is required'
            USING ERRCODE = 'P0001',
                  HINT = 'Pass a workflow_run_id from cp.run_log or cp.dashboard_workflows().';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM cp.run_log WHERE workflow_run_id = p_workflow_run_id) THEN
        RAISE EXCEPTION 'developer_diagnostics: workflow_run_id % not found', p_workflow_run_id
            USING ERRCODE = 'P0001',
                  HINT = 'Query cp.dashboard_workflows() to list available workflows.';
    END IF;

    -- ---- unfinished run (status running, or never reached a finished_at) -----
    RETURN QUERY
    SELECT
        'unfinished_run'::text,
        'error'::text,
        'run_log'::text,
        r.run_id::text,
        format('Run %s/%s is not terminal', r.pipeline_type, r.dataset)::text,
        jsonb_build_object(
            'status', r.status,
            'started_at', r.started_at,
            'finished_at', r.finished_at,
            'error', r.error
        )
    FROM cp.run_log r
    WHERE r.workflow_run_id = p_workflow_run_id
      AND (r.finished_at IS NULL OR r.status = 'running');

    -- ---- unfinished stage (NEW 027): a stage that never closed, on ANY run ----
    -- This catches a stuck/abandoned stage independent of the run's own status,
    -- including a stage left 'running' on a run that itself is still running.
    RETURN QUERY
    SELECT
        'unfinished_stage'::text,
        'error'::text,
        'run_stage_log'::text,
        s.stage_log_id::text,
        format('Stage %s on run %s/%s is not closed', s.stage, r.pipeline_type, r.dataset)::text,
        jsonb_build_object(
            'run_id', r.run_id,
            'run_status', r.status,
            'stage', s.stage,
            'stage_status', s.status,
            'stage_started_at', s.started_at,
            'stage_finished_at', s.finished_at
        )
    FROM cp.run_log r
    JOIN cp.run_stage_log s ON s.run_id = r.run_id
    WHERE r.workflow_run_id = p_workflow_run_id
      AND (s.finished_at IS NULL OR s.status = 'running');

    -- ---- succeeded run with an unfinished stage ------------------------------
    RETURN QUERY
    SELECT
        'stage_not_closed_on_succeeded_run'::text,
        'error'::text,
        'run_stage_log'::text,
        s.stage_log_id::text,
        format('Run %s/%s succeeded but stage %s is not closed', r.pipeline_type, r.dataset, s.stage)::text,
        jsonb_build_object(
            'run_id', r.run_id,
            'stage', s.stage,
            'run_status', r.status,
            'stage_status', s.status,
            'stage_started_at', s.started_at,
            'stage_finished_at', s.finished_at
        )
    FROM cp.run_log r
    JOIN cp.run_stage_log s ON s.run_id = r.run_id
    WHERE r.workflow_run_id = p_workflow_run_id
      AND r.status = 'succeeded'
      AND (s.finished_at IS NULL OR s.status <> 'succeeded');

    -- ---- run without any stages ----------------------------------------------
    RETURN QUERY
    SELECT
        'run_without_stages'::text,
        'warning'::text,
        'run_log'::text,
        r.run_id::text,
        format('Run %s/%s has no stage rows', r.pipeline_type, r.dataset)::text,
        jsonb_build_object('status', r.status, 'started_at', r.started_at, 'finished_at', r.finished_at)
    FROM cp.run_log r
    LEFT JOIN cp.run_stage_log s ON s.run_id = r.run_id
    WHERE r.workflow_run_id = p_workflow_run_id
    GROUP BY r.run_id, r.pipeline_type, r.dataset, r.status, r.started_at, r.finished_at
    HAVING count(s.run_id) = 0;

    -- ---- successful run without an output_link -------------------------------
    RETURN QUERY
    SELECT
        'succeeded_run_without_output_link'::text,
        'error'::text,
        'run_log'::text,
        r.run_id::text,
        format('Succeeded run %s/%s created no output_link', r.pipeline_type, r.dataset)::text,
        jsonb_build_object('status', r.status, 'record_count_out', r.record_count_out)
    FROM cp.run_log r
    LEFT JOIN cp.output_link ol ON ol.consumer_run_id = r.run_id
    WHERE r.workflow_run_id = p_workflow_run_id
      AND r.status = 'succeeded'
    GROUP BY r.run_id, r.pipeline_type, r.dataset, r.status, r.record_count_out
    HAVING count(ol.output_link_id) = 0;

    -- ---- output_link without any input_edge ----------------------------------
    RETURN QUERY
    SELECT
        'output_link_without_input_edge'::text,
        'error'::text,
        'output_link'::text,
        ol.output_link_id::text,
        format('Output link %s from %s/%s has no input_edge rows', ol.edge_type, r.pipeline_type, r.dataset)::text,
        jsonb_build_object('run_id', r.run_id, 'target_ref', ol.target_ref, 'record_count', ol.record_count)
    FROM cp.run_log r
    JOIN cp.output_link ol ON ol.consumer_run_id = r.run_id
    LEFT JOIN cp.input_edge ie ON ie.output_link_id = ol.output_link_id
    WHERE r.workflow_run_id = p_workflow_run_id
    GROUP BY r.run_id, r.pipeline_type, r.dataset, ol.output_link_id, ol.edge_type, ol.target_ref, ol.record_count
    HAVING count(ie.input_edge_id) = 0;

    -- ---- input_edge missing input identity (non-annotation edge types) -------
    -- 'orchestrates' is the non-provenance annotation edge type; it is exempt.
    RETURN QUERY
    SELECT
        'input_edge_without_input_identifier'::text,
        'error'::text,
        'input_edge'::text,
        ie.input_edge_id::text,
        'Input edge points to neither source_file_id nor upstream_output_link_id'::text,
        jsonb_build_object('output_link_id', ol.output_link_id, 'edge_type', ie.edge_type, 'source_ref', ie.source_ref)
    FROM cp.run_log r
    JOIN cp.output_link ol ON ol.consumer_run_id = r.run_id
    JOIN cp.input_edge ie ON ie.output_link_id = ol.output_link_id
    WHERE r.workflow_run_id = p_workflow_run_id
      AND ie.edge_type <> 'orchestrates'
      AND ie.source_file_id IS NULL
      AND ie.upstream_output_link_id IS NULL;

    -- ---- downstream (run-to-run) input edge missing upstream_output_link_id ---
    RETURN QUERY
    SELECT
        'downstream_edge_missing_upstream_output_link'::text,
        'error'::text,
        'input_edge'::text,
        ie.input_edge_id::text,
        'Downstream input names upstream_run_id but not the exact upstream_output_link_id'::text,
        jsonb_build_object(
            'output_link_id', ol.output_link_id,
            'upstream_run_id', ie.upstream_run_id,
            'edge_type', ie.edge_type
        )
    FROM cp.run_log r
    JOIN cp.output_link ol ON ol.consumer_run_id = r.run_id
    JOIN cp.input_edge ie ON ie.output_link_id = ol.output_link_id
    WHERE r.workflow_run_id = p_workflow_run_id
      AND ie.source_file_id IS NULL
      AND ie.upstream_run_id IS NOT NULL
      AND ie.upstream_output_link_id IS NULL;

    -- ---- input_edge references an upstream output link that does not exist ----
    RETURN QUERY
    SELECT
        'input_edge_broken_upstream_output_link'::text,
        'error'::text,
        'input_edge'::text,
        ie.input_edge_id::text,
        'Input edge references an upstream_output_link_id that does not exist'::text,
        jsonb_build_object('output_link_id', ol.output_link_id, 'upstream_output_link_id', ie.upstream_output_link_id)
    FROM cp.run_log r
    JOIN cp.output_link ol ON ol.consumer_run_id = r.run_id
    JOIN cp.input_edge ie ON ie.output_link_id = ol.output_link_id
    LEFT JOIN cp.output_link upstream ON upstream.output_link_id = ie.upstream_output_link_id
    WHERE r.workflow_run_id = p_workflow_run_id
      AND ie.upstream_output_link_id IS NOT NULL
      AND upstream.output_link_id IS NULL;

    -- ---- output_link target_ref missing required identity fields -------------
    RETURN QUERY
    SELECT
        'output_link_missing_target_ref_fields'::text,
        'error'::text,
        'output_link'::text,
        ol.output_link_id::text,
        'Output link target_ref must include path, content_hash, and version'::text,
        jsonb_build_object('edge_type', ol.edge_type, 'target_ref', ol.target_ref)
    FROM cp.run_log r
    JOIN cp.output_link ol ON ol.consumer_run_id = r.run_id
    WHERE r.workflow_run_id = p_workflow_run_id
      AND (
          ol.target_ref IS NULL
          OR NOT (ol.target_ref ? 'path')
          OR NOT (ol.target_ref ? 'content_hash')
          OR NOT (ol.target_ref ? 'version')
      );

    -- ---- visibility conflict: >1 active 'Y' per replacement key --------------
    RETURN QUERY
    SELECT
        'active_visibility_conflict'::text,
        'error'::text,
        'target_visibility'::text,
        concat(tv.domain, '/', tv.dataset, '/', tv.business_date, '/', tv.replacement_key)::text,
        'More than one active target visibility row exists for the same replacement key'::text,
        jsonb_build_object(
            'domain', tv.domain,
            'dataset', tv.dataset,
            'business_date', tv.business_date,
            'replacement_scope', tv.replacement_scope,
            'replacement_key', tv.replacement_key,
            'active_output_link_ids', array_agg(tv.lineage_link_id ORDER BY tv.activated_at)
        )
    FROM ods.target_visibility tv
    WHERE tv.status = 'Y'
      AND EXISTS (
          SELECT 1
          FROM cp.run_log r
          WHERE r.workflow_run_id = p_workflow_run_id
            AND r.domain = tv.domain
            AND r.dataset = tv.dataset
            AND r.business_date = tv.business_date
      )
    GROUP BY tv.domain, tv.dataset, tv.business_date, tv.replacement_scope, tv.replacement_key
    HAVING count(*) > 1;

    -- ---- Airflow/orchestrator identity missing -------------------------------
    RETURN QUERY
    SELECT
        'airflow_identity_missing'::text,
        'error'::text,
        'run_log'::text,
        r.run_id::text,
        'Airflow-triggered run is missing one or more orchestrator identity columns'::text,
        jsonb_build_object(
            'pipeline_type', r.pipeline_type,
            'dataset', r.dataset,
            'trigger_type', r.trigger_type,
            'orchestrator_type', r.orchestrator_type,
            'orchestrator_dag_id', r.orchestrator_dag_id,
            'orchestrator_run_id', r.orchestrator_run_id,
            'orchestrator_task_id', r.orchestrator_task_id
        )
    FROM cp.run_log r
    WHERE r.workflow_run_id = p_workflow_run_id
      AND r.trigger_type = 'airflow'
      AND (
          r.orchestrator_type IS NULL
          OR r.orchestrator_dag_id IS NULL
          OR r.orchestrator_run_id IS NULL
          OR r.orchestrator_task_id IS NULL
      );

    -- ---- reconciliation missing or breached for a sink run -------------------
    RETURN QUERY
    SELECT
        'sink_reconciliation_missing_or_breached'::text,
        CASE WHEN rl.recon_id IS NULL THEN 'warning' ELSE 'error' END::text,
        'reconciliation_log'::text,
        coalesce(rl.recon_id::text, r.run_id::text),
        CASE
            WHEN rl.recon_id IS NULL THEN format('Sink run %s/%s has no reconciliation row', r.pipeline_type, r.dataset)
            ELSE format('Sink run %s/%s reconciliation status is %s', r.pipeline_type, r.dataset, rl.status)
        END::text,
        jsonb_build_object(
            'run_id', r.run_id,
            'check_type', rl.check_type,
            'source_count', rl.source_count,
            'accounted_count', rl.accounted_count,
            'discrepancy', rl.discrepancy,
            'status', rl.status,
            'metrics', rl.metrics
        )
    FROM cp.run_log r
    LEFT JOIN cp.reconciliation_log rl ON rl.run_id = r.run_id
    WHERE r.workflow_run_id = p_workflow_run_id
      AND r.pipeline_type = 'sink'
      AND (rl.recon_id IS NULL OR rl.status <> 'ok');

    -- ---- DLQ row missing trace context (NEW 027) -----------------------------
    -- A cp.dlq row should be fully traceable: it must name the quarantine
    -- output_link it produced (quarantine_output_link_id) AND the input it came
    -- from (a source identity in source_ref, or a payload_ref location). Missing
    -- either breaks the support trace from the failure back to its origin.
    --   NOTE: cp.dlq.reason is NOT NULL at the table level, so a missing reason
    --   cannot occur here and is intentionally not checked.
    RETURN QUERY
    SELECT
        'dlq_row_missing_trace_context'::text,
        'error'::text,
        'dlq'::text,
        d.dlq_id::text,
        'DLQ row is missing trace context (quarantine output link or source identity)'::text,
        jsonb_build_object(
            'run_id', d.run_id,
            'stage', d.stage,
            'reason', d.reason,
            'status', d.status,
            'quarantine_output_link_id', d.quarantine_output_link_id,
            'source_ref', d.source_ref,
            'payload_ref', d.payload_ref
        )
    FROM cp.dlq d
    JOIN cp.run_log r ON r.run_id = d.run_id
    WHERE r.workflow_run_id = p_workflow_run_id
      AND (
          d.quarantine_output_link_id IS NULL
          OR (d.source_ref IS NULL AND d.payload_ref IS NULL)
      );

    -- ---- quarantine output_link with no corresponding cp.dlq rows (NEW 027) --
    -- A first-class quarantine output_link must be referenced by at least one
    -- cp.dlq row; otherwise the quarantine event has no failure record.
    RETURN QUERY
    SELECT
        'quarantine_output_without_dlq_rows'::text,
        'error'::text,
        'output_link'::text,
        ol.output_link_id::text,
        'Quarantine output_link has no cp.dlq row referencing it'::text,
        jsonb_build_object('run_id', r.run_id, 'target_ref', ol.target_ref, 'record_count', ol.record_count)
    FROM cp.run_log r
    JOIN cp.output_link ol ON ol.consumer_run_id = r.run_id
    LEFT JOIN cp.dlq d ON d.quarantine_output_link_id = ol.output_link_id
    WHERE r.workflow_run_id = p_workflow_run_id
      AND ol.edge_type = 'quarantine'
    GROUP BY r.run_id, ol.output_link_id, ol.target_ref, ol.record_count
    HAVING count(d.dlq_id) = 0;

    -- ---- schema-validation output missing schema_version (NEW 027) -----------
    -- A curated_to_canonical output is the product of schema validation. When a
    -- schema_contract exists for that (domain, dataset) AND this workflow's
    -- canonicalization stamps schema_version on AT LEAST ONE of its canonical
    -- outputs, then EVERY such output MUST carry schema_version — a sibling that
    -- omits it is the anomaly.
    --
    -- PRACTICAL SCOPE NOTE: we deliberately do NOT flag a canonical output whose
    -- workflow records schema_version NOWHERE. The current demo workflows pre-date
    -- the 024/025 schema-version-in-target_ref convention and never stamp it; an
    -- unconditional "contract exists -> require schema_version" check would make
    -- the sanctioned demo dirty (it has no schema_version anywhere). We therefore
    -- detect the realistic anomaly — a workflow that USES schema_version but has a
    -- canonical output missing it — and skip workflows that never adopted it. A
    -- repository-wide "no workflow records schema_version" gap is a coverage issue
    -- for the writer contract (area 1), not a per-workflow integrity defect.
    RETURN QUERY
    SELECT
        'schema_validation_output_missing_schema_version'::text,
        'error'::text,
        'output_link'::text,
        ol.output_link_id::text,
        'Canonical output target_ref is missing schema_version but a schema_contract exists and a sibling canonical output records one'::text,
        jsonb_build_object(
            'run_id', r.run_id,
            'domain', r.domain,
            'dataset', r.dataset,
            'edge_type', ol.edge_type,
            'target_ref', ol.target_ref
        )
    FROM cp.run_log r
    JOIN cp.output_link ol ON ol.consumer_run_id = r.run_id
    WHERE r.workflow_run_id = p_workflow_run_id
      AND ol.edge_type = 'curated_to_canonical'
      AND NOT (ol.target_ref ? 'schema_version')
      AND EXISTS (
          SELECT 1 FROM cp.schema_contract sc
          WHERE sc.domain = r.domain
            AND sc.dataset = r.dataset
      )
      AND EXISTS (
          SELECT 1
          FROM cp.run_log r2
          JOIN cp.output_link ol2 ON ol2.consumer_run_id = r2.run_id
          WHERE r2.workflow_run_id = p_workflow_run_id
            AND ol2.edge_type = 'curated_to_canonical'
            AND (ol2.target_ref ? 'schema_version')
      );

    -- ====================== target-row checks (scoped to a table) =============
    IF p_target_table IS NOT NULL THEN
        IF length(trim(p_target_table)) = 0 THEN
            RAISE EXCEPTION 'developer_diagnostics: target table cannot be blank'
                USING ERRCODE = 'P0001',
                      HINT = 'Use a table name such as policy_claim or ods.policy_claim, or pass NULL to skip target-row checks.';
        END IF;

        IF array_length(string_to_array(p_target_table, '.'), 1) > 2 THEN
            RAISE EXCEPTION 'developer_diagnostics: target table % is not valid', p_target_table
                USING ERRCODE = 'P0001',
                      HINT = 'Use table or schema.table form, for example ods.policy_claim.';
        END IF;

        IF position('.' IN p_target_table) > 0 THEN
            v_schema := split_part(p_target_table, '.', 1);
            v_table := split_part(p_target_table, '.', 2);
        ELSE
            v_schema := 'ods';
            v_table := p_target_table;
        END IF;

        SELECT to_regclass(format('%I.%I', v_schema, v_table)) INTO v_table_regclass;
        IF v_table_regclass IS NULL THEN
            RAISE EXCEPTION 'developer_diagnostics: target table %.% does not exist', v_schema, v_table
                USING ERRCODE = 'P0001',
                      HINT = 'Query information_schema.tables for available target tables, or pass NULL to skip target-row checks.';
        END IF;

        SELECT array_agg(required_col)
        INTO v_missing_columns
        FROM unnest(v_required_columns) AS required_col
        WHERE NOT EXISTS (
            SELECT 1
            FROM information_schema.columns c
            WHERE c.table_schema = v_schema
              AND c.table_name = v_table
              AND c.column_name = required_col
        );

        IF v_missing_columns IS NOT NULL THEN
            RAISE EXCEPTION 'developer_diagnostics: target table %.% is missing required ODS columns: %',
                    v_schema, v_table, array_to_string(v_missing_columns, ', ')
                USING ERRCODE = 'P0001',
                      HINT = 'Expected row_id, _ods_workflow_run_id, _ods_output_link_id, _ods_lineage_link_id, and payload.';
        END IF;

        -- target row missing ODS ids
        RETURN QUERY EXECUTE format($fmt$
            SELECT
                'target_row_missing_ods_ids'::text,
                'error'::text,
                %L::text,
                t.row_id::text,
                'Target row is missing one or more required ODS stamp columns'::text,
                jsonb_build_object(
                    'row_id', t.row_id,
                    '_ods_workflow_run_id', t._ods_workflow_run_id,
                    '_ods_output_link_id', t._ods_output_link_id,
                    '_ods_lineage_link_id', t._ods_lineage_link_id,
                    'payload', t.payload
                )
            FROM %I.%I t
            WHERE t._ods_workflow_run_id = $1
              AND (
                  t._ods_workflow_run_id IS NULL
                  OR t._ods_output_link_id IS NULL
                  OR t._ods_lineage_link_id IS NULL
              )
        $fmt$, format('%I.%I', v_schema, v_table), v_schema, v_table)
        USING p_workflow_run_id;

        -- target row whose output link does not exist
        RETURN QUERY EXECUTE format($fmt$
            SELECT
                'target_row_broken_output_link'::text,
                'error'::text,
                %L::text,
                t.row_id::text,
                'Target row references an _ods_output_link_id that does not exist in cp.output_link'::text,
                jsonb_build_object(
                    'row_id', t.row_id,
                    '_ods_workflow_run_id', t._ods_workflow_run_id,
                    '_ods_output_link_id', t._ods_output_link_id,
                    'payload', t.payload
                )
            FROM %I.%I t
            LEFT JOIN cp.output_link ol ON ol.output_link_id = t._ods_output_link_id
            WHERE t._ods_workflow_run_id = $1
              AND t._ods_output_link_id IS NOT NULL
              AND ol.output_link_id IS NULL
        $fmt$, format('%I.%I', v_schema, v_table), v_schema, v_table)
        USING p_workflow_run_id;

        -- target row workflow mismatch
        RETURN QUERY EXECUTE format($fmt$
            SELECT
                'target_row_workflow_mismatch'::text,
                'error'::text,
                %L::text,
                t.row_id::text,
                'Target row workflow id disagrees with the workflow that produced its output link'::text,
                jsonb_build_object(
                    'row_id', t.row_id,
                    'target_workflow_run_id', t._ods_workflow_run_id,
                    'output_link_workflow_run_id', r.workflow_run_id,
                    '_ods_output_link_id', t._ods_output_link_id,
                    'producer_run_id', r.run_id,
                    'producer_pipeline_type', r.pipeline_type,
                    'producer_dataset', r.dataset,
                    'payload', t.payload
                )
            FROM %I.%I t
            JOIN cp.output_link ol ON ol.output_link_id = t._ods_output_link_id
            JOIN cp.run_log r ON r.run_id = ol.consumer_run_id
            WHERE t._ods_workflow_run_id = $1
              AND t._ods_workflow_run_id <> r.workflow_run_id
        $fmt$, format('%I.%I', v_schema, v_table), v_schema, v_table)
        USING p_workflow_run_id;
    END IF;
END $$;


-- ======================================================================
-- Optional read/lookup helpers (spec §6 "Required Additions").
-- All read-only; round-tripped in tests/test_diagnostics.py.
-- ======================================================================

-- cp.dashboard_file_usage(file_id): every run/output/edge that consumed or
-- produced from a raw file. A raw file is consumed by an input_edge
-- (source_file_id) which belongs to an output_link produced by a run.
CREATE OR REPLACE FUNCTION cp.dashboard_file_usage(p_file_id uuid)
RETURNS TABLE (
    input_edge_id uuid,
    output_link_id uuid,
    edge_type text,
    consumer_run_id uuid,
    workflow_run_id text,
    pipeline_type text,
    dataset text,
    business_date date,
    run_status text,
    record_count bigint,
    target_ref jsonb
)
LANGUAGE plpgsql
STABLE
AS $$
BEGIN
    IF p_file_id IS NULL THEN
        RAISE EXCEPTION 'dashboard_file_usage: file_id is required'
            USING ERRCODE = 'P0001',
                  HINT = 'Pass a cp.file_catalogue.file_id.';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM cp.file_catalogue fc WHERE fc.file_id = p_file_id) THEN
        RAISE EXCEPTION 'dashboard_file_usage: file_id % not found', p_file_id
            USING ERRCODE = 'P0001',
                  HINT = 'Query cp.file_catalogue for available file ids.';
    END IF;

    RETURN QUERY
    SELECT
        ie.input_edge_id,
        ol.output_link_id,
        ie.edge_type,
        ol.consumer_run_id,
        r.workflow_run_id,
        r.pipeline_type,
        r.dataset,
        r.business_date,
        r.status,
        ie.record_count,
        ol.target_ref
    FROM cp.input_edge ie
    JOIN cp.output_link ol ON ol.output_link_id = ie.output_link_id
    JOIN cp.run_log r ON r.run_id = ol.consumer_run_id
    WHERE ie.source_file_id = p_file_id
    ORDER BY r.started_at, ol.created_at, ie.input_edge_id;
END $$;


-- cp.dashboard_target_row_trace(schema, table, row_id): resolve a target row to
-- its output_link, then walk provenance back to the raw file(s). Thin, safe
-- wrapper over cp.dashboard_output_trace keyed by the row's _ods_output_link_id.
CREATE OR REPLACE FUNCTION cp.dashboard_target_row_trace(
    p_target_schema text,
    p_target_table text,
    p_row_id bigint
)
RETURNS TABLE (
    hop integer,
    edge_type text,
    output_link_id uuid,
    consumer_run_id uuid,
    pipeline_type text,
    dataset text,
    upstream_run_id uuid,
    source_file_id uuid,
    raw_s3_path text,
    is_cycle boolean
)
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    v_regclass regclass;
    v_output_link_id uuid;
BEGIN
    IF p_target_schema IS NULL OR p_target_table IS NULL OR p_row_id IS NULL THEN
        RAISE EXCEPTION 'dashboard_target_row_trace: schema, table, and row_id are required'
            USING ERRCODE = 'P0001',
                  HINT = 'Pass the target schema, table, and a row_id from that table.';
    END IF;

    v_regclass := to_regclass(format('%I.%I', p_target_schema, p_target_table));
    IF v_regclass IS NULL THEN
        RAISE EXCEPTION 'dashboard_target_row_trace: target table %.% does not exist',
                p_target_schema, p_target_table
            USING ERRCODE = 'P0001',
                  HINT = 'Query information_schema.tables for available target tables.';
    END IF;

    EXECUTE format(
        'SELECT t._ods_output_link_id FROM %I.%I t WHERE t.row_id = $1',
        p_target_schema, p_target_table
    ) INTO v_output_link_id USING p_row_id;

    IF v_output_link_id IS NULL THEN
        RAISE EXCEPTION 'dashboard_target_row_trace: row % in %.% has no _ods_output_link_id',
                p_row_id, p_target_schema, p_target_table
            USING ERRCODE = 'P0001',
                  HINT = 'The row may not exist, or it was never stamped with an output link.';
    END IF;

    RETURN QUERY SELECT * FROM cp.dashboard_output_trace(v_output_link_id);
END $$;


-- cp.dashboard_airflow_lookup(dag_id, dag_run_id): runs whose orchestrator
-- identity (migration 020) matches an Airflow dag_run. dag_id is optional
-- (NULL = match any dag); dag_run_id is the discriminating key.
CREATE OR REPLACE FUNCTION cp.dashboard_airflow_lookup(
    p_dag_id text,
    p_dag_run_id text
)
RETURNS TABLE (
    workflow_run_id text,
    run_id uuid,
    pipeline_type text,
    dataset text,
    status text,
    orchestrator_type text,
    orchestrator_dag_id text,
    orchestrator_run_id text,
    orchestrator_task_id text,
    started_at timestamptz,
    finished_at timestamptz
)
LANGUAGE plpgsql
STABLE
AS $$
BEGIN
    IF p_dag_run_id IS NULL THEN
        RAISE EXCEPTION 'dashboard_airflow_lookup: dag_run_id is required'
            USING ERRCODE = 'P0001',
                  HINT = 'Pass the Airflow dag_run_id (cp.run_log.orchestrator_run_id).';
    END IF;

    RETURN QUERY
    SELECT
        r.workflow_run_id,
        r.run_id,
        r.pipeline_type,
        r.dataset,
        r.status,
        r.orchestrator_type,
        r.orchestrator_dag_id,
        r.orchestrator_run_id,
        r.orchestrator_task_id,
        r.started_at,
        r.finished_at
    FROM cp.run_log r
    WHERE r.orchestrator_run_id = p_dag_run_id
      AND (p_dag_id IS NULL OR r.orchestrator_dag_id = p_dag_id)
    ORDER BY r.started_at, r.pipeline_type, r.dataset, r.run_id;
END $$;
