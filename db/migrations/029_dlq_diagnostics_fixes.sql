-- 029_dlq_diagnostics_fixes.sql — six confirmed Codex-review fixes against the
-- DLQ / diagnostics / contract surface. Each affected function is RE-DECLARED
-- here (CREATE OR REPLACE, same signature) so the prior copy in 027/023/024 is
-- cleanly superseded; SUPERSEDED banners are added to those prior copies.
--
-- Spec/source: external (Codex) review — all six findings verified TRUE against
--              live code. New migration ONLY; 001–028 are applied and frozen.
-- Tests: tests/test_diagnostics.py + tests/test_contract.py (P1a/P2a/P2b/P3) and
--        tests/test_policy_claims_dlq_workflow.py (P1b) — written failing first.
--
-- FINDINGS FIXED
--   P1a (HIGH) — developer_diagnostics "input_edge_without_input_identifier"
--     exempted ONLY 'orchestrates', so a SANCTIONED 'quarantine' (or 'replay')
--     edge — which legitimately carries context in source_ref and anchors to
--     nothing per the 012 edge_must_anchor CHECK — was falsely flagged dirty.
--     FIX: exempt the SAME set the CHECK exempts:
--          NOT IN ('orchestrates','quarantine','replay').
--
--   P2a (MED) — the "target_row_missing_ods_ids" check filtered on
--     `t._ods_workflow_run_id = $1 AND (... t._ods_workflow_run_id IS NULL ...)`,
--     a contradiction: a row that LOST its workflow id can never satisfy
--     `= $1`, so the very anomaly it claimed to detect was invisible.
--     FIX: scope the target-row checks by the OUTPUT LINK that belongs to this
--     workflow (t._ods_output_link_id -> cp.output_link -> cp.run_log where
--     workflow_run_id = $1) so a row with NULL _ods_workflow_run_id but a valid
--     link is still attributable and flagged; ALSO keep flagging rows whose
--     _ods_output_link_id/_ods_lineage_link_id is null but whose
--     _ods_workflow_run_id = $1. A row with ALL ODS ids null is unattributable
--     to ANY workflow, so it gets a NEW table-wide check
--     'target_row_orphan_no_ods_ids' (NOT $1-scoped) so it is never invisible.
--
--   P2b (MED) — dashboard_file_usage only returns the DIRECT ingest edges
--     (source_file_id = p_file_id). It is kept AS-IS (the direct-edge view). A
--     NEW cp.dashboard_file_impact(p_file_id) returns every DOWNSTREAM output
--     whose cp.v_provenance chain reaches that raw file (canonical, merge, sink,
--     aggregate) — the downstream-impact view.
--
--   P2c (MED) — resolve_dlq coalesced refs, so resolve_dlq(id,'resolved') with
--     no refs closed a DLQ untraceably. FIX: a TERMINAL resolution
--     ('resolved'/'replayed') whose EFFECTIVE resolved_by_run_id AND
--     resolved_by_output_link_id would BOTH be null RAISES. 'rejected' and the
--     non-terminal states stay lenient. failed_payload/reason never touched.
--
--   P3 (LOW) — get_schema_contract ordered the latest by `schema_version DESC`
--     (TEXT), so 'claim.v9' sorted AFTER 'claim.v10'. FIX: order by the trailing
--     integer of schema_version DESC (true numeric semver), then by
--     effective_from / created_at as a stable tiebreak. Exact-version path
--     (p_schema_version supplied) unchanged.
--
-- (P1b is a harness fix — harness/policy_claims_dlq_workflow.py — not SQL.)


-- ======================================================================
-- SUPERSEDED: cp.developer_diagnostics(text, text) as defined in
--   027_diagnostics.sql is REPLACED below (same signature). 027's other
--   read helpers (dashboard_file_usage / _target_row_trace / _airflow_lookup)
--   remain authoritative. P1a + P2a are the only body changes; every other 027
--   check is reproduced verbatim.
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

    -- ---- unfinished stage (027): a stage that never closed, on ANY run --------
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
    -- P1a FIX: the 012 edge_must_anchor CHECK legitimately exempts
    --   'quarantine','orchestrates','replay' from anchoring to a file/upstream
    --   output — they carry context in source_ref (e.g. a quarantine edge's
    --   source_ref.raw_file_id, a replay edge's dlq_id). Exempt the SAME set here
    --   so a sanctioned DLQ/replay lineage edge is NOT reported dirty. (Was: only
    --   'orchestrates' exempt, falsely flagging valid quarantine/replay edges.)
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
      AND ie.edge_type NOT IN ('orchestrates','quarantine','replay')
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

    -- ---- DLQ row missing trace context (027) ---------------------------------
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

    -- ---- quarantine output_link with no corresponding cp.dlq rows (027) ------
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

    -- ---- schema-validation output missing schema_version (027) ---------------
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
        -- P2a FIX: attribute a row to THIS workflow either by its own
        --   _ods_workflow_run_id = $1 OR by its output link belonging to a run
        --   whose workflow_run_id = $1 (so a row that LOST _ods_workflow_run_id
        --   but still names a valid link of this workflow is detected). Flag the
        --   row when ANY required ODS stamp (_ods_workflow_run_id /
        --   _ods_output_link_id / _ods_lineage_link_id) is null. The contradictory
        --   `_ods_workflow_run_id = $1 AND _ods_workflow_run_id IS NULL` predicate
        --   that made the null-workflow case invisible is gone.
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
            LEFT JOIN cp.output_link ol ON ol.output_link_id = t._ods_output_link_id
            LEFT JOIN cp.run_log r      ON r.run_id = ol.consumer_run_id
            WHERE (
                      t._ods_workflow_run_id = $1
                      OR r.workflow_run_id = $1
                  )
              AND (
                      t._ods_workflow_run_id IS NULL
                      OR t._ods_output_link_id IS NULL
                      OR t._ods_lineage_link_id IS NULL
                  )
        $fmt$, format('%I.%I', v_schema, v_table), v_schema, v_table)
        USING p_workflow_run_id;

        -- target row that is a TOTAL ORPHAN: ALL three ODS ids are null, so it is
        -- unattributable to ANY workflow and the $1-scoped checks above can never
        -- see it. P2a FIX: detect it TABLE-WIDE (not $1-scoped) so it is never
        -- silently invisible. severity 'error'; object_type is the target table.
        RETURN QUERY EXECUTE format($fmt$
            SELECT
                'target_row_orphan_no_ods_ids'::text,
                'error'::text,
                %L::text,
                t.row_id::text,
                'Target row has NO ODS stamp columns at all (total orphan, unattributable to any workflow)'::text,
                jsonb_build_object(
                    'row_id', t.row_id,
                    '_ods_workflow_run_id', t._ods_workflow_run_id,
                    '_ods_output_link_id', t._ods_output_link_id,
                    '_ods_lineage_link_id', t._ods_lineage_link_id,
                    'payload', t.payload
                )
            FROM %I.%I t
            WHERE t._ods_workflow_run_id IS NULL
              AND t._ods_output_link_id IS NULL
              AND t._ods_lineage_link_id IS NULL
        $fmt$, format('%I.%I', v_schema, v_table), v_schema, v_table);

        -- target row whose output link does not exist
        -- P2a: also attributable by link->run workflow (a row with NULL
        --   _ods_workflow_run_id but a non-existent link is still this workflow's
        --   concern only when stamped with $1; a broken link cannot be joined to a
        --   run, so we keep the $1 self-stamp scope here — a null-workflow + broken
        --   link row is caught by target_row_missing_ods_ids / orphan above).
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

        -- target row workflow mismatch (unchanged): the row names a workflow id
        -- that disagrees with the workflow that produced its output link.
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
-- SUPERSEDED: cp.resolve_dlq(uuid, text, uuid, uuid) as defined in
--   023_dlq_lifecycle.sql is REPLACED below (same signature). P2c: a TERMINAL
--   resolution must be traceable to a run/output.
-- ======================================================================
CREATE OR REPLACE FUNCTION cp.resolve_dlq(
    p_dlq_id uuid, p_status text,
    p_resolved_by_run_id uuid DEFAULT NULL,
    p_resolved_by_output_link_id uuid DEFAULT NULL
) RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    v_eff_run uuid;
    v_eff_link uuid;
BEGIN
    -- P2c: for a TERMINAL resolution ('resolved'/'replayed'), compute the
    -- EFFECTIVE resolution refs (the value that WOULD be stored: the passed arg,
    -- else the value already on the row). If BOTH would be null the resolution is
    -- untraceable -> RAISE. 'rejected' and the non-terminal states stay lenient.
    IF p_status IN ('resolved','replayed') THEN
        SELECT coalesce(p_resolved_by_run_id, d.resolved_by_run_id),
               coalesce(p_resolved_by_output_link_id, d.resolved_by_output_link_id)
          INTO v_eff_run, v_eff_link
        FROM cp.dlq d
        WHERE d.dlq_id = p_dlq_id;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'resolve_dlq: no dlq row %', p_dlq_id;
        END IF;
        IF v_eff_run IS NULL AND v_eff_link IS NULL THEN
            RAISE EXCEPTION 'resolve_dlq: a terminal resolution (%) must be traceable to a resolved_by_run_id or resolved_by_output_link_id', p_status
                USING ERRCODE = 'P0001',
                      HINT = 'Pass resolved_by_run_id and/or resolved_by_output_link_id (or set them on a prior corrected/replayed step).';
        END IF;
    END IF;

    UPDATE cp.dlq
       SET status = p_status,
           resolved_by_run_id = coalesce(p_resolved_by_run_id, resolved_by_run_id),
           resolved_by_output_link_id = coalesce(p_resolved_by_output_link_id, resolved_by_output_link_id),
           replayed_at = CASE WHEN p_status IN ('replayed','resolved')
                              THEN clock_timestamp() ELSE replayed_at END,
           replay_run_id = coalesce(p_resolved_by_run_id, replay_run_id)
     WHERE dlq_id = p_dlq_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'resolve_dlq: no dlq row %', p_dlq_id;
    END IF;
END $$;


-- ======================================================================
-- SUPERSEDED: cp.get_schema_contract(text, text, text, text) as defined in
--   024_schema_contract.sql is REPLACED below (same signature). P3: pick the
--   latest by NUMERIC semver, not by text order, so 'claim.v10' beats 'claim.v9'.
--   Exact-version path (p_schema_version supplied) is unchanged.
-- ======================================================================
CREATE OR REPLACE FUNCTION cp.get_schema_contract(
    p_domain text, p_dataset text, p_layer text,
    p_schema_version text DEFAULT NULL
) RETURNS cp.schema_contract LANGUAGE sql STABLE AS $$
    SELECT *
    FROM cp.schema_contract
    WHERE domain = p_domain
      AND dataset = p_dataset
      AND layer = p_layer
      AND (p_schema_version IS NULL OR schema_version = p_schema_version)
    -- "Latest" = highest NUMERIC semver suffix (trailing integer of the version
    -- tag), so claim.v10 > claim.v9 (text DESC got this WRONG). NULLIF guards a
    -- version with no digits (-> NULL, sorts last). effective_from / created_at
    -- are stable tiebreaks ("currently effective / most recently registered").
    ORDER BY nullif(regexp_replace(schema_version, '\D', '', 'g'), '')::bigint
                 DESC NULLS LAST,
             effective_from DESC NULLS LAST,
             created_at DESC
    LIMIT 1;
$$;


-- ======================================================================
-- P2b — cp.dashboard_file_impact(p_file_id): the DOWNSTREAM-IMPACT view.
--   Every output_link DERIVED from the raw file p_file_id — the canonical, merge,
--   sink and aggregate outputs whose provenance chain reaches that file, NOT just
--   the direct ingest edge that cp.dashboard_file_usage returns.
--
--   WHY A DESCENDANT WALK (not `WHERE v_provenance.source_file_id = p_file_id`):
--   cp.v_provenance walks UPSTREAM and only emits source_file_id on the raw
--   ingest edge itself — it does NOT propagate that file id down to descendant
--   outputs. So the file impact is the DOWNSTREAM closure: start at the output
--   link(s) whose own edge names source_file_id = p_file_id (the raw_to_curated
--   leaves), then follow link->link adjacency DOWNWARD via
--   lineage_edge.upstream_lineage_link_id (the mirror of trace_row.sql's upstream
--   walk) to every output that consumed them, transitively. A CYCLE guard mirrors
--   the v_provenance / trace_row guards so a forged cyclic upstream_lineage_link_id
--   cannot hang the walk.
-- ======================================================================
CREATE OR REPLACE FUNCTION cp.dashboard_file_impact(p_file_id uuid)
RETURNS TABLE (
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
        RAISE EXCEPTION 'dashboard_file_impact: file_id is required'
            USING ERRCODE = 'P0001',
                  HINT = 'Pass a cp.file_catalogue.file_id.';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM cp.file_catalogue fc WHERE fc.file_id = p_file_id) THEN
        RAISE EXCEPTION 'dashboard_file_impact: file_id % not found', p_file_id
            USING ERRCODE = 'P0001',
                  HINT = 'Query cp.file_catalogue for available file ids.';
    END IF;

    RETURN QUERY
    WITH RECURSIVE impact AS (
        -- anchor: every link whose OWN edge derived directly from this raw file.
        SELECT DISTINCT e.lineage_link_id
        FROM cp.lineage_edge e
        WHERE e.source_file_id = p_file_id
      UNION ALL
        -- recurse DOWNSTREAM: any link whose edge names an in-set link as its
        -- exact upstream output (link->link adjacency).
        SELECT de.lineage_link_id
        FROM impact i
        JOIN cp.lineage_edge de ON de.upstream_lineage_link_id = i.lineage_link_id
    )
    CYCLE lineage_link_id SET is_cycle USING path
    SELECT
        ol.output_link_id,
        ol.edge_type,
        ol.consumer_run_id,
        r.workflow_run_id,
        r.pipeline_type,
        r.dataset,
        r.business_date,
        r.status,
        ol.record_count,
        ol.target_ref
    FROM cp.output_link ol
    JOIN cp.run_log r ON r.run_id = ol.consumer_run_id
    WHERE ol.output_link_id IN (SELECT DISTINCT lineage_link_id FROM impact)
    ORDER BY r.started_at, ol.created_at, ol.output_link_id;
END $$;
