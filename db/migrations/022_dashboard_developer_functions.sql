-- 022_dashboard_developer_functions.sql
--
-- Database-side support for the dashboard and for developers who cannot use the
-- dashboard UI. These functions are read-only and intentionally use the
-- developer-facing output/input names exposed by migrations 017/019:
--   cp.output_link(output_link_id, ...)
--   cp.input_edge(input_edge_id, output_link_id, upstream_output_link_id, ...)
--
-- Exception policy:
--   * required workflow/output ids are validated up front;
--   * target table diagnostics validate table existence and required columns;
--   * errors include hints telling the caller what to query next.

CREATE OR REPLACE FUNCTION cp.dashboard_workflows()
RETURNS TABLE (
    workflow_run_id text,
    business_date date,
    trigger_type text,
    domain text,
    run_count bigint,
    stage_count bigint,
    output_count bigint,
    input_count bigint,
    target_visibility_count bigint,
    succeeded_count bigint,
    failed_count bigint,
    running_count bigint,
    has_replay boolean,
    orchestrator_type text,
    orchestrator_dag_id text,
    orchestrator_run_id text,
    datasets text[],
    pipeline_types text[],
    first_started_at timestamptz,
    last_finished_at timestamptz
)
LANGUAGE sql
STABLE
AS $$
WITH run_rollup AS (
    SELECT
        workflow_run_id,
        min(business_date) AS business_date,
        min(trigger_type) AS trigger_type,
        min(domain) AS domain,
        count(*) AS run_count,
        count(*) FILTER (WHERE status = 'succeeded') AS succeeded_count,
        count(*) FILTER (WHERE status = 'failed') AS failed_count,
        count(*) FILTER (WHERE status = 'running') AS running_count,
        bool_or(replay_of_run_id IS NOT NULL) AS has_replay,
        max(orchestrator_type) FILTER (WHERE orchestrator_type IS NOT NULL) AS orchestrator_type,
        max(orchestrator_dag_id) FILTER (WHERE orchestrator_dag_id IS NOT NULL) AS orchestrator_dag_id,
        max(orchestrator_run_id) FILTER (WHERE orchestrator_run_id IS NOT NULL) AS orchestrator_run_id,
        array_agg(DISTINCT dataset ORDER BY dataset) AS datasets,
        array_agg(DISTINCT pipeline_type ORDER BY pipeline_type) AS pipeline_types,
        min(started_at) AS first_started_at,
        max(finished_at) AS last_finished_at
    FROM cp.run_log
    GROUP BY workflow_run_id
),
stage_rollup AS (
    SELECT r.workflow_run_id, count(*) AS stage_count
    FROM cp.run_log r
    JOIN cp.run_stage_log s ON s.run_id = r.run_id
    GROUP BY r.workflow_run_id
),
output_rollup AS (
    SELECT r.workflow_run_id, count(*) AS output_count
    FROM cp.run_log r
    JOIN cp.output_link ol ON ol.consumer_run_id = r.run_id
    GROUP BY r.workflow_run_id
),
input_rollup AS (
    SELECT r.workflow_run_id, count(*) AS input_count
    FROM cp.run_log r
    JOIN cp.output_link ol ON ol.consumer_run_id = r.run_id
    JOIN cp.input_edge ie ON ie.output_link_id = ol.output_link_id
    GROUP BY r.workflow_run_id
),
visibility_rollup AS (
    SELECT workflow_run_id, count(*) AS target_visibility_count
    FROM ods.target_visibility
    GROUP BY workflow_run_id
)
SELECT
    rr.workflow_run_id,
    rr.business_date,
    rr.trigger_type,
    rr.domain,
    rr.run_count,
    coalesce(sr.stage_count, 0),
    coalesce(oroll.output_count, 0),
    coalesce(ir.input_count, 0),
    coalesce(vr.target_visibility_count, 0),
    rr.succeeded_count,
    rr.failed_count,
    rr.running_count,
    rr.has_replay,
    rr.orchestrator_type,
    rr.orchestrator_dag_id,
    rr.orchestrator_run_id,
    rr.datasets,
    rr.pipeline_types,
    rr.first_started_at,
    rr.last_finished_at
FROM run_rollup rr
LEFT JOIN stage_rollup sr USING (workflow_run_id)
LEFT JOIN output_rollup oroll USING (workflow_run_id)
LEFT JOIN input_rollup ir USING (workflow_run_id)
LEFT JOIN visibility_rollup vr USING (workflow_run_id)
ORDER BY rr.first_started_at, rr.workflow_run_id;
$$;


CREATE OR REPLACE FUNCTION cp.dashboard_workflow_detail(p_workflow_run_id text)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    v_detail jsonb;
BEGIN
    IF p_workflow_run_id IS NULL THEN
        RAISE EXCEPTION 'dashboard_workflow_detail: workflow_run_id is required'
            USING ERRCODE = 'P0001',
                  HINT = 'Pass a workflow_run_id from cp.run_log or cp.dashboard_workflows().';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM cp.run_log WHERE workflow_run_id = p_workflow_run_id) THEN
        RAISE EXCEPTION 'dashboard_workflow_detail: workflow_run_id % not found', p_workflow_run_id
            USING ERRCODE = 'P0001',
                  HINT = 'Query cp.dashboard_workflows() to list available workflows.';
    END IF;

    SELECT jsonb_build_object(
        'workflow', (
            SELECT to_jsonb(w)
            FROM cp.dashboard_workflows() w
            WHERE w.workflow_run_id = p_workflow_run_id
        ),
        'runs', coalesce((
            SELECT jsonb_agg(
                jsonb_build_object(
                    'run_id', r.run_id,
                    'workflow_run_id', r.workflow_run_id,
                    'trigger_type', r.trigger_type,
                    'replay_of_run_id', r.replay_of_run_id,
                    'pipeline_type', r.pipeline_type,
                    'domain', r.domain,
                    'dataset', r.dataset,
                    'business_date', r.business_date,
                    'file_id', r.file_id,
                    'status', r.status,
                    'record_count_in', r.record_count_in,
                    'record_count_out', r.record_count_out,
                    'error', r.error,
                    'started_at', r.started_at,
                    'finished_at', r.finished_at,
                    'orchestrator_type', r.orchestrator_type,
                    'orchestrator_dag_id', r.orchestrator_dag_id,
                    'orchestrator_run_id', r.orchestrator_run_id,
                    'orchestrator_task_id', r.orchestrator_task_id,
                    'orchestrator_try_number', r.orchestrator_try_number,
                    'orchestrator_map_index', r.orchestrator_map_index,
                    'orchestrator_url', r.orchestrator_url,
                    'orchestrator_payload', r.orchestrator_payload,
                    'stages', coalesce((
                        SELECT jsonb_agg(to_jsonb(s) ORDER BY s.started_at, s.stage_log_id)
                        FROM cp.run_stage_log s
                        WHERE s.run_id = r.run_id
                    ), '[]'::jsonb)
                )
                ORDER BY r.started_at, r.pipeline_type, r.dataset, r.run_id
            )
            FROM cp.run_log r
            WHERE r.workflow_run_id = p_workflow_run_id
        ), '[]'::jsonb),
        'output_links', coalesce((
            SELECT jsonb_agg(
                jsonb_build_object(
                    'output_link_id', ol.output_link_id,
                    'consumer_run_id', ol.consumer_run_id,
                    'workflow_run_id', r.workflow_run_id,
                    'edge_type', ol.edge_type,
                    'sink_type', ol.sink_type,
                    'target_ref', ol.target_ref,
                    'transform_version', ol.transform_version,
                    'record_count', ol.record_count,
                    'created_at', ol.created_at,
                    'input_edges', coalesce((
                        SELECT jsonb_agg(to_jsonb(ie) ORDER BY ie.input_slot, ie.input_edge_id)
                        FROM cp.input_edge ie
                        WHERE ie.output_link_id = ol.output_link_id
                    ), '[]'::jsonb)
                )
                ORDER BY r.started_at, ol.created_at, ol.output_link_id
            )
            FROM cp.run_log r
            JOIN cp.output_link ol ON ol.consumer_run_id = r.run_id
            WHERE r.workflow_run_id = p_workflow_run_id
        ), '[]'::jsonb),
        'target_visibility', coalesce((
            SELECT jsonb_agg(
                jsonb_build_object(
                    'visibility_id', tv.visibility_id,
                    'domain', tv.domain,
                    'dataset', tv.dataset,
                    'business_date', tv.business_date,
                    'sink_type', tv.sink_type,
                    'target_name', tv.target_name,
                    'file_id', tv.file_id,
                    'output_link_id', tv.lineage_link_id,
                    'producer_run_id', tv.producer_run_id,
                    'workflow_run_id', tv.workflow_run_id,
                    'replacement_scope', tv.replacement_scope,
                    'replacement_key', tv.replacement_key,
                    'status', tv.status,
                    'activated_at', tv.activated_at,
                    'deactivated_at', tv.deactivated_at,
                    'superseded_by', tv.superseded_by,
                    'reason', tv.reason,
                    'created_at', tv.created_at
                )
                ORDER BY tv.dataset, tv.business_date, tv.replacement_key, tv.activated_at
            )
            FROM ods.target_visibility tv
            WHERE tv.workflow_run_id = p_workflow_run_id
        ), '[]'::jsonb)
    )
    INTO v_detail;

    RETURN v_detail;
END $$;


CREATE OR REPLACE FUNCTION cp.dashboard_output_trace(p_output_link_id uuid)
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
BEGIN
    IF p_output_link_id IS NULL THEN
        RAISE EXCEPTION 'dashboard_output_trace: output_link_id is required'
            USING ERRCODE = 'P0001',
                  HINT = 'Pass cp.output_link.output_link_id or a target row _ods_output_link_id.';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM cp.output_link ol WHERE ol.output_link_id = p_output_link_id) THEN
        RAISE EXCEPTION 'dashboard_output_trace: output_link_id % not found', p_output_link_id
            USING ERRCODE = 'P0001',
                  HINT = 'Query cp.output_link or inspect the target row _ods_output_link_id.';
    END IF;

    RETURN QUERY
    WITH RECURSIVE chain AS (
        SELECT p.lineage_link_id AS output_link_id,
               p.edge_type,
               p.consumer_run_id,
               p.upstream_run_id,
               p.source_file_id,
               1 AS hop
        FROM cp.v_provenance p
        WHERE p.lineage_link_id = p_output_link_id
      UNION ALL
        SELECT p.lineage_link_id AS output_link_id,
               p.edge_type,
               p.consumer_run_id,
               p.upstream_run_id,
               p.source_file_id,
               c.hop + 1
        FROM chain c
        JOIN cp.lineage_edge ce ON ce.lineage_link_id = c.output_link_id
        JOIN cp.v_provenance p ON p.lineage_link_id = ce.upstream_lineage_link_id
        WHERE ce.upstream_lineage_link_id IS NOT NULL
    ) CYCLE output_link_id SET is_cycle USING path
    SELECT DISTINCT
        c.hop,
        c.edge_type,
        c.output_link_id,
        c.consumer_run_id,
        r.pipeline_type,
        r.dataset,
        c.upstream_run_id,
        c.source_file_id,
        fc.s3_raw_path,
        c.is_cycle
    FROM chain c
    LEFT JOIN cp.run_log r ON r.run_id = c.consumer_run_id
    LEFT JOIN cp.file_catalogue fc ON fc.file_id = c.source_file_id
    ORDER BY c.hop, c.edge_type, c.consumer_run_id, c.source_file_id;
END $$;


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
      AND ie.source_file_id IS NULL
      AND ie.upstream_output_link_id IS NULL;

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
              AND ol.output_link_id IS NULL
        $fmt$, format('%I.%I', v_schema, v_table), v_schema, v_table)
        USING p_workflow_run_id;

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
