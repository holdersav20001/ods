-- 030_audit_fixes.sql — full-platform audit SQL/function fixes (A1).
--
-- Consolidated findings: docs/reviews/2026-06-03-audit-consolidated.md
-- SQL detail:            docs/reviews/2026-06-03-audit-a1-sql.md
--
-- This migration is PURELY ADDITIVE in the migration ordering sense: it
-- re-declares four cp.* functions (each LIVE copy faithfully reproduced from
-- pg_get_functiondef, then patched) and adds one CHECK to ods.orders. It does
-- NOT edit any applied migration 001-029; instead each prior LIVE definition
-- carries a SUPERSEDED banner pointing here, and 030 applies last so THESE
-- definitions win.
--
-- FINDINGS FIXED HERE
--   F2 (HIGH) cp.quarantine          — stamp source_file_id on the quarantine
--                                       edge so the quarantine output traces to
--                                       the raw file (was only in source_ref JSON).
--   F4 (P1)   cp.get_schema_contract  — "latest" sort: replace the broken
--                                       digit-concatenation with a correct,
--                                       deterministic effective/recency order.
--   F5 (P2)   cp.developer_diagnostics — active_visibility_conflict GROUP BY now
--                                       mirrors the FULL uq_target_visibility_active
--                                       column set (no false positives).
--   F6 (P2)   cp.reconcile_workflow    — deterministic terminal-run tiebreak (seq),
--                                       DLQ no longer double-counts after replay,
--                                       sink_out includes detail_to_aggregate.
--   F8 (P2)   ods.orders               — CHECK guaranteeing the dual _ods_* mirror
--                                       columns stay consistent (rename shelved).

-- =====================================================================
-- F2 — cp.quarantine: trace the quarantine output to the raw file.
--
--   ╔══════════════════════════════════════════════════════════════════════╗
--   ║ The 023 copy of cp.quarantine (itself the 012/002 lineage) is          ║
--   ║ SUPERSEDED by this 030 body. 030 applies last so THIS definition wins. ║
--   ║ ALL prior behaviour is preserved VERBATIM:                             ║
--   ║   * inserts the cp.dlq row (status='open') with failed_payload,        ║
--   ║   * writes the first-class 'quarantine' output_link + edge,            ║
--   ║   * content_hash discriminated by dlq_id (distinct events don't        ║
--   ║     collapse via ON CONFLICT), path falls back to 'dlq:'||dlq_id,      ║
--   ║   * stamps quarantine_output_link_id on the dlq row.                   ║
--   ║ CHANGE: a new TRAILING optional param p_source_file_id (DEFAULT NULL)  ║
--   ║ is added and, when supplied, STAMPED on the quarantine edge's          ║
--   ║ source_file_id column (kept ALSO in source_ref). source_file_id is     ║
--   ║ exempt-but-allowed by the 012 edge_must_anchor CHECK (the CHECK only   ║
--   ║ EXEMPTS quarantine from REQUIRING an anchor; a non-null one is legal), ║
--   ║ so the quarantine link now appears in cp.v_provenance with a non-null  ║
--   ║ source_file_id and trace_row.sql / dashboard_output_trace reach the    ║
--   ║ raw file instead of dead-ending. The 023 over-claim ("source_ref names ║
--   ║ the raw file so the quarantine event is anchored to its origin") is    ║
--   ║ now TRUE at the lineage layer, not merely in JSON.                     ║
--   ║                                                                        ║
--   ║ SIGNATURE CHANGE (7 -> 8 params, trailing optional) -> DROP+CREATE.    ║
--   ║ The new param is the LAST positional arg and defaults to NULL, so      ║
--   ║ EVERY existing caller (the 5/6/7-arg forms in tests + harness +        ║
--   ║ control.dlq.quarantine) keeps working unchanged.                       ║
--   ╚══════════════════════════════════════════════════════════════════════╝
-- =====================================================================
DROP FUNCTION IF EXISTS cp.quarantine(uuid, text, text, jsonb, text, bigint, jsonb);

CREATE OR REPLACE FUNCTION cp.quarantine(
    p_run_id uuid,
    p_stage text,
    p_reason text,
    p_source_ref jsonb,
    p_payload_ref text,
    p_record_count bigint,
    p_failed_payload jsonb DEFAULT NULL::jsonb,
    p_source_file_id uuid DEFAULT NULL::uuid
)
RETURNS uuid
LANGUAGE plpgsql
AS $function$
DECLARE v_dlq uuid; v_link uuid;
BEGIN
    INSERT INTO cp.dlq (run_id, stage, reason, source_ref, payload_ref,
                        record_count, failed_payload, status)
    VALUES (p_run_id, p_stage, p_reason, p_source_ref, p_payload_ref,
            p_record_count, p_failed_payload, 'open')
    RETURNING dlq_id INTO v_dlq;
    -- First-class quarantine output_link. content_hash discriminated by dlq_id so
    -- two quarantine events in one run with the same payload_ref do not collapse
    -- via ON CONFLICT. path falls back to 'dlq:'||dlq_id so target_ref_contract
    -- (non-empty path + content_hash + non-empty version) holds. Capture the
    -- returned link id and stamp it on the dlq row (spec quarantine_output_link_id).
    --
    -- F2: ALSO stamp the edge's source_file_id with p_source_file_id (when given)
    -- so the quarantine output traces to the raw file via cp.v_provenance /
    -- trace_row.sql / dashboard_output_trace. write_lineage_link reads
    -- source_file_id from each edge object (nullif(elem->>'source_file_id','')),
    -- so a NULL p_source_file_id is stored as NULL (identical to the pre-030
    -- behaviour) and the 012 edge_must_anchor exemption still holds. source_ref is
    -- preserved exactly as before (the raw id remains in the JSON too).
    v_link := cp.write_lineage_link(
        p_run_id, 'quarantine',
        jsonb_build_object(
            'path', coalesce(nullif(p_payload_ref, ''), 'dlq:' || v_dlq::text),
            'content_hash', v_dlq::text,
            'dlq_id', v_dlq,
            'version', 1),
        p_record_count,
        jsonb_build_array(jsonb_build_object(
            'source_ref', p_source_ref,
            'source_file_id', p_source_file_id,
            'edge_type', 'quarantine',
            'record_count', p_record_count)));
    UPDATE cp.dlq SET quarantine_output_link_id = v_link WHERE dlq_id = v_dlq;
    RETURN v_dlq;
END $function$;

-- =====================================================================
-- F4 — cp.get_schema_contract: correct, deterministic "latest" order.
--
--   ╔══════════════════════════════════════════════════════════════════════╗
--   ║ The 024-seeded / 029-era copy of cp.get_schema_contract is SUPERSEDED  ║
--   ║ by this 030 body. 030 applies last so THIS definition wins. Signature  ║
--   ║ is UNCHANGED (4 args) -> CREATE OR REPLACE suffices.                   ║
--   ║                                                                        ║
--   ║ DEFECT (F4): the prior "latest" ORDER BY used                          ║
--   ║   nullif(regexp_replace(schema_version,'\D','','g'),'')::bigint DESC   ║
--   ║ which STRIPS all non-digits and CONCATENATES them, so 'claim.v1.2'->12,║
--   ║ 'v1.10'->110, 'v1.0'->10. Thus v1.10 (110) > v2 (2) and v1.0 ties v10  ║
--   ║ — "latest" could resolve to the WRONG contract.                        ║
--   ║                                                                        ║
--   ║ FIX: order by the currently-effective / most-recently-registered       ║
--   ║ contract: effective_from DESC NULLS LAST, then created_at DESC. This   ║
--   ║ is deterministic (created_at DEFAULT clock_timestamp() advances within ║
--   ║ a txn, so even a single multi-row INSERT gets strictly increasing      ║
--   ║ created_at and the last-registered version wins) and free of the       ║
--   ║ digit-concatenation bug. The exact-version path (p_schema_version      ║
--   ║ given) is UNCHANGED.                                                    ║
--   ╚══════════════════════════════════════════════════════════════════════╝
-- =====================================================================
CREATE OR REPLACE FUNCTION cp.get_schema_contract(
    p_domain text,
    p_dataset text,
    p_layer text,
    p_schema_version text DEFAULT NULL::text
)
RETURNS cp.schema_contract
LANGUAGE sql
STABLE
AS $function$
    SELECT *
    FROM cp.schema_contract
    WHERE domain = p_domain
      AND dataset = p_dataset
      AND layer = p_layer
      AND (p_schema_version IS NULL OR schema_version = p_schema_version)
    -- "Latest" = the currently-effective / most-recently-registered contract.
    -- (F4: replaces the broken digit-concatenation order. See banner above.)
    ORDER BY effective_from DESC NULLS LAST,
             created_at DESC
    LIMIT 1;
$function$;

-- =====================================================================
-- F5 — cp.developer_diagnostics: active_visibility_conflict GROUP BY now
--      mirrors the FULL uq_target_visibility_active column set.
--
--   ╔══════════════════════════════════════════════════════════════════════╗
--   ║ The 027/029 copy of cp.developer_diagnostics is SUPERSEDED by this 030 ║
--   ║ body. 030 applies last so THIS definition wins. Signature + return     ║
--   ║ type UNCHANGED -> CREATE OR REPLACE suffices. The body is the 029 LIVE ║
--   ║ body reproduced VERBATIM with EXACTLY ONE change:                      ║
--   ║                                                                        ║
--   ║ DEFECT (F5): the 'active_visibility_conflict' check GROUPed BY         ║
--   ║   (domain, dataset, business_date, replacement_scope, replacement_key) ║
--   ║ — a SUBSET that OMITS sink_type and target_name. But                   ║
--   ║ uq_target_visibility_active is UNIQUE on                               ║
--   ║   (domain,dataset,business_date,sink_type,target_name,                 ║
--   ║    replacement_scope,replacement_key) WHERE status='Y'. So two         ║
--   ║ legitimately-distinct active rows (different sink_type/target_name)    ║
--   ║ were FALSELY flagged as a conflict.                                    ║
--   ║                                                                        ║
--   ║ FIX: add tv.sink_type, tv.target_name to the GROUP BY so the           ║
--   ║ diagnostic mirrors the real invariant — only a true >1-Y-per-key is    ║
--   ║ flagged. The object_id concat is also widened to include sink_type +   ║
--   ║ target_name so the reported key is unambiguous.                        ║
--   ╚══════════════════════════════════════════════════════════════════════╝
-- =====================================================================
CREATE OR REPLACE FUNCTION cp.developer_diagnostics(p_workflow_run_id text, p_target_table text DEFAULT NULL::text)
 RETURNS TABLE(check_name text, severity text, object_type text, object_id text, message text, details jsonb)
 LANGUAGE plpgsql
 STABLE
AS $function$
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
    -- F5 FIX: GROUP BY the FULL uq_target_visibility_active column set
    --   (adds sink_type + target_name). The unique index is on
    --   (domain,dataset,business_date,sink_type,target_name,replacement_scope,
    --    replacement_key) WHERE status='Y'; mirroring it here means only a TRUE
    --   >1-active-per-key is flagged. Previously the GROUP BY omitted
    --   sink_type/target_name, so two legitimately-distinct active rows that
    --   differed only by sink_type/target_name were FALSELY flagged.
    RETURN QUERY
    SELECT
        'active_visibility_conflict'::text,
        'error'::text,
        'target_visibility'::text,
        concat(tv.domain, '/', tv.dataset, '/', tv.business_date, '/',
               tv.sink_type, '/', tv.target_name, '/', tv.replacement_key)::text,
        'More than one active target visibility row exists for the same replacement key'::text,
        jsonb_build_object(
            'domain', tv.domain,
            'dataset', tv.dataset,
            'business_date', tv.business_date,
            'sink_type', tv.sink_type,
            'target_name', tv.target_name,
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
    GROUP BY tv.domain, tv.dataset, tv.business_date, tv.sink_type, tv.target_name,
             tv.replacement_scope, tv.replacement_key
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
END $function$;

-- =====================================================================
-- F6 — cp.reconcile_workflow: deterministic terminal run, no DLQ
--      double-count, sink_out includes detail_to_aggregate.
--
--   ╔══════════════════════════════════════════════════════════════════════╗
--   ║ The 015 copy of cp.reconcile_workflow is SUPERSEDED by this 030 body.  ║
--   ║ 030 applies last so THIS definition wins. Signature + return type      ║
--   ║ UNCHANGED (1 arg, void) -> CREATE OR REPLACE suffices. The body is the ║
--   ║ 015 LIVE body reproduced VERBATIM with THREE corrections:              ║
--   ║                                                                        ║
--   ║ (a) TERMINAL RUN: was ORDER BY started_at DESC, run_id DESC — a clock  ║
--   ║     + random-UUID tiebreak. 028 migrated discovery selectors to        ║
--   ║     seq DESC (deterministic, monotonic). Now: finished_at DESC NULLS   ║
--   ║     LAST, seq DESC — matches the 028 deterministic tiebreak.           ║
--   ║                                                                        ║
--   ║ (b) DLQ ACCOUNTING: was sum(cp.dlq.record_count) over ALL DLQ rows of  ║
--   ║     the workflow regardless of status -> double-counted after a        ║
--   ║     replay/resolve (the loss is recovered but still summed as          ║
--   ║     un-accounted). Now: only count DLQ rows still representing          ║
--   ║     un-recovered loss — status NOT IN ('resolved','replayed'). A row   ║
--   ║     that was replayed/resolved no longer inflates dlq_out, so a         ║
--   ║     post-replay workflow reconciles without a phantom discrepancy.     ║
--   ║                                                                        ║
--   ║ (c) SINK_OUT: the canonical_to_sink iteration ignored                  ║
--   ║     detail_to_aggregate sink outputs. Now both edge types are counted  ║
--   ║     for the dataset loop AND the per-dataset row count, so the         ║
--   ║     cross-hop sum is correct for aggregate sinks too.                  ║
--   ╚══════════════════════════════════════════════════════════════════════╝
-- =====================================================================
CREATE OR REPLACE FUNCTION cp.reconcile_workflow(p_workflow_run_id text)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_raw_in bigint; v_sink_out bigint := 0; v_dlq_out bigint;
    v_accounted bigint; v_disc bigint; v_status text;
    v_terminal_run uuid; v_ds text; v_cnt bigint;
BEGIN
    -- (a) deterministic terminal run: finished_at DESC NULLS LAST, seq DESC
    --     (matches 028 discovery tiebreak; replaces started_at/run_id DESC).
    SELECT run_id INTO v_terminal_run FROM cp.run_log
     WHERE workflow_run_id = p_workflow_run_id
     ORDER BY finished_at DESC NULLS LAST, seq DESC LIMIT 1;
    IF v_terminal_run IS NULL THEN
        RAISE EXCEPTION 'reconcile_workflow: no runs for workflow %', p_workflow_run_id;
    END IF;

    -- raw_in: rows that ENTERED — the raw_to_curated link counts for this workflow.
    SELECT coalesce(sum(l.record_count), 0) INTO v_raw_in
      FROM cp.lineage_link l JOIN cp.run_log r ON r.run_id = l.consumer_run_id
     WHERE r.workflow_run_id = p_workflow_run_id AND l.edge_type = 'raw_to_curated';

    -- sink_out: actual sink rows, summed over the workflow's distinct datasets
    -- that have at least one sink-class link (iterate with dynamic %I).
    -- (c) include detail_to_aggregate (an aggregate output is a sink-class output
    --     of a dataset too) alongside canonical_to_sink so its rows are counted.
    FOR v_ds IN
        SELECT DISTINCT r.dataset
          FROM cp.lineage_link l JOIN cp.run_log r ON r.run_id = l.consumer_run_id
         WHERE r.workflow_run_id = p_workflow_run_id
           AND l.edge_type IN ('canonical_to_sink','detail_to_aggregate')
    LOOP
        IF to_regclass('ods.' || quote_ident(v_ds)) IS NULL THEN
            RAISE EXCEPTION 'reconcile_workflow: target table ods.% does not exist', v_ds;
        END IF;
        EXECUTE format(
            'SELECT count(*) FROM ods.%I t '
            'JOIN cp.lineage_link l ON l.lineage_link_id = t._ods_lineage_link_id '
            'JOIN cp.run_log r ON r.run_id = l.consumer_run_id '
            'WHERE r.workflow_run_id = $1 '
            '  AND l.edge_type IN (''canonical_to_sink'',''detail_to_aggregate'')',
            v_ds)
          INTO v_cnt USING p_workflow_run_id;
        v_sink_out := v_sink_out + v_cnt;
    END LOOP;

    -- dlq_out: rows quarantined that STILL represent un-recovered loss.
    -- (b) exclude DLQ rows that were resolved/replayed — that loss has been
    --     recovered (a replay run re-emitted it) so counting it again would
    --     double-count it against raw_in.
    SELECT coalesce(sum(d.record_count), 0) INTO v_dlq_out
      FROM cp.dlq d JOIN cp.run_log r ON r.run_id = d.run_id
     WHERE r.workflow_run_id = p_workflow_run_id
       AND d.status NOT IN ('resolved','replayed');

    v_accounted := v_sink_out + v_dlq_out;
    v_disc := v_raw_in - v_accounted;
    v_status := CASE WHEN v_disc = 0 THEN 'ok'
                     WHEN v_disc > 0 THEN 'breach'
                     ELSE 'double_count' END;

    INSERT INTO cp.reconciliation_log (run_id, check_type, source_count,
                                       accounted_count, discrepancy, status, metrics)
    VALUES (v_terminal_run, 'workflow', v_raw_in, v_accounted, v_disc, v_status,
            jsonb_build_object('raw_in', v_raw_in, 'sink_out', v_sink_out,
                               'dlq_out', v_dlq_out,
                               'workflow_run_id', p_workflow_run_id,
                               'graph_derived', true));
END $function$;

-- =====================================================================
-- F8 — ods.orders: keep the dual _ods_* mirror columns consistent.
--
--   The output_link physical rename is SHELVED (intentional, see spec
--   2026-05-30-output-link-input-edge-rename.md). Target tables therefore carry
--   BOTH _ods_lineage_link_id (the authoritative FK, written by every
--   reconciler) AND _ods_output_link_id (the additive new-name mirror, read by
--   diagnostics / row trace). If a writer ever stamped one and not the other,
--   reconcilers and diagnostics would diverge SILENTLY.
--
--   This CHECK makes that divergence impossible at the storage layer: when both
--   columns are present, they MUST be equal. A null on either side is tolerated
--   (the mirror is additive / best-effort and pre-018 rows have NULL mirror), so
--   this never breaks an existing or partial write — it only forbids two
--   DIFFERENT non-null ids. These mirror columns MUST stay equal for as long as
--   the rename is shelved; do NOT un-shelve here.
--
--   The harness ensure_*targets create the demo tables ad-hoc and are FIX-B's
--   domain; the SAME CHECK should be added to those DDLs there. Here we add it
--   only for the core ods.orders target.
-- =====================================================================
ALTER TABLE ods.orders DROP CONSTRAINT IF EXISTS ods_orders_link_mirror_consistent;
ALTER TABLE ods.orders ADD CONSTRAINT ods_orders_link_mirror_consistent CHECK (
    _ods_output_link_id IS NULL
    OR _ods_lineage_link_id IS NULL
    OR _ods_output_link_id = _ods_lineage_link_id
);
