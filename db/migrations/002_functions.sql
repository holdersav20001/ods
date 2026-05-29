-- 002_functions.sql — cp.* plpgsql control-plane primitives.

-- cp.start_run: insert one run_log row (status default 'running'), return run_id.
-- NOTE: orchestration trigger-edge deferred to P3 (needs link); see plan
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
    RETURNING run_id INTO v_run;
    RETURN v_run;
END $$;

-- cp.patch_run: update only whitelisted keys present in p_patch.
-- Whitelist: status, record_count_in, record_count_out, error.
-- Terminal status ('succeeded'/'failed') also stamps finished_at = now().
-- SUPERSEDED by 007_finished_at_clock.sql (finished_at -> clock_timestamp). Edit there, not here.
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
                                THEN now() ELSE finished_at END
    WHERE run_id = p_run_id;
END $$;

-- cp.register_file: idempotent on (file_md5, business_date). No-op update so
-- RETURNING always yields the existing/new file_id.
-- file<->run association lives in run_log.file_id; file_catalogue dedups across runs
DROP FUNCTION IF EXISTS cp.register_file(text, text, text, date, text, text);
CREATE OR REPLACE FUNCTION cp.register_file(
    p_s3_raw_path text, p_file_md5 text,
    p_business_date date, p_domain text, p_dataset text
) RETURNS uuid LANGUAGE plpgsql AS $$
DECLARE v_file uuid;
BEGIN
    INSERT INTO cp.file_catalogue (s3_raw_path, file_md5, business_date, domain, dataset)
    VALUES (p_s3_raw_path, p_file_md5, p_business_date, p_domain, p_dataset)
    ON CONFLICT (file_md5, business_date)
        DO UPDATE SET state = cp.file_catalogue.state  -- no-op so RETURNING fires
    RETURNING file_id INTO v_file;
    RETURN v_file;
END $$;

-- cp.start_stage: insert run_stage_log row (status 'running'), return stage_log_id.
CREATE OR REPLACE FUNCTION cp.start_stage(
    p_run_id uuid, p_stage text, p_attempt int
) RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE v_id bigint;
BEGIN
    INSERT INTO cp.run_stage_log (run_id, stage, attempt, status)
    VALUES (p_run_id, p_stage, p_attempt, 'running')
    RETURNING stage_log_id INTO v_id;
    RETURN v_id;
END $$;

-- cp.finish_stage: stamp status, counts, metrics, finished_at on the stage row.
-- SUPERSEDED by 007_finished_at_clock.sql (finished_at -> clock_timestamp). Edit there, not here.
CREATE OR REPLACE FUNCTION cp.finish_stage(
    p_stage_log_id bigint, p_status text, p_in bigint, p_out bigint, p_metrics jsonb
) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    UPDATE cp.run_stage_log SET
        status = p_status, record_count_in = p_in, record_count_out = p_out,
        metrics = p_metrics, finished_at = now()
    WHERE stage_log_id = p_stage_log_id;
END $$;

-- cp.write_lineage_link: THE reference pattern — atomic link + N edges in one txn.
CREATE OR REPLACE FUNCTION cp.write_lineage_link(
    p_consumer_run_id uuid, p_edge_type text, p_target_ref jsonb, p_record_count bigint,
    p_edges jsonb, p_sink_type text DEFAULT NULL, p_transform_version text DEFAULT NULL
) RETURNS uuid LANGUAGE plpgsql AS $$
DECLARE v_link uuid; v_edge jsonb;
BEGIN
    IF jsonb_array_length(coalesce(p_edges,'[]'::jsonb)) = 0 THEN
        RAISE EXCEPTION 'write_lineage_link: at least one edge required (run %, type %)', p_consumer_run_id, p_edge_type;
    END IF;
    INSERT INTO cp.lineage_link (consumer_run_id, edge_type, sink_type, target_ref, transform_version, record_count)
    VALUES (p_consumer_run_id, p_edge_type, p_sink_type, p_target_ref, p_transform_version, p_record_count)
    ON CONFLICT (consumer_run_id, edge_type, (target_ref->>'content_hash')) DO NOTHING
    RETURNING lineage_link_id INTO v_link;
    IF v_link IS NULL THEN   -- idempotent replay: link already exists, reuse, edges already written
        SELECT lineage_link_id INTO v_link FROM cp.lineage_link
        WHERE consumer_run_id = p_consumer_run_id AND edge_type = p_edge_type
          AND target_ref->>'content_hash' = p_target_ref->>'content_hash';
        RETURN v_link;
    END IF;
    FOR v_edge IN SELECT * FROM jsonb_array_elements(p_edges) LOOP
        INSERT INTO cp.lineage_edge (lineage_link_id, upstream_run_id, source_file_id, input_slot, edge_type, source_ref, record_count)
        VALUES (v_link,
                nullif(v_edge->>'upstream_run_id','')::uuid,
                nullif(v_edge->>'source_file_id','')::uuid,
                coalesce((v_edge->>'input_slot')::int, 0),
                coalesce(v_edge->>'edge_type', p_edge_type),
                v_edge->'source_ref',
                (v_edge->>'record_count')::bigint);
    END LOOP;
    RETURN v_link;
END $$;

-- cp.write_link_then_rows: THE only sanctioned path to stamp target rows.
-- TARGET-TABLE CONTRACT: the dynamic insert below requires every ods.<dataset>
-- target table to have exactly these columns:
--   payload              jsonb NOT NULL
--   _ods_workflow_run_id text
--   _ods_lineage_link_id uuid NOT NULL REFERENCES cp.lineage_link(lineage_link_id)
-- NOTE: the to_regclass guard only verifies the table EXISTS, not that its shape
-- matches this contract; a mis-shaped target will fail at the EXECUTE insert.
-- SUPERSEDED by 008_link_then_rows_idempotent.sql (row-idempotency short-circuit). Edit there, not here.
CREATE OR REPLACE FUNCTION cp.write_link_then_rows(
    p_consumer_run_id uuid, p_edge_type text, p_target_ref jsonb, p_record_count bigint,
    p_edges jsonb, p_rows jsonb, p_sink_type text DEFAULT NULL, p_transform_version text DEFAULT NULL
) RETURNS uuid LANGUAGE plpgsql AS $$
DECLARE v_link uuid; v_dataset text; v_wfid text; v_row jsonb;
BEGIN
    SELECT dataset, workflow_run_id INTO v_dataset, v_wfid
    FROM cp.run_log WHERE run_id = p_consumer_run_id;
    IF v_dataset IS NULL THEN
        RAISE EXCEPTION 'write_link_then_rows: no run_log row for consumer_run_id %', p_consumer_run_id;
    END IF;
    IF to_regclass('ods.' || quote_ident(v_dataset)) IS NULL THEN
        RAISE EXCEPTION 'write_link_then_rows: target table ods.% does not exist', v_dataset;
    END IF;
    v_link := cp.write_lineage_link(p_consumer_run_id, p_edge_type, p_target_ref,
                                    p_record_count, p_edges, p_sink_type, p_transform_version);
    FOR v_row IN SELECT value FROM jsonb_array_elements(p_rows) LOOP
        EXECUTE format('INSERT INTO ods.%I (payload, _ods_workflow_run_id, _ods_lineage_link_id) VALUES ($1,$2,$3)', v_dataset)
            USING v_row, v_wfid, v_link;
    END LOOP;
    RETURN v_link;
END $$;

-- cp.write_reconciliation_check: compute discrepancy/status, insert recon row.
CREATE OR REPLACE FUNCTION cp.write_reconciliation_check(
    p_run_id uuid, p_check_type text, p_source_count bigint,
    p_accounted_count bigint, p_metrics jsonb DEFAULT NULL
) RETURNS void LANGUAGE plpgsql AS $$
DECLARE v_disc bigint; v_status text;
BEGIN
    v_disc := p_source_count - p_accounted_count;
    v_status := CASE WHEN v_disc = 0 THEN 'ok'
                     WHEN v_disc > 0 THEN 'breach'
                     ELSE 'double_count' END;
    INSERT INTO cp.reconciliation_log (run_id, check_type, source_count, accounted_count, discrepancy, status, metrics)
    VALUES (p_run_id, p_check_type, p_source_count, p_accounted_count, v_disc, v_status, p_metrics);
END $$;

-- cp.quarantine: insert dlq row AND surface a 'quarantine' link+edge in lineage.
CREATE OR REPLACE FUNCTION cp.quarantine(
    p_run_id uuid, p_stage text, p_reason text, p_source_ref jsonb,
    p_payload_ref text, p_record_count bigint
) RETURNS uuid LANGUAGE plpgsql AS $$
DECLARE v_dlq uuid;
BEGIN
    INSERT INTO cp.dlq (run_id, stage, reason, source_ref, payload_ref, record_count)
    VALUES (p_run_id, p_stage, p_reason, p_source_ref, p_payload_ref, p_record_count)
    RETURNING dlq_id INTO v_dlq;
    -- Discriminate the link's content_hash by the unique dlq_id so two quarantine
    -- events in the same run with the same payload_ref do NOT collapse via ON CONFLICT.
    PERFORM cp.write_lineage_link(
        p_run_id, 'quarantine',
        jsonb_build_object('path', p_payload_ref, 'content_hash', v_dlq::text, 'dlq_id', v_dlq),
        p_record_count,
        jsonb_build_array(jsonb_build_object(
            'source_ref', p_source_ref, 'edge_type', 'quarantine', 'record_count', p_record_count)));
    RETURN v_dlq;
END $$;

-- cp.latest_succeeded_run: newest succeeded run for the slice, null if none.
CREATE OR REPLACE FUNCTION cp.latest_succeeded_run(
    p_domain text, p_dataset text, p_business_date date, p_pipeline_type text
) RETURNS uuid LANGUAGE plpgsql AS $$
DECLARE v_run uuid;
BEGIN
    SELECT run_id INTO v_run FROM cp.run_log
    WHERE domain = p_domain AND dataset = p_dataset
      AND business_date = p_business_date AND pipeline_type = p_pipeline_type
      AND status = 'succeeded'
    ORDER BY finished_at DESC NULLS LAST, run_id DESC
    LIMIT 1;
    RETURN v_run;
END $$;
