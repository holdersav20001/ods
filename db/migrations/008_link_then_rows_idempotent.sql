-- 008_link_then_rows_idempotent.sql — make cp.write_link_then_rows
-- row-idempotent on retry (spec decision #5 / QA H1).
--
--     ╔══════════════════════════════════════════════════════════════════╗
--     ║ SUPERSEDED by 016_target_visibility.sql (P10-D). 016 DROPs this    ║
--     ║ exact 8-arg signature and re-declares cp.write_link_then_rows with ║
--     ║ a trailing p_source_file_id uuid DEFAULT NULL (additive). ALL of   ║
--     ║ this body's behaviour (row-idempotency retry guard, the no-run /   ║
--     ║ missing-table RAISEs, the write_lineage_link delegation) is        ║
--     ║ preserved verbatim there. 016 applies last so its definition wins. ║
--     ╚══════════════════════════════════════════════════════════════════╝
--
-- BUG: cp.write_link_then_rows obtains v_link idempotently (cp.write_lineage_link
-- dedups on (consumer_run_id, edge_type, target_ref->>'content_hash')), but then
-- UNCONDITIONALLY re-inserts the target rows. A retried/identical sink write
-- therefore DOUBLES the rows in ods.<dataset> — breaking the idempotency
-- guarantee (re-running an unchanged task must produce the same link AND stable
-- counts).
--
-- FIX: the rows BELONG to the link. After resolving v_link, if that link already
-- has target rows, this is a retry of an identical write — short-circuit and
-- return before the insert loop. Same content_hash twice => same link, same
-- rows, stable counts.
--
-- Reproduced verbatim from 002 EXCEPT for the added v_exists short-circuit guard
-- (and its DECLARE). Signature unchanged.

CREATE OR REPLACE FUNCTION cp.write_link_then_rows(
    p_consumer_run_id uuid, p_edge_type text, p_target_ref jsonb, p_record_count bigint,
    p_edges jsonb, p_rows jsonb, p_sink_type text DEFAULT NULL, p_transform_version text DEFAULT NULL
) RETURNS uuid LANGUAGE plpgsql AS $$
DECLARE v_link uuid; v_dataset text; v_wfid text; v_row jsonb; v_exists boolean;
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
    -- IDEMPOTENT RETRY GUARD: rows belong to the link. If the link already has
    -- target rows, this is a repeat of an identical write — do not double.
    EXECUTE format('SELECT EXISTS (SELECT 1 FROM ods.%I WHERE _ods_lineage_link_id = $1)', v_dataset)
        INTO v_exists USING v_link;
    IF v_exists THEN
        RETURN v_link;
    END IF;
    FOR v_row IN SELECT value FROM jsonb_array_elements(p_rows) LOOP
        EXECUTE format('INSERT INTO ods.%I (payload, _ods_workflow_run_id, _ods_lineage_link_id) VALUES ($1,$2,$3)', v_dataset)
            USING v_row, v_wfid, v_link;
    END LOOP;
    RETURN v_link;
END $$;
