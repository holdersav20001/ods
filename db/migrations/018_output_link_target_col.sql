-- 018_output_link_target_col.sql — naming cleanup, OPTION B (target column).
--
-- Spec: docs/specs/2026-05-30-output-link-input-edge-rename.md
--       (§"Target Row Columns" option 1 — WRITE BOTH columns).
--
-- PURPOSE
--   Spec §"Target Row Columns": the human name for a target row's output
--   identity is _ods_output_link_id (= cp.output_link.output_link_id). For a
--   short, backward-compatible transition we WRITE BOTH columns
--   (option 1, recommended): the existing _ods_lineage_link_id (which keeps the
--   FK + every existing join/view/trace working) AND a new, additive
--   _ods_output_link_id mirror that carries the SAME id under the new name for
--   the new-name API / row trace.
--
--   _ods_output_link_id does NOT get its own FK — the authoritative FK stays on
--   _ods_lineage_link_id. The new column is a queryable mirror only.
--
--   ╔══════════════════════════════════════════════════════════════════════╗
--   ║ The 016 copy of cp.write_link_then_rows is SUPERSEDED by this 018 body.║
--   ║ 018 applies last so THIS definition wins. ALL prior behaviour is       ║
--   ║ preserved VERBATIM:                                                     ║
--   ║   * 008 row-idempotency retry guard,                                    ║
--   ║   * the no-run_log-row / missing-target-table RAISEs,                   ║
--   ║   * the cp.write_lineage_link delegation (009/010/012/017 hardening     ║
--   ║     still applies — resolved by name at call time),                     ║
--   ║   * the 016 best-effort _ods_source_file_id stamping.                   ║
--   ║ ONLY ADDITION: a parallel best-effort stamp of _ods_output_link_id =    ║
--   ║ the SAME link id, WHEN the target table carries that column (mirrors    ║
--   ║ the 016 _ods_source_file_id column-detection pattern). The signature is ║
--   ║ UNCHANGED (same 9 params) so CREATE OR REPLACE is sufficient — no DROP, ║
--   ║ no caller breakage.                                                     ║
--   ╚══════════════════════════════════════════════════════════════════════╝

-- =====================================================================
-- Add the additive _ods_output_link_id mirror to the core sample target.
--   No FK (the FK stays on _ods_lineage_link_id). Nullable & additive: existing
--   rows are unaffected.
-- =====================================================================
ALTER TABLE ods.orders
    ADD COLUMN IF NOT EXISTS _ods_output_link_id UUID;

-- =====================================================================
-- cp.write_link_then_rows RE-DECLARED (signature unchanged) to ALSO stamp
--   _ods_output_link_id when the target table carries it. Two independent
--   best-effort column probes (_ods_source_file_id, _ods_output_link_id) drive
--   four INSERT shapes so a table with neither, either, or both columns is
--   handled — and pre-016 demo tables (no extra columns) behave exactly as the
--   008 body.
-- =====================================================================
CREATE OR REPLACE FUNCTION cp.write_link_then_rows(
    p_consumer_run_id uuid, p_edge_type text, p_target_ref jsonb, p_record_count bigint,
    p_edges jsonb, p_rows jsonb, p_sink_type text DEFAULT NULL,
    p_transform_version text DEFAULT NULL, p_source_file_id uuid DEFAULT NULL
) RETURNS uuid LANGUAGE plpgsql AS $$
DECLARE v_link uuid; v_dataset text; v_wfid text; v_row jsonb; v_exists boolean;
        v_has_src_col boolean; v_has_out_col boolean;
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
    -- IDEMPOTENT RETRY GUARD (008): rows belong to the link. If the link already
    -- has target rows, this is a repeat of an identical write — do not double.
    EXECUTE format('SELECT EXISTS (SELECT 1 FROM ods.%I WHERE _ods_lineage_link_id = $1)', v_dataset)
        INTO v_exists USING v_link;
    IF v_exists THEN
        RETURN v_link;
    END IF;
    -- PER-TABLE best-effort column detection (mirrors the 016 pattern). Stamp a
    -- column only when the target table actually carries it; otherwise omit it
    -- from the INSERT so pre-016 tables behave exactly as the 008 body.
    --   _ods_source_file_id (016): row-level file attribution.
    --   _ods_output_link_id (018): the new-name mirror of the link id.
    SELECT EXISTS (
        SELECT 1 FROM information_schema.columns
         WHERE table_schema = 'ods' AND table_name = v_dataset
           AND column_name = '_ods_source_file_id')
      INTO v_has_src_col;
    SELECT EXISTS (
        SELECT 1 FROM information_schema.columns
         WHERE table_schema = 'ods' AND table_name = v_dataset
           AND column_name = '_ods_output_link_id')
      INTO v_has_out_col;
    FOR v_row IN SELECT value FROM jsonb_array_elements(p_rows) LOOP
        IF v_has_src_col AND v_has_out_col THEN
            EXECUTE format('INSERT INTO ods.%I (payload, _ods_workflow_run_id, _ods_lineage_link_id, _ods_source_file_id, _ods_output_link_id) VALUES ($1,$2,$3,$4,$3)', v_dataset)
                USING v_row, v_wfid, v_link, p_source_file_id;
        ELSIF v_has_out_col THEN
            EXECUTE format('INSERT INTO ods.%I (payload, _ods_workflow_run_id, _ods_lineage_link_id, _ods_output_link_id) VALUES ($1,$2,$3,$3)', v_dataset)
                USING v_row, v_wfid, v_link;
        ELSIF v_has_src_col THEN
            EXECUTE format('INSERT INTO ods.%I (payload, _ods_workflow_run_id, _ods_lineage_link_id, _ods_source_file_id) VALUES ($1,$2,$3,$4)', v_dataset)
                USING v_row, v_wfid, v_link, p_source_file_id;
        ELSE
            EXECUTE format('INSERT INTO ods.%I (payload, _ods_workflow_run_id, _ods_lineage_link_id) VALUES ($1,$2,$3)', v_dataset)
                USING v_row, v_wfid, v_link;
        END IF;
    END LOOP;
    RETURN v_link;
END $$;
