-- Migration 37 — rename pipeline.lineage_edge.slot_name -> input_slot.
--
-- "slot_name" was ambiguous in code review: developers expected it to be
-- the dataset's slot in a merge composition (which is what
-- pipeline.dataset_config.slot_name carries) when in fact it is the
-- per-row "which input contributed this edge" tag inside a
-- pipeline.lineage_link bundle. Renaming to `input_slot` makes that
-- intent explicit and keeps the column generic for non-file inputs
-- (Kafka topics, Postgres tables, API pulls).
--
-- dataset_config.slot_name is intentionally LEFT ALONE — it represents
-- a different concept (dataset-level role in a merge dataset_config).
BEGIN;

ALTER TABLE pipeline.lineage_edge
    RENAME COLUMN slot_name TO input_slot;

COMMENT ON COLUMN pipeline.lineage_edge.input_slot IS
    'Per-edge tag identifying which input this contribution represents '
    'within the parent lineage_link bundle. Single-source writes use '
    '''main''. Multi-source writes (e.g. merge) use the dataset role '
    '(e.g. ''core'', ''enrichment'').';

DROP VIEW IF EXISTS pipeline.v_lineage_link_sources;
CREATE OR REPLACE VIEW pipeline.v_lineage_link_sources AS
SELECT
    ll.lineage_link_id,
    ll.consumer_run_id,
    ll.edge_type AS link_edge_type,
    ll.target_ref,
    ll.record_count   AS link_record_count,
    ll.created_at,
    le.upstream_run_id,
    le.source_file_id,
    le.source_ref,
    le.input_slot,
    le.edge_type      AS edge_edge_type,
    le.record_count   AS edge_record_count
FROM pipeline.lineage_link ll
LEFT JOIN pipeline.lineage_edge le
    ON le.lineage_link_id = ll.lineage_link_id;

COMMENT ON VIEW pipeline.v_lineage_link_sources IS
    'Join-friendly view: target_ref + input_slot + upstream source for a '
    'lineage_link bundle. Walk back to source by _ods_lineage_link_id.';

CREATE OR REPLACE FUNCTION pipeline.control_write_lineage_link(
    p_lineage_link_id uuid,
    p_consumer_run_id uuid,
    p_edge_type       text,
    p_target_ref      text,
    p_record_count    bigint,
    p_contributions   jsonb
) RETURNS uuid
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pipeline, pg_temp
AS $$
DECLARE
    v_contrib jsonb;
BEGIN
    IF p_lineage_link_id IS NULL THEN
        RAISE EXCEPTION 'lineage_link_id is required' USING ERRCODE = '23502';
    END IF;
    IF p_consumer_run_id IS NULL THEN
        RAISE EXCEPTION 'consumer_run_id is required' USING ERRCODE = '23502';
    END IF;
    PERFORM pipeline.control_require_text('edge_type', p_edge_type);

    IF p_contributions IS NULL
       OR jsonb_array_length(p_contributions) = 0 THEN
        RAISE EXCEPTION 'contributions must contain at least one entry'
            USING ERRCODE = '22023';
    END IF;

    INSERT INTO pipeline.lineage_link
        (lineage_link_id, consumer_run_id, edge_type, target_ref, record_count)
    VALUES
        (p_lineage_link_id, p_consumer_run_id, p_edge_type, p_target_ref,
         p_record_count);

    FOR v_contrib IN SELECT jsonb_array_elements(p_contributions) LOOP
        INSERT INTO pipeline.lineage_edge
            (consumer_run_id, upstream_run_id, source_file_id,
             edge_type, source_ref, target_ref, record_count,
             lineage_link_id, input_slot)
        VALUES
            (p_consumer_run_id,
             NULLIF(v_contrib->>'upstream_run_id', '')::uuid,
             NULLIF(v_contrib->>'source_file_id', '')::uuid,
             COALESCE(v_contrib->>'edge_type', p_edge_type),
             v_contrib->>'source_ref',
             p_target_ref,
             NULLIF(v_contrib->>'record_count', '')::bigint,
             p_lineage_link_id,
             -- Accept either the new key (input_slot) or the legacy
             -- key (slot_name) during a transition window. The Python
             -- helper now emits input_slot.
             COALESCE(v_contrib->>'input_slot', v_contrib->>'slot_name'));
    END LOOP;

    RETURN p_lineage_link_id;
END;
$$;

COMMIT;
