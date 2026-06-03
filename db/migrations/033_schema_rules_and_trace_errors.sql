-- 033_schema_rules_and_trace_errors.sql
--
-- Tighten the developer-facing platform after the detailed audit:
--   1. schema_contract gains validation_rules so contracts can validate more
--      than presence/nullability (types, ranges, enums, date format).
--   2. dashboard_target_row_trace now validates required target columns before
--      dynamic SQL, producing the same clear exception style as diagnostics.

ALTER TABLE cp.schema_contract
    ADD COLUMN IF NOT EXISTS validation_rules jsonb NOT NULL DEFAULT '{}'::jsonb;

UPDATE cp.schema_contract
SET validation_rules = jsonb_build_object(
    'columns', jsonb_build_object(
        'claim_id', jsonb_build_object('type', 'string'),
        'policy_id', jsonb_build_object('type', 'string'),
        'claim_date', jsonb_build_object('type', 'date'),
        'claim_status', jsonb_build_object(
            'type', 'string',
            'allowed', jsonb_build_array('open', 'closed')
        ),
        'claim_amount', jsonb_build_object(
            'type', 'number',
            'min', 0
        )
    )
)
WHERE domain IN ('insurance', 'insurance_dlq')
  AND dataset = 'claim'
  AND layer = 'canonicalization'
  AND schema_version = 'claim.v1';

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
    v_missing text[];
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

    SELECT array_agg(col)
    INTO v_missing
    FROM unnest(ARRAY['row_id', '_ods_output_link_id']) AS required(col)
    WHERE NOT EXISTS (
        SELECT 1
        FROM information_schema.columns c
        WHERE c.table_schema = p_target_schema
          AND c.table_name = p_target_table
          AND c.column_name = required.col
    );

    IF coalesce(array_length(v_missing, 1), 0) > 0 THEN
        RAISE EXCEPTION 'dashboard_target_row_trace: target table %.% is missing required ODS columns: %',
                p_target_schema, p_target_table, array_to_string(v_missing, ', ')
            USING ERRCODE = 'P0001',
                  HINT = 'Expected row_id and _ods_output_link_id so the row can be traced to an output link.';
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
