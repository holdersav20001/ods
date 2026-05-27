-- Migration 34: rename ambiguous control-table identifiers.
--
-- Three sources of confusion in the existing schema use the same English
-- word "parent" to mean different things:
--
--   1. pipeline.lineage_edge.parent_run_id   — data parent (upstream producer)
--   2. pipeline.run_log.parents              — orchestration parents
--   3. pipeline.file_state                   — idempotency log, name clashes
--                                              with pipeline.file_catalogue.state
--
-- This migration renames them so each column/table tells you exactly which
-- relationship it represents. No backward-compatibility views — clean break.
--
-- Mapping:
--   pipeline.lineage_edge.child_run_id     -> consumer_run_id
--   pipeline.lineage_edge.parent_run_id    -> upstream_run_id
--   pipeline.lineage_edge.parent_file_id   -> source_file_id
--   pipeline.run_log.parents               -> orchestrators
--   pipeline.file_state                    -> pipeline.file_processing_attempt

BEGIN;

-- ── 1. Drop functions whose plpgsql bodies hard-reference old names ──────────
--    (function bodies are stored as text and do not auto-update on RENAME).
DROP FUNCTION IF EXISTS pipeline.control_start_run(
    uuid, text, text, text, date, uuid, text, bigint, integer, jsonb, jsonb
);
DROP FUNCTION IF EXISTS pipeline.control_patch_run(uuid, jsonb);
DROP FUNCTION IF EXISTS pipeline.control_set_file_state(
    text, uuid, text, bigint, text
);
DROP FUNCTION IF EXISTS pipeline.control_write_lineage_edge(
    uuid, text, uuid, uuid, text, text, bigint
);

-- ── 2. Drop views that hard-reference renamed columns ────────────────────────
DROP VIEW IF EXISTS pipeline.v_lineage;
DROP VIEW IF EXISTS ods.v_rerun_candidates;

-- ── 3. Rename pipeline.lineage_edge columns ──────────────────────────────────
ALTER TABLE pipeline.lineage_edge
    RENAME COLUMN child_run_id TO consumer_run_id;
ALTER TABLE pipeline.lineage_edge
    RENAME COLUMN parent_run_id TO upstream_run_id;
ALTER TABLE pipeline.lineage_edge
    RENAME COLUMN parent_file_id TO source_file_id;

ALTER INDEX IF EXISTS lineage_edge_child_run_idx
    RENAME TO lineage_edge_consumer_run_idx;
ALTER INDEX IF EXISTS lineage_edge_file_id_idx
    RENAME TO lineage_edge_source_file_idx;

-- ── 4. Rename pipeline.run_log.parents -> orchestrators ──────────────────────
ALTER TABLE pipeline.run_log
    RENAME COLUMN parents TO orchestrators;

-- ── 5. Rename pipeline.file_state -> pipeline.file_processing_attempt ────────
ALTER TABLE pipeline.file_state
    RENAME TO file_processing_attempt;

-- ── 6. Recreate dropped views with the new column names ──────────────────────
CREATE VIEW pipeline.v_lineage AS
SELECT
    r.run_id, r.pipeline_type, r.domain, r.dataset, r.business_date,
    fc.s3_raw_path AS source_ref,
    r.kafka_topic, r.kafka_offset_start, r.kafka_offset_end,
    r.config_version_id, r.schema_version_id, r.orchestrators, r.created_at
FROM pipeline.run_log r
LEFT JOIN pipeline.file_catalogue fc ON fc.file_id = r.file_id;

CREATE VIEW ods.v_rerun_candidates AS
SELECT
    rl.run_id,
    rl.pipeline_type,
    rl.domain,
    rl.dataset,
    rl.business_date,
    rl.file_id,
    rl.error_summary,
    rl.ended_at,
    EXISTS (
        SELECT 1 FROM pipeline.lineage_edge le
         JOIN pipeline.run_log parent ON parent.run_id = le.upstream_run_id
        WHERE le.consumer_run_id = rl.run_id
          AND parent.status = 'succeeded'
    ) AS has_succeeded_parent
  FROM pipeline.run_log rl
 WHERE rl.status = 'failed';

COMMENT ON VIEW ods.v_rerun_candidates IS
    'Runs in failed status. has_succeeded_parent indicates whether an '
    'upstream run is in a stable terminal state and the failed run is '
    'safe to replay via python -m ods_pipeline.ops replay.';

-- ── 7. Recreate functions with new parameter and column names ────────────────

CREATE OR REPLACE FUNCTION pipeline.control_start_run(
    p_run_id uuid,
    p_pipeline_type text,
    p_domain text,
    p_dataset text,
    p_business_date date DEFAULT NULL,
    p_file_id uuid DEFAULT NULL,
    p_kafka_topic text DEFAULT NULL,
    p_config_version_id bigint DEFAULT NULL,
    p_schema_version_id integer DEFAULT NULL,
    p_orchestrators jsonb DEFAULT NULL,
    p_runtime_context jsonb DEFAULT NULL
) RETURNS uuid
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pipeline, pg_temp
AS $$
DECLARE
    v_inserted uuid;
    v_existing record;
    v_mismatches text[] := ARRAY[]::text[];
BEGIN
    IF p_run_id IS NULL THEN
        RAISE EXCEPTION 'run_id is required'
            USING ERRCODE = '23502';
    END IF;

    PERFORM pipeline.control_require_text('pipeline_type', p_pipeline_type);
    PERFORM pipeline.control_require_text('domain', p_domain);
    PERFORM pipeline.control_require_text('dataset', p_dataset);

    INSERT INTO pipeline.run_log
        (run_id, pipeline_type, domain, dataset, business_date,
         file_id, status, kafka_topic, config_version_id,
         schema_version_id, orchestrators, runtime_context)
    VALUES
        (p_run_id, p_pipeline_type, p_domain, p_dataset, p_business_date,
         p_file_id, 'running', p_kafka_topic, p_config_version_id,
         p_schema_version_id, p_orchestrators, p_runtime_context)
    ON CONFLICT (run_id) DO NOTHING
    RETURNING run_id INTO v_inserted;

    IF v_inserted IS NULL THEN
        SELECT pipeline_type, domain, dataset, business_date, file_id,
               kafka_topic, config_version_id, schema_version_id, orchestrators
          INTO v_existing
          FROM pipeline.run_log
         WHERE run_id = p_run_id;

        IF NOT FOUND THEN
            RAISE EXCEPTION 'run_log conflict for %, but row not found', p_run_id;
        END IF;

        IF p_pipeline_type IS NOT NULL
           AND v_existing.pipeline_type IS DISTINCT FROM p_pipeline_type THEN
            v_mismatches := array_append(v_mismatches, 'pipeline_type');
        END IF;
        IF p_domain IS NOT NULL AND v_existing.domain IS DISTINCT FROM p_domain THEN
            v_mismatches := array_append(v_mismatches, 'domain');
        END IF;
        IF p_dataset IS NOT NULL AND v_existing.dataset IS DISTINCT FROM p_dataset THEN
            v_mismatches := array_append(v_mismatches, 'dataset');
        END IF;
        IF p_business_date IS NOT NULL
           AND v_existing.business_date IS DISTINCT FROM p_business_date THEN
            v_mismatches := array_append(v_mismatches, 'business_date');
        END IF;
        IF p_file_id IS NOT NULL AND v_existing.file_id IS DISTINCT FROM p_file_id THEN
            v_mismatches := array_append(v_mismatches, 'file_id');
        END IF;
        IF p_kafka_topic IS NOT NULL
           AND v_existing.kafka_topic IS DISTINCT FROM p_kafka_topic THEN
            v_mismatches := array_append(v_mismatches, 'kafka_topic');
        END IF;
        IF p_config_version_id IS NOT NULL
           AND v_existing.config_version_id IS DISTINCT FROM p_config_version_id THEN
            v_mismatches := array_append(v_mismatches, 'config_version_id');
        END IF;
        IF p_schema_version_id IS NOT NULL
           AND v_existing.schema_version_id IS DISTINCT FROM p_schema_version_id THEN
            v_mismatches := array_append(v_mismatches, 'schema_version_id');
        END IF;
        IF p_orchestrators IS NOT NULL
           AND v_existing.orchestrators IS DISTINCT FROM p_orchestrators THEN
            v_mismatches := array_append(v_mismatches, 'orchestrators');
        END IF;

        IF array_length(v_mismatches, 1) IS NOT NULL THEN
            RAISE EXCEPTION 'run_id % already exists with different metadata: %',
                p_run_id, array_to_string(v_mismatches, ', ');
        END IF;
    END IF;

    RETURN p_run_id;
END;
$$;


CREATE OR REPLACE FUNCTION pipeline.control_patch_run(
    p_run_id uuid,
    p_fields jsonb
) RETURNS uuid
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pipeline, pg_temp
AS $$
DECLARE
    v_allowed text[] := ARRAY[
        'status',
        'record_count_source',
        'record_count_dq_pass',
        'record_count_dq_fail',
        'record_count_published',
        'kafka_topic',
        'kafka_offset_start',
        'kafka_offset_end',
        'error_summary',
        'runtime_context',
        'orchestrators',
        'config_version_id',
        'schema_version_id',
        'business_date',
        'file_id',
        'ended_at'
    ];
    v_key text;
BEGIN
    IF p_run_id IS NULL THEN
        RAISE EXCEPTION 'run_id is required'
            USING ERRCODE = '23502';
    END IF;
    IF p_fields IS NULL THEN
        RAISE EXCEPTION 'fields is required'
            USING ERRCODE = '23502';
    END IF;

    FOR v_key IN SELECT jsonb_object_keys(p_fields) LOOP
        IF NOT (v_key = ANY (v_allowed)) THEN
            RAISE EXCEPTION 'control_patch_run: field "%" is not patchable', v_key;
        END IF;
    END LOOP;

    UPDATE pipeline.run_log
       SET status = CASE
               WHEN p_fields ? 'status' THEN p_fields->>'status'
               ELSE status
           END,
           record_count_source = CASE
               WHEN p_fields ? 'record_count_source'
                   THEN (p_fields->>'record_count_source')::bigint
               ELSE record_count_source
           END,
           record_count_dq_pass = CASE
               WHEN p_fields ? 'record_count_dq_pass'
                   THEN (p_fields->>'record_count_dq_pass')::bigint
               ELSE record_count_dq_pass
           END,
           record_count_dq_fail = CASE
               WHEN p_fields ? 'record_count_dq_fail'
                   THEN (p_fields->>'record_count_dq_fail')::bigint
               ELSE record_count_dq_fail
           END,
           record_count_published = CASE
               WHEN p_fields ? 'record_count_published'
                   THEN (p_fields->>'record_count_published')::bigint
               ELSE record_count_published
           END,
           kafka_topic = CASE
               WHEN p_fields ? 'kafka_topic' THEN p_fields->>'kafka_topic'
               ELSE kafka_topic
           END,
           kafka_offset_start = CASE
               WHEN p_fields ? 'kafka_offset_start'
                   THEN (p_fields->>'kafka_offset_start')::bigint
               ELSE kafka_offset_start
           END,
           kafka_offset_end = CASE
               WHEN p_fields ? 'kafka_offset_end'
                   THEN (p_fields->>'kafka_offset_end')::bigint
               ELSE kafka_offset_end
           END,
           error_summary = CASE
               WHEN p_fields ? 'error_summary' THEN p_fields->>'error_summary'
               ELSE error_summary
           END,
           runtime_context = CASE
               WHEN p_fields ? 'runtime_context' THEN p_fields->'runtime_context'
               ELSE runtime_context
           END,
           orchestrators = CASE
               WHEN p_fields ? 'orchestrators' THEN p_fields->'orchestrators'
               ELSE orchestrators
           END,
           config_version_id = CASE
               WHEN p_fields ? 'config_version_id'
                   THEN (p_fields->>'config_version_id')::bigint
               ELSE config_version_id
           END,
           schema_version_id = CASE
               WHEN p_fields ? 'schema_version_id'
                   THEN (p_fields->>'schema_version_id')::integer
               ELSE schema_version_id
           END,
           business_date = CASE
               WHEN p_fields ? 'business_date'
                   THEN (p_fields->>'business_date')::date
               ELSE business_date
           END,
           file_id = CASE
               WHEN p_fields ? 'file_id' THEN (p_fields->>'file_id')::uuid
               ELSE file_id
           END,
           ended_at = CASE
               WHEN p_fields ? 'ended_at'
                   THEN (p_fields->>'ended_at')::timestamp
               ELSE ended_at
           END
     WHERE run_id = p_run_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'run_id % not found', p_run_id;
    END IF;

    RETURN p_run_id;
END;
$$;


CREATE OR REPLACE FUNCTION pipeline.control_set_file_state(
    p_s3_path text,
    p_run_id uuid,
    p_status text,
    p_record_count bigint DEFAULT NULL,
    p_error_reason text DEFAULT NULL
) RETURNS text
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pipeline, pg_temp
AS $$
BEGIN
    PERFORM pipeline.control_require_text('s3_path', p_s3_path);
    IF p_run_id IS NULL THEN
        RAISE EXCEPTION 'run_id is required'
            USING ERRCODE = '23502';
    END IF;
    PERFORM pipeline.control_require_text('status', p_status);
    PERFORM pipeline.control_assert_allowed(
        'status',
        p_status,
        ARRAY['new', 'processing', 'completed', 'failed']
    );
    PERFORM pipeline.control_assert_nonnegative('record_count', p_record_count);

    INSERT INTO pipeline.file_processing_attempt
        (s3_path, run_id, status, record_count, error_reason)
    VALUES
        (p_s3_path, p_run_id, p_status, p_record_count, p_error_reason)
    ON CONFLICT (s3_path) DO UPDATE SET
        run_id = EXCLUDED.run_id,
        status = EXCLUDED.status,
        record_count = EXCLUDED.record_count,
        error_reason = EXCLUDED.error_reason,
        updated_at = NOW();

    RETURN p_s3_path;
END;
$$;


CREATE OR REPLACE FUNCTION pipeline.control_write_lineage_edge(
    p_consumer_run_id uuid,
    p_edge_type text,
    p_upstream_run_id uuid DEFAULT NULL,
    p_source_file_id uuid DEFAULT NULL,
    p_source_ref text DEFAULT NULL,
    p_target_ref text DEFAULT NULL,
    p_record_count bigint DEFAULT NULL
) RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pipeline, pg_temp
AS $$
DECLARE
    v_id bigint;
BEGIN
    IF p_consumer_run_id IS NULL THEN
        RAISE EXCEPTION 'consumer_run_id is required'
            USING ERRCODE = '23502';
    END IF;

    PERFORM pipeline.control_require_text('edge_type', p_edge_type);
    PERFORM pipeline.control_assert_nonnegative('record_count', p_record_count);

    IF p_upstream_run_id IS NULL AND p_source_file_id IS NULL THEN
        RAISE EXCEPTION
            'lineage edge requires upstream_run_id or source_file_id';
    END IF;

    INSERT INTO pipeline.lineage_edge
        (consumer_run_id, upstream_run_id, source_file_id,
         edge_type, source_ref, target_ref, record_count)
    VALUES
        (p_consumer_run_id, p_upstream_run_id, p_source_file_id,
         p_edge_type, p_source_ref, p_target_ref, p_record_count)
    RETURNING lineage_edge_id INTO v_id;

    RETURN v_id;
END;
$$;

COMMIT;
