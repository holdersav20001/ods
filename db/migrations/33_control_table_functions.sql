-- 33_control_table_functions.sql
--
-- Database-owned API for pipeline control tables.  Project code should call
-- these functions rather than issuing direct INSERT/UPDATE statements against
-- pipeline.run_log, pipeline.run_stage_log, pipeline.file_catalogue, and the
-- related audit tables.

BEGIN;

CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA public;

CREATE OR REPLACE FUNCTION pipeline.control_require_text(
    p_field text,
    p_value text
) RETURNS void
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
    IF p_value IS NULL OR btrim(p_value) = '' THEN
        RAISE EXCEPTION '% is required', p_field
            USING ERRCODE = '23502';
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION pipeline.control_assert_allowed(
    p_field text,
    p_value text,
    p_allowed text[]
) RETURNS void
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
    IF p_value IS NULL THEN
        RETURN;
    END IF;

    IF NOT p_value = ANY(p_allowed) THEN
        RAISE EXCEPTION '% has invalid value %, expected one of %',
            p_field, p_value, array_to_string(p_allowed, ', ')
            USING ERRCODE = '22023';
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION pipeline.control_assert_nonnegative(
    p_field text,
    p_value bigint
) RETURNS void
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
    IF p_value IS NOT NULL AND p_value < 0 THEN
        RAISE EXCEPTION '% must be non-negative, got %', p_field, p_value
            USING ERRCODE = '22003';
    END IF;
END;
$$;

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
    p_parents jsonb DEFAULT NULL,
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
         schema_version_id, parents, runtime_context)
    VALUES
        (p_run_id, p_pipeline_type, p_domain, p_dataset, p_business_date,
         p_file_id, 'running', p_kafka_topic, p_config_version_id,
         p_schema_version_id, p_parents, p_runtime_context)
    ON CONFLICT (run_id) DO NOTHING
    RETURNING run_id INTO v_inserted;

    IF v_inserted IS NULL THEN
        SELECT pipeline_type, domain, dataset, business_date, file_id,
               kafka_topic, config_version_id, schema_version_id, parents
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
        IF p_parents IS NOT NULL AND v_existing.parents IS DISTINCT FROM p_parents THEN
            v_mismatches := array_append(v_mismatches, 'parents');
        END IF;

        IF array_length(v_mismatches, 1) IS NOT NULL THEN
            RAISE EXCEPTION 'run_id % already exists with different metadata: %',
                p_run_id, array_to_string(v_mismatches, ', ');
        END IF;
    END IF;

    RETURN p_run_id;
END;
$$;

CREATE OR REPLACE FUNCTION pipeline.control_update_run(
    p_run_id uuid,
    p_status text DEFAULT NULL,
    p_record_count_source bigint DEFAULT NULL,
    p_record_count_dq_pass bigint DEFAULT NULL,
    p_record_count_dq_fail bigint DEFAULT NULL,
    p_record_count_published bigint DEFAULT NULL,
    p_kafka_topic text DEFAULT NULL,
    p_kafka_offset_start bigint DEFAULT NULL,
    p_kafka_offset_end bigint DEFAULT NULL,
    p_error_summary text DEFAULT NULL,
    p_runtime_context jsonb DEFAULT NULL
) RETURNS uuid
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pipeline, pg_temp
AS $$
BEGIN
    IF p_run_id IS NULL THEN
        RAISE EXCEPTION 'run_id is required'
            USING ERRCODE = '23502';
    END IF;

    PERFORM pipeline.control_assert_allowed(
        'status',
        p_status,
        ARRAY['running', 'succeeded', 'failed', 'partial']
    );
    PERFORM pipeline.control_assert_nonnegative(
        'record_count_source',
        p_record_count_source
    );
    PERFORM pipeline.control_assert_nonnegative(
        'record_count_dq_pass',
        p_record_count_dq_pass
    );
    PERFORM pipeline.control_assert_nonnegative(
        'record_count_dq_fail',
        p_record_count_dq_fail
    );
    PERFORM pipeline.control_assert_nonnegative(
        'record_count_published',
        p_record_count_published
    );
    PERFORM pipeline.control_assert_nonnegative(
        'kafka_offset_start',
        p_kafka_offset_start
    );
    PERFORM pipeline.control_assert_nonnegative(
        'kafka_offset_end',
        p_kafka_offset_end
    );

    IF p_kafka_offset_start IS NOT NULL
       AND p_kafka_offset_end IS NOT NULL
       AND p_kafka_offset_end < p_kafka_offset_start THEN
        RAISE EXCEPTION 'kafka_offset_end must be >= kafka_offset_start'
            USING ERRCODE = '22023';
    END IF;

    UPDATE pipeline.run_log
       SET status = COALESCE(p_status, status),
           record_count_source = COALESCE(p_record_count_source, record_count_source),
           record_count_dq_pass = COALESCE(p_record_count_dq_pass, record_count_dq_pass),
           record_count_dq_fail = COALESCE(p_record_count_dq_fail, record_count_dq_fail),
           record_count_published = COALESCE(
               p_record_count_published,
               record_count_published
           ),
           kafka_topic = COALESCE(p_kafka_topic, kafka_topic),
           kafka_offset_start = COALESCE(p_kafka_offset_start, kafka_offset_start),
           kafka_offset_end = COALESCE(p_kafka_offset_end, kafka_offset_end),
           error_summary = COALESCE(p_error_summary, error_summary),
           runtime_context = COALESCE(p_runtime_context, runtime_context),
           ended_at = CASE
               WHEN p_status IN ('succeeded', 'failed', 'partial')
                    THEN COALESCE(ended_at, NOW())
               ELSE ended_at
           END
     WHERE run_id = p_run_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'run_id % not found', p_run_id;
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
    v_invalid text[];
BEGIN
    IF p_run_id IS NULL THEN
        RAISE EXCEPTION 'run_id is required'
            USING ERRCODE = '23502';
    END IF;

    IF p_fields IS NULL OR p_fields = '{}'::jsonb THEN
        RETURN p_run_id;
    END IF;

    IF jsonb_typeof(p_fields) <> 'object' THEN
        RAISE EXCEPTION 'run_log patch fields must be a JSON object'
            USING ERRCODE = '22023';
    END IF;

    SELECT array_agg(key)
      INTO v_invalid
      FROM jsonb_object_keys(p_fields) AS key
     WHERE key NOT IN (
        'status',
        'record_count_source',
        'record_count_dq_pass',
        'record_count_dq_fail',
        'record_count_published',
        'kafka_topic',
        'kafka_offset_start',
        'kafka_offset_end',
        'config_version_id',
        'schema_version_id',
        'parents',
        'runtime_context',
        'error_summary',
        'file_id',
        'business_date'
     );

    IF array_length(v_invalid, 1) IS NOT NULL THEN
        RAISE EXCEPTION 'Unknown run_log fields: %', v_invalid;
    END IF;

    IF p_fields ? 'status' THEN
        IF p_fields->>'status' IS NULL OR btrim(p_fields->>'status') = '' THEN
            RAISE EXCEPTION 'status is required when provided'
                USING ERRCODE = '23502';
        END IF;
        PERFORM pipeline.control_assert_allowed(
            'status',
            p_fields->>'status',
            ARRAY['running', 'succeeded', 'failed', 'partial']
        );
    END IF;

    IF p_fields ? 'record_count_source' AND p_fields->>'record_count_source' IS NOT NULL THEN
        PERFORM pipeline.control_assert_nonnegative(
            'record_count_source',
            (p_fields->>'record_count_source')::bigint
        );
    END IF;
    IF p_fields ? 'record_count_dq_pass' AND p_fields->>'record_count_dq_pass' IS NOT NULL THEN
        PERFORM pipeline.control_assert_nonnegative(
            'record_count_dq_pass',
            (p_fields->>'record_count_dq_pass')::bigint
        );
    END IF;
    IF p_fields ? 'record_count_dq_fail' AND p_fields->>'record_count_dq_fail' IS NOT NULL THEN
        PERFORM pipeline.control_assert_nonnegative(
            'record_count_dq_fail',
            (p_fields->>'record_count_dq_fail')::bigint
        );
    END IF;
    IF p_fields ? 'record_count_published' AND p_fields->>'record_count_published' IS NOT NULL THEN
        PERFORM pipeline.control_assert_nonnegative(
            'record_count_published',
            (p_fields->>'record_count_published')::bigint
        );
    END IF;
    IF p_fields ? 'kafka_offset_start' AND p_fields->>'kafka_offset_start' IS NOT NULL THEN
        PERFORM pipeline.control_assert_nonnegative(
            'kafka_offset_start',
            (p_fields->>'kafka_offset_start')::bigint
        );
    END IF;
    IF p_fields ? 'kafka_offset_end' AND p_fields->>'kafka_offset_end' IS NOT NULL THEN
        PERFORM pipeline.control_assert_nonnegative(
            'kafka_offset_end',
            (p_fields->>'kafka_offset_end')::bigint
        );
    END IF;

    IF p_fields ? 'kafka_offset_start'
       AND p_fields ? 'kafka_offset_end'
       AND p_fields->>'kafka_offset_start' IS NOT NULL
       AND p_fields->>'kafka_offset_end' IS NOT NULL
       AND (p_fields->>'kafka_offset_end')::bigint
           < (p_fields->>'kafka_offset_start')::bigint THEN
        RAISE EXCEPTION 'kafka_offset_end must be >= kafka_offset_start'
            USING ERRCODE = '22023';
    END IF;

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
           parents = CASE
               WHEN p_fields ? 'parents' THEN p_fields->'parents'
               ELSE parents
           END,
           runtime_context = CASE
               WHEN p_fields ? 'runtime_context' THEN p_fields->'runtime_context'
               ELSE runtime_context
           END,
           error_summary = CASE
               WHEN p_fields ? 'error_summary' THEN p_fields->>'error_summary'
               ELSE error_summary
           END,
           file_id = CASE
               WHEN p_fields ? 'file_id' THEN (p_fields->>'file_id')::uuid
               ELSE file_id
           END,
           business_date = CASE
               WHEN p_fields ? 'business_date' THEN (p_fields->>'business_date')::date
               ELSE business_date
           END,
           ended_at = CASE
               WHEN p_fields->>'status' IN ('succeeded', 'failed', 'partial')
                   THEN COALESCE(ended_at, NOW())
               ELSE ended_at
           END
     WHERE run_id = p_run_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'run_id % not found', p_run_id;
    END IF;

    RETURN p_run_id;
END;
$$;

CREATE OR REPLACE FUNCTION pipeline.control_register_file(
    p_domain text,
    p_dataset text,
    p_business_date date,
    p_file_md5 text,
    p_s3_raw_path text,
    p_file_id uuid DEFAULT NULL,
    p_sftp_path text DEFAULT NULL,
    p_s3_curated_path text DEFAULT NULL,
    p_file_size_bytes bigint DEFAULT NULL,
    p_source_row_count bigint DEFAULT NULL,
    p_state text DEFAULT 'ingesting',
    p_last_run_id uuid DEFAULT NULL
) RETURNS uuid
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pipeline, pg_temp
AS $$
DECLARE
    v_file_id uuid := COALESCE(p_file_id, public.gen_random_uuid());
BEGIN
    PERFORM pipeline.control_require_text('domain', p_domain);
    PERFORM pipeline.control_require_text('dataset', p_dataset);
    PERFORM pipeline.control_require_text('file_md5', p_file_md5);
    PERFORM pipeline.control_require_text('s3_raw_path', p_s3_raw_path);
    PERFORM pipeline.control_assert_allowed(
        'state',
        p_state,
        ARRAY['received', 'ingesting', 'curated', 'staged', 'sunk', 'completed', 'failed']
    );
    PERFORM pipeline.control_assert_nonnegative('file_size_bytes', p_file_size_bytes);
    PERFORM pipeline.control_assert_nonnegative('source_row_count', p_source_row_count);

    IF p_business_date IS NULL THEN
        RAISE EXCEPTION 'business_date is required'
            USING ERRCODE = '23502';
    END IF;

    IF p_file_id IS NOT NULL THEN
        UPDATE pipeline.file_catalogue
           SET state = COALESCE(p_state, state),
               file_md5 = COALESCE(p_file_md5, file_md5),
               s3_raw_path = COALESCE(p_s3_raw_path, s3_raw_path),
               sftp_path = COALESCE(p_sftp_path, sftp_path),
               s3_curated_path = COALESCE(p_s3_curated_path, s3_curated_path),
               file_size_bytes = COALESCE(p_file_size_bytes, file_size_bytes),
               source_row_count = COALESCE(p_source_row_count, source_row_count),
               last_run_id = COALESCE(p_last_run_id, last_run_id),
               state_updated_at = NOW()
         WHERE file_id = p_file_id;

        IF FOUND THEN
            RETURN p_file_id;
        END IF;
    END IF;

    INSERT INTO pipeline.file_catalogue
        (file_id, domain, dataset, business_date, file_md5,
         s3_raw_path, sftp_path, s3_curated_path,
         file_size_bytes, source_row_count, state, last_run_id)
    VALUES
        (v_file_id, p_domain, p_dataset, p_business_date, p_file_md5,
         p_s3_raw_path, p_sftp_path, p_s3_curated_path,
         p_file_size_bytes, p_source_row_count, p_state, p_last_run_id)
    ON CONFLICT (domain, dataset, s3_raw_path)
    WHERE s3_raw_path IS NOT NULL
    DO UPDATE SET
        state = EXCLUDED.state,
        business_date = EXCLUDED.business_date,
        file_md5 = EXCLUDED.file_md5,
        s3_raw_path = COALESCE(EXCLUDED.s3_raw_path, pipeline.file_catalogue.s3_raw_path),
        s3_curated_path = COALESCE(
            EXCLUDED.s3_curated_path,
            pipeline.file_catalogue.s3_curated_path
        ),
        sftp_path = COALESCE(EXCLUDED.sftp_path, pipeline.file_catalogue.sftp_path),
        file_size_bytes = COALESCE(
            EXCLUDED.file_size_bytes,
            pipeline.file_catalogue.file_size_bytes
        ),
        source_row_count = COALESCE(
            EXCLUDED.source_row_count,
            pipeline.file_catalogue.source_row_count
        ),
        last_run_id = EXCLUDED.last_run_id,
        state_updated_at = NOW()
    RETURNING file_id INTO v_file_id;

    RETURN v_file_id;
END;
$$;

CREATE OR REPLACE FUNCTION pipeline.control_update_file_catalogue(
    p_file_id uuid DEFAULT NULL,
    p_s3_raw_path text DEFAULT NULL,
    p_state text DEFAULT NULL,
    p_s3_curated_path text DEFAULT NULL,
    p_source_row_count bigint DEFAULT NULL,
    p_last_run_id uuid DEFAULT NULL
) RETURNS uuid
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pipeline, pg_temp
AS $$
DECLARE
    v_file_id uuid;
BEGIN
    IF p_file_id IS NULL THEN
        PERFORM pipeline.control_require_text('s3_raw_path', p_s3_raw_path);
    END IF;

    PERFORM pipeline.control_assert_allowed(
        'state',
        p_state,
        ARRAY['received', 'ingesting', 'curated', 'staged', 'sunk', 'completed', 'failed']
    );
    PERFORM pipeline.control_assert_nonnegative('source_row_count', p_source_row_count);

    UPDATE pipeline.file_catalogue
       SET state = COALESCE(p_state, state),
           s3_curated_path = COALESCE(p_s3_curated_path, s3_curated_path),
           source_row_count = COALESCE(p_source_row_count, source_row_count),
           last_run_id = COALESCE(p_last_run_id, last_run_id),
           state_updated_at = NOW()
     WHERE (p_file_id IS NOT NULL AND file_id = p_file_id)
        OR (p_file_id IS NULL AND p_s3_raw_path IS NOT NULL AND s3_raw_path = p_s3_raw_path)
    RETURNING file_id INTO v_file_id;

    IF v_file_id IS NULL THEN
        RAISE EXCEPTION 'file_catalogue row not found (file_id=%, s3_raw_path=%)',
            p_file_id, p_s3_raw_path;
    END IF;

    RETURN v_file_id;
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

    INSERT INTO pipeline.file_state
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

CREATE OR REPLACE FUNCTION pipeline.control_write_stage_event(
    p_run_id uuid,
    p_stage text,
    p_status text,
    p_event_type text DEFAULT NULL,
    p_attempt_number integer DEFAULT 1,
    p_input_ref text DEFAULT NULL,
    p_output_ref text DEFAULT NULL,
    p_record_count_in bigint DEFAULT NULL,
    p_record_count_out bigint DEFAULT NULL,
    p_metrics jsonb DEFAULT NULL,
    p_error text DEFAULT NULL,
    p_airflow_dag_id text DEFAULT NULL,
    p_airflow_run_id text DEFAULT NULL,
    p_spark_app_id text DEFAULT NULL
) RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pipeline, pg_temp
AS $$
DECLARE
    v_id bigint;
    v_is_open boolean := (
        p_event_type = 'stage_started'
        OR (p_event_type IS NULL AND p_status = 'running')
    );
BEGIN
    IF p_run_id IS NULL THEN
        RAISE EXCEPTION 'run_id is required'
            USING ERRCODE = '23502';
    END IF;

    PERFORM pipeline.control_require_text('stage', p_stage);
    PERFORM pipeline.control_require_text('status', p_status);
    PERFORM pipeline.control_assert_allowed(
        'status',
        p_status,
        ARRAY['running', 'succeeded', 'failed', 'skipped', 'warned', 'partial']
    );
    PERFORM pipeline.control_assert_allowed(
        'event_type',
        p_event_type,
        ARRAY[
            'stage_started',
            'stage_completed',
            'stage_failed',
            'stage_skipped',
            'stage_warned',
            'stage_heartbeat'
        ]
    );
    PERFORM pipeline.control_assert_nonnegative('record_count_in', p_record_count_in);
    PERFORM pipeline.control_assert_nonnegative('record_count_out', p_record_count_out);

    IF p_attempt_number IS NULL OR p_attempt_number <= 0 THEN
        RAISE EXCEPTION 'attempt_number must be positive'
            USING ERRCODE = '22023';
    END IF;

    IF v_is_open THEN
        INSERT INTO pipeline.run_stage_log
            (run_id, stage, status, event_type, attempt_number,
             started_at, ended_at, input_ref, output_ref,
             record_count_in, record_count_out, metrics, error,
             airflow_dag_id, airflow_run_id, spark_app_id)
        VALUES
            (p_run_id, p_stage, p_status, COALESCE(p_event_type, 'stage_started'),
             p_attempt_number, NOW(), NULL, p_input_ref, p_output_ref,
             p_record_count_in, p_record_count_out, p_metrics, p_error,
             p_airflow_dag_id, p_airflow_run_id, p_spark_app_id)
        ON CONFLICT (run_id, stage, attempt_number)
        WHERE event_type = 'stage_started'
        DO NOTHING
        RETURNING id INTO v_id;
    ELSE
        INSERT INTO pipeline.run_stage_log
            (run_id, stage, status, event_type, attempt_number,
             started_at, ended_at, input_ref, output_ref,
             record_count_in, record_count_out, metrics, error,
             airflow_dag_id, airflow_run_id, spark_app_id)
        VALUES
            (p_run_id, p_stage, p_status, p_event_type, p_attempt_number,
             NOW(), NOW(), p_input_ref, p_output_ref,
             p_record_count_in, p_record_count_out, p_metrics, p_error,
             p_airflow_dag_id, p_airflow_run_id, p_spark_app_id)
        RETURNING id INTO v_id;
    END IF;

    RETURN v_id;
END;
$$;

CREATE OR REPLACE FUNCTION pipeline.control_start_stage(
    p_run_id uuid,
    p_stage text,
    p_attempt_number integer DEFAULT NULL,
    p_input_ref text DEFAULT NULL,
    p_record_count_in bigint DEFAULT NULL,
    p_metrics jsonb DEFAULT NULL,
    p_airflow_dag_id text DEFAULT NULL,
    p_airflow_run_id text DEFAULT NULL,
    p_spark_app_id text DEFAULT NULL
) RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pipeline, pg_temp
AS $$
DECLARE
    v_attempt_number integer;
BEGIN
    IF p_run_id IS NULL THEN
        RAISE EXCEPTION 'run_id is required'
            USING ERRCODE = '23502';
    END IF;

    PERFORM pipeline.control_require_text('stage', p_stage);
    PERFORM pipeline.control_assert_nonnegative('record_count_in', p_record_count_in);

    IF p_attempt_number IS NULL THEN
        SELECT COALESCE(MAX(attempt_number), 0) + 1
          INTO v_attempt_number
          FROM pipeline.run_stage_log
         WHERE run_id = p_run_id
           AND stage = p_stage;
    ELSE
        v_attempt_number := p_attempt_number;
    END IF;

    IF v_attempt_number <= 0 THEN
        RAISE EXCEPTION 'attempt_number must be positive'
            USING ERRCODE = '22023';
    END IF;

    INSERT INTO pipeline.run_stage_log
        (run_id, stage, status, event_type, attempt_number,
         started_at, ended_at, input_ref, record_count_in, metrics,
         airflow_dag_id, airflow_run_id, spark_app_id)
    VALUES
        (p_run_id, p_stage, 'running', 'stage_started', v_attempt_number,
         NOW(), NULL, p_input_ref, p_record_count_in, p_metrics,
         p_airflow_dag_id, p_airflow_run_id, p_spark_app_id)
    ON CONFLICT (run_id, stage, attempt_number)
    WHERE event_type = 'stage_started'
    DO NOTHING;

    RETURN v_attempt_number;
END;
$$;

CREATE OR REPLACE FUNCTION pipeline.control_finish_stage(
    p_run_id uuid,
    p_stage text,
    p_status text,
    p_event_type text DEFAULT NULL,
    p_attempt_number integer DEFAULT 1,
    p_input_ref text DEFAULT NULL,
    p_output_ref text DEFAULT NULL,
    p_record_count_in bigint DEFAULT NULL,
    p_record_count_out bigint DEFAULT NULL,
    p_metrics jsonb DEFAULT NULL,
    p_error text DEFAULT NULL,
    p_airflow_dag_id text DEFAULT NULL,
    p_airflow_run_id text DEFAULT NULL,
    p_spark_app_id text DEFAULT NULL
) RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pipeline, pg_temp
AS $$
DECLARE
    v_id bigint;
    v_event_type text := COALESCE(
        p_event_type,
        CASE
            WHEN p_status = 'failed' THEN 'stage_failed'
            WHEN p_status = 'skipped' THEN 'stage_skipped'
            WHEN p_status = 'warned' THEN 'stage_warned'
            ELSE 'stage_completed'
        END
    );
BEGIN
    IF p_run_id IS NULL THEN
        RAISE EXCEPTION 'run_id is required'
            USING ERRCODE = '23502';
    END IF;

    PERFORM pipeline.control_require_text('stage', p_stage);
    PERFORM pipeline.control_require_text('status', p_status);
    PERFORM pipeline.control_assert_allowed(
        'status',
        p_status,
        ARRAY['succeeded', 'failed', 'skipped', 'warned', 'partial']
    );
    PERFORM pipeline.control_assert_allowed(
        'event_type',
        v_event_type,
        ARRAY[
            'stage_completed',
            'stage_failed',
            'stage_skipped',
            'stage_warned'
        ]
    );
    PERFORM pipeline.control_assert_nonnegative('record_count_in', p_record_count_in);
    PERFORM pipeline.control_assert_nonnegative('record_count_out', p_record_count_out);

    IF p_attempt_number IS NULL OR p_attempt_number <= 0 THEN
        RAISE EXCEPTION 'attempt_number must be positive'
            USING ERRCODE = '22023';
    END IF;

    SELECT id
      INTO v_id
      FROM pipeline.run_stage_log
     WHERE run_id = p_run_id
       AND stage = p_stage
       AND attempt_number = p_attempt_number
       AND status = 'running'
       AND ended_at IS NULL
     ORDER BY started_at DESC, id DESC
     LIMIT 1
     FOR UPDATE SKIP LOCKED;

    IF v_id IS NOT NULL THEN
        UPDATE pipeline.run_stage_log
           SET status = p_status,
               event_type = v_event_type,
               ended_at = NOW(),
               input_ref = COALESCE(p_input_ref, input_ref),
               output_ref = COALESCE(p_output_ref, output_ref),
               record_count_in = COALESCE(p_record_count_in, record_count_in),
               record_count_out = COALESCE(p_record_count_out, record_count_out),
               metrics = COALESCE(p_metrics, metrics),
               error = COALESCE(p_error, error),
               airflow_dag_id = COALESCE(p_airflow_dag_id, airflow_dag_id),
               airflow_run_id = COALESCE(p_airflow_run_id, airflow_run_id),
               spark_app_id = COALESCE(p_spark_app_id, spark_app_id)
         WHERE id = v_id;
    ELSE
        INSERT INTO pipeline.run_stage_log
            (run_id, stage, status, event_type, attempt_number,
             started_at, ended_at, input_ref, output_ref,
             record_count_in, record_count_out, metrics, error,
             airflow_dag_id, airflow_run_id, spark_app_id)
        VALUES
            (p_run_id, p_stage, p_status, v_event_type, p_attempt_number,
             NOW(), NOW(), p_input_ref, p_output_ref,
             p_record_count_in, p_record_count_out, p_metrics, p_error,
             p_airflow_dag_id, p_airflow_run_id, p_spark_app_id)
        RETURNING id INTO v_id;
    END IF;

    RETURN v_id;
END;
$$;

CREATE OR REPLACE FUNCTION pipeline.control_write_lineage_edge(
    p_child_run_id uuid,
    p_edge_type text,
    p_parent_run_id uuid DEFAULT NULL,
    p_parent_file_id uuid DEFAULT NULL,
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
    IF p_child_run_id IS NULL THEN
        RAISE EXCEPTION 'child_run_id is required'
            USING ERRCODE = '23502';
    END IF;

    PERFORM pipeline.control_require_text('edge_type', p_edge_type);
    PERFORM pipeline.control_assert_nonnegative('record_count', p_record_count);

    IF p_parent_run_id IS NULL AND p_parent_file_id IS NULL THEN
        RAISE EXCEPTION 'lineage edge requires parent_run_id or parent_file_id';
    END IF;

    INSERT INTO pipeline.lineage_edge
        (child_run_id, parent_run_id, parent_file_id,
         edge_type, source_ref, target_ref, record_count)
    VALUES
        (p_child_run_id, p_parent_run_id, p_parent_file_id,
         p_edge_type, p_source_ref, p_target_ref, p_record_count)
    RETURNING lineage_edge_id INTO v_id;

    RETURN v_id;
END;
$$;

CREATE OR REPLACE FUNCTION pipeline.control_write_reconciliation_check(
    p_check_type text,
    p_run_id uuid,
    p_domain text,
    p_dataset text,
    p_business_date date,
    p_source_count bigint DEFAULT NULL,
    p_kafka_count bigint DEFAULT NULL,
    p_postgres_count bigint DEFAULT NULL,
    p_status text DEFAULT 'ok',
    p_detail text DEFAULT NULL,
    p_window_start timestamp DEFAULT NULL,
    p_window_end timestamp DEFAULT NULL
) RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pipeline, pg_temp
AS $$
DECLARE
    v_id bigint;
    v_discrepancy bigint;
    v_pct numeric(8,4);
BEGIN
    PERFORM pipeline.control_require_text('check_type', p_check_type);
    PERFORM pipeline.control_require_text('domain', p_domain);
    PERFORM pipeline.control_require_text('dataset', p_dataset);
    PERFORM pipeline.control_require_text('status', p_status);
    PERFORM pipeline.control_assert_allowed(
        'status',
        p_status,
        ARRAY['ok', 'failed', 'pending', 'passed', 'skipped', 'warned']
    );
    PERFORM pipeline.control_assert_nonnegative('source_count', p_source_count);
    PERFORM pipeline.control_assert_nonnegative('kafka_count', p_kafka_count);
    PERFORM pipeline.control_assert_nonnegative('postgres_count', p_postgres_count);

    IF p_window_start IS NOT NULL
       AND p_window_end IS NOT NULL
       AND p_window_end < p_window_start THEN
        RAISE EXCEPTION 'window_end must be >= window_start'
            USING ERRCODE = '22023';
    END IF;

    IF p_source_count IS NOT NULL AND p_kafka_count IS NOT NULL THEN
        v_discrepancy := p_kafka_count - p_source_count;
    ELSIF p_kafka_count IS NOT NULL AND p_postgres_count IS NOT NULL THEN
        v_discrepancy := p_postgres_count - p_kafka_count;
    ELSIF p_source_count IS NOT NULL AND p_postgres_count IS NOT NULL THEN
        v_discrepancy := p_postgres_count - p_source_count;
    END IF;

    IF v_discrepancy IS NOT NULL AND COALESCE(p_source_count, 0) <> 0 THEN
        v_pct := ROUND((100.0 * v_discrepancy / p_source_count)::numeric, 4);
    END IF;

    INSERT INTO pipeline.reconciliation_log
        (check_type, run_id, domain, dataset, business_date,
         window_start, window_end, source_count, kafka_count, postgres_count,
         discrepancy_count, discrepancy_pct, status, detail)
    VALUES
        (p_check_type, p_run_id, p_domain, p_dataset, p_business_date,
         p_window_start, p_window_end, p_source_count, p_kafka_count,
         p_postgres_count, v_discrepancy, v_pct, p_status, p_detail)
    RETURNING id INTO v_id;

    RETURN v_id;
END;
$$;

CREATE OR REPLACE FUNCTION pipeline.control_record_run_event(
    p_run_id text,
    p_event_type text,
    p_domain text,
    p_dataset text,
    p_business_date text,
    p_status text,
    p_pipeline_type text DEFAULT NULL,
    p_record_count_source integer DEFAULT NULL,
    p_record_count_dq_pass integer DEFAULT NULL,
    p_record_count_dq_fail integer DEFAULT NULL,
    p_record_count_published integer DEFAULT NULL,
    p_kafka_topic text DEFAULT NULL,
    p_kafka_offset_end bigint DEFAULT NULL,
    p_error_summary text DEFAULT NULL,
    p_occurred_at timestamp DEFAULT NULL,
    p_file_id text DEFAULT NULL,
    p_s3_raw_path text DEFAULT NULL,
    p_s3_curated_path text DEFAULT NULL,
    p_file_md5 text DEFAULT NULL,
    p_kafka_offset_start bigint DEFAULT NULL,
    p_stages jsonb DEFAULT NULL
) RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pipeline, pg_temp
AS $$
DECLARE
    v_id bigint;
BEGIN
    PERFORM pipeline.control_require_text('run_id', p_run_id);
    PERFORM pipeline.control_require_text('event_type', p_event_type);
    PERFORM pipeline.control_require_text('domain', p_domain);
    PERFORM pipeline.control_require_text('dataset', p_dataset);
    PERFORM pipeline.control_require_text('business_date', p_business_date);
    PERFORM pipeline.control_require_text('status', p_status);
    PERFORM pipeline.control_assert_allowed(
        'status',
        p_status,
        ARRAY['running', 'succeeded', 'failed', 'partial']
    );
    PERFORM pipeline.control_assert_nonnegative(
        'record_count_source',
        p_record_count_source
    );
    PERFORM pipeline.control_assert_nonnegative(
        'record_count_dq_pass',
        p_record_count_dq_pass
    );
    PERFORM pipeline.control_assert_nonnegative(
        'record_count_dq_fail',
        p_record_count_dq_fail
    );
    PERFORM pipeline.control_assert_nonnegative(
        'record_count_published',
        p_record_count_published
    );
    PERFORM pipeline.control_assert_nonnegative(
        'kafka_offset_start',
        p_kafka_offset_start
    );
    PERFORM pipeline.control_assert_nonnegative(
        'kafka_offset_end',
        p_kafka_offset_end
    );

    IF p_file_md5 IS NOT NULL THEN
        PERFORM pipeline.control_require_text('file_md5', p_file_md5);
    END IF;

    IF p_kafka_offset_start IS NOT NULL
       AND p_kafka_offset_end IS NOT NULL
       AND p_kafka_offset_end < p_kafka_offset_start THEN
        RAISE EXCEPTION 'kafka_offset_end must be >= kafka_offset_start'
            USING ERRCODE = '22023';
    END IF;

    INSERT INTO pipeline.run_events
        (run_id, event_type, pipeline_type, domain, dataset, business_date,
         status, record_count_source, record_count_dq_pass,
         record_count_dq_fail, record_count_published,
         kafka_topic, kafka_offset_end, error_summary, occurred_at,
         file_id, s3_raw_path, s3_curated_path, file_md5,
         kafka_offset_start, stages)
    VALUES
        (p_run_id, p_event_type, p_pipeline_type, p_domain, p_dataset,
         p_business_date, p_status, p_record_count_source,
         p_record_count_dq_pass, p_record_count_dq_fail,
         p_record_count_published, p_kafka_topic, p_kafka_offset_end,
         p_error_summary, COALESCE(p_occurred_at, NOW()),
         p_file_id, p_s3_raw_path, p_s3_curated_path, p_file_md5,
         p_kafka_offset_start, p_stages)
    ON CONFLICT DO NOTHING
    RETURNING id INTO v_id;

    RETURN v_id;
END;
$$;

GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA pipeline TO ods;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'ods_app') THEN
        EXECUTE 'GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA pipeline TO ods_app';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'airflow_app') THEN
        EXECUTE 'GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA pipeline TO airflow_app';
    END IF;
END;
$$;

COMMIT;
