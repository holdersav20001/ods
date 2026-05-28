-- Migration 35: clarity renames for misleading column names + state values.
--
-- Four changes that remove legacy / misleading vocabulary from the control
-- plane.  None of them touch wire protocol (Avro envelope field names stay
-- as ``_ods_run_id``).
--
--   pipeline.file_catalogue.state value 'sunk'           -> 'loaded'
--   pipeline.reconciliation_log.kafka_count              -> accounted_count
--   pipeline.run_log.record_count_published              -> record_count_target
--   pipeline.run_events.record_count_published           -> record_count_target
--   pipeline.run_log.pipeline_type value 's3_batch'      -> 'orchestration'
--
-- Why each:
--   * 'sunk' was data-engineering jargon; half the team read it as "lost".
--     'loaded' matches what target rows actually got.
--   * kafka_count was already used as a generic "accounted-for rows" counter
--     by non-Kafka routes (direct-Postgres, API-pull archive).  Rename
--     reflects the actual semantics.
--   * record_count_published implies Kafka publish; on the direct-Postgres
--     route it means rows-to-Postgres.  record_count_target is route-agnostic.
--   * pipeline_type='s3_batch' was the orchestration *wrapper* run for the
--     whole S3 file batch route, not a data pipeline.  dataset_config.source_type
--     keeps the value 's3_batch' (that field describes the *input* type — a
--     different concept).
--
-- No backward-compatibility views.  Clean break.

BEGIN;

-- ── 1. Drop functions whose plpgsql bodies / parameter lists reference the
--       renamed columns or removed state value. ───────────────────────────────
DROP FUNCTION IF EXISTS pipeline.control_update_run(
    uuid, text, bigint, bigint, bigint, bigint, text, bigint, bigint, text, jsonb
);
DROP FUNCTION IF EXISTS pipeline.control_patch_run(uuid, jsonb);
DROP FUNCTION IF EXISTS pipeline.control_write_reconciliation_check(
    text, uuid, text, text, date, bigint, bigint, bigint, text, text,
    timestamp, timestamp
);
DROP FUNCTION IF EXISTS pipeline.control_register_file(
    text, text, date, text, text, uuid, text, text, bigint, bigint, text, uuid
);
DROP FUNCTION IF EXISTS pipeline.control_update_file_catalogue(
    uuid, text, text, text, bigint, uuid
);

-- ── 2. Drop views that reference the renamed columns. ───────────────────────
DROP VIEW IF EXISTS ods.v_recon_latest_failed;
DROP VIEW IF EXISTS ods.v_recon_dlq_adjusted;

-- ── 3. Column renames. ──────────────────────────────────────────────────────
ALTER TABLE pipeline.reconciliation_log
    RENAME COLUMN kafka_count TO accounted_count;
ALTER TABLE pipeline.run_log
    RENAME COLUMN record_count_published TO record_count_target;
ALTER TABLE pipeline.run_events
    RENAME COLUMN record_count_published TO record_count_target;

-- ── 4. Value rewrites. ──────────────────────────────────────────────────────
UPDATE pipeline.file_catalogue
   SET state = 'loaded'
 WHERE state = 'sunk';

UPDATE pipeline.run_log
   SET pipeline_type = 'orchestration'
 WHERE pipeline_type = 's3_batch';

-- ── 5. Recreate views with new column names. ────────────────────────────────
CREATE VIEW ods.v_recon_latest_failed AS
SELECT DISTINCT ON (domain, dataset)
    run_id,
    check_type,
    domain,
    dataset,
    business_date,
    source_count,
    accounted_count,
    postgres_count,
    discrepancy_count,
    discrepancy_pct,
    detail,
    created_at
  FROM pipeline.reconciliation_log
 WHERE status = 'failed'
 ORDER BY domain, dataset, created_at DESC;

COMMENT ON VIEW ods.v_recon_latest_failed IS
    'Most recent failed reconciliation per (domain, dataset). Operators '
    'use this as the "what is broken right now" panel.';

CREATE VIEW ods.v_recon_dlq_adjusted AS
SELECT
    rl.run_id,
    rl.domain,
    rl.dataset,
    rl.business_date,
    rl.record_count_source,
    rl.record_count_dq_fail   AS dlq_count,
    rl.record_count_target,
    (COALESCE(rl.record_count_source, 0)
     - COALESCE(rl.record_count_dq_fail, 0)
     - COALESCE(rl.record_count_target, 0)) AS adjusted_delta,
    rl.status,
    rl.started_at,
    rl.ended_at
  FROM pipeline.run_log rl
 WHERE rl.record_count_source IS NOT NULL;

COMMENT ON VIEW ods.v_recon_dlq_adjusted IS
    'Per-run check that source - dq_fail - target == 0. Non-zero '
    'adjusted_delta means lost or duplicated records that simple T0 '
    'reconciliation may have missed.';

-- ── 6. Recreate functions with new parameter and column names. ──────────────

CREATE OR REPLACE FUNCTION pipeline.control_update_run(
    p_run_id uuid,
    p_status text DEFAULT NULL,
    p_record_count_source bigint DEFAULT NULL,
    p_record_count_dq_pass bigint DEFAULT NULL,
    p_record_count_dq_fail bigint DEFAULT NULL,
    p_record_count_target bigint DEFAULT NULL,
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
    PERFORM pipeline.control_assert_nonnegative('record_count_source', p_record_count_source);
    PERFORM pipeline.control_assert_nonnegative('record_count_dq_pass', p_record_count_dq_pass);
    PERFORM pipeline.control_assert_nonnegative('record_count_dq_fail', p_record_count_dq_fail);
    PERFORM pipeline.control_assert_nonnegative('record_count_target', p_record_count_target);
    PERFORM pipeline.control_assert_nonnegative('kafka_offset_start', p_kafka_offset_start);
    PERFORM pipeline.control_assert_nonnegative('kafka_offset_end', p_kafka_offset_end);

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
           record_count_target = COALESCE(p_record_count_target, record_count_target),
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
    v_allowed text[] := ARRAY[
        'status',
        'record_count_source',
        'record_count_dq_pass',
        'record_count_dq_fail',
        'record_count_target',
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
           record_count_target = CASE
               WHEN p_fields ? 'record_count_target'
                   THEN (p_fields->>'record_count_target')::bigint
               ELSE record_count_target
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


CREATE OR REPLACE FUNCTION pipeline.control_write_reconciliation_check(
    p_check_type text,
    p_run_id uuid,
    p_domain text,
    p_dataset text,
    p_business_date date,
    p_source_count bigint DEFAULT NULL,
    p_accounted_count bigint DEFAULT NULL,
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
    PERFORM pipeline.control_assert_nonnegative('accounted_count', p_accounted_count);
    PERFORM pipeline.control_assert_nonnegative('postgres_count', p_postgres_count);

    IF p_window_start IS NOT NULL
       AND p_window_end IS NOT NULL
       AND p_window_end < p_window_start THEN
        RAISE EXCEPTION 'window_end must be >= window_start'
            USING ERRCODE = '22023';
    END IF;

    IF p_source_count IS NOT NULL AND p_accounted_count IS NOT NULL THEN
        v_discrepancy := p_accounted_count - p_source_count;
    ELSIF p_accounted_count IS NOT NULL AND p_postgres_count IS NOT NULL THEN
        v_discrepancy := p_postgres_count - p_accounted_count;
    ELSIF p_source_count IS NOT NULL AND p_postgres_count IS NOT NULL THEN
        v_discrepancy := p_postgres_count - p_source_count;
    END IF;

    IF v_discrepancy IS NOT NULL AND COALESCE(p_source_count, 0) <> 0 THEN
        v_pct := ROUND((100.0 * v_discrepancy / p_source_count)::numeric, 4);
    END IF;

    INSERT INTO pipeline.reconciliation_log
        (check_type, run_id, domain, dataset, business_date,
         window_start, window_end, source_count, accounted_count, postgres_count,
         discrepancy_count, discrepancy_pct, status, detail)
    VALUES
        (p_check_type, p_run_id, p_domain, p_dataset, p_business_date,
         p_window_start, p_window_end, p_source_count, p_accounted_count,
         p_postgres_count, v_discrepancy, v_pct, p_status, p_detail)
    RETURNING id INTO v_id;

    RETURN v_id;
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
        ARRAY['received', 'ingesting', 'curated', 'staged', 'loaded', 'completed', 'failed']
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
        ARRAY['received', 'ingesting', 'curated', 'staged', 'loaded', 'completed', 'failed']
    );
    PERFORM pipeline.control_assert_nonnegative('source_row_count', p_source_row_count);

    UPDATE pipeline.file_catalogue
       SET state = COALESCE(p_state, state),
           state_updated_at = CASE
               WHEN p_state IS NOT NULL THEN NOW() ELSE state_updated_at
           END,
           s3_curated_path = COALESCE(p_s3_curated_path, s3_curated_path),
           source_row_count = COALESCE(p_source_row_count, source_row_count),
           last_run_id = COALESCE(p_last_run_id, last_run_id)
     WHERE (p_file_id IS NOT NULL AND file_id = p_file_id)
        OR (p_file_id IS NULL AND s3_raw_path = p_s3_raw_path)
    RETURNING file_id INTO v_file_id;

    IF v_file_id IS NULL THEN
        RAISE EXCEPTION 'file_catalogue row not found for file_id=% / s3_raw_path=%',
            p_file_id, p_s3_raw_path;
    END IF;

    RETURN v_file_id;
END;
$$;

COMMIT;
