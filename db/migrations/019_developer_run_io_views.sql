-- 019_developer_run_io_views.sql
--
-- Developer-facing read views for the output_link / input_edge model.
--
-- PURPOSE
--   The normalized model is correct, but a developer usually starts from a
--   process/run and wants a practical answer:
--
--     1. What did this run write?
--     2. What did it read?
--     3. Where are those things physically?
--
--   These views keep the normalized write model unchanged:
--     * cp.output_link = one produced output.
--     * cp.input_edge  = one input relationship used to produce that output.
--     * cp.file_catalogue = raw file identities.
--
--   They expose a friendlier read shape where the actual consumed input is
--   called input_reference_id. The input_edge_id remains available as the audit
--   row key for the relationship, but it is not presented as "the input".
--
-- QUICK QUERIES
--   -- Everything one run wrote and read:
--   SELECT *
--   FROM cp.v_run_io
--   WHERE run_id = '<run_id>'::uuid
--   ORDER BY output_created_at, input_slot NULLS LAST;
--
--   -- Explain one output and its inputs:
--   SELECT *
--   FROM cp.v_run_io
--   WHERE output_link_id = '<output_link_id>'::uuid
--   ORDER BY input_slot NULLS LAST;
--
--   -- Trace the input relationship row that confused us:
--   SELECT *
--   FROM cp.v_run_io
--   WHERE input_edge_id::text LIKE 'cb521f6e%';

-- =====================================================================
-- cp.v_run_outputs
--   One row per output produced by a run.
-- =====================================================================
CREATE OR REPLACE VIEW cp.v_run_outputs AS
SELECT
    r.run_id,
    r.workflow_run_id,
    r.pipeline_type,
    r.domain,
    r.dataset,
    r.business_date,
    r.status AS run_status,
    r.record_count_in AS run_record_count_in,
    r.record_count_out AS run_record_count_out,

    ol.output_link_id,
    ol.edge_type AS output_edge_type,
    ol.sink_type AS output_sink_type,
    ol.target_ref AS output_ref,
    ol.target_ref->>'path' AS output_path,
    ol.target_ref->>'content_hash' AS output_content_hash,
    NULLIF(ol.target_ref->>'version', '')::integer AS output_version,
    ol.target_ref->>'kind' AS output_kind,
    ol.target_ref->>'layer' AS output_layer,
    ol.target_ref->>'schema' AS output_schema,
    ol.target_ref->>'table' AS output_table,
    ol.target_ref->>'format' AS output_format,
    ol.transform_version,
    ol.record_count AS output_record_count,
    ol.created_at AS output_created_at
FROM cp.output_link ol
JOIN cp.run_log r
  ON r.run_id = ol.consumer_run_id;

COMMENT ON VIEW cp.v_run_outputs IS
    'Developer read view: one row per produced output_link, joined to its producing run.';

-- =====================================================================
-- cp.v_run_inputs
--   One row per input_edge used by a run output.
--
--   input_reference_type / input_reference_id are the developer-friendly
--   columns:
--     * raw_file    -> input_reference_id = cp.file_catalogue.file_id
--     * output_link -> input_reference_id = upstream cp.output_link.output_link_id
--
--   input_edge_id remains the row key for the relationship itself.
-- =====================================================================
CREATE OR REPLACE VIEW cp.v_run_inputs AS
SELECT
    r.run_id,
    r.workflow_run_id,
    r.pipeline_type,
    r.domain,
    r.dataset,
    r.business_date,
    r.status AS run_status,

    ie.input_edge_id,
    ie.output_link_id,
    ie.input_slot,
    ie.edge_type AS input_edge_type,
    ie.record_count AS input_record_count,
    ie.source_ref AS input_source_ref,

    CASE
        WHEN ie.source_file_id IS NOT NULL THEN 'raw_file'
        WHEN ie.upstream_output_link_id IS NOT NULL THEN 'output_link'
        ELSE 'unknown'
    END AS input_reference_type,
    COALESCE(
        ie.source_file_id::text,
        ie.upstream_output_link_id::text
    ) AS input_reference_id,

    ie.source_file_id,
    ie.upstream_output_link_id,
    ie.upstream_run_id,

    CASE
        WHEN ie.source_file_id IS NOT NULL THEN f.s3_raw_path
        ELSE upstream_ol.target_ref->>'path'
    END AS input_path,
    CASE
        WHEN ie.source_file_id IS NOT NULL THEN f.file_md5
        ELSE upstream_ol.target_ref->>'content_hash'
    END AS input_content_hash,
    CASE
        WHEN ie.source_file_id IS NOT NULL THEN f.dataset
        ELSE upstream_run.dataset
    END AS input_dataset,
    CASE
        WHEN ie.source_file_id IS NOT NULL THEN f.domain
        ELSE upstream_run.domain
    END AS input_domain,
    CASE
        WHEN ie.source_file_id IS NOT NULL THEN f.business_date
        ELSE upstream_run.business_date
    END AS input_business_date,
    upstream_run.pipeline_type AS upstream_pipeline_type,
    upstream_ol.edge_type AS upstream_output_edge_type,

    COALESCE(
        ie.source_ref->>'input_role',
        ie.source_ref->>'role',
        ie.source_ref->>'dataset',
        ie.source_ref->>'layer',
        CASE WHEN ie.input_slot IS NOT NULL THEN 'slot_' || ie.input_slot::text END
    ) AS input_role
FROM cp.input_edge ie
JOIN cp.output_link ol
  ON ol.output_link_id = ie.output_link_id
JOIN cp.run_log r
  ON r.run_id = ol.consumer_run_id
LEFT JOIN cp.file_catalogue f
  ON f.file_id = ie.source_file_id
LEFT JOIN cp.output_link upstream_ol
  ON upstream_ol.output_link_id = ie.upstream_output_link_id
LEFT JOIN cp.run_log upstream_run
  ON upstream_run.run_id = upstream_ol.consumer_run_id;

COMMENT ON VIEW cp.v_run_inputs IS
    'Developer read view: one row per input_edge with input_reference_type/input_reference_id naming the actual consumed input.';

-- =====================================================================
-- cp.v_run_io
--   One row per produced output + consumed input relationship.
--
--   This is the view a developer should usually start with:
--     SELECT * FROM cp.v_run_io WHERE run_id = '<process run id>';
-- =====================================================================
CREATE OR REPLACE VIEW cp.v_run_io AS
SELECT
    o.run_id,
    o.workflow_run_id,
    o.pipeline_type,
    o.domain,
    o.dataset,
    o.business_date,
    o.run_status,

    o.output_link_id,
    o.output_edge_type,
    o.output_sink_type,
    o.output_kind,
    o.output_layer,
    o.output_schema,
    o.output_table,
    o.output_format,
    o.output_path,
    o.output_content_hash,
    o.output_version,
    o.output_record_count,
    o.transform_version,
    o.output_created_at,

    i.input_edge_id,
    i.input_slot,
    i.input_edge_type,
    i.input_reference_type,
    i.input_reference_id,
    i.input_role,
    i.input_path,
    i.input_content_hash,
    i.input_domain,
    i.input_dataset,
    i.input_business_date,
    i.upstream_run_id,
    i.upstream_output_link_id,
    i.upstream_pipeline_type,
    i.upstream_output_edge_type,
    i.source_file_id,
    i.input_record_count,
    i.input_source_ref
FROM cp.v_run_outputs o
LEFT JOIN cp.v_run_inputs i
  ON i.output_link_id = o.output_link_id;

COMMENT ON VIEW cp.v_run_io IS
    'Developer read view: for each run output, show the inputs it used and their physical locations.';
