-- Migration 36: lineage_link as the ONLY lineage handle for file-batch routes.
--
-- Clean break.  No backward compatibility.  Historical rows in file-batch
-- target tables are TRUNCATEd.  Legacy _ods_run_id and _ods_file_id columns
-- are DROPPED.  The single column _ods_lineage_link_id (NOT NULL) replaces
-- them as the lineage handle.
--
-- pipeline.lineage_edge keeps its lineage_link_id column nullable for now
-- ONLY so that api_pull and event-driven writes (out of scope here) continue
-- to function.  A follow-up migration will fold api_pull onto the same
-- pattern and make the column NOT NULL globally.
--
-- The dag_ingest_direct_postgres DAG and the Glue jobs it submits are
-- rewritten in the same PR to:
--   * NOT pre-allocate downstream UUIDs in init_run.
--   * Each task mints its own run_id at start.
--   * Each task discovers its upstream via control tables, not via XCom.
--   * Each consumer write goes through ods_pipeline.lineage.write_link.

BEGIN;

-- ── 1. pipeline.lineage_link — the bundle / write event ──────────────────────
CREATE TABLE pipeline.lineage_link (
    lineage_link_id  UUID PRIMARY KEY,
    consumer_run_id  UUID NOT NULL REFERENCES pipeline.run_log(run_id) ON DELETE CASCADE,
    edge_type        TEXT NOT NULL,
    target_ref       TEXT,
    record_count     BIGINT,
    created_at       TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_lineage_link_consumer ON pipeline.lineage_link(consumer_run_id);
CREATE INDEX idx_lineage_link_edge_type ON pipeline.lineage_link(edge_type);

COMMENT ON TABLE pipeline.lineage_link IS
    'One row per atomic consumer write event. Target rows on file-batch '
    'routes carry _ods_lineage_link_id which dereferences to this row and '
    'via pipeline.lineage_edge.lineage_link_id reaches every contributing '
    'source.';

-- ── 2. Wire pipeline.lineage_edge to the bundle ──────────────────────────────
--    Nullable on this migration only because api_pull and event routes still
--    use bare write_edge without a link.  A future migration will make it
--    NOT NULL globally.
ALTER TABLE pipeline.lineage_edge
    ADD COLUMN lineage_link_id UUID REFERENCES pipeline.lineage_link(lineage_link_id) ON DELETE CASCADE,
    ADD COLUMN slot_name       TEXT;

-- Cascade existing pipeline.run_log child FKs so wiping a run cleans up
-- lineage_edge / run_stage_log automatically (no orphan rows after rerun
-- or test teardown).
ALTER TABLE pipeline.lineage_edge
    DROP CONSTRAINT IF EXISTS lineage_edge_child_run_id_fkey,
    ADD  CONSTRAINT lineage_edge_child_run_id_fkey
        FOREIGN KEY (consumer_run_id)
        REFERENCES pipeline.run_log(run_id) ON DELETE CASCADE;

ALTER TABLE pipeline.run_stage_log
    DROP CONSTRAINT IF EXISTS run_stage_log_run_id_fkey,
    ADD  CONSTRAINT run_stage_log_run_id_fkey
        FOREIGN KEY (run_id)
        REFERENCES pipeline.run_log(run_id) ON DELETE CASCADE;

CREATE INDEX idx_lineage_edge_link ON pipeline.lineage_edge(lineage_link_id);

COMMENT ON COLUMN pipeline.lineage_edge.lineage_link_id IS
    'FK to the lineage_link write event this edge contributed to. Nullable '
    'only for legacy api_pull / event writes; required for file-batch.';

COMMENT ON COLUMN pipeline.lineage_edge.slot_name IS
    'Optional contribution role: core, enrichment, lookup, driver. NULL for '
    'single-source edges.';

-- ── 3. Helper view: walk from a target row to all sources in one JOIN ───────
CREATE OR REPLACE VIEW pipeline.v_lineage_link_sources AS
SELECT
    ll.lineage_link_id,
    ll.consumer_run_id,
    ll.edge_type AS link_edge_type,
    ll.target_ref,
    ll.record_count AS total_records,
    le.source_file_id,
    le.upstream_run_id,
    le.source_ref,
    le.slot_name,
    le.record_count AS contribution_records
FROM pipeline.lineage_link ll
LEFT JOIN pipeline.lineage_edge le
    ON le.lineage_link_id = ll.lineage_link_id;

COMMENT ON VIEW pipeline.v_lineage_link_sources IS
    'Flattens a lineage_link write event with its contributing edges. JOIN '
    'a target row by _ods_lineage_link_id to walk back to every source.';

-- ── 4. Stored procedure for atomic link + edges write ───────────────────────
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
             lineage_link_id, slot_name)
        VALUES
            (p_consumer_run_id,
             NULLIF(v_contrib->>'upstream_run_id', '')::uuid,
             NULLIF(v_contrib->>'source_file_id', '')::uuid,
             COALESCE(v_contrib->>'edge_type', p_edge_type),
             v_contrib->>'source_ref',
             p_target_ref,
             NULLIF(v_contrib->>'record_count', '')::bigint,
             p_lineage_link_id,
             v_contrib->>'slot_name');
    END LOOP;

    RETURN p_lineage_link_id;
END;
$$;

-- ── 5. File-batch target tables: clean-slate column surgery ─────────────────
--    For every file-batch target table:
--      a) TRUNCATE existing rows (no backfill).
--      b) DROP legacy _ods_run_id and _ods_file_id columns.
--      c) ADD _ods_lineage_link_id UUID NOT NULL.
--    Index on the new column for join performance.
--
--    Tables out of scope (api_pull, event):
--      * ods.insurance_api_pull_demo
--      * ods.insurance_api_pull_risk
--      * ods.insurance_api_pull_lowlat_demo
--      They keep the legacy columns until a future migration folds them in.

DO $$
DECLARE
    t text;
BEGIN
    FOR t IN
        SELECT unnest(ARRAY[
            'ods.insurance_policy',
            'ods.insurance_policy_history',
            'ods.insurance_risk',
            'ods.insurance_file_direct_pg_append_demo',
            'ods.insurance_file_direct_pg_upsert_demo',
            'ods.insurance_file_direct_pg_risk_demo',
            'ods.policies_enriched',
            -- pipeline.slot_staging_* are UPSTREAM/staging tables, not
            -- consumer targets — they retain _ods_run_id / _ods_file_id so
            -- the merge step can join contributions back to source runs.
            'pipeline_test.dual_current',
            'pipeline_test.dual_history'
        ])
    LOOP
        IF EXISTS (
            SELECT 1 FROM pg_tables
            WHERE schemaname || '.' || tablename = t
        ) THEN
            -- a) wipe history
            EXECUTE format('TRUNCATE TABLE %s', t);

            -- b) drop legacy lineage columns + their indexes
            EXECUTE format(
                'ALTER TABLE %s '
                '   DROP COLUMN IF EXISTS _ods_run_id, '
                '   DROP COLUMN IF EXISTS _ods_file_id', t
            );

            -- The split-source case carries two run_id columns on
            -- policies_enriched. Drop those too where they exist.
            EXECUTE format(
                'ALTER TABLE %s '
                '   DROP COLUMN IF EXISTS _ods_run_id_core, '
                '   DROP COLUMN IF EXISTS _ods_run_id_enrich, '
                '   DROP COLUMN IF EXISTS _ods_merge_run_id', t
            );

            -- c) add the single lineage handle
            EXECUTE format(
                'ALTER TABLE %s '
                '   ADD COLUMN _ods_lineage_link_id UUID NOT NULL', t
            );

            EXECUTE format(
                'CREATE INDEX idx_%s_lineage_link ON %s(_ods_lineage_link_id)',
                replace(t, '.', '_'), t
            );
        END IF;
    END LOOP;
END $$;

-- ── 6. Drop the deprecated multi-source orchestration tables ────────────────
--    pipeline.merge_run_log and pipeline.merge_contribution_log were the
--    old workaround for multi-source target rows. lineage_link replaces
--    them. Wipe and drop.
DROP TABLE IF EXISTS pipeline.merge_contribution_log;
DROP TABLE IF EXISTS pipeline.merge_run_log;

-- ── 7. Update ods_pipeline.metadata FILE_RECORD_FIELDS contract ─────────────
--    Documented here for traceability; the actual Python constant lives in
--    ods_pipeline/metadata.py and is updated in the same PR.  After this
--    migration the only ODS metadata column on file-batch target rows is
--    _ods_lineage_link_id, plus any business-date / domain helpers the
--    target table chooses to keep (e.g. _ods_business_date) which are NOT
--    lineage-bearing.

COMMIT;
