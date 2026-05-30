-- 016_target_visibility.sql — P10-D: target-visibility / active-slice layer.
--
-- Spec: docs/specs/2026-05-30-target-visibility-active-slice.md (§3-8).
--
-- PURPOSE
--   Lineage (cp.lineage_link/edge) records WHAT HAPPENED — immutable audit truth,
--   keeps both the original and every corrected/refeed output forever. Business
--   users need a different answer: WHICH OUTPUT IS ACTIVE NOW. This migration adds
--   a small target-side visibility table that marks the active output(s) for a
--   business slice, so business-facing views filter to active data WITHOUT
--   deleting lineage history or physically rewriting millions of target rows.
--
--   It also closes the changed-content / sink-restart supersession at the BUSINESS
--   layer: a corrected refeed deactivates the prior active slice row and activates
--   the corrected one (status N -> Y, superseded_by chain), while lineage stays
--   immutable.
--
-- SIMPLE-FIRST IMPLEMENTATION DECISIONS (spec §10 open decisions — defaulted):
--   * replacement_scope='slice' ONLY this pass. The file/output scope columns
--     exist (extensible) but no file/output-scope logic is built yet.
--   * default replacement_key = domain || '/' || dataset || '/' || business_date.
--   * target rows DO carry _ods_source_file_id (row-level file attribution),
--     populated only when a row maps cleanly to ONE source file (single-file
--     ingest->sink path); NULL for aggregate/merge-derived rows.
--   * the table lives in the ods schema (the local harness target schema). In
--     production the equivalent table lives next to the target data.
--
-- RECON-OK GATE INTERPRETATION (spec §7 step 3 — which recon row is authoritative
--   for a link): activate_target_visibility requires the LATEST graph-derived
--   reconciliation_log row for the link to be status='ok'. Authority order:
--     1. check_type='sink_link' scoped by metrics->>'lineage_link_id' = link
--        (the per-output, falsifiable check; P10-C / 015) — latest by recon_id.
--     2. else check_type='sink_graph' for the link's consumer_run_id (run-scoped
--        graph-derived check; F7 / 011) — latest by recon_id.
--   The ARITHMETIC check_type='sink' row is self-consistent by construction and
--   CANNOT catch real row loss, so it is NEVER the gate. If neither graph-derived
--   row exists, activation RAISES (cannot confirm a row-materialized sink).

-- =====================================================================
-- §3  ods.target_visibility — business truth: which output is active now.
-- =====================================================================
CREATE TABLE ods.target_visibility (
    visibility_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Business slice / target identity.
    domain              TEXT NOT NULL,
    dataset             TEXT NOT NULL,
    business_date       DATE NOT NULL,
    sink_type           TEXT NOT NULL,   -- postgres | oracle | kafka | s3 | ...
    target_name         TEXT NOT NULL,   -- e.g. ods.orders, ORACLE_SCHEMA.TABLE, topic

    -- Output identity.
    file_id             UUID,            -- raw/corrected file when row-level file attribution exists
    -- ON DELETE CASCADE: a visibility row BELONGS to its link/run — it cannot
    -- exist without them. In production lineage is immutable (never deleted); the
    -- cascade exists so test cleanup fixtures that DELETE a run's lineage (children
    -- first) also drop the dependent visibility rows, instead of FK-blocking. It
    -- never silently strands a visibility row pointing at a vanished link.
    lineage_link_id     UUID NOT NULL REFERENCES cp.lineage_link(lineage_link_id) ON DELETE CASCADE,
    producer_run_id     UUID NOT NULL REFERENCES cp.run_log(run_id) ON DELETE CASCADE,
    workflow_run_id     TEXT NOT NULL,

    -- Replacement grouping.
    replacement_scope   TEXT NOT NULL DEFAULT 'slice',
    replacement_key     TEXT NOT NULL,

    -- Visibility state.
    status              CHAR(1) NOT NULL CHECK (status IN ('Y', 'N')),
    activated_at        TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    deactivated_at      TIMESTAMPTZ,
    -- ON DELETE SET NULL so cascading a deleted link's visibility row does not
    -- FK-block on a sibling row whose superseded_by points at it.
    superseded_by       UUID REFERENCES ods.target_visibility(visibility_id) ON DELETE SET NULL,
    reason              TEXT,

    created_at          TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

    CONSTRAINT inactive_has_deactivated_at CHECK (
        status = 'Y' OR deactivated_at IS NOT NULL
    )
);

-- At most one active (status='Y') row per replacement scope/key (§8 invariant).
CREATE UNIQUE INDEX uq_target_visibility_active
    ON ods.target_visibility (
        domain, dataset, business_date, sink_type, target_name,
        replacement_scope, replacement_key
    )
    WHERE status = 'Y';

CREATE INDEX idx_target_visibility_file
    ON ods.target_visibility (file_id);

CREATE INDEX idx_target_visibility_link
    ON ods.target_visibility (lineage_link_id);

-- =====================================================================
-- §4  ods.orders._ods_source_file_id — row-level file attribution.
--   Populated when a row maps cleanly to ONE source file (single-file
--   ingest->sink path). NULL for aggregate/merge-derived rows (do not pretend
--   an aggregate came from one file). Nullable & additive: no existing row
--   changes, no behaviour change for write_link_then_rows callers that don't
--   stamp it.
-- =====================================================================
ALTER TABLE ods.orders
    ADD COLUMN _ods_source_file_id UUID;

-- =====================================================================
-- §4 (stamping) — cp.write_link_then_rows RE-DECLARED to optionally stamp
--   _ods_source_file_id on the target rows.
--
--   PRIOR COPY: migration 008 (008_link_then_rows_idempotent.sql). That body is
--   SUPERSEDED by this one. 016 applies last so THIS definition wins. ALL prior
--   behaviour is preserved verbatim:
--     * 008 row-idempotency retry guard (rows belong to the link; re-call does
--       not double),
--     * the no-run_log-row / missing-target-table RAISEs,
--     * the cp.write_lineage_link delegation (so 009/010/012 hardening still
--       applies, function resolution is by name at call time).
--
--   ONLY ADDITION: a trailing p_source_file_id uuid DEFAULT NULL parameter,
--   stamped into _ods_source_file_id on each inserted row. When NULL (the
--   default, and for aggregate/merge outputs) the column is left NULL — the
--   previous 5/6/7-arg callers are unaffected (default applies). The INSERT now
--   names _ods_source_file_id; it remains in the SAME transaction as the link.
--
--     ╔══════════════════════════════════════════════════════════════════╗
--     ║ The 008 copy of cp.write_link_then_rows is SUPERSEDED by this 016  ║
--     ║ body. 016 applies last so this definition wins. Behaviour preserved║
--     ║ (008 row-idempotency, 010 guards via write_lineage_link); the ONLY ║
--     ║ change is the additive p_source_file_id stamping.                  ║
--     ╚══════════════════════════════════════════════════════════════════╝
-- =====================================================================
-- DROP the prior 8-arg signature FIRST. Adding the 9th parameter changes the
-- signature, so CREATE OR REPLACE would otherwise leave the old 8-arg overload
-- in place and an 8-arg call would become AMBIGUOUS. Dropping the exact old
-- signature guarantees exactly one cp.write_link_then_rows remains (the new
-- 9-arg one; its trailing DEFAULT keeps every prior 5/6/7-arg caller working).
DROP FUNCTION IF EXISTS cp.write_link_then_rows(
    uuid, text, jsonb, bigint, jsonb, jsonb, text, text);

CREATE OR REPLACE FUNCTION cp.write_link_then_rows(
    p_consumer_run_id uuid, p_edge_type text, p_target_ref jsonb, p_record_count bigint,
    p_edges jsonb, p_rows jsonb, p_sink_type text DEFAULT NULL,
    p_transform_version text DEFAULT NULL, p_source_file_id uuid DEFAULT NULL
) RETURNS uuid LANGUAGE plpgsql AS $$
DECLARE v_link uuid; v_dataset text; v_wfid text; v_row jsonb; v_exists boolean;
        v_has_src_col boolean;
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
    -- §4 stamping is PER-TABLE best-effort: stamp _ods_source_file_id only when
    -- the target table actually carries the column. Target tables created before
    -- 016 (e.g. ods.customer_transaction in the customer-transaction harness) do
    -- NOT have it; for those the INSERT omits the column and behaves exactly as
    -- the 008 body. ods.orders (016) has it, so the single-file path stamps it.
    SELECT EXISTS (
        SELECT 1 FROM information_schema.columns
         WHERE table_schema = 'ods' AND table_name = v_dataset
           AND column_name = '_ods_source_file_id')
      INTO v_has_src_col;
    FOR v_row IN SELECT value FROM jsonb_array_elements(p_rows) LOOP
        IF v_has_src_col THEN
            EXECUTE format('INSERT INTO ods.%I (payload, _ods_workflow_run_id, _ods_lineage_link_id, _ods_source_file_id) VALUES ($1,$2,$3,$4)', v_dataset)
                USING v_row, v_wfid, v_link, p_source_file_id;
        ELSE
            EXECUTE format('INSERT INTO ods.%I (payload, _ods_workflow_run_id, _ods_lineage_link_id) VALUES ($1,$2,$3)', v_dataset)
                USING v_row, v_wfid, v_link;
        END IF;
    END LOOP;
    RETURN v_link;
END $$;

-- =====================================================================
-- §5  ods.v_orders_active — business-facing view: only ACTIVE corrected rows.
--   Joins target_visibility on the row's stamped lineage_link_id; the file_id
--   match is "match-or-null" (a slice-scope activation carries the file_id but a
--   row whose _ods_source_file_id is NULL — aggregate — still shows iff the
--   visibility row's file_id is also NULL, OR the file_ids match). Filters to the
--   sales/orders/ods.orders slice and status='Y'.
-- =====================================================================
CREATE VIEW ods.v_orders_active AS
SELECT o.*
FROM ods.orders o
JOIN ods.target_visibility tv
  ON tv.lineage_link_id = o._ods_lineage_link_id
 AND (tv.file_id IS NULL OR tv.file_id = o._ods_source_file_id)
WHERE tv.domain = 'sales'
  AND tv.dataset = 'orders'
  AND tv.target_name = 'ods.orders'
  AND tv.status = 'Y';

-- =====================================================================
-- §7  cp.activate_target_visibility — the single activation primitive.
--   Idempotent for the same p_lineage_link_id. Enforces §8 invariants:
--     * link must exist AND belong to producer_run (1),
--     * producer run must have reached 'succeeded' (2),
--     * graph-derived reconciliation for the link/run must be 'ok' (3),
--     * refeed deactivates the prior active slice row BEFORE setting its
--       superseded_by, in the SAME transaction as activating the corrected row
--       (5/6, ordering per §6: insert new -> set old.superseded_by).
-- =====================================================================
CREATE OR REPLACE FUNCTION cp.activate_target_visibility(
    p_domain             text,
    p_dataset            text,
    p_business_date      date,
    p_sink_type          text,
    p_target_name        text,
    p_file_id            uuid,
    p_lineage_link_id    uuid,
    p_producer_run_id    uuid,
    p_workflow_run_id    text,
    p_replacement_scope  text DEFAULT 'slice',
    p_replacement_key    text DEFAULT NULL,
    p_reason             text DEFAULT NULL
) RETURNS uuid LANGUAGE plpgsql AS $$
DECLARE
    v_link_run    uuid;
    v_run_status  text;
    v_key         text;
    v_existing    uuid;
    v_prior       uuid;
    v_recon_ok    text;     -- status of the authoritative graph-derived recon row
    v_recon_type  text;     -- which check it was (for the error message)
    v_new         uuid;
BEGIN
    -- 1. Verify the link exists AND belongs to the producer run.
    SELECT consumer_run_id INTO v_link_run
      FROM cp.lineage_link WHERE lineage_link_id = p_lineage_link_id;
    IF v_link_run IS NULL THEN
        RAISE EXCEPTION 'activate_target_visibility: no lineage_link %', p_lineage_link_id;
    END IF;
    IF v_link_run <> p_producer_run_id THEN
        RAISE EXCEPTION
            'activate_target_visibility: link % belongs to run %, not producer run %',
            p_lineage_link_id, v_link_run, p_producer_run_id;
    END IF;

    -- 2. Verify the producer run reached 'succeeded'. No failed/partial run may
    --    become business-active (§8).
    SELECT status INTO v_run_status FROM cp.run_log WHERE run_id = p_producer_run_id;
    IF v_run_status IS DISTINCT FROM 'succeeded' THEN
        RAISE EXCEPTION
            'activate_target_visibility: producer run % is % (must be succeeded)',
            p_producer_run_id, coalesce(v_run_status, '<missing>');
    END IF;

    -- 4. Resolve the default replacement_key BEFORE the idempotency probe so the
    --    probe keys on the same scope/key the insert would use.
    v_key := coalesce(
        p_replacement_key,
        p_domain || '/' || p_dataset || '/' || p_business_date::text);

    -- 6 (idempotent retry, §6/§7 step 6): if an ACTIVE row already exists for this
    --    SAME lineage_link_id in this scope/key, return it — do not create a 2nd.
    SELECT visibility_id INTO v_existing
      FROM ods.target_visibility
     WHERE lineage_link_id = p_lineage_link_id
       AND status = 'Y'
       AND replacement_scope = p_replacement_scope
       AND replacement_key = v_key;
    IF v_existing IS NOT NULL THEN
        RETURN v_existing;
    END IF;

    -- 3. Verify graph-derived reconciliation for this link/run is 'ok' (the sink
    --    is row materialized). Authority: latest 'sink_link' row for this link,
    --    else latest 'sink_graph' row for the run. The arithmetic 'sink' check is
    --    NEVER authoritative (self-consistent by construction). No graph row =>
    --    cannot confirm => RAISE.
    SELECT status, check_type INTO v_recon_ok, v_recon_type
      FROM cp.reconciliation_log
     WHERE check_type = 'sink_link'
       AND metrics->>'lineage_link_id' = p_lineage_link_id::text
     ORDER BY recon_id DESC
     LIMIT 1;
    IF v_recon_ok IS NULL THEN
        SELECT status, check_type INTO v_recon_ok, v_recon_type
          FROM cp.reconciliation_log
         WHERE run_id = p_producer_run_id
           AND check_type = 'sink_graph'
         ORDER BY recon_id DESC
         LIMIT 1;
    END IF;
    IF v_recon_ok IS NULL THEN
        RAISE EXCEPTION
            'activate_target_visibility: no graph-derived sink reconciliation for '
            'link % / run % — cannot confirm row-materialized sink',
            p_lineage_link_id, p_producer_run_id;
    END IF;
    IF v_recon_ok <> 'ok' THEN
        RAISE EXCEPTION
            'activate_target_visibility: reconciliation (%) for link %/run % is % '
            '(must be ok) — refusing to activate',
            v_recon_type, p_lineage_link_id, p_producer_run_id, v_recon_ok;
    END IF;

    -- Find the currently-active row for the SAME scope/key (the slice being
    -- superseded). Slice-scope: at most one (the partial unique index guarantees
    -- it). Capture it BEFORE inserting the new row.
    SELECT visibility_id INTO v_prior
      FROM ods.target_visibility
     WHERE domain = p_domain AND dataset = p_dataset
       AND business_date = p_business_date AND sink_type = p_sink_type
       AND target_name = p_target_name
       AND replacement_scope = p_replacement_scope
       AND replacement_key = v_key
       AND status = 'Y';

    -- §6 ordering: deactivate the prior active row FIRST (clears the partial
    -- unique index so the new Y row can be inserted), THEN insert the new active
    -- row, THEN stamp the prior row's superseded_by to point at the new row.
    IF v_prior IS NOT NULL THEN
        UPDATE ods.target_visibility
           SET status = 'N', deactivated_at = clock_timestamp()
         WHERE visibility_id = v_prior;
    END IF;

    INSERT INTO ods.target_visibility (
        domain, dataset, business_date, sink_type, target_name,
        file_id, lineage_link_id, producer_run_id, workflow_run_id,
        replacement_scope, replacement_key, status, reason)
    VALUES (
        p_domain, p_dataset, p_business_date, p_sink_type, p_target_name,
        p_file_id, p_lineage_link_id, p_producer_run_id, p_workflow_run_id,
        p_replacement_scope, v_key, 'Y', p_reason)
    RETURNING visibility_id INTO v_new;

    IF v_prior IS NOT NULL THEN
        UPDATE ods.target_visibility
           SET superseded_by = v_new
         WHERE visibility_id = v_prior;
    END IF;

    RETURN v_new;
END $$;
