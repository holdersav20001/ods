-- 026_refeed_replacement_policy.sql — formalize the refeed REPLACEMENT POLICY.
--
-- Spec:  docs/specs/2026-06-03-working-platform-completion-plan.md
--        "4. Formalize Replay/Refeed Policy".
-- Doc:   docs/reference/refeed-replacement-policy.md.
-- Tests: tests/test_refeed_policy.py.
--
-- WHY
--   Migration 016 built cp.activate_target_visibility with a SINGLE supersession
--   rule: deactivate the one prior active row for the SAME
--   (domain,dataset,business_date,sink_type,target_name,replacement_scope,
--    replacement_key) tuple, then insert the corrected Y. That is exactly the
--   changed-only, business-key-grain behaviour the policy/claims demo proves and
--   it MUST be preserved verbatim. But spec area 4 requires two more scopes the
--   016 body cannot express:
--
--     * slice       — a whole-(domain,dataset,business_date) refeed must
--                     supersede ALL currently-active rows for that slice/target,
--                     even when the original load activated MANY per-business_key
--                     Y rows (each with its own replacement_key). The 016
--                     per-key dedup only ever touches ONE prior row, so it would
--                     leave the other business_key rows active — wrong.
--     * append_only — add a new Y WITHOUT superseding any prior (both stay Y).
--                     The 016 body ALWAYS deactivates the matching prior.
--
--   This migration re-declares cp.activate_target_visibility with ONE new
--   trailing parameter (p_supersede boolean DEFAULT true) and a scope-aware
--   deactivation step. Every prior gate (link-belongs-to-run, succeeded,
--   graph-derived recon ok, idempotent retry) and the business_key/file/default
--   supersession are PRESERVED EXACTLY; the trailing DEFAULT keeps every existing
--   12-arg caller (control/visibility.activate, the two demo workflows, composers)
--   working unchanged.
--
--     ╔══════════════════════════════════════════════════════════════════╗
--     ║ The 016 copy of cp.activate_target_visibility is SUPERSEDED by this ║
--     ║ 026 body. 026 applies last so THIS definition wins. Behaviour for   ║
--     ║ business_key / file / the default single-key path is PRESERVED      ║
--     ║ VERBATIM; the ONLY additions are p_supersede (append_only) and the  ║
--     ║ slice-scope whole-slice deactivation. The recon/visibility gate     ║
--     ║ (succeeded + graph-derived recon ok) is unchanged.                  ║
--     ╚══════════════════════════════════════════════════════════════════╝
--
-- manual_approval (a 'P'/pending visibility status) is a DOCUMENTED FUTURE
-- OPTION only — NOT built here. ods.target_visibility.status remains
-- CHECK (status IN ('Y','N')); see the policy doc.

-- DROP the exact 12-arg 016 signature FIRST. Adding the 13th parameter changes
-- the signature, so CREATE OR REPLACE would otherwise leave the old 12-arg
-- overload in place and a 12-arg call would become AMBIGUOUS. Dropping the exact
-- old signature guarantees exactly one cp.activate_target_visibility remains (the
-- new 13-arg one; its trailing DEFAULT keeps every prior 12-arg caller working).
DROP FUNCTION IF EXISTS cp.activate_target_visibility(
    text, date, text, text, uuid, uuid, uuid, text, text, text, text, text)
    CASCADE;
-- Belt-and-braces: the 016 body declares this signature; drop it by the same
-- ordered parameter-type list (domain,dataset,business_date,...).
DROP FUNCTION IF EXISTS cp.activate_target_visibility(
    text, text, date, text, text, uuid, uuid, uuid, text, text, text, text)
    CASCADE;

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
    p_reason             text DEFAULT NULL,
    p_supersede          boolean DEFAULT true
) RETURNS uuid LANGUAGE plpgsql AS $$
DECLARE
    v_link_run    uuid;
    v_run_status  text;
    v_key         text;
    v_existing    uuid;
    v_prior       uuid;
    v_priors      uuid[];   -- slice-scope: ALL prior active rows for the slice
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
    --    become business-active (§8). [PRESERVED]
    SELECT status INTO v_run_status FROM cp.run_log WHERE run_id = p_producer_run_id;
    IF v_run_status IS DISTINCT FROM 'succeeded' THEN
        RAISE EXCEPTION
            'activate_target_visibility: producer run % is % (must be succeeded)',
            p_producer_run_id, coalesce(v_run_status, '<missing>');
    END IF;

    -- 4. Resolve the default replacement_key BEFORE the idempotency probe so the
    --    probe keys on the same scope/key the insert would use. [PRESERVED]
    v_key := coalesce(
        p_replacement_key,
        p_domain || '/' || p_dataset || '/' || p_business_date::text);

    -- 6 (idempotent retry, §6/§7 step 6): if an ACTIVE row already exists for this
    --    SAME lineage_link_id in this scope/key, return it — do not create a 2nd.
    --    [PRESERVED] (append_only intentionally uses a unique-per-append key, so
    --    this probe never collapses two distinct appends.)
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
    --    is row materialized). [PRESERVED VERBATIM — the recon/visibility gate.]
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

    -- ============================ DEACTIVATION ============================ --
    -- The scope decides WHICH prior active rows this activation supersedes.
    IF NOT p_supersede THEN
        -- append_only: add WITHOUT superseding anything. No prior row changes.
        -- The caller supplies a per-append-unique replacement_key so the partial
        -- unique active index (one Y per scope/key) is not violated.
        v_priors := ARRAY[]::uuid[];

    ELSIF p_replacement_scope = 'slice' THEN
        -- slice: supersede the WHOLE slice. Deactivate EVERY currently-active row
        -- for (domain,dataset,business_date,sink_type,target_name) regardless of
        -- its own replacement_scope/replacement_key (the original load may have
        -- activated MANY per-business_key Y rows). Capture them BEFORE inserting
        -- so the partial unique index is clear for the slice's new Y.
        SELECT array_agg(visibility_id) INTO v_priors
          FROM ods.target_visibility
         WHERE domain = p_domain AND dataset = p_dataset
           AND business_date = p_business_date AND sink_type = p_sink_type
           AND target_name = p_target_name
           AND status = 'Y';
        v_priors := coalesce(v_priors, ARRAY[]::uuid[]);
        IF array_length(v_priors, 1) IS NOT NULL THEN
            UPDATE ods.target_visibility
               SET status = 'N', deactivated_at = clock_timestamp()
             WHERE visibility_id = ANY(v_priors);
        END IF;

    ELSE
        -- business_key / file / the default single-key path [PRESERVED VERBATIM]:
        -- deactivate ONLY the one prior active row for the SAME scope/key.
        SELECT visibility_id INTO v_prior
          FROM ods.target_visibility
         WHERE domain = p_domain AND dataset = p_dataset
           AND business_date = p_business_date AND sink_type = p_sink_type
           AND target_name = p_target_name
           AND replacement_scope = p_replacement_scope
           AND replacement_key = v_key
           AND status = 'Y';
        IF v_prior IS NOT NULL THEN
            UPDATE ods.target_visibility
               SET status = 'N', deactivated_at = clock_timestamp()
             WHERE visibility_id = v_prior;
            v_priors := ARRAY[v_prior];
        ELSE
            v_priors := ARRAY[]::uuid[];
        END IF;
    END IF;

    -- §6 ordering: prior rows deactivated ABOVE (index cleared); now insert the
    -- new active row, THEN stamp every superseded prior's superseded_by at it.
    INSERT INTO ods.target_visibility (
        domain, dataset, business_date, sink_type, target_name,
        file_id, lineage_link_id, producer_run_id, workflow_run_id,
        replacement_scope, replacement_key, status, reason)
    VALUES (
        p_domain, p_dataset, p_business_date, p_sink_type, p_target_name,
        p_file_id, p_lineage_link_id, p_producer_run_id, p_workflow_run_id,
        p_replacement_scope, v_key, 'Y', p_reason)
    RETURNING visibility_id INTO v_new;

    IF array_length(v_priors, 1) IS NOT NULL THEN
        UPDATE ods.target_visibility
           SET superseded_by = v_new
         WHERE visibility_id = ANY(v_priors);
    END IF;

    RETURN v_new;
END $$;
