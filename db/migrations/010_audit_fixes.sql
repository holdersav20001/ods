-- 010_audit_fixes.sql — fix six confirmed defects from the independent
-- adversarial audit (A1–A4). Findings: docs/reviews/2026-05-30-audit-consolidated.md
-- Spec: docs/specs/2026-05-29-control-plane-design-v2.md
-- Decision: docs/reviews/2026-05-29-lineage-link-decision.md
--
-- The headline (F1) is that the P5 fix RE-INTRODUCED the C1 over-claim class it
-- killed — it moved it from the write key to the discovery selector
-- (cp.run_output_link picked ONE link per (run, edge_type) via a created_at tie
-- broken by a random gen_random_uuid()). This migration removes the random pick,
-- binds merge per-slot counts to their own upstream, guards the trace recursion,
-- forbids identity-less links, rejects non-idempotent reuse, and makes files
-- per-dataset.
--
-- Fixes (each TDD'd by a flipped audit probe):
--   F1  cp.run_output_link — output-identity discovery, RAISE on ambiguity.
--   F4  cp.lineage_link target_ref_has_identity CHECK (no empty-key links).
--   F5  cp.write_lineage_link — reject reuse with a DIFFERENT edge set.
--   F6  cp.file_catalogue per-dataset key + cp.register_file 4-col ON CONFLICT.
-- (F2 merge slot binding + F3 trace_row cycle guard live in harness/ + control/.)

-- =====================================================================
-- F1 — cp.run_output_link: discover by OUTPUT IDENTITY, never random-pick.
--   Old body: ORDER BY created_at DESC, lineage_link_id DESC LIMIT 1. created_at
--   is now() (txn-stable), so a run's multiple outputs of one edge_type tie and
--   the winner was a random UUID. New contract:
--     * p_target_path given  -> the EXACT output at that path (RAISE if none).
--     * p_target_path NULL   -> the run's sole output of that edge_type; RAISE
--       if zero, RAISE if >1 (ambiguous — caller must disambiguate by path).
--   No silent pick. Fail loud. (A1-1, A3-C1, A4-S1.)
--
--   Drop the 009 two-arg signature first: the new function has a DEFAULTed third
--   arg, so a two-arg call would be AMBIGUOUS against the old overload. There is
--   no `CREATE OR REPLACE` across differing argument lists — the old function is
--   a separate object that must be dropped explicitly.
-- =====================================================================
DROP FUNCTION IF EXISTS cp.run_output_link(uuid, text);
CREATE OR REPLACE FUNCTION cp.run_output_link(
    p_run_id uuid, p_edge_type text, p_target_path text DEFAULT NULL)
RETURNS uuid LANGUAGE plpgsql STABLE AS $$
DECLARE v_link uuid; v_n int;
BEGIN
  IF p_target_path IS NOT NULL THEN
    SELECT lineage_link_id INTO v_link FROM cp.lineage_link
     WHERE consumer_run_id=p_run_id AND edge_type=p_edge_type AND target_ref->>'path'=p_target_path;
    IF v_link IS NULL THEN RAISE EXCEPTION 'run_output_link: no % output of run % at path %', p_edge_type, p_run_id, p_target_path; END IF;
    RETURN v_link;
  END IF;
  SELECT count(*) INTO v_n FROM cp.lineage_link WHERE consumer_run_id=p_run_id AND edge_type=p_edge_type;
  IF v_n = 0 THEN RAISE EXCEPTION 'run_output_link: no % output of run %', p_edge_type, p_run_id; END IF;
  IF v_n > 1 THEN RAISE EXCEPTION 'run_output_link: ambiguous — % has % outputs of type %; pass p_target_path', p_run_id, v_n, p_edge_type; END IF;
  SELECT lineage_link_id INTO v_link FROM cp.lineage_link WHERE consumer_run_id=p_run_id AND edge_type=p_edge_type;
  RETURN v_link;
END $$;

-- =====================================================================
-- F4 — a link must carry SOME output identity. The hardened 009 key COALESCEs
--   path and content_hash to ''; two outputs that BOTH lack path AND
--   content_hash collapse to the same (run, edge, '', '', '') key, silently
--   dropping the second. Forbid identity-less links outright. (A1-2.)
--   All existing fakes set at least one (verified), so this is non-breaking.
-- =====================================================================
ALTER TABLE cp.lineage_link ADD CONSTRAINT target_ref_has_identity CHECK (
    coalesce(target_ref->>'path','') <> '' OR coalesce(target_ref->>'content_hash','') <> '' );

-- =====================================================================
-- F6 — register_file is per-dataset. Decision: same content (md5) on one
--   business_date in two DIFFERENT datasets are TWO distinct files. The 3-col
--   key (file_md5, business_date) returned the FIRST dataset's file_id for the
--   second, silently mis-pinning provenance. Widen to 4 cols. (A1-3, A3-C3.)
-- =====================================================================
ALTER TABLE cp.file_catalogue DROP CONSTRAINT file_catalogue_file_md5_business_date_key;
ALTER TABLE cp.file_catalogue ADD CONSTRAINT file_catalogue_md5_date_dataset_key
    UNIQUE (file_md5, business_date, domain, dataset);

CREATE OR REPLACE FUNCTION cp.register_file(
    p_s3_raw_path text, p_file_md5 text,
    p_business_date date, p_domain text, p_dataset text
) RETURNS uuid LANGUAGE plpgsql AS $$
DECLARE v_file uuid;
BEGIN
    INSERT INTO cp.file_catalogue (s3_raw_path, file_md5, business_date, domain, dataset)
    VALUES (p_s3_raw_path, p_file_md5, p_business_date, p_domain, p_dataset)
    ON CONFLICT (file_md5, business_date, domain, dataset)
        DO UPDATE SET state = cp.file_catalogue.state  -- no-op so RETURNING fires
    RETURNING file_id INTO v_file;
    RETURN v_file;
END $$;

-- =====================================================================
-- F5 — write_lineage_link: an idempotent re-call that supplies a DIFFERENT edge
--   set must NOT silently reuse the existing link and discard the new edges.
--   Reproduces the 009 body verbatim EXCEPT the conflict/reuse branch now
--   compares the incoming edge set against the stored edges (on the
--   disambiguating fields) and RAISES on any difference. Identical re-call still
--   returns the link silently (decision-#5 idempotency). (A1-4 / probe 6.)
--   NOTE: the 009 copy of this function is SUPERSEDED by this one (banner added
--   to 009); 010 applies last so this definition wins.
-- =====================================================================
CREATE OR REPLACE FUNCTION cp.write_lineage_link(
    p_consumer_run_id uuid, p_edge_type text, p_target_ref jsonb, p_record_count bigint,
    p_edges jsonb, p_sink_type text DEFAULT NULL, p_transform_version text DEFAULT NULL
) RETURNS uuid LANGUAGE plpgsql AS $$
DECLARE v_link uuid; v_edge jsonb; v_incoming jsonb; v_stored jsonb;
BEGIN
    IF jsonb_array_length(coalesce(p_edges,'[]'::jsonb)) = 0 THEN
        RAISE EXCEPTION 'write_lineage_link: at least one edge required (run %, type %)', p_consumer_run_id, p_edge_type;
    END IF;
    INSERT INTO cp.lineage_link (consumer_run_id, edge_type, sink_type, target_ref, transform_version, record_count)
    VALUES (p_consumer_run_id, p_edge_type, p_sink_type, p_target_ref, p_transform_version, p_record_count)
    ON CONFLICT (consumer_run_id, edge_type,
                 COALESCE(sink_type, ''),
                 COALESCE((target_ref->>'path'), ''),
                 COALESCE((target_ref->>'content_hash'), '')) DO NOTHING
    RETURNING lineage_link_id INTO v_link;
    IF v_link IS NULL THEN   -- link already exists: idempotent reuse path
        SELECT lineage_link_id INTO v_link FROM cp.lineage_link
        WHERE consumer_run_id = p_consumer_run_id AND edge_type = p_edge_type
          AND COALESCE(sink_type, '') = COALESCE(p_sink_type, '')
          AND COALESCE(target_ref->>'path', '') = COALESCE(p_target_ref->>'path', '')
          AND COALESCE(target_ref->>'content_hash', '') = COALESCE(p_target_ref->>'content_hash', '');

        -- F5: the reuse is idempotent ONLY if the supplied edge set matches the
        -- stored one. Compare the disambiguating fields as an ORDER-INDEPENDENT
        -- multiset (sorted jsonb array). A mismatch (different count or content)
        -- means a non-idempotent reuse that would silently lose lineage -> RAISE.
        SELECT jsonb_agg(e ORDER BY e::text) INTO v_incoming FROM (
            SELECT jsonb_build_object(
                'upstream_run_id', nullif(elem->>'upstream_run_id',''),
                'source_file_id', nullif(elem->>'source_file_id',''),
                'input_slot', coalesce((elem->>'input_slot')::int, 0),
                'edge_type', coalesce(elem->>'edge_type', p_edge_type),
                'upstream_lineage_link_id', nullif(elem->>'upstream_lineage_link_id',''),
                'record_count', (elem->>'record_count')::bigint
            ) AS e
            FROM jsonb_array_elements(p_edges) AS elem
        ) inc;
        SELECT jsonb_agg(e ORDER BY e::text) INTO v_stored FROM (
            SELECT jsonb_build_object(
                'upstream_run_id', upstream_run_id::text,
                'source_file_id', source_file_id::text,
                'input_slot', coalesce(input_slot, 0),
                'edge_type', edge_type,
                'upstream_lineage_link_id', upstream_lineage_link_id::text,
                'record_count', record_count
            ) AS e
            FROM cp.lineage_edge WHERE lineage_link_id = v_link
        ) sto;
        IF v_incoming IS DISTINCT FROM v_stored THEN
            RAISE EXCEPTION 'write_lineage_link: link exists with a different edge set (non-idempotent reuse)';
        END IF;
        RETURN v_link;
    END IF;
    FOR v_edge IN SELECT * FROM jsonb_array_elements(p_edges) LOOP
        INSERT INTO cp.lineage_edge (lineage_link_id, upstream_run_id, upstream_lineage_link_id,
                                     source_file_id, input_slot, edge_type, source_ref, record_count)
        VALUES (v_link,
                nullif(v_edge->>'upstream_run_id','')::uuid,
                nullif(v_edge->>'upstream_lineage_link_id','')::uuid,
                nullif(v_edge->>'source_file_id','')::uuid,
                coalesce((v_edge->>'input_slot')::int, 0),
                coalesce(v_edge->>'edge_type', p_edge_type),
                v_edge->'source_ref',
                (v_edge->>'record_count')::bigint);
    END LOOP;
    RETURN v_link;
END $$;

-- =====================================================================
-- Re-declare cp.write_link_then_rows is NOT needed: it calls
-- cp.write_lineage_link internally (function resolution is by name at call
-- time), so it picks up the F5-hardened body automatically.
-- =====================================================================
