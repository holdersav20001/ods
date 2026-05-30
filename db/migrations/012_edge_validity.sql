-- 012_edge_validity.sql — close two confirmed defects an external reviewer
-- (Codex) found that our own audit (A1–A4) missed.
--
-- Spec:     docs/specs/2026-05-29-control-plane-design-v2.md
-- Decision: docs/reviews/2026-05-29-lineage-link-decision.md
-- Probes:   tests/test_edge_validity.py (TDD — written failing first).
--
-- DEFECT 1 (P1b) — malformed edges create lineage leaves that trace to nowhere.
--   PROVEN: a raw_to_curated edge with NULL source_file_id, NULL
--   upstream_lineage_link_id, NULL upstream_run_id was accepted under ANY link,
--   producing a dangling leaf. The 009 CHECK only forced upstream_lineage_link_id
--   for run-to-run edge_types; nothing forced a FILE edge to name a file, nothing
--   stopped an edge's edge_type DIFFERING from its link's (smuggling), and nothing
--   required a provenance edge to anchor to SOMETHING.
--
-- DEFECT 2 (P2) — target_ref was convention; make it a contract.
--   The 010 target_ref_has_identity CHECK was too weak (path OR content_hash;
--   version never checked). Tighten to: non-empty path AND non-empty content_hash
--   AND a present 'version' key.
--
-- Fixes (each TDD'd by a flipped probe in tests/test_edge_validity.py):
--   1. raw_edge_requires_source_file  — a raw_to_curated edge must name a file.
--   2. edge_must_anchor               — a provenance edge must anchor to a file
--                                       OR an upstream output link, EXCEPT the
--                                       recognised annotation/non-trace types
--                                       (quarantine, orchestrates, replay).
--   3. cp.write_lineage_link          — re-declared (010 body verbatim incl. the
--                                       F5 changed-edge-set guard) PLUS a guard
--                                       that RAISES on edge_type smuggling, with
--                                       'replay' as the only allowed annotation.
--   4. target_ref_contract            — drop target_ref_has_identity (010), add
--                                       the path+hash+version contract.
--   5. cp.quarantine                  — re-declared to carry 'version', 1 and a
--                                       non-empty path fallback ('dlq:'||dlq_id).
--
-- SUPERSEDES: the 010 cp.write_lineage_link body (banner added there) and the
-- 002 cp.quarantine body (banner added there). 012 applies last so these win.

-- =====================================================================
-- DEFECT 1, fix 1 — a raw_to_curated edge is a FILE edge: it MUST name the raw
--   file it derived from. A raw edge with NULL source_file_id is a dangling
--   leaf with no raw anchor. (Codex P1b.)
-- =====================================================================
ALTER TABLE cp.lineage_edge ADD CONSTRAINT raw_edge_requires_source_file CHECK (
    edge_type <> 'raw_to_curated' OR source_file_id IS NOT NULL );

-- =====================================================================
-- DEFECT 1, fix 2 — every PROVENANCE edge must anchor to SOMETHING the walk can
--   follow: a file (source_file_id) OR an upstream output link
--   (upstream_lineage_link_id). The exceptions are the recognised
--   annotation/non-trace edge types:
--     * quarantine   — carries a DLQ payload ref, not a chain anchor.
--     * orchestrates — a non-provenance trigger (is_provenance=false).
--     * replay       — an annotation to the ORIGINAL run; the chain reaches raw
--                      via its sibling provenance edge regardless of this one.
--   (Codex P1b.)
-- =====================================================================
ALTER TABLE cp.lineage_edge ADD CONSTRAINT edge_must_anchor CHECK (
    edge_type IN ('quarantine','orchestrates','replay')
    OR source_file_id IS NOT NULL
    OR upstream_lineage_link_id IS NOT NULL );

-- =====================================================================
-- DEFECT 2 — target_ref CONTRACT. Drop the weak 010 identity CHECK (path OR
--   content_hash) and require BOTH non-empty PLUS a present 'version' key. A
--   link's output identity is a contract, not a convention. (Codex P2.)
--
--   *** SUPERSEDED by 014_integrity.sql (P10-B / R4 GAP 2). ***
--   This 012 target_ref_contract only required the 'version' KEY to exist
--   (`target_ref ? 'version'`), so {"version": null} and {"version": ""} both
--   passed — a null/empty version is as useless as none. 014 DROPS this
--   constraint and RE-ADDS it requiring a NON-EMPTY version too; 014 applies
--   last, so its definition wins. The DDL below is kept as-applied.
-- =====================================================================
ALTER TABLE cp.lineage_link DROP CONSTRAINT target_ref_has_identity;
ALTER TABLE cp.lineage_link ADD CONSTRAINT target_ref_contract CHECK (
    coalesce(target_ref->>'path','') <> ''
    AND coalesce(target_ref->>'content_hash','') <> ''
    AND target_ref ? 'version' );

-- =====================================================================
-- DEFECT 1, fix 3 — cp.write_lineage_link: no edge_type SMUGGLING.
--   Reproduces the 010 body VERBATIM (incl. the F5 idempotent changed-edge-set
--   guard and the F1-era at-least-one-edge / ON CONFLICT behaviour) EXCEPT it
--   adds an up-front guard: for each edge, v_edge_type = coalesce(per-edge
--   override, p_edge_type); if it differs from the link's p_edge_type it is
--   smuggling and we RAISE — UNLESS it is the one allowed annotation 'replay'
--   (a replay link legitimately carries a replay-annotation edge alongside its
--   provenance edges).
--   NOTE: the 010 copy of this function is SUPERSEDED by this one (banner added
--   to 010); 012 applies last so this definition wins.
-- =====================================================================
CREATE OR REPLACE FUNCTION cp.write_lineage_link(
    p_consumer_run_id uuid, p_edge_type text, p_target_ref jsonb, p_record_count bigint,
    p_edges jsonb, p_sink_type text DEFAULT NULL, p_transform_version text DEFAULT NULL
) RETURNS uuid LANGUAGE plpgsql AS $$
DECLARE v_link uuid; v_edge jsonb; v_incoming jsonb; v_stored jsonb; v_edge_type text;
BEGIN
    IF jsonb_array_length(coalesce(p_edges,'[]'::jsonb)) = 0 THEN
        RAISE EXCEPTION 'write_lineage_link: at least one edge required (run %, type %)', p_consumer_run_id, p_edge_type;
    END IF;
    -- P1b: no edge_type smuggling. An edge's effective edge_type must match the
    -- link's, unless it is the allowed annotation 'replay'.
    FOR v_edge IN SELECT * FROM jsonb_array_elements(p_edges) LOOP
        v_edge_type := coalesce(v_edge->>'edge_type', p_edge_type);
        IF v_edge_type <> p_edge_type AND v_edge_type <> 'replay' THEN
            RAISE EXCEPTION 'write_lineage_link: edge edge_type % does not match link edge_type % (only annotation ''replay'' may differ)', v_edge_type, p_edge_type;
        END IF;
    END LOOP;
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
-- DEFECT 2, fix 5 — cp.quarantine must COMPLY with target_ref_contract: the
--   002 body emitted target_ref WITHOUT a 'version' key and used p_payload_ref
--   directly as the path (which may be NULL). Re-declare to add 'version', 1 and
--   a non-empty path fallback ('dlq:'||dlq_id when payload_ref is NULL/empty).
--   The quarantine EDGE carries no file/upstream anchor — that is legitimate
--   (it is a DLQ payload ref) and is exempt by edge_must_anchor above; its
--   edge_type matches the link's so it is NOT smuggling. Reproduces the 002 body
--   verbatim except the target_ref construction.
--   NOTE: the 002 copy is SUPERSEDED by this one (banner added to 002).
-- =====================================================================
CREATE OR REPLACE FUNCTION cp.quarantine(
    p_run_id uuid, p_stage text, p_reason text, p_source_ref jsonb,
    p_payload_ref text, p_record_count bigint
) RETURNS uuid LANGUAGE plpgsql AS $$
DECLARE v_dlq uuid;
BEGIN
    INSERT INTO cp.dlq (run_id, stage, reason, source_ref, payload_ref, record_count)
    VALUES (p_run_id, p_stage, p_reason, p_source_ref, p_payload_ref, p_record_count)
    RETURNING dlq_id INTO v_dlq;
    -- Discriminate the link's content_hash by the unique dlq_id so two quarantine
    -- events in the same run with the same payload_ref do NOT collapse via ON CONFLICT.
    -- P2: path falls back to 'dlq:'||dlq_id when payload_ref is NULL/empty so the
    -- target_ref_contract (non-empty path) holds; 'version', 1 satisfies the
    -- present-version clause.
    PERFORM cp.write_lineage_link(
        p_run_id, 'quarantine',
        jsonb_build_object(
            'path', coalesce(nullif(p_payload_ref, ''), 'dlq:' || v_dlq::text),
            'content_hash', v_dlq::text,
            'dlq_id', v_dlq,
            'version', 1),
        p_record_count,
        jsonb_build_array(jsonb_build_object(
            'source_ref', p_source_ref, 'edge_type', 'quarantine', 'record_count', p_record_count)));
    RETURN v_dlq;
END $$;
