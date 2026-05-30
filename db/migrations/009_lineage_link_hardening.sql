-- 009_lineage_link_hardening.sql — fix two confirmed lineage defects (C1–C4).
--
-- Decision: docs/reviews/2026-05-29-lineage-link-decision.md  (Option A, hardened)
-- Spec:     docs/specs/2026-05-29-control-plane-design-v2.md   (C1 invariant)
--
-- Defect 1 (OUTPUT-side BUG): the dedup key
--   (consumer_run_id, edge_type, target_ref->>'content_hash')
-- collapses fan-out. Two sinks of the SAME canonical bytes share content_hash +
-- edge_type + consumer_run_id, so the 2nd canonical_to_sink link was silently
-- dropped by ON CONFLICT. FIX: add OUTPUT IDENTITY (sink_type + target path) to
-- the key so distinct outputs of one run cannot collapse, while same input+target
-- still dedups to one link (decision-#5 idempotency preserved).
--
-- Defect 2 (INPUT-side GAP): lineage_edge named upstreams by upstream_run_id
-- only. For run-to-run edges, if an upstream run produced multiple outputs the
-- provenance walk over-claimed ALL of them. FIX: add upstream_lineage_link_id to
-- lineage_edge (REQUIRED for run-to-run edge types) and rewrite cp.v_provenance
-- to walk link->link (link adjacency) instead of run->run.

-- =====================================================================
-- (a) INPUT-side disambiguator column + conditional NOT NULL
-- =====================================================================
ALTER TABLE cp.lineage_edge
    ADD COLUMN upstream_lineage_link_id UUID REFERENCES cp.lineage_link(lineage_link_id);

-- run-to-run edges MUST name the upstream output link; file/quarantine/replay
-- edges may leave it null (file edges are pinned by source_file_id).
ALTER TABLE cp.lineage_edge ADD CONSTRAINT upstream_link_required_for_run_edges CHECK (
    edge_type NOT IN ('curated_to_canonical','merge_to_canonical','canonical_to_sink')
    OR upstream_lineage_link_id IS NOT NULL
);

-- =====================================================================
-- (b) Hardened dedup/uniqueness key — include OUTPUT IDENTITY.
--     COALESCE so NULL parts (e.g. sink_type NULL on non-sink links) do NOT
--     make rows spuriously distinct: NULLs are DISTINCT in a unique index, which
--     would BREAK idempotency for non-sink links. COALESCE(...,'') folds them.
-- =====================================================================
DROP INDEX IF EXISTS cp.uq_lineage_link_target;
CREATE UNIQUE INDEX uq_lineage_link_target ON cp.lineage_link (
    consumer_run_id, edge_type,
    COALESCE(sink_type, ''),
    COALESCE(target_ref->>'path', ''),
    COALESCE(target_ref->>'content_hash', '')
);

-- =====================================================================
-- (c) Re-declare cp.write_lineage_link with the matching ON CONFLICT + write
--     the new upstream_lineage_link_id edge column. Reproduced verbatim from
--     002 except: ON CONFLICT inference matches the 5-part index, the re-select
--     branch uses the SAME 5-part key, and the edge insert carries
--     upstream_lineage_link_id.
--
--     ╔══════════════════════════════════════════════════════════════════╗
--     ║ SUPERSEDED by 010_audit_fixes.sql (F5).                           ║
--     ║ This 009 body has the audit defect F5: its idempotent-reuse       ║
--     ║ branch (v_link IS NULL) returns the existing link WITHOUT checking ║
--     ║ that the supplied p_edges match the stored edges — a CHANGED edge  ║
--     ║ set is silently discarded. 010 re-declares this function with an   ║
--     ║ edge-set comparison that RAISES on non-idempotent reuse. 010       ║
--     ║ applies last, so the 010 definition is the live one. This copy is  ║
--     ║ kept only for the migration history.                              ║
--     ╚══════════════════════════════════════════════════════════════════╝
-- =====================================================================
CREATE OR REPLACE FUNCTION cp.write_lineage_link(
    p_consumer_run_id uuid, p_edge_type text, p_target_ref jsonb, p_record_count bigint,
    p_edges jsonb, p_sink_type text DEFAULT NULL, p_transform_version text DEFAULT NULL
) RETURNS uuid LANGUAGE plpgsql AS $$
DECLARE v_link uuid; v_edge jsonb;
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
    IF v_link IS NULL THEN   -- idempotent replay: link already exists, reuse, edges already written
        SELECT lineage_link_id INTO v_link FROM cp.lineage_link
        WHERE consumer_run_id = p_consumer_run_id AND edge_type = p_edge_type
          AND COALESCE(sink_type, '') = COALESCE(p_sink_type, '')
          AND COALESCE(target_ref->>'path', '') = COALESCE(p_target_ref->>'path', '')
          AND COALESCE(target_ref->>'content_hash', '') = COALESCE(p_target_ref->>'content_hash', '');
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
-- (d) Output-link discovery: the link a given run produced for a given
--     edge_type — how a downstream stage names its EXACT upstream output.
-- =====================================================================
CREATE OR REPLACE FUNCTION cp.run_output_link(p_run_id uuid, p_edge_type text)
RETURNS uuid LANGUAGE sql STABLE AS $$
    SELECT lineage_link_id FROM cp.lineage_link
    WHERE consumer_run_id = p_run_id AND edge_type = p_edge_type
    ORDER BY created_at DESC, lineage_link_id DESC LIMIT 1;
$$;

-- =====================================================================
-- (e) Rewrite cp.v_provenance to walk LINK->LINK (link adjacency). This is what
--     fixes defect 2: recursion follows upstream_lineage_link_id (the exact
--     upstream output) instead of upstream_run_id -> consumer_run_id (which
--     pulled in ALL of an upstream run's outputs).
--
--     Base   = every link's provenance edges.
--     Recurse= from each edge's upstream_lineage_link_id to THAT link's edges.
--     File leaf (source_file_id) is preserved for raw_to_curated edges (those
--     carry no upstream_lineage_link_id, so recursion stops there — the raw
--     anchor). Output columns are preserved (lineage_link_id, lineage_edge_id,
--     upstream_run_id, source_file_id, edge_type, consumer_run_id) so existing
--     consumers (trace_row.sql) keep working. CYCLE key is now lineage_link_id
--     (the recursion key), so a link revisited on a path breaks the loop.
-- =====================================================================
CREATE OR REPLACE VIEW cp.v_provenance AS
WITH RECURSIVE walk AS (
    SELECT l.lineage_link_id, e.lineage_edge_id, e.upstream_run_id,
           e.upstream_lineage_link_id, e.source_file_id, e.edge_type, l.consumer_run_id
    FROM cp.lineage_link l
    JOIN cp.lineage_edge e ON e.lineage_link_id = l.lineage_link_id
    JOIN cp.edge_type t    ON t.edge_type = e.edge_type AND t.is_provenance
  UNION ALL
    SELECT pl.lineage_link_id, pe.lineage_edge_id, pe.upstream_run_id,
           pe.upstream_lineage_link_id, pe.source_file_id, pe.edge_type, pl.consumer_run_id
    FROM walk w
    JOIN cp.lineage_link pl ON pl.lineage_link_id = w.upstream_lineage_link_id
    JOIN cp.lineage_edge pe ON pe.lineage_link_id = pl.lineage_link_id
    JOIN cp.edge_type t     ON t.edge_type = pe.edge_type AND t.is_provenance
)
CYCLE lineage_link_id SET is_cycle USING path
-- Expose only the original output columns plus is_cycle (path has pseudo-type
-- record[], which Postgres refuses as a view output column). upstream_lineage_link_id
-- is an internal recursion key and is intentionally not exposed (keeps the column
-- set identical to the pre-009 view for existing consumers).
SELECT lineage_link_id, lineage_edge_id, upstream_run_id,
       source_file_id, edge_type, consumer_run_id, is_cycle
FROM walk;
