-- trace_row.sql — reconstruct the full provenance chain back to raw for a row/link.
--
-- WALK MODEL: LINK->LINK (matches cp.v_provenance since migration 009). The
--   recursion follows each edge's upstream_lineage_link_id — the EXACT upstream
--   output — NOT upstream_run_id -> consumer_run_id (run adjacency). Run
--   adjacency over-claims when a run emits multiple outputs of one edge_type
--   (the C1/defect-2 hazard); link adjacency names the precise output, so a
--   downstream link's chain never pulls a sibling output of a multi-output
--   upstream run. The raw leaf is still reached via source_file_id on the
--   raw_to_curated edge (those edges carry no upstream_lineage_link_id, so the
--   walk terminates there at the registered raw file).
--
-- USAGE
--   Given a single output id, walk cp.v_provenance (the recursive provenance
--   view, which only follows is_provenance edges) from that output all the way
--   back to the raw source file, and join each terminal edge to
--   cp.file_catalogue for the raw S3 path.
--
--   NAMING (output_link cleanup, spec 2026-05-30-output-link-input-edge-rename):
--   the %(link_id)s bind parameter is an OUTPUT-LINK id — i.e. the value a target
--   row carries as _ods_output_link_id (the new-name mirror) which equals its
--   _ods_lineage_link_id and equals cp.output_link.output_link_id. The physical
--   column is still named lineage_link_id, so the query body below is unchanged
--   and OLD callers passing {"link_id": <lineage_link_id>} keep working verbatim;
--   the bind name is kept as link_id for backward compatibility. To trace a
--   target row by the new name, pass {"link_id": row["_ods_output_link_id"]}.
--
--   python: cur.execute(open('control/queries/trace_row.sql').read(), {"link_id": link_id})
--   psql:   \set link_id '<uuid>'   then run with the param substituted, or
--           swap the %(link_id)s placeholder below for :'link_id'.
--
--   The query is parameterised by the named placeholder %(link_id)s (psycopg
--   named-parameter style). It is the single bind point for the link to trace.
--
-- RESULT (one row per provenance hop, ordered from the curated link down to raw)
--   hop            : 1-based depth of the hop (1 = the link's own edge)
--   edge_type      : provenance edge type (e.g. raw_to_curated, curated_to_canonical)
--   consumer_run_id: the run that produced this hop's output
--   upstream_run_id: the discovered upstream run (NULL at the raw anchor)
--   source_file_id : the raw file anchor (non-NULL only at the raw leaf)
--   raw_s3_path    : file_catalogue.s3_raw_path for that source_file_id (raw leaf)
--
-- GENERALISES: for the ingest hop the chain is one hop
--   (raw_to_curated -> source_file_id -> file_catalogue.s3_raw_path). As later
--   hops are added (canonical, merged, ...), v_provenance recurses through them
--   and this query returns the full ordered chain unchanged.

WITH RECURSIVE chain AS (
    -- anchor: the edges of the link we were asked to trace
    SELECT p.lineage_link_id,
           p.edge_type,
           p.consumer_run_id,
           p.upstream_run_id,
           p.source_file_id,
           1 AS hop
    FROM cp.v_provenance p
    WHERE p.lineage_link_id = %(link_id)s
  UNION ALL
    -- recurse: LINK->LINK. From this hop's link, read its edges to find each
    -- edge's upstream_lineage_link_id (the EXACT upstream output), then re-anchor
    -- on THAT link's provenance edges. v_provenance does not expose
    -- upstream_lineage_link_id (it is the view's internal recursion key), so we
    -- read it from cp.lineage_edge directly. Edges with a NULL
    -- upstream_lineage_link_id (the raw_to_curated leaf, quarantine, replay) do
    -- not recurse — the chain terminates at the raw file via source_file_id.
    SELECT p.lineage_link_id,
           p.edge_type,
           p.consumer_run_id,
           p.upstream_run_id,
           p.source_file_id,
           c.hop + 1
    FROM chain c
    JOIN cp.lineage_edge ce ON ce.lineage_link_id = c.lineage_link_id
    JOIN cp.v_provenance  p  ON p.lineage_link_id = ce.upstream_lineage_link_id
    WHERE ce.upstream_lineage_link_id IS NOT NULL
)
-- CYCLE GUARD (F3 / A4-S7a): this query runs its OWN WITH RECURSIVE, separate
-- from cp.v_provenance's guarded walk. Without a guard, a forged/malformed
-- cyclic upstream_lineage_link_id (e.g. an edge whose upstream is its own link)
-- makes this recursion never terminate and the trace-to-raw query HANGS
-- (QueryCanceled under statement_timeout). Mirror the v_provenance guard (mig
-- 006/009): track the set of lineage_link_id visited on each path; when the walk
-- would revisit a link it has already seen, mark is_cycle=true and STOP recursing
-- that branch — guaranteeing termination on a cycle while leaving a legitimate
-- (acyclic) deep chain fully traced.
CYCLE lineage_link_id SET is_cycle USING path
-- DISTINCT dedupes the fan-in case: cp.v_provenance is itself recursive, so for
-- a multi-edge link (e.g. an N-edge merge_to_canonical) the anchor already
-- surfaces both this link's edges AND the upstream edges those runs produced;
-- this query's own upstream recursion then re-walks them. Without DISTINCT the
-- same (hop, edge_type, consumer, upstream, source_file) hop is emitted once per
-- redundant path. DISTINCT on the full hop identity collapses those duplicates
-- so the reconstructed chain shows each hop exactly once (3 distinct raw paths
-- for a 3-way merge, not 3x duplicated rows).
SELECT DISTINCT c.hop,
       c.edge_type,
       c.consumer_run_id,
       c.upstream_run_id,
       c.source_file_id,
       fc.s3_raw_path AS raw_s3_path
FROM chain c
LEFT JOIN cp.file_catalogue fc ON fc.file_id = c.source_file_id
ORDER BY c.hop;
