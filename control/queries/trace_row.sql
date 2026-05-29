-- trace_row.sql — reconstruct the full provenance chain back to raw for a row/link.
--
-- USAGE
--   Given a single lineage_link_id, walk cp.v_provenance (the recursive
--   provenance view, which only follows is_provenance edges) from that link
--   all the way back to the raw source file, and join each terminal edge to
--   cp.file_catalogue for the raw S3 path.
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
    -- recurse: follow upstream_run_id to the link that run produced
    SELECT p.lineage_link_id,
           p.edge_type,
           p.consumer_run_id,
           p.upstream_run_id,
           p.source_file_id,
           c.hop + 1
    FROM chain c
    JOIN cp.lineage_link ul ON ul.consumer_run_id = c.upstream_run_id
    JOIN cp.v_provenance  p  ON p.lineage_link_id = ul.lineage_link_id
    WHERE c.upstream_run_id IS NOT NULL
)
SELECT c.hop,
       c.edge_type,
       c.consumer_run_id,
       c.upstream_run_id,
       c.source_file_id,
       fc.s3_raw_path AS raw_s3_path
FROM chain c
LEFT JOIN cp.file_catalogue fc ON fc.file_id = c.source_file_id
ORDER BY c.hop;
