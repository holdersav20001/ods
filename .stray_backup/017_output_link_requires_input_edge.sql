-- 017_output_link_requires_input_edge.sql
--
-- Enforce the core lineage invariant at the database boundary:
--
--   every produced output (cp.lineage_link) must have at least one input
--   relationship (cp.lineage_edge).
--
-- cp.write_lineage_link already enforces this for the sanctioned function path,
-- but a direct INSERT into cp.lineage_link could still create a zero-edge output
-- that cannot trace to raw. This constraint trigger closes that bypass.
--
-- It is DEFERRABLE INITIALLY DEFERRED because the valid write pattern inserts the
-- link first and the edge rows immediately after, in the same transaction. The
-- check runs at commit / SET CONSTRAINTS time, after the edges have had a chance
-- to be inserted.

CREATE OR REPLACE FUNCTION cp.trg_lineage_link_has_edge() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM cp.lineage_edge e
        WHERE e.lineage_link_id = NEW.lineage_link_id
    ) THEN
        RAISE EXCEPTION
            'lineage_link % has no lineage_edge rows; every output must declare at least one input',
            NEW.lineage_link_id;
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS lineage_link_has_edge ON cp.lineage_link;

CREATE CONSTRAINT TRIGGER lineage_link_has_edge
    AFTER INSERT OR UPDATE ON cp.lineage_link
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION cp.trg_lineage_link_has_edge();

