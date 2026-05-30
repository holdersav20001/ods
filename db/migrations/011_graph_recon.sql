-- 011_graph_recon.sql — graph-derived sink reconciliation (audit F7).
--
-- Finding: docs/reviews/2026-05-30-audit-consolidated.md (F7, MED — system-level)
-- Spec:    docs/specs/2026-05-29-control-plane-design-v2.md  (H-recon non-vacuous)
--
-- THE DEFECT: cp.write_reconciliation_check computes discrepancy from TWO
-- caller-supplied numbers (source_count, accounted_count). Every recon test
-- feeds self-consistent fakes (source == good+dlq by construction), so the layer
-- is UNFALSIFIABLE: it can never catch a sink that actually wrote FEWER rows than
-- its source claimed. A wholly-failed upstream's lost rows are reconciled by
-- nothing.
--
-- THE FIX: at the sink — the one place real rows land in ods.<dataset> — derive
-- `accounted` from the ACTUAL target rows in the database, not from a number the
-- caller passes. If rows were lost, the DB-derived count is smaller than the
-- source and recon BREACHES. The caller supplies only p_source_count (what it
-- believes the upstream produced); accounted is the ground truth from ods.<ds>.
--
-- This does NOT touch cp.write_reconciliation_check (arithmetic recon stays for
-- the non-sink hops, which have no target table to count). It adds a NEW,
-- graph-derived check (check_type='sink_graph') that is the real, falsifiable
-- end-of-pipeline reconciliation.

-- =====================================================================
-- cp.reconcile_sink — accounted = count of THIS run's committed ods.<dataset>
--   rows (those stamped with a canonical_to_sink link of this run). Fan-out
--   (two sink links of one run) is handled by scoping on consumer_run_id +
--   edge_type, so BOTH sink links' rows are counted. Dynamic SQL uses %I for the
--   table identifier and a BOUND param ($1) for the run id (mirrors
--   write_link_then_rows; no value interpolation into the SQL text).
-- =====================================================================
CREATE OR REPLACE FUNCTION cp.reconcile_sink(
    p_run_id uuid, p_source_count bigint
) RETURNS void LANGUAGE plpgsql AS $$
DECLARE v_dataset text; v_accounted bigint; v_disc bigint; v_status text;
BEGIN
    SELECT dataset INTO v_dataset FROM cp.run_log WHERE run_id = p_run_id;
    IF v_dataset IS NULL THEN
        RAISE EXCEPTION 'reconcile_sink: no run_log row for run %', p_run_id;
    END IF;
    IF to_regclass('ods.' || quote_ident(v_dataset)) IS NULL THEN
        RAISE EXCEPTION 'reconcile_sink: target table ods.% does not exist', v_dataset;
    END IF;

    -- GROUND TRUTH: count actual target rows whose lineage link is a
    -- canonical_to_sink link of THIS run. This is independent of any
    -- caller-supplied accounted number — it comes from the committed DB state.
    EXECUTE format(
        'SELECT count(*) FROM ods.%I r '
        'JOIN cp.lineage_link l ON l.lineage_link_id = r._ods_lineage_link_id '
        'WHERE l.consumer_run_id = $1 AND l.edge_type = ''canonical_to_sink''',
        v_dataset)
      INTO v_accounted USING p_run_id;

    v_disc := p_source_count - v_accounted;
    v_status := CASE WHEN v_disc = 0 THEN 'ok'
                     WHEN v_disc > 0 THEN 'breach'
                     ELSE 'double_count' END;

    INSERT INTO cp.reconciliation_log (run_id, check_type, source_count,
                                       accounted_count, discrepancy, status, metrics)
    VALUES (p_run_id, 'sink_graph', p_source_count, v_accounted, v_disc, v_status,
            jsonb_build_object('derived_accounted', v_accounted,
                               'graph_derived', true));
END $$;
