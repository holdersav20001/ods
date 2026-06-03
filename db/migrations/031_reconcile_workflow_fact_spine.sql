-- 031_reconcile_workflow_fact_spine.sql — SOUND fact-spine cross-hop recon.
--
-- Consolidated findings: docs/reviews/2026-06-03-audit-consolidated.md
--   (F6 REVISED block — fact-spine decision, FINAL).
--
-- This migration re-declares cp.reconcile_workflow. It does NOT edit any applied
-- migration 001-030; the 030 body carries a ***SUPERSEDED by 031*** banner and
-- 031 applies LAST, so THIS definition wins.
--
-- WHY (verified empirically; do not re-litigate)
--   The 030 body computed a WHOLE-WORKFLOW row conservation:
--     raw_in  = Σ record_count of ALL raw_to_curated links of the workflow,
--     sink_out = rows stamped canonical_to_sink OR detail_to_aggregate,
--     dlq_out  = Σ unresolved cp.dlq.record_count,  status ok iff raw_in == sink_out + dlq_out.
--   This is UNSOUND for any merge/aggregate pipeline: it sums the DIMENSION raw
--   input (customer/policy) into raw_in and the row-REDUCING AGGREGATE rollup
--   into sink_out, NEITHER of which lies on the row-conservation spine. It passed
--   `sales` only by arithmetic coincidence (customer 3 + transaction 6 == detail 6
--   + aggregate 3) and "breached" the insurance / insurance_dlq star schemas by
--   design.
--
--   The only UNIVERSAL cross-hop invariant is the FACT SPINE:
--     raw(FACT dataset) == leaf-detail rows + dlq_unresolved.
--   Dimensions and aggregates are OFF-spine. Aggregate integrity is already
--   verified PER-HOP by cp.reconcile_sink_link (aggregate sink rows == aggregate
--   link record_count), which the workflows already call — so the aggregate is
--   deliberately NOT folded into reconcile_workflow here.
--
-- VERIFIED fact-spine balances (the wired workflows assert these):
--   sales        : fact 'transaction' raw=6  == leaf 'customer_transaction' 6  + dlq 0  -> ok
--   insurance    : fact 'claim'       raw=4  == leaf 'policy_claim'         4  + dlq 0  -> ok
--   insurance_dlq: fact 'claim_dlq'   raw=4  == leaf 'policy_claim_dlq'     3  + dlq 1  -> ok
--                  (the headline 4 = 3 good + 1 quarantined)
--
-- =====================================================================
-- cp.reconcile_workflow — FACT-SPINE conservation.
--
--   ╔══════════════════════════════════════════════════════════════════════╗
--   ║ Re-declares cp.reconcile_workflow as a SOUND fact-spine conservation.  ║
--   ║ The 030 body (itself the 015 lineage) is SUPERSEDED; 031 applies last  ║
--   ║ so THIS definition wins.                                               ║
--   ║                                                                        ║
--   ║ SIGNATURE CHANGE (1 arg -> 3 args, the two new ones trailing+optional) ║
--   ║ -> DROP+CREATE. The new params default to NULL so EVERY existing       ║
--   ║ caller — the single-arg cp.reconcile_workflow(wf) probes in tests +    ║
--   ║ control.recon.reconcile_workflow's no-arg form — keeps working as a    ║
--   ║ single-source fallback (raw_in = ALL raw_to_curated; sink_out = ALL    ║
--   ║ canonical_to_sink leaf rows).                                          ║
--   ║                                                                        ║
--   ║ KEPT from FIX-A (030):                                                  ║
--   ║  (a) terminal run = finished_at DESC NULLS LAST, seq DESC (028 tiebreak)║
--   ║  (b) dlq_out = Σ record_count WHERE status NOT IN ('resolved',          ║
--   ║      'replayed') — a replayed/resolved loss is recovered, not summed.   ║
--   ║ REVERTED from FIX-A (030):                                              ║
--   ║  (c) sink_out counts ONLY canonical_to_sink rows — NEVER                ║
--   ║      detail_to_aggregate. The aggregate is OFF-spine (verified per-hop  ║
--   ║      by reconcile_sink_link).                                           ║
--   ║                                                                        ║
--   ║ NEW fact-scoping params:                                               ║
--   ║  p_source_datasets text[] — when non-NULL, raw_in counts ONLY           ║
--   ║      raw_to_curated links whose producing run.dataset = ANY(...)        ║
--   ║      (the FACT dataset(s)); the DIMENSION (customer/policy) is thereby  ║
--   ║      EXCLUDED from raw_in. When NULL: ALL raw_to_curated (single-source ║
--   ║      fallback).                                                         ║
--   ║  p_leaf_target text — when non-NULL, sink_out counts rows of            ║
--   ║      ods.<p_leaf_target> whose canonical_to_sink link's producing run   ║
--   ║      has dataset = p_leaf_target (the SPECIFIC leaf detail). This       ║
--   ║      EXCLUDES the daily aggregate dataset, which also carries a         ║
--   ║      canonical_to_sink sink link. When NULL: all canonical_to_sink leaf ║
--   ║      datasets (single-source fallback). RAISES if the named leaf table  ║
--   ║      is missing.                                                        ║
--   ║                                                                        ║
--   ║ Dimensions are excluded from raw_in via p_source_datasets; aggregates  ║
--   ║ are never in sink_out; refeed/replay reconcile at changed-slice grain   ║
--   ║ via cp.reconcile_sink_link, NOT here (a changed-only slice deliberately ║
--   ║ does not conserve whole-fact rows).                                    ║
--   ║                                                                        ║
--   ║ The status formula + the cp.reconciliation_log INSERT (check_type=      ║
--   ║ 'workflow') are UNCHANGED; metrics gain source_datasets, leaf_target,   ║
--   ║ and aggregates_excluded:true alongside raw_in/sink_out/dlq_out/         ║
--   ║ workflow_run_id/graph_derived.                                         ║
--   ╚══════════════════════════════════════════════════════════════════════╝
-- =====================================================================
DROP FUNCTION IF EXISTS cp.reconcile_workflow(text);

CREATE OR REPLACE FUNCTION cp.reconcile_workflow(
    p_workflow_run_id text,
    p_source_datasets text[] DEFAULT NULL,
    p_leaf_target text DEFAULT NULL
)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_raw_in bigint; v_sink_out bigint := 0; v_dlq_out bigint;
    v_accounted bigint; v_disc bigint; v_status text;
    v_terminal_run uuid; v_ds text; v_cnt bigint;
BEGIN
    -- (a) deterministic terminal run: finished_at DESC NULLS LAST, seq DESC
    --     (matches 028 discovery tiebreak; kept from FIX-A / 030).
    SELECT run_id INTO v_terminal_run FROM cp.run_log
     WHERE workflow_run_id = p_workflow_run_id
     ORDER BY finished_at DESC NULLS LAST, seq DESC LIMIT 1;
    IF v_terminal_run IS NULL THEN
        RAISE EXCEPTION 'reconcile_workflow: no runs for workflow %', p_workflow_run_id;
    END IF;

    -- raw_in: rows that ENTERED on the FACT SPINE — the raw_to_curated link
    -- counts for this workflow, RESTRICTED to the fact dataset(s) when
    -- p_source_datasets is given (so the customer/policy DIMENSION is OFF-spine
    -- and excluded). When NULL: ALL raw_to_curated (single-source fallback).
    SELECT coalesce(sum(l.record_count), 0) INTO v_raw_in
      FROM cp.lineage_link l JOIN cp.run_log r ON r.run_id = l.consumer_run_id
     WHERE r.workflow_run_id = p_workflow_run_id
       AND l.edge_type = 'raw_to_curated'
       AND (p_source_datasets IS NULL OR r.dataset = ANY(p_source_datasets));

    -- sink_out: actual LEAF-detail sink rows. ONLY canonical_to_sink — NEVER
    -- detail_to_aggregate (the aggregate is OFF-spine; verified per-hop by
    -- reconcile_sink_link). (c) reverts the unsound 030 change.
    --
    -- When p_leaf_target is given, count rows of ONLY ods.<p_leaf_target> whose
    -- canonical_to_sink link's producing run.dataset = p_leaf_target (the SPECIFIC
    -- leaf detail), which EXCLUDES the daily aggregate dataset (it also carries a
    -- canonical_to_sink sink link). When NULL, iterate every distinct dataset that
    -- has a canonical_to_sink link (single-source fallback). Same dynamic %I +
    -- to_regclass guard pattern as the 015/030 body.
    FOR v_ds IN
        SELECT DISTINCT r.dataset
          FROM cp.lineage_link l JOIN cp.run_log r ON r.run_id = l.consumer_run_id
         WHERE r.workflow_run_id = p_workflow_run_id
           AND l.edge_type = 'canonical_to_sink'
           AND (p_leaf_target IS NULL OR r.dataset = p_leaf_target)
    LOOP
        IF to_regclass('ods.' || quote_ident(v_ds)) IS NULL THEN
            RAISE EXCEPTION 'reconcile_workflow: target table ods.% does not exist', v_ds;
        END IF;
        EXECUTE format(
            'SELECT count(*) FROM ods.%I t '
            'JOIN cp.lineage_link l ON l.lineage_link_id = t._ods_lineage_link_id '
            'JOIN cp.run_log r ON r.run_id = l.consumer_run_id '
            'WHERE r.workflow_run_id = $1 '
            '  AND l.edge_type = ''canonical_to_sink'' '
            '  AND ($2 IS NULL OR r.dataset = $2)',
            v_ds)
          INTO v_cnt USING p_workflow_run_id, p_leaf_target;
        v_sink_out := v_sink_out + v_cnt;
    END LOOP;

    -- dlq_out: rows quarantined that STILL represent un-recovered loss.
    -- (b) exclude DLQ rows that were resolved/replayed — that loss has been
    --     recovered (a replay run re-emitted it) so counting it again would
    --     double-count it against raw_in. Kept from FIX-A / 030.
    SELECT coalesce(sum(d.record_count), 0) INTO v_dlq_out
      FROM cp.dlq d JOIN cp.run_log r ON r.run_id = d.run_id
     WHERE r.workflow_run_id = p_workflow_run_id
       AND d.status NOT IN ('resolved','replayed');

    v_accounted := v_sink_out + v_dlq_out;
    v_disc := v_raw_in - v_accounted;
    v_status := CASE WHEN v_disc = 0 THEN 'ok'
                     WHEN v_disc > 0 THEN 'breach'
                     ELSE 'double_count' END;

    INSERT INTO cp.reconciliation_log (run_id, check_type, source_count,
                                       accounted_count, discrepancy, status, metrics)
    VALUES (v_terminal_run, 'workflow', v_raw_in, v_accounted, v_disc, v_status,
            jsonb_build_object('raw_in', v_raw_in, 'sink_out', v_sink_out,
                               'dlq_out', v_dlq_out,
                               'workflow_run_id', p_workflow_run_id,
                               'source_datasets', to_jsonb(p_source_datasets),
                               'leaf_target', p_leaf_target,
                               'aggregates_excluded', true,
                               'graph_derived', true));
END $function$;
