-- 032_schema_contract_version_order.sql — fix a 4-part regression an adversarial
-- re-audit found in our own 030/031 audit-fix commits.
--
-- Consolidated findings: docs/reviews/2026-06-03-audit-consolidated.md
--   (re-audit of the 030/031 audit-fix commits).
--
-- This migration is PURELY ADDITIVE in the migration ordering sense: it
-- re-declares TWO cp.* functions. It does NOT edit any applied migration 001-031.
-- The 030 body of cp.get_schema_contract and the 031 body of
-- cp.reconcile_workflow each carry a ***SUPERSEDED by 032*** banner, and 032
-- applies LAST, so THESE definitions win.
--
-- REGRESSIONS FIXED HERE
--   #1 (HIGH) cp.get_schema_contract — the 030 "fix" (effective_from DESC NULLS
--             LAST, created_at DESC) is NOT effective-aware: a FUTURE-dated
--             contract (effective_from in the future) is returned as the
--             "latest/active" contract. Repro: v1 (eff 2020-01-01) + v2 (eff
--             2099-01-01) -> get_schema_contract(...) returned the 2099 one.
--   #2 (HIGH) cp.get_schema_contract — the 030 "fix" is NOT version-aware:
--             "latest" is decided purely by created_at (insert order). Repro:
--             register v10 then later v9 (NULL effective_from) -> returned v9. It
--             silently dropped the intended numeric-semver "latest".
--   #4 (MED)  cp.reconcile_workflow — the 031 body reports a vacuous false `ok`
--             when raw_in=0 AND sink_out=0 AND dlq_out=0 (e.g. a typo'd/empty
--             p_source_datasets, or a p_leaf_target matching no rows): 0 == 0 + 0
--             -> status 'ok'. Repro: reconcile_workflow(<wf>, ARRAY['NONEXISTENT'],
--             'customer_transaction') -> status ok, raw_in 0.

-- =====================================================================
-- Part A — cp.get_schema_contract: correct, version-aware, effective-aware
--          "latest" resolution.
--
--   ╔══════════════════════════════════════════════════════════════════════╗
--   ║ The 030 copy of cp.get_schema_contract is SUPERSEDED by this 032 body. ║
--   ║ 032 applies last so THIS definition wins. Signature is UNCHANGED       ║
--   ║ (4 args, returns cp.schema_contract, STABLE, LANGUAGE sql) ->          ║
--   ║ CREATE OR REPLACE suffices.                                            ║
--   ║                                                                        ║
--   ║ DEFECTS (re-audit #1 + #2): the 030 latest order was                   ║
--   ║   ORDER BY effective_from DESC NULLS LAST, created_at DESC             ║
--   ║ which is (#1) NOT effective-aware — a future-dated effective_from       ║
--   ║ sorts FIRST, so a contract that is not yet in force is returned as      ║
--   ║ "latest/active" — and (#2) NOT version-aware — "latest" is decided      ║
--   ║ purely by created_at (insert order), so registering v10 then later v9   ║
--   ║ (NULL effective_from) returns v9.                                       ║
--   ║                                                                        ║
--   ║ FIX (latest path, p_schema_version IS NULL):                            ║
--   ║  * EXCLUDE future-dated contracts: a row is eligible for "latest" only  ║
--   ║    when effective_from IS NULL (no stated effective date) OR            ║
--   ║    effective_from <= current_date (already in force). The exact-version ║
--   ║    path (p_schema_version given) is EXEMPT — it returns the requested   ║
--   ║    version regardless of effective date.                                ║
--   ║  * ORDER BY a real numeric VERSION VECTOR (int[]) so the highest semver  ║
--   ║    wins, THEN effective_from DESC NULLS LAST, THEN created_at DESC as a  ║
--   ║    final deterministic tiebreak. The version vector normalizes the      ║
--   ║    schema_version: every run of non-digits -> '.', trim leading/        ║
--   ║    trailing '.', empty -> '0', split on '.', cast to int[]. Postgres     ║
--   ║    compares int[] element-wise so {1,10} > {1,2} > {1} and {2} > {1,10}  ║
--   ║    (correct semver: v1.10 > v1.2; v2 > v1.10; v10 > v9). Verified in     ║
--   ║    psql against every schema_version in the repo's seeded contracts     ║
--   ║    (claim.v1 etc.) + the test versions before committing.               ║
--   ║                                                                        ║
--   ║ The exact-version WHERE clause (schema_version = p_schema_version) is    ║
--   ║ UNCHANGED, so the exact-version path still returns a specific version    ║
--   ║ even when it is future-dated. The no-match case still yields a composite ║
--   ║ row of NULLs (LIMIT 1 over an empty set -> NULL columns).               ║
--   ╚══════════════════════════════════════════════════════════════════════╝
-- =====================================================================
CREATE OR REPLACE FUNCTION cp.get_schema_contract(
    p_domain text,
    p_dataset text,
    p_layer text,
    p_schema_version text DEFAULT NULL::text
)
RETURNS cp.schema_contract
LANGUAGE sql
STABLE
AS $function$
    SELECT *
    FROM cp.schema_contract
    WHERE domain = p_domain
      AND dataset = p_dataset
      AND layer = p_layer
      AND (p_schema_version IS NULL OR schema_version = p_schema_version)
      -- #1: on the LATEST path (p_schema_version IS NULL) exclude FUTURE-dated
      -- contracts (not yet in force). The exact-version path is exempt: it must
      -- return the requested version regardless of its effective date.
      AND (p_schema_version IS NOT NULL
           OR effective_from IS NULL
           OR effective_from <= current_date)
    -- #2: "latest" = highest NUMERIC version vector (correct semver), then the
    -- most-recently-effective, then the most-recently-registered as the final
    -- deterministic tiebreak. The version vector: every run of non-digits -> '.',
    -- trim '.', empty -> '0', split, cast to int[] (element-wise compared).
    ORDER BY
        string_to_array(
            coalesce(nullif(btrim(regexp_replace(schema_version, '\D+', '.', 'g'), '.'), ''), '0'),
            '.'
        )::int[] DESC,
        effective_from DESC NULLS LAST,
        created_at DESC
    LIMIT 1;
$function$;

-- =====================================================================
-- Part B — cp.reconcile_workflow: reject a vacuous all-zero reconcile.
--
--   ╔══════════════════════════════════════════════════════════════════════╗
--   ║ The 031 copy of cp.reconcile_workflow (the FACT-SPINE body, itself the ║
--   ║ 030/015 lineage) is SUPERSEDED by this 032 body. 032 applies last so   ║
--   ║ THIS definition wins. Signature + return type UNCHANGED                ║
--   ║   (text, text[] DEFAULT NULL, text DEFAULT NULL) RETURNS void          ║
--   ║ -> CREATE OR REPLACE suffices. The 031 body is reproduced VERBATIM      ║
--   ║ with EXACTLY ONE addition:                                             ║
--   ║                                                                        ║
--   ║ DEFECT (re-audit #4): when raw_in=0 AND sink_out=0 AND dlq_out=0 the    ║
--   ║ discrepancy is 0 and the status is reported 'ok' — a VACUOUS false ok.  ║
--   ║ That happens when p_source_datasets is typo'd/empty (no raw_to_curated  ║
--   ║ link matches) or p_leaf_target matches no sink rows, i.e. the caller    ║
--   ║ asked to reconcile NOTHING and got a green light. A genuine workflow    ║
--   ║ always has >=1 fact row on the spine.                                  ║
--   ║                                                                        ║
--   ║ FIX: after computing raw_in/sink_out/dlq_out, if all three are 0 RAISE  ║
--   ║ (ERRCODE P0001) instead of inserting an 'ok' reconciliation_log row.    ║
--   ║ The status formula and the reconciliation_log INSERT for every          ║
--   ║ NON-zero case are UNCHANGED. The fact-spine tests + the 3 demos always  ║
--   ║ have >=1 fact row, so they are unaffected; test_missing_leaf_table      ║
--   ║ already RAISES earlier (the to_regclass guard).                        ║
--   ╚══════════════════════════════════════════════════════════════════════╝
-- =====================================================================
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

    -- #4: reject a VACUOUS reconcile. raw_in=0 AND sink_out=0 AND dlq_out=0 means
    -- nothing was selected on either side (a typo'd/empty p_source_datasets, or a
    -- p_leaf_target that matches no rows): 0 == 0 + 0 would otherwise report a
    -- false 'ok'. A genuine workflow always has >=1 fact row, so an all-zero
    -- reconcile is a CONFIGURATION error, not a clean balance.
    IF v_raw_in = 0 AND v_sink_out = 0 AND v_dlq_out = 0 THEN
        RAISE EXCEPTION 'reconcile_workflow: nothing to reconcile for workflow % (raw_in=0, sink_out=0) — check p_source_datasets/p_leaf_target', p_workflow_run_id
            USING ERRCODE = 'P0001';
    END IF;

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
