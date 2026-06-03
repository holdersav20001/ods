-- 015_recon_selectors.sql — P10-C: exact output selection + per-output and
-- cross-hop reconciliation. Closes the remaining Codex/team findings.
-- Spec: docs/specs/2026-05-29-control-plane-design-v2.md
-- Findings: Codex P1 (R1-C4 / R2-C1), Codex P4 (R1-C5), A4-S5 (R3-C5).
--
-- THEME B — cp.run_output_link: make the PATH branch EXACT-or-RAISE and add an
--           optional content_hash disambiguator (no silent stale pick).
-- THEME E — cp.reconcile_sink_link: per-OUTPUT (per-link) sink recon, so a
--           correct fan-out (one run, K sink links) does not false-double-count.
-- THEME F — cp.reconcile_workflow: end-to-end cross-hop recon, so a wholly-failed
--           upstream that silently loses rows BREACHES.
--
-- All recon rows compute discrepancy = source_count - accounted_count and status
-- (ok/breach/double_count) consistently, satisfying the 014
-- recon_internally_consistent CHECK. Dynamic SQL uses %I (format) + bound params
-- (USING) — never string-interpolated values.
--
-- DEFERRED (NOT built here — out of scope, recorded only): the cross-row
--   invariant SUM(edges.record_count WHERE edge_type = link.edge_type) ==
--   link.record_count (R4 #9, deferred from P10-B). It needs a deferrable
--   CONSTRAINT TRIGGER evaluated at COMMIT and an annotation-edge exclusion
--   (a 'replay'-annotation edge must not count toward the sum). It remains OPEN.

-- =====================================================================
-- THEME B — cp.run_output_link: EXACT output selection.
--   New 4-arg contract (supersedes the 010 3-arg body):
--     cp.run_output_link(run, edge_type, p_target_path?, p_content_hash?)
--   * p_content_hash given -> match (run, edge_type, path?, content_hash) EXACT;
--       RAISE if none. (path is an optional extra filter here.)
--   * elif p_target_path given -> match (run, edge_type, path). If count > 1 ->
--       RAISE 'ambiguous — pass p_content_hash' (NO silent pick); RAISE if none.
--   * else (neither) -> the run's SOLE output of that edge_type; RAISE if zero,
--       RAISE if >1 (ambiguous). (Unchanged 010 F1 behaviour.)
--   DROP the 3-arg signature first: the new function adds a DEFAULTed 4th arg, so
--   a 3-arg call would be AMBIGUOUS against the old overload. No CREATE OR REPLACE
--   across differing argument lists — the old object must be dropped explicitly.
-- =====================================================================
DROP FUNCTION IF EXISTS cp.run_output_link(uuid, text, text);
CREATE OR REPLACE FUNCTION cp.run_output_link(
    p_run_id uuid, p_edge_type text,
    p_target_path text DEFAULT NULL, p_content_hash text DEFAULT NULL)
RETURNS uuid LANGUAGE plpgsql STABLE AS $$
DECLARE v_link uuid; v_n int;
BEGIN
  -- content_hash branch: EXACT (optionally further scoped by path).
  IF p_content_hash IS NOT NULL THEN
    SELECT count(*) INTO v_n FROM cp.lineage_link
     WHERE consumer_run_id=p_run_id AND edge_type=p_edge_type
       AND target_ref->>'content_hash'=p_content_hash
       AND (p_target_path IS NULL OR target_ref->>'path'=p_target_path);
    IF v_n = 0 THEN
      RAISE EXCEPTION 'run_output_link: no % output of run % at content_hash % (path %)',
        p_edge_type, p_run_id, p_content_hash, p_target_path;
    END IF;
    IF v_n > 1 THEN
      -- content_hash is part of the 5-part output-identity key, so >1 here means
      -- the (path, content_hash) pair still ties — only possible if path is NULL
      -- and the same content_hash exists at two paths. Fail loud rather than pick.
      RAISE EXCEPTION 'run_output_link: ambiguous — run % has % outputs of type % at content_hash %; pass p_target_path',
        p_run_id, v_n, p_edge_type, p_content_hash;
    END IF;
    SELECT lineage_link_id INTO v_link FROM cp.lineage_link
     WHERE consumer_run_id=p_run_id AND edge_type=p_edge_type
       AND target_ref->>'content_hash'=p_content_hash
       AND (p_target_path IS NULL OR target_ref->>'path'=p_target_path);
    RETURN v_link;
  END IF;

  -- path branch: EXACT-or-RAISE. >1 link at one path => ambiguous (the Codex P1
  -- / R2-A5b refeed dual). NO silent pick — caller must disambiguate by hash.
  IF p_target_path IS NOT NULL THEN
    SELECT count(*) INTO v_n FROM cp.lineage_link
     WHERE consumer_run_id=p_run_id AND edge_type=p_edge_type
       AND target_ref->>'path'=p_target_path;
    IF v_n = 0 THEN
      RAISE EXCEPTION 'run_output_link: no % output of run % at path %',
        p_edge_type, p_run_id, p_target_path;
    END IF;
    IF v_n > 1 THEN
      RAISE EXCEPTION 'run_output_link: ambiguous — run % has % outputs of type % at path %; pass p_content_hash',
        p_run_id, v_n, p_edge_type, p_target_path;
    END IF;
    SELECT lineage_link_id INTO v_link FROM cp.lineage_link
     WHERE consumer_run_id=p_run_id AND edge_type=p_edge_type
       AND target_ref->>'path'=p_target_path;
    RETURN v_link;
  END IF;

  -- no path / no hash: the run's sole output of that edge_type (010 F1).
  SELECT count(*) INTO v_n FROM cp.lineage_link
   WHERE consumer_run_id=p_run_id AND edge_type=p_edge_type;
  IF v_n = 0 THEN RAISE EXCEPTION 'run_output_link: no % output of run %', p_edge_type, p_run_id; END IF;
  IF v_n > 1 THEN RAISE EXCEPTION 'run_output_link: ambiguous — % has % outputs of type %; pass p_target_path', p_run_id, v_n, p_edge_type; END IF;
  SELECT lineage_link_id INTO v_link FROM cp.lineage_link
   WHERE consumer_run_id=p_run_id AND edge_type=p_edge_type;
  RETURN v_link;
END $$;

-- =====================================================================
-- THEME E — cp.reconcile_sink_link: PER-OUTPUT sink reconciliation.
--   cp.reconcile_sink is RUN-scoped: it counts ALL ods.<dataset> rows joined to
--   ANY canonical_to_sink link of the run, so a correct Decision-#6 fan-out (one
--   run, K sink links for the SAME upstream rows) double-counts -> false breach.
--   This keys recon on the LINK: accounted = count of ods.<dataset> rows whose
--   _ods_lineage_link_id = THIS link only. The link's run + dataset come from
--   run_log via consumer_run_id (same %I + bound-param pattern as reconcile_sink).
--   reconciliation_log.run_id = the link's consumer_run_id.
-- =====================================================================
CREATE OR REPLACE FUNCTION cp.reconcile_sink_link(
    p_lineage_link_id uuid, p_source_count bigint
) RETURNS void LANGUAGE plpgsql AS $$
DECLARE v_run uuid; v_dataset text; v_accounted bigint; v_disc bigint; v_status text;
BEGIN
    SELECT l.consumer_run_id, r.dataset INTO v_run, v_dataset
      FROM cp.lineage_link l JOIN cp.run_log r ON r.run_id = l.consumer_run_id
     WHERE l.lineage_link_id = p_lineage_link_id;
    IF v_run IS NULL THEN
        RAISE EXCEPTION 'reconcile_sink_link: no lineage_link/run for link %', p_lineage_link_id;
    END IF;
    IF to_regclass('ods.' || quote_ident(v_dataset)) IS NULL THEN
        RAISE EXCEPTION 'reconcile_sink_link: target table ods.% does not exist', v_dataset;
    END IF;

    -- GROUND TRUTH for THIS output: count rows stamped with this link only.
    EXECUTE format(
        'SELECT count(*) FROM ods.%I WHERE _ods_lineage_link_id = $1',
        v_dataset)
      INTO v_accounted USING p_lineage_link_id;

    v_disc := p_source_count - v_accounted;
    v_status := CASE WHEN v_disc = 0 THEN 'ok'
                     WHEN v_disc > 0 THEN 'breach'
                     ELSE 'double_count' END;

    INSERT INTO cp.reconciliation_log (run_id, check_type, source_count,
                                       accounted_count, discrepancy, status, metrics)
    VALUES (v_run, 'sink_link', p_source_count, v_accounted, v_disc, v_status,
            jsonb_build_object('derived_accounted', v_accounted,
                               'lineage_link_id', p_lineage_link_id::text,
                               'graph_derived', true));
END $$;

-- =====================================================================
-- THEME F — cp.reconcile_workflow: end-to-end CROSS-HOP reconciliation.
--   A wholly-failed upstream loses rows with NO per-run breach (each run's recon
--   is self-consistent). This compares what ENTERED the workflow to what LEFT it:
--     raw_in   = SUM(lineage_link.record_count) over links with
--                edge_type='raw_to_curated' whose consumer_run_id is a run in
--                this workflow_run_id.
--     sink_out = actual sink rows: SUM over the workflow's DISTINCT datasets of
--                count(ods.<dataset>) where _ods_lineage_link_id is a
--                canonical_to_sink link of a run in this workflow. (Iterate
--                datasets with dynamic %I — the workflow may span >1 dataset.)
--     dlq_out  = SUM(cp.dlq.record_count) for runs in this workflow.
--   accounted = sink_out + dlq_out; discrepancy = raw_in - accounted.
--   reconciliation_log.run_id = the terminal run of the workflow (latest
--   started_at); the workflow_run_id is also recorded in metrics.
--
--   *** SUPERSEDED by 030_audit_fixes.sql (F6). ***
--   030 re-declares cp.reconcile_workflow (same signature) reproducing this body
--   with three corrections: (a) terminal run ordered by finished_at DESC NULLS
--   LAST, seq DESC (the 028 deterministic tiebreak) instead of started_at/run_id;
--   (b) dlq_out counts only un-recovered loss (status NOT IN
--   ('resolved','replayed')) so a replay/resolve does not double-count;
--   (c) sink_out includes detail_to_aggregate sink outputs, not just
--   canonical_to_sink. 030 applies last so its definition wins.
-- =====================================================================
CREATE OR REPLACE FUNCTION cp.reconcile_workflow(
    p_workflow_run_id text
) RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    v_raw_in bigint; v_sink_out bigint := 0; v_dlq_out bigint;
    v_accounted bigint; v_disc bigint; v_status text;
    v_terminal_run uuid; v_ds text; v_cnt bigint;
BEGIN
    SELECT run_id INTO v_terminal_run FROM cp.run_log
     WHERE workflow_run_id = p_workflow_run_id
     ORDER BY started_at DESC, run_id DESC LIMIT 1;
    IF v_terminal_run IS NULL THEN
        RAISE EXCEPTION 'reconcile_workflow: no runs for workflow %', p_workflow_run_id;
    END IF;

    -- raw_in: rows that ENTERED — the raw_to_curated link counts for this workflow.
    SELECT coalesce(sum(l.record_count), 0) INTO v_raw_in
      FROM cp.lineage_link l JOIN cp.run_log r ON r.run_id = l.consumer_run_id
     WHERE r.workflow_run_id = p_workflow_run_id AND l.edge_type = 'raw_to_curated';

    -- sink_out: actual sink rows, summed over the workflow's distinct datasets
    -- that have at least one canonical_to_sink link (iterate with dynamic %I).
    FOR v_ds IN
        SELECT DISTINCT r.dataset
          FROM cp.lineage_link l JOIN cp.run_log r ON r.run_id = l.consumer_run_id
         WHERE r.workflow_run_id = p_workflow_run_id
           AND l.edge_type = 'canonical_to_sink'
    LOOP
        IF to_regclass('ods.' || quote_ident(v_ds)) IS NULL THEN
            RAISE EXCEPTION 'reconcile_workflow: target table ods.% does not exist', v_ds;
        END IF;
        EXECUTE format(
            'SELECT count(*) FROM ods.%I t '
            'JOIN cp.lineage_link l ON l.lineage_link_id = t._ods_lineage_link_id '
            'JOIN cp.run_log r ON r.run_id = l.consumer_run_id '
            'WHERE r.workflow_run_id = $1 AND l.edge_type = ''canonical_to_sink''',
            v_ds)
          INTO v_cnt USING p_workflow_run_id;
        v_sink_out := v_sink_out + v_cnt;
    END LOOP;

    -- dlq_out: rows legitimately quarantined (accounted for, not lost).
    SELECT coalesce(sum(d.record_count), 0) INTO v_dlq_out
      FROM cp.dlq d JOIN cp.run_log r ON r.run_id = d.run_id
     WHERE r.workflow_run_id = p_workflow_run_id;

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
                               'graph_derived', true));
END $$;
