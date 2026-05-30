-- 014_integrity.sql — P10-B: make the DB the lineage AUTHORITY and close the
-- 13 integrity gaps the team review (R4) + an external reviewer (Codex, R3/C1)
-- found. Strict TDD: every constraint below is backed by a flipped probe in
-- tests/test_team_r4.py (CONFIRMED_GAP_* asserts ACCEPT -> now asserts REJECT)
-- and tests/test_team_r3.py (the C1 direct-insert smuggling probes).
--
-- Reviews:  docs/reviews/2026-05-30-team-r4-integrity.md     (R4, 13 gaps)
--           docs/reviews/2026-05-30-team-r3-completeness.md  (C1, smuggling bypass)
-- Probes:   tests/test_team_r4.py, tests/test_team_r3.py
--
-- This migration is purely additive at the table level (CHECK constraints +
-- one BEFORE INSERT trigger). It does NOT edit applied migrations 001-013.
-- The one exception is target_ref_contract, which was added in 012; it is
-- DROPPED here and RE-ADDED stronger (a banner was added to 012 marking the
-- 012 definition SUPERSEDED by this one — 014 applies last, so this wins).


-- =====================================================================
-- THEME C — DB-as-authority: edge-type-match TRIGGER (R3 / Codex C1).
--
--   THE DEFECT: the edge_type-vs-link_type smuggling guard lives ONLY inside
--   cp.write_lineage_link. A DIRECT INSERT INTO cp.lineage_edge can forge an
--   edge whose edge_type differs from its parent link's edge_type — e.g. a
--   raw_to_curated edge smuggled under a curated_to_canonical link — bypassing
--   the function guard entirely and making trace_row.sql over-claim a raw file
--   the canonical never derived from.
--
--   THE FIX: duplicate the guard at the TABLE level via a BEFORE INSERT trigger
--   so it holds regardless of caller. An edge's edge_type must EQUAL its parent
--   link's edge_type, with 'replay' as the single allowed annotation exception
--   (a replay link legitimately carries a replay-annotation edge alongside its
--   provenance edges — mirrors the function guard and 012 edge_must_anchor's
--   replay exclusion). The sanctioned cp.write_lineage_link path always emits
--   edges whose type = the link's type (or 'replay'), so it is unaffected.
-- =====================================================================
CREATE OR REPLACE FUNCTION cp.trg_edge_type_matches_link() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE v_link_type text;
BEGIN
    SELECT edge_type INTO v_link_type
    FROM cp.lineage_link WHERE lineage_link_id = NEW.lineage_link_id;
    IF v_link_type IS NULL THEN
        RAISE EXCEPTION 'edge references missing link %', NEW.lineage_link_id;
    END IF;
    -- edge_type must equal the parent link's, except the allowed annotation 'replay'
    IF NEW.edge_type <> v_link_type AND NEW.edge_type <> 'replay' THEN
        RAISE EXCEPTION 'edge edge_type % does not match parent link edge_type % (only annotation ''replay'' may differ)',
            NEW.edge_type, v_link_type;
    END IF;
    RETURN NEW;
END $$;

CREATE TRIGGER edge_type_matches_link
    BEFORE INSERT ON cp.lineage_edge
    FOR EACH ROW EXECUTE FUNCTION cp.trg_edge_type_matches_link();


-- =====================================================================
-- THEME D — constraint batch (R4 gaps). Each is row-level (same-row columns)
--   so they are plain CHECKs. Producers were read in this pass and confirmed
--   to emit only valid data (see notes), so none of these relax to fit a
--   malformed producer.
-- =====================================================================

-- ---- D1: non-negative counts (CONFIRMED_GAP_1a..1i) -----------------
-- A negative count silently corrupts SUM-based recon and merge totals: a single
-- -1 row can mask N missing rows in a SUM, turning a real breach into a false
-- 'ok'. NULL stays allowed where the column is nullable.
ALTER TABLE cp.lineage_link
    ADD CONSTRAINT record_count_non_negative CHECK (record_count >= 0);
ALTER TABLE cp.lineage_edge
    ADD CONSTRAINT record_count_non_negative CHECK (record_count >= 0);
ALTER TABLE cp.dlq
    ADD CONSTRAINT record_count_non_negative CHECK (record_count >= 0);
ALTER TABLE cp.run_log
    ADD CONSTRAINT record_count_in_non_negative
        CHECK (record_count_in IS NULL OR record_count_in >= 0);
ALTER TABLE cp.run_log
    ADD CONSTRAINT record_count_out_non_negative
        CHECK (record_count_out IS NULL OR record_count_out >= 0);
ALTER TABLE cp.run_stage_log
    ADD CONSTRAINT record_count_in_non_negative
        CHECK (record_count_in IS NULL OR record_count_in >= 0);
ALTER TABLE cp.run_stage_log
    ADD CONSTRAINT record_count_out_non_negative
        CHECK (record_count_out IS NULL OR record_count_out >= 0);
-- recon counts are NOT NULL; both must be >= 0 (discrepancy MAY be negative —
-- over-accounting / double_count — and is governed by D6 instead).
ALTER TABLE cp.reconciliation_log
    ADD CONSTRAINT source_count_non_negative CHECK (source_count >= 0);
ALTER TABLE cp.reconciliation_log
    ADD CONSTRAINT accounted_count_non_negative CHECK (accounted_count >= 0);

-- ---- D2: target_ref version non-empty (CONFIRMED_GAP_2) -------------
-- The 012 target_ref_contract required path + content_hash non-empty but only
-- `target_ref ? 'version'` (key EXISTS). version is meant to defeat overwrite-
-- mutability (decision #4); a null/empty version is as useless as no version.
-- Drop the 012 CHECK and re-add requiring a NON-EMPTY version too.
-- VERIFIED: every producer (harness/composers.py, harness/fakes.py,
-- control/lineage.py, and the audit/team fakes) emits "version": 1, which
-- coalesces to '1' (non-empty) — none are broken by this tightening.
ALTER TABLE cp.lineage_link DROP CONSTRAINT target_ref_contract;
ALTER TABLE cp.lineage_link ADD CONSTRAINT target_ref_contract CHECK (
    coalesce(target_ref->>'path','') <> ''
    AND coalesce(target_ref->>'content_hash','') <> ''
    AND coalesce(target_ref->>'version','') <> '' );

-- ---- D3: status enums (CONFIRMED_GAP_3a/3b/3c) ----------------------
-- A garbage status is invisible to status-based recon/orchestration filters.
-- VERIFIED by reading every status producer: run_log/run_stage_log only ever
-- carry 'running' (default/restart), 'succeeded', or 'failed'; reconciliation_log
-- only 'ok'/'breach'/'double_count' (write_reconciliation_check + reconcile_sink).
ALTER TABLE cp.run_log
    ADD CONSTRAINT status_enum CHECK (status IN ('running','succeeded','failed'));
ALTER TABLE cp.run_stage_log
    ADD CONSTRAINT status_enum CHECK (status IN ('running','succeeded','failed'));
ALTER TABLE cp.reconciliation_log
    ADD CONSTRAINT status_enum CHECK (status IN ('ok','breach','double_count'));

-- ---- D4: trigger_type enum (CONFIRMED_GAP_3d) ----------------------
-- Spec enumerates airflow|manual|replay|dlq_drain; free text breaks trigger
-- routing. VERIFIED: producers only emit these four.
ALTER TABLE cp.run_log
    ADD CONSTRAINT trigger_type_enum
        CHECK (trigger_type IN ('airflow','manual','replay','dlq_drain'));

-- ---- D5: attempt / input_slot bounds (CONFIRMED_GAP_4b/4c) ----------
-- Retry accounting is 1-based; merge slot binding is 0-based.
ALTER TABLE cp.run_stage_log
    ADD CONSTRAINT attempt_positive CHECK (attempt >= 1);
ALTER TABLE cp.lineage_edge
    ADD CONSTRAINT input_slot_non_negative CHECK (input_slot >= 0);

-- ---- D6: recon cannot LIE (headline D2; CONFIRMED_GAP_5a/5b) --------
-- Nothing tied status to discrepancy, nor discrepancy to source-accounted, so a
-- stored row could claim status='ok' with discrepancy=500 (a real breach
-- reported clean) — the recon table, the system's own truth check, could
-- self-contradict. Enforce full internal consistency so a row cannot lie:
--   * discrepancy is exactly source_count - accounted_count, and
--   * status is the deterministic function of discrepancy's sign.
-- VERIFIED: cp.write_reconciliation_check and cp.reconcile_sink both compute
-- v_disc := source - accounted and v_status := CASE 0->'ok' / >0->'breach' /
-- else 'double_count' — they comply by construction. (status & discrepancy are
-- both NOT NULL, so the boolean equalities never evaluate to NULL.)
ALTER TABLE cp.reconciliation_log
    ADD CONSTRAINT recon_internally_consistent CHECK (
        discrepancy = source_count - accounted_count
        AND (status = 'ok')           = (discrepancy = 0)
        AND (status = 'breach')       = (discrepancy > 0)
        AND (status = 'double_count') = (discrepancy < 0) );


-- =====================================================================
-- DEFERRED (NOT built here) — R4 #9 / GAP 6: the cross-row invariant
--   SUM(edges.record_count) per link == link.record_count.
--   This is a cross-ROW (and cross-table) constraint, so it cannot be a plain
--   row CHECK — it needs a deferrable CONSTRAINT TRIGGER evaluated at COMMIT.
--   It also has an annotation subtlety: a 'replay'-annotation edge must be
--   EXCLUDED from the sum. It belongs in a later pass alongside the per-output
--   reconciliation work (P10-C). Recorded here as deliberately deferred; the
--   probe test_r4_link_count_vs_edge_sum_unenforced stays asserting the gap.
-- =====================================================================


-- =====================================================================
-- PRIVILEGE / OPERATIONAL HARDENING (Theme C) — DEPLOYMENT GUIDANCE ONLY.
--   NOT APPLIED in this owner-run test DB (the `ods` role is owner/superuser
--   here, so REVOKE has no effect against it and would break the test suite).
--
--   The trigger above makes the DB authoritative for edge_type even under a
--   direct INSERT. To make the DB authoritative for ALL direct mutation of the
--   lineage tables, a deployment should additionally:
--
--     1. Run the application as a NON-OWNER, NON-SUPERUSER role (e.g. `ods_app`).
--     2. REVOKE INSERT, UPDATE, DELETE ON cp.lineage_link, cp.lineage_edge
--        (and the other cp.* state tables) FROM ods_app.
--     3. Route ALL writes through the SECURITY DEFINER cp.* functions
--        (write_lineage_link, write_link_then_rows, quarantine, etc.), each
--        with a PINNED `search_path` (e.g. `SET search_path = cp, ods, pg_temp`)
--        to prevent search-path hijack of unqualified object references.
--     4. GRANT EXECUTE on those functions to ods_app; GRANT only the SELECT the
--        app genuinely needs on the tables.
--
--   With (1)-(4) the app can only mutate lineage THROUGH the guarded functions,
--   making the DB the sole authority for every lineage write — not just for
--   edge_type. This is documented in tests/README.md as well.
-- =====================================================================
