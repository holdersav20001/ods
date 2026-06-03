-- 023_dlq_lifecycle.sql — DLQ lifecycle: preserve a failed row's history and
-- make it resolvable, WITHOUT a second DLQ table.
--
-- Spec:  docs/specs/2026-06-03-working-platform-completion-plan.md
--        "DLQ Implementation For This Repository" (lines ~198-277).
-- Tests: tests/test_dlq_lifecycle.py (TDD — written failing first).
--
-- DESIGN (per spec lines 238-273):
--   The original failed payload must NEVER be overwritten. When a DLQ row is
--   fixed/replayed we record a RESOLUTION relationship and flip status; we do
--   NOT move the row to another table and lose the failure history. So we extend
--   cp.dlq in place with the smallest useful columns:
--     * status                       — lifecycle state (open -> ... -> resolved).
--     * failed_payload               — the actual rejected row, preserved.
--     * quarantine_output_link_id    — the first-class quarantine output_link the
--                                      quarantine event created (Q500 in the spec).
--     * resolved_by_run_id           — the replay/fix run that resolved it.
--     * resolved_by_output_link_id   — the corrected output the fix produced.
--
--   cp.quarantine is re-declared (012 copy SUPERSEDED — banner added there) to:
--     * accept + persist p_failed_payload jsonb (DEFAULT NULL; existing callers
--       keep working),
--     * CAPTURE the lineage_link_id that write_lineage_link returns for the
--       quarantine output and store it on the dlq row,
--     * set status = 'open'.
--   The quarantine output stays a first-class output_link (edge_type='quarantine')
--   with a real target_ref.path/content_hash/version, exactly as 012 produced it,
--   so good + quarantine both trace to the raw source via cp.v_provenance.
--
--   cp.resolve_dlq updates status + resolution refs ONLY; it never touches
--   failed_payload or reason (history preserved).
--
-- SUPERSEDES: the 012 cp.quarantine body (banner added there). 023 applies after
-- 012 so this definition wins.

-- =====================================================================
-- Extend cp.dlq in place. IF NOT EXISTS so a partial/re-run apply is safe.
-- =====================================================================
-- The lineage_link / run_log refs use ON DELETE SET NULL: if the physical
-- quarantine link or a resolution run is later PURGED, the dlq pointer clears
-- rather than blocking the purge. The failure HISTORY (failed_payload, reason)
-- is in non-referencing columns and is always preserved. This also keeps
-- existing slice-cleanup callers (which DELETE cp.lineage_link before cp.dlq)
-- working unchanged.
ALTER TABLE cp.dlq
  ADD COLUMN IF NOT EXISTS status text NOT NULL DEFAULT 'open',
  ADD COLUMN IF NOT EXISTS failed_payload jsonb,
  ADD COLUMN IF NOT EXISTS quarantine_output_link_id uuid
      REFERENCES cp.lineage_link(lineage_link_id) ON DELETE SET NULL,
  ADD COLUMN IF NOT EXISTS resolved_by_run_id uuid
      REFERENCES cp.run_log(run_id) ON DELETE SET NULL,
  ADD COLUMN IF NOT EXISTS resolved_by_output_link_id uuid
      REFERENCES cp.lineage_link(lineage_link_id) ON DELETE SET NULL;

-- Lifecycle enum (spec lines 252-260): open -> under_review -> corrected ->
-- replayed -> resolved, or rejected. Guard against typos in status writes.
ALTER TABLE cp.dlq DROP CONSTRAINT IF EXISTS dlq_status_enum;
ALTER TABLE cp.dlq ADD CONSTRAINT dlq_status_enum
  CHECK (status IN ('open','under_review','corrected','replayed','resolved','rejected'));

-- =====================================================================
-- cp.quarantine — re-declared. Adds p_failed_payload (DEFAULT NULL) as the LAST
--   param so all existing 6-arg callers keep working. Reproduces the 012 body
--   (target_ref path/content_hash/version contract + dlq_id discrimination)
--   EXCEPT: it captures the quarantine output_link id and stores failed_payload,
--   status='open', quarantine_output_link_id on the dlq row in the SAME txn.
--
--   We FIRST drop the 012 6-arg signature. A 7-arg overload with a DEFAULT 7th
--   param would otherwise sit ALONGSIDE the 6-arg function (CREATE OR REPLACE
--   only replaces a same-signature function), making a 6-arg call ambiguous
--   ('function cp.quarantine(...) is not unique'). One signature only.
--
--   *** SUPERSEDED by 030_audit_fixes.sql (F2). ***
--   This 7-arg body is dropped + re-declared in 030 with an 8th trailing
--   optional param p_source_file_id (DEFAULT NULL) that is STAMPED on the
--   quarantine edge's source_file_id, so the quarantine output traces to the raw
--   file (it formerly dead-ended: the raw id lived only in source_ref JSON).
--   030 applies last so its 8-arg definition wins. All 5/6/7-arg callers keep
--   working (the new param defaults to NULL). The "source_ref names the raw file
--   so the quarantine event is anchored to its origin" claim below is now TRUE
--   at the lineage layer, not merely in JSON.
-- =====================================================================
DROP FUNCTION IF EXISTS cp.quarantine(uuid, text, text, jsonb, text, bigint);

CREATE OR REPLACE FUNCTION cp.quarantine(
    p_run_id uuid, p_stage text, p_reason text, p_source_ref jsonb,
    p_payload_ref text, p_record_count bigint,
    p_failed_payload jsonb DEFAULT NULL
) RETURNS uuid LANGUAGE plpgsql AS $$
DECLARE v_dlq uuid; v_link uuid;
BEGIN
    INSERT INTO cp.dlq (run_id, stage, reason, source_ref, payload_ref,
                        record_count, failed_payload, status)
    VALUES (p_run_id, p_stage, p_reason, p_source_ref, p_payload_ref,
            p_record_count, p_failed_payload, 'open')
    RETURNING dlq_id INTO v_dlq;
    -- First-class quarantine output_link. content_hash discriminated by dlq_id so
    -- two quarantine events in one run with the same payload_ref do not collapse
    -- via ON CONFLICT. path falls back to 'dlq:'||dlq_id so target_ref_contract
    -- (non-empty path + content_hash + non-empty version) holds. Capture the
    -- returned link id and stamp it on the dlq row (spec quarantine_output_link_id).
    v_link := cp.write_lineage_link(
        p_run_id, 'quarantine',
        jsonb_build_object(
            'path', coalesce(nullif(p_payload_ref, ''), 'dlq:' || v_dlq::text),
            'content_hash', v_dlq::text,
            'dlq_id', v_dlq,
            'version', 1),
        p_record_count,
        jsonb_build_array(jsonb_build_object(
            'source_ref', p_source_ref, 'edge_type', 'quarantine', 'record_count', p_record_count)));
    UPDATE cp.dlq SET quarantine_output_link_id = v_link WHERE dlq_id = v_dlq;
    RETURN v_dlq;
END $$;

-- =====================================================================
-- cp.resolve_dlq — flip a DLQ row's lifecycle status and record resolution refs.
--   NEVER touches failed_payload or reason: failure history is preserved
--   (spec line 246). p_status is validated by the dlq_status_enum CHECK on UPDATE.
--   Raises if the dlq_id does not exist so callers cannot silently no-op.
--
--   *** SUPERSEDED by 029_dlq_diagnostics_fixes.sql (P2c). ***
--   This body coalesces refs but lets resolve_dlq(id,'resolved') with NO refs
--   close a DLQ untraceably. 029 re-declares cp.resolve_dlq (same signature) to
--   RAISE when a TERMINAL resolution ('resolved'/'replayed') would leave BOTH the
--   effective resolved_by_run_id and resolved_by_output_link_id null. 029 applies
--   last so its definition wins; this body is kept as-applied.
-- =====================================================================
CREATE OR REPLACE FUNCTION cp.resolve_dlq(
    p_dlq_id uuid, p_status text,
    p_resolved_by_run_id uuid DEFAULT NULL,
    p_resolved_by_output_link_id uuid DEFAULT NULL
) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    UPDATE cp.dlq
       SET status = p_status,
           resolved_by_run_id = coalesce(p_resolved_by_run_id, resolved_by_run_id),
           resolved_by_output_link_id = coalesce(p_resolved_by_output_link_id, resolved_by_output_link_id),
           replayed_at = CASE WHEN p_status IN ('replayed','resolved')
                              THEN clock_timestamp() ELSE replayed_at END,
           replay_run_id = coalesce(p_resolved_by_run_id, replay_run_id)
     WHERE dlq_id = p_dlq_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'resolve_dlq: no dlq row %', p_dlq_id;
    END IF;
END $$;
