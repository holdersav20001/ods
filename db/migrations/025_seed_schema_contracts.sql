-- 025_seed_schema_contracts.sql — seed the demo schema contract(s) for the
-- insurance/claim silver layer (area 3 + the DLQ demo workflow, step B).
--
-- Spec:  docs/specs/2026-06-03-working-platform-completion-plan.md
--        "3. Add Schema Validation Contracts" (lines ~358-424).
-- Used by: harness/policy_claims_dlq_workflow.py (canonicalization stage reads
--          this contract via control.schema.get_contract and validates the raw
--          claim rows against it; bad rows are quarantined).
--
-- DESIGN: 024 created cp.schema_contract (the storage) but seeded no rows. The
--   DLQ demo needs a CONCRETE contract for (insurance, claim, canonicalization,
--   claim.v1). This is a plain table INSERT — no new function, so the cp function
--   contract counts (ASSERTED / pg_proc) are unchanged. Idempotent via ON
--   CONFLICT on the (domain, dataset, layer, schema_version) UNIQUE key so a
--   re-apply (or apply over an already-seeded DB) does not error or duplicate.
--
--   required_columns   — every claim silver row MUST carry these (a missing one
--                        or a NULL on a non-nullable one => the row is BAD and is
--                        quarantined by control.schema.validate_rows).
--   nullable_columns   — claim columns that MAY be null. Per the claim model all
--                        five required columns are mandatory => [] (none nullable).
--   business_key       — [policy_id, claim_id] (the detail-grain identity).
--   replacement_scope  — business_key (matches the workflow's per-key visibility).
--   replacement_key_template — {policy_id}:{claim_id} (matches detail_business_key).
--
-- DOMAIN: the DLQ demo runs under its OWN domain ``insurance_dlq`` (distinct from
--   the existing insurance policy/claims demo) so each demo's DOMAIN-SCOPED reset
--   clears only its own runs and neither demo's reset can FK-violate the other's
--   committed target rows. We seed the claim contract under BOTH domains: the DLQ
--   demo reads ``insurance_dlq`` and the plain ``insurance`` row is the
--   general-purpose claim.v1 contract for the rest of the platform/tests.

INSERT INTO cp.schema_contract (
    domain, dataset, layer, schema_version,
    required_columns, nullable_columns, business_key,
    replacement_scope, replacement_key_template)
VALUES
  ('insurance', 'claim', 'canonicalization', 'claim.v1',
   '["claim_id","policy_id","claim_date","claim_status","claim_amount"]'::jsonb,
   '[]'::jsonb,
   '["policy_id","claim_id"]'::jsonb,
   'business_key', '{policy_id}:{claim_id}'),
  ('insurance_dlq', 'claim', 'canonicalization', 'claim.v1',
   '["claim_id","policy_id","claim_date","claim_status","claim_amount"]'::jsonb,
   '[]'::jsonb,
   '["policy_id","claim_id"]'::jsonb,
   'business_key', '{policy_id}:{claim_id}')
ON CONFLICT (domain, dataset, layer, schema_version) DO NOTHING;
