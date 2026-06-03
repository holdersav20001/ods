-- 024_schema_contract.sql — schema-validation contract storage (area 3).
--
-- Spec:  docs/specs/2026-06-03-working-platform-completion-plan.md
--        "3. Add Schema Validation Contracts" (lines ~358-424).
-- Tests: tests/test_contract.py (round-trip + ASSERTED) and
--        tests/test_schema_contract.py (Python get_contract/validate_rows).
--
-- DESIGN: the DB STORES the contract; the VALIDATION LOGIC lives in Python
--   (control/schema.py) so the DLQ workflow (step B) can call it. A lean,
--   purpose-built table (user decision) rather than overloading dataset_config:
--   a contract is keyed by (domain, dataset, layer, schema_version) and records
--   the column/key rules + replacement policy the canonicalization step enforces.
--   The exact output's schema_version is recorded in target_ref (see spec example
--   {"path":..., "schema_version":"claim.v1"}); this table is the source of truth
--   for what that version REQUIRES.

CREATE TABLE cp.schema_contract (
    schema_contract_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    domain text NOT NULL,
    dataset text NOT NULL,
    layer text NOT NULL,                                     -- e.g. 'silver' / pipeline_type
    schema_version text NOT NULL,                            -- e.g. 'claim.v1'
    required_columns jsonb NOT NULL DEFAULT '[]'::jsonb,
    nullable_columns jsonb NOT NULL DEFAULT '[]'::jsonb,
    business_key jsonb NOT NULL DEFAULT '[]'::jsonb,         -- column list forming the business key
    replacement_scope text NOT NULL DEFAULT 'business_key',
    replacement_key_template text,                           -- e.g. '{policy_id}:{claim_id}'
    effective_from date,
    effective_to date,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (domain, dataset, layer, schema_version)
);

-- =====================================================================
-- cp.get_schema_contract — fetch the contract for (domain, dataset, layer). If
--   p_schema_version is supplied, return that exact version; otherwise return the
--   LATEST by schema_version (descending text order — versions are sortable tags
--   like 'claim.v1', 'claim.v2'). Returns the whole row so callers (and the Python
--   helper) get every contract field. Zero rows -> returns no row (NULL via SELECT).
-- =====================================================================
CREATE OR REPLACE FUNCTION cp.get_schema_contract(
    p_domain text, p_dataset text, p_layer text,
    p_schema_version text DEFAULT NULL
) RETURNS cp.schema_contract LANGUAGE sql STABLE AS $$
    SELECT *
    FROM cp.schema_contract
    WHERE domain = p_domain
      AND dataset = p_dataset
      AND layer = p_layer
      AND (p_schema_version IS NULL OR schema_version = p_schema_version)
    ORDER BY schema_version DESC
    LIMIT 1;
$$;
