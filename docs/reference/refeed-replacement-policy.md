# Refeed / Replay Replacement Policy

When a corrected file is re-ingested (a **refeed** / **replay**), the platform
must decide *which* prior business-active output(s) the corrected output replaces.
That decision is the **replacement policy**, expressed as a
`replacement_scope` (+ a `replacement_key` that identifies the unit being
replaced).

This policy operates on the **business-truth** layer (`ods.target_visibility` —
"which output is active now"), NOT on lineage (`cp.lineage_link/edge`, which is
immutable audit truth and keeps the original *and* every corrected output
forever). A refeed deactivates the prior active visibility row(s) (status
`Y -> N`, `superseded_by` chain) and activates the corrected one; lineage is
never rewritten.

The active scope/key for a dataset is declared on its contract:
`cp.schema_contract.replacement_scope` and
`cp.schema_contract.replacement_key_template` (migrations 024/025). The
activation primitive is `cp.activate_target_visibility(...)` (migration 016,
superseded by **026**), wrapped by `control.visibility.activate(...)`.

All scopes are gated identically: a producer run MUST be `succeeded` AND its
graph-derived sink reconciliation MUST be `ok` before any output can become
business-active. A failed or recon-breached refeed activates nothing.

---

## Supported scopes

| scope             | what it replaces                                   | `replacement_key` shape                | supersede | status |
|-------------------|----------------------------------------------------|----------------------------------------|-----------|--------|
| `business_key`    | only the specific business keys that changed       | the per-row business key, e.g. `{policy_id}:{claim_id}` | yes | implemented |
| `slice`           | the WHOLE `(domain,dataset,business_date)` slice   | `{domain}/{dataset}/{business_date}`   | yes | implemented |
| `file`            | rows derived from one specific source file         | the source file identity (e.g. `file_id`) | yes | implemented |
| `append_only`     | nothing — adds a new active row alongside priors   | a per-append-UNIQUE key (e.g. a uuid)  | **no**    | implemented |
| `manual_approval` | produces *pending* (not active) visibility         | n/a (would need a `'P'` status)        | n/a       | **documented future option** |

### `business_key` — changed-only (default for the policy/claims grain)

Replace ONLY specific business keys. Each changed key is activated with
`replacement_scope='business_key'` and `replacement_key=<the business key>`.
That supersedes (status `N`) only **that key's** prior active row; UNCHANGED
keys keep their original `Y` untouched. A single refeed sink link can therefore
carry MANY per-key `Y` rows. This is the proven Day-2 changed-only behaviour the
insurance policy/claims demo relies on
(`harness/policy_claims_workflow.py`, `tests/test_policy_claims_workflow.py`).

Invariant: at most one active `Y` per
`(domain,dataset,business_date,sink_type,target_name,replacement_scope,replacement_key)`
(partial unique index `uq_target_visibility_active`).

Use when: corrections arrive at row/business-key grain and you must NOT
blanket-deactivate the rest of the slice.

### `slice` — whole-slice refeed

Replace the ENTIRE `(domain,dataset,business_date)` slice. Activating with
`replacement_scope='slice'` deactivates **ALL** currently-active rows for that
`(domain,dataset,business_date,sink_type,target_name)` — regardless of each
prior row's own `replacement_scope`/`replacement_key` (so it correctly
supersedes many prior per-`business_key` `Y` rows from the original load) — then
inserts a single slice `Y`. `replacement_key` is the slice key
`{domain}/{dataset}/{business_date}` (the default when `p_replacement_key` is
NULL).

Use when: the corrected file is a full reload of the day and the whole slice
should be replaced atomically.

### `file` — per-file replacement

Replace only the rows derived from one specific original file. Activating with
`replacement_scope='file'` and `replacement_key=<source file identity>`
supersedes only the prior `file`-scoped row for the same file; other files'
active rows are untouched. No new function logic is required beyond the
`business_key` per-key path — the scope + key simply identify a different
replacement unit.

Use when: a single source file in a multi-file slice is re-delivered corrected.

### `append_only` — add without superseding

Add a new active row WITHOUT deactivating any prior — both stay `Y`. Call
`control.visibility.activate(..., supersede=False)` (SQL `p_supersede=false`).
Because the active uniqueness invariant still forbids two `Y` rows for the SAME
scope/key, each append MUST use a **distinct, per-append `replacement_key`**
(e.g. a fresh uuid), so every append is its own active row. With
`p_supersede=false` the function skips all deactivation and inserts the new `Y`.

Use when: outputs accumulate (e.g. late-arriving partitions that augment rather
than replace prior data).

### `manual_approval` — documented future option (NOT implemented)

Intended semantics: a corrected output is registered as **pending** (awaiting a
human approval) rather than immediately business-active. Implementing this needs
a third visibility status, `'P'` (pending), but `ods.target_visibility.status`
is currently constrained `CHECK (status IN ('Y','N'))` (migration 016). Adding
`'P'` and the approve/reject transitions is deferred.

This is a **documented future option only**. `tests/test_refeed_policy.py`
carries a `strict` `xfail` (`test_manual_approval_pending_is_future_option`)
that asserts inserting a `'P'` row is rejected today by the `CHECK` constraint —
so the day someone adds `'P'`, that test fails loudly and this doc must be
updated.

---

## Implementation notes

* `cp.activate_target_visibility` (migration **026**, superseding the 016 body)
  takes a trailing `p_supersede boolean DEFAULT true`. The `slice` branch
  deactivates the whole slice; the `append_only` path (`p_supersede=false`)
  deactivates nothing; `business_key`/`file`/default keep the original
  single-key supersession **verbatim**.
* `control.visibility.activate(..., replacement_scope=..., replacement_key=...,
  supersede=True)` is the Python wrapper.
* All scopes preserve the recon/visibility gate (succeeded run + graph-derived
  recon `ok`).

See `tests/test_refeed_policy.py` (spec area 4 "Required Tests") for the
end-to-end proofs of each scope.
