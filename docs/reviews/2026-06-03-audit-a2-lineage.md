# Audit A2 — Lineage / Provenance Completeness & Correctness

Date: 2026-06-03
Lens: A2 (lineage / provenance completeness + correctness)
Scope: `cp.v_provenance` (006/009), `control/queries/trace_row.sql`, `control/lineage.py`,
all output_link / input_edge construction in the three harness workflows, DLQ/quarantine
provenance, refeed/merge.
Method: EXPECTED derived from spec; each finding PROVEN by a read-only trace query against
the committed demo data (TCP 5440, ods_cp) or a code citation. Read-only — DOCUMENT, do NOT fix.

## Summary

CONFIRMED findings: 4 (1 HIGH, 1 HIGH, 1 MEDIUM, 1 LOW). SUSPECTED: 1.

**Headline 1 (HIGH — incomplete input edges, the recurring bug class, STILL PRESENT in
customer_transaction):** the customer_transaction *refeed* aggregate under-reaches. It is
computed from the FULL 6-row detail set but its single `detail_to_aggregate` input edge names
only the *corrected* detail sink (2 physical rows). The original Day-2 detail sink that supplied
the other 4 rows is NOT named. This is the exact class fixed in the DLQ workflow
(`_aggregate_from_detail` now takes a `detail_inputs` LIST) but NOT ported to
`customer_transaction_workflow`. PROVEN by trace query.

**Headline 2 (HIGH — over-claim / unreachable-raw):** the `quarantine` output_link is
`is_provenance=true` but its only input edge carries neither `source_file_id` nor
`upstream_lineage_link_id` (the raw file id is buried inside `source_ref` JSON). A quarantine
output therefore **cannot trace to raw** — its `trace_row.sql` result is a single dead-end hop.
The 023 migration comment claiming "good + quarantine both trace to the raw source via
cp.v_provenance" is an over-claim. PROVEN by trace query.

---

## CONFIRMED-1 (HIGH): customer_transaction refeed aggregate has INCOMPLETE input edges (under-reach)

**Output:** `customer_transaction_workflow.refeed_execution` aggregate output (live link
`1df26174-1f74-4298-99ce-a18e034f603e`).
**Files:** `harness/customer_transaction_workflow.py:499-555` (`_aggregate_from_detail` takes a
single `detail_sink`), `:685-695` (`refeed_execution` computes `aggregate_rows =
_aggregate_rows(detail_rows)` over the FULL detail set, then passes only the corrected
`detail_sink` and `detail_rows=detail_rows`).

**EXPECTED (spec / parity with the just-fixed DLQ path):** a recomputed aggregate must name
EVERY contributing active detail output. The DLQ workflow does this —
`policy_claims_dlq_workflow._aggregate_from_detail` (`:518-569`) takes `detail_inputs: list`,
one `detail_to_aggregate` edge per contributing sink, and `_recompute_affected_aggregates`
(`:875-885`) explicitly appends BOTH the original normal detail sink AND the corrected replay
sink. Its own docstring (`:527-530`): "consumes EVERY contributing active detail output ... so
the aggregate's provenance is COMPLETE rather than only the corrected slice."

**ACTUAL (CONFIRMED, query):**
```
refeed agg 1df26174 -> detail_sink 79ce8448  edge record_count=6  source_ref={'table':'ods.customer_transaction'}
  detail_sink 79ce8448 declared record_count = 2
  detail_sink 79ce8448 PHYSICAL rows in ods.customer_transaction = 2
ALL ct detail sinks: 283b0230=6, 0db875ae=6, 0f513ee1=6, 79ce8448=2   (79ce8448 = corrected, only changed rows)
```
The aggregate was computed from all 6 detail rows but its single input edge names only the
2-row corrected sink. The original Day-2 detail sink (6 rows, the 4 unchanged contributors) is
NOT an input edge. The edge's `record_count=6` further OVER-CLAIMS relative to the 2-row
upstream it names (see CONFIRMED-3). A trace from the refeed aggregate reaches only
`transaction/2026-05-29-refeed.json` + `customer/2026-05-29.json` and never the original
`transaction/2026-05-29.json` that 4 of its 6 aggregated rows came from.

**Impact:** support tracing the refeed daily aggregate cannot reach the raw source of 4 of its 6
constituent rows. Same defect class as the one "just fixed"; the fix was applied to the DLQ
workflow only.

**Fix (do NOT apply — audit-only):** mirror the DLQ fix — give
`customer_transaction_workflow._aggregate_from_detail` a `detail_inputs` list and have
`refeed_execution` pass BOTH the original Day-2 detail sink (record_count = unchanged
contributors) AND the corrected detail sink (record_count = changed rows).

## CONFIRMED-2 (HIGH): quarantine output cannot trace to raw; 023 comment over-claims

**Output:** the `quarantine` output_link built by `cp.quarantine`
(`db/migrations/023_dlq_lifecycle.sql`, body lines building the edge:
`jsonb_build_array(jsonb_build_object('source_ref', p_source_ref, 'edge_type','quarantine',
'record_count', p_record_count))`). Same shape in the superseded `012_edge_validity.sql:209-210`.

**EXPECTED:** 023 header comment: "The quarantine output stays a first-class output_link ... so
good + quarantine both trace to the raw source via cp.v_provenance." So a quarantine output's
trace should reach a raw file.

**ACTUAL (CONFIRMED, query on live quarantine link `ef35976e-...`):**
```
QUARANTINE edge: source_file_id=NULL, upstream_lineage_link_id=NULL,
  source_ref={'raw_file_id':'b8099101-...','raw_path':'s3://raw/insurance_dlq/claim_dlq/2026-05-29.json','schema_version':'claim.v1'}
TRACE quarantine output ef35976e:
  hop 1 quarantine src_file=None raw=None      <-- single hop, DEAD END, no raw
```
The edge is exempt from the `edge_must_anchor` CHECK (`012:58-61` lists `quarantine` in the
exemption) yet `quarantine` is `is_provenance=true` (`001` seed), so it participates in the walk
but terminates with no anchor. The raw file is reachable ONLY by parsing `source_ref->>'raw_file_id'`
out of JSON — exactly the "source_ref carries identity that should be a real column" anti-pattern
the brief calls out.

**Impact:** the platform's promise ("trace any output to raw") fails for quarantine outputs.
The 012 and 023 comments disagree: 012 says the quarantine edge "carries a DLQ payload ref, not
a chain anchor" (i.e. intentionally a leaf), while 023 says it traces to raw. The 023 claim is false.

**Fix (audit-only):** either (a) add `source_file_id` to the quarantine edge (promote
`raw_file_id` from `source_ref` to the real column — it is already known at quarantine time as
`ingest["file_id"]`, see `policy_claims_dlq_workflow.py:361`), so the quarantine output traces to
raw; OR (b) correct the 023 comment to state quarantine outputs intentionally do NOT trace to raw
and remove `quarantine` from `is_provenance` / document the dead-end. (a) is preferred — it makes
the promise true.

## CONFIRMED-3 (MEDIUM): per-edge record_count over-claims on the refeed aggregate edge

**Edge:** refeed aggregate `1df26174` detail_to_aggregate edge, `record_count=6` naming an
upstream output whose declared `record_count=2`.
**Query:**
```
EDGE record_count != upstream output declared record_count:
  link=detail_to_aggregate edge=detail_to_aggregate edge_rc=6 upstream_declared_rc=2  <-- CONFIRMED-1's edge
  link=detail_to_aggregate edge=detail_to_aggregate edge_rc=4 upstream_declared_rc=2  (DLQ recompute, see note)
  link=detail_to_aggregate edge=detail_to_aggregate edge_rc=2 upstream_declared_rc=3  (DLQ recompute)
  link=canonical_to_sink ... edge_rc=2 upstream_declared_rc in {6,4,3}                (changed-only sinks — legit)
  link=curated_to_canonical edge_rc in {3,1} upstream_declared_rc=4                   (good-rows split — legit)
```
**Assessment:** the `canonical_to_sink` (changed-only sink) and `curated_to_canonical`
(good-rows-of-N) mismatches are LEGITIMATE — a per-edge record_count is the *contributing*
count, which legitimately differs from the upstream output's full count (the convention used by
the DLQ recompute's `detail_original`/`detail_corrected` edges, `:879-885`, where edge_rc =
contributing slice). BUT the refeed aggregate edge (`edge_rc=6`) names an upstream that
physically holds 2 rows and claims 6 — it is neither the upstream's full count (2) nor a valid
contributing slice (the other 4 came from a DIFFERENT, un-named sink). This is a direct symptom
of CONFIRMED-1: the code passes `len(detail_rows)` (6) as the edge record_count
(`customer_transaction_workflow.py:544`, `"record_count": len(detail_rows)`) regardless of which
sink it names. Fixing CONFIRMED-1 (split into two edges) resolves this.

## CONFIRMED-4 (LOW): record_count convention is inconsistent between merge edges and aggregate edges

**Observation:** merge edges name the upstream output's FULL declared count
(`_merge_to_detail` customer edge `record_count=len(CUSTOMER_ROWS)`,
`customer_transaction_workflow.py:418`; policy edge `record_count=len(POLICY_ROWS)`,
`policy_claims_dlq_workflow.py:461`) — i.e. edge_rc == upstream output's full count. But the DLQ
recompute aggregate edges deliberately use the *contributing slice* count
(`policy_claims_dlq_workflow.py:882,885`: `record_count=len(normal_affected)` /
`len(corrected_detail_rows)`). Two different semantics for the same `record_count` field across
edge types, undocumented. Not a provenance-correctness break (the graph is complete in the DLQ
case), but a consumer computing "rows contributed" vs "upstream output size" from `record_count`
will get different meanings per edge type. **Fix (audit-only):** document the field's semantics
(contributing-count) and make merge edges consistent, OR add a separate `contributed_count`.

## SUSPECTED-1: customer_transaction refeed detail sink itself is complete, but daily aggregate parity is the only gap

The refeed *detail* sink (`79ce8448`) correctly traces to both the corrected raw transaction and
the reused customer silver (PROVEN: ct_daily SINK `031298ce` reaches
`transaction/2026-05-29-refeed.json` + `customer/2026-05-29.json`). The merge correctly consumes
the ORIGINAL customer silver + CORRECTED transaction silver (`refeed_execution:663,672-676`), no
stale upstream — merge/refeed wiring is CORRECT. The ONLY gap in customer_transaction is the
aggregate-completeness defect (CONFIRMED-1). SUSPECTED-only because I did not exhaustively
enumerate every possible refeed shape (multi-key corrections), only the committed demo shape.

---

## Things that are CORRECT (verified, no finding)

- **DLQ recompute aggregate completeness (the prior fix) — VERIFIED COMPLETE.** Live recomputed
  aggregate `5cbef139` traces through BOTH detail sinks (hop3 shows two `canonical_to_sink`,
  hop4 four `merge_to_canonical`) reaching BOTH `claim_dlq/2026-05-29.json` AND
  `policy_dlq/2026-05-29.json`. The `_recompute_affected_aggregates` `detail_inputs` list names
  every contributor. The incomplete-input-edge bug is genuinely fixed *here*.
- **Corrected/replayed row traces to raw AND carries DLQ context.** Corrected canonical output
  `6ba25637` trace: hop2 `raw_to_curated` -> raw `claim_dlq/2026-05-29.json` (reaches raw), and
  hop1 `replay` -> hop2 `quarantine` (the prior DLQ identity, dead-ends as a leaf — DLQ context
  without polluting the raw set). dlq row `c0f3a195` is `status=resolved`,
  `resolved_by_output_link_id=6ba25637`. Correct.
- **No over-claim from sibling outputs.** trace_row's LINK->LINK walk (009) + the explicit CYCLE
  guard in `trace_row.sql:86` correctly avoids pulling a multi-output upstream's other outputs;
  normal `policy_claim` rows reach exactly their two raw files (policy + claim of the right
  business_date), no extras. v_provenance (link-adjacency) and trace_row agree.
- **Normal target rows in every table reach raw:** customer_transaction(_daily), policy_claim,
  policy_claim_daily, policy_claim_dlq, policy_claim_daily_dlq all traced to their raw file(s).
- **Edge anchoring:** the ONLY unanchored is_provenance edge across all committed data is the
  single `quarantine` edge (CONFIRMED-2). No stray dangling provenance edges elsewhere.
