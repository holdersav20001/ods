# Lineage model — decision & required changes

**Date:** 2026-05-29
**Status:** decision (proposed — pending sign-off), supersedes the open question in the review
**Decision:** **Keep `cp.lineage_link` (Option A), hardened into an explicit per-output handle.** Do not migrate
to an `artifact` + adjacency model now.
**Evidence trail:** `docs/reviews/2026-05-29-lineage-link-vs-artifact-review.md` (full analysis: options A/B/C,
Kafka-as-micro-files, and the Codex review). This document is the resolution; the review is kept as evidence.
**Affects:** `docs/specs/2026-05-29-control-plane-design-v2.md`, `docs/plans/2026-05-29-control-plane-implementation.md`

---

## 1. Decision

Keep `lineage_link` as the lineage handle for the current build. Treat the link explicitly as **the id of one
output / one write-event** — never as a per-run id. Add the changes in §3 before the relevant build phases.

We do **not** adopt the `artifact` + adjacency rebuild (review Option B/B1) now, because P1/P2 (schema, functions,
target-table FK, harness, DLQ/replay, fan-out tests, `write_link_then_rows`) are already built around
`lineage_link`, and the migration cost grows every phase. See §5 for when to revisit.

## 2. Why (the two findings that forced changes)

The review and the Codex pass surfaced **two issues that are current defects in v2**, not future nice-to-haves.
Both stem from one root: **once a single run can produce multiple outputs, anything that identifies an output by
`run_id` (or by a key that omits output identity) loses or corrupts lineage.**

### 2.1 Output-side defect — dedup key collapses fan-out (BUG, gated before P3d)

Current idempotency key on `write_lineage_link`:

```
ON CONFLICT (consumer_run_id, edge_type, target_ref->>'content_hash') DO NOTHING
```

Fan-out writes the **same canonical bytes** to two sinks (e.g. postgres + kafka). Same bytes → **same
`content_hash`**, same `edge_type='canonical_to_sink'`, same `consumer_run_id` → the conflict key matches → the
**second sink link is silently dropped**.

This **contradicts the v2 spec's own fan-out requirement** ("two `canonical_to_sink` links;
`child.record_count == parent.record_count` per `sink_type`"). The key as written cannot produce two links for two
sinks of identical content. **This is a correctness bug, not hardening.**

### 2.2 Input-side defect — run-to-run edges can't name which upstream output (GAP)

`lineage_edge` identifies an upstream by `upstream_run_id` (+ `source_file_id` for file inputs). File inputs are
unambiguous — `source_file_id` pins the exact file. But **run-to-run** edges (e.g. `curated_to_canonical`, where
the upstream is a prior run's *output*) carry **no `upstream_lineage_link_id`**. If that upstream run produced
multiple outputs, the edge names only the run, so the recursive provenance walk **treats all outputs of that run
as ancestors** — over-claiming lineage for a consumer that read only one of them.

This is the exact input-side dual of the output-side multi-output hazard.

## 3. Required changes (Option A, hardened)

### C1 — Spec invariant: one link per output, never per run *(spec)*
Promote from convention to a stated, tested invariant: **`lineage_link_id` identifies exactly one
output / one write-event. A run that produces K outputs mints K links. `run_id` is never the implied output id.**

### C2 — Fix the dedup/uniqueness key *(schema + functions; gate before P3d)*
Include **output identity** in the conflict key so distinct outputs of one run cannot collapse. Add `sink_type`
and the target path:

```
ON CONFLICT (consumer_run_id, edge_type, sink_type, target_ref->>'path', target_ref->>'content_hash')
```

(Or an explicit `target_ref->>'output_id'`.) Re-verify restart-task / refeed idempotency (spec decision #5) under
the new key — same input + same target must still dedup to one link; different target must produce two.

### C3 — Add `upstream_lineage_link_id` to `lineage_edge`, REQUIRED for run-to-run edges *(schema)*
An edge whose upstream is a prior run's output must name **that output**, not just the run. Make
`upstream_lineage_link_id UUID REFERENCES cp.lineage_link` **NOT NULL for run-to-run edge types**
(`curated_to_canonical`, `merge_to_canonical`, `canonical_to_sink`); it stays null only for true file-source edges
(`raw_to_curated`), which are pinned by `source_file_id`. This is **required**, not "preferable" — it is the only
disambiguation for multi-output upstreams.

### C4 — Tests *(plan P3/P4)*
- **Multi-output collision:** one run writes two outputs, same `edge_type` and same `content_hash`, **different
  targets** → assert **two distinct `lineage_link_id`** (proves C2).
- **Downstream specificity:** a task consumes one output of a multi-output upstream run → the edge's
  `upstream_lineage_link_id` names that exact output; provenance walk does **not** pull the run's other outputs
  (proves C3).
- **Multi-output independence (invariant):** two outputs from one run trace independently to their own inputs
  (proves C1).

## 4. The synthesis (why this is the target, not a compromise)

Once C3 lands, `lineage_edge` carries `upstream_lineage_link_id` → edges become **link→link adjacency**, with
`lineage_link` playing the role the `artifact` node would have played in Option B1. **Hardened-A is therefore
B1's adjacency model minus the artifact registry and minus the per-batch sink-registration cost** (no need to mint
an id + content_hash for every Kafka/Oracle write).

So this is not "settle for A over the cleaner B." Hardened-A **reaches B1's correctness cheaply**; the only thing
left on the B side is the artifact registry, whose sole extra benefit is making non-file sink outputs first-class
artifacts — deferrable until there is a concrete need.

## 5. When to revisit Option B (artifact + adjacency)

Revisit only if one of these becomes true:

- Non-file sink outputs must be **first-class queryable artifacts** in their own right (not just lineage targets).
- We adopt **OpenLineage** interchange and want native dataset-in/dataset-out events.
- Per-batch sink registration (id + content_hash + ref for every Kafka/Oracle write) becomes **cheap or already
  required** for another reason.

Until then, hardened-A (C1–C4) is the model.

## 6. Open caveats carried forward (unchanged by this decision)

- **Grain:** lineage is **write-event grain**, not **row grain**. A merged row traces to all N inputs
  collectively, not "which input." Acceptable for ODS (count reconciliation). Per-row "which source" would need a
  separate data-plane row tag under **any** model.
- **DLQ / quarantine:** keep as a `quarantine` lineage edge under a link; unaffected.
- **Atomicity (X1):** unchanged — link + all its edges in one transaction.

## 7. Action list

- [ ] C1 — add the "one link per output" invariant to the spec (decisions section).
- [ ] C2 — change the conflict key in `002_functions.sql` / `write_lineage_link`; re-verify decision-#5 idempotency. **Gate before P3d.**
- [ ] C3 — add `upstream_lineage_link_id` to `lineage_edge` (NOT NULL for run-to-run edge types) in `001_schema.sql`.
- [ ] C4 — add the three tests to plan P3/P4.
- [ ] Sign-off on this decision before P3d fan-out work proceeds.
