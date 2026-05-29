# Lineage model review — `lineage_link` vs. output-artifact id

**Date:** 2026-05-29
**Status:** open design question — needs decision before P3/P4 build solidifies the schema
**Scope:** whether `cp.lineage_link` should exist, or whether the **output artifact id** should be the lineage handle
**Related:** `docs/specs/2026-05-29-control-plane-design-v2.md` (current model), `docs/reviews/2026-05-29-design-review-consolidated.md`

---

## 1. The current model (what we have)

A write-event is split across two tables, with direction encoded **structurally** (not as a column):

- `cp.lineage_link` — the **output side**. One row per write-event. PK `lineage_link_id`. Holds
  `consumer_run_id`, `target_ref {path, content_hash, version}`, `transform_version`, `sink_type`,
  `record_count`. **This is the stampable handle**: target rows carry
  `_ods_lineage_link_id UUID NOT NULL REFERENCES cp.lineage_link`.
- `cp.lineage_edge` — the **input side**. N rows per link. Holds `upstream_run_id`, `source_file_id`,
  `input_slot`, `source_ref`, per-input `record_count`. No `consumer_run_id` (derived via the link FK).

So: **link = output, edge = inputs.** A data row points at exactly one link; the link fans in to N input edges.
`lineage_edge` has **no input/output flag** — there is no need, because the output is the link row itself, not a
tagged edge.

---

## 2. The issue

### 2.1 Why does `lineage_link` exist at all?

The link is a **synthetic node** representing "an output was produced." Its sole indispensable jobs are:

1. **Give each output a single, stable, foreign-keyable id** that can be pushed down into the consumer/target
   column (`_ods_lineage_link_id`, in prod an Oracle column), so any row traces back in one FK hop.
2. **Anchor the atomic write-event** (X1: link + all its edges in one transaction).

A run produces **many** outputs → **many** links ("1 link per write-event, N per run"). Each distinct output
gets its own link id. That is the property that prevents lineage loss (see 2.2).

### 2.2 The multi-output hazard (the real concern)

When a process writes its **first** output, the link id is *that output's* id. When it writes a **second**
output, it **must** mint a **second** link — otherwise both outputs share one id and you can no longer tell which
inputs/lineage belong to which output. **Stamping multiple distinct outputs with one id loses lineage.**

The current model handles this correctly **by construction** (1 link = 1 write-event = 1 output). The hazard is
real only if an implementer reuses a link id across outputs — a discipline question, not a structural guarantee
beyond "mint a new link per write."

### 2.3 The grain caveat (merge / multi-file)

A link is **write-event grain**, not **row grain**. For a merge of N input files into one canonical output, every
output row is stamped with the **one** merge link id, which fans in to **N** input edges. A given output row
therefore traces to **all N** sources collectively — even if (for a union/append merge) that row actually came
from exactly **one** input file.

- This gives **write-event provenance** ("every row of this output derived from these N inputs collectively").
- It does **not** give **per-row attribution** ("this row came from input file 3").
- For ODS canonical merge this is acceptable: reconciliation is by counts
  (`SUM(edge.record_count) == link.record_count`), not per-row source tagging.
- If per-row "which source" is ever required, neither link nor adjacency gives it for free — you need a row-level
  source tag in the data plane.

### 2.4 The original objection to dropping the link

"Sinks aren't files." `canonical_to_sink` targets `postgres | kafka | s3`. An Oracle write or a Kafka publish has
**no output file**, hence no natural file id to stamp. The link existed as the **uniform** stampable handle over
**file and non-file** targets. This objection is what kept the link table alive — **until the micro-file idea
below dissolves it.**

---

## 3. Candidate solutions

### Option A — Keep `lineage_link` (status quo)

Synthetic output node; uniform handle over file and non-file sinks.

- **Pros:** uniform stamp across all sink types with zero per-sink registration cost; atomicity anchor is
  structural; already specced and partially built.
- **Cons:** the output is a *synthetic* node, not the real artifact; an extra table and an extra hop
  (row → link → edges) when for file outputs the real artifact already has identity; the multi-output discipline
  ("mint a new link per output") is convention, enforced only by "write a new row."

### Option B — Use the **output artifact id** as the handle; drop `lineage_link`

Register **every output** as an artifact with its own id + content_hash + ref, then stamp `output_artifact_id`
down to the target. **Unlocked by treating Kafka/Oracle writes as micro-files** (§4): a Kafka batch's
offset-range is its "path", its payload digest its `content_hash`; an Oracle batch write is likewise a registered
artifact. Then **every** output — raw, curated, canonical, sink — has a real artifact id, and the "sinks aren't
files" objection (§2.4) disappears.

Schema shift:

| Now | Proposed (Option B) |
|-----|---------------------|
| `lineage_link` (synthetic output node) + `lineage_edge` (inputs) | **drop `lineage_link`** |
| `file_catalogue` = raw files only | `artifact` = ALL inputs+outputs (raw / curated / canonical / sink micro-files), each `id + content_hash + ref` |
| stamp `_ods_lineage_link_id` | stamp `_ods_output_artifact_id → artifact(id)` |
| edge = input contribution under a link | edge = `(output_artifact_id ← input_artifact_id, edge_type, input_slot, record_count)` |

Two flavours of the edge table under Option B:

- **B1 — Adjacency (recommended).** Each edge row = one `(output_artifact, input_artifact)` pair. Output identity
  is the real artifact id (FK target); N inputs = N rows sharing `output_artifact_id`. **No role column needed** —
  the output is named by being the FK target. Trace = recursive join output→input down to raw. Output-only attrs
  (hash, version, transform_version) live on the `artifact` row.
- **B2 — Role/direction column (OpenLineage style).** One table, each row tagged `role ∈ {input, output}`,
  grouped by an `event_id`. Use only if you need to record an output with zero inputs, or want event-grouping
  distinct from artifact identity. Adds a nullable-attrs concern (input rows carry null transform_version etc.).

- **Pros (B / B1):** output is a real artifact, not synthetic; FK from Oracle points at a real thing; one fewer
  synthetic concept; OpenLineage-aligned; multi-output is naturally collision-free (each output = its own
  artifact id); `file_catalogue` generalises to a single artifact registry.
- **Cons (B / B1):** requires **registering every sink write as an artifact** — minting an id, computing a
  `content_hash` over the batch, and synthesising a `ref` (e.g. offset range) for Kafka/Oracle. That is real
  work and real cost at write time. Also a non-trivial migration from the current two-table model and the
  partially-built P1/P2 code.

### Option C — Ban multi-output write-out (one output per task)

Force each task to produce exactly one output → `run_id` ≈ output id; stamp a run-derived id.

- **Pros:** simplest; removes the multi-output hazard by fiat.
- **Cons:** kills legitimate fan-out (one canonical → multiple sink targets; partitioned writes). Too
  restrictive for likely-zero benefit over B. **Not recommended.**

---

## 4. The "Kafka as micro-files" idea (the pivotal unlock)

Treat every non-file sink write as a **micro-file artifact**:

- **Kafka:** one publish batch = one artifact. `path` = topic + partition + offset-range; `content_hash` =
  digest of the serialized batch; `record_count` = message count.
- **Oracle / Postgres:** one batch write = one artifact. `path` = table + a batch marker; `content_hash` = digest
  of the written rows (or of the staged file if staged).

Consequence: **every output, file or not, gains a real artifact id.** This is what makes Option B viable and
removes the only structural reason the synthetic `lineage_link` had to exist (§2.4).

---

## 5. The single decision that resolves this

**Are we willing to register every sink write as an artifact (mint id + content_hash + ref for each Kafka/Oracle
batch)?**

- **Yes** → adopt **Option B / B1**: `artifact` registry + adjacency edges, stamp `output_artifact_id`. Drop
  `lineage_link`. Cleaner, OpenLineage-aligned, multi-output safe by construction.
- **No (registering/hashing every sink batch is too heavy)** → keep **Option A**: `lineage_link` stays as the
  lightweight synthetic handle. Tighten the spec to make "mint a new link per output" an explicit, tested
  invariant (partial unique / one-output-per-write discipline).

---

## 6. Invariant that holds under every option

**Each distinct output must have its own id.** This is the property that prevents the multi-output lineage loss
in §2.2.

- Option A enforces it structurally: 1 link row = 1 output.
- Option B/B1 enforces it via the real artifact id: 1 artifact = 1 output.

Whichever model is chosen, this invariant must be tested explicitly (write two outputs from one run → two distinct
ids → each traces independently to its own inputs).

---

## 7. Caveats checklist (for the reviewer)

- [ ] **Grain (§2.3):** confirm write-event provenance (not per-row attribution) is sufficient for ODS. If
      per-row source is ever needed, that is a separate data-plane row-tag requirement, unsolved by either model.
- [ ] **Sink registration cost (§3 Option B):** quantify the cost of hashing/registering every Kafka/Oracle batch
      before committing to Option B.
- [ ] **Migration cost:** Option B reworks `file_catalogue` → `artifact`, drops `lineage_link`, and changes the
      stamped column on target tables. Assess against P1/P2 work already done.
- [ ] **Atomicity (X1):** under Option B the atomic boundary moves from "link + edges in one txn" to "output
      artifact + its edges in one txn." Re-verify the guarantee survives the refactor.
- [ ] **Idempotency:** today `write_lineage_link` dedups on `(consumer_run_id, edge_type, content_hash)`. Under
      Option B, dedup key becomes the artifact `content_hash` (+ producing run). Confirm restart-task/refeed
      semantics (spec decision #5) still hold.
- [ ] **Multi-output test (§6):** required under whichever option, currently not explicit in the plan.
- [ ] **DLQ / quarantine edge:** currently a `quarantine` lineage edge under a link. Confirm it maps cleanly to
      the artifact/adjacency model (a quarantine "output artifact" = the DLQ payload).

---

## 8. Recommendation

Lean **Option B / B1** *if* sink-write registration is acceptable — it removes a synthetic concept, aligns with
OpenLineage, FKs the data plane to a real artifact, and is multi-output-safe by construction. The "Kafka as
micro-files" framing is what makes it clean. If per-batch sink hashing/registration is judged too heavy at write
time, **Option A stays**, but the spec must promote "one id per output" from convention to a tested invariant.

**Do not** pursue Option C.

---

## 9. Codex review recommendation

Codex recommendation: keep **Option A (`lineage_link`) for the current build**, but strengthen it so
multi-output runs are explicit and downstream reads can identify the specific output they consumed.

Option B/B1 is still the cleaner long-term lineage model if the project is ready to register every sink write as
an artifact. However, the current repo has already built the schema, functions, target-table FK contract, harness,
DLQ/replay flow, fan-out tests, and `write_link_then_rows` around `lineage_link`. Switching to an `artifact` +
adjacency model now is a real schema rebuild, not a small simplification.

The multi-output Airflow case is legitimate and should be supported:

- One Airflow task / `run_id` may read two files, produce a low-granularity output, then aggregate and produce a
  second output.
- That is acceptable if the task writes **two distinct outputs** and therefore mints **two distinct
  `lineage_link_id` values**.
- The low-granularity rows must be stamped with link A; the aggregate rows must be stamped with link B.
- Both links may share the same upstream input edges if both outputs derive from the same two files.

So "different target" mostly resolves the concern for stamping: two targets should mean two write-events and two
link ids. The remaining risk is downstream ambiguity. If a later task records only `upstream_run_id`, and that
upstream run produced multiple links, the recursive provenance walk can accidentally treat **all outputs from that
run** as upstream. A downstream consumer that read only the aggregate output should be able to name the aggregate
output, not merely the run that produced it.

Recommended near-term changes under Option A:

1. Promote this invariant into the spec: **one `lineage_link_id` per distinct output/write-event, not per run**.
2. Strengthen the idempotency / uniqueness key. Today it is `(consumer_run_id, edge_type, content_hash)`, which can
   collapse distinct outputs from the same run if they share a digest. Include output identity such as
   `target_ref->>'path'`, `sink_type`, or an explicit `target_ref->>'output_id'`.
3. Add a multi-output collision test: one run writes two outputs with the same `edge_type` and same
   `content_hash` but different targets; assert two distinct `lineage_link_id` values.
4. Add a downstream-specificity test or schema hook: when a task consumes one output from a multi-output upstream
   run, the edge should identify the specific upstream output (`source_ref` at minimum; preferably an FK such as
   `upstream_lineage_link_id` if this becomes load-bearing).

Bottom line: **keep `lineage_link` now, but make it the explicit output handle.** Do not allow `run_id` to become
the implied output id once runs can produce multiple outputs.
