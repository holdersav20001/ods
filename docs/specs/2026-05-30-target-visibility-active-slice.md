# Target visibility / active-slice control table

**Date:** 2026-05-30
**Status:** proposed
**Scope:** how business-facing target tables decide which refeed/replay output is currently active
**Related:** `docs/specs/2026-05-29-control-plane-design-v2.md`,
`docs/reviews/2026-05-29-lineage-link-decision.md`

---

## 1. Purpose

Lineage records **what happened**. It should keep both the original file and any corrected/refeed file forever,
with full traceability.

Business users usually need a different answer: **which output should I use right now?**

This spec adds a small target-side visibility table that marks the active output(s) for a business slice. The
table lets business views filter to active data without deleting lineage history or physically updating millions
of target rows.

The table answers:

- for `domain/dataset/business_date/target`, which source file or output link is active?
- which old file/output was superseded by a corrected refeed?
- when did the activation/deactivation happen?
- which control-plane run and lineage link produced the active rows?

---

## 2. Design principle

Keep these concerns separate:

- `cp.lineage_link` / `cp.lineage_edge`: audit truth, provenance, replay/refeed chain, counts, transforms.
- target visibility table: business truth, i.e. whether an output is currently usable by business-facing views.

Do **not** delete or rewrite lineage rows when a refeed supersedes an earlier output. Instead, deactivate the old
target visibility row and activate the corrected one.

---

## 3. Table

In the local Postgres harness this can live in `ods.target_visibility`. In production, create the equivalent table
next to the target data, for example in the Oracle target schema. If the target database cannot FK to the control
plane, store UUIDs as values and validate them through control-plane checks.

```sql
CREATE TABLE ods.target_visibility (
    visibility_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Business slice / target identity.
    domain              TEXT NOT NULL,
    dataset             TEXT NOT NULL,
    business_date       DATE NOT NULL,
    sink_type           TEXT NOT NULL,   -- postgres | oracle | kafka | s3 | ...
    target_name         TEXT NOT NULL,   -- e.g. ods.orders, ORACLE_SCHEMA.TABLE, topic name

    -- Output identity.
    file_id             UUID,            -- raw/corrected file when row-level file attribution exists
    lineage_link_id     UUID NOT NULL REFERENCES cp.lineage_link(lineage_link_id),
    producer_run_id     UUID NOT NULL REFERENCES cp.run_log(run_id),
    workflow_run_id     TEXT NOT NULL,

    -- Replacement grouping.
    replacement_scope   TEXT NOT NULL DEFAULT 'slice',
    replacement_key     TEXT NOT NULL,

    -- Visibility state.
    status              CHAR(1) NOT NULL CHECK (status IN ('Y', 'N')),
    activated_at        TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    deactivated_at      TIMESTAMPTZ,
    superseded_by       UUID REFERENCES ods.target_visibility(visibility_id),
    reason              TEXT,

    created_at          TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

    CONSTRAINT inactive_has_deactivated_at CHECK (
        status = 'Y' OR deactivated_at IS NOT NULL
    )
);

CREATE UNIQUE INDEX uq_target_visibility_active
    ON ods.target_visibility (
        domain, dataset, business_date, sink_type, target_name,
        replacement_scope, replacement_key
    )
    WHERE status = 'Y';

CREATE INDEX idx_target_visibility_file
    ON ods.target_visibility (file_id);

CREATE INDEX idx_target_visibility_link
    ON ods.target_visibility (lineage_link_id);
```

### Replacement scope

`replacement_scope` defines what can be active at the same time.

- `slice`: one active output for the entire `(domain, dataset, business_date, sink_type, target_name)` slice.
  Use this when a corrected file replaces the whole business-date output.
- `file`: multiple active files may exist for the same business slice, but only one active output per logical
  source file group. Use this when a slice is built from many files and a corrected file replaces only one of
  them.
- `output`: one active row per explicit output identity. Use only for special fan-out or partitioned targets.

`replacement_key` is the concrete key inside that scope.

Examples:

- `slice`: `replacement_key = domain || '/' || dataset || '/' || business_date`
- `file`: `replacement_key = vendor_logical_file_name` or another stable upstream file identity
- `output`: `replacement_key = target_ref->>'path'` or an explicit output id

For simple first implementation, use `replacement_scope='slice'` and the slice key. Add `file` scope when the
target needs multiple active source files in one business date.

---

## 4. Target row columns

Target rows should continue to carry:

```sql
_ods_lineage_link_id UUID NOT NULL
_ods_workflow_run_id TEXT
```

Recommended addition:

```sql
_ods_source_file_id UUID
```

`_ods_source_file_id` is useful for row-level file attribution. It should be populated when a row maps cleanly to
one raw/corrected source file, such as append, union, and row-preserving transforms.

For aggregate rows derived from many files, `_ods_source_file_id` should be null unless there is a separate
source-set model. Do not pretend an aggregate row came from one file when it came from many.

---

## 5. Business-facing views

Preferred row-level view when target rows carry `_ods_source_file_id`:

```sql
CREATE VIEW ods.v_orders_active AS
SELECT o.*
FROM ods.orders o
JOIN ods.target_visibility tv
  ON tv.lineage_link_id = o._ods_lineage_link_id
 AND (tv.file_id IS NULL OR tv.file_id = o._ods_source_file_id)
WHERE tv.domain = 'sales'
  AND tv.dataset = 'orders'
  AND tv.target_name = 'ods.orders'
  AND tv.status = 'Y';
```

If target rows do not carry `_ods_source_file_id`, the view can filter by `_ods_lineage_link_id` only. That is
still enough for whole-slice replacement, but not enough for exact row-to-file attribution inside a merged output.

---

## 6. Lifecycle

### Initial successful load

After the target write and reconciliation pass:

1. Insert one visibility row with `status='Y'`.
2. Set `lineage_link_id` to the sink/output link stamped on target rows.
3. Set `file_id` if the output maps to one source file.
4. Use `replacement_scope='slice'` unless the target intentionally keeps multiple active files per slice.

### Refeed / corrected file

A corrected file is processed as a new execution with new lineage. After target write and reconciliation pass:

1. Find the active visibility row(s) being superseded for the same replacement scope/key.
2. Update those rows to `status='N'`, set `deactivated_at`, and set `superseded_by` after inserting the new row.
3. Insert the corrected output row with `status='Y'`.
4. Commit the visibility change with, or immediately after, the target write/reconciliation transaction.

No old lineage rows are deleted. Old target rows can remain physically present as long as business views filter
through `target_visibility`.

### Failed or partial run

Do not activate anything until the run has:

- completed the target write,
- passed graph-derived reconciliation,
- and reached `run_log.status='succeeded'`.

Failed runs remain visible in lineage/audit but do not become business-active.

### Airflow retry / restart

Retried writes for the same output should be idempotent:

- If the target rows already exist for the same `lineage_link_id`, do not insert duplicate rows.
- If the active visibility row already exists for the same `lineage_link_id`, return it without creating a second
  active row.
- If a retry supplies the same output identity but a different edge set, fail rather than silently reusing old
  visibility.

---

## 7. Suggested function

The control-plane can expose a single activation primitive. In production this may run against the target database
rather than the `cp` database, but the contract should be the same.

```sql
cp.activate_target_visibility(
    p_domain             text,
    p_dataset            text,
    p_business_date      date,
    p_sink_type          text,
    p_target_name        text,
    p_file_id            uuid,
    p_lineage_link_id    uuid,
    p_producer_run_id    uuid,
    p_workflow_run_id    text,
    p_replacement_scope  text DEFAULT 'slice',
    p_replacement_key    text DEFAULT NULL,
    p_reason             text DEFAULT NULL
) RETURNS uuid
```

Behavior:

1. Verify `p_lineage_link_id` exists and belongs to `p_producer_run_id`.
2. Verify the producer run is `succeeded`.
3. Verify graph-derived reconciliation for the producer run is `ok` when the sink is row materialized.
4. Resolve default `replacement_key` when null.
5. Deactivate the currently active row for the same replacement scope/key.
6. Insert or return the active row for `p_lineage_link_id`.
7. Return `visibility_id`.

The function should be idempotent for the same `p_lineage_link_id`.

---

## 8. Invariants

- At most one `status='Y'` row exists for each `(domain, dataset, business_date, sink_type, target_name,
  replacement_scope, replacement_key)`.
- A visibility row must point to a real `lineage_link_id`.
- A visibility row must point to a `producer_run_id` that reached `succeeded`.
- No failed or partially reconciled run may become active.
- Refeed deactivates the superseded active row before, or in the same transaction as, activating the corrected row.
- Business-facing views must filter to `status='Y'`.
- Lineage history remains immutable and includes both old and corrected outputs.

---

## 9. Test plan

1. **Initial activation:** successful sink run creates one `status='Y'` visibility row.
2. **Refed slice:** original output is active; corrected output arrives; old row becomes `N`, new row becomes `Y`.
3. **Business view:** view returns only corrected rows after refeed.
4. **No activation on failure:** failed run writes no active visibility row.
5. **No activation on recon breach:** target write with row loss records breach and cannot activate.
6. **Idempotent retry:** calling activation twice for the same link returns one active row.
7. **File-scope replacement:** two active files exist in one slice; corrected file deactivates only its predecessor.
8. **Whole-slice replacement:** corrected slice deactivates all prior active rows for that slice key.
9. **Aggregate output:** aggregate target can activate by `lineage_link_id` with `file_id` null.
10. **Audit query:** given an inactive visibility row, query its `superseded_by` chain to the active replacement.

---

## 10. Open decisions

- Should the first implementation use only `replacement_scope='slice'`, or do we need file-level replacement from
  day one?
- What is the stable `replacement_key` for file-level replacement: raw file name, vendor correction id, upstream
  message id, or another source-system key?
- Should target views filter by `_ods_lineage_link_id` only, or should target rows also carry `_ods_source_file_id`?
- Should activation live in `cp` schema, target schema, or both with replication?
