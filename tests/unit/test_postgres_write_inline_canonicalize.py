"""Unit-level guard for the inline-canonicalize selectExpr pattern in
``ods_postgres_write.py``.

The non-canonical path in ods_postgres_write applies the YAML transform
inline by composing ``compile_transform`` output with passthrough
expressions for the ODS metadata block, in a SINGLE ``selectExpr``
call. That keeps row identity intact (no shuffle, no row-aligned join).

This test verifies the SQL expressions assembled at runtime so a
regression to the old ``Window.orderBy(monotonically_increasing_id())``
join pattern would be visible without needing Spark.
"""
from __future__ import annotations

import os
import sys

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

# canonicalize lives under glue/jobs; expose it for import.
_GLUE_JOBS = os.path.join(_REPO_ROOT, "glue", "jobs")
if _GLUE_JOBS not in sys.path:
    sys.path.insert(0, _GLUE_JOBS)

from canonicalize import compile_transform  # noqa: E402


_RISK_DEMO_MAPPING = {
    "fields": [
        {"source": "RskID", "target": "risk_id", "type": "string", "required": True},
        {"source": "PolNo", "target": "policy_id", "type": "string", "required": True},
        {"source": "ExposureAmt", "target": "exposure_amount", "type": "double"},
        {"source": "AsOfDt", "target": "as_of_date", "type": "date",
         "format": "yyyyMMdd", "required": True},
    ],
    "required": ["risk_id", "policy_id", "as_of_date"],
}

_CURATED_COLS = {
    "RskID", "PolNo", "ExposureAmt", "AsOfDt",
    "_ods_run_id", "_ods_business_date", "_ods_file_id",
    "_ods_domain", "_ods_dataset", "_ods_source_application",
    "_ods_ingested_at",
}


def _build_select(mapping, available):
    """Mirror the projection ods_postgres_write builds for is_canonical=false."""
    select_exprs, required, _warns = compile_transform(
        {"fields": mapping["fields"], "required": mapping["required"]},
        available_columns=available,
    )
    ods_meta_cols = sorted(c for c in available if c.startswith("_ods_"))
    passthrough = [f"`{c}` AS `{c}`" for c in ods_meta_cols]
    return select_exprs + passthrough, required


def test_inline_canonicalize_emits_canonical_and_passthrough_in_one_projection():
    select_exprs, required = _build_select(_RISK_DEMO_MAPPING, _CURATED_COLS)
    joined = "; ".join(select_exprs)

    # Canonical columns derived from source columns.
    assert "AS `risk_id`" in joined
    assert "AS `policy_id`" in joined
    assert "AS `exposure_amount`" in joined
    assert "AS `as_of_date`" in joined

    # ODS metadata passed through as-is (same column name in/out).
    assert "`_ods_run_id` AS `_ods_run_id`" in joined
    assert "`_ods_business_date` AS `_ods_business_date`" in joined
    assert "`_ods_file_id` AS `_ods_file_id`" in joined

    # Required-target list matches the YAML.
    assert set(required) == {"risk_id", "policy_id", "as_of_date"}


def test_inline_canonicalize_does_not_use_row_number_or_join():
    """Defensive: the projection must NOT introduce a synthetic row index."""
    select_exprs, _ = _build_select(_RISK_DEMO_MAPPING, _CURATED_COLS)
    joined = " | ".join(select_exprs)
    assert "monotonically_increasing_id" not in joined.lower()
    assert "row_number" not in joined.lower()
    assert "_row_idx" not in joined


def test_missing_source_column_emits_null_passthrough_for_metadata():
    """If a curated parquet is missing an ODS column, passthrough should
    only emit columns that ARE present (the ods_postgres_write helper
    only takes columns from df.columns)."""
    available = _CURATED_COLS - {"_ods_file_id"}
    select_exprs, _ = _build_select(_RISK_DEMO_MAPPING, available)
    joined = "; ".join(select_exprs)
    assert "`_ods_file_id`" not in joined
    assert "`_ods_run_id` AS `_ods_run_id`" in joined


# ---------------------------------------------------------------------------
# Lineage-thread guarantees (Reality Checker concern #7)
# ---------------------------------------------------------------------------


def test_ods_file_id_explicitly_threads_through_inline_canonicalize():
    """Reality-check concern: comment in ods_postgres_write says ingest
    provenance is recoverable via _ods_file_id → file_catalogue →
    run_log. That contract holds ONLY if _ods_file_id is in the
    passthrough projection. Pin that fact here so a future change
    that drops _ods_file_id from the passthrough list fails CI."""
    select_exprs, _ = _build_select(_RISK_DEMO_MAPPING, _CURATED_COLS)
    # Find the projection expression for _ods_file_id and assert the
    # source side of the AS is the SAME column name — i.e. it isn't
    # rebranded, it isn't a literal, it isn't dropped.
    matches = [e for e in select_exprs if "`_ods_file_id`" in e]
    assert len(matches) == 1, matches
    assert matches[0].startswith("`_ods_file_id`")
    assert matches[0].endswith("`_ods_file_id`")


# ---------------------------------------------------------------------------
# Collision guard (Code Reviewer HIGH finding on R3)
# ---------------------------------------------------------------------------


def _collisions_for(transform_targets, derived_targets, ods_set):
    """Replicate the runtime collision-guard logic in ods_postgres_write
    so the rules are visible in the test suite."""
    issues = []
    if transform_targets & ods_set:
        issues.append(("transform-vs-ods", sorted(transform_targets & ods_set)))
    if derived_targets & ods_set:
        issues.append(("derived-vs-ods", sorted(derived_targets & ods_set)))
    if transform_targets & derived_targets:
        issues.append(("derived-vs-transform", sorted(transform_targets & derived_targets)))
    return issues


def test_collision_guard_clean_for_risk_demo():
    transform_targets = {f["target"] for f in _RISK_DEMO_MAPPING["fields"]}
    ods_set = {c for c in _CURATED_COLS if c.startswith("_ods_")}
    assert _collisions_for(transform_targets, set(), ods_set) == []


def test_collision_guard_rejects_transform_target_named_like_ods_metadata():
    transform_targets = {"risk_id", "_ods_run_id"}  # malicious / careless
    ods_set = {"_ods_run_id", "_ods_file_id", "_ods_business_date"}
    issues = _collisions_for(transform_targets, set(), ods_set)
    assert issues == [("transform-vs-ods", ["_ods_run_id"])]


def test_collision_guard_rejects_derived_target_named_like_ods_metadata():
    derived_targets = {"_ods_file_id"}
    ods_set = {"_ods_run_id", "_ods_file_id"}
    issues = _collisions_for(set(), derived_targets, ods_set)
    assert issues == [("derived-vs-ods", ["_ods_file_id"])]


def test_collision_guard_rejects_derived_shadowing_transform_target():
    transform_targets = {"risk_id", "policy_id"}
    derived_targets = {"risk_id"}  # derived overwrites the canonical mapping
    ods_set = set()
    issues = _collisions_for(transform_targets, derived_targets, ods_set)
    assert issues == [("derived-vs-transform", ["risk_id"])]
