"""Centralised correlation predicate (T10 / A1).

Asserts:
- each pattern type uses its declared correlation field
- mismatched correlation rejects the message
- broadcast (no context correlation) accepts every message of that pattern
- unknown pattern_type raises
- legacy file-pattern run_id fallback preserved for backward compat with
  glue.jobs.canonicalize.matches_context
"""
from __future__ import annotations

import pytest

from ods_pipeline.messages import correlate
from ods_pipeline.models import PatternType


# ---------- file (legacy two-key match) ----------

def test_file_pattern_matches_by_file_id():
    assert correlate(
        {"_ods_file_id": "f-1"},
        {"_ods_file_id": "f-1"},
        pattern_type=PatternType.FILE,
    )


def test_file_pattern_matches_by_run_id_fallback():
    assert correlate(
        {"_ods_run_id": "r-1"},
        {"_ods_run_id": "r-1"},
        pattern_type=PatternType.FILE,
    )


def test_file_pattern_rejects_mismatch():
    assert not correlate(
        {"_ods_file_id": "f-1", "_ods_run_id": "r-1"},
        {"_ods_file_id": "f-2", "_ods_run_id": "r-2"},
        pattern_type=PatternType.FILE,
    )


def test_file_pattern_no_context_is_broadcast():
    assert correlate(
        {"_ods_file_id": "f-1", "_ods_run_id": "r-1"},
        {},
        pattern_type=PatternType.FILE,
    )


# ---------- cdc ----------

def test_cdc_pattern_matches_by_change_lsn():
    assert correlate(
        {"_ods_change_lsn": "0/16B22A8"},
        {"_ods_change_lsn": "0/16B22A8"},
        pattern_type=PatternType.CDC,
    )


def test_cdc_pattern_rejects_mismatch():
    assert not correlate(
        {"_ods_change_lsn": "0/16B22A8"},
        {"_ods_change_lsn": "0/16B22A9"},
        pattern_type=PatternType.CDC,
    )


# ---------- api ----------

def test_api_pattern_matches_by_request_id():
    assert correlate(
        {"_ods_source_request_id": "req-42"},
        {"_ods_source_request_id": "req-42"},
        pattern_type=PatternType.API,
    )


def test_api_pattern_no_context_broadcast():
    assert correlate(
        {"_ods_source_request_id": "req-42"},
        {},
        pattern_type=PatternType.API,
    )


# ---------- event ----------

def test_event_pattern_matches_by_event_id():
    assert correlate(
        {"_ods_source_event_id": "evt-7"},
        {"_ods_source_event_id": "evt-7"},
        pattern_type=PatternType.EVENT,
    )


def test_event_pattern_message_missing_field_no_match():
    assert not correlate(
        {"some_other": 1},
        {"_ods_source_event_id": "evt-7"},
        pattern_type=PatternType.EVENT,
    )


# ---------- guard rails ----------

def test_unknown_pattern_type_raises():
    with pytest.raises(ValueError, match="unknown pattern_type"):
        correlate({}, {}, pattern_type="bogus")


def test_canonicalize_matches_context_delegates_to_correlate():
    """Existing canonicalize.matches_context now delegates — preserves contract."""
    from glue.jobs.canonicalize import matches_context

    assert matches_context({"_ods_file_id": "f-1"}, file_id="f-1", parent_run_id=None)
    assert not matches_context({"_ods_file_id": "f-2"}, file_id="f-1", parent_run_id=None)
    assert matches_context({"_ods_run_id": "r-1"}, file_id=None, parent_run_id="r-1")
    assert matches_context({}, file_id=None, parent_run_id=None)
