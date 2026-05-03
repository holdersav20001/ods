"""IngestionPattern template + registry (T12 / A4)."""
from __future__ import annotations

import pytest

from ods_pipeline.models import PatternType, Stage
from ods_pipeline.patterns import PATTERNS, IngestionPattern, get, register


def test_file_pattern_for_policies_registered():
    p = get("insurance.policies")
    assert p.pattern_type == PatternType.FILE
    assert p.correlation_field == "_ods_file_id"
    assert "ods.insurance.policies" in p.topics
    assert "jdbc-sink-policies" in p.sinks
    assert "jdbc-sink-policy-history" in p.sinks
    assert "dual_sink_parity" in p.recon_checks


def test_file_pattern_for_risk_registered():
    p = get("insurance.risk")
    assert p.pattern_type == PatternType.FILE
    assert "jdbc-sink-risk" in p.sinks


def test_unknown_pattern_raises():
    with pytest.raises(KeyError, match="no pattern registered"):
        get("nope.nada")


def test_pattern_rejects_unknown_pattern_type():
    with pytest.raises(ValueError, match="unknown pattern_type"):
        IngestionPattern(name="x", pattern_type="bogus", stages=(),
                          topics=(), sinks=())


def test_pattern_rejects_unknown_stage():
    with pytest.raises(ValueError, match="unknown stages"):
        IngestionPattern(name="x", pattern_type=PatternType.FILE,
                          stages=("not_a_stage",),
                          topics=(), sinks=())


def test_pattern_correlation_field_is_pattern_typed():
    cdc = IngestionPattern(name="cdc.demo", pattern_type=PatternType.CDC,
                            stages=(Stage.RAW_READ,), topics=("t",), sinks=())
    api = IngestionPattern(name="api.demo", pattern_type=PatternType.API,
                            stages=(Stage.MESSAGE_RECEIVE,), topics=("t",), sinks=())
    event = IngestionPattern(name="evt.demo", pattern_type=PatternType.EVENT,
                              stages=(Stage.MESSAGE_RECEIVE,), topics=("t",), sinks=())
    assert cdc.correlation_field == "_ods_change_lsn"
    assert api.correlation_field == "_ods_source_request_id"
    assert event.correlation_field == "_ods_source_event_id"


def test_register_replaces_existing():
    existing = IngestionPattern(name="dup", pattern_type=PatternType.FILE,
                                 stages=(Stage.RAW_READ,),
                                 topics=("t",), sinks=())
    register(existing)
    replacement = IngestionPattern(name="dup", pattern_type=PatternType.CDC,
                                    stages=(Stage.RAW_READ,),
                                    topics=("t2",), sinks=())
    register(replacement)
    assert get("dup").pattern_type == PatternType.CDC


def test_pattern_is_frozen():
    p = get("insurance.policies")
    with pytest.raises(Exception):
        p.name = "changed"  # type: ignore[misc]


def test_registry_exposes_all_known_patterns():
    """At least the two file patterns ship in this commit."""
    assert "insurance.policies" in PATTERNS
    assert "insurance.risk" in PATTERNS
