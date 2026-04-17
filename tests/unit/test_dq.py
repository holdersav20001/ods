# tests/unit/test_dq.py
"""
Unit tests for glue/jobs/dq.py — DQ rule evaluator.

PySpark is only available inside the Glue container.
All tests are skipped automatically when PySpark is not installed.
"""
import sys
import os
from datetime import datetime

import pytest

pyspark = pytest.importorskip("pyspark", reason="PySpark not installed — run inside Glue container")

# Make glue/jobs importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../glue/jobs"))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, ArrayType
)

from dq import evaluate_dq_rules, DQConfigurationError


# ---------------------------------------------------------------------------
# Session-scoped SparkSession — cheap to share across all tests in this file
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("test_dq")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield session
    session.stop()


# ---------------------------------------------------------------------------
# Helper schema used by most tests
# ---------------------------------------------------------------------------

POLICY_SCHEMA = StructType([
    StructField("policy_id",      StringType(),  True),
    StructField("premium_amount", DoubleType(),  True),
    StructField("start_date",     StringType(),  True),
    StructField("end_date",       StringType(),  True),
    StructField("agent_code",     StringType(),  True),
    StructField("postcode",       StringType(),  True),
])

MINIMAL_RULES = {
    "hard_blocks": [],
    "soft_warns": [],
}


# ---------------------------------------------------------------------------
# Test 1 — not_null hard block
# ---------------------------------------------------------------------------

def test_not_null_hard_block(spark):
    data = [
        ("POL-001", 1200.0, "2024-01-01", "2025-01-01", "AGT1", "SW1A"),  # passes
        (None,      1500.0, "2024-02-01", "2025-02-01", "AGT2", "SW1B"),  # fails — null policy_id
    ]
    df = spark.createDataFrame(data, schema=POLICY_SCHEMA)
    rules = {
        "hard_blocks": [{"rule": "not_null", "field": "policy_id"}],
        "soft_warns": [],
    }
    passing_df, failing_df, warnings = evaluate_dq_rules(df, rules, total_count=2)

    passing_ids = [r["policy_id"] for r in passing_df.collect()]
    failing_rows = failing_df.collect()

    assert passing_ids == ["POL-001"]
    assert len(failing_rows) == 1
    assert failing_rows[0]["policy_id"] is None


# ---------------------------------------------------------------------------
# Test 2 — unique hard block
# ---------------------------------------------------------------------------

def test_unique_hard_block(spark):
    data = [
        ("POL-DUP", 1200.0, "2024-01-01", "2025-01-01", "AGT1", "SW1A"),
        ("POL-DUP", 1500.0, "2024-02-01", "2025-02-01", "AGT2", "SW1B"),
        ("POL-UNI", 800.0,  "2024-03-01", "2025-03-01", "AGT3", "SW1C"),
    ]
    df = spark.createDataFrame(data, schema=POLICY_SCHEMA)
    rules = {
        "hard_blocks": [{"rule": "unique", "field": "policy_id"}],
        "soft_warns": [],
    }
    passing_df, failing_df, warnings = evaluate_dq_rules(df, rules, total_count=3)

    passing_ids = [r["policy_id"] for r in passing_df.collect()]
    failing_ids = [r["policy_id"] for r in failing_df.collect()]

    assert sorted(passing_ids) == ["POL-UNI"]
    assert sorted(failing_ids) == ["POL-DUP", "POL-DUP"]  # BOTH occurrences tagged


# ---------------------------------------------------------------------------
# Test 3 — greater_than hard block
# ---------------------------------------------------------------------------

def test_greater_than_hard_block(spark):
    data = [
        ("POL-001", 1200.0, "2024-01-01", "2025-01-01", "AGT1", "SW1A"),  # passes
        ("POL-002", 0.0,    "2024-02-01", "2025-02-01", "AGT2", "SW1B"),  # fails — value=0
        ("POL-003", -50.0,  "2024-03-01", "2025-03-01", "AGT3", "SW1C"),  # fails — negative
    ]
    df = spark.createDataFrame(data, schema=POLICY_SCHEMA)
    rules = {
        "hard_blocks": [{"rule": "greater_than", "field": "premium_amount", "value": 0}],
        "soft_warns": [],
    }
    passing_df, failing_df, warnings = evaluate_dq_rules(df, rules, total_count=3)

    passing_ids = [r["policy_id"] for r in passing_df.collect()]
    failing_ids = sorted([r["policy_id"] for r in failing_df.collect()])

    assert passing_ids == ["POL-001"]
    assert failing_ids == ["POL-002", "POL-003"]


# ---------------------------------------------------------------------------
# Test 4 — valid_date hard block
# ---------------------------------------------------------------------------

def test_valid_date_hard_block(spark):
    data = [
        ("POL-001", 1200.0, "2024-01-15", "2025-01-15", "AGT1", "SW1A"),  # passes
        ("POL-002", 1500.0, "not-a-date", "2025-02-15", "AGT2", "SW1B"),  # fails — bad date
        ("POL-003", 800.0,  None,         "2025-03-15", "AGT3", "SW1C"),  # fails — null
    ]
    df = spark.createDataFrame(data, schema=POLICY_SCHEMA)
    rules = {
        "hard_blocks": [{"rule": "valid_date", "field": "start_date"}],
        "soft_warns": [],
    }
    passing_df, failing_df, warnings = evaluate_dq_rules(df, rules, total_count=3)

    passing_ids = [r["policy_id"] for r in passing_df.collect()]
    failing_ids = sorted([r["policy_id"] for r in failing_df.collect()])

    assert passing_ids == ["POL-001"]
    assert failing_ids == ["POL-002", "POL-003"]


# ---------------------------------------------------------------------------
# Test 5 — date_gte column hard block
# ---------------------------------------------------------------------------

def test_date_gte_column_hard_block(spark):
    data = [
        ("POL-001", 1200.0, "2024-01-01", "2025-01-01", "AGT1", "SW1A"),  # passes — end > start
        ("POL-002", 1500.0, "2025-06-01", "2024-01-01", "AGT2", "SW1B"),  # fails — end < start
    ]
    df = spark.createDataFrame(data, schema=POLICY_SCHEMA)
    rules = {
        "hard_blocks": [
            {
                "rule": "date_gte",
                "field": "end_date",
                "value_type": "column",
                "value": "start_date",
            }
        ],
        "soft_warns": [],
    }
    passing_df, failing_df, warnings = evaluate_dq_rules(df, rules, total_count=2)

    passing_ids = [r["policy_id"] for r in passing_df.collect()]
    failing_ids = [r["policy_id"] for r in failing_df.collect()]

    assert passing_ids == ["POL-001"]
    assert failing_ids == ["POL-002"]


# ---------------------------------------------------------------------------
# Test 6 — _dq_fail_reason format
# ---------------------------------------------------------------------------

def test_fail_reason_format(spark):
    data = [
        (None, 1200.0, "2024-01-01", "2025-01-01", "AGT1", "SW1A"),  # null policy_id
    ]
    df = spark.createDataFrame(data, schema=POLICY_SCHEMA)
    rules = {
        "hard_blocks": [{"rule": "not_null", "field": "policy_id"}],
        "soft_warns": [],
    }
    _, failing_df, _ = evaluate_dq_rules(df, rules, total_count=1)

    assert "_dq_fail_reason" in failing_df.columns

    # Must be ArrayType(StringType())
    from pyspark.sql.types import ArrayType, StringType
    fail_field = failing_df.schema["_dq_fail_reason"]
    assert isinstance(fail_field.dataType, ArrayType)
    assert isinstance(fail_field.dataType.elementType, StringType)

    # Each entry: "rule_type:field:detail"
    reasons = failing_df.collect()[0]["_dq_fail_reason"]
    assert len(reasons) >= 1
    for reason in reasons:
        parts = reason.split(":")
        assert len(parts) == 3, f"Expected 3 colon-separated parts, got: {reason!r}"
        rule_type, field, detail = parts
        assert rule_type == "not_null"
        assert field == "policy_id"


# ---------------------------------------------------------------------------
# Test 7 — multi-rule failure (both reasons in array)
# ---------------------------------------------------------------------------

def test_multi_rule_failure(spark):
    # policy_id is null AND premium_amount <= 0 — row should fail both rules
    data = [
        (None, -100.0, "2024-01-01", "2025-01-01", "AGT1", "SW1A"),
    ]
    df = spark.createDataFrame(data, schema=POLICY_SCHEMA)
    rules = {
        "hard_blocks": [
            {"rule": "not_null",     "field": "policy_id"},
            {"rule": "greater_than", "field": "premium_amount", "value": 0},
        ],
        "soft_warns": [],
    }
    _, failing_df, _ = evaluate_dq_rules(df, rules, total_count=1)

    rows = failing_df.collect()
    assert len(rows) == 1
    reasons = rows[0]["_dq_fail_reason"]
    rule_types = [r.split(":")[0] for r in reasons]
    assert "not_null" in rule_types
    assert "greater_than" in rule_types


# ---------------------------------------------------------------------------
# Test 8 — soft_warn less_than
# ---------------------------------------------------------------------------

def test_soft_warn_less_than(spark):
    data = [
        ("POL-001", 1000.0,  "2024-01-01", "2025-01-01", "AGT1", "SW1A"),  # passes warn
        ("POL-002", 60000.0, "2024-02-01", "2025-02-01", "AGT2", "SW1B"),  # triggers warn
        ("POL-003", 50000.0, "2024-03-01", "2025-03-01", "AGT3", "SW1C"),  # triggers warn (>=)
    ]
    df = spark.createDataFrame(data, schema=POLICY_SCHEMA)
    rules = {
        "hard_blocks": [],
        "soft_warns": [{"rule": "less_than", "field": "premium_amount", "value": 50000}],
    }
    passing_df, failing_df, warnings = evaluate_dq_rules(df, rules, total_count=3)

    # All rows pass (soft warn doesn't filter)
    assert passing_df.count() == 3
    assert failing_df.count() == 0

    # Warning emitted
    assert len(warnings) == 1
    w = warnings[0]
    assert w["field"] == "premium_amount"
    assert w["rule"] == "less_than"
    assert w["failing_count"] == 2
    assert w["total_count"] == 3
    assert "message" in w


# ---------------------------------------------------------------------------
# Test 9 — soft_warn not_past (injected as_of)
# ---------------------------------------------------------------------------

def test_soft_warn_not_past(spark):
    # as_of = 2025-06-01: end_dates before this are "past"
    as_of = datetime(2025, 6, 1)
    data = [
        ("POL-001", 1200.0, "2024-01-01", "2024-01-01", "AGT1", "SW1A"),  # past — triggers warn
        ("POL-002", 1500.0, "2024-02-01", "2026-12-01", "AGT2", "SW1B"),  # future — ok
    ]
    df = spark.createDataFrame(data, schema=POLICY_SCHEMA)
    rules = {
        "hard_blocks": [],
        "soft_warns": [{"rule": "not_past", "field": "end_date"}],
    }
    passing_df, failing_df, warnings = evaluate_dq_rules(df, rules, total_count=2, as_of=as_of)

    assert len(warnings) == 1
    w = warnings[0]
    assert w["field"] == "end_date"
    assert w["rule"] == "not_past"
    assert w["failing_count"] == 1
    assert w["total_count"] == 2


# ---------------------------------------------------------------------------
# Test 10 — completeness_pct — one warning per field
# ---------------------------------------------------------------------------

def test_completeness_pct_per_field(spark):
    # threshold=0.8 means null_pct must be <= 0.2 to pass
    # Here both agent_code and postcode have 3/5 = 60% null → null_pct=0.6 > 0.2 → warn
    data = [
        ("POL-001", 1200.0, "2024-01-01", "2025-01-01", None,   None),
        ("POL-002", 1500.0, "2024-02-01", "2025-02-01", None,   None),
        ("POL-003", 800.0,  "2024-03-01", "2025-03-01", None,   None),
        ("POL-004", 900.0,  "2024-04-01", "2025-04-01", "AGT4", "SW14"),
        ("POL-005", 1100.0, "2024-05-01", "2025-05-01", "AGT5", "SW15"),
    ]
    df = spark.createDataFrame(data, schema=POLICY_SCHEMA)
    rules = {
        "hard_blocks": [],
        "soft_warns": [
            {
                "rule": "completeness_pct",
                "fields": ["agent_code", "postcode"],
                "threshold": 0.8,
            }
        ],
    }
    passing_df, failing_df, warnings = evaluate_dq_rules(df, rules, total_count=5)

    # One warning per field — not one combined
    assert len(warnings) == 2
    warn_fields = {w["field"] for w in warnings}
    assert warn_fields == {"agent_code", "postcode"}

    for w in warnings:
        assert w["rule"] == "completeness_pct"
        assert w["failing_count"] == 3
        assert w["total_count"] == 5
        assert "message" in w


# ---------------------------------------------------------------------------
# Test 11 — empty DF returns early
# ---------------------------------------------------------------------------

def test_empty_df_returns_early(spark):
    df = spark.createDataFrame([], schema=POLICY_SCHEMA)
    rules = {
        "hard_blocks": [{"rule": "not_null", "field": "policy_id"}],
        "soft_warns": [{"rule": "less_than", "field": "premium_amount", "value": 50000}],
    }
    # Should not raise even though total_count=0 would normally be invalid,
    # but the empty guard fires AFTER validation — so we pass total_count=1
    passing_df, failing_df, warnings = evaluate_dq_rules(df, rules, total_count=1)

    assert passing_df.count() == 0
    assert failing_df.count() == 0
    assert warnings == []

    # failing_df must still have _dq_fail_reason column even when empty
    assert "_dq_fail_reason" in failing_df.columns


# ---------------------------------------------------------------------------
# Test 12 — DQConfigurationError: unknown rule
# ---------------------------------------------------------------------------

def test_dq_config_error_unknown_rule(spark):
    data = [("POL-001", 1200.0, "2024-01-01", "2025-01-01", "AGT1", "SW1A")]
    df = spark.createDataFrame(data, schema=POLICY_SCHEMA)
    rules = {
        "hard_blocks": [{"rule": "nonexistent_rule", "field": "policy_id"}],
        "soft_warns": [],
    }
    with pytest.raises(DQConfigurationError, match="unknown.*rule|nonexistent_rule"):
        evaluate_dq_rules(df, rules, total_count=1)


# ---------------------------------------------------------------------------
# Test 13 — DQConfigurationError: field not in schema
# ---------------------------------------------------------------------------

def test_dq_config_error_missing_field(spark):
    data = [("POL-001", 1200.0, "2024-01-01", "2025-01-01", "AGT1", "SW1A")]
    df = spark.createDataFrame(data, schema=POLICY_SCHEMA)
    rules = {
        "hard_blocks": [{"rule": "not_null", "field": "nonexistent_column"}],
        "soft_warns": [],
    }
    with pytest.raises(DQConfigurationError, match="nonexistent_column"):
        evaluate_dq_rules(df, rules, total_count=1)


# ---------------------------------------------------------------------------
# Test 14 — DQConfigurationError: total_count <= 0
# ---------------------------------------------------------------------------

def test_dq_config_error_total_count_zero(spark):
    data = [("POL-001", 1200.0, "2024-01-01", "2025-01-01", "AGT1", "SW1A")]
    df = spark.createDataFrame(data, schema=POLICY_SCHEMA)
    with pytest.raises(DQConfigurationError, match="total_count"):
        evaluate_dq_rules(df, MINIMAL_RULES, total_count=0)


# ---------------------------------------------------------------------------
# Test 15 — set disjointness: no row in both passing and failing
# ---------------------------------------------------------------------------

def test_passing_rows_not_in_failing_df(spark):
    data = [
        ("POL-001", 1200.0, "2024-01-01", "2025-01-01", "AGT1", "SW1A"),  # passes
        (None,      1500.0, "2024-02-01", "2025-02-01", "AGT2", "SW1B"),  # fails
        ("POL-003", 0.0,    "2024-03-01", "2025-03-01", "AGT3", "SW1C"),  # fails
    ]
    df = spark.createDataFrame(data, schema=POLICY_SCHEMA)
    rules = {
        "hard_blocks": [
            {"rule": "not_null",     "field": "policy_id"},
            {"rule": "greater_than", "field": "premium_amount", "value": 0},
        ],
        "soft_warns": [],
    }
    passing_df, failing_df, _ = evaluate_dq_rules(df, rules, total_count=3)

    passing_ids = {r["policy_id"] for r in passing_df.collect()}
    failing_ids = {r["policy_id"] for r in failing_df.collect()}

    # No overlap — disjoint sets
    overlap = passing_ids & failing_ids
    assert overlap == set(), f"Rows in both DFs: {overlap}"
    assert passing_ids == {"POL-001"}
