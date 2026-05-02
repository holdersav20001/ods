import os
import sys

import pytest

HERE = os.path.dirname(__file__)
sys.path.insert(0, os.path.abspath(os.path.join(HERE, "..", "..", "glue", "jobs")))

from canonicalize import compile_transform


def test_compile_transform_renames_casts_and_derives_fields():
    mapping = {
        "fields": [
            {"source": "RskID", "target": "risk_id", "type": "string", "required": True},
            {"source": "ExposureAmt", "target": "exposure_amount", "type": "double"},
            {
                "source": "AsOfDt",
                "target": "as_of_date",
                "type": "date",
                "format": "yyyyMMdd",
                "required": True,
            },
        ],
        "derived": [
            {"target": "risk_key", "expr": "concat(risk_id, '|', cast(as_of_date as string))"}
        ],
        "required": ["risk_id"],
    }

    exprs, required, warnings = compile_transform(
        mapping,
        available_columns={"RskID", "ExposureAmt", "AsOfDt"},
    )

    assert "cast(`RskID` as string) AS `risk_id`" in exprs
    assert "to_date(`AsOfDt`, 'yyyyMMdd') AS `as_of_date`" in exprs
    assert "concat(risk_id, '|', cast(as_of_date as string)) AS `risk_key`" in exprs
    assert required == ["as_of_date", "risk_id"]
    assert warnings == []


def test_compile_transform_warns_for_missing_source_and_outputs_null():
    mapping = {
        "fields": [
            {"source": "Missing", "target": "risk_id", "type": "string", "required": True},
        ],
    }

    exprs, required, warnings = compile_transform(mapping, available_columns=set())

    assert exprs == ["NULL AS `risk_id`"]
    assert required == ["risk_id"]
    assert warnings == ["source column missing: Missing"]


def test_compile_transform_rejects_empty_fields():
    with pytest.raises(ValueError, match="transform.fields"):
        compile_transform({"fields": []})
