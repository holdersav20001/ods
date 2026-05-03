"""Schema contract checks for Avro payloads used by Kafka sinks."""
from __future__ import annotations

import json
from pathlib import Path

from fastavro import parse_schema


ROOT = Path(__file__).resolve().parents[2]
SCHEMA_DIR = ROOT / "schemas"


def _schema(path: str) -> dict:
    return json.loads((SCHEMA_DIR / path).read_text(encoding="utf-8"))


def _field_names(schema: dict) -> list[str]:
    return [field["name"] for field in schema["fields"]]


def test_all_avro_schemas_parse():
    for path in sorted(SCHEMA_DIR.rglob("*.avsc")):
        parse_schema(json.loads(path.read_text(encoding="utf-8")))


def test_policy_upsert_keeps_policy_payload_contract():
    history = _schema("insurance/policies.avsc")
    upsert = _schema("insurance/policies_upsert.avsc")

    assert _field_names(upsert) == _field_names(history)
    assert upsert["fields"][0]["name"] == "policy_id"
    assert upsert["fields"][0]["type"] == "string"


def test_risk_canonical_preserves_source_lineage_metadata():
    canonical = _schema("insurance/risk_canonical.avsc")
    names = set(_field_names(canonical))

    assert {
        "_ods_file_id",
        "_ods_run_id",
        "_ods_raw_run_id",
        "_ods_canonicalize_run_id",
        "_ods_domain",
        "_ods_dataset",
        "_ods_source_application",
    }.issubset(names)
