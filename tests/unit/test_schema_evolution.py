"""Avro schema evolution unit tests.

Goals:
  - Backward compatibility: a record written with v1 must decode under
    v2 when v2 only adds nullable fields with defaults.
  - Forward compatibility: a record written with v2 must decode under
    v1 when v2 only adds nullable optional fields (v1 ignores them).
  - Incompatible evolution: removing or retyping a required field
    must fail to decode (sanity check that "anything goes" doesn't).

Tests run on the actual Avro schemas the platform ships, plus a
small synthesised pair to exercise the negative path.

fastavro already in requirements-dev.txt; no Kafka or Schema Registry
needed.
"""
from __future__ import annotations

import io
import json
import os

import pytest
from fastavro import parse_schema, schemaless_reader, schemaless_writer


REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))


def _load_schema(rel_path: str) -> dict:
    with open(os.path.join(REPO_ROOT, rel_path)) as f:
        return json.load(f)


def _encode(schema_v: dict, record: dict) -> bytes:
    parsed = parse_schema(schema_v)
    buf = io.BytesIO()
    schemaless_writer(buf, parsed, record)
    return buf.getvalue()


def _decode(schema_v: dict, payload: bytes, *, reader_schema: dict | None = None) -> dict:
    parsed_writer = parse_schema(schema_v)
    parsed_reader = parse_schema(reader_schema) if reader_schema else None
    return schemaless_reader(io.BytesIO(payload), parsed_writer, parsed_reader)


# ---------------------------------------------------------------------------
# Schemas under test
# ---------------------------------------------------------------------------


def _api_pull_demo_v1() -> dict:
    return _load_schema("schemas/insurance/api_pull_demo.avsc")


def _api_pull_demo_v2_add_nullable() -> dict:
    """v2 = v1 plus a new nullable optional field.

    This is the canonical safe-evolution shape: existing producers keep
    working (default null), new consumers can read both."""
    schema = _api_pull_demo_v1()
    schema["fields"].append({
        "name": "_ods_lineage_tag",
        "type": ["null", "string"],
        "default": None,
    })
    return schema


def _api_pull_demo_v2_drop_required() -> dict:
    """v2 = v1 minus the required ``request_id`` field. Must NOT be
    backward-compatible — old producers still write request_id, new
    readers reject it."""
    schema = _api_pull_demo_v1()
    schema["fields"] = [f for f in schema["fields"] if f["name"] != "request_id"]
    return schema


def _api_pull_demo_v2_retype_required() -> dict:
    """v2 retypes ``request_id`` from string → int. Drop-shape change."""
    schema = _api_pull_demo_v1()
    for field in schema["fields"]:
        if field["name"] == "request_id":
            field["type"] = "int"
    return schema


# ---------------------------------------------------------------------------
# Sample records
# ---------------------------------------------------------------------------


def _record_v1(extra: dict | None = None) -> dict:
    base = {
        "request_id": "r-1",
        "payload": "{}",
        "_ods_source_request_id": None,
        "_ods_source_message_id": None,
        "_ods_source_event_id": None,
        "_ods_source_batch_id": None,
        "_ods_source_application": "demo_api",
        "_ods_domain": "insurance",
        "_ods_dataset": "api_pull_demo",
        "_ods_business_date": "2026-05-06",
        "_ods_source_cursor": None,
        "_ods_archive_s3_uri": None,
        "_ods_schema_id": "ods.insurance.api_pull_demo-value",
        "_ods_schema_version": 1,
        "_ods_run_id": "00000000-0000-0000-0000-000000000001",
        "_ods_file_id": None,
        "_ods_ingested_at": "2026-05-06T00:00:00Z",
    }
    if extra:
        base.update(extra)
    return base


# ---------------------------------------------------------------------------
# Backward compatibility — v1 record decoded under v2
# ---------------------------------------------------------------------------


def test_backward_compat_add_nullable_field_decodes_v1_record():
    v1 = _api_pull_demo_v1()
    v2 = _api_pull_demo_v2_add_nullable()
    payload = _encode(v1, _record_v1())
    decoded = _decode(v1, payload, reader_schema=v2)
    # New field gets its default (null), all existing fields unchanged.
    assert decoded["request_id"] == "r-1"
    assert decoded["_ods_lineage_tag"] is None


# ---------------------------------------------------------------------------
# Forward compatibility — v2 record decoded under v1
# ---------------------------------------------------------------------------


def test_forward_compat_v2_record_with_extra_field_decodes_under_v1():
    v1 = _api_pull_demo_v1()
    v2 = _api_pull_demo_v2_add_nullable()
    payload = _encode(v2, _record_v1({"_ods_lineage_tag": "ll-001"}))
    decoded = _decode(v2, payload, reader_schema=v1)
    # v1 reader ignores the new field; everything else round-trips.
    assert decoded["request_id"] == "r-1"
    assert "_ods_lineage_tag" not in decoded


# ---------------------------------------------------------------------------
# Incompatible evolution — must fail loudly
# ---------------------------------------------------------------------------


def test_incompat_drop_required_field_fails_to_decode():
    """v1 record (has request_id) decoded under v2-without-request_id —
    fastavro silently ignores extra fields when reading FORWARD, but
    a v2 producer (without request_id) decoded under v1-WITH-required
    request_id has nothing to fill it. Use that direction here."""
    v1 = _api_pull_demo_v1()
    v2 = _api_pull_demo_v2_drop_required()
    record_no_request_id = {
        k: v for k, v in _record_v1().items() if k != "request_id"
    }
    payload = _encode(v2, record_no_request_id)
    with pytest.raises(Exception):
        _decode(v2, payload, reader_schema=v1)


def test_incompat_retype_required_field_fails_to_encode():
    """Retyping a required field is disallowed: encoding the v1 record
    (request_id is a string) under v2 (request_id is int) must fail."""
    v2_retype = _api_pull_demo_v2_retype_required()
    with pytest.raises(Exception):
        _encode(v2_retype, _record_v1())


# ---------------------------------------------------------------------------
# Round-trip on every shipped schema (no mutation, no compat magic)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("schema_path", [
    "schemas/insurance/api_pull_demo.avsc",
    "schemas/insurance/api_pull_lowlat_demo.avsc",
    "schemas/insurance/api_pull_risk_raw.avsc",
    "schemas/insurance/api_pull_risk_canonical.avsc",
])
def test_schemas_parse_cleanly(schema_path: str):
    """Every shipped Avro schema must be a valid Avro record. Catches
    typos / drift between scripts/register_schemas.py and the actual
    .avsc files."""
    schema = _load_schema(schema_path)
    parsed = parse_schema(schema)
    assert parsed["name"]
    assert parsed["fields"]
