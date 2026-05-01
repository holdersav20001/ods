import json
import os
from pathlib import Path
import requests
import pytest

SCHEMA_REGISTRY = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
POLICIES_SUBJECT = "ods.insurance.policies-value"
POLICIES_SCHEMA_PATH = Path(__file__).resolve().parents[2] / "schemas" / "insurance" / "policies.avsc"


def schema_registry_available():
    try:
        requests.get(f"{SCHEMA_REGISTRY}/subjects", timeout=2)
        return True
    except Exception:
        return False


def ensure_policies_schema_registered():
    schema = json.loads(POLICIES_SCHEMA_PATH.read_text())
    payload = {"schemaType": "AVRO", "schema": json.dumps(schema)}
    response = requests.post(
        f"{SCHEMA_REGISTRY}/subjects/{POLICIES_SUBJECT}/versions",
        json=payload,
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        timeout=10,
    )
    assert response.status_code in (200, 201, 409), response.text


@pytest.mark.skipif(not schema_registry_available(), reason="Schema Registry not running")
def test_policies_schema_registered():
    ensure_policies_schema_registered()
    r = requests.get(f"{SCHEMA_REGISTRY}/subjects")
    assert POLICIES_SUBJECT in r.json()


@pytest.mark.skipif(not schema_registry_available(), reason="Schema Registry not running")
def test_policies_schema_has_required_fields():
    ensure_policies_schema_registered()
    r = requests.get(
        f"{SCHEMA_REGISTRY}/subjects/{POLICIES_SUBJECT}/versions/latest"
    )
    schema = json.loads(r.json()["schema"])
    field_names = [f["name"] for f in schema["fields"]]
    for required in [
        "policy_id",
        "premium",
        "effective_date",
        "_ods_run_id",
        "_ods_file_id",
        "_ods_domain",
        "_ods_dataset",
        "_ods_source_application",
    ]:
        assert required in field_names, f"Missing field: {required}"


@pytest.mark.skipif(not schema_registry_available(), reason="Schema Registry not running")
def test_policies_schema_policy_id_is_string():
    ensure_policies_schema_registered()
    r = requests.get(
        f"{SCHEMA_REGISTRY}/subjects/{POLICIES_SUBJECT}/versions/latest"
    )
    schema = json.loads(r.json()["schema"])
    policy_id_field = next(f for f in schema["fields"] if f["name"] == "policy_id")
    assert policy_id_field["type"] == "string"
