import json
import os
import requests
import pytest

SCHEMA_REGISTRY = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")


def schema_registry_available():
    try:
        requests.get(f"{SCHEMA_REGISTRY}/subjects", timeout=2)
        return True
    except Exception:
        return False


@pytest.mark.skipif(not schema_registry_available(), reason="Schema Registry not running")
def test_policies_schema_registered():
    r = requests.get(f"{SCHEMA_REGISTRY}/subjects")
    assert "ods-insurance-policies-value" in r.json()


@pytest.mark.skipif(not schema_registry_available(), reason="Schema Registry not running")
def test_policies_schema_has_required_fields():
    r = requests.get(
        f"{SCHEMA_REGISTRY}/subjects/ods-insurance-policies-value/versions/latest"
    )
    schema = json.loads(r.json()["schema"])
    field_names = [f["name"] for f in schema["fields"]]
    for required in ["policy_id", "premium_amount", "start_date", "_ods_run_id"]:
        assert required in field_names, f"Missing field: {required}"


@pytest.mark.skipif(not schema_registry_available(), reason="Schema Registry not running")
def test_policies_schema_policy_id_is_string():
    r = requests.get(
        f"{SCHEMA_REGISTRY}/subjects/ods-insurance-policies-value/versions/latest"
    )
    schema = json.loads(r.json()["schema"])
    policy_id_field = next(f for f in schema["fields"] if f["name"] == "policy_id")
    assert policy_id_field["type"] == "string"
