#!/usr/bin/env python3
"""Register all Avro schemas from schemas/ into Confluent Schema Registry."""
import json
import os
import sys
import requests

SCHEMA_REGISTRY = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")


def register_schema(subject: str, schema_path: str) -> int:
    with open(schema_path) as f:
        schema_str = json.dumps(json.load(f))
    payload = {"schema": schema_str, "schemaType": "AVRO"}
    r = requests.post(
        f"{SCHEMA_REGISTRY}/subjects/{subject}/versions",
        json=payload,
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
    )
    r.raise_for_status()
    schema_id = r.json()["id"]
    print(f"Registered {subject} → schema id {schema_id}")
    return schema_id


if __name__ == "__main__":
    schemas = [
        ("ods.insurance.policies-value",          "schemas/insurance/policies.avsc"),
        ("ods.pipeline.run-events-value",          "schemas/pipeline/run_event.avsc"),
        ("ods.insurance.events_append-value",      "schemas/insurance/events_append.avsc"),
        ("ods.insurance.policies_upsert-value",    "schemas/insurance/policies_upsert.avsc"),
    ]
    for subject, path in schemas:
        register_schema(subject, path)
    print("All schemas registered.")
