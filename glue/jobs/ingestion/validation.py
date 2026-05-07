"""Schema Registry validation for the ingested DataFrame.

Pulls the configured Avro subject from Schema Registry, extracts the
non-system field names (``_ods_*`` columns are added later by the
enrichment step), and ensures every required field is present in the
DataFrame's columns.

A 404 from the registry is intentionally tolerated — schemas can be
registered after the dataset is onboarded; we don't want to block
ingestion on a paperwork sequence. Any other transport error is
treated as a hard failure (the caller's stage_scope re-raises and
writes ``stage_failed``).
"""
from __future__ import annotations

import json
import os

import requests


class SchemaValidationError(RuntimeError):
    """Raised when the DataFrame is missing required schema columns."""


def validate_columns(
    *,
    df_columns: list[str],
    schema_id: str,
    schema_version: str,
    registry_url: str | None = None,
    timeout: int = 10,
) -> dict:
    """Check that every required field on the registered Avro schema is
    present in ``df_columns``. Returns the metrics dict the caller should
    attach to the stage row.

    Raises :class:`SchemaValidationError` on missing columns; raises
    :class:`requests.RequestException` on transport problems other
    than 404.
    """
    base = registry_url or os.environ.get(
        "SCHEMA_REGISTRY_URL", "http://schema-registry:8081"
    )
    url = f"{base}/subjects/{schema_id}/versions/{schema_version}"
    resp = requests.get(url, timeout=timeout)
    if resp.status_code == 404:
        # Schema not yet registered — pass through, downstream tooling
        # will pick it up on the next push.
        return {
            "schema_id": schema_id,
            "schema_version": schema_version,
            "registry_state": "not_registered",
        }
    resp.raise_for_status()

    schema_str = resp.json().get("schema", "{}")
    avro_schema = json.loads(schema_str)
    avro_fields = [
        f["name"] if isinstance(f, dict) else f
        for f in avro_schema.get("fields", [])
    ]
    required = [f for f in avro_fields if not f.startswith("_ods_")]
    missing = [f for f in required if f not in set(df_columns)]
    if missing:
        raise SchemaValidationError(
            f"Schema validation failed — missing columns: {missing}"
        )
    return {"schema_id": schema_id, "schema_version": schema_version}
