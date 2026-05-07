"""Unit tests for the modular ``glue.jobs.ingestion`` package.

Covers the non-Spark modules — validation, registration, reading
business_date resolution. Spark-bound steps (read_raw, quality.evaluate,
curating.write_and_verify) are exercised by the existing Glue
integration tests.
"""
from __future__ import annotations

import hashlib
import os
import sys
import types
from unittest import mock

import pytest


# Make the repo root importable + stub the ``glue.jobs.utils`` module
# (the validation/registration modules don't touch it, but reading does
# its csv-path import lazily).
_HERE = os.path.dirname(__file__)
_ROOT = os.path.abspath(os.path.join(_HERE, "..", ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)
_GLUE_JOBS = os.path.abspath(os.path.join(_ROOT, "glue", "jobs"))
if _GLUE_JOBS not in sys.path:
    sys.path.insert(0, _GLUE_JOBS)


# ---------------------------------------------------------------------------
# validation
# ---------------------------------------------------------------------------

from glue.jobs.ingestion import validation   # noqa: E402


class _FakeResponse:
    def __init__(self, *, status_code: int = 200, payload: dict | None = None):
        self.status_code = status_code
        self._payload = payload or {}

    def raise_for_status(self) -> None:
        if self.status_code >= 400 and self.status_code != 404:
            raise RuntimeError(f"http {self.status_code}")

    def json(self) -> dict:
        return self._payload


def test_validate_columns_passes_when_all_required_present() -> None:
    schema = {"fields": [
        {"name": "policy_id"}, {"name": "amount"}, {"name": "_ods_run_id"},
    ]}
    payload = {"schema": '{"fields":[{"name":"policy_id"},{"name":"amount"},{"name":"_ods_run_id"}]}'}
    with mock.patch("glue.jobs.ingestion.validation.requests.get",
                    return_value=_FakeResponse(payload=payload)):
        out = validation.validate_columns(
            df_columns=["policy_id", "amount"],
            schema_id="ods.insurance.policies", schema_version="1",
            registry_url="http://fake",
        )
    assert out["schema_id"] == "ods.insurance.policies"
    assert "registry_state" not in out


def test_validate_columns_raises_when_required_field_missing() -> None:
    payload = {"schema": '{"fields":[{"name":"policy_id"},{"name":"amount"}]}'}
    with mock.patch("glue.jobs.ingestion.validation.requests.get",
                    return_value=_FakeResponse(payload=payload)):
        with pytest.raises(validation.SchemaValidationError, match="amount"):
            validation.validate_columns(
                df_columns=["policy_id"],
                schema_id="ods.insurance.policies", schema_version="1",
                registry_url="http://fake",
            )


def test_validate_columns_passes_through_on_404() -> None:
    """Schema not registered yet — early-onboarding path stays open."""
    with mock.patch("glue.jobs.ingestion.validation.requests.get",
                    return_value=_FakeResponse(status_code=404)):
        out = validation.validate_columns(
            df_columns=["x"],
            schema_id="ods.new.thing", schema_version="1",
            registry_url="http://fake",
        )
    assert out["registry_state"] == "not_registered"


def test_validate_columns_ignores_ods_system_fields_in_required_set() -> None:
    """Only non-_ods_* fields must be present in the DataFrame."""
    payload = {"schema": '{"fields":[{"name":"_ods_run_id"},{"name":"_ods_file_id"},{"name":"policy_id"}]}'}
    with mock.patch("glue.jobs.ingestion.validation.requests.get",
                    return_value=_FakeResponse(payload=payload)):
        out = validation.validate_columns(
            df_columns=["policy_id"],
            schema_id="ods.insurance.policies", schema_version="2",
            registry_url="http://fake",
        )
    assert out == {"schema_id": "ods.insurance.policies", "schema_version": "2"}


# ---------------------------------------------------------------------------
# registration
# ---------------------------------------------------------------------------

from glue.jobs.ingestion import registration   # noqa: E402


class _FakeS3:
    def __init__(self, body: bytes, etag: str | None = None):
        self.body = body
        self.size = len(body)
        self._etag = etag if etag is not None else hashlib.md5(body).hexdigest()

    def head_object(self, Bucket, Key):
        return {"ETag": f'"{self._etag}"', "ContentLength": self.size}

    def get_object(self, Bucket, Key):
        # Mimic boto3 streaming body.
        return {"Body": types.SimpleNamespace(read=lambda: self.body)}


def test_head_md5_uses_etag_when_single_part() -> None:
    body = b"hello,world\n1,2\n"
    fake_etag = hashlib.md5(body).hexdigest()
    md5, size = registration.head_md5(
        "s3://bucket/key.csv",
        s3_client=_FakeS3(body, etag=fake_etag),
    )
    assert md5 == fake_etag
    assert size == len(body)


def test_head_md5_falls_back_to_streaming_when_multipart_etag() -> None:
    body = b"some bytes"
    md5, _ = registration.head_md5(
        "s3://bucket/key.csv",
        s3_client=_FakeS3(body, etag="abc-3"),  # multipart-style
    )
    assert md5 == hashlib.md5(body).hexdigest()


# ---------------------------------------------------------------------------
# reading.resolve_business_date — jsonl path (no Spark needed)
# ---------------------------------------------------------------------------

from glue.jobs.ingestion import reading   # noqa: E402


class _FakeCursor:
    def __init__(self, rows):
        self._rows = rows

    def __enter__(self): return self

    def __exit__(self, *exc): return None

    def execute(self, sql, params): self._sql = sql; self._params = params

    def fetchone(self): return self._rows[0] if self._rows else None


class _FakeConn:
    def __init__(self, rows): self._rows = rows
    def cursor(self): return _FakeCursor(self._rows)


def test_resolve_business_date_jsonl_uses_file_id_when_supplied() -> None:
    conn = _FakeConn([("2026-04-30",)])
    bd = reading.resolve_business_date(
        conn,
        config={"raw_format": "jsonl"},
        s3_input_path="s3://raw/api_pull/x.jsonl.gz",
        file_id="abc-123",
    )
    assert bd == "2026-04-30"


def test_resolve_business_date_jsonl_raises_when_no_catalogue_row() -> None:
    conn = _FakeConn([])
    with pytest.raises(ValueError, match="business_date"):
        reading.resolve_business_date(
            conn,
            config={"raw_format": "jsonl"},
            s3_input_path="s3://raw/missing.jsonl.gz",
            file_id=None,
        )


def test_resolve_business_date_rejects_unsupported_raw_format() -> None:
    with pytest.raises(ValueError, match="unsupported raw_format"):
        reading.resolve_business_date(
            None,
            config={"raw_format": "avro"},
            s3_input_path="s3://x/y",
            file_id=None,
        )
