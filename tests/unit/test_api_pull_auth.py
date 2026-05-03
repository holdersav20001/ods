"""Bearer / NoAuth auth-provider unit tests for the api_pull poller."""
from __future__ import annotations

import pytest
import requests

from ods_pipeline.ingest.api_pull.auth import (
    BearerAuth,
    NoAuth,
    build_auth,
)


def test_bearer_reads_token_from_env():
    auth = BearerAuth("API_TOKEN", env={"API_TOKEN": "tok-123"})
    session = requests.Session()
    auth.apply(session)
    assert session.headers.get("Authorization") == "Bearer tok-123"


def test_bearer_missing_secret_raises():
    with pytest.raises(ValueError, match="not set in environment"):
        BearerAuth("API_TOKEN", env={})


def test_bearer_blank_secret_ref_raises():
    with pytest.raises(ValueError, match="non-empty secret_ref"):
        BearerAuth("", env={"API_TOKEN": "tok"})


def test_no_auth_does_not_set_header():
    auth = NoAuth()
    session = requests.Session()
    auth.apply(session)
    assert "Authorization" not in session.headers


def test_build_auth_none_returns_no_auth():
    auth = build_auth(None)
    assert auth.type == "none"
    auth = build_auth({"type": "none"})
    assert auth.type == "none"


def test_build_auth_bearer():
    auth = build_auth(
        {"type": "bearer", "secret_ref": "API_TOKEN"},
        env={"API_TOKEN": "tok-xyz"},
    )
    session = requests.Session()
    auth.apply(session)
    assert session.headers["Authorization"] == "Bearer tok-xyz"


def test_build_auth_unknown_type_raises():
    with pytest.raises(ValueError, match="unsupported auth.type"):
        build_auth({"type": "oauth2"}, env={})
