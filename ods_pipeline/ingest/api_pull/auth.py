"""HTTP auth providers for the api_pull poller.

A provider is anything that mutates a ``requests.Session`` so subsequent
requests carry the correct credentials. Bearer is implemented for slice 1;
``none`` is allowed for stub/test sources. ``basic``, ``mtls`` and OAuth
plug into the same Protocol — adding one is one new class + one
``build_auth`` branch.

Secret material is never read from ``source_config``. It is looked up by
``secret_ref`` from the environment (Airflow Secrets Backend mounts as
env vars). This keeps tokens out of the dataset_config table.
"""
from __future__ import annotations

import os
from typing import Mapping, Protocol

import requests


class AuthProvider(Protocol):
    """Anything that knows how to attach credentials to a Session."""

    type: str

    def apply(self, session: requests.Session) -> None: ...


class NoAuth:
    """No-op auth — used by tests and public stub APIs."""

    type = "none"

    def apply(self, session: requests.Session) -> None:  # noqa: D401 — Protocol impl
        return None


class BearerAuth:
    """``Authorization: Bearer <token>`` from a named env var.

    The env var name comes from ``secret_ref`` in source_config. Missing or
    blank token raises immediately so we fail fast on misconfig rather
    than emit unauthenticated requests.
    """

    type = "bearer"

    def __init__(self, secret_ref: str, *, env: Mapping[str, str] | None = None):
        if not secret_ref:
            raise ValueError("bearer auth requires a non-empty secret_ref")
        env = env if env is not None else os.environ
        token = env.get(secret_ref)
        if not token:
            raise ValueError(
                f"bearer auth secret_ref={secret_ref!r} not set in environment"
            )
        self._token = token

    def apply(self, session: requests.Session) -> None:
        session.headers["Authorization"] = f"Bearer {self._token}"


def build_auth(
    auth_config: Mapping[str, object] | None,
    *,
    env: Mapping[str, str] | None = None,
) -> AuthProvider:
    """Construct an AuthProvider from the ``source.auth`` block in YAML."""
    if not auth_config:
        return NoAuth()
    auth_type = str(auth_config.get("type", "none")).lower()
    if auth_type == "none":
        return NoAuth()
    if auth_type == "bearer":
        secret_ref = str(auth_config.get("secret_ref", ""))
        return BearerAuth(secret_ref, env=env)
    raise ValueError(
        f"unsupported auth.type={auth_type!r}; "
        f"slice 1 supports 'none' and 'bearer' only"
    )
