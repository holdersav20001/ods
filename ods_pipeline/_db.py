"""Connection helpers for the ODS pipeline control-plane client."""
from __future__ import annotations

import os

import psycopg2


def build_dsn(dsn: str | None = None) -> str:
    """Return a libpq DSN string from explicit arg or environment variables.

    Precedence:
      1. ``dsn`` argument
      2. ``PIPELINE_PG_DSN`` env var
      3. ``PG_DSN`` env var
      4. Individual ``POSTGRES_*`` env vars
    """
    if dsn:
        return dsn
    for key in ("PIPELINE_PG_DSN", "PG_DSN"):
        val = os.environ.get(key)
        if val:
            return val
    host = os.environ.get("POSTGRES_HOST")
    if not host:
        raise ValueError(
            "No Postgres DSN configured. "
            "Set PIPELINE_PG_DSN, PG_DSN, or POSTGRES_HOST."
        )
    return (
        f"host={host} "
        f"port={os.environ.get('POSTGRES_PORT', '5432')} "
        f"dbname={os.environ.get('POSTGRES_DB', 'ods_dev')} "
        f"user={os.environ.get('POSTGRES_USER', 'ods')} "
        f"password={os.environ.get('POSTGRES_PASSWORD', 'ods')}"
    )


def connect(dsn: str | None = None) -> psycopg2.extensions.connection:
    """Open and return a psycopg2 connection.

    Use as a context manager::

        with ods_pipeline.connect(dsn) as conn:
            ods_pipeline.stages.write(conn, ...)
    """
    return psycopg2.connect(build_dsn(dsn))
