"""ODS ingestion pipeline — modular replacement for the
``glue/jobs/ods_ingestion.py`` monolith.

Public API: :func:`run` (same signature as the legacy entrypoint).
The legacy file is now a thin shim that re-exports this function so
existing ``--py-files`` lists in DAGs continue to work.
"""
from .pipeline import run

__all__ = ["run"]
