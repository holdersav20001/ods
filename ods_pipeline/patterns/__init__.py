"""Ingestion pattern registry (A4 / T12).

Each pattern type (file, cdc, api, event) has a concrete `IngestionPattern`
declaring its stages, topics, sinks, recon checks and correlation field.
Consumers (DAG factory, Glue entrypoints, event_api) look up patterns
by name from the registry to drive execution.
"""
from ods_pipeline.patterns.base import (
    PATTERNS,
    IngestionPattern,
    get,
    register,
)
from ods_pipeline.patterns import event as _event  # noqa: F401 — registers patterns
from ods_pipeline.patterns import file as _file  # noqa: F401 — registers patterns
from ods_pipeline.patterns import api_pull as _api_pull  # noqa: F401 — registers patterns

__all__ = ["IngestionPattern", "PATTERNS", "get", "register"]
