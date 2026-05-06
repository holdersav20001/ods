"""IngestionPattern dataclass + registry."""
from __future__ import annotations

from dataclasses import dataclass

from ods_pipeline.models import PATTERN_CORRELATION_FIELD, PatternType, Stage


@dataclass(frozen=True)
class IngestionPattern:
    """Declarative spec of an ingestion flow.

    Pattern types:
      - ``file``  — SFTP/S3 file -> Glue ingestion -> raw Kafka -> canonical Kafka -> sink
      - ``cdc``   — DB CDC stream -> raw Kafka -> canonical Kafka -> sink
      - ``api``   — synchronous request -> S3 archive + raw Kafka -> canonical -> sink
      - ``event`` — async event stream -> raw Kafka -> canonical -> sink

    The ``correlation_field`` is derived from ``pattern_type`` so callers
    cannot accidentally desync them.
    """

    name: str
    pattern_type: str
    stages: tuple[str, ...]
    topics: tuple[str, ...]
    sinks: tuple[str, ...]
    recon_checks: tuple[str, ...] = ()
    yaml_config: str | None = None

    def __post_init__(self):
        if self.pattern_type not in PatternType.ALL:
            raise ValueError(
                f"unknown pattern_type {self.pattern_type!r}; "
                f"must be one of {sorted(PatternType.ALL)}"
            )
        unknown_stages = set(self.stages) - Stage.all_values()
        if unknown_stages:
            raise ValueError(f"unknown stages: {sorted(unknown_stages)}")

    @property
    def correlation_field(self) -> str:
        """Canonical envelope correlation field for this pattern type."""
        return PATTERN_CORRELATION_FIELD[self.pattern_type]


# Module-level registry. Patterns register themselves at import time.
PATTERNS: dict[str, IngestionPattern] = {}


def register(pattern: IngestionPattern) -> IngestionPattern:
    """Register ``pattern`` in the global registry. Re-register replaces."""
    PATTERNS[pattern.name] = pattern
    return pattern


def get(name: str) -> IngestionPattern:
    """Return pattern by name. Raises KeyError if unknown."""
    if name not in PATTERNS:
        raise KeyError(
            f"no pattern registered named {name!r}; "
            f"known: {sorted(PATTERNS)}"
        )
    return PATTERNS[name]
