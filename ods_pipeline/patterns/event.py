"""Event-pattern demo registration (T16).

First non-file IngestionPattern. Proves the IngestionPattern abstraction
generalises beyond the file pattern by composing the same control-plane
primitives (runs, stages, lineage, recon) for an HTTP-trigger source.
"""
from __future__ import annotations

from ods_pipeline.models import PatternType, Stage
from ods_pipeline.patterns.base import IngestionPattern, register


EVENT_DEMO = register(IngestionPattern(
    name="insurance.event_demo",
    pattern_type=PatternType.EVENT,
    stages=(
        Stage.MESSAGE_RECEIVE,
        Stage.MESSAGE_VALIDATE,
        Stage.MESSAGE_ARCHIVE,
        Stage.KAFKA_PUBLISH,
        Stage.CANONICAL_TRANSFORM,
        Stage.RECON_MESSAGE,
        Stage.SINK_PG_WAIT,
        Stage.FINALISE,
    ),
    topics=("ods.insurance.events", "ods.insurance.events.canonical"),
    sinks=("jdbc-sink-events",),
    recon_checks=("recon_message_count",),
    yaml_config="patterns/insurance/event_demo.yaml",
))
