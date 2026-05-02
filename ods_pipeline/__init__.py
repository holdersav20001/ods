"""ODS Pipeline control-plane client.

Usage
-----
::

    import ods_pipeline

    with ods_pipeline.connect(dsn) as conn:
        file_id = ods_pipeline.files.upsert(conn, domain="insurance", ...)
        ods_pipeline.runs.start(conn, run_id=run_id, ...)
        ods_pipeline.stages.write(conn, run_id=run_id, stage=ods_pipeline.Stage.RAW_READ, ...)
        ods_pipeline.lineage.write_edge(conn, child_run_id=run_id, ...)
        ods_pipeline.runs.finish(conn, run_id=run_id, status="succeeded")

    ods_pipeline.events.produce("run_succeeded", run_id=run_id, ...)
"""

from ods_pipeline._db import build_dsn, connect
from ods_pipeline import files, runs, stages, lineage, reconciliation, events, metadata, offsets, messages, dlq
from ods_pipeline.models import Stage, StageEvent, RunStatus, TERMINAL_STATUSES

__all__ = [
    # connection helpers
    "build_dsn",
    "connect",
    # sub-modules
    "files",
    "runs",
    "stages",
    "lineage",
    "reconciliation",
    "events",
    "metadata",
    "offsets",
    "messages",
    "dlq",
    # constants
    "Stage",
    "StageEvent",
    "RunStatus",
    "TERMINAL_STATUSES",
]
