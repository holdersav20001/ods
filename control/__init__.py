"""ODS control-plane client: thin typed wrappers over the cp.* Postgres functions.

Every wrapper takes the psycopg connection as its first argument and a keyword
`commit: bool = True`. Production callers commit per write; tests pass
commit=False and rely on the conn fixture's rollback for isolation.
"""
from . import dlq, lineage, recon, runs, sdk, stages, visibility
from .sdk import task

# Naming cleanup (spec docs/specs/2026-05-30-output-link-input-edge-rename.md,
# Option B): expose the PREFERRED new-name write wrappers at the package top
# level so callers can use ``control.write_output_link`` /
# ``control.write_output_then_rows`` directly. The module objects (control.lineage
# etc.) remain exported unchanged for existing callers.
write_output_link = lineage.write_output_link
write_output_then_rows = lineage.write_output_then_rows

__all__ = [
    "dlq", "lineage", "recon", "runs", "sdk", "stages", "visibility",
    "task", "write_output_link", "write_output_then_rows",
]
