# glue/jobs/utils_jobs.py
"""Glue job log writes (pipeline.glue_job_log)."""
import utils_bootstrap  # noqa: F401  ensure ods_pipeline on sys.path
from psycopg2 import sql

from ods_pipeline.models import ALLOWED_JOB_LOG_FIELDS


def write_job_log(conn, **fields) -> None:
    if not fields:
        raise ValueError("write_job_log requires at least one field")
    invalid = set(fields) - ALLOWED_JOB_LOG_FIELDS
    if invalid:
        raise ValueError(f"Unknown glue_job_log fields: {sorted(invalid)}")
    cols = list(fields.keys())
    # Defence in depth: belt-and-braces guard against unsafe identifiers
    # creeping into the whitelist.
    for c in cols:
        if not (isinstance(c, str) and c.isidentifier() and c in ALLOWED_JOB_LOG_FIELDS):
            raise ValueError(f"Illegal glue_job_log field name: {c!r}")
    statement = sql.SQL(
        "INSERT INTO pipeline.glue_job_log ({cols}) VALUES ({vals})"
    ).format(
        cols=sql.SQL(", ").join(sql.Identifier(c) for c in cols),
        vals=sql.SQL(", ").join(sql.Placeholder() for _ in cols),
    )
    with conn.cursor() as cur:
        cur.execute(statement, list(fields.values()))
    conn.commit()
