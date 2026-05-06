# glue/jobs/utils.py
import hashlib
import os
import re
import sys
from datetime import date

from psycopg2 import sql

_HERE = os.path.dirname(__file__)
for _root in (
    os.path.abspath(os.path.join(_HERE, "..", "..")),
    "/home/glue_user",
):
    if _root not in sys.path:
        sys.path.insert(0, _root)

from ods_pipeline.models import ALLOWED_JOB_LOG_FIELDS  # noqa: E402


def extract_business_date(filename: str, pattern: str) -> date:
    match = re.search(pattern, filename)
    if not match:
        raise ValueError(
            f"Cannot extract business_date from {filename!r} using pattern {pattern!r}"
        )
    s = match.group(1)
    return date(int(s[:4]), int(s[4:6]), int(s[6:8]))


def generate_message_key(key_fields: list, row: dict) -> str:
    parts = "|".join(str(row.get(f, "")) for f in sorted(key_fields))
    return hashlib.sha256(parts.encode()).hexdigest()


def load_dataset_config(conn, domain: str, dataset: str) -> dict:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, domain, dataset, filename_pattern, target_topic,
                   schema_id, schema_version, key_fields, dq_rules,
                   data_classification, version,
                   is_canonical, canonical_topic, canonical_schema_id,
                   transform_yaml_path, source_type,
                   COALESCE(raw_format, 'csv'),
                   COALESCE(source_config, '{}'::jsonb),
                   postgres_target_table, s3_curated_path, write_mode,
                   COALESCE(delivery, 'file_pipeline'),
                   COALESCE(recon_tolerance_records, 0),
                   COALESCE(recon_tolerance_pct, 0)
            FROM pipeline.dataset_config
            WHERE domain = %s AND dataset = %s AND active = TRUE
            """,
            (domain, dataset),
        )
        row = cur.fetchone()
    if not row:
        raise ValueError(f"No active config for {domain}/{dataset}")
    cols = [
        "id", "domain", "dataset", "filename_pattern", "target_topic",
        "schema_id", "schema_version", "key_fields", "dq_rules",
        "data_classification", "version", "is_canonical",
        "canonical_topic", "canonical_schema_id", "transform_yaml_path",
        "source_type", "raw_format", "source_config",
        "postgres_target_table", "s3_curated_path", "write_mode",
        "delivery", "recon_tolerance_records", "recon_tolerance_pct",
    ]
    return dict(zip(cols, row))


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


def set_file_state(conn, s3_path: str, run_id: str, status: str, **extra) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline.file_state (s3_path, run_id, status, record_count, error_reason)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (s3_path) DO UPDATE
              SET run_id=EXCLUDED.run_id,
                  status=EXCLUDED.status,
                  record_count=EXCLUDED.record_count,
                  error_reason=EXCLUDED.error_reason,
                  updated_at=NOW()
            """,
            (
                s3_path,
                run_id,
                status,
                extra.get("record_count"),
                extra.get("error_reason"),
            ),
        )
    conn.commit()


def get_file_state(conn, s3_path: str) -> str | None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT status FROM pipeline.file_state WHERE s3_path = %s",
            (s3_path,),
        )
        row = cur.fetchone()
    return row[0] if row else None


def upsert_run_header(
    pg_dsn,
    *,
    run_id,
    pipeline_type,
    domain,
    dataset,
    business_date,
    file_id=None,
    kafka_topic=None,
    config_version_id=None,
    schema_version_id=None,
    parents=None,
) -> None:
    import psycopg2

    from ods_pipeline import runs

    with psycopg2.connect(pg_dsn) as conn:
        runs.start(
            conn,
            run_id=run_id,
            pipeline_type=pipeline_type,
            domain=domain,
            dataset=dataset,
            business_date=business_date,
            file_id=file_id,
            kafka_topic=kafka_topic,
            config_version_id=config_version_id,
            schema_version_id=schema_version_id,
            parents=parents,
        )


def update_run_fields(pg_dsn, run_id, **fields) -> None:
    import psycopg2

    from ods_pipeline import runs

    with psycopg2.connect(pg_dsn) as conn:
        runs.update(conn, run_id, **fields)


def write_stage_row(
    pg_dsn,
    *,
    run_id,
    stage,
    status,
    event_type=None,
    attempt_number=1,
    input_ref=None,
    output_ref=None,
    record_count_in=None,
    record_count_out=None,
    metrics=None,
    error=None,
    airflow_dag_id=None,
    airflow_run_id=None,
    spark_app_id=None,
) -> None:
    import psycopg2

    from ods_pipeline import stages

    kwargs = {
        "run_id": run_id,
        "stage": stage,
        "attempt_number": attempt_number,
        "input_ref": input_ref,
        "output_ref": output_ref,
        "record_count_in": record_count_in,
        "record_count_out": record_count_out,
        "metrics": metrics,
        "error": error,
        "airflow_dag_id": airflow_dag_id,
        "airflow_run_id": airflow_run_id,
        "spark_app_id": spark_app_id,
    }
    with psycopg2.connect(pg_dsn) as conn:
        if status == "running":
            stages.start(conn, **kwargs)
        else:
            stages.finish(conn, status=status, event_type=event_type, **kwargs)


