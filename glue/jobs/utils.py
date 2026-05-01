# glue/jobs/utils.py
import hashlib
import json
import os
import re
import sys
from datetime import date

_HERE = os.path.dirname(__file__)
for _root in (
    os.path.abspath(os.path.join(_HERE, "..", "..")),
    "/home/glue_user",
):
    if _root not in sys.path:
        sys.path.insert(0, _root)


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
                   transform_yaml_path
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
    ]
    return dict(zip(cols, row))


def write_job_log(conn, **fields) -> None:
    cols = ", ".join(fields.keys())
    placeholders = ", ".join(["%s"] * len(fields))
    with conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO pipeline.glue_job_log ({cols}) VALUES ({placeholders})",
            list(fields.values()),
        )
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


