# glue/jobs/utils.py
import hashlib
import json
import re
from datetime import date

import psycopg2


# ---------------------------------------------------------------------------
# Stage vocabulary — shared constants for run_stage_log.stage
# ---------------------------------------------------------------------------

class Stage:
    """Canonical stage names for pipeline.run_stage_log.stage."""
    RAW_READ        = "raw_read"        # ingestion: read CSV/file from S3 raw
    SCHEMA_VALIDATE = "schema_validate" # validate columns against schema registry
    DQ_CHECK        = "dq_check"        # data quality rules evaluation
    CURATED_WRITE   = "curated_write"   # write Parquet to S3 curated
    CURATED_READ    = "curated_read"    # publish: read curated Parquet
    KAFKA_PUBLISH   = "kafka_publish"   # produce Avro messages to Kafka topic
    RECON_T0        = "recon_t0"        # T0 offset reconciliation check
    SINK_PG_WAIT    = "sink_pg_wait"    # wait for JDBC sink to consume offsets
    SINK_S3_WAIT    = "sink_s3_wait"    # wait for S3 sink to consume offsets
    FINALISE        = "finalise"        # DAG finalise: mark run succeeded


class StageEvent:
    """event_type values for pipeline.run_stage_log.event_type.

    Filtering convention:
        terminal events  = stage_completed | stage_failed | stage_skipped | stage_warned
        in-progress      = stage_started
    """
    STARTED   = "stage_started"
    COMPLETED = "stage_completed"
    FAILED    = "stage_failed"
    SKIPPED   = "stage_skipped"
    WARNED    = "stage_warned"   # completed with warnings (e.g. DQ soft blocks)


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
                   data_classification, version
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
        "data_classification", "version",
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
              SET run_id=EXCLUDED.run_id, status=EXCLUDED.status,
                  record_count=EXCLUDED.record_count, error_reason=EXCLUDED.error_reason,
                  updated_at=NOW()
            """,
            (s3_path, run_id, status, extra.get("record_count"), extra.get("error_reason")),
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


# ---------------------------------------------------------------------------
# New consolidated logging helpers (pipeline.run_log + pipeline.run_stage_log)
# ---------------------------------------------------------------------------

def upsert_run_header(pg_dsn, *, run_id, pipeline_type, domain, dataset,
                      business_date, file_id=None, kafka_topic=None,
                      config_version_id=None):
    with psycopg2.connect(pg_dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline.run_log
                (run_id, pipeline_type, domain, dataset, business_date,
                 file_id, status, kafka_topic, config_version_id)
            VALUES (%s,%s,%s,%s,%s, %s,'running',%s,%s)
            ON CONFLICT (run_id) DO NOTHING
            """,
            (run_id, pipeline_type, domain, dataset, business_date,
             file_id, kafka_topic, config_version_id),
        )


ALLOWED_RUN_LOG_FIELDS = {
    'status', 'record_count_source', 'record_count_dq_pass', 'record_count_dq_fail',
    'record_count_published', 'kafka_topic', 'kafka_offset_start', 'kafka_offset_end',
    'config_version_id', 'schema_version_id', 'parents', 'error_summary', 'file_id',
    'business_date',
}
TERMINAL_STATUSES = ('succeeded', 'failed', 'partial')


def update_run_fields(pg_dsn, run_id, **fields):
    if not fields:
        return
    invalid = set(fields) - ALLOWED_RUN_LOG_FIELDS
    if invalid:
        raise ValueError(f"unknown run_log fields: {sorted(invalid)}")
    cols = list(fields.keys())
    vals = [json.dumps(v) if k == 'parents' and v is not None else v
            for k, v in fields.items()]
    sets = ', '.join(f"{c}=%s" for c in cols)
    if fields.get('status') in TERMINAL_STATUSES:
        sets += ", ended_at=COALESCE(ended_at, NOW())"
    with psycopg2.connect(pg_dsn) as conn, conn.cursor() as cur:
        cur.execute(
            f"UPDATE pipeline.run_log SET {sets} WHERE run_id=%s",
            vals + [run_id],
        )


def write_stage_row(pg_dsn, *, run_id, stage, status,
                    event_type=None,
                    attempt_number=1,
                    input_ref=None, output_ref=None,
                    record_count_in=None, record_count_out=None,
                    metrics=None, error=None,
                    airflow_dag_id=None, airflow_run_id=None, spark_app_id=None):
    with psycopg2.connect(pg_dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline.run_stage_log
                (run_id, stage, status, event_type, attempt_number,
                 started_at, ended_at,
                 input_ref, output_ref, record_count_in, record_count_out, metrics, error,
                 airflow_dag_id, airflow_run_id, spark_app_id)
            VALUES (%s,%s,%s,%s,%s, NOW(), NOW(), %s,%s,%s,%s,%s,%s, %s,%s,%s)
            """,
            (run_id, stage, status, event_type, attempt_number,
             input_ref, output_ref,
             record_count_in, record_count_out,
             json.dumps(metrics) if metrics else None, error,
             airflow_dag_id, airflow_run_id, spark_app_id),
        )


def write_lineage_edge(pg_dsn, *, child_run_id, edge_type,
                       parent_run_id=None, parent_file_id=None,
                       source_ref=None, target_ref=None, record_count=None):
    """Insert one row into pipeline.lineage_edge."""
    with psycopg2.connect(pg_dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline.lineage_edge
                (child_run_id, parent_run_id, parent_file_id,
                 edge_type, source_ref, target_ref, record_count)
            VALUES (%s,%s,%s, %s,%s,%s,%s)
            """,
            (child_run_id, parent_run_id, parent_file_id,
             edge_type, source_ref, target_ref, record_count),
        )


def upsert_file_catalogue(pg_dsn, *, domain, dataset, business_date,
                          file_md5, s3_raw_path=None, sftp_path=None,
                          s3_curated_path=None, file_size_bytes=None,
                          source_row_count=None, state="ingested",
                          last_run_id=None) -> str:
    """Upsert a file_catalogue row keyed on (domain, dataset, file_md5).

    Returns the file_id UUID as a string.
    """
    import uuid as _uuid
    with psycopg2.connect(pg_dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline.file_catalogue
                (file_id, domain, dataset, business_date, file_md5,
                 s3_raw_path, sftp_path, s3_curated_path,
                 file_size_bytes, source_row_count, state, last_run_id)
            VALUES (%s,%s,%s,%s,%s, %s,%s,%s, %s,%s,%s,%s)
            ON CONFLICT (domain, dataset, file_md5) DO UPDATE SET
                state          = EXCLUDED.state,
                s3_raw_path    = COALESCE(EXCLUDED.s3_raw_path,    pipeline.file_catalogue.s3_raw_path),
                s3_curated_path= COALESCE(EXCLUDED.s3_curated_path,pipeline.file_catalogue.s3_curated_path),
                sftp_path      = COALESCE(EXCLUDED.sftp_path,      pipeline.file_catalogue.sftp_path),
                file_size_bytes= COALESCE(EXCLUDED.file_size_bytes, pipeline.file_catalogue.file_size_bytes),
                source_row_count=COALESCE(EXCLUDED.source_row_count,pipeline.file_catalogue.source_row_count),
                last_run_id    = EXCLUDED.last_run_id,
                state_updated_at = NOW()
            RETURNING file_id
            """,
            (str(_uuid.uuid4()), domain, dataset, business_date, file_md5,
             s3_raw_path, sftp_path, s3_curated_path,
             file_size_bytes, source_row_count, state, last_run_id),
        )
        return str(cur.fetchone()[0])


def write_recon_row(pg_dsn, *, check_type, run_id, domain, dataset, business_date,
                    source_count=None, kafka_count=None, postgres_count=None,
                    status, detail=None, window_start=None, window_end=None):
    """Insert a row into pipeline.reconciliation_log.

    Computes discrepancy and pct automatically from the supplied counts.
    Opens its own connection so it can be called from anywhere.
    """
    discrepancy = None
    if source_count is not None and kafka_count is not None:
        discrepancy = (kafka_count or 0) - (source_count or 0)
    elif kafka_count is not None and postgres_count is not None:
        discrepancy = (postgres_count or 0) - (kafka_count or 0)
    pct = None
    if discrepancy is not None and source_count:
        pct = round(100.0 * discrepancy / source_count, 4)
    with psycopg2.connect(pg_dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline.reconciliation_log
                (check_type, run_id, domain, dataset, business_date,
                 window_start, window_end,
                 source_count, kafka_count, postgres_count,
                 discrepancy_count, discrepancy_pct, status, detail)
            VALUES (%s,%s,%s,%s,%s, %s,%s, %s,%s,%s, %s,%s,%s,%s)
            """,
            (check_type, run_id, domain, dataset, business_date,
             window_start, window_end,
             source_count, kafka_count, postgres_count,
             discrepancy, pct, status, detail),
        )
