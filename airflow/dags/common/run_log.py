import json
import psycopg2

TERMINAL_STATUSES = ('succeeded', 'failed', 'partial')

ALLOWED_FIELDS = {
    'status', 'record_count_source', 'record_count_dq_pass', 'record_count_dq_fail',
    'record_count_published', 'kafka_topic', 'kafka_offset_start', 'kafka_offset_end',
    'config_version_id', 'schema_version_id', 'parents', 'error_summary', 'file_id',
    'business_date',
}


def insert_run_header(conn, *, run_id, pipeline_type, domain, dataset,
                      business_date, file_id, config_version_id,
                      schema_version_id=None, parents=None,
                      kafka_topic=None):
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline.run_log
                    (run_id, pipeline_type, domain, dataset, business_date,
                     file_id, status, kafka_topic, config_version_id, schema_version_id, parents)
                VALUES (%s,%s,%s,%s,%s, %s,'running',%s,%s,%s,%s)
                """,
                (run_id, pipeline_type, domain, dataset, business_date,
                 file_id, kafka_topic, config_version_id, schema_version_id,
                 json.dumps(parents) if parents else None),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def update_run_header(conn, run_id, **fields):
    if not fields:
        return
    invalid = set(fields) - ALLOWED_FIELDS
    if invalid:
        raise ValueError(f"unknown run_log fields: {sorted(invalid)}")
    cols = list(fields.keys())
    vals = [json.dumps(v) if k == 'parents' and v is not None else v
            for k, v in fields.items()]
    sets = ', '.join(f"{c}=%s" for c in cols)
    is_terminal = fields.get('status') in TERMINAL_STATUSES
    if is_terminal:
        sets += ", ended_at=COALESCE(ended_at, NOW())"
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE pipeline.run_log SET {sets} WHERE run_id=%s",
                vals + [run_id],
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def write_stage(conn, *, run_id, stage, status,
                input_ref=None, output_ref=None,
                record_count_in=None, record_count_out=None,
                metrics=None, error=None):
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline.run_stage_log
                    (run_id, stage, status, started_at, ended_at,
                     input_ref, output_ref, record_count_in, record_count_out, metrics, error)
                VALUES (%s,%s,%s, NOW(), NOW(), %s,%s,%s,%s,%s,%s)
                """,
                (run_id, stage, status, input_ref, output_ref,
                 record_count_in, record_count_out,
                 json.dumps(metrics) if metrics else None, error),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def write_recon(conn, *, check_type, run_id, domain, dataset, business_date,
                source_count=None, kafka_count=None, postgres_count=None,
                status, detail=None, window_start=None, window_end=None):
    discrepancy = None
    if source_count is not None and kafka_count is not None:
        discrepancy = (kafka_count or 0) - (source_count or 0)
    elif kafka_count is not None and postgres_count is not None:
        discrepancy = (postgres_count or 0) - (kafka_count or 0)
    pct = None
    if discrepancy is not None and source_count:
        pct = round(100.0 * discrepancy / source_count, 4)
    try:
        with conn.cursor() as cur:
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
        conn.commit()
    except Exception:
        conn.rollback()
        raise
