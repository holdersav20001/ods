# glue/jobs/utils.py
import hashlib
import json
import re
from datetime import date

import psycopg2


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
