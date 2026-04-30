"""End-to-end integration test: drop a file via SFTP, expect rows to land in
ods.insurance_policy through the full Airflow + Spark + Kafka + Connect pipeline.
"""
from __future__ import annotations

import os
import time

import paramiko
import psycopg2
import pytest


SFTP_HOST = os.environ.get("SFTP_HOST", "localhost")
SFTP_PORT = int(os.environ.get("SFTP_PORT", "2222"))
SFTP_USER = "ods"
SFTP_PASS = "odspass"


@pytest.fixture(scope="module")
def pg_conn():
    conn = psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", "5440")),
        dbname="ods_dev",
        user="ods",
        password="ods",
    )
    yield conn
    conn.close()


def _put(name: str, body: str) -> None:
    t = paramiko.Transport((SFTP_HOST, SFTP_PORT))
    t.connect(username=SFTP_USER, password=SFTP_PASS)
    sftp = paramiko.SFTPClient.from_transport(t)
    try:
        sftp.chdir("upload")
    except IOError:
        pass
    with sftp.file(name, "w") as f:
        f.write(body)
    sftp.close()
    t.close()


def test_drop_file_lands_in_postgres(pg_conn):
    body = (
        "policy_id,status,premium,effective_date\n"
        "P100,ACTIVE,250.00,2026-04-01\n"
        "P101,ACTIVE,300.00,2026-04-15\n"
    )

    # Reset all state so test is re-runnable (FK order: run_stage_log → run_log → file_catalogue → target)
    with pg_conn.cursor() as cur:
        cur.execute(
            "DELETE FROM ods.insurance_policy WHERE policy_id IN ('P100','P101')"
        )
        cur.execute(
            "DELETE FROM pipeline.lineage_edge WHERE child_run_id IN ("
            "  SELECT run_id FROM pipeline.run_log WHERE domain='insurance' "
            "  AND dataset='policies' AND business_date='2026-04-28') "
            "OR parent_file_id IN ("
            "  SELECT file_id FROM pipeline.file_catalogue WHERE domain='insurance' "
            "  AND dataset='policies' AND business_date='2026-04-28')"
        )
        cur.execute(
            "DELETE FROM pipeline.run_stage_log WHERE run_id IN ("
            "  SELECT run_id FROM pipeline.run_log WHERE domain='insurance' "
            "  AND dataset='policies' AND business_date='2026-04-28')"
        )
        cur.execute(
            "DELETE FROM pipeline.run_log WHERE domain='insurance' AND dataset='policies' "
            "AND business_date='2026-04-28'"
        )
        cur.execute(
            "DELETE FROM pipeline.file_catalogue WHERE domain='insurance' "
            "AND dataset='policies' AND business_date='2026-04-28'"
        )
        cur.execute(
            "DELETE FROM pipeline.file_state "
            "WHERE s3_path IN (%s, %s)",
            (
                "s3://ods-raw-local/insurance/policies/date=20260428/policies_20260428.csv",
                "s3://ods-curated-local/insurance/policies/date=2026-04-28/",
            ),
        )
    pg_conn.commit()

    _put("policies_20260428.csv", body)

    deadline = time.time() + 180
    n = 0
    while time.time() < deadline:
        pg_conn.rollback()
        with pg_conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM ods.insurance_policy "
                "WHERE policy_id IN ('P100','P101')"
            )
            n = cur.fetchone()[0]
        if n == 2:
            return
        time.sleep(3)
    raise AssertionError(f"expected 2 rows in ods.insurance_policy, got {n}")
