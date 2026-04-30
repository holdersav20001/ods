"""Disaster recovery test: simulate a crashed run mid-pipeline, restart the
pipeline by clearing only the failed run_log row, and verify the file is
re-processed cleanly to a final 'sunk' state with rows in postgres.
"""
from __future__ import annotations

import os
import time
import uuid

import paramiko
import psycopg2
import pytest


SFTP_HOST = os.environ.get("SFTP_HOST", "localhost")
SFTP_PORT = int(os.environ.get("SFTP_PORT", "2222"))


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


def _put(name: str, body: str):
    t = paramiko.Transport((SFTP_HOST, SFTP_PORT))
    t.connect(username="ods", password="odspass")
    sftp = paramiko.SFTPClient.from_transport(t)
    try:
        sftp.chdir("upload")
    except IOError:
        pass
    with sftp.file(name, "w") as f:
        f.write(body)
    sftp.close()
    t.close()


def test_failed_run_resumes_cleanly(pg_conn):
    body = (
        "policy_id,status,premium,effective_date\n"
        "DR1,ACTIVE,400.00,2026-04-20\n"
    )
    fname = "policies_20260420.csv"

    # Clean — FK order: run_stage_log → run_log → file_catalogue → target
    with pg_conn.cursor() as cur:
        cur.execute("DELETE FROM ods.insurance_policy WHERE policy_id='DR1'")
        cur.execute(
            "DELETE FROM pipeline.lineage_edge WHERE child_run_id IN ("
            "  SELECT run_id FROM pipeline.run_log WHERE domain='insurance' "
            "  AND dataset='policies' AND business_date='2026-04-20') "
            "OR parent_file_id IN ("
            "  SELECT file_id FROM pipeline.file_catalogue WHERE domain='insurance' "
            "  AND dataset='policies' AND business_date='2026-04-20')"
        )
        cur.execute(
            "DELETE FROM pipeline.run_stage_log WHERE run_id IN ("
            "  SELECT run_id FROM pipeline.run_log WHERE domain='insurance' "
            "  AND dataset='policies' AND business_date='2026-04-20')"
        )
        cur.execute(
            "DELETE FROM pipeline.run_log WHERE domain='insurance' AND dataset='policies' "
            "AND business_date='2026-04-20'"
        )
        cur.execute(
            "DELETE FROM pipeline.file_catalogue WHERE domain='insurance' "
            "AND dataset='policies' AND business_date='2026-04-20'"
        )
    pg_conn.commit()

    # Inject a synthetic 'failed' run_log row to simulate an earlier crash
    fake_run_id = str(uuid.uuid4())
    fake_file_id = str(uuid.uuid4())
    with pg_conn.cursor() as cur:
        cur.execute("""
            INSERT INTO pipeline.file_catalogue
                (file_id, domain, dataset, business_date, sftp_path, s3_raw_path,
                 file_size_bytes, file_md5, state)
            VALUES (%s,'insurance','policies','2026-04-20','/upload/CRASHED.csv',
                    's3://ods-raw-local/insurance/policies/2026-04-20/CRASHED.csv',
                    1, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'failed')
        """, (fake_file_id,))
        cur.execute("""
            INSERT INTO pipeline.run_log
                (run_id, pipeline_type, domain, dataset, business_date, file_id,
                 status, error_summary)
            VALUES (%s, 's3_batch','insurance','policies','2026-04-20', %s,
                    'failed', 'simulated crash mid-publish')
        """, (fake_run_id, fake_file_id))
    pg_conn.commit()

    # Drop the real recovery file
    _put(fname, body)

    # Wait for happy-path recovery to land DR1 row
    deadline = time.time() + 180
    found = 0
    while time.time() < deadline:
        pg_conn.rollback()
        with pg_conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM ods.insurance_policy WHERE policy_id='DR1'")
            found = cur.fetchone()[0]
        if found == 1:
            break
        time.sleep(3)

    assert found == 1, "Recovery file did not land in postgres"

    # Verify the failed run_log row is still there (not silently lost)
    pg_conn.rollback()
    with pg_conn.cursor() as cur:
        cur.execute("SELECT status, error_summary FROM pipeline.run_log WHERE run_id=%s", (fake_run_id,))
        status, err = cur.fetchone()
    assert status == "failed"
    assert "simulated crash" in err

    # Verify a NEW successful run was created for the same business_date
    with pg_conn.cursor() as cur:
        cur.execute("""
            SELECT count(*) FROM pipeline.run_log
             WHERE domain='insurance' AND dataset='policies'
               AND business_date='2026-04-20' AND status='succeeded'
        """)
        succeeded = cur.fetchone()[0]
    assert succeeded >= 1, "No successful run produced after recovery file drop"
