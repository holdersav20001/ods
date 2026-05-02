"""Integration test: SFTP drop is idempotent — same content twice = one catalogue row."""
import hashlib
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


def _sftp_put(filename: str, body: str) -> None:
    t = paramiko.Transport((SFTP_HOST, SFTP_PORT))
    t.connect(username=SFTP_USER, password=SFTP_PASS)
    sftp = paramiko.SFTPClient.from_transport(t)
    try:
        sftp.chdir("upload")
    except IOError:
        pass
    with sftp.file(filename, "w") as f:
        f.write(body)
    sftp.close()
    t.close()


def _wait_file_catalogue_count(pg, md5: str, timeout: int = 60) -> int:
    deadline = time.time() + timeout
    n = 0
    while time.time() < deadline:
        with pg.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM pipeline.file_catalogue WHERE file_md5=%s",
                (md5,),
            )
            n = cur.fetchone()[0]
        if n >= 1:
            return n
        pg.rollback()
        time.sleep(1)
    return n


def test_same_file_twice_yields_single_catalogue_row(pg_conn):
    body = "policy_id,status,premium\nP1,ACTIVE,100.00\n"
    md5 = hashlib.md5(body.encode()).hexdigest()
    filename = "policies_20260428.csv"
    raw_path = f"s3://ods-raw-local/insurance/policies/2026-04-28/{filename}"

    # Clean prior state for this content and path. The drop DAG dedupes by
    # s3_raw_path, while this test asserts by md5.
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM pipeline.lineage_edge
             WHERE child_run_id IN (
                   SELECT run_id FROM pipeline.run_log
                    WHERE file_id IN (
                          SELECT file_id FROM pipeline.file_catalogue
                           WHERE file_md5=%s OR s3_raw_path=%s
                    )
             )
                OR parent_file_id IN (
                   SELECT file_id FROM pipeline.file_catalogue
                    WHERE file_md5=%s OR s3_raw_path=%s
                )
            """,
            (md5, raw_path, md5, raw_path),
        )
        cur.execute(
            """
            DELETE FROM pipeline.run_stage_log
             WHERE run_id IN (
                   SELECT run_id FROM pipeline.run_log
                    WHERE file_id IN (
                          SELECT file_id FROM pipeline.file_catalogue
                           WHERE file_md5=%s OR s3_raw_path=%s
                    )
             )
            """,
            (md5, raw_path),
        )
        cur.execute(
            """
            DELETE FROM pipeline.run_log
             WHERE file_id IN (
                   SELECT file_id FROM pipeline.file_catalogue
                    WHERE file_md5=%s OR s3_raw_path=%s
             )
            """,
            (md5, raw_path),
        )
        cur.execute(
            "DELETE FROM pipeline.file_catalogue WHERE file_md5=%s OR s3_raw_path=%s",
            (md5, raw_path),
        )
    pg_conn.commit()

    _sftp_put(filename, body)
    n = _wait_file_catalogue_count(pg_conn, md5)
    assert n == 1, f"expected 1 catalogue row after first drop, got {n}"

    _sftp_put(filename, body)
    time.sleep(5)
    pg_conn.rollback()
    with pg_conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM pipeline.file_catalogue WHERE file_md5=%s",
            (md5,),
        )
        assert cur.fetchone()[0] == 1, "second identical drop must not create new row"
