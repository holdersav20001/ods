"""Negative-path integration tests for the local pipeline.

Each test drops a file via SFTP and asserts the pipeline records the failure
truthfully in pipeline.run_log / pipeline.run_stage_log rather than silently
swallowing it or producing rows in postgres.
"""
from __future__ import annotations

import os
import time

import paramiko
import psycopg2
import pytest
import requests


SFTP_HOST = os.environ.get("SFTP_HOST", "localhost")
SFTP_PORT = int(os.environ.get("SFTP_PORT", "2222"))
CONNECT_URL = os.environ.get("CONNECT_URL", "http://localhost:8083")


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


def _clean_for_bd(pg_conn, bd: str) -> None:
    pg_conn.rollback()
    with pg_conn.cursor() as cur:
        cur.execute("DELETE FROM ods.insurance_policy WHERE _ods_business_date=%s", (bd,))
        cur.execute(
            """
            DELETE FROM pipeline.lineage_edge
             WHERE child_run_id IN (
                   SELECT run_id FROM pipeline.run_log
                    WHERE business_date=%s
             )
                OR parent_file_id IN (
                   SELECT file_id FROM pipeline.file_catalogue
                    WHERE business_date=%s
             )
            """,
            (bd, bd),
        )
        cur.execute(
            "DELETE FROM pipeline.run_stage_log s USING pipeline.run_log r "
            "WHERE s.run_id = r.run_id AND r.business_date=%s",
            (bd,),
        )
        cur.execute("DELETE FROM pipeline.run_events WHERE business_date=%s", (bd,))
        cur.execute("DELETE FROM pipeline.reconciliation_log WHERE business_date=%s", (bd,))
        cur.execute("DELETE FROM pipeline.run_log WHERE business_date=%s", (bd,))
        cur.execute(
            "DELETE FROM pipeline.file_state WHERE s3_path LIKE %s",
            (f"%/{bd}/%",),
        )
        cur.execute(
            "DELETE FROM pipeline.file_state WHERE s3_path LIKE %s",
            (f"%/date={bd}/%",),
        )
        cur.execute("DELETE FROM pipeline.file_catalogue WHERE business_date=%s", (bd,))
    pg_conn.commit()


def _put(name: str, body: str) -> None:
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


def _wait_for_run(pg_conn, business_date: str, since_iso: str, timeout_s: int = 240) -> tuple[str, str]:
    """Poll run_log for a finalised row (ended_at set) started after `since_iso`."""
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        pg_conn.rollback()
        with pg_conn.cursor() as cur:
            cur.execute(
                """
                SELECT status, COALESCE(error_summary,'')
                  FROM pipeline.run_log
                 WHERE business_date=%s
                   AND started_at >= %s
                   AND ended_at IS NOT NULL
                 ORDER BY started_at DESC LIMIT 1
                """,
                (business_date, since_iso),
            )
            row = cur.fetchone()
        if row:
            return row
        time.sleep(3)
    raise AssertionError(f"no run_log row finalised for bd={business_date}")


def test_dq_block_records_failure(pg_conn):
    """Duplicate policy_id violates the 'unique' DQ rule → status=failed."""
    body = (
        "policy_id,status,premium,effective_date\n"
        "DUP1,ACTIVE,500.00,2026-04-10\n"
        "DUP1,LAPSED,600.00,2026-04-10\n"
    )
    bd = "2026-04-10"

    _clean_for_bd(pg_conn, bd)
    with pg_conn.cursor() as cur:
        cur.execute("SELECT NOW()::timestamp")
        since_iso = cur.fetchone()[0]
    pg_conn.commit()

    _put("policies_20260410.csv", body)

    # Wait for terminal state, then re-read the row directly. The run_log row
    # transitions running -> (briefly succeeded by upsert) -> failed as the
    # DQ-fail finaliser overwrites; assert on the finalised state.
    _wait_for_run(pg_conn, bd, since_iso, timeout_s=240)
    deadline = time.time() + 60
    final_status, final_err, dq_fail = "running", "", 0
    while time.time() < deadline:
        pg_conn.rollback()
        with pg_conn.cursor() as cur:
            cur.execute(
                """
                SELECT status, COALESCE(error_summary,''), COALESCE(record_count_dq_fail,0)
                  FROM pipeline.run_log
                 WHERE business_date=%s
                   AND started_at >= %s
                 ORDER BY started_at DESC LIMIT 1
                """,
                (bd, since_iso),
            )
            final_status, final_err, dq_fail = cur.fetchone()
        if final_status == "failed":
            break
        time.sleep(2)

    assert final_status == "failed", (
        f"expected final status=failed; got {final_status} (err={final_err!r})"
    )
    assert dq_fail >= 2, f"expected record_count_dq_fail>=2, got {dq_fail}"


def test_sink_failure_marks_run_partial(pg_conn):
    """Pause the JDBC sink, drop a file. Expect wait_sinks to fail and the
    run_log row to land at status='partial' (not silently 'succeeded')."""
    body = (
        "policy_id,status,premium,effective_date\n"
        "SF1,ACTIVE,150.00,2026-04-11\n"
    )
    bd = "2026-04-11"

    _clean_for_bd(pg_conn, bd)
    with pg_conn.cursor() as cur:
        cur.execute("DELETE FROM ods.insurance_policy WHERE policy_id='SF1'")
        cur.execute("SELECT NOW()::timestamp")
        since_iso = cur.fetchone()[0]
    pg_conn.commit()

    # Stop the entire kafka-connect container so neither sink can consume;
    # wait_sinks must time out and the run finalises as 'partial'.
    import subprocess
    subprocess.run(["docker", "compose", "stop", "kafka-connect", "connect-bootstrap"],
                   capture_output=True, check=False, timeout=30)
    try:
        _put("policies_20260411.csv", body)
        # Wait until wait_sinks stage has written its sink_pg row, then read
        # the run_log status that wait_sinks finalised the run with.
        deadline = time.time() + 420
        sink_pg_status = None
        run_status = None
        while time.time() < deadline:
            pg_conn.rollback()
            with pg_conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT s.status, r.status
                      FROM pipeline.run_stage_log s
                      JOIN pipeline.run_log r ON r.run_id = s.run_id
                     WHERE r.business_date=%s
                       AND r.started_at >= %s
                       AND s.stage='sink_pg_wait'
                     ORDER BY s.started_at DESC LIMIT 1
                    """,
                    (bd, since_iso),
                )
                row = cur.fetchone()
            if row:
                sink_pg_status, run_status = row
                break
            time.sleep(3)
        assert sink_pg_status == "failed", (
            f"sink_pg_wait should be failed; got {sink_pg_status!r} (run={run_status!r})"
        )
        assert run_status in ("partial", "failed"), (
            f"run should land partial/failed; got {run_status!r}"
        )
        status = run_status
    finally:
        # Restart kafka-connect so subsequent tests / pipeline runs work.
        subprocess.run(["docker", "compose", "start", "kafka-connect"],
                       capture_output=True, check=False, timeout=30)
        # Wait for connect REST to come back up
        for _ in range(30):
            try:
                if requests.get(f"{CONNECT_URL}/", timeout=2).ok:
                    break
            except requests.RequestException:
                time.sleep(2)
