"""Control-plane soak — drive the run_log / file_catalogue / lineage /
reconciliation primitives in a tight loop and assert invariants hold.

No Glue, no Spark, no Kafka — just the SQL contracts. Catches:

  - state drift across many concurrent-style runs (FK violations,
    open transactions left dangling, idempotency failures);
  - stuck-running rows that previous integration tests miss because
    their happy path always closes;
  - failed-run quarantine: a poisoned run must not corrupt sibling
    runs in the same loop iteration.

Single test, ~50 iterations + a deliberate failure injection. Runs
in well under 10 s on a warm Postgres.
"""
from __future__ import annotations

import json
import uuid

import pytest

import ods_pipeline


DOMAIN = "insurance"
DATASET = "control_plane_soak"
ITERATIONS = 50


@pytest.fixture
def cleanup(pg_conn):
    def _wipe():
        pg_conn.rollback()
        with pg_conn.cursor() as cur:
            cur.execute(
                "DELETE FROM pipeline.reconciliation_log "
                "WHERE domain=%s AND dataset=%s",
                (DOMAIN, DATASET),
            )
            cur.execute(
                "DELETE FROM pipeline.lineage_edge "
                "WHERE child_run_id IN (SELECT run_id FROM pipeline.run_log "
                "                        WHERE domain=%s AND dataset=%s)",
                (DOMAIN, DATASET),
            )
            cur.execute(
                "DELETE FROM pipeline.run_stage_log "
                "WHERE run_id IN (SELECT run_id FROM pipeline.run_log "
                "                  WHERE domain=%s AND dataset=%s)",
                (DOMAIN, DATASET),
            )
            cur.execute(
                "DELETE FROM pipeline.run_log WHERE domain=%s AND dataset=%s",
                (DOMAIN, DATASET),
            )
            cur.execute(
                "DELETE FROM pipeline.file_catalogue WHERE domain=%s AND dataset=%s",
                (DOMAIN, DATASET),
            )
        pg_conn.commit()
    _wipe()
    yield
    _wipe()


def _drive_one_run(
    pg_conn,
    *,
    iteration: int,
    business_date: str,
    fail: bool = False,
) -> tuple[str, str]:
    """Simulate one ingestion run — start, register file, write lineage,
    write recon, finish. Returns (run_id, file_id)."""
    run_id = str(uuid.uuid4())
    md5 = f"{iteration:032x}"
    s3_path = f"s3://soak/{DATASET}/{iteration:03d}.csv"

    ods_pipeline.runs.start(
        pg_conn,
        run_id=run_id,
        pipeline_type="ingestion",
        domain=DOMAIN,
        dataset=DATASET,
        business_date=business_date,
    )
    file_id = ods_pipeline.files.upsert(
        pg_conn,
        domain=DOMAIN,
        dataset=DATASET,
        business_date=business_date,
        file_md5=md5,
        s3_raw_path=s3_path,
        file_size_bytes=100 * (iteration + 1),
        source_row_count=iteration + 1,
        state="curated",
        last_run_id=run_id,
    )
    ods_pipeline.lineage.write_edge(
        pg_conn,
        child_run_id=run_id,
        parent_file_id=file_id,
        edge_type="raw_to_curated",
        source_ref=s3_path,
        target_ref=f"s3://curated/{DATASET}/{iteration:03d}/",
        record_count=iteration + 1,
    )
    ods_pipeline.reconciliation.write_check(
        pg_conn,
        check_type="t0_publish_count",
        run_id=run_id,
        domain=DOMAIN,
        dataset=DATASET,
        business_date=business_date,
        source_count=iteration + 1,
        kafka_count=iteration + 1,
        status="ok",
        detail=json.dumps({"iteration": iteration}, sort_keys=True),
    )
    if fail:
        ods_pipeline.runs.update(
            pg_conn, run_id, status="failed",
            error_summary="injected failure",
        )
    else:
        ods_pipeline.runs.update(
            pg_conn, run_id, status="succeeded",
            record_count_published=iteration + 1,
        )
    return run_id, file_id


# ---------------------------------------------------------------------------
# Soak
# ---------------------------------------------------------------------------


def test_control_plane_soak_holds_state_across_many_runs(pg_conn, cleanup):
    business_date = "2026-05-06"
    fail_at = ITERATIONS // 2  # inject one failure mid-batch
    run_ids: list[str] = []

    for i in range(ITERATIONS):
        rid, _ = _drive_one_run(
            pg_conn,
            iteration=i,
            business_date=business_date,
            fail=(i == fail_at),
        )
        run_ids.append(rid)

    pg_conn.rollback()
    with pg_conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*), COUNT(*) FILTER (WHERE status='succeeded'), "
            "       COUNT(*) FILTER (WHERE status='failed'), "
            "       COUNT(*) FILTER (WHERE status='running') "
            "  FROM pipeline.run_log WHERE domain=%s AND dataset=%s",
            (DOMAIN, DATASET),
        )
        total, succeeded, failed, running = cur.fetchone()
    assert total == ITERATIONS, total
    assert succeeded == ITERATIONS - 1
    assert failed == 1
    assert running == 0, "no run may be left in the running state"

    # file_catalogue: one row per iteration (distinct s3_raw_path).
    with pg_conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM pipeline.file_catalogue "
            "WHERE domain=%s AND dataset=%s", (DOMAIN, DATASET),
        )
        assert cur.fetchone()[0] == ITERATIONS

    # lineage_edge: one per run.
    with pg_conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM pipeline.lineage_edge "
            "WHERE child_run_id::text = ANY(%s)", (run_ids,),
        )
        assert cur.fetchone()[0] == ITERATIONS

    # reconciliation_log: one per run.
    with pg_conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM pipeline.reconciliation_log "
            "WHERE domain=%s AND dataset=%s", (DOMAIN, DATASET),
        )
        assert cur.fetchone()[0] == ITERATIONS


def test_file_catalogue_idempotent_on_repeat_md5(pg_conn, cleanup):
    """Same (domain, dataset, s3_raw_path) re-upsert returns the SAME
    file_id — no duplicate rows. Critical for replay safety."""
    business_date = "2026-05-06"
    md5 = "a" * 32
    s3_path = "s3://soak/idempotent.csv"

    file_id_1 = ods_pipeline.files.upsert(
        pg_conn,
        domain=DOMAIN, dataset=DATASET, business_date=business_date,
        file_md5=md5, s3_raw_path=s3_path,
        file_size_bytes=100, source_row_count=10,
        state="received",
    )
    file_id_2 = ods_pipeline.files.upsert(
        pg_conn,
        domain=DOMAIN, dataset=DATASET, business_date=business_date,
        file_md5=md5, s3_raw_path=s3_path,
        file_size_bytes=100, source_row_count=10,
        state="curated",  # state changed
    )
    assert file_id_1 == file_id_2

    with pg_conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*), MAX(state) FROM pipeline.file_catalogue "
            "WHERE domain=%s AND dataset=%s AND s3_raw_path=%s",
            (DOMAIN, DATASET, s3_path),
        )
        count, latest_state = cur.fetchone()
    assert count == 1
    assert latest_state == "curated"  # state advances on re-upsert
