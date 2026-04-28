import os, sys, uuid, pytest

HERE = os.path.dirname(__file__)
sys.path.insert(0, os.path.abspath(os.path.join(HERE, '..', '..', 'airflow', 'dags')))
from common.run_log import (
    insert_run_header, update_run_header, write_stage, write_recon
)

@pytest.fixture
def isolated_run(pg_conn):
    """Create a run, yield its run_id, clean up after."""
    run_id = str(uuid.uuid4())
    yield run_id
    # rollback any failed-transaction state before cleanup
    try:
        pg_conn.rollback()
    except Exception:
        pass
    # cleanup: delete dependent rows then header
    with pg_conn.cursor() as cur:
        cur.execute("DELETE FROM pipeline.reconciliation_log WHERE run_id=%s", (run_id,))
        cur.execute("DELETE FROM pipeline.run_stage_log WHERE run_id=%s", (run_id,))
        cur.execute("DELETE FROM pipeline.run_log WHERE run_id=%s", (run_id,))
    pg_conn.commit()

def test_insert_run_header_and_update(pg_conn, isolated_run):
    rid = isolated_run
    insert_run_header(pg_conn, run_id=rid, pipeline_type='s3_batch',
                      domain='insurance', dataset='policies',
                      business_date='2026-04-28', file_id=None,
                      config_version_id=1)
    update_run_header(pg_conn, rid, status='succeeded',
                      record_count_source=10, record_count_published=10,
                      kafka_offset_start=0, kafka_offset_end=10,
                      kafka_topic='ods.insurance.policies')
    with pg_conn.cursor() as cur:
        cur.execute("SELECT status, record_count_published, ended_at FROM pipeline.run_log WHERE run_id=%s", (rid,))
        s, n, ended = cur.fetchone()
    assert s == 'succeeded'
    assert n == 10
    assert ended is not None  # ended_at populated when status moves to terminal

def test_write_stage(pg_conn, isolated_run):
    rid = isolated_run
    insert_run_header(pg_conn, run_id=rid, pipeline_type='s3_batch',
                      domain='insurance', dataset='policies',
                      business_date='2026-04-28', file_id=None,
                      config_version_id=1)
    write_stage(pg_conn, run_id=rid, stage='ingest', status='succeeded',
                input_ref='s3://raw/x.csv', output_ref='s3://staging/x.parquet',
                record_count_in=10, record_count_out=10, metrics={'duration_s': 4.2})
    with pg_conn.cursor() as cur:
        cur.execute("SELECT stage, record_count_out, metrics FROM pipeline.run_stage_log WHERE run_id=%s", (rid,))
        st, n, m = cur.fetchone()
    assert st == 'ingest'
    assert n == 10
    assert m['duration_s'] == 4.2

def test_write_recon_discrepancy_calculation(pg_conn, isolated_run):
    rid = isolated_run
    insert_run_header(pg_conn, run_id=rid, pipeline_type='s3_batch',
                      domain='insurance', dataset='policies',
                      business_date='2026-04-28', file_id=None,
                      config_version_id=1)
    write_recon(pg_conn, check_type='t0_publish_count', run_id=rid,
                domain='insurance', dataset='policies', business_date='2026-04-28',
                source_count=10, kafka_count=9, postgres_count=None,
                status='failed', detail='offset delta < source')
    with pg_conn.cursor() as cur:
        cur.execute("SELECT discrepancy_count, status FROM pipeline.reconciliation_log WHERE run_id=%s ORDER BY id DESC LIMIT 1", (rid,))
        d, s = cur.fetchone()
    assert d == -1
    assert s == 'failed'

def test_update_run_header_rejects_unknown_field(pg_conn, isolated_run):
    rid = isolated_run
    insert_run_header(pg_conn, run_id=rid, pipeline_type='s3_batch',
                      domain='insurance', dataset='policies',
                      business_date='2026-04-28', file_id=None,
                      config_version_id=1)
    with pytest.raises(ValueError):
        update_run_header(pg_conn, rid, bogus_column='x')


def test_update_run_header_terminal_sets_ended_at_only_for_terminal_status(pg_conn, isolated_run):
    rid = isolated_run
    insert_run_header(pg_conn, run_id=rid, pipeline_type='s3_batch',
                      domain='insurance', dataset='policies',
                      business_date='2026-04-28', file_id=None,
                      config_version_id=1)
    # Non-terminal update: ended_at should remain NULL.
    update_run_header(pg_conn, rid, record_count_source=5)
    with pg_conn.cursor() as cur:
        cur.execute("SELECT ended_at FROM pipeline.run_log WHERE run_id=%s", (rid,))
        (ended,) = cur.fetchone()
    assert ended is None
