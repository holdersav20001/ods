import os, sys, uuid, pytest
HERE = os.path.dirname(__file__)
sys.path.insert(0, os.path.abspath(os.path.join(HERE, '..', '..', 'airflow', 'dags')))
from common.recon import t0_check_publish

@pytest.fixture
def cleanup_recon(pg_conn):
    rids = []
    yield rids
    try:
        pg_conn.rollback()
    except Exception:
        pass
    if rids:
        with pg_conn.cursor() as cur:
            cur.execute(
                "DELETE FROM pipeline.reconciliation_log WHERE run_id = ANY(%s::uuid[])",
                (rids,),
            )
        pg_conn.commit()

def test_t0_pass(pg_conn, cleanup_recon):
    rid = str(uuid.uuid4())
    cleanup_recon.append(rid)
    res = t0_check_publish(pg_conn, run_id=rid, domain='insurance', dataset='policies',
                           business_date='2026-04-28', source_count=10,
                           kafka_offset_start=100, kafka_offset_end=110)
    assert res.passed is True
    assert res.discrepancy == 0
    assert res.accounted_count == 10
    with pg_conn.cursor() as cur:
        cur.execute("SELECT status FROM pipeline.reconciliation_log WHERE run_id=%s AND check_type='t0_publish_count'", (rid,))
        (s,) = cur.fetchone()
    assert s == 'ok'

def test_t0_fail_writes_failed_recon_row(pg_conn, cleanup_recon):
    rid = str(uuid.uuid4())
    cleanup_recon.append(rid)
    res = t0_check_publish(pg_conn, run_id=rid, domain='insurance', dataset='policies',
                           business_date='2026-04-28', source_count=10,
                           kafka_offset_start=100, kafka_offset_end=109)
    assert res.passed is False
    assert res.discrepancy == -1
    with pg_conn.cursor() as cur:
        cur.execute("SELECT status, discrepancy_count, detail FROM pipeline.reconciliation_log WHERE run_id=%s AND check_type='t0_publish_count'", (rid,))
        s, d, detail = cur.fetchone()
    assert s == 'failed'
    assert d == -1
    assert 'discrepancy=-1' in detail
