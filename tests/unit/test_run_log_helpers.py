import os, sys, uuid, pytest

HERE = os.path.dirname(__file__)
sys.path.insert(0, os.path.abspath(os.path.join(HERE, '..', '..')))
import ods_pipeline

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
    ods_pipeline.runs.start(pg_conn, run_id=rid, pipeline_type='orchestration',
                            domain='insurance', dataset='policies',
                            business_date='2026-04-28', file_id=None,
                            config_version_id=1)
    ods_pipeline.runs.update(pg_conn, rid, status='succeeded',
                             record_count_source=10, record_count_target=10,
                             kafka_offset_start=0, kafka_offset_end=10,
                             kafka_topic='ods.insurance.policies')
    with pg_conn.cursor() as cur:
        cur.execute("SELECT status, record_count_target, ended_at FROM pipeline.run_log WHERE run_id=%s", (rid,))
        s, n, ended = cur.fetchone()
    assert s == 'succeeded'
    assert n == 10
    assert ended is not None  # ended_at populated when status moves to terminal

def test_write_stage(pg_conn, isolated_run):
    rid = isolated_run
    ods_pipeline.runs.start(pg_conn, run_id=rid, pipeline_type='orchestration',
                            domain='insurance', dataset='policies',
                            business_date='2026-04-28', file_id=None,
                            config_version_id=1)
    ods_pipeline.stages.write(pg_conn, run_id=rid, stage='ingest', status='succeeded',
                              input_ref='s3://raw/x.csv', output_ref='s3://staging/x.parquet',
                              record_count_in=10, record_count_out=10,
                              metrics={'duration_s': 4.2})
    with pg_conn.cursor() as cur:
        cur.execute("SELECT stage, record_count_out, metrics FROM pipeline.run_stage_log WHERE run_id=%s", (rid,))
        st, n, m = cur.fetchone()
    assert st == 'ingest'
    assert n == 10
    assert m['duration_s'] == 4.2


def test_write_stage_started_keeps_ended_at_null(pg_conn, isolated_run):
    rid = isolated_run
    ods_pipeline.runs.start(pg_conn, run_id=rid, pipeline_type='orchestration',
                            domain='insurance', dataset='policies',
                            business_date='2026-04-28', file_id=None,
                            config_version_id=1)
    ods_pipeline.stages.write(pg_conn, run_id=rid, stage='raw_read',
                              event_type='stage_started', status='running',
                              input_ref='s3://raw/x.csv')
    with pg_conn.cursor() as cur:
        cur.execute(
            "SELECT ended_at FROM pipeline.run_stage_log WHERE run_id=%s AND stage=%s",
            (rid, 'raw_read'),
        )
        (ended,) = cur.fetchone()
    assert ended is None


def test_stage_start_and_finish_closes_open_row(pg_conn, isolated_run):
    rid = isolated_run
    ods_pipeline.runs.start(pg_conn, run_id=rid, pipeline_type='orchestration',
                            domain='insurance', dataset='policies',
                            business_date='2026-04-28', file_id=None,
                            config_version_id=1)
    ods_pipeline.stages.start(pg_conn, run_id=rid, stage='raw_read',
                              input_ref='s3://raw/x.csv')
    with pg_conn.cursor() as cur:
        cur.execute(
            "SELECT status, ended_at FROM pipeline.run_stage_log "
            "WHERE run_id=%s AND stage='raw_read'",
            (rid,),
        )
        status, ended_at = cur.fetchone()
    assert status == 'running'
    assert ended_at is None

    ods_pipeline.stages.finish(pg_conn, run_id=rid, stage='raw_read',
                               status='succeeded',
                               record_count_out=10)
    with pg_conn.cursor() as cur:
        cur.execute(
            "SELECT count(*), max(status), count(*) FILTER (WHERE ended_at IS NULL) "
            "FROM pipeline.run_stage_log WHERE run_id=%s AND stage='raw_read'",
            (rid,),
        )
        count, status, open_count = cur.fetchone()
    assert count == 1
    assert status == 'succeeded'
    assert open_count == 0

def test_write_recon_discrepancy_calculation(pg_conn, isolated_run):
    rid = isolated_run
    ods_pipeline.runs.start(pg_conn, run_id=rid, pipeline_type='orchestration',
                            domain='insurance', dataset='policies',
                            business_date='2026-04-28', file_id=None,
                            config_version_id=1)
    ods_pipeline.reconciliation.write_check(
        pg_conn, check_type='t0_publish_count', run_id=rid,
        domain='insurance', dataset='policies', business_date='2026-04-28',
        source_count=10, accounted_count=9, postgres_count=None,
        status='failed', detail='offset delta < source',
    )
    with pg_conn.cursor() as cur:
        cur.execute("SELECT discrepancy_count, status FROM pipeline.reconciliation_log WHERE run_id=%s ORDER BY id DESC LIMIT 1", (rid,))
        d, s = cur.fetchone()
    assert d == -1
    assert s == 'failed'

def test_update_run_header_rejects_unknown_field(pg_conn, isolated_run):
    rid = isolated_run
    ods_pipeline.runs.start(pg_conn, run_id=rid, pipeline_type='orchestration',
                            domain='insurance', dataset='policies',
                            business_date='2026-04-28', file_id=None,
                            config_version_id=1)
    with pytest.raises(ValueError):
        ods_pipeline.runs.update(pg_conn, rid, bogus_column='x')


def test_start_run_rejects_conflicting_duplicate_metadata(pg_conn, isolated_run):
    rid = isolated_run
    ods_pipeline.runs.start(pg_conn, run_id=rid, pipeline_type='ingestion',
                            domain='insurance', dataset='policies',
                            business_date='2026-04-28', file_id=None,
                            config_version_id=1)
    with pytest.raises(ValueError, match="different metadata"):
        ods_pipeline.runs.start(pg_conn, run_id=rid, pipeline_type='publish',
                                domain='insurance', dataset='policies',
                                business_date='2026-04-28', file_id=None,
                                config_version_id=1)


def test_start_run_allows_matching_duplicate_metadata(pg_conn, isolated_run):
    rid = isolated_run
    kwargs = dict(pipeline_type='ingestion',
                  domain='insurance', dataset='policies',
                  business_date='2026-04-28', file_id=None,
                  config_version_id=1)
    ods_pipeline.runs.start(pg_conn, run_id=rid, **kwargs)
    ods_pipeline.runs.start(pg_conn, run_id=rid, **kwargs)
    with pg_conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM pipeline.run_log WHERE run_id=%s", (rid,))
        (count,) = cur.fetchone()
    assert count == 1


def test_update_run_header_terminal_sets_ended_at_only_for_terminal_status(pg_conn, isolated_run):
    rid = isolated_run
    ods_pipeline.runs.start(pg_conn, run_id=rid, pipeline_type='orchestration',
                            domain='insurance', dataset='policies',
                            business_date='2026-04-28', file_id=None,
                            config_version_id=1)
    # Non-terminal update: ended_at should remain NULL.
    ods_pipeline.runs.update(pg_conn, rid, record_count_source=5)
    with pg_conn.cursor() as cur:
        cur.execute("SELECT ended_at FROM pipeline.run_log WHERE run_id=%s", (rid,))
        (ended,) = cur.fetchone()
    assert ended is None


def test_start_run_allows_null_business_date(pg_conn, isolated_run):
    rid = isolated_run
    ods_pipeline.runs.start(pg_conn, run_id=rid, pipeline_type='publish',
                            domain='insurance', dataset='policies',
                            business_date=None, kafka_topic='ods.insurance.policies')
    with pg_conn.cursor() as cur:
        cur.execute("SELECT business_date FROM pipeline.run_log WHERE run_id=%s", (rid,))
        (business_date,) = cur.fetchone()
    assert business_date is None


def test_start_and_update_run_runtime_context(pg_conn, isolated_run):
    rid = isolated_run
    ods_pipeline.runs.start(
        pg_conn,
        run_id=rid,
        pipeline_type='direct_postgres',
        domain='insurance',
        dataset='policies',
        business_date='2026-04-28',
        runtime_context={
            'platform': 'glue',
            'glue_job_name': 'ods_postgres_write',
            'glue_job_run_id': 'jr_initial',
        },
    )
    ods_pipeline.runs.update(
        pg_conn,
        rid,
        runtime_context={
            'platform': 'glue',
            'glue_job_name': 'ods_postgres_write',
            'glue_job_run_id': 'jr_initial',
            'spark_app_id': 'application_123',
        },
    )
    with pg_conn.cursor() as cur:
        cur.execute("SELECT runtime_context FROM pipeline.run_log WHERE run_id=%s", (rid,))
        (runtime_context,) = cur.fetchone()
    assert runtime_context['glue_job_run_id'] == 'jr_initial'
    assert runtime_context['spark_app_id'] == 'application_123'


def test_start_run_runtime_context_is_not_identity_metadata(pg_conn, isolated_run):
    rid = isolated_run
    kwargs = dict(
        run_id=rid,
        pipeline_type='direct_postgres',
        domain='insurance',
        dataset='policies',
        business_date='2026-04-28',
    )
    ods_pipeline.runs.start(
        pg_conn,
        **kwargs,
        runtime_context={'glue_job_run_id': 'jr_initial'},
    )
    ods_pipeline.runs.start(
        pg_conn,
        **kwargs,
        runtime_context={'glue_job_run_id': 'jr_retry'},
    )
    with pg_conn.cursor() as cur:
        cur.execute("SELECT runtime_context FROM pipeline.run_log WHERE run_id=%s", (rid,))
        (runtime_context,) = cur.fetchone()
    assert runtime_context['glue_job_run_id'] == 'jr_initial'


