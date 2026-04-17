import psycopg2
import pytest

@pytest.fixture(scope="module")
def conn():
    c = psycopg2.connect(
        host="localhost", port=5432,
        dbname="ods_dev", user="ods", password="ods"
    )
    yield c
    c.close()

def test_pipeline_schema_exists(conn):
    cur = conn.cursor()
    cur.execute(
        "SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'pipeline'"
    )
    assert cur.fetchone() is not None

def test_dataset_config_table_exists(conn):
    cur = conn.cursor()
    cur.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema='pipeline' AND table_name='dataset_config'"
    )
    assert cur.fetchone() is not None

def test_glue_job_log_has_config_snapshot(conn):
    cur = conn.cursor()
    cur.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema='pipeline' AND table_name='glue_job_log' "
        "AND column_name='config_snapshot'"
    )
    assert cur.fetchone() is not None

def test_file_state_status_constraint(conn):
    cur = conn.cursor()
    cur.execute(
        "SELECT check_clause FROM information_schema.check_constraints cc "
        "JOIN information_schema.constraint_column_usage ccu "
        "ON cc.constraint_name = ccu.constraint_name "
        "WHERE ccu.table_schema='pipeline' AND ccu.table_name='file_state' "
        "AND ccu.column_name='status'"
    )
    row = cur.fetchone()
    assert row is not None

def test_policies_seed_exists(conn):
    cur = conn.cursor()
    cur.execute(
        "SELECT dataset, target_topic FROM pipeline.dataset_config WHERE dataset='policies'"
    )
    row = cur.fetchone()
    assert row is not None
    assert row[0] == 'policies'
    assert row[1] == 'ods.insurance.policies'

def test_policies_dq_rules_have_hard_blocks(conn):
    cur = conn.cursor()
    cur.execute(
        "SELECT dq_rules->'hard_blocks' FROM pipeline.dataset_config WHERE dataset='policies'"
    )
    row = cur.fetchone()
    assert row is not None
    assert len(row[0]) >= 4

def test_file_catalogue_links_to_dataset_config(conn):
    cur = conn.cursor()
    cur.execute(
        "SELECT fc.name_pattern FROM pipeline.file_catalogue fc "
        "JOIN pipeline.dataset_config dc ON fc.dataset_config_id = dc.id "
        "WHERE dc.dataset='policies'"
    )
    row = cur.fetchone()
    assert row is not None
    assert row[0] == 'policies_*.csv'
