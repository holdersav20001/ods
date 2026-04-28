import psycopg2
import pytest

@pytest.fixture(scope="module")
def conn():
    c = psycopg2.connect(
        host="127.0.0.1", port=5440,
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
    # After migration 03, glue_job_log is renamed to glue_job_log_deprecated_2026_04_28
    cur = conn.cursor()
    cur.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema='pipeline' AND table_name='glue_job_log_deprecated_2026_04_28' "
        "AND column_name='config_snapshot'"
    )
    assert cur.fetchone() is not None

def test_file_state_status_constraint(conn):
    # After migration 03, file_state is renamed to file_state_deprecated_2026_04_28
    cur = conn.cursor()
    cur.execute(
        "SELECT check_clause FROM information_schema.check_constraints cc "
        "JOIN information_schema.constraint_column_usage ccu "
        "ON cc.constraint_name = ccu.constraint_name "
        "WHERE ccu.table_schema='pipeline' AND ccu.table_name='file_state_deprecated_2026_04_28' "
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
    # After migration 03, the old file_catalogue is renamed to file_catalogue_deprecated_2026_04_28
    cur = conn.cursor()
    cur.execute(
        "SELECT fc.name_pattern FROM pipeline.file_catalogue_deprecated_2026_04_28 fc "
        "JOIN pipeline.dataset_config dc ON fc.dataset_config_id = dc.id "
        "WHERE dc.dataset='policies'"
    )
    row = cur.fetchone()
    assert row is not None
    assert row[0] == 'policies_*.csv'


# --- New tests for migration 03 + 04 ---

def test_run_log_table_exists(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name, data_type FROM information_schema.columns
            WHERE table_schema='pipeline' AND table_name='run_log'
            ORDER BY ordinal_position
        """)
        cols = {r[0]: r[1] for r in cur.fetchall()}
    assert 'run_id' in cols and cols['run_id'] == 'uuid'
    assert 'pipeline_type' in cols
    assert 'record_count_published' in cols
    assert 'kafka_offset_start' in cols and cols['kafka_offset_start'] == 'bigint'
    assert 'parents' in cols and cols['parents'] == 'jsonb'

def test_run_stage_log_fk(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 1 FROM information_schema.table_constraints
            WHERE table_schema='pipeline' AND table_name='run_stage_log'
              AND constraint_type='FOREIGN KEY'
        """)
        assert cur.fetchone() is not None

def test_reconciliation_log_table_exists(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pipeline.reconciliation_log LIMIT 1")
    assert True

def test_legacy_tables_renamed(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema='pipeline'
              AND table_name LIKE '%_deprecated_2026_04_28'
        """)
        names = {r[0] for r in cur.fetchall()}
    assert 'glue_job_log_deprecated_2026_04_28' in names
    assert 'lineage_deprecated_2026_04_28' in names
    assert 'file_state_deprecated_2026_04_28' in names
    assert 'ingestion_file_state_deprecated_2026_04_28' in names

def test_dataset_config_has_version_columns(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema='pipeline' AND table_name='dataset_config'
        """)
        cols = {r[0] for r in cur.fetchall()}
    assert 'config_version_id' in cols
    assert 'config_yaml_hash' in cols
    assert 'recon_tolerance_records' in cols
    assert 'recon_tolerance_pct' in cols

def test_v_lineage_view_exists(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pipeline.v_lineage LIMIT 1")
    assert True
