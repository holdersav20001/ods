import os
import psycopg2
import pytest

@pytest.fixture(scope="module")
def conn():
    c = psycopg2.connect(
        host=os.environ.get("TEST_PG_HOST", "127.0.0.1"),
        port=int(os.environ.get("TEST_PG_PORT", "5440")),
        dbname=os.environ.get("TEST_PG_DB", "ods_dev"),
        user=os.environ.get("TEST_PG_USER", "ods"),
        password=os.environ.get("TEST_PG_PASSWORD", "ods"),
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
        "WHERE table_schema='pipeline' AND table_name='run_stage_log' "
        "AND column_name='metrics'"
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
        "SELECT dq_rules FROM pipeline.dataset_config WHERE dataset='policies'"
    )
    row = cur.fetchone()
    assert row is not None
    rules = row[0]
    hard_blocks = rules["hard_blocks"]
    assert {"field": "policy_id", "rule": "not_null"} in hard_blocks
    assert {"field": "policy_id", "rule": "unique"} in hard_blocks
    rule_fields = {
        rule.get("field")
        for section in ("hard_blocks", "soft_warns")
        for rule in rules.get(section, [])
        if "field" in rule
    }
    completeness_fields = {
        field
        for rule in rules.get("soft_warns", [])
        for field in rule.get("fields", [])
    }
    assert "premium_amount" not in rule_fields | completeness_fields

def test_file_catalogue_links_to_dataset_config(conn):
    cur = conn.cursor()
    cur.execute(
        "SELECT filename_pattern, postgres_target_table "
        "FROM pipeline.dataset_config "
        "WHERE domain='insurance' AND dataset='policies'"
    )
    row = cur.fetchone()
    assert row is not None
    assert row[0] == r'policies_(?P<bd>\d{8})\.csv'
    assert row[1] == 'ods.insurance_policy'


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
    assert 'orchestrators' in cols and cols['orchestrators'] == 'jsonb'
    assert 'runtime_context' in cols and cols['runtime_context'] == 'jsonb'

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
        cur.execute("""
            SELECT column_name, data_type FROM information_schema.columns
            WHERE table_schema='pipeline' AND table_name='reconciliation_log'
        """)
        cols = {r[0]: r[1] for r in cur.fetchall()}
    assert 'check_type' in cols and cols['check_type'] == 'character varying'
    assert 'discrepancy_count' in cols and cols['discrepancy_count'] == 'bigint'
    assert 'status' in cols and cols['status'] == 'character varying'
    assert 'source_count' in cols and cols['source_count'] == 'bigint'

def test_legacy_tables_renamed(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema='pipeline'
              AND table_name LIKE '%_deprecated_2026_04_28'
        """)
        names = {r[0] for r in cur.fetchall()}
    assert names == set()

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
    assert 'is_canonical' in cols
    assert 'canonical_topic' in cols
    assert 'transform_yaml_path' in cols

def test_v_lineage_view_exists(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 1 FROM information_schema.views
            WHERE table_schema='pipeline' AND table_name='v_lineage'
        """)
        assert cur.fetchone() is not None
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema='pipeline' AND table_name='v_lineage'
        """)
        cols = {r[0] for r in cur.fetchall()}
    assert 'run_id' in cols
    assert 'kafka_topic' in cols
    assert 'source_ref' in cols


def test_control_functions_raise_explicit_validation_errors(conn):
    with conn.cursor() as cur:
        with pytest.raises(psycopg2.Error, match="run_id is required"):
            cur.execute(
                """
                SELECT pipeline.control_start_run(
                    NULL, 'ingestion', 'insurance', 'policies'
                )
                """
            )
        conn.rollback()

    with conn.cursor() as cur:
        with pytest.raises(psycopg2.Error, match="status has invalid value"):
            cur.execute(
                """
                SELECT pipeline.control_update_run(
                    '00000000-0000-0000-0000-000000000001'::uuid,
                    'done'
                )
                """
            )
        conn.rollback()

    with conn.cursor() as cur:
        with pytest.raises(psycopg2.Error, match="source_count must be non-negative"):
            cur.execute(
                """
                SELECT pipeline.control_write_reconciliation_check(
                    't0_publish_count',
                    NULL,
                    'insurance',
                    'policies',
                    NULL,
                    -1,
                    0,
                    0,
                    'ok'
                )
                """
            )
        conn.rollback()


def test_control_start_run_rejects_duplicate_run_metadata(conn):
    run_id = "00000000-0000-0000-0000-000000000033"

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT pipeline.control_start_run(
                %s::uuid, 'ingestion', 'insurance', 'policies'
            )
            """,
            (run_id,),
        )
        cur.execute(
            """
            SELECT pipeline.control_start_run(
                %s::uuid, 'ingestion', 'insurance', 'policies'
            )
            """,
            (run_id,),
        )
        with pytest.raises(psycopg2.Error, match="already exists with different metadata"):
            cur.execute(
                """
                SELECT pipeline.control_start_run(
                    %s::uuid, 'ingestion', 'insurance', 'claims'
                )
                """,
                (run_id,),
            )
        conn.rollback()
