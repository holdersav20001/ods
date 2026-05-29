EXPECTED_TABLES = {
    "edge_type", "dataset_config", "file_catalogue", "run_log",
    "run_stage_log", "lineage_link", "lineage_edge",
    "reconciliation_log", "dlq",
}

def test_all_cp_tables_exist(conn):
    rows = conn.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema='cp'"
    ).fetchall()
    present = {r[0] for r in rows}
    assert EXPECTED_TABLES <= present, EXPECTED_TABLES - present
