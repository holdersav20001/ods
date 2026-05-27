import pytest

import ods_ingestion_control as control


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        self.conn.statements.append((sql, params))
        if self.conn.raise_on_execute:
            raise RuntimeError("database said no")

    def fetchone(self):
        return (self.conn.result,)


class FakeConn:
    def __init__(self, result="ok"):
        self.result = result
        self.statements = []
        self.commits = 0
        self.rollbacks = 0
        self.raise_on_execute = False

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


def _last_sql(conn):
    return conn.statements[-1][0]


def test_start_run_calls_database_function_and_commits():
    conn = FakeConn(result="run-1")

    result = control.start_run(
        conn,
        run_id="run-1",
        pipeline_type="ingestion",
        domain="insurance",
        dataset="policies",
        business_date="2026-05-23",
        parents=[{"run_id": "parent", "edge_type": "orchestrates"}],
    )

    assert result == "run-1"
    assert "pipeline.control_start_run" in _last_sql(conn)
    assert conn.commits == 1
    assert conn.rollbacks == 0


def test_finish_stage_routes_to_finish_stage_function_without_commit_when_requested():
    conn = FakeConn(result=42)

    result = control.finish_stage(
        conn,
        run_id="run-1",
        stage="raw_read",
        status="succeeded",
        attempt_number=2,
        metrics={"records": 10},
        commit=False,
    )

    assert result == 42
    assert "pipeline.control_finish_stage" in _last_sql(conn)
    assert conn.commits == 0
    assert conn.rollbacks == 0


@pytest.mark.parametrize(
    ("func", "kwargs", "function_name"),
    [
        (
            control.update_run,
            {"run_id": "run-1", "status": "succeeded"},
            "pipeline.control_update_run",
        ),
        (
            control.register_file,
            {
                "domain": "insurance",
                "dataset": "policies",
                "business_date": "2026-05-23",
                "file_md5": "a" * 32,
                "s3_raw_path": "s3://raw/file.csv",
            },
            "pipeline.control_register_file",
        ),
        (
            control.update_file_catalogue,
            {"s3_raw_path": "s3://raw/file.csv", "state": "completed"},
            "pipeline.control_update_file_catalogue",
        ),
        (
            control.set_file_state,
            {
                "s3_path": "s3://raw/file.csv",
                "run_id": "run-1",
                "status": "completed",
            },
            "pipeline.control_set_file_state",
        ),
        (
            control.start_stage,
            {"run_id": "run-1", "stage": "raw_read"},
            "pipeline.control_start_stage",
        ),
        (
            control.write_lineage_edge,
            {
                "child_run_id": "run-1",
                "edge_type": "raw_to_curated",
                "parent_file_id": "file-1",
            },
            "pipeline.control_write_lineage_edge",
        ),
        (
            control.write_reconciliation_check,
            {
                "check_type": "t0_ingestion_count",
                "run_id": "run-1",
                "domain": "insurance",
                "dataset": "policies",
                "business_date": "2026-05-23",
            },
            "pipeline.control_write_reconciliation_check",
        ),
        (
            control.record_run_event,
            {
                "run_id": "run-1",
                "event_type": "ingestion.completed",
                "domain": "insurance",
                "dataset": "policies",
                "business_date": "2026-05-23",
                "status": "succeeded",
            },
            "pipeline.control_record_run_event",
        ),
    ],
)
def test_public_functions_route_to_control_database_functions(func, kwargs, function_name):
    conn = FakeConn(result="ok")

    func(conn, **kwargs)

    assert function_name in _last_sql(conn)
    assert conn.commits == 1


def test_database_error_rolls_back_when_function_owns_transaction():
    conn = FakeConn()
    conn.raise_on_execute = True

    with pytest.raises(RuntimeError, match="database said no"):
        control.update_run(conn, run_id="run-1", status="failed")

    assert conn.commits == 0
    assert conn.rollbacks == 1


def test_control_function_migration_contains_expected_api():
    with open("db/migrations/33_control_table_functions.sql", encoding="utf-8") as f:
        sql = f.read()

    for name in [
        "control_start_run",
        "control_update_run",
        "control_register_file",
        "control_update_file_catalogue",
        "control_set_file_state",
        "control_start_stage",
        "control_finish_stage",
        "control_write_lineage_edge",
        "control_write_reconciliation_check",
        "control_record_run_event",
    ]:
        assert f"FUNCTION pipeline.{name}" in sql
    assert "SECURITY DEFINER" in sql
    assert "FUNCTION pipeline.control_require_text" in sql
    assert "FUNCTION pipeline.control_assert_allowed" in sql
    assert "FUNCTION pipeline.control_assert_nonnegative" in sql
    assert "run_id is required" in sql
    assert "already exists with different metadata" in sql
    assert "GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA pipeline" in sql
