"""Top-level pytest config — DB fixture + auto-markers by path."""
import os
from pathlib import Path

import psycopg2
import pytest


@pytest.fixture(scope="session")
def pg_conn():
    conn = psycopg2.connect(
        host=os.environ.get("TEST_PG_HOST", "127.0.0.1"),
        port=int(os.environ.get("TEST_PG_PORT", "5440")),
        dbname=os.environ.get("TEST_PG_DB", "ods_dev"),
        user=os.environ.get("TEST_PG_USER", "ods"),
        password=os.environ.get("TEST_PG_PASSWORD", "ods"),
    )
    yield conn
    conn.close()


def pytest_collection_modifyitems(config, items):
    """Auto-apply markers based on path so tests don't need per-file decorators.

    - tests/unit/*  -> @pytest.mark.unit
    - tests/integration/*  -> @pytest.mark.integration
    - tests/e2e/*  -> @pytest.mark.e2e
    """
    tests_root = Path(__file__).parent.resolve()
    for item in items:
        item_path = Path(str(item.fspath)).resolve()
        try:
            relative = item_path.relative_to(tests_root)
        except ValueError:
            continue
        parts = relative.parts
        if not parts:
            continue
        top = parts[0]
        if top == "unit":
            item.add_marker(pytest.mark.unit)
        elif top == "integration":
            item.add_marker(pytest.mark.integration)
        elif top == "e2e":
            item.add_marker(pytest.mark.e2e)
