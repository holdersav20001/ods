import psycopg2
import pytest

@pytest.fixture(scope="session")
def pg_conn():
    conn = psycopg2.connect(
        host="localhost", port=5432,
        dbname="ods_dev", user="ods", password="ods"
    )
    yield conn
    conn.close()
