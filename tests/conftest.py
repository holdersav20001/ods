import psycopg2
import pytest

@pytest.fixture(scope="session")
def pg_conn():
    conn = psycopg2.connect(
        host="127.0.0.1", port=5440,
        dbname="ods_dev", user="ods", password="ods"
    )
    yield conn
    conn.close()
