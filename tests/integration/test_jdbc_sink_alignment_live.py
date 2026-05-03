"""JDBC connector contracts aligned with live Postgres DDL and Avro schemas."""
from __future__ import annotations

import json
import os
from pathlib import Path

import psycopg2
import pytest


ROOT = Path(__file__).resolve().parents[2]


CONNECTOR_SCHEMA = {
    "ods.insurance.policies": ROOT / "schemas" / "insurance" / "policies.avsc",
    "ods.insurance.risk-canonical": ROOT / "schemas" / "insurance" / "risk_canonical.avsc",
    "ods.insurance.api_pull_demo": ROOT / "schemas" / "insurance" / "api_pull_demo.avsc",
    "ods.insurance.api_pull_risk-canonical": ROOT / "schemas" / "insurance" / "api_pull_risk_canonical.avsc",
}


@pytest.fixture
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


def _connector(path: str) -> dict:
    return json.loads((ROOT / path).read_text(encoding="utf-8"))["config"]


def _schema_fields(path: Path) -> set[str]:
    schema = json.loads(path.read_text(encoding="utf-8"))
    return {field["name"] for field in schema["fields"]}


def _table_columns(pg_conn, table_ref: str) -> set[str]:
    schema, table = table_ref.split(".", 1)
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
              FROM information_schema.columns
             WHERE table_schema = %s
               AND table_name = %s
            """,
            (schema, table),
        )
        return {row[0] for row in cur.fetchall()}


def _primary_key_columns(pg_conn, table_ref: str) -> set[str]:
    schema, table = table_ref.split(".", 1)
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            SELECT kcu.column_name
              FROM information_schema.table_constraints tc
              JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
               AND tc.table_schema = kcu.table_schema
               AND tc.table_name = kcu.table_name
             WHERE tc.constraint_type = 'PRIMARY KEY'
               AND tc.table_schema = %s
               AND tc.table_name = %s
             ORDER BY kcu.ordinal_position
            """,
            (schema, table),
        )
        return {row[0] for row in cur.fetchall()}


@pytest.mark.parametrize(
    "path",
    [
        "docker/connect-config/jdbc-sink-policies.json",
        "docker/connect-config/jdbc-sink-policy-history.json",
        "docker/connect-config/jdbc-sink-risk.json",
        "docker/connect-config/jdbc-sink-api-pull-demo.json",
        "docker/connect-config/jdbc-sink-api-pull-risk.json",
    ],
)
def test_connector_value_schema_fields_exist_in_target_table(pg_conn, path):
    config = _connector(path)
    schema_path = CONNECTOR_SCHEMA[config["topics"]]

    missing = _schema_fields(schema_path) - _table_columns(pg_conn, config["table.name.format"])

    assert not missing, f"{path} fields missing from target table: {sorted(missing)}"


@pytest.mark.parametrize(
    "path",
    [
        "docker/connect-config/jdbc-sink-policies.json",
        "docker/connect-config/jdbc-sink-risk.json",
        "docker/connect-config/jdbc-sink-api-pull-demo.json",
    ],
)
def test_upsert_connector_pk_fields_match_target_primary_key(pg_conn, path):
    config = _connector(path)

    assert config["insert.mode"] == "upsert"
    assert set(config["pk.fields"].split(",")) == _primary_key_columns(
        pg_conn,
        config["table.name.format"],
    )


def test_history_connector_remains_append_only(pg_conn):
    config = _connector("docker/connect-config/jdbc-sink-policy-history.json")

    assert config["insert.mode"] == "insert"
    assert config["pk.mode"] == "none"
    assert _primary_key_columns(pg_conn, config["table.name.format"]) == set()
