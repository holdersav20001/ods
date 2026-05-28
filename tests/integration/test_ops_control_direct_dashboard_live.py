"""Live smoke for the Direct Postgres + Direct Kafka dashboard tabs.

Seeds minimal control-plane data for one ``delivery='direct_postgres'``
dataset and one ``delivery='direct_kafka'`` dataset, then hits the new
FastAPI routes with TestClient and asserts the operator panels surface
the seeded rows.

The Direct Kafka sink-lag panel needs a Kafka broker; that part is
guarded with ``include_sink_lag=false`` so the test is robust whether
or not a broker is reachable.
"""
from __future__ import annotations

import json
import uuid
from datetime import date, datetime, timezone

import psycopg2.extras
import pytest

fastapi = pytest.importorskip("fastapi")
from fastapi.testclient import TestClient  # noqa: E402

from scripts.ops_control_dashboard import app  # noqa: E402


DOMAIN = "ops_dash_direct"
DATASET_PG = "direct_pg_panel"
DATASET_KAFKA = "direct_kafka_panel"
SOURCE_APPLICATION = "ops_dash_direct_source"


def _has_column(cur, table: str, column: str) -> bool:
    cur.execute(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'pipeline'
          AND table_name = %s
          AND column_name = %s
        """,
        (table, column),
    )
    return cur.fetchone() is not None


def _cleanup(pg_conn):
    pg_conn.rollback()
    with pg_conn.cursor() as cur:
        for ds in (DATASET_PG, DATASET_KAFKA):
            cur.execute(
                """
                DELETE FROM pipeline.run_stage_log
                 WHERE run_id IN (
                     SELECT run_id FROM pipeline.run_log
                      WHERE domain=%s AND dataset=%s
                 )
                """,
                (DOMAIN, ds),
            )
            cur.execute(
                "DELETE FROM pipeline.reconciliation_log WHERE domain=%s AND dataset=%s",
                (DOMAIN, ds),
            )
            cur.execute(
                "DELETE FROM pipeline.run_log WHERE domain=%s AND dataset=%s",
                (DOMAIN, ds),
            )
            cur.execute(
                """
                DELETE FROM pipeline.api_pull_watermark
                 WHERE domain=%s AND dataset=%s
                """,
                (DOMAIN, ds),
            )
            cur.execute(
                "DELETE FROM pipeline.dataset_config WHERE domain=%s AND dataset=%s",
                (DOMAIN, ds),
            )
    pg_conn.commit()


@pytest.fixture
def direct_dashboard_rows(pg_conn):
    _cleanup(pg_conn)

    # Discover dataset_config column shape (it has been extended over migrations).
    with pg_conn.cursor() as cur:
        has_source_type = _has_column(cur, "dataset_config", "source_type")
        has_raw_format = _has_column(cur, "dataset_config", "raw_format")
        has_source_config = _has_column(cur, "dataset_config", "source_config")
        has_pg_target = _has_column(cur, "dataset_config", "postgres_target_table")
        has_canonical = _has_column(cur, "dataset_config", "canonical_topic")
        has_config_version = _has_column(cur, "dataset_config", "config_version_id")

    # Build dynamic INSERT for both datasets.
    base_cols = [
        "domain", "dataset", "filename_pattern", "target_topic",
        "schema_id", "schema_version", "key_fields", "active", "delivery",
    ]
    if has_source_type:
        base_cols.append("source_type")
    if has_raw_format:
        base_cols.append("raw_format")
    if has_source_config:
        base_cols.append("source_config")
    if has_pg_target:
        base_cols.append("postgres_target_table")
    if has_canonical:
        base_cols.append("canonical_topic")
    if has_config_version:
        base_cols.append("config_version_id")

    placeholders = ",".join(["%s"] * len(base_cols))
    insert_sql = (
        f"INSERT INTO pipeline.dataset_config ({', '.join(base_cols)}) "
        f"VALUES ({placeholders}) "
        "ON CONFLICT (domain, dataset) DO NOTHING"
    )

    def _row_for(dataset: str, delivery: str, source_type: str, postgres_table: str | None):
        row = [
            DOMAIN, dataset, "data_*.parquet",
            f"ods.{DOMAIN}.{dataset}",
            f"ods.{DOMAIN}.{dataset}-value",
            1,
            psycopg2.extras.Json(["request_id"]),
            True,
            delivery,
        ]
        if has_source_type:
            row.append(source_type)
        if has_raw_format:
            row.append("jsonl" if source_type == "api_pull" else "parquet")
        if has_source_config:
            sc = (
                {
                    "application": SOURCE_APPLICATION,
                    "url": "https://example.invalid/items",
                    "auth": {"type": "bearer", "secret_ref": "DASH_TOKEN"},
                    "cursor": {"style": "since_timestamp"},
                }
                if source_type == "api_pull"
                else {"application": SOURCE_APPLICATION}
            )
            row.append(psycopg2.extras.Json(sc))
        if has_pg_target:
            row.append(postgres_table)
        if has_canonical:
            row.append(f"ods.{DOMAIN}.{dataset}.canonical")
        if has_config_version:
            row.append(1)
        return row

    pg_run_id = str(uuid.uuid4())
    kafka_run_id = str(uuid.uuid4())

    with pg_conn.cursor() as cur:
        cur.execute(
            insert_sql,
            _row_for(DATASET_PG, "direct_postgres", "s3_batch", f"ods_{DOMAIN}_{DATASET_PG}"),
        )
        cur.execute(
            insert_sql,
            _row_for(DATASET_KAFKA, "direct_kafka", "api_pull", None),
        )

        # direct_postgres run_log row.
        cur.execute(
            """
            INSERT INTO pipeline.run_log
                (run_id, pipeline_type, domain, dataset, business_date, status,
                 started_at, ended_at, record_count_source, record_count_target,
                 error_summary)
            VALUES (%s, 'direct_postgres', %s, %s, %s, 'succeeded',
                    NOW() - INTERVAL '5 minutes', NOW() - INTERVAL '4 minutes',
                    7, 7, NULL)
            """,
            (pg_run_id, DOMAIN, DATASET_PG, date.today()),
        )
        # A failed direct_postgres run in the last 24h.
        failed_pg = str(uuid.uuid4())
        cur.execute(
            """
            INSERT INTO pipeline.run_log
                (run_id, pipeline_type, domain, dataset, business_date, status,
                 started_at, ended_at, error_summary)
            VALUES (%s, 'direct_postgres', %s, %s, %s, 'failed',
                    NOW() - INTERVAL '2 hours', NOW() - INTERVAL '2 hours',
                    'synthetic direct_postgres failure')
            """,
            (failed_pg, DOMAIN, DATASET_PG, date.today()),
        )
        # Add a curated_write stage row for the succeeded direct_postgres run.
        curated_path = f"s3://ods-curated-local/{DOMAIN}/{DATASET_PG}/run.parquet"
        cur.execute(
            """
            INSERT INTO pipeline.run_stage_log
                (run_id, stage, status, started_at, ended_at, output_ref)
            VALUES (%s, 'curated_write', 'succeeded',
                    NOW() - INTERVAL '5 minutes', NOW() - INTERVAL '4 minutes', %s)
            """,
            (pg_run_id, curated_path),
        )

        # direct_postgres reconciliation row.
        cur.execute(
            """
            INSERT INTO pipeline.reconciliation_log
                (check_type, run_id, domain, dataset, business_date,
                 source_count, postgres_count, discrepancy_count, status, detail)
            VALUES ('direct_postgres_count', %s, %s, %s, %s, 7, 7, 0, 'ok', %s)
            """,
            (
                pg_run_id, DOMAIN, DATASET_PG, date.today(),
                json.dumps({"note": "ops_dash_seed"}),
            ),
        )

        # direct_kafka api_pull run_log row.
        cur.execute(
            """
            INSERT INTO pipeline.run_log
                (run_id, pipeline_type, domain, dataset, business_date, status,
                 started_at, ended_at, record_count_source, record_count_target,
                 kafka_topic, kafka_offset_start, kafka_offset_end, error_summary)
            VALUES (%s, 'api_pull', %s, %s, %s, 'succeeded',
                    NOW() - INTERVAL '6 minutes', NOW() - INTERVAL '5 minutes',
                    11, 11, %s, 0, 11, NULL)
            """,
            (
                kafka_run_id, DOMAIN, DATASET_KAFKA, date.today(),
                f"ods.{DOMAIN}.{DATASET_KAFKA}",
            ),
        )
        # kafka_publish stage with offset_end_by_partition metric.
        cur.execute(
            """
            INSERT INTO pipeline.run_stage_log
                (run_id, stage, status, started_at, ended_at,
                 record_count_in, record_count_out, metrics)
            VALUES (%s, 'kafka_publish', 'succeeded',
                    NOW() - INTERVAL '6 minutes', NOW() - INTERVAL '5 minutes',
                    11, 11, %s)
            """,
            (
                kafka_run_id,
                psycopg2.extras.Json({"offset_end_by_partition": {"0": 11}}),
            ),
        )

        # direct_kafka watermark row.
        cur.execute(
            """
            INSERT INTO pipeline.api_pull_watermark
                (domain, dataset, source_application, cursor_type,
                 committed_cursor_value, pending_cursor_value, updated_at)
            VALUES (%s, %s, %s, 'since_timestamp',
                    '2026-05-02T11:00:00Z', NULL, NOW() - INTERVAL '90 seconds')
            """,
            (DOMAIN, DATASET_KAFKA, SOURCE_APPLICATION),
        )

        # api_pull_publish_count reconciliation row.
        cur.execute(
            """
            INSERT INTO pipeline.reconciliation_log
                (check_type, run_id, domain, dataset, business_date,
                 source_count, accounted_count, discrepancy_count, status, detail)
            VALUES ('api_pull_publish_count', %s, %s, %s, %s, 11, 11, 0, 'ok', %s)
            """,
            (
                kafka_run_id, DOMAIN, DATASET_KAFKA, date.today(),
                json.dumps({"note": "ops_dash_seed"}),
            ),
        )

    pg_conn.commit()

    yield {
        "pg_run_id": pg_run_id,
        "kafka_run_id": kafka_run_id,
        "curated_path": curated_path,
    }

    _cleanup(pg_conn)


def test_direct_postgres_dashboard_endpoint(direct_dashboard_rows):
    client = TestClient(app)
    response = client.get(
        "/api/direct-postgres",
        params={"domain": DOMAIN, "dataset": DATASET_PG, "limit": 25},
    )
    assert response.status_code == 200, response.text
    data = response.json()

    assert data["available"] is True
    assert data["delivery"] == "direct_postgres"
    assert any(d["dataset"] == DATASET_PG for d in data["datasets"])
    assert any(r["run_id"] == direct_dashboard_rows["pg_run_id"] for r in data["latest_runs"])
    assert any(
        rec["check_type"] == "direct_postgres_count"
        and rec["source_count"] == 7
        and rec["postgres_count"] == 7
        for rec in data["reconciliations"]
    )
    assert any(
        cur["curated_parquet_path"] == direct_dashboard_rows["curated_path"]
        for cur in data["curated_files"]
    )
    assert any(r["status"] == "failed" for r in data["failed_recent"])


def test_direct_postgres_dashboard_html_navigation_links():
    client = TestClient(app)
    home = client.get("/")
    assert home.status_code == 200
    body = home.text
    assert "showTab('direct-postgres')" in body
    assert "showTab('direct-kafka')" in body
    assert "Direct Postgres" in body
    assert "Direct Kafka" in body


def test_direct_kafka_dashboard_endpoint(direct_dashboard_rows):
    client = TestClient(app)
    # Disable the sink-lag query so the test does not require a broker.
    response = client.get(
        "/api/direct-kafka",
        params={
            "domain": DOMAIN,
            "dataset": DATASET_KAFKA,
            "include_sink_lag": "false",
            "limit": 25,
        },
    )
    assert response.status_code == 200, response.text
    data = response.json()

    assert data["available"] is True
    assert data["delivery"] == "direct_kafka"
    assert any(d["dataset"] == DATASET_KAFKA for d in data["datasets"])
    assert any(r["run_id"] == direct_dashboard_rows["kafka_run_id"] for r in data["latest_runs"])
    assert any(
        w["dataset"] == DATASET_KAFKA and w["source_application"] == SOURCE_APPLICATION
        for w in data["watermarks"]
    )
    assert any(
        rec["check_type"] == "api_pull_publish_count"
        and rec["accounted_count"] == 11
        for rec in data["reconciliations"]
    )
    # sink_lag panel was disabled -> empty list, not absent.
    assert data["sink_lag"] == []


def test_direct_kafka_sink_lag_skips_when_kafka_unavailable(direct_dashboard_rows):
    """Sink-lag panel is best-effort: it must not 503 when broker is down."""
    try:
        from confluent_kafka import Consumer  # noqa: F401
    except Exception:
        pytest.skip("confluent_kafka not importable; sink-lag panel cannot run")

    client = TestClient(app)
    response = client.get(
        "/api/direct-kafka",
        params={
            "domain": DOMAIN,
            "dataset": DATASET_KAFKA,
            "include_sink_lag": "true",
            "limit": 25,
        },
    )
    # Whether or not Kafka is up, the endpoint itself must succeed.
    assert response.status_code == 200, response.text
    data = response.json()
    # If a broker isn't reachable each entry will carry available=false,
    # otherwise we expect at least the consumer_group naming to round-trip.
    for entry in data.get("sink_lag", []):
        assert entry["dataset"] == DATASET_KAFKA
        if entry.get("available") is True:
            assert entry["consumer_group"].startswith("connect-jdbc-sink-")
