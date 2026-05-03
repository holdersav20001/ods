"""Live smoke for the API Pull operations dashboard endpoint.

Uses real Postgres control-plane tables. The dashboard itself is a thin
FastAPI layer over those durable tables, so this test seeds a small
api_pull story and verifies the endpoint surfaces the operator panels.
"""
from __future__ import annotations

import json
import uuid

import psycopg2.extras
import pytest

fastapi = pytest.importorskip("fastapi")
from fastapi.testclient import TestClient  # noqa: E402

import ods_pipeline
from ods_pipeline.ingest.api_pull import TRIGGERED_BY_API_PULL_EDGE, WatermarkStore
from ods_pipeline.models import Stage, StageEvent
from scripts.ops_control_dashboard import app


DOMAIN = "insurance"
DATASET = "api_pull_dashboard_test"
SOURCE_APPLICATION = "dashboard_source"


@pytest.fixture
def dashboard_rows(pg_conn):
    def cleanup():
        pg_conn.rollback()
        with pg_conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM pipeline.run_stage_log
                 WHERE run_id IN (
                     SELECT run_id FROM pipeline.run_log
                      WHERE domain=%s AND dataset=%s
                 )
                """,
                (DOMAIN, DATASET),
            )
            cur.execute(
                """
                DELETE FROM pipeline.lineage_edge
                 WHERE child_run_id IN (
                     SELECT run_id FROM pipeline.run_log
                      WHERE domain=%s AND dataset=%s
                 )
                """,
                (DOMAIN, DATASET),
            )
            cur.execute(
                "DELETE FROM pipeline.reconciliation_log WHERE domain=%s AND dataset=%s",
                (DOMAIN, DATASET),
            )
            cur.execute(
                "DELETE FROM pipeline.run_log WHERE domain=%s AND dataset=%s",
                (DOMAIN, DATASET),
            )
            cur.execute(
                "DELETE FROM pipeline.file_catalogue WHERE domain=%s AND dataset=%s",
                (DOMAIN, DATASET),
            )
            cur.execute(
                """
                DELETE FROM pipeline.api_pull_watermark
                 WHERE domain=%s AND dataset=%s AND source_application=%s
                """,
                (DOMAIN, DATASET, SOURCE_APPLICATION),
            )
            cur.execute(
                "DELETE FROM pipeline.dataset_config WHERE domain=%s AND dataset=%s",
                (DOMAIN, DATASET),
            )
        pg_conn.commit()

    cleanup()

    with pg_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline.dataset_config
                (domain, dataset, filename_pattern, target_topic, schema_id,
                 schema_version, key_fields, active, source_type, raw_format,
                 source_config, canonical_topic, postgres_target_table,
                 config_version_id)
            VALUES (%s,%s,NULL,%s,%s,1,%s,TRUE,'api_pull','jsonl',
                    %s,%s,%s,1)
            ON CONFLICT (domain, dataset) DO UPDATE SET
                source_type = EXCLUDED.source_type,
                raw_format = EXCLUDED.raw_format,
                source_config = EXCLUDED.source_config,
                target_topic = EXCLUDED.target_topic,
                canonical_topic = EXCLUDED.canonical_topic,
                postgres_target_table = EXCLUDED.postgres_target_table,
                active = TRUE
            """,
            (
                DOMAIN,
                DATASET,
                f"ods.{DOMAIN}.{DATASET}",
                f"ods.{DOMAIN}.{DATASET}-value",
                psycopg2.extras.Json(["request_id"]),
                psycopg2.extras.Json(
                    {
                        "application": SOURCE_APPLICATION,
                        "url": "https://example.invalid/items",
                        "auth": {"type": "bearer", "secret_ref": "DASHBOARD_TOKEN"},
                        "cursor": {"style": "since_timestamp"},
                    }
                ),
                f"ods.{DOMAIN}.{DATASET}.canonical",
                f"ods.{DOMAIN}_{DATASET}",
            ),
        )
    pg_conn.commit()

    api_run_id = str(uuid.uuid4())
    downstream_run_id = str(uuid.uuid4())
    ods_pipeline.runs.start(
        pg_conn,
        run_id=api_run_id,
        pipeline_type="api_pull",
        domain=DOMAIN,
        dataset=DATASET,
        business_date="2026-05-03",
        kafka_topic=f"ods.{DOMAIN}.{DATASET}",
    )
    ods_pipeline.runs.update(
        pg_conn,
        api_run_id,
        status="partial",
        record_count_source=3,
        error_summary="downstream dag_ingest ended failed",
    )

    file_id = ods_pipeline.files.upsert(
        pg_conn,
        domain=DOMAIN,
        dataset=DATASET,
        business_date="2026-05-03",
        file_md5="1" * 32,
        s3_raw_path=f"s3://ods-raw-local/api_pull/{DOMAIN}/{DATASET}/run.jsonl.gz",
        file_size_bytes=456,
        source_row_count=3,
        state="received",
        last_run_id=api_run_id,
    )

    ods_pipeline.stages.write(
        pg_conn,
        run_id=api_run_id,
        stage=Stage.MESSAGE_ARCHIVE,
        status="succeeded",
        event_type=StageEvent.COMPLETED,
        output_ref=f"s3://ods-raw-local/api_pull/{DOMAIN}/{DATASET}/run.jsonl.gz",
        record_count_in=3,
        record_count_out=3,
    )
    ods_pipeline.lineage.write_edge(
        pg_conn,
        child_run_id=api_run_id,
        parent_file_id=file_id,
        edge_type="api_to_archive",
        source_ref="https://example.invalid/items",
        target_ref=f"s3://ods-raw-local/api_pull/{DOMAIN}/{DATASET}/run.jsonl.gz",
        record_count=3,
    )
    ods_pipeline.reconciliation.write_check(
        pg_conn,
        check_type="api_pull_archive_count",
        run_id=api_run_id,
        domain=DOMAIN,
        dataset=DATASET,
        business_date="2026-05-03",
        source_count=3,
        kafka_count=None,
        postgres_count=None,
        status="ok",
        detail=json.dumps({"fetched_count": 3, "archived_count": 3}),
    )

    store = WatermarkStore(pg_conn)
    store.read(
        domain=DOMAIN,
        dataset=DATASET,
        source_application=SOURCE_APPLICATION,
        cursor_type="since_timestamp",
    )
    store.record_pending(
        domain=DOMAIN,
        dataset=DATASET,
        source_application=SOURCE_APPLICATION,
        run_id=api_run_id,
        new_cursor_value="2026-05-03T12:00:00Z",
    )

    ods_pipeline.runs.start(
        pg_conn,
        run_id=downstream_run_id,
        pipeline_type="s3_batch",
        domain=DOMAIN,
        dataset=DATASET,
        business_date="2026-05-03",
        file_id=file_id,
        parents=[{"run_id": api_run_id, "edge_type": TRIGGERED_BY_API_PULL_EDGE}],
    )
    ods_pipeline.runs.update(
        pg_conn,
        downstream_run_id,
        status="failed",
        error_summary="synthetic downstream failure",
    )

    yield {
        "api_run_id": api_run_id,
        "downstream_run_id": downstream_run_id,
        "file_id": file_id,
    }

    cleanup()


def test_api_pull_dashboard_endpoint_surfaces_operator_panels(dashboard_rows):
    client = TestClient(app)
    response = client.get(
        "/api/api-pull",
        params={"domain": DOMAIN, "dataset": DATASET, "limit": 20},
    )
    assert response.status_code == 200, response.text
    data = response.json()

    assert data["available"] is True
    assert any(row["dataset"] == DATASET for row in data["datasets"])
    assert any(row["source_application"] == SOURCE_APPLICATION for row in data["watermarks"])
    assert any(row["run_id"] == dashboard_rows["api_run_id"] for row in data["latest_runs"])
    assert any(row["file_id"] == dashboard_rows["file_id"] for row in data["archives"])
    assert any(row["check_type"] == "api_pull_archive_count" for row in data["archive_reconciliation"])
    assert any(row["run_id"] == dashboard_rows["api_run_id"] for row in data["failed_pulls"])
    assert any(
        row["downstream_run_id"] == dashboard_rows["downstream_run_id"]
        for row in data["replay_candidates"]
    )
