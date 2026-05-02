"""Event API service (T16 / 8.4) — message/API IngestionPattern demo."""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

fastapi = pytest.importorskip("fastapi")
from fastapi.testclient import TestClient

from services.event_api.main import build_app


@pytest.fixture
def app(monkeypatch):
    """Build app with mocked S3 + Postgres + Kafka so we run pure-unit.

    Uses monkeypatch.setattr so global state is auto-restored even on
    test interruption — avoids leaking the start_run mock into other
    test modules that import ods_pipeline.messages.
    """
    pg = MagicMock()
    cur = MagicMock()
    cur_cm = MagicMock()
    cur_cm.__enter__ = MagicMock(return_value=cur)
    cur_cm.__exit__ = MagicMock(return_value=False)
    pg.cursor.return_value = cur_cm
    cur.fetchone.return_value = ("abc",)

    s3 = MagicMock()
    producer = MagicMock()
    factory_calls = {"pg": 0, "producer": 0}

    def pg_factory():
        factory_calls["pg"] += 1
        return pg

    def producer_factory():
        factory_calls["producer"] += 1
        return producer

    import ods_pipeline.messages as messages
    start_run_mock = MagicMock()
    monkeypatch.setattr(messages, "start_run", start_run_mock)

    app_obj = build_app(
        pg_factory=pg_factory,
        s3_client=s3,
        producer_factory=producer_factory,
        archive_bucket="ods-test-bucket",
    )
    yield app_obj, s3, producer, start_run_mock, factory_calls


def test_healthz_returns_pattern_name(app):
    app_obj, *_ = app
    client = TestClient(app_obj)
    r = client.get("/healthz")
    assert r.status_code == 200
    assert r.json() == {"status": "ok", "pattern": "insurance.event_demo"}


def test_post_events_archives_and_publishes(app):
    app_obj, s3, producer, start_run, _ = app
    client = TestClient(app_obj)

    r = client.post("/events", json={
        "event_id": "evt-1",
        "business_date": "2026-05-02",
        "payload": {"policy_id": "P-1", "premium": 100.5},
    })

    assert r.status_code == 200, r.text
    body = r.json()
    assert body["accepted"] is True
    assert body["pattern"] == "insurance.event_demo"
    assert body["event_id"] == "evt-1"
    assert body["archive_uri"].startswith("s3://ods-test-bucket/raw/event/insurance/event_demo/2026-05-02/evt-1.jsonl")

    s3.put_object.assert_called_once()
    s3kw = s3.put_object.call_args.kwargs
    assert s3kw["Bucket"] == "ods-test-bucket"
    assert s3kw["Key"].endswith("evt-1.jsonl")

    start_run.assert_called_once()
    sk = start_run.call_args.kwargs
    assert sk["domain"] == "insurance"
    assert sk["dataset"] == "event_demo"
    assert sk["correlation"] == {"_ods_source_event_id": "evt-1"}
    assert sk["kafka_topic"] == "ods.insurance.events"

    producer.produce.assert_called_once()
    pkw = producer.produce.call_args.kwargs
    assert pkw["topic"] == "ods.insurance.events"
    producer.flush.assert_called_once()


def test_post_events_synthesises_event_id_when_missing(app):
    app_obj, *_ = app
    client = TestClient(app_obj)

    r = client.post("/events", json={"payload": {"x": 1}})
    assert r.status_code == 200
    assert len(r.json()["event_id"]) == 36  # uuid


def test_post_events_returns_502_when_s3_fails(app):
    app_obj, s3, producer, start_run, _ = app
    s3.put_object.side_effect = RuntimeError("s3 down")
    client = TestClient(app_obj)

    r = client.post("/events", json={"event_id": "evt-x", "payload": {}})
    assert r.status_code == 502
    assert "S3 archive failed" in r.json()["detail"]
    start_run.assert_not_called()
    producer.produce.assert_not_called()
