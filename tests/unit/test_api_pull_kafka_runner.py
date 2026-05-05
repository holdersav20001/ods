"""Unit tests for ods_pipeline.ingest.api_pull_kafka.run_once.

Drives the runner with a fake HTTP session and a fake idempotent
producer so we exercise the envelope shape, cursor advance, and
offset-window capture without Kafka or Schema Registry.
"""
from __future__ import annotations

import json
import uuid

import pytest

from ods_pipeline.ingest.api_pull_kafka import (
    PublishedBatch,
    build_envelope,
    run_once,
)


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class _FakeResponse:
    def __init__(self, body, headers=None, status_code=200):
        self._body = body
        self.headers = headers or {}
        self.status_code = status_code
        self.content = (
            json.dumps(body).encode("utf-8") if body is not None else b""
        )

    def json(self):
        return self._body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class _FakeSession:
    def __init__(self, script):
        self._script = list(script)
        self.calls: list[tuple[str, dict]] = []

    def get(self, url, params=None):
        self.calls.append((url, dict(params or {})))
        if not self._script:
            raise AssertionError(f"no scripted response for {url}?{params}")
        return self._script.pop(0)


class _FakeMessage:
    def __init__(self, partition: int, offset: int):
        self._p = partition
        self._o = offset

    def partition(self):
        return self._p

    def offset(self):
        return self._o


class _FakeProducer:
    """Mimics confluent_kafka.SerializingProducer enough for the runner."""

    def __init__(self, *, partitions: int = 1, fail_after: int | None = None):
        self.produced: list[dict] = []
        self._partitions = partitions
        self._fail_after = fail_after
        self._next_offset = {p: 0 for p in range(partitions)}

    def produce(self, *, topic, key, value, on_delivery):
        idx = len(self.produced)
        if self._fail_after is not None and idx >= self._fail_after:
            on_delivery(RuntimeError("simulated delivery failure"), None)
            self.produced.append(value)
            return
        partition = idx % self._partitions
        offset = self._next_offset[partition]
        self._next_offset[partition] += 1
        self.produced.append(value)
        on_delivery(None, _FakeMessage(partition, offset))

    def flush(self, timeout=30):
        return 0  # all delivered synchronously above


# ---------------------------------------------------------------------------
# build_envelope
# ---------------------------------------------------------------------------


def test_envelope_carries_full_ods_metadata():
    e = build_envelope(
        record={"request_id": "r1", "amount": 12.5},
        run_id="r-uuid",
        domain="insurance",
        dataset="api_pull_lowlat_demo",
        source_application="lowlat_api",
        source_request_id="req-uuid",
        business_date="2026-05-05",
        cursor_value="2026-04-04T00:00:00Z",
        schema_id="ods.insurance.api_pull_lowlat_demo-value",
        schema_version=1,
    )
    assert e["request_id"] == "r1"
    assert e["amount"] == 12.5
    assert e["_ods_run_id"] == "r-uuid"
    assert e["_ods_business_date"] == "2026-05-05"
    assert e["_ods_source_request_id"] == "req-uuid"
    assert e["_ods_source_application"] == "lowlat_api"
    assert e["_ods_source_cursor"] == "2026-04-04T00:00:00Z"
    assert e["_ods_archive_s3_uri"] is None  # filled by S3 sink later
    assert e["_ods_domain"] == "insurance"
    assert e["_ods_dataset"] == "api_pull_lowlat_demo"
    assert e["_ods_file_id"] is None  # synthetic id is the DAG's job
    assert e["_ods_schema_id"] == "ods.insurance.api_pull_lowlat_demo-value"
    assert e["_ods_schema_version"] == 1


# ---------------------------------------------------------------------------
# run_once
# ---------------------------------------------------------------------------


def _dataset_config():
    return {
        "domain": "insurance",
        "dataset": "api_pull_lowlat_demo",
        "schema_id": "ods.insurance.api_pull_lowlat_demo-value",
        "schema_version": 1,
        "target_topic": "ods.insurance.api_pull_lowlat_demo",
        "source": {
            "application": "lowlat_api",
            "url": "https://api.example/items",
            "cursor": {
                "style": "since_timestamp",
                "request_param": "updated_since",
                "response_field": "updated_at",
                "initial": "2026-01-01T00:00:00Z",
            },
            "page": {"style": "none"},
            "timeout_seconds": 5,
            "retries": 0,
        },
    }


def test_run_once_publishes_records_and_advances_cursor():
    session = _FakeSession([
        _FakeResponse([
            {"request_id": "r1", "amount": 10.0, "updated_at": "2026-04-02T00:00:00Z"},
            {"request_id": "r2", "amount": 20.0, "updated_at": "2026-04-03T00:00:00Z"},
        ]),
    ])
    producer = _FakeProducer()
    run_id = str(uuid.uuid4())

    result = run_once(
        dataset_config=_dataset_config(),
        committed_cursor_value=None,
        run_id=run_id,
        business_date="2026-05-05",
        session=session,
        producer=producer,
    )

    assert isinstance(result, PublishedBatch)
    assert result.no_changes is False
    assert result.record_count == 2
    assert result.new_cursor_value == "2026-04-03T00:00:00Z"
    assert result.target_topic == "ods.insurance.api_pull_lowlat_demo"
    # Partition 0 produced offsets [0, 1] -> end offset 2 (next-message)
    assert result.offset_start_by_partition == {0: 0}
    assert result.offset_end_by_partition == {0: 2}
    assert {p["request_id"] for p in producer.produced} == {"r1", "r2"}
    for env in producer.produced:
        assert env["_ods_run_id"] == run_id
        assert env["_ods_source_application"] == "lowlat_api"


def test_run_once_no_changes_skips_produce():
    session = _FakeSession([_FakeResponse([])])
    producer = _FakeProducer()
    result = run_once(
        dataset_config=_dataset_config(),
        committed_cursor_value="2026-04-04T00:00:00Z",
        run_id=str(uuid.uuid4()),
        business_date="2026-05-05",
        session=session,
        producer=producer,
    )
    assert result.no_changes is True
    assert result.record_count == 0
    assert result.new_cursor_value is None
    assert producer.produced == []


def test_run_once_passes_committed_cursor_to_request():
    session = _FakeSession([_FakeResponse([])])
    producer = _FakeProducer()
    run_once(
        dataset_config=_dataset_config(),
        committed_cursor_value="2026-04-09T12:00:00Z",
        run_id=str(uuid.uuid4()),
        business_date="2026-05-05",
        session=session,
        producer=producer,
    )
    assert session.calls == [
        ("https://api.example/items",
         {"updated_since": "2026-04-09T12:00:00Z"}),
    ]


def test_run_once_raises_on_delivery_failure():
    session = _FakeSession([
        _FakeResponse([
            {"request_id": "r1", "amount": 10.0, "updated_at": "2026-04-02T00:00:00Z"},
            {"request_id": "r2", "amount": 20.0, "updated_at": "2026-04-03T00:00:00Z"},
        ]),
    ])
    producer = _FakeProducer(fail_after=1)
    with pytest.raises(RuntimeError, match="kafka delivery errors"):
        run_once(
            dataset_config=_dataset_config(),
            committed_cursor_value=None,
            run_id=str(uuid.uuid4()),
            business_date="2026-05-05",
            session=session,
            producer=producer,
        )


def test_run_once_multipage_aggregates_offsets():
    session = _FakeSession([
        _FakeResponse(
            [{"request_id": "r1", "amount": 1, "updated_at": "2026-04-02T00:00:00Z"}],
            headers={"Link": '<https://api.example/items?page=2>; rel="next"'},
        ),
        _FakeResponse(
            [{"request_id": "r2", "amount": 2, "updated_at": "2026-04-04T00:00:00Z"}],
            headers={},
        ),
    ])
    # Re-configure cursor to use link_header paging (default for since_timestamp).
    cfg = _dataset_config()
    cfg["source"]["page"] = {"style": "link_header"}

    producer = _FakeProducer()
    result = run_once(
        dataset_config=cfg,
        committed_cursor_value=None,
        run_id=str(uuid.uuid4()),
        business_date="2026-05-05",
        session=session,
        producer=producer,
    )
    assert result.record_count == 2
    assert result.page_count == 2
    assert result.new_cursor_value == "2026-04-04T00:00:00Z"
    assert result.offset_end_by_partition == {0: 2}
