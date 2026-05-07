"""Unit tests for ods_pipeline.ingest.api_pull_kafka.loop.run_loop.

Drives the loop with a fake clock, fake watermark store, and a
monkey-patched ``run_once`` so we exercise the iteration / cursor
re-read / sleep / stop_event semantics without HTTP, Kafka, Schema
Registry or psycopg2.
"""
from __future__ import annotations

from threading import Event
from unittest.mock import MagicMock

import pytest

from ods_pipeline.ingest.api_pull_kafka import loop as loop_module
from ods_pipeline.ingest.api_pull_kafka.runner import PublishedBatch


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class _FakeWatermarkRow:
    def __init__(self, committed: str | None):
        self.committed_cursor_value = committed


class _FakeStore:
    """Fake ``WatermarkStore`` that records calls and yields a configurable
    cursor sequence so a test can assert each iteration re-read."""

    def __init__(self, cursor_sequence: list[str | None]):
        self._sequence = list(cursor_sequence)
        self.read_calls: list[dict] = []
        self.record_pending_calls: list[dict] = []
        self.closed = 0

    def read(self, **kwargs):
        self.read_calls.append(kwargs)
        if self._sequence:
            return _FakeWatermarkRow(self._sequence.pop(0))
        return _FakeWatermarkRow(None)

    def record_pending(self, **kwargs):
        self.record_pending_calls.append(kwargs)

    def close(self):
        self.closed += 1


class _FakeClock:
    """Drop-in for ``time.sleep`` that fires ``stop_event`` after N calls."""

    def __init__(self, *, stop_event: Event, fire_after: int):
        self.stop_event = stop_event
        self.fire_after = fire_after
        self.calls: list[float] = []

    def sleep(self, seconds: float) -> None:
        self.calls.append(seconds)
        if len(self.calls) >= self.fire_after:
            self.stop_event.set()


def _dataset_config(poll_interval: float = 5.0) -> dict:
    return {
        "domain": "wholesale",
        "dataset": "orders",
        "schema_id": "wholesale.orders",
        "schema_version": 1,
        "target_topic": "wholesale.orders.raw",
        "source": {
            "application": "orders-api",
            "url": "https://example.test/orders",
            "cursor": {"style": "since_timestamp", "param": "since"},
            "poll_interval_seconds": poll_interval,
        },
    }


def _make_published(*, cursor: str | None, count: int = 3) -> PublishedBatch:
    return PublishedBatch(
        domain="wholesale",
        dataset="orders",
        source_application="orders-api",
        run_id="rid",
        target_topic="wholesale.orders.raw",
        record_count=count,
        page_count=1,
        old_cursor_value=None,
        new_cursor_value=cursor,
        source_request_id="srid",
        no_changes=count == 0,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_run_loop_calls_run_once_n_times_until_stop(monkeypatch):
    """Loop calls ``run_once`` once per tick and exits when ``stop_event``
    fires after N sleeps."""
    stop = Event()
    clock = _FakeClock(stop_event=stop, fire_after=3)
    store_factory = lambda: _FakeStore(["c0", "c1", "c2"])

    run_once_mock = MagicMock(
        side_effect=[
            _make_published(cursor="c1"),
            _make_published(cursor="c2"),
            _make_published(cursor="c3"),
            _make_published(cursor="c4"),
        ]
    )
    monkeypatch.setattr(loop_module, "run_once", run_once_mock)

    loop_module.run_loop(
        _dataset_config(poll_interval=5.0),
        kafka_bootstrap="ignored",
        schema_registry_url="ignored",
        schema_str="ignored",
        watermark_store_factory=store_factory,
        stop_event=stop,
        sleep=clock.sleep,
        producer=MagicMock(),  # avoid building a real producer
    )

    assert run_once_mock.call_count == 3
    # poll_interval honoured
    assert clock.calls == [5.0, 5.0, 5.0]


def test_run_loop_re_reads_cursor_each_iteration(monkeypatch):
    """Each iteration must re-read the committed cursor and pass it to
    ``run_once`` so a parallel finalise step is reflected immediately."""
    stop = Event()
    clock = _FakeClock(stop_event=stop, fire_after=2)
    store = _FakeStore(["c-iter0", "c-iter1", "c-iter2"])

    run_once_mock = MagicMock(
        side_effect=[
            _make_published(cursor="c-iter1"),
            _make_published(cursor="c-iter2"),
        ]
    )
    monkeypatch.setattr(loop_module, "run_once", run_once_mock)

    loop_module.run_loop(
        _dataset_config(),
        kafka_bootstrap="ignored",
        schema_registry_url="ignored",
        schema_str="ignored",
        watermark_store_factory=lambda: store,
        stop_event=stop,
        sleep=clock.sleep,
        producer=MagicMock(),
    )

    # 2 iterations -> 2 reads BEFORE run_once + 2 record_pending writes.
    # (Each watermark factory call returns the same store instance.)
    assert run_once_mock.call_count == 2
    committed_args = [
        call.kwargs["committed_cursor_value"]
        for call in run_once_mock.call_args_list
    ]
    assert committed_args == ["c-iter0", "c-iter1"]
    # record_pending was called per non-empty publish
    assert [c["new_cursor_value"] for c in store.record_pending_calls] == [
        "c-iter1",
        "c-iter2",
    ]


def test_run_loop_swallows_transient_runtime_error(monkeypatch):
    """RuntimeError from ``run_once`` is treated as transient and the
    loop retries on the next tick instead of bubbling out."""
    stop = Event()
    clock = _FakeClock(stop_event=stop, fire_after=3)
    store_factory = lambda: _FakeStore([None, None, None])

    run_once_mock = MagicMock(
        side_effect=[
            RuntimeError("transient kafka error"),
            RuntimeError("HTTP 503 from upstream"),
            _make_published(cursor="c-recovered"),
        ]
    )
    monkeypatch.setattr(loop_module, "run_once", run_once_mock)

    loop_module.run_loop(
        _dataset_config(),
        kafka_bootstrap="ignored",
        schema_registry_url="ignored",
        schema_str="ignored",
        watermark_store_factory=store_factory,
        stop_event=stop,
        sleep=clock.sleep,
        producer=MagicMock(),
    )

    # All three iterations executed; loop did not raise.
    assert run_once_mock.call_count == 3


def test_run_loop_raises_on_value_error(monkeypatch):
    """Config / validator errors are unrecoverable and must propagate
    so the supervising operator does not auto-restart blindly."""
    stop = Event()
    clock = _FakeClock(stop_event=stop, fire_after=10)
    store_factory = lambda: _FakeStore([None])

    run_once_mock = MagicMock(side_effect=ValueError("bad cursor style"))
    monkeypatch.setattr(loop_module, "run_once", run_once_mock)

    with pytest.raises(ValueError, match="bad cursor style"):
        loop_module.run_loop(
            _dataset_config(),
            kafka_bootstrap="ignored",
            schema_registry_url="ignored",
            schema_str="ignored",
            watermark_store_factory=store_factory,
            stop_event=stop,
            sleep=clock.sleep,
            producer=MagicMock(),
        )

    assert run_once_mock.call_count == 1


def test_run_loop_honours_poll_interval(monkeypatch):
    """The configured ``source.poll_interval_seconds`` is the value
    passed to ``sleep``."""
    stop = Event()
    clock = _FakeClock(stop_event=stop, fire_after=2)
    store_factory = lambda: _FakeStore([None, None])

    run_once_mock = MagicMock(
        side_effect=[
            _make_published(cursor="c1"),
            _make_published(cursor="c2"),
        ]
    )
    monkeypatch.setattr(loop_module, "run_once", run_once_mock)

    loop_module.run_loop(
        _dataset_config(poll_interval=2.5),
        kafka_bootstrap="ignored",
        schema_registry_url="ignored",
        schema_str="ignored",
        watermark_store_factory=store_factory,
        stop_event=stop,
        sleep=clock.sleep,
        producer=MagicMock(),
    )

    assert clock.calls == [2.5, 2.5]


def test_run_loop_skips_record_pending_on_no_changes(monkeypatch):
    """``no_changes=True`` means the source returned no new records;
    we must not advance pending_cursor_value in that case."""
    stop = Event()
    clock = _FakeClock(stop_event=stop, fire_after=1)
    store = _FakeStore(["c0"])

    run_once_mock = MagicMock(
        return_value=_make_published(cursor=None, count=0)
    )
    monkeypatch.setattr(loop_module, "run_once", run_once_mock)

    loop_module.run_loop(
        _dataset_config(),
        kafka_bootstrap="ignored",
        schema_registry_url="ignored",
        schema_str="ignored",
        watermark_store_factory=lambda: store,
        stop_event=stop,
        sleep=clock.sleep,
        producer=MagicMock(),
    )

    assert store.record_pending_calls == []


def test_run_loop_rejects_non_positive_poll_interval():
    """Defensive: zero or negative ``poll_interval_seconds`` is a config
    error — would otherwise spin the loop without backoff."""
    cfg = _dataset_config(poll_interval=0)
    with pytest.raises(ValueError, match="poll_interval_seconds"):
        loop_module.run_loop(
            cfg,
            kafka_bootstrap="ignored",
            schema_registry_url="ignored",
            schema_str="ignored",
            watermark_store_factory=lambda: _FakeStore([]),
            stop_event=Event(),
            sleep=lambda _s: None,
            producer=MagicMock(),
        )
