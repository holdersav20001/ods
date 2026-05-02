"""Unit contracts for transactional Kafka producer plumbing (B5, B6).

This test does NOT require a live Kafka broker — it exercises:

1. ``ods_pipeline.offsets.OffsetTracker`` — per-message ``(partition, offset)``
   capture via the ``on_delivery`` callback. Verifies the recon contract
   ``len(rows) == tracker.delivered_count`` and that errors are accumulated
   non-fatally (callback runs on the librdkafka poll thread and must not
   raise).

2. The transactional lifecycle helper invoked by ``ods_s3_publish``:
   ``init_transactions()`` → ``begin_transaction()`` → ``produce()*N`` →
   ``commit_transaction()`` on success; ``abort_transaction()`` on any
   exception inside the produce loop; ``producer.close()`` always called in
   ``finally``.

The Producer is mocked (``unittest.mock.MagicMock``) so the test is
network-free and runs in the standard ``tests/unit`` suite.
"""
from __future__ import annotations

import os
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, call

import pytest

HERE = os.path.dirname(__file__)
sys.path.insert(0, os.path.abspath(os.path.join(HERE, "..", "..")))

from ods_pipeline.offsets import OffsetTracker  # noqa: E402
from ods_pipeline.publish import publish_with_transaction  # noqa: E402


def _msg(partition: int, offset: int):
    """Mimic the subset of confluent_kafka.Message used by the tracker."""
    return SimpleNamespace(partition=lambda p=partition: p, offset=lambda o=offset: o)


# ---------------------------------------------------------------------------
# OffsetTracker
# ---------------------------------------------------------------------------

class TestOffsetTracker:
    def test_captures_partition_and_offset_per_delivery(self):
        tracker = OffsetTracker()
        tracker.on_delivery(None, _msg(0, 100))
        tracker.on_delivery(None, _msg(0, 101))
        tracker.on_delivery(None, _msg(1, 50))
        assert tracker.delivered_count == 3
        assert tracker.per_partition_counts() == {0: 2, 1: 1}
        assert tracker.per_partition_ranges() == {0: (100, 102), 1: (50, 51)}
        assert tracker.errors == []

    def test_accumulates_errors_does_not_raise(self):
        tracker = OffsetTracker()
        tracker.on_delivery(RuntimeError("broker down"), None)
        tracker.on_delivery(None, _msg(0, 5))
        tracker.on_delivery("string error", None)
        assert tracker.errors == ["broker down", "string error"]
        # Successful delivery still recorded — error path is independent.
        assert tracker.delivered_count == 1

    def test_empty_tracker_reports_zero(self):
        tracker = OffsetTracker()
        assert tracker.delivered_count == 0
        assert tracker.per_partition_counts() == {}
        assert tracker.per_partition_ranges() == {}

    def test_recon_contract_count_equals_rows_attempted(self):
        """The §Task 4 acceptance criterion: ``len(rows) == tracker.delivered_count``."""
        rows = list(range(7))
        tracker = OffsetTracker()
        for i, _row in enumerate(rows):
            tracker.on_delivery(None, _msg(partition=i % 2, offset=1000 + i))
        assert tracker.delivered_count == len(rows)


# ---------------------------------------------------------------------------
# Transactional lifecycle: init → begin → produce → commit | abort + close
# ---------------------------------------------------------------------------

def _make_producer_mock():
    """A fake Producer whose produce() invokes on_delivery synchronously.

    Real librdkafka invokes the callback on its poll thread when the broker
    ack arrives (or errors). For unit purposes we trigger it inline so the
    tracker observes deliveries without spinning a thread.
    """
    state = {"next_offset": {}}

    def produce(topic, key=None, value=None, on_delivery=None, partition=None):
        # Round-robin partition assignment if not specified (matches default
        # librdkafka partitioner semantics for None-key, but this is a fake).
        p = partition if partition is not None else 0
        offset = state["next_offset"].get(p, 0)
        state["next_offset"][p] = offset + 1
        if on_delivery is not None:
            on_delivery(None, _msg(p, offset))

    producer = MagicMock()
    producer.produce.side_effect = produce
    producer.flush.return_value = 0
    return producer


def _run_publish(producer, topic, payloads, tracker):
    """Wrap the production helper with caller-owned producer.close().

    The production lifecycle helper ``ods_pipeline.publish.publish_with_transaction``
    deliberately does NOT close the producer (the caller does, in their own
    finally block — same shape as the Glue job). This wrapper mirrors that
    shape so test assertions of "close happens last" map directly to caller
    behaviour.
    """
    payloads_with_topic = [{**p, "topic": topic} for p in payloads]
    try:
        publish_with_transaction(producer, payloads_with_topic, tracker)
    finally:
        producer.close()


class TestTransactionalLifecycle:
    def test_happy_path_init_begin_produce_commit_close(self):
        producer = _make_producer_mock()
        tracker = OffsetTracker()
        payloads = [{"key": f"k{i}", "value": f"v{i}"} for i in range(5)]

        _run_publish(producer, "ods.test", payloads, tracker)

        # Lifecycle ordering: init → begin → produce*5 → flush → commit → close
        method_names = [c[0] for c in producer.method_calls]
        assert method_names[0] == "init_transactions"
        assert method_names[1] == "begin_transaction"
        produce_idx = [i for i, n in enumerate(method_names) if n == "produce"]
        assert len(produce_idx) == 5
        assert "flush" in method_names
        assert "commit_transaction" in method_names
        assert "abort_transaction" not in method_names
        assert method_names[-1] == "close"

        # Tracker captured every delivery
        assert tracker.delivered_count == 5
        assert tracker.errors == []

    def test_exception_in_loop_aborts_and_still_closes(self):
        producer = _make_producer_mock()
        # Make the 3rd produce raise.
        original_produce = producer.produce.side_effect
        call_count = {"n": 0}

        def produce_then_fail(*args, **kwargs):
            call_count["n"] += 1
            if call_count["n"] == 3:
                raise RuntimeError("simulated produce failure")
            return original_produce(*args, **kwargs)

        producer.produce.side_effect = produce_then_fail
        tracker = OffsetTracker()
        payloads = [{"key": f"k{i}", "value": f"v{i}"} for i in range(5)]

        with pytest.raises(RuntimeError, match="simulated produce failure"):
            _run_publish(producer, "ods.test", payloads, tracker)

        method_names = [c[0] for c in producer.method_calls]
        assert "abort_transaction" in method_names
        assert "commit_transaction" not in method_names
        assert method_names[-1] == "close", "close MUST be in finally"

    def test_delivery_error_aborts_transaction(self):
        """Tracker.errors populated → commit must NOT happen."""
        producer = MagicMock()

        def produce(topic, key=None, value=None, on_delivery=None, **_):
            on_delivery(RuntimeError("broker rejected"), None)

        producer.produce.side_effect = produce
        producer.flush.return_value = 0
        tracker = OffsetTracker()

        with pytest.raises(RuntimeError, match="kafka delivery failures"):
            _run_publish(
                producer, "ods.test", [{"key": "k", "value": "v"}], tracker
            )

        method_names = [c[0] for c in producer.method_calls]
        assert "abort_transaction" in method_names
        assert "commit_transaction" not in method_names
        assert method_names[-1] == "close"
