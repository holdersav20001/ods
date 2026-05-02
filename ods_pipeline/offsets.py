"""Kafka offset helpers for partition-safe reconciliation and sink waits."""
from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any


OffsetMap = dict[int, int]
OffsetRangeMap = dict[int, tuple[int, int]]


def persist_ranges(
    conn,
    *,
    run_id: str,
    stage: str,
    topic: str,
    ranges: Mapping[int, tuple[int, int]],
    commit: bool = True,
) -> int:
    """Write per-partition offset ranges to ``pipeline.run_kafka_offsets``.

    Returns the number of rows inserted/updated.

    Designed to be called inside the SAME transaction as the run-status update
    so a crash between Kafka transaction commit and Postgres commit leaves a
    detectable inconsistency (no offset rows + run still 'running').

    ``commit=True`` (default) preserves single-call ergonomics; ``commit=False``
    lets a caller (B8 exactly-once flow) own the transaction boundary and
    commit alongside ``runs.update``.
    """
    if not ranges:
        if commit:
            conn.commit()
        return 0
    rows = [
        (run_id, stage, topic, int(partition), int(start), int(end))
        for partition, (start, end) in ranges.items()
    ]
    try:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO pipeline.run_kafka_offsets
                    (run_id, stage, topic, partition, offset_start, offset_end)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_id, stage, topic, partition)
                DO UPDATE SET
                    offset_start = EXCLUDED.offset_start,
                    offset_end   = EXCLUDED.offset_end,
                    recorded_at  = now()
                """,
                rows,
            )
        if commit:
            conn.commit()
        return len(rows)
    except Exception:
        if commit:
            conn.rollback()
        raise


def read_ranges(conn, *, run_id: str, stage: str | None = None) -> dict[str, OffsetRangeMap]:
    """Return ``{topic: {partition: (start, end)}}`` for a run (optionally a stage).

    Empty dict if no offsets recorded.  Used by B8 resume-time idempotency
    check and T8 dashboards.
    """
    where = "run_id = %s"
    params: tuple[Any, ...] = (run_id,)
    if stage is not None:
        where += " AND stage = %s"
        params = (run_id, stage)
    out: dict[str, OffsetRangeMap] = {}
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT topic, partition, offset_start, offset_end
              FROM pipeline.run_kafka_offsets
             WHERE {where}
            """,
            params,
        )
        for topic, partition, offset_start, offset_end in cur.fetchall():
            out.setdefault(topic, {})[int(partition)] = (int(offset_start), int(offset_end))
    return out


def has_recorded_offsets(conn, *, run_id: str, stage: str) -> bool:
    """Cheap existence check used by B8 resume-time idempotency in ``runs.start``.

    Returns True if any ``run_kafka_offsets`` row exists for the
    ``(run_id, stage)`` pair — i.e. a prior attempt's Kafka transaction
    committed AND its offsets were persisted to Postgres in the same tx.
    Caller should treat this as 'republish would duplicate; skip'.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1 FROM pipeline.run_kafka_offsets
             WHERE run_id = %s AND stage = %s
             LIMIT 1
            """,
            (run_id, stage),
        )
        return cur.fetchone() is not None


def normalise_offset_map(raw: Mapping[Any, Any] | str | None) -> OffsetMap:
    """Return ``{partition: offset}`` with integer keys and values."""
    if raw is None:
        return {}
    if isinstance(raw, str):
        raw = json.loads(raw)
    return {int(partition): int(offset) for partition, offset in raw.items()}


def normalise_range_map(raw: Mapping[Any, Any] | str | None) -> OffsetRangeMap:
    """Return ``{partition: (start, end)}`` from dict/list JSON shapes."""
    if raw is None:
        return {}
    if isinstance(raw, str):
        raw = json.loads(raw)
    ranges: OffsetRangeMap = {}
    for partition, value in raw.items():
        if isinstance(value, Mapping):
            start, end = value["start"], value["end"]
        else:
            start, end = value[0], value[1]
        ranges[int(partition)] = (int(start), int(end))
    return ranges


def range_count(ranges: Mapping[Any, Any] | str | None) -> int:
    """Count records represented by per-partition offset ranges."""
    return sum(
        max(0, end - start)
        for start, end in normalise_range_map(ranges).values()
    )


def delta_count(start_offsets: Mapping[Any, Any], end_offsets: Mapping[Any, Any]) -> int:
    """Count records between two per-partition high-watermark snapshots."""
    starts = normalise_offset_map(start_offsets)
    ends = normalise_offset_map(end_offsets)
    return sum(
        max(0, ends.get(partition, 0) - starts.get(partition, 0))
        for partition in set(starts) | set(ends)
    )


def ranges_from_offsets(start_offsets: Mapping[Any, Any], end_offsets: Mapping[Any, Any]) -> OffsetRangeMap:
    """Build per-partition ranges from two offset snapshots."""
    starts = normalise_offset_map(start_offsets)
    ends = normalise_offset_map(end_offsets)
    return {
        partition: (starts.get(partition, 0), ends.get(partition, 0))
        for partition in sorted(set(starts) | set(ends))
    }


def jsonable_offset_map(offsets: Mapping[Any, Any] | None) -> dict[str, int]:
    """Return an offset map that serializes cleanly to JSON."""
    return {
        str(partition): offset
        for partition, offset in sorted(normalise_offset_map(offsets).items())
    }


def jsonable_range_map(ranges: Mapping[Any, Any] | None) -> dict[str, dict[str, int]]:
    """Return a range map that serializes cleanly to JSON."""
    return {
        str(partition): {"start": start, "end": end}
        for partition, (start, end) in sorted(normalise_range_map(ranges).items())
    }


def partitions_consumed(committed_offsets: Mapping[Any, Any], target_offsets: Mapping[Any, Any]) -> bool:
    """True only when every target partition has reached its target offset."""
    committed = normalise_offset_map(committed_offsets)
    targets = normalise_offset_map(target_offsets)
    if not targets:
        return False
    return all(committed.get(partition, -1) >= target for partition, target in targets.items())


class OffsetTracker:
    """Capture broker-confirmed `(partition, offset)` per delivered Kafka message.

    Designed for use with confluent_kafka's transactional producer (B5/B6).
    The producer's per-message ``on_delivery`` callback runs on the librdkafka
    poll thread, so the callback MUST NOT raise.  Errors are accumulated in
    ``errors`` for the caller to inspect after ``producer.flush()``.

    Recon contract: ``len(rows_attempted) == tracker.delivered_count`` proves
    every queued ``produce()`` resulted in a broker-acknowledged write inside
    the open transaction. Safer than ``offset_end - offset_start`` because it
    is immune to other producers writing to the same topic concurrently.

    Usage:

        tracker = OffsetTracker()
        for row in rows:
            producer.produce(topic, key=k, value=v, on_delivery=tracker.on_delivery)
        producer.flush()
        if tracker.errors:
            raise RuntimeError(f"{len(tracker.errors)} delivery failures")
        assert tracker.delivered_count == len(rows)
    """

    def __init__(self) -> None:
        # {partition: [offset, ...]} — order is delivery order, not produce order.
        self._delivered: dict[int, list[int]] = {}
        self.errors: list[str] = []

    def on_delivery(self, err, msg) -> None:  # noqa: D401 — librdkafka callback shape
        """confluent_kafka per-message delivery callback. Must not raise."""
        if err is not None:
            self.errors.append(str(err))
            return
        try:
            partition = int(msg.partition())
            offset = int(msg.offset())
        except Exception as exc:  # pragma: no cover — defensive
            self.errors.append(f"unparseable delivery report: {exc}")
            return
        self._delivered.setdefault(partition, []).append(offset)

    @property
    def delivered_count(self) -> int:
        """Total number of broker-acknowledged messages across all partitions."""
        return sum(len(offsets) for offsets in self._delivered.values())

    def per_partition_counts(self) -> dict[int, int]:
        """``{partition: delivered_count}``."""
        return {partition: len(offsets) for partition, offsets in self._delivered.items()}

    def per_partition_ranges(self) -> OffsetRangeMap:
        """``{partition: (min_offset, max_offset+1)}`` for delivered messages.

        ``end`` is exclusive (matches the rest of this module's range
        convention).  Empty partitions are omitted.
        """
        return {
            partition: (min(offsets), max(offsets) + 1)
            for partition, offsets in self._delivered.items()
            if offsets
        }
