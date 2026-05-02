"""Kafka offset helpers for partition-safe reconciliation and sink waits."""
from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any


OffsetMap = dict[int, int]
OffsetRangeMap = dict[int, tuple[int, int]]


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
