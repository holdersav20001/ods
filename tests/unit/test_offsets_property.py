"""Property tests for partition-aware offset helpers."""
from __future__ import annotations

from hypothesis import given, settings
from hypothesis import strategies as st

from ods_pipeline import offsets


_offset_map = st.dictionaries(
    keys=st.integers(min_value=0, max_value=12),
    values=st.integers(min_value=0, max_value=1_000_000),
    max_size=8,
)


@given(_offset_map, _offset_map)
@settings(max_examples=100)
def test_delta_count_is_non_negative_and_bounded_by_positive_partition_deltas(starts, ends):
    expected = sum(
        max(0, ends.get(partition, 0) - starts.get(partition, 0))
        for partition in set(starts) | set(ends)
    )

    assert offsets.delta_count(starts, ends) == expected
    assert offsets.delta_count(starts, ends) >= 0


@given(_offset_map, _offset_map)
@settings(max_examples=100)
def test_ranges_from_offsets_round_trips_count(starts, ends):
    ranges = offsets.ranges_from_offsets(starts, ends)

    assert offsets.range_count(ranges) == offsets.delta_count(starts, ends)
    assert set(ranges) == set(starts) | set(ends)
    for partition, (start, end) in ranges.items():
        assert start == starts.get(partition, 0)
        assert end == ends.get(partition, 0)


@given(_offset_map)
@settings(max_examples=100)
def test_jsonable_offset_map_normalises_to_string_keys(raw):
    jsonable = offsets.jsonable_offset_map(raw)

    assert all(isinstance(partition, str) for partition in jsonable)
    assert offsets.normalise_offset_map(jsonable) == offsets.normalise_offset_map(raw)
