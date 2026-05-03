from ods_pipeline import offsets


def test_delta_count_uses_each_partition_independently():
    assert offsets.delta_count({"0": 10, "1": 20}, {"0": 13, "1": 25}) == 8


def test_range_count_accepts_json_range_shape():
    raw = '{"0":{"start":10,"end":13},"1":{"start":20,"end":25}}'
    assert offsets.range_count(raw) == 8


def test_partitions_consumed_requires_every_target_partition():
    assert not offsets.partitions_consumed({0: 10, 1: 19}, {0: 10, 1: 20})
    assert offsets.partitions_consumed({0: 10, 1: 20, 2: 999}, {0: 10, 1: 20})


def test_jsonable_range_map_sorts_and_stringifies_keys():
    result = offsets.jsonable_range_map({2: (5, 7), "0": [1, 3]})
    assert result == {
        "0": {"start": 1, "end": 3},
        "2": {"start": 5, "end": 7},
    }
