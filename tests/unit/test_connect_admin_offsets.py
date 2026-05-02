import importlib.util


def _load_module():
    spec = importlib.util.spec_from_file_location(
        "connect_admin_test", "airflow/dags/common/connect_admin.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _payload(*entries):
    return {"offsets": list(entries)}


def _entry(topic, partition, offset):
    return {
        "partition": {
            "kafka_topic": topic,
            "kafka_partition": partition,
        },
        "offset": {"kafka_offset": offset},
    }


def test_wait_until_offset_consumed_requires_all_partitions(monkeypatch):
    module = _load_module()
    payloads = iter([
        _payload(_entry("topic-a", 0, 10), _entry("topic-a", 1, 19)),
        _payload(_entry("topic-a", 0, 10), _entry("topic-a", 1, 20)),
    ])

    monkeypatch.setattr(
        module,
        "get_status",
        lambda _connector: {"connector": {"state": "RUNNING"}},
    )
    monkeypatch.setattr(module, "get_offsets", lambda _connector: next(payloads))
    monkeypatch.setattr(module.time, "sleep", lambda *_args: None)

    assert module.wait_until_offset_consumed(
        "jdbc-sink-test",
        "topic-a",
        30,
        target_offsets_by_partition={0: 10, 1: 20},
        timeout_s=5,
        poll_interval_s=0,
    )


def test_wait_until_offset_consumed_scalar_fallback_uses_sum(monkeypatch):
    module = _load_module()
    monkeypatch.setattr(
        module,
        "get_status",
        lambda _connector: {"connector": {"state": "RUNNING"}},
    )
    monkeypatch.setattr(
        module,
        "get_offsets",
        lambda _connector: _payload(_entry("topic-a", 0, 10), _entry("topic-a", 1, 20)),
    )

    assert module.wait_until_offset_consumed(
        "jdbc-sink-test",
        "topic-a",
        30,
        timeout_s=1,
        poll_interval_s=0,
    )
