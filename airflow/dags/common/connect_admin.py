"""Kafka Connect REST helpers — wait for sink connector to consume up to a target offset."""
from __future__ import annotations

import os
import time
from typing import Optional

import requests

from ods_pipeline.offsets import normalise_offset_map, partitions_consumed


CONNECT_URL = os.environ.get("CONNECT_URL", "http://kafka-connect:8083")


def get_status(connector_name: str) -> dict:
    r = requests.get(f"{CONNECT_URL}/connectors/{connector_name}/status", timeout=5)
    r.raise_for_status()
    return r.json()


def get_offsets(connector_name: str) -> Optional[dict]:
    r = requests.get(f"{CONNECT_URL}/connectors/{connector_name}/offsets", timeout=5)
    if not r.ok:
        return None
    return r.json()


def _connector_offsets_by_partition(offsets_payload: dict | None, topic: str) -> dict[int, int]:
    """Extract committed sink offsets for *topic* from Kafka Connect REST payload."""
    if not offsets_payload:
        return {}
    offsets: dict[int, int] = {}
    for entry in offsets_payload.get("offsets", []):
        partition = entry.get("partition", {}) or {}
        if partition.get("kafka_topic") != topic:
            continue
        kafka_partition = partition.get("kafka_partition")
        if kafka_partition is None:
            kafka_partition = partition.get("partition")
        if kafka_partition is None:
            continue
        committed = (entry.get("offset", {}) or {}).get("kafka_offset")
        if committed is None:
            continue
        offsets[int(kafka_partition)] = int(committed)
    return offsets


def wait_until_offset_consumed(
    connector_name: str,
    topic: str,
    target_offset: int,
    target_offsets_by_partition: dict[int, int] | dict[str, int] | None = None,
    timeout_s: int = 120,
    poll_interval_s: float = 3.0,
) -> bool:
    """Block until the connector has consumed the requested topic offsets.

    Prefer ``target_offsets_by_partition``. The scalar ``target_offset`` remains
    as a legacy fallback and is evaluated as the sum of committed offsets.
    """
    partition_targets = normalise_offset_map(target_offsets_by_partition)
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            status = get_status(connector_name)
        except requests.RequestException:
            time.sleep(poll_interval_s)
            continue

        connector_state = status.get("connector", {}).get("state")
        if connector_state and connector_state != "RUNNING":
            time.sleep(poll_interval_s)
            continue

        offsets_payload = get_offsets(connector_name)
        committed_by_partition = _connector_offsets_by_partition(offsets_payload, topic)
        if partition_targets:
            if partitions_consumed(committed_by_partition, partition_targets):
                return True
        elif sum(committed_by_partition.values()) >= int(target_offset):
            return True

        time.sleep(poll_interval_s)
    return False
