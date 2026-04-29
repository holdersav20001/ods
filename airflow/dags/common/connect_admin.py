"""Kafka Connect REST helpers — wait for sink connector to consume up to a target offset."""
from __future__ import annotations

import os
import time
from typing import Optional

import requests


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


def wait_until_offset_consumed(
    connector_name: str,
    topic: str,
    target_offset: int,
    timeout_s: int = 120,
    poll_interval_s: float = 3.0,
) -> bool:
    """Block until the connector's committed offset for `topic` >= `target_offset`.

    Returns True on success, False on timeout or non-RUNNING state past deadline.
    """
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
        if offsets_payload:
            for entry in offsets_payload.get("offsets", []):
                partition = entry.get("partition", {}) or {}
                if partition.get("kafka_topic") != topic:
                    continue
                committed = (entry.get("offset", {}) or {}).get("kafka_offset", 0)
                if committed >= target_offset:
                    return True

        time.sleep(poll_interval_s)
    return False
