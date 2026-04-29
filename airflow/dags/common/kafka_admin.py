"""Kafka admin helpers — read topic high/low watermark offsets."""
from __future__ import annotations

import os
import uuid
from typing import Tuple

from confluent_kafka import Consumer, TopicPartition


def topic_offsets(topic: str, timeout_s: float = 5.0) -> Tuple[int, int]:
    """Return (start_offset_sum, end_offset_sum) across all partitions of `topic`."""
    consumer = Consumer(
        {
            "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP", "broker:29092"),
            "group.id": f"tmp-offset-reader-{uuid.uuid4()}",
            "enable.auto.commit": False,
        }
    )
    try:
        cluster_md = consumer.list_topics(topic, timeout=timeout_s)
        topic_md = cluster_md.topics.get(topic)
        if topic_md is None or topic_md.error is not None:
            return 0, 0
        partitions = [TopicPartition(topic, p) for p in topic_md.partitions]
        start_total = 0
        end_total = 0
        for tp in partitions:
            lo, hi = consumer.get_watermark_offsets(tp, timeout=timeout_s)
            start_total += lo
            end_total += hi
        return start_total, end_total
    finally:
        consumer.close()
