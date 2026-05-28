"""DLQ subcommand: list / show / replay.

Operators inspect S3 DLQ records and replay specific envelopes back through
the canonical pipeline. Replay creates a new ``run_log`` row linked via
``lineage_edge`` (``edge_type='replay'``, ``upstream_run_id=<original failed
run>``) so the original evidence is preserved.

Designed to be unit-testable: side-effects (S3, Kafka, Postgres) are passed
in as already-constructed clients via the dependency injection in
``_DlqOps``. ``dispatch()`` builds default clients from env when invoked
from the CLI; tests construct ``_DlqOps`` directly.
"""
from __future__ import annotations

import argparse
import json
import sys
import uuid
from collections import Counter
from collections.abc import Iterable
from typing import Any

# ---------------------------------------------------------------------------
# argparse wiring
# ---------------------------------------------------------------------------

def register(parser: argparse.ArgumentParser) -> None:
    sub = parser.add_subparsers(dest="dlq_cmd", required=True)

    p_list = sub.add_parser("list", help="counts by domain/dataset/run/stage")
    p_list.add_argument("--domain")
    p_list.add_argument("--dataset")
    p_list.add_argument("--prefix", help="restrict to specific S3 prefix")

    p_show = sub.add_parser("show", help="dump envelope JSON for a key")
    p_show.add_argument("s3_uri")

    p_replay = sub.add_parser("replay", help="re-publish a DLQ record")
    p_replay.add_argument("s3_uri")
    p_replay.add_argument("--target-topic", required=True,
                          help="Kafka topic to republish into")
    p_replay.add_argument("--dry-run", action="store_true",
                          help="print intended action; do not produce/write")


def dispatch(args) -> int:  # pragma: no cover — thin glue
    import boto3

    s3 = boto3.client("s3")
    pg = _connect_pg()
    producer = None  # lazily init only on replay
    bucket = _default_bucket()
    ops = _DlqOps(s3=s3, pg_conn=pg, bucket=bucket,
                  producer_factory=lambda: _make_producer())
    if args.dlq_cmd == "list":
        for line in ops.list(domain=args.domain, dataset=args.dataset,
                              prefix=args.prefix):
            print(line)
        return 0
    if args.dlq_cmd == "show":
        print(json.dumps(ops.show(args.s3_uri), indent=2, default=str))
        return 0
    if args.dlq_cmd == "replay":
        result = ops.replay(args.s3_uri, target_topic=args.target_topic,
                            dry_run=args.dry_run)
        print(json.dumps(result, indent=2, default=str))
        return 0
    print(f"unknown subcommand {args.dlq_cmd!r}", file=sys.stderr)
    return 2


# ---------------------------------------------------------------------------
# Core logic — DI-friendly
# ---------------------------------------------------------------------------

class _DlqOps:
    """Stateless wrapper. All side-effect collaborators are injected."""

    def __init__(self, *, s3, pg_conn, bucket: str,
                 producer_factory=None):
        self._s3 = s3
        self._pg = pg_conn
        self._bucket = bucket
        self._producer_factory = producer_factory

    # --- list -----------------------------------------------------------

    def list(self, *, domain: str | None = None, dataset: str | None = None,
             prefix: str | None = None) -> Iterable[str]:
        keys = list(self._iter_keys(domain=domain, dataset=dataset,
                                    prefix=prefix))
        if not keys:
            yield "(no DLQ records found)"
            return
        breakdown: Counter = Counter()
        for key in keys:
            parts = key.split("/")
            # layout: <domain>/<dataset>/<stage>/date=.../run_id=.../<n>.json
            if len(parts) >= 5:
                d, ds, stage = parts[0], parts[1], parts[2]
                run_id = next((p.split("=", 1)[1] for p in parts
                               if p.startswith("run_id=")), "?")
                breakdown[(d, ds, stage, run_id)] += 1
        yield f"total: {len(keys)} record(s)"
        yield "domain/dataset/stage/run_id\tcount"
        for (d, ds, stage, run_id), count in sorted(breakdown.items()):
            yield f"{d}/{ds}/{stage}/{run_id}\t{count}"

    # --- show -----------------------------------------------------------

    def show(self, s3_uri: str) -> dict[str, Any]:
        bucket, key = _split_s3_uri(s3_uri)
        obj = self._s3.get_object(Bucket=bucket, Key=key)
        return json.loads(obj["Body"].read())

    # --- replay ---------------------------------------------------------

    def replay(self, s3_uri: str, *, target_topic: str,
               dry_run: bool = False) -> dict[str, Any]:
        envelope = self.show(s3_uri)
        original_run = envelope.get("_ods_run_id")
        domain = envelope.get("_ods_domain")
        dataset = envelope.get("_ods_dataset")
        payload = envelope.get("payload", {})
        replay_run_id = str(uuid.uuid4())
        result = {
            "replay_run_id": replay_run_id,
            "original_run_id": original_run,
            "target_topic": target_topic,
            "domain": domain,
            "dataset": dataset,
            "dry_run": dry_run,
        }
        if dry_run:
            result["status"] = "dry-run"
            return result
        # Real replay: open new run, link via lineage, produce, commit
        from ods_pipeline import lineage, runs
        runs.start(self._pg, run_id=replay_run_id, pipeline_type="dlq_replay",
                   domain=domain or "unknown",
                   dataset=dataset or "unknown",
                   business_date=envelope.get("_ods_business_date"),
                   orchestrators=[original_run] if original_run else None)
        if original_run:
            lineage.write_edge(self._pg, consumer_run_id=replay_run_id,
                               upstream_run_id=original_run,
                               edge_type="replay",
                               source_ref=s3_uri,
                               target_ref=f"kafka://{target_topic}",
                               record_count=1)
        producer = self._producer_factory()
        producer.produce(topic=target_topic,
                         value=json.dumps(payload).encode("utf-8"))
        producer.flush()
        result["status"] = "replayed"
        return result

    # --- helpers --------------------------------------------------------

    def _iter_keys(self, *, domain: str | None, dataset: str | None,
                   prefix: str | None):
        if prefix is not None:
            search = prefix
        else:
            parts = []
            if domain:
                parts.append(domain)
            if dataset:
                parts.append(dataset)
            search = "/".join(parts) + "/" if parts else ""
        paginator = self._s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self._bucket, Prefix=search):
            for obj in page.get("Contents", []):
                yield obj["Key"]


# ---------------------------------------------------------------------------
# helpers (also used by CLI builder)
# ---------------------------------------------------------------------------

def _split_s3_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("s3://"):
        raise ValueError(f"not an s3 URI: {uri!r}")
    rest = uri[len("s3://"):]
    bucket, _, key = rest.partition("/")
    if not bucket or not key:
        raise ValueError(f"malformed s3 URI: {uri!r}")
    return bucket, key


def _default_bucket() -> str:  # pragma: no cover
    import os
    env = os.environ.get("ENV", "local")
    return f"ods-dlq-{env}"


def _connect_pg():  # pragma: no cover
    import os

    import psycopg2
    return psycopg2.connect(
        host=os.environ.get("PG_HOST", "127.0.0.1"),
        port=int(os.environ.get("PG_PORT", "5440")),
        dbname=os.environ.get("PG_DB", "ods_dev"),
        user=os.environ.get("PG_USER", "ods"),
        password=os.environ.get("PG_PASSWORD", "ods"),
    )


def _make_producer():  # pragma: no cover
    import os

    from confluent_kafka import Producer
    return Producer({"bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP",
                                                          "localhost:9092")})
