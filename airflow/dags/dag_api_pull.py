"""dag_api_pull — schedule-driven HTTP poll for ``source_type='api_pull'``
datasets, archive to S3 as JSONL, register in pipeline.file_catalogue,
trigger dag_ingest, then promote the api_pull watermark only after the
triggered dag_ingest run finishes successfully.

Stays decoupled from dag_ingest: dag_ingest remains source-pattern
agnostic. dag_api_pull triggers it, watches its run_log row, and owns
the pending → committed cursor lifecycle in
``pipeline.api_pull_watermark`` via ods_pipeline.ingest.api_pull.
"""
from __future__ import annotations

import json
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timezone

log = logging.getLogger(__name__)

import boto3
import pendulum
import psycopg2

from airflow import DAG
from airflow.decorators import task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule

# Match the import-path bootstrapping used by dag_ingest so ods_pipeline
# resolves whether the DAG file is loaded from /opt/airflow/dags or a
# bind-mounted repo root.
_DAG_DIR = os.path.dirname(__file__)
for _root in (
    os.path.abspath(os.path.join(_DAG_DIR, "..")),
    os.path.abspath(os.path.join(_DAG_DIR, "..", "..")),
):
    if _root not in sys.path:
        sys.path.insert(0, _root)

import ods_pipeline
from ods_pipeline.ingest.api_pull import (
    TRIGGERED_BY_API_PULL_EDGE,
    WatermarkStore,
    derive_dag_ingest_parent_run_id,
    ingest_status_for_api_pull_run,
    poll_and_archive,
)
from ods_pipeline.ingest.api_pull_kafka import run_once as run_once_direct_kafka
from ods_pipeline.models import Stage, StageEvent

PG_DSN = os.environ.get(
    "PIPELINE_PG_DSN",
    "host=postgres port=5432 dbname=ods_dev user=ods password=ods",
)
S3_RAW_BUCKET = os.environ.get("S3_RAW_BUCKET", "ods-raw-local")
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://localstack:4566")
DOWNSTREAM_POLL_SECONDS = float(os.environ.get("API_PULL_DOWNSTREAM_POLL_SECONDS", "5"))
DOWNSTREAM_TIMEOUT_SECONDS = float(os.environ.get("API_PULL_DOWNSTREAM_TIMEOUT_SECONDS", "1800"))
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "broker:29092")
SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")


def _connect_pg():
    return psycopg2.connect(PG_DSN)


def _s3():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
        region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
    )


@task
def list_active_api_datasets() -> list[dict]:
    """Return every dataset_config row with ``source_type='api_pull'``.

    Each row carries the URL/auth/cursor/page block in ``source_config``;
    the poller validates the contents at call time so we keep the SQL
    surface minimal here.
    """
    conn = _connect_pg()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT domain, dataset, schema_id, schema_version,
                       target_topic, raw_format, source_config,
                       config_version_id, COALESCE(delivery, 'file_pipeline')
                  FROM pipeline.dataset_config
                 WHERE active = TRUE AND source_type = 'api_pull'
                """
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    out: list[dict] = []
    for (
        domain, dataset, schema_id, schema_version, target_topic,
        raw_format, source_config, config_version_id, delivery,
    ) in rows:
        if isinstance(source_config, str):
            source_config = json.loads(source_config)
        out.append({
            "domain": domain,
            "dataset": dataset,
            "schema_id": schema_id,
            "schema_version": schema_version,
            "target_topic": target_topic,
            "raw_format": raw_format,
            "source": source_config or {},
            "config_version_id": config_version_id,
            "delivery": delivery or "file_pipeline",
        })
    return out


def _fetch_schema_str(subject: str, version: int | str = "latest") -> str:
    """Pull the registered Avro schema from Schema Registry.

    Used by the direct-Kafka dispatch path. Lives inline so a partial
    Schema Registry outage cannot break poll_one for file-pipeline
    datasets — the helper is only invoked when ``delivery=='direct_kafka'``.

    ``version`` defaults to ``"latest"`` for backward compatibility, but
    callers should pin to ``dataset_config.schema_version`` so two runs
    of the same dataset cannot pick up a silently rebased wire shape.
    Numeric strings are coerced to ints; anything else is sent verbatim.
    """
    import requests

    # Strict whitelist (Reality Checker F8). Schema Registry treats any
    # non-positive integer as "latest" — including ``-1`` — so a slip-up
    # in the dataset_config NUMERIC column would silently un-pin the
    # wire shape. Accept only:
    #   * ``int`` >= 1
    #   * ``str`` matching ``^\d+$`` after ``strip()``, value >= 1
    # Everything else (None, "3.0", "v2", "  ", "-1", "0") falls back
    # to "latest" with a WARN — the same posture as a missing column,
    # so operators see the same signal in run_log.
    version_path: str
    if isinstance(version, int) and version >= 1:
        version_path = str(version)
    elif isinstance(version, str):
        stripped = version.strip()
        if stripped.isdigit() and int(stripped) >= 1:
            version_path = str(int(stripped))  # strips leading zeros
        else:
            log.warning(
                "schema version %r is not a positive integer; "
                "falling back to 'latest' for subject %s",
                version, subject,
            )
            version_path = "latest"
    else:
        version_path = "latest"

    resp = requests.get(
        f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions/{version_path}",
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()["schema"]


def _poll_one_direct_kafka(cfg: dict) -> dict | None:
    """Direct-Kafka delivery: long-running runner publishes per-record
    Avro to the raw topic and records the offset window. No
    ``dag_ingest`` is triggered; ``finalise_watermark`` waits on the
    JDBC sink consumer offsets to promote the cursor.

    See ``docs/api-pull-direct-kafka-design.md``.
    """
    domain = cfg["domain"]
    dataset = cfg["dataset"]
    source = cfg.get("source") or {}
    source_application = str(source.get("application", f"{domain}.{dataset}"))
    cursor_style = str((source.get("cursor") or {}).get("style", "since_timestamp"))
    business_date = datetime.now(timezone.utc).date().isoformat()
    run_id = str(uuid.uuid4())

    conn = _connect_pg()
    store = WatermarkStore(conn)
    locked = False
    published = None
    try:
        watermark = store.read(
            domain=domain,
            dataset=dataset,
            source_application=source_application,
            cursor_type=cursor_style,
        )
        locked = store.try_lock(
            domain=domain,
            dataset=dataset,
            source_application=source_application,
            run_id=run_id,
        )
        if not locked:
            return None

        ods_pipeline.runs.start(
            conn,
            run_id=run_id,
            pipeline_type="api_pull",
            domain=domain,
            dataset=dataset,
            business_date=business_date,
            kafka_topic=cfg.get("target_topic"),
            config_version_id=cfg.get("config_version_id"),
        )
        with ods_pipeline.stages.stage_scope(
            conn,
            run_id=run_id,
            stage=Stage.RAW_POLL,
            metrics={
                "source_application": source_application,
                "cursor_style": cursor_style,
                "delivery": "direct_kafka",
                "committed_cursor_value": watermark.committed_cursor_value,
            },
        ) as raw_poll:
            sv = cfg.get("schema_version")
            if sv is None:
                log.warning(
                    "schema_version missing for %s.%s; falling back to 'latest'. "
                    "Backfill dataset_config.schema_version to pin the wire shape.",
                    domain, dataset,
                )
                sv = "latest"
            schema_str = _fetch_schema_str(cfg["schema_id"], sv)
            published = run_once_direct_kafka(
                dataset_config={
                    "domain": domain,
                    "dataset": dataset,
                    "schema_id": cfg["schema_id"],
                    "schema_version": cfg.get("schema_version", 1),
                    "target_topic": cfg["target_topic"],
                    "source": source,
                },
                kafka_bootstrap=KAFKA_BOOTSTRAP,
                schema_registry_url=SCHEMA_REGISTRY_URL,
                schema_str=schema_str,
                committed_cursor_value=watermark.committed_cursor_value,
                run_id=run_id,
                business_date=business_date,
            )

            if published.no_changes:
                raw_poll.skip("no_changes")
            else:
                raw_poll.set_result(
                    output_ref=f"kafka://{published.target_topic}",
                    record_count_out=published.record_count,
                    metrics={
                        "page_count": published.page_count,
                        "old_cursor_value": published.old_cursor_value,
                        "new_cursor_value": published.new_cursor_value,
                        "source_request_id": published.source_request_id,
                        "offset_start_by_partition": published.offset_start_by_partition,
                        "offset_end_by_partition": published.offset_end_by_partition,
                    },
                )

        if published.no_changes:
            ods_pipeline.runs.update(
                conn,
                run_id,
                status="succeeded",
                record_count_source=0,
                record_count_target=0,
            )
            ods_pipeline.events.produce(
                "api_pull.skipped_no_changes",
                run_id=run_id,
                domain=domain,
                dataset=dataset,
                business_date=business_date,
                status="succeeded",
            )
            return None

        ods_pipeline.stages.write(
            conn,
            run_id=run_id,
            stage=Stage.KAFKA_PUBLISH,
            status="succeeded",
            event_type=StageEvent.COMPLETED,
            output_ref=f"kafka://{published.target_topic}",
            record_count_in=published.record_count,
            record_count_out=published.record_count,
            metrics={
                "offset_start_by_partition": published.offset_start_by_partition,
                "offset_end_by_partition": published.offset_end_by_partition,
            },
        )

        # Lineage: api → kafka topic. No file_id (direct-Kafka shape
        # does not pre-archive); use a synthetic uuid5 so dashboards
        # joining on file_id keep working.
        synthetic_file_id = str(
            uuid.uuid5(uuid.NAMESPACE_OID, f"api_pull_kafka:{run_id}")
        )
        ods_pipeline.lineage.write_edge(
            conn,
            consumer_run_id=run_id,
            source_file_id=None,
            edge_type="api_to_kafka",
            source_ref=str(source.get("url", "")),
            target_ref=f"kafka://{published.target_topic}",
            record_count=published.record_count,
        )

        # Recon: produce-count vs offset delta. Equal by construction
        # (idempotent producer ACKs every record), but write the row so
        # operators can spot an unexpected delivery-failure path.
        offset_delta = sum(
            (published.offset_end_by_partition.get(p, 0)
             - published.offset_start_by_partition.get(p, 0))
            for p in published.offset_end_by_partition
        )
        ods_pipeline.reconciliation.write_check(
            conn,
            check_type="api_pull_publish_count",
            run_id=run_id,
            domain=domain,
            dataset=dataset,
            business_date=business_date,
            source_count=published.record_count,
            accounted_count=offset_delta,
            postgres_count=None,
            status="ok" if offset_delta == published.record_count else "failed",
            detail=json.dumps({
                "fetched_count": published.record_count,
                "produced_offset_delta": offset_delta,
                "page_count": published.page_count,
                "source_request_id": published.source_request_id,
                "old_cursor_value": published.old_cursor_value,
                "new_cursor_value": published.new_cursor_value,
                "offset_start_by_partition": published.offset_start_by_partition,
                "offset_end_by_partition": published.offset_end_by_partition,
            }, sort_keys=True),
        )

        if published.new_cursor_value:
            store.record_pending(
                domain=domain,
                dataset=dataset,
                source_application=source_application,
                run_id=run_id,
                new_cursor_value=published.new_cursor_value,
            )

        ods_pipeline.runs.update(
            conn,
            run_id,
            record_count_source=published.record_count,
            record_count_target=published.record_count,
        )
        ods_pipeline.events.produce(
            "api_pull.archived",
            run_id=run_id,
            domain=domain,
            dataset=dataset,
            business_date=business_date,
            status="running",
        )
    except Exception as exc:
        try:
            ods_pipeline.runs.update(
                conn,
                run_id,
                status="failed",
                error_summary=f"api_pull direct_kafka failed: {exc}",
            )
            ods_pipeline.stages.write(
                conn,
                run_id=run_id,
                stage=Stage.RAW_POLL,
                status="failed",
                event_type=StageEvent.FAILED,
                error=str(exc),
            )
        except Exception:
            pass
        raise
    finally:
        if locked:
            try:
                store.unlock(
                    domain=domain,
                    dataset=dataset,
                    source_application=source_application,
                )
            except Exception:
                pass
        conn.close()

    return {
        "delivery": "direct_kafka",
        "domain": domain,
        "dataset": dataset,
        "business_date": business_date,
        "api_pull_run_id": run_id,
        "source_application": source_application,
        "new_cursor_value": published.new_cursor_value,
        "kafka_topic": published.target_topic,
        "offset_end_by_partition": published.offset_end_by_partition,
    }


@task
def poll_one(cfg: dict) -> dict | None:
    """Run one poll for one dataset and prepare a dag_ingest trigger conf.

    Returns ``None`` (so the downstream TriggerDagRunOperator.expand entry
    is skipped) when there are no new records — the watermark is left
    unchanged so the next schedule re-issues the same window if that
    changes upstream.

    Dispatches on ``cfg['delivery']``. ``direct_kafka`` routes to the
    Avro-producing runner and returns a marker dict (no dag_ingest
    trigger needed). ``file_pipeline`` (default) keeps the existing
    S3-archive + dag_ingest path.
    """
    if (cfg or {}).get("delivery") == "direct_kafka":
        return _poll_one_direct_kafka(cfg)

    domain = cfg["domain"]
    dataset = cfg["dataset"]
    source = cfg.get("source") or {}
    source_application = str(source.get("application", f"{domain}.{dataset}"))
    cursor_style = str((source.get("cursor") or {}).get("style", "since_timestamp"))
    business_date = datetime.now(timezone.utc).date().isoformat()
    run_id = str(uuid.uuid4())

    conn = _connect_pg()
    store = WatermarkStore(conn)
    locked = False
    archive = None
    try:
        watermark = store.read(
            domain=domain,
            dataset=dataset,
            source_application=source_application,
            cursor_type=cursor_style,
        )
        locked = store.try_lock(
            domain=domain,
            dataset=dataset,
            source_application=source_application,
            run_id=run_id,
        )
        if not locked:
            # Another scheduler beat us to it — abort cleanly.
            return None

        ods_pipeline.runs.start(
            conn,
            run_id=run_id,
            pipeline_type="api_pull",
            domain=domain,
            dataset=dataset,
            business_date=business_date,
            kafka_topic=cfg.get("target_topic"),
            config_version_id=cfg.get("config_version_id"),
        )
        with ods_pipeline.stages.stage_scope(
            conn,
            run_id=run_id,
            stage=Stage.RAW_POLL,
            metrics={
                "source_application": source_application,
                "cursor_style": cursor_style,
                "committed_cursor_value": watermark.committed_cursor_value,
            },
        ) as raw_poll:
            archive = poll_and_archive(
                dataset_config={
                    "domain": domain,
                    "dataset": dataset,
                    "schema_id": cfg.get("schema_id"),
                    "schema_version": cfg.get("schema_version", 1),
                    "source": source,
                },
                s3_client=_s3(),
                archive_bucket=S3_RAW_BUCKET,
                committed_cursor_value=watermark.committed_cursor_value,
                run_id=run_id,
                business_date=business_date,
            )

            if archive.no_changes:
                raw_poll.skip("no_changes")
            else:
                raw_poll.set_result(
                    output_ref=archive.s3_uri,
                    record_count_out=archive.record_count,
                    metrics={
                        "page_count": archive.page_count,
                        "old_cursor_value": archive.old_cursor_value,
                        "new_cursor_value": archive.new_cursor_value,
                        "source_request_id": archive.source_request_id,
                    },
                )

        if archive.no_changes:
            ods_pipeline.runs.update(
                conn,
                run_id,
                status="succeeded",
                record_count_source=0,
                record_count_target=0,
            )
            ods_pipeline.events.produce(
                "api_pull.skipped_no_changes",
                run_id=run_id,
                domain=domain,
                dataset=dataset,
                business_date=business_date,
                status="succeeded",
            )
            return None

        ods_pipeline.stages.write(
            conn,
            run_id=run_id,
            stage=Stage.MESSAGE_ARCHIVE,
            status="succeeded",
            event_type=StageEvent.COMPLETED,
            output_ref=archive.s3_uri,
            record_count_in=archive.record_count,
            record_count_out=archive.record_count,
            metrics={
                "file_md5": archive.file_md5,
                "file_size_bytes": archive.file_size_bytes,
            },
        )

        file_id = ods_pipeline.files.upsert(
            conn,
            domain=domain,
            dataset=dataset,
            business_date=business_date,
            file_md5=archive.file_md5,
            s3_raw_path=archive.s3_uri,
            file_size_bytes=archive.file_size_bytes,
            source_row_count=archive.record_count,
            state="received",
            last_run_id=run_id,
        )

        ods_pipeline.lineage.write_edge(
            conn,
            consumer_run_id=run_id,
            source_file_id=file_id,
            edge_type="api_to_archive",
            source_ref=str(source.get("url", "")),
            target_ref=archive.s3_uri,
            record_count=archive.record_count,
        )

        ods_pipeline.reconciliation.write_check(
            conn,
            check_type="api_pull_archive_count",
            run_id=run_id,
            domain=domain,
            dataset=dataset,
            business_date=business_date,
            source_count=archive.record_count,
            accounted_count=None,
            postgres_count=None,
            status="ok",
            detail=json.dumps({
                "fetched_count": archive.record_count,
                "archived_count": archive.record_count,
                "page_count": archive.page_count,
                "source_request_id": archive.source_request_id,
                "old_cursor_value": archive.old_cursor_value,
                "new_cursor_value": archive.new_cursor_value,
            }, sort_keys=True),
        )

        if archive.new_cursor_value:
            store.record_pending(
                domain=domain,
                dataset=dataset,
                source_application=source_application,
                run_id=run_id,
                new_cursor_value=archive.new_cursor_value,
            )

        ods_pipeline.runs.update(
            conn,
            run_id,
            record_count_source=archive.record_count,
        )
        ods_pipeline.events.produce(
            "api_pull.archived",
            run_id=run_id,
            domain=domain,
            dataset=dataset,
            business_date=business_date,
            status="running",
        )
    except Exception as exc:
        try:
            ods_pipeline.runs.update(
                conn,
                run_id,
                status="failed",
                error_summary=f"api_pull poll failed: {exc}",
            )
            ods_pipeline.stages.write(
                conn,
                run_id=run_id,
                stage=Stage.RAW_POLL,
                status="failed",
                event_type=StageEvent.FAILED,
                error=str(exc),
            )
        except Exception:
            pass
        raise
    finally:
        if locked:
            try:
                store.unlock(
                    domain=domain,
                    dataset=dataset,
                    source_application=source_application,
                )
            except Exception:
                pass
        conn.close()

    # Two layers of linkage so finalise_watermark observes the EXACT
    # downstream execution launched by THIS poll, even under
    # TriggerDagRunOperator retries / manual replays:
    #
    #   1. ``triggered_by_run_id`` + ``triggered_by_edge_type`` —
    #      written by dag_ingest.init_run into run_log.orchestrators.
    #   2. ``upstream_run_id`` — pre-minted deterministically (uuid5 of
    #      ``api_pull_run_id``) and consumed by dag_ingest.init_run as
    #      the s3_batch parent run_id, so the linkage helper can do an
    #      exact PK lookup, not a "latest by edge" scan.
    expected_parent_run_id = derive_dag_ingest_parent_run_id(run_id)
    return {
        "file_id": file_id,
        "domain": domain,
        "dataset": dataset,
        "business_date": business_date,
        "api_pull_run_id": run_id,
        "source_application": source_application,
        "new_cursor_value": archive.new_cursor_value,
        "triggered_by_run_id": run_id,
        "triggered_by_edge_type": TRIGGERED_BY_API_PULL_EDGE,
        "upstream_run_id": expected_parent_run_id,
        "dag_ingest_parent_run_id": expected_parent_run_id,
    }


def _direct_kafka_sink_status(cfg: dict) -> str | None:
    """Return ``'succeeded'``, ``'failed'`` or ``None`` (still running)
    for the JDBC sink that consumes this poll's produced offsets.

    Uses ``confluent_kafka.AdminClient`` to compare the JDBC consumer
    group's committed offsets to the per-partition end-offsets we
    captured at produce time. ``connect-jdbc-sink-<dataset>`` is the
    consumer group name produced by Kafka Connect for our sinks; the
    canonical-style sinks reuse the same naming.
    """
    try:
        from confluent_kafka import Consumer, TopicPartition
    except Exception:
        return None

    end_offsets = cfg.get("offset_end_by_partition") or {}
    if not end_offsets:
        return None
    topic = cfg["kafka_topic"]
    sink_name = f"jdbc-sink-{cfg['dataset']}".replace("_", "-")
    group_id = f"connect-{sink_name}"

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": group_id,
        "enable.auto.commit": False,
        "session.timeout.ms": 6000,
    })
    try:
        tps = [TopicPartition(topic, int(p)) for p in end_offsets.keys()]
        committed = consumer.committed(tps, timeout=10)
    except Exception:
        return None
    finally:
        try:
            consumer.close()
        except Exception:
            pass

    consumed = {int(tp.partition): int(tp.offset) for tp in committed if tp.offset >= 0}
    if not consumed:
        return None
    for partition, target in end_offsets.items():
        # Connect commits offsets as "next-message" — compare ≥.
        if consumed.get(int(partition), -1) < int(target):
            return None
    return "succeeded"


@task(trigger_rule=TriggerRule.ALL_DONE)
def finalise_watermark(triggered_confs: list[dict | None]) -> None:
    """Wait for each poll's downstream to finish, then promote or clear
    the corresponding pending cursor.

    Two delivery modes, two oracles:
      - file_pipeline: poll ``run_log`` for the linked ``dag_ingest``
        parent run via ``ingest_status_for_api_pull_run``.
      - direct_kafka: poll Kafka Connect consumer-group offsets for
        the dataset's JDBC sink via ``_direct_kafka_sink_status``.

    Same two-phase commit contract; same timeout / retry envelope.
    """
    pending = [c for c in (triggered_confs or []) if c]
    if not pending:
        return

    conn = _connect_pg()
    store = WatermarkStore(conn)
    deadline = time.monotonic() + DOWNSTREAM_TIMEOUT_SECONDS
    try:
        while pending and time.monotonic() < deadline:
            still_pending: list[dict] = []
            for cfg in pending:
                if cfg.get("delivery") == "direct_kafka":
                    ingest_status = _direct_kafka_sink_status(cfg)
                else:
                    ingest_status = ingest_status_for_api_pull_run(
                        conn,
                        cfg["api_pull_run_id"],
                        expected_parent_run_id=cfg.get("dag_ingest_parent_run_id"),
                    )
                if ingest_status == "succeeded":
                    promoted = store.promote(
                        domain=cfg["domain"],
                        dataset=cfg["dataset"],
                        source_application=cfg["source_application"],
                        run_id=cfg["api_pull_run_id"],
                    )
                    ods_pipeline.runs.update(
                        conn,
                        cfg["api_pull_run_id"],
                        status="succeeded",
                    )
                    ods_pipeline.events.produce(
                        "api_pull.completed",
                        run_id=cfg["api_pull_run_id"],
                        domain=cfg["domain"],
                        dataset=cfg["dataset"],
                        business_date=cfg["business_date"],
                        status="succeeded",
                    )
                    if not promoted:
                        # Another sensor beat us to it — accept silently.
                        pass
                elif ingest_status in ("failed", "partial"):
                    store.clear_pending(
                        domain=cfg["domain"],
                        dataset=cfg["dataset"],
                        source_application=cfg["source_application"],
                        run_id=cfg["api_pull_run_id"],
                    )
                    ods_pipeline.runs.update(
                        conn,
                        cfg["api_pull_run_id"],
                        status="failed",
                        error_summary=f"downstream dag_ingest ended {ingest_status}",
                    )
                    ods_pipeline.events.produce(
                        "api_pull.failed",
                        run_id=cfg["api_pull_run_id"],
                        domain=cfg["domain"],
                        dataset=cfg["dataset"],
                        business_date=cfg["business_date"],
                        status="failed",
                    )
                else:
                    still_pending.append(cfg)
            pending = still_pending
            if pending:
                time.sleep(DOWNSTREAM_POLL_SECONDS)
        for cfg in pending:
            # Timed out waiting on downstream — discard pending so next
            # poll re-issues the window. Run is left in 'running'; the
            # downstream completion (if it eventually arrives) will be
            # observed on a later DAG invocation.
            store.clear_pending(
                domain=cfg["domain"],
                dataset=cfg["dataset"],
                source_application=cfg["source_application"],
                run_id=cfg["api_pull_run_id"],
            )
            ods_pipeline.runs.update(
                conn,
                cfg["api_pull_run_id"],
                status="partial",
                error_summary="downstream dag_ingest did not finish before sensor timeout",
            )
    finally:
        conn.close()


# _ingest_status_for_api_pull_run lives in ods_pipeline.ingest.api_pull.linkage
# (re-exported as ingest_status_for_api_pull_run) so plain pytest, without
# Airflow installed, can exercise the SQL contract directly.


@task
def filter_triggerable(polled: list[dict | None]) -> list[dict]:
    """Drop None entries (no_changes / lock loss) AND direct-Kafka entries
    so TriggerDagRunOperator only receives configs for the file-pipeline
    path. Direct-Kafka has no ``dag_ingest`` to trigger; its sink lag is
    awaited by ``finalise_watermark`` instead."""
    return [
        p for p in (polled or [])
        if p and p.get("delivery", "file_pipeline") == "file_pipeline"
    ]


with DAG(
    dag_id="dag_api_pull",
    start_date=pendulum.datetime(2026, 5, 1, tz="UTC"),
    schedule="*/15 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["ods", "api"],
):
    datasets = list_active_api_datasets()
    polled = poll_one.expand(cfg=datasets)
    triggerable = filter_triggerable(polled)
    triggered = TriggerDagRunOperator.partial(
        task_id="trigger_ingest",
        trigger_dag_id="dag_ingest",
        wait_for_completion=False,
    ).expand(conf=triggerable)
    finaliser = finalise_watermark(polled)
    triggered >> finaliser
