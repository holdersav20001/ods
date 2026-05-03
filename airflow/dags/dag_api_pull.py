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
import os
import sys
import time
import uuid
from datetime import datetime, timezone

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
    ingest_status_for_api_pull_run,
    poll_and_archive,
)
from ods_pipeline.models import Stage, StageEvent


PG_DSN = os.environ.get(
    "PIPELINE_PG_DSN",
    "host=postgres port=5432 dbname=ods_dev user=ods password=ods",
)
S3_RAW_BUCKET = os.environ.get("S3_RAW_BUCKET", "ods-raw-local")
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://localstack:4566")
DOWNSTREAM_POLL_SECONDS = float(os.environ.get("API_PULL_DOWNSTREAM_POLL_SECONDS", "5"))
DOWNSTREAM_TIMEOUT_SECONDS = float(os.environ.get("API_PULL_DOWNSTREAM_TIMEOUT_SECONDS", "1800"))


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
                       config_version_id
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
        raw_format, source_config, config_version_id,
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
        })
    return out


@task
def poll_one(cfg: dict) -> dict | None:
    """Run one poll for one dataset and prepare a dag_ingest trigger conf.

    Returns ``None`` (so the downstream TriggerDagRunOperator.expand entry
    is skipped) when there are no new records — the watermark is left
    unchanged so the next schedule re-issues the same window if that
    changes upstream.
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
        ods_pipeline.stages.start(
            conn,
            run_id=run_id,
            stage=Stage.RAW_POLL,
            metrics={
                "source_application": source_application,
                "cursor_style": cursor_style,
                "committed_cursor_value": watermark.committed_cursor_value,
            },
        )

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

        ods_pipeline.stages.finish(
            conn,
            run_id=run_id,
            stage=Stage.RAW_POLL,
            status="succeeded" if not archive.no_changes else "skipped",
            event_type=StageEvent.COMPLETED if not archive.no_changes else StageEvent.SKIPPED,
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
                record_count_published=0,
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
            child_run_id=run_id,
            parent_file_id=file_id,
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
            kafka_count=None,
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

    # ``triggered_by_run_id`` lets dag_ingest record this poll as a parent
    # link in run_log.parents. finalise_watermark looks up the downstream
    # parent run by JSONB containment of this exact api_pull run_id, so a
    # replay or concurrent run on the same file_id cannot promote/clear
    # the wrong cursor.
    return {
        "file_id": file_id,
        "domain": domain,
        "dataset": dataset,
        "business_date": business_date,
        "api_pull_run_id": run_id,
        "source_application": source_application,
        "new_cursor_value": archive.new_cursor_value,
        "triggered_by_run_id": run_id,
        "triggered_by_edge_type": "triggered_by_api_pull",
    }


@task(trigger_rule=TriggerRule.ALL_DONE)
def finalise_watermark(triggered_confs: list[dict | None]) -> None:
    """Wait for each triggered dag_ingest run to finish, then promote or
    clear the corresponding pending cursor.

    Implemented as a polling sensor task rather than coupling
    dag_ingest.finalise to the api_pull control-plane: dag_ingest stays
    pattern-agnostic and dag_api_pull owns its own commit lifecycle.
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
                ingest_status = ingest_status_for_api_pull_run(
                    conn, cfg["api_pull_run_id"],
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
    """Drop None entries (no_changes / lock loss) so TriggerDagRunOperator
    only receives valid dag_ingest configs."""
    return [p for p in (polled or []) if p]


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
