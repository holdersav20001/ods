"""Finalisation step — lineage edge + file_catalogue + run.status + event.

Runs in two shapes:

* :func:`finalise_success` — write the raw→curated lineage edge, mark
  ``file_catalogue.state='curated'``, flip ``run_log`` to ``'succeeded'``,
  set ``file_catalogue.state='completed'``, and emit the
  ``ingestion.completed`` event with success metrics.

* :func:`finalise_failure` — flip ``run_log`` to ``'failed'``, mark the
  file as ``'failed'``, and emit the failure event. Does NOT write a
  lineage edge (no curated artifact was produced).

Stateless write order matters here: lineage edge writes BEFORE the run
flips to ``succeeded`` so the dashboard rule "succeeded ⇒ proof rows
present" holds.
"""
from __future__ import annotations

from typing import Any

import ods_pipeline
from glue.jobs.ingestion import registration


def finalise_success(
    conn: Any,
    *,
    run_id: str,
    domain: str,
    dataset: str,
    business_date: str,
    file_id: str,
    file_md5: str,
    s3_input_path: str,
    curated_uri: str,
    source_count: int,
    written_count: int,
    failing_count: int,
) -> None:
    """Write the success-path control-plane rows in canonical order."""
    # 1. Lineage first — so "succeeded" later implies the edge exists.
    ods_pipeline.lineage.write_edge(
        conn,
        child_run_id=run_id,
        parent_file_id=file_id,
        edge_type="raw_to_curated",
        source_ref=s3_input_path,
        target_ref=curated_uri,
        record_count=written_count,
    )

    # 2. file_catalogue: ingesting → curated.
    registration.mark_curated(conn, file_id=file_id, curated_uri=curated_uri)

    # 3. run_log → succeeded with final counts.
    ods_pipeline.runs.update(
        conn, run_id,
        status="succeeded",
        record_count_source=source_count,
        record_count_dq_pass=written_count,
        record_count_dq_fail=failing_count,
    )

    # 4. file_catalogue: curated → completed.
    registration.mark_completed(
        conn,
        s3_input_path=s3_input_path,
        run_id=run_id,
        record_count=written_count,
    )

    # 5. Emit the success event last — consumers (Slack alerts, recon
    #    dashboards) only fire after all the durable rows are in place.
    ods_pipeline.events.produce(
        "ingestion.completed", run_id, domain, dataset,
        business_date, "succeeded",
        pipeline_type="ingestion",
        file_id=file_id,
        s3_raw_path=s3_input_path,
        s3_curated_path=curated_uri,
        file_md5=file_md5,
        record_count_source=source_count,
        record_count_dq_pass=written_count,
        record_count_dq_fail=failing_count,
    )


def finalise_failure(
    conn: Any,
    *,
    run_id: str,
    domain: str,
    dataset: str,
    business_date: str | None,
    file_id: str | None,
    file_md5: str | None,
    s3_input_path: str,
    error_summary: str,
    source_count: int | None = None,
    dq_pass_count: int | None = None,
    failing_count: int | None = None,
) -> None:
    """Write the failure-path control-plane rows in canonical order."""
    ods_pipeline.runs.update(
        conn, run_id, status="failed", error_summary=error_summary,
    )
    registration.mark_failed(
        conn, s3_input_path=s3_input_path, run_id=run_id, reason=error_summary,
    )
    ods_pipeline.events.produce(
        "ingestion.completed", run_id, domain, dataset,
        business_date or "", "failed",
        pipeline_type="ingestion",
        file_id=file_id,
        file_md5=file_md5,
        s3_raw_path=s3_input_path,
        error_summary=error_summary,
        record_count_source=source_count,
        record_count_dq_pass=dq_pass_count,
        record_count_dq_fail=failing_count,
    )
