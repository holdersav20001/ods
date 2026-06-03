"""Developer SDK context managers (spec area 5) — live round-trip tests.

The SDK (control/sdk.py) is a THIN ergonomic wrapper over the existing control
wrappers (runs/stages/lineage/recon). These tests prove the spec's "Required
Tests": run lifecycle, stage lifecycle, failure recording + re-raise, output
creation, and that the returned ids (run_id, output_link_id) are usable by a
trace (cp.v_provenance back to the raw input).

EVERY call passes commit=False; the `conn` fixture rolls back for isolation.
"""
import datetime
import uuid

import pytest

from control import runs
from control.sdk import task

BD = datetime.date(2025, 3, 20)


def _file(conn):
    """A real registered raw file, to anchor a well-formed raw_to_curated edge
    (012 raw_edge_requires_source_file)."""
    return runs.register_file(
        conn, s3_raw_path=f"s3://raw/{uuid.uuid4()}.csv",
        file_md5=uuid.uuid4().hex, business_date=BD,
        domain="insurance", dataset="claim", commit=False)


def _open_task(conn):
    """task(...) kwargs for a canonicalization run on the insurance/claim slice."""
    return task(
        conn,
        workflow_run_id=str(uuid.uuid4()),
        pipeline_type="canonicalization",
        domain="insurance",
        dataset="claim",
        business_date=BD,
        trigger_type="manual",
        commit=False,
    )


# 1 — successful task: run is running on enter, succeeded on clean exit.
def test_task_success_creates_and_finalises_run(conn):
    with _open_task(conn) as run:
        run_id = run.run_id
        uuid.UUID(run_id)  # the id is exposed and is a real uuid
        status = conn.execute(
            "SELECT status FROM cp.run_log WHERE run_id=%s", (run_id,)
        ).fetchone()[0]
        assert status == "running"

    status, finished = conn.execute(
        "SELECT status, finished_at FROM cp.run_log WHERE run_id=%s", (run_id,)
    ).fetchone()
    assert status == "succeeded"
    assert finished is not None


# 2 — exception inside the task body marks the run failed AND re-raises.
def test_task_exception_marks_run_failed_and_reraises(conn):
    captured = {}
    with pytest.raises(ValueError, match="boom"):
        with _open_task(conn) as run:
            captured["run_id"] = run.run_id
            raise ValueError("boom")

    status, error = conn.execute(
        "SELECT status, error FROM cp.run_log WHERE run_id=%s",
        (captured["run_id"],)
    ).fetchone()
    assert status == "failed"
    assert error and "boom" in error


# 3 — stage context creates a run_stage_log row and finishes it succeeded.
def test_stage_success_records_counts_and_metrics(conn):
    with _open_task(conn) as run:
        with run.stage("validate_schema") as stage:
            stage_log_id = stage.stage_log_id
            assert stage_log_id is not None  # the stage id is exposed (bigserial)
            stage.finish(record_count_in=4, record_count_out=3,
                         metrics={"dropped": 1})

    status, rin, rout, metrics, finished = conn.execute(
        "SELECT status, record_count_in, record_count_out, metrics, finished_at "
        "FROM cp.run_stage_log WHERE stage_log_id=%s", (stage_log_id,)
    ).fetchone()
    assert status == "succeeded"
    assert (rin, rout) == (4, 3)
    assert metrics == {"dropped": 1}
    assert finished is not None


# 4 — exception inside a stage marks the stage failed and propagates (the run is
#     then also marked failed by the task scope; we assert the stage clearly).
def test_stage_exception_marks_stage_failed_and_propagates(conn):
    captured = {}
    with pytest.raises(ValueError, match="bad row"):
        with _open_task(conn) as run:
            with run.stage("validate_schema") as stage:
                captured["stage_log_id"] = stage.stage_log_id
                raise ValueError("bad row")

    stage_status = conn.execute(
        "SELECT status FROM cp.run_stage_log WHERE stage_log_id=%s",
        (captured["stage_log_id"],)
    ).fetchone()[0]
    assert stage_status == "failed"


# 5 — write_output creates an output_link + its input_edge(s) and returns the id.
def test_write_output_creates_link_and_input_edge(conn):
    file_id = _file(conn)
    with _open_task(conn) as run:
        output_link_id = run.write_output(
            edge_type="raw_to_curated",
            target_ref={"path": "s3://curated/claim", "content_hash": "c1",
                        "version": 1, "schema_version": "claim.v1"},
            record_count=3,
            inputs=[{"edge_type": "raw_to_curated", "source_file_id": file_id,
                     "record_count": 3}],
        )
        # ids are not hidden: returned value, last_output_link_id, and the list
        assert isinstance(output_link_id, str)
        assert run.last_output_link_id == output_link_id
        assert run.output_link_ids == [output_link_id]

    link = conn.execute(
        "SELECT 1 FROM cp.lineage_link WHERE lineage_link_id=%s",
        (output_link_id,)).fetchone()
    assert link is not None
    n_edges, src = conn.execute(
        "SELECT count(*), max(source_file_id::text) FROM cp.lineage_edge "
        "WHERE lineage_link_id=%s", (output_link_id,)).fetchone()
    assert n_edges == 1
    assert src == file_id


# 6 — the returned ids are usable by a trace: cp.v_provenance from the
#     output_link_id reaches the raw input source_file_id.
def test_returned_ids_trace_to_raw_via_provenance(conn):
    file_id = _file(conn)
    with _open_task(conn) as run:
        run_id = run.run_id
        output_link_id = run.write_output(
            edge_type="raw_to_curated",
            target_ref={"path": "s3://curated/claim-trace", "content_hash": "c2",
                        "version": 1, "schema_version": "claim.v1"},
            record_count=3,
            inputs=[{"edge_type": "raw_to_curated", "source_file_id": file_id,
                     "record_count": 3}],
        )

    # the output link's consumer is this run
    consumer = conn.execute(
        "SELECT consumer_run_id FROM cp.lineage_link WHERE lineage_link_id=%s",
        (output_link_id,)).fetchone()[0]
    assert str(consumer) == run_id

    # the trace from the output_link_id reaches the raw input file
    reached = conn.execute(
        "SELECT source_file_id::text FROM cp.v_provenance "
        "WHERE lineage_link_id=%s AND source_file_id IS NOT NULL",
        (output_link_id,)).fetchall()
    assert [r[0] for r in reached] == [file_id]
